# Admin Purge Queue (DLQ-Aware) for the SQS Web Console

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-05-16

---

## 1. Background and Motivation

The elastickv SQS Web Console (`web/admin/src/pages/SqsDetail.tsx`) currently lets an operator **list, describe, and delete** queues, but not **purge their contents**. A queue that should keep existing (its name, ARN, RedrivePolicy, tags) but whose messages should be drained has no path through the admin SPA — the operator must drop to the AWS CLI or `cmd/elastickv-admin`, SigV4-sign a `PurgeQueue` call against the public SQS endpoint, and re-authenticate against whatever credentials store backs that path.

The primary use case is **clearing a Dead-Letter Queue (DLQ)**. Failed messages routed to a DLQ accumulate indefinitely once `RedrivePolicy.maxReceiveCount` fires: the redriver in `adapter/sqs_redrive.go` moves the message and the DLQ has no automatic eviction. After a long-running incident has been triaged and operators have decided the DLQ contents are no longer useful (root cause addressed, messages already exported to another system, etc.), they need to clear the DLQ without dropping the queue itself — recreating it would invalidate the source queue's RedrivePolicy ARN reference for the duration of the recreate.

This document proposes a `AdminPurgeQueue` admin RPC, a corresponding HTTP endpoint, and an SPA button on the queue detail page. The feature works for any queue (DLQ or not) because the underlying mechanism — generation bump via `purgeQueueWithRetry` — is identical; the **UI is DLQ-aware** so the operator gets the right framing and warnings when the queue is in fact a DLQ.

---

## 2. Goals and Non-Goals

### 2.1 Goals

1. **Admin-side purge** — an `AdminPurgeQueue(ctx, principal, name) error` method on `*adapter.SQSServer` that drains the queue's messages while leaving the queue meta, ARN, RedrivePolicy, and tags intact. Mirrors `AdminDeleteQueue`'s shape so the admin authorization, leader check, and audit pattern stay parallel.
2. **HTTP endpoint** at `DELETE /admin/api/v1/sqs/queues/{name}/messages` — RESTful "delete messages, not the queue". The collection root semantics avoid collision with the existing `DELETE /admin/api/v1/sqs/queues/{name}` (delete the queue itself).
3. **SPA button** on the queue detail page (`web/admin/src/pages/SqsDetail.tsx`) labelled either "Purge messages" or "Purge DLQ" depending on whether the queue is a DLQ. Confirmation modal requires typing the queue name, mirroring the existing Delete confirmation.
4. **DLQ awareness** — `AdminQueueSummary` gains two fields:
   - `IsDLQ bool` — true iff at least one other queue's `RedrivePolicy.deadLetterTargetArn` resolves to this queue.
   - `DLQSources []string` — the source-queue names that point at this queue. The SPA renders these as a chip list so the operator can confirm they understand what queue feeds the DLQ before purging.
5. **Same AWS-shaped error mapping** as the SigV4 path — purging more than once per 60 seconds returns the SQS `PurgeQueueInProgress` semantics that `tryPurgeQueueOnce` already enforces. The admin response surfaces it as a structured `429 Too Many Requests` JSON payload (`{"code":"PurgeQueueInProgress", "retry_after_seconds":N}`).
6. **Audit** — same `slog` audit line as `AdminDeleteQueue` (`admin.sqs.purge_queue` with principal subject, queue name, generation before/after).
7. **Read-only role hard-fails** with `ErrAdminForbidden`. The role check uses the live role store (`principalForWrite`), not the JWT-cached role, matching the existing pattern in `internal/admin/sqs_handler.go`.

### 2.2 Non-Goals

1. **Redrive ("StartMessageMoveTask")** — moving DLQ messages back to the source queue is a separate feature with very different semantics (per-message dispatch, throttling, partial-success handling). Tracked as future work in `2026_04_26_partial_sqs_split_queue_fifo.md` extensions / a future RFC. The button on the SPA will not pretend to redrive.
2. **Filtered purge** ("purge messages older than X", "purge messages matching attribute Y"). The wire-level `PurgeQueue` is all-or-nothing and we mirror that; selective deletion belongs to a future `DeleteMessageBatch` automation. AWS itself only offers all-or-nothing `PurgeQueue`.
3. **Cross-queue bulk purge** ("purge every DLQ in this region"). Operators do this themselves with a script that calls the admin endpoint N times; building a bulk API would require new throttling rules and is not the user-facing pain point.
4. **Per-message inspection** ("Peek the DLQ before purging"). The base SQS adapter's design doc §13 (`2026_04_24_proposed_sqs_compatible_adapter.md`) already mentions a console Peek tab; whether that ships is independent of this proposal.
5. **Restoring purged messages.** A purge is final by design — the generation bump makes the old messages unreachable and the reaper eventually deletes them. Operators who need a recovery window must export the queue first (out of scope here).

---

## 3. Design

### 3.1 Backend: AdminPurgeQueue RPC

```go
// AdminPurgeQueue is the SigV4-bypass counterpart to purgeQueue.
// Bumps the queue's generation so every message under the old
// generation becomes unreachable, leaving the meta record (name,
// ARN, RedrivePolicy, tags, attributes) in place. The reaper
// eventually deletes the orphaned message keys via the existing
// tombstone path.
//
// Mirrors AdminDeleteQueue's sentinel-error surface:
//   - ErrAdminForbidden          — read-only principal
//   - ErrAdminNotLeader          — follower
//   - ErrAdminSQSNotFound        — queue absent
//   - ErrAdminSQSPurgeInProgress — last purge < 60 s ago (new sentinel)
//   - ErrAdminSQSValidation      — empty / malformed name
func (s *SQSServer) AdminPurgeQueue(
    ctx context.Context,
    principal AdminPrincipal,
    name string,
) error
```

Implementation reuses `purgeQueueWithRetry` verbatim — the SigV4 wire handler at `adapter/sqs_purge.go:22` already wraps it, and the admin handler does the same. The 60-second rate-limit lives on the meta record (`tryPurgeQueueOnce` reads it) so it survives leader failover and is enforced uniformly whether the call came from the SigV4 path or the admin path.

The `PurgeQueueInProgress` mapping: `tryPurgeQueueOnce` returns an `sqsAPIError` with code `PurgeQueueInProgress` and HTTP 400 when the 60-second window is active. The admin wrapper sniffs this via a new helper `isSQSAdminPurgeInProgress(err)` (parallel to the existing `isSQSAdminQueueDoesNotExist`) and returns `ErrAdminSQSPurgeInProgress`. The HTTP handler maps that to `429 Too Many Requests` with `Retry-After` header populated from the meta's `LastPurgedAtMillis + 60s - now()`.

### 3.2 Backend: AdminQueueSummary extensions

Two new fields on `AdminQueueSummary`:

```go
type AdminQueueSummary struct {
    Name       string
    IsFIFO     bool
    Generation uint64
    CreatedAt  time.Time
    Attributes map[string]string
    Counters   AdminQueueCounters
    // New:
    IsDLQ      bool     // some other queue's RedrivePolicy points here
    DLQSources []string // names of those queues, sorted
}
```

`AdminDescribeQueue` populates `IsDLQ` / `DLQSources` by reverse-scanning the catalog: walk every queue's meta, parse its `RedrivePolicy` via `parseRedrivePolicy` (already in `adapter/sqs_redrive.go:43`), match the resolved DLQ target against `name`. The scan is bounded by the catalog cap that `scanApproxCounters` already enforces, so cost is `O(known queues)` per Describe call — the same order the existing counter scan pays.

A future optimization could maintain a reverse-index (`!sqs|catalog|dlq_source|<dlq_name>|<source_name>`) updated by `CreateQueue` / `SetQueueAttributes` / `DeleteQueue`. Out of scope for this PR; the reverse-scan is correct and simple, and the catalog cap (`sqsCatalogScanCap = 1024`) already bounds the work. The reverse-index can land later if profiling shows the Describe path is hot.

### 3.3 Backend: HTTP endpoint

| Method   | Path                                              | Purpose                                |
|----------|---------------------------------------------------|----------------------------------------|
| `DELETE` | `/admin/api/v1/sqs/queues/{name}` *(existing)*    | Delete the queue itself                |
| `DELETE` | `/admin/api/v1/sqs/queues/{name}/messages` *(new)*| Purge the queue's messages, keep meta  |

The router in `internal/admin/sqs_handler.go` already path-prefixes on `pathSqsQueues = "/admin/api/v1/sqs/queues"`. The new endpoint is dispatched by suffix match: `strings.HasSuffix(trailing, "/messages")` after the `{name}` segment is parsed. A new handler `handlePurge` lives in `sqs_handler.go` parallel to `handleDelete`, calling `source.PurgeQueue(ctx, name)` against the `QueuesSource` interface (which is implemented by `sqsQueuesBridge.PurgeQueue` in `main_admin.go`, which itself wraps `AdminPurgeQueue`).

Response shape on success: `204 No Content`. Same as `handleDelete`. On `ErrAdminSQSPurgeInProgress`: `429 Too Many Requests` with body `{"code":"PurgeQueueInProgress", "retry_after_seconds":42}` and `Retry-After: 42` header.

### 3.4 Frontend: SqsDetailPage button

Two states for the button:

| Queue type   | Button label          | Confirmation copy                                                                                                                           |
|--------------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| Not a DLQ    | "Purge messages"      | "This will permanently delete every message in **{name}**. The queue itself remains. Type **{name}** to confirm."                            |
| Is a DLQ     | "Purge DLQ"           | "This queue is the DLQ for **{sources}**. Purging deletes every failed message routed here. Type **{name}** to confirm."                     |

The button sits next to the existing "Delete queue" button. The confirmation flow reuses the `Modal` component already used for Delete. On submit, the SPA `POST`s … actually, `DELETE`s the new endpoint, then refreshes the queue detail. On 429 (PurgeQueueInProgress), the modal stays open and shows the retry-after countdown derived from the response header.

API client gets one new method:

```ts
// web/admin/src/api/client.ts
purgeQueue(name: string): Promise<void>
```

mirroring the existing `deleteQueue` shape.

### 3.5 Audit and observability

New structured log line at `slog.Info` level (matches `AdminDeleteQueue`):

```
admin.sqs.purge_queue
  subject=<principal.Subject>
  role=<principal.Role>
  queue=<name>
  generation_before=<n>
  generation_after=<n+1>
  is_dlq=<bool>
  dlq_sources=<csv>
```

New Prometheus counter on the existing `monitoring.SQSMetrics` registry:

```
elastickv_sqs_admin_purge_queue_total{queue, outcome}
  outcome ∈ {"ok", "forbidden", "not_leader", "not_found", "purge_in_progress", "internal_error"}
```

The label cardinality budget is the same `sqsMaxTrackedQueues = 512` cap that already governs the depth gauges — the same `admitForCounterBudget` helper from PR #743 r3 handles the collapse to `_other`.

---

## 4. Authorization

`AdminPurgeQueue` requires the **full-access** role. A read-only operator who clicks the button in the SPA receives a 403 from the backend (the live-role re-check in `principalForWrite` runs before the handler dispatches), and the modal surfaces the error in its existing error region. The button itself stays visible for read-only users — same UX choice as Delete, documented in `SqsDetailPage`'s existing comment.

The 60-second rate-limit is **not** an authorization concern (any caller hitting it gets the same 429), but it interacts with the audit log: a 429 still emits an audit line with `outcome=purge_in_progress` so a flood of rate-limited attempts is visible to operators.

---

## 5. Failure Modes

1. **Concurrent purge from SigV4 path and admin path.** The 60-second meta-stored rate limit applies uniformly. Whichever path's `tryPurgeQueueOnce` reads the meta first wins; the other gets `PurgeQueueInProgress` on its next attempt. No data corruption; both retries see a consistent generation.
2. **Leader change mid-purge.** `purgeQueueWithRetry` already handles this — the retry loop sees `ErrAdminNotLeader` (translated from the underlying coordinator error), the admin handler propagates the structured error, the SPA shows a "leader unavailable" message. No half-purged state because the generation bump is a single OCC commit.
3. **Catalog inconsistency (`IsDLQ` stale).** The reverse-scan reads the catalog at the read-side timestamp; a queue created or destroyed between the scan and the purge can change the `IsDLQ` flag we surfaced. This is a UI display issue, not a correctness issue — the purge itself doesn't care whether the queue is a DLQ. Worst case the SPA labels the button "Purge messages" for a queue that became a DLQ a millisecond ago; the underlying operation is the same.
4. **Purge of a non-existent queue.** Returns `ErrAdminSQSNotFound` → HTTP 404. SPA shows a "queue no longer exists" toast and navigates back to the queue list.
5. **Massive queue.** `purgeQueueWithRetry` is `O(1)` in message count — only the meta record is touched. Cleanup of the orphaned message keys is the reaper's job and is paced by `sqsReaperInterval`. The admin call returns as soon as the OCC commit lands.

---

## 6. Testing Plan

1. **Unit:** `TestAdminPurgeQueue_*` in `adapter/sqs_admin_test.go`:
   - happy path bumps the generation
   - read-only principal → `ErrAdminForbidden`
   - follower → `ErrAdminNotLeader`
   - missing queue → `ErrAdminSQSNotFound`
   - second purge within 60 s → `ErrAdminSQSPurgeInProgress`
   - empty name → `ErrAdminSQSValidation`
2. **Unit:** `TestAdminQueueSummary_IsDLQ_*` in `adapter/sqs_admin_test.go`:
   - queue with no inbound RedrivePolicy → `IsDLQ=false, DLQSources=nil`
   - queue referenced by one source → `IsDLQ=true, DLQSources=["source-a"]`
   - queue referenced by two sources → sorted slice
   - queue referenced by itself (paranoid edge) → `IsDLQ=true, DLQSources=["self"]`
3. **Integration:** `TestSqsHandler_PurgeMessages_*` in `internal/admin/sqs_handler_test.go`:
   - `DELETE /admin/api/v1/sqs/queues/orders/messages` against a single-node fixture → 204, generation incremented
   - 429 with `Retry-After` header on the immediate second call
   - Bridge bubbles `ErrAdminForbidden` through to 403
4. **Frontend:** Playwright / RTL test pinning:
   - button label switches between "Purge messages" and "Purge DLQ" based on `is_dlq`
   - confirmation modal requires the exact name (case-sensitive, trimmed)
   - 429 keeps modal open and shows countdown
5. **Jepsen:** out of scope. Purge has been Jepsen-covered since Phase 2 via the SigV4 path; the admin RPC shares the same `tryPurgeQueueOnce` and inherits the coverage.

---

## 7. Rollout Plan

| Phase | Content                                                                                                  |
|-------|----------------------------------------------------------------------------------------------------------|
| 1     | This proposal doc lands (one PR). Operators have time to flag concerns.                                  |
| 2     | Backend: `AdminPurgeQueue` + sentinel errors + `IsDLQ` / `DLQSources` on `AdminQueueSummary` + tests.    |
| 3     | HTTP handler in `internal/admin/sqs_handler.go` + bridge in `main_admin.go` + integration tests.          |
| 4     | SPA button + confirmation modal + API client wiring + frontend tests.                                    |
| 5     | Doc rename `_proposed_` → `_implemented_`.                                                               |

Phases 2 / 3 / 4 each ship as separate PRs so they review small. Phase 4 lands the user-visible feature.

---

## 8. Open Questions

1. **Should the SPA also show inbound DLQ links from the source side?** I.e., on a non-DLQ queue's detail page, show "DLQ: arn:aws:sqs:…:failures" as a navigation chip. Useful but separable; not blocking this proposal.
2. **Reverse-index for `IsDLQ` lookup.** The reverse-scan is correct but `O(known queues)` per Describe. If the Describe path turns out to be hot in production, fold in the reverse-index. Leaving it out for now per "don't add abstraction beyond what the task requires".
3. **Per-DLQ purge metrics retention.** Should we keep `elastickv_sqs_admin_purge_queue_total` cardinality bounded per queue forever, or expire entries for queues that no longer exist? The existing `monitoring.SQSMetrics.ForgetQueue` lifecycle (PR #743) already handles this for the depth gauges; reusing it for the new counter is a one-liner.

---

## 9. Alternatives Considered

### 9.1 Reuse the existing `DELETE /admin/api/v1/sqs/queues/{name}` endpoint with a query parameter

`?messages_only=true` would avoid the new path. Rejected: REST collection semantics get muddier (a path that sometimes deletes the collection and sometimes its members based on a query param fails to "do one thing"), and clients that fail closed on unknown query params would silently full-delete the queue. The two operations are different enough to deserve different paths.

### 9.2 Bundle purge into a "Drain" button that also redrives

I.e., redrive everything back to the source queue if this is a DLQ, drop the rest. Rejected: the redrive path has very different semantics (per-message handling, partial success, throttling against the source) and conflating the two would force operators to think about both at once. The user's request is specifically "clear", not "recover".

### 9.3 Expose the SigV4 PurgeQueue endpoint via the admin proxy

Just forward the AWS-shape JSON-1.0 request through. Rejected: the admin SPA already speaks elastickv-native JSON for every other operation; forcing it to construct SigV4-flavored bodies just to purge is gratuitous coupling. The cost of `AdminPurgeQueue` is one short wrapper, and it stays parallel to `AdminDeleteQueue`.

---

## 10. Summary

`AdminPurgeQueue` wraps the existing `purgeQueueWithRetry` in the same shape `AdminDeleteQueue` already uses for delete. `AdminQueueSummary` gains an `IsDLQ` flag (reverse-scan today; reverse-index later if profiling demands) so the SPA can frame the confirmation modal appropriately. The user-visible feature is one button on the queue detail page; the design surface is intentionally small because the underlying primitive (generation bump) is already production-tested on the SigV4 path.
