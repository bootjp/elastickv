# Admin Queue Peek and Purge (DLQ-Aware) for the SQS Web Console

**Status:** Proposed
**Author:** bootjp
**Date:** 2026-05-16

---

## 1. Background and Motivation

The elastickv SQS Web Console (`web/admin/src/pages/SqsDetail.tsx`) currently lets an operator **list, describe, and delete** queues, but not **inspect or purge their contents**. Two operator workflows are blocked:

1. **Triage a DLQ before purging it.** Failed messages routed to a DLQ accumulate indefinitely once `RedrivePolicy.maxReceiveCount` fires; the redriver in `adapter/sqs_redrive.go` moves the message and the DLQ has no automatic eviction. Before clearing the backlog the operator needs to see what's actually in there — message bodies, sent timestamps, group IDs, receive counts — to decide whether the messages are still useful (e.g. should be exported to another system) or genuinely safe to drop. Today they must drop to the AWS CLI or `cmd/elastickv-admin`, SigV4-sign `ReceiveMessage` calls against the public endpoint, and re-authenticate against whatever credentials store backs that path. Worse, `ReceiveMessage` is destructive in the sense that it bumps the receive count and starts the visibility timer — every triage glance can advance a message toward purge eligibility under the very RedrivePolicy that put it in the DLQ in the first place.

2. **Drain a DLQ (or any queue) without dropping the queue itself.** Once the operator has triaged, they need to clear the DLQ. The queue itself must keep existing — its ARN is referenced by the source queue's RedrivePolicy, and recreating it would invalidate that reference for the duration of the recreate.

This document proposes:

- `AdminPeekQueue` — read-only, non-destructive sample of a queue's messages. Surfaces body / attributes / timestamps / receive counts without changing visibility or issuing receipt handles.
- `AdminPurgeQueue` — same wire-level guarantee as the SigV4 `PurgeQueue` (generation bump via `purgeQueueWithRetry`), exposed through the admin auth path.
- Corresponding HTTP endpoints and SPA views.

Both features work for any queue. The **UI is DLQ-aware** so the operator gets the right framing and warnings when the queue is in fact a DLQ (e.g. "This queue is the DLQ for orders, payments — these are messages that failed processing"). Peek + Purge cover the full triage-then-clear workflow without leaving the Web Console.

---

## 2. Goals and Non-Goals

### 2.1 Goals

1. **Admin-side peek** — an `AdminPeekQueue(ctx, principal, name, opts) ([]AdminPeekedMessage, string, error)` method on `*adapter.SQSServer` that returns a non-destructive sample of currently-visible messages. The call does NOT bump receive counts, does NOT start a visibility timer, and does NOT issue receipt handles — it is a pure read over the message keyspace. `opts.Limit` caps results (default 20, max 100); the returned continuation token lets the SPA paginate.
2. **Admin-side purge** — an `AdminPurgeQueue(ctx, principal, name) error` method that drains the queue's messages while leaving the queue meta, ARN, RedrivePolicy, and tags intact. Mirrors `AdminDeleteQueue`'s shape so the admin authorization, leader check, and audit pattern stay parallel.
3. **HTTP endpoints**:
   - `GET    /admin/api/v1/sqs/queues/{name}/messages?limit=N&cursor=C` — peek (non-destructive)
   - `DELETE /admin/api/v1/sqs/queues/{name}/messages`                  — purge
   The collection root semantics keep the queue itself (`{name}`) endpoint distinct from operations on its messages (`{name}/messages`).
4. **SPA views** on the queue detail page (`web/admin/src/pages/SqsDetail.tsx`):
   - **Messages tab**: paginated table of peeked messages (body preview, sent time, group ID for FIFO, receive count, message ID). Body is truncated to 256 chars in the row; a row click opens a modal with the full body + every attribute. Page size selector (20 / 50 / 100). "Refresh" button re-fetches the current page.
   - **Purge button**: labelled either "Purge messages" or "Purge DLQ" depending on whether the queue is a DLQ. Confirmation modal requires typing the queue name, mirroring the existing Delete confirmation.
5. **DLQ awareness** — `AdminQueueSummary` gains two fields:
   - `IsDLQ bool` — true iff at least one other queue's `RedrivePolicy.deadLetterTargetArn` resolves to this queue.
   - `DLQSources []string` — the source-queue names that point at this queue. The SPA renders these as a chip list on the detail page so the operator can confirm they understand what queue feeds the DLQ before purging.
6. **Same AWS-shaped error mapping** as the SigV4 path — purging more than once per 60 seconds returns the SQS `PurgeQueueInProgress` semantics that `tryPurgeQueueOnce` already enforces. The admin response surfaces it as a structured `429 Too Many Requests` JSON payload (`{"code":"PurgeQueueInProgress", "retry_after_seconds":N}`).
7. **Audit** — `admin.sqs.purge_queue` (subject, role, queue, generation_before, generation_after). Peek is a read and does NOT generate an audit line per call (the SPA polls; per-poll audit would drown the log) but the admin handler emits the standard request-log line with `route` / `subject` / `status_code` so the call is still traceable.
8. **Read-only role can peek but not purge.** Peek is gated by `principalForRead` (same as List / Describe); purge is gated by `principalForWrite`. Both checks use the live role store, not the JWT-cached role, matching the existing pattern in `internal/admin/sqs_handler.go`.

### 2.2 Non-Goals

1. **Redrive ("StartMessageMoveTask")** — moving DLQ messages back to the source queue is a separate feature with very different semantics (per-message dispatch, throttling, partial-success handling). The triage Peek view enables an operator to make the redrive-vs-purge decision; the actual redrive automation is tracked as future work and a future RFC.
2. **Filtered purge** ("purge messages older than X", "purge messages matching attribute Y"). The wire-level `PurgeQueue` is all-or-nothing and we mirror that; selective deletion belongs to a future `DeleteMessageBatch` automation. AWS itself only offers all-or-nothing `PurgeQueue`.
3. **Per-message deletion from the Peek view** ("delete this one message from the DLQ"). A `Delete` button next to each peeked row sounds useful but the receipt-handle / generation interplay (Peek doesn't mint receipt handles; per-message Delete needs the message's primary key, not its handle) makes the wire shape diverge from AWS. Future RFC if operators ask for it; the all-or-nothing purge plus the Peek view covers the most common triage workflows.
4. **In-flight (invisible) messages in the Peek view.** Peek shows currently-visible messages only — the visibility index on the leader points at `visible_at <= now`. In-flight messages are by definition rented out to some consumer; surfacing them in Peek would confuse "messages I can act on" with "messages another worker is processing". A future PR can add an `?include_in_flight=true` query param with the timer-remaining shown per row.
5. **Cross-queue bulk operations** ("purge every DLQ in this region", "peek across multiple queues"). Operators do this themselves with a script that calls the admin endpoint N times; building a bulk API would require new throttling rules and is not the user-facing pain point.
6. **Restoring purged messages.** A purge is final by design — the generation bump makes the old messages unreachable and the reaper eventually deletes them. Operators who need a recovery window must Peek-and-export first (the Peek view's full-body modal includes a "Copy as JSON" action so manual export is one click per message).

---

## 3. Design

### 3.1 Backend: AdminPeekQueue RPC

```go
// AdminPeekMessageOptions controls a peek call.
type AdminPeekMessageOptions struct {
    // Limit caps the number of messages returned. Clamped to
    // [1, 100]; 0 means "use default (20)".
    Limit int
    // Cursor is an opaque continuation token from a prior call;
    // empty means "start from the front of the visibility index".
    Cursor string
    // BodyMaxBytes truncates message bodies at this length to bound
    // response size. Clamped to [256, 65536]; 0 means "use default
    // (4096)". The full body is always retained on the server; only
    // the wire representation is truncated.
    BodyMaxBytes int
}

// AdminPeekedMessage is one row in the peek result.
type AdminPeekedMessage struct {
    MessageID        string
    Body             string   // truncated per opts.BodyMaxBytes
    BodyTruncated    bool     // true when Body was cut
    BodyOriginalSize int64    // bytes in the original body, for display
    SentTimestamp    time.Time
    ReceiveCount     int32    // ApproximateReceiveCount
    GroupID          string   // FIFO MessageGroupId, empty for standard
    DeduplicationID  string   // FIFO MessageDeduplicationId, empty for standard
    Attributes       map[string]string // SQS message attributes
}

// AdminPeekQueue returns a non-destructive sample of currently-visible
// messages in name. Receive counts are NOT incremented and visibility
// timers are NOT started — peek is a pure read over the leader's
// visibility index. Returns the rows plus a continuation cursor that
// the caller passes back as opts.Cursor to fetch the next page.
//
// Sentinel errors:
//   - ErrAdminForbidden     — peek requires read role; nil principal is denied
//   - ErrAdminNotLeader     — peek runs on the leader (consistent with the
//                             visibility index's leader-only writer; a follower
//                             read would race the leader's index updates)
//   - ErrAdminSQSNotFound   — queue absent
//   - ErrAdminSQSValidation — empty / malformed name, invalid cursor
func (s *SQSServer) AdminPeekQueue(
    ctx context.Context,
    principal AdminPrincipal,
    name string,
    opts AdminPeekMessageOptions,
) (rows []AdminPeekedMessage, nextCursor string, err error)
```

**Implementation.** Peek opens a read transaction at the leader's next read timestamp (`nextTxnReadTS`) and walks the visibility index for the queue. The visibility index is already keyed by `(queue_name, generation, visible_at, message_id)` so the scan naturally yields visible-now messages in `visible_at` order — exactly the order `ReceiveMessage` would deliver. The walk:

1. Decodes the cursor (`encoding/json` over a small struct `{Generation uint64, LastKey []byte}`); empty cursor means "start at the front".
2. Issues a bounded `ScanAt` over the visibility-index prefix up to `Limit` keys; for each key fetches the message record via `GetAt` (one round-trip per row — the visibility index stores only the (visible_at, msg_id) pair, not the body).
3. Truncates bodies to `BodyMaxBytes`, records `BodyTruncated` / `BodyOriginalSize` for the SPA to render "showing first 4 kB of 12 kB" labels.
4. Builds the next cursor from the last scanned key; returns it as an opaque string for the SPA.

Cost is `O(Limit)` round-trips against Pebble at peek time — tiny for the bounded result sets the SPA uses. The bound on `Limit` (max 100) prevents an operator script from accidentally issuing million-row peeks against the leader. Throttle: peek shares the per-queue read-side budget the SigV4 `ReceiveMessage` path consults (see `2026_04_26_proposed_sqs_per_queue_throttling.md` §4.2) so a tight peek loop cannot starve real consumers.

**Why leader-only.** The visibility index is updated by the leader as part of every `SendMessage` / `ReceiveMessage` / `DeleteMessage` commit. A follower read could see an index entry whose underlying message record was already deleted (race between the leader's apply and the follower's lease-read fence). Peek is rare and bursty rather than steady-state, so the lease-read fast path's win doesn't apply; running it on the leader keeps the snapshot internally consistent.

### 3.2 Backend: AdminPurgeQueue RPC

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

### 3.3 Backend: AdminQueueSummary extensions

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

`AdminDescribeQueue` populates `IsDLQ` / `DLQSources` by reverse-scanning the catalog. Implementation: a single `ScanAt` over the queue-meta prefix (`SqsQueueMetaPrefix`, the prefix `scanQueueNames` already walks) returns all queue meta records in one storage round-trip; the handler then unmarshals each, runs `parseRedrivePolicy` (already in `adapter/sqs_redrive.go:43`) against the queue's `RedrivePolicy` attribute, and matches the resolved DLQ target against `name`. Cost is **one Pebble prefix scan + O(N) unmarshal-and-string-compare in memory** — not O(N) point lookups. The catalog cap (`sqsQueueScanPageLimit`, paged) already bounds the scan exactly as `scanQueueNames` does today, so the worst-case load is the same one the existing admin list endpoint pays per request.

A future optimization maintains a reverse-index (`!sqs|catalog|dlq_source|<dlq_name>|<source_name>`) updated transactionally by `CreateQueue` / `SetQueueAttributes` / `DeleteQueue`, reducing Describe-time cost to O(direct sources). Out of scope for this PR; the reverse-scan is correct and the prefix-scan formulation is fast enough that the reverse-index only pays for itself in deployments with many thousands of queues — Gemini's r1 review on this proposal flagged the scan cost, and the reverse-index is the documented next step if production profiling shows the Describe path is hot.

### 3.4 Backend: HTTP endpoints

| Method   | Path                                                                | Purpose                                |
|----------|---------------------------------------------------------------------|----------------------------------------|
| `DELETE` | `/admin/api/v1/sqs/queues/{name}` *(existing)*                      | Delete the queue itself                |
| `GET`    | `/admin/api/v1/sqs/queues/{name}/messages?limit=N&cursor=C` *(new)* | Peek a sample of visible messages      |
| `DELETE` | `/admin/api/v1/sqs/queues/{name}/messages` *(new)*                  | Purge all messages, keep meta intact   |

Method-based dispatch on the same `/messages` sub-resource: `GET` → peek, `DELETE` → purge. The router in `internal/admin/sqs_handler.go` already path-prefixes on `pathSqsQueues = "/admin/api/v1/sqs/queues"`. The new endpoint is dispatched by **path-segment match, not suffix match**: after parsing the `{name}` segment, the handler splits the remaining trailing path into segments (using `path.Clean` first to normalise away leading / trailing / duplicated slashes), and accepts the path iff the trailing segments are exactly `["messages"]`. This rejects `/messages/` (one extra empty segment after Clean), `/messages/foo` (sub-resource we don't define), and `/Messages` (case-sensitive AWS naming) without relying on suffix heuristics. Two new handlers `handlePeek` (GET) and `handlePurge` (DELETE) live in `sqs_handler.go` parallel to `handleDelete`, calling `source.PeekQueue(ctx, name, opts)` / `source.PurgeQueue(ctx, name)` against the `QueuesSource` interface (implemented by `sqsQueuesBridge` in `main_admin.go`, which wraps `AdminPeekQueue` / `AdminPurgeQueue`). The path-segment matcher is the same shape DynamoHandler already uses for its sub-resource routing — keeping the two parallel reduces the cognitive load of operators who reason about both surfaces.

**Peek response shape** (JSON):

```json
{
  "messages": [
    {
      "message_id": "01JCXR8V…",
      "body": "{\"orderId\": 42, …}",
      "body_truncated": false,
      "body_original_size": 142,
      "sent_timestamp": "2026-05-16T09:42:11Z",
      "receive_count": 3,
      "group_id": "tenant-7",
      "deduplication_id": "01JCXR8V-dedup",
      "attributes": { "Source": "checkout", "Retry": "2" }
    },
    …
  ],
  "next_cursor": "eyJHZW5lcmF0aW9uIjoxLCJMYXN0S2V5IjoiZXhhbXBsZSJ9"
}
```

`next_cursor` is omitted when no further pages exist. 404 / 403 / 503 use the existing structured admin-error envelope.

Response shape on success: `204 No Content`. Same as `handleDelete`. On `ErrAdminSQSPurgeInProgress`: `429 Too Many Requests` with body `{"code":"PurgeQueueInProgress", "retry_after_seconds":42}` and `Retry-After: 42` header.

### 3.5 Frontend: SqsDetailPage Messages tab + Purge button

The queue detail page gains two new pieces of UI on top of the existing attributes / counters / delete-button layout:

**Messages tab.** A new tab labelled "Messages" sits next to the existing "Overview" view. On mount it issues `GET /admin/api/v1/sqs/queues/{name}/messages?limit=20` and renders the result as a table:

| Column            | Source                                              | Rendering                                                                                                                          |
|-------------------|-----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| Message ID        | `message_id`                                        | first 8 chars in monospace; full ID in tooltip + copy-on-click                                                                     |
| Sent              | `sent_timestamp`                                    | relative ("2m ago") + absolute ISO in tooltip                                                                                      |
| Group ID          | `group_id` (FIFO only; column hidden for standard)  | as-is in monospace                                                                                                                 |
| Receive count     | `receive_count`                                     | plain integer; muted styling when 0                                                                                                |
| Body preview      | `body` (already truncated by backend)               | first 96 chars; "…" suffix when `body_truncated`. Row click opens detail modal.                                                    |
| Size              | `body_original_size`                                | human-readable ("1.4 kB") so operators can spot oversized messages                                                                 |

Below the table: a page-size selector (20 / 50 / 100), a Refresh button, and Next / Previous controls driven by the cursor. Detail modal shows full body + every attribute + the timestamps; a "Copy as JSON" button copies the row's full record to the clipboard for manual export.

**Purge button.** Sits in the page header next to the existing Delete button. Two states:

| Queue type   | Button label          | Confirmation copy                                                                                                                           |
|--------------|-----------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| Not a DLQ    | "Purge messages"      | "This will permanently delete every message in **{name}**. The queue itself remains. Type **{name}** to confirm."                            |
| Is a DLQ     | "Purge DLQ"           | "This queue is the DLQ for **{sources}**. Purging deletes every failed message routed here. Type **{name}** to confirm."                     |

Confirmation flow reuses the `Modal` component already used for Delete. On submit, the SPA `DELETE`s the new endpoint, then refreshes the queue detail and (if open) the Messages tab. On 429 (PurgeQueueInProgress), the modal stays open and shows the retry-after countdown derived from the response header.

**API client.** Two new methods on `web/admin/src/api/client.ts`:

```ts
peekQueue(
  name: string,
  opts?: { limit?: number; cursor?: string; bodyMaxBytes?: number },
  signal?: AbortSignal,
): Promise<{ messages: PeekedMessage[]; nextCursor?: string }>

purgeQueue(name: string): Promise<void>
```

mirroring the existing `deleteQueue` / `describeQueue` shape. `peekQueue` is `signal`-aware so the SPA can cancel an in-flight peek when the user navigates away or refreshes — matches the existing `useApiQuery` cancellation pattern.

### 3.6 Audit and observability

New structured log line at `slog.Info` level (matches `AdminDeleteQueue`):

```
admin.sqs.purge_queue
  subject=<principal.Subject>
  role=<principal.Role>
  queue=<name>
  generation_before=<n>
  generation_after=<n+1>
```

The audit line stays **lean** — `subject`, `role`, `queue`, and the two generations are everything an operator needs to reconstruct who-purged-what-when. Gemini's r1 review flagged that an earlier draft logged `is_dlq` / `dlq_sources` too; computing those would force every purge to pay the catalog reverse-scan cost (§3.2), turning an O(1) generation bump into O(N). The DLQ relationship is observable separately via `AdminDescribeQueue` at audit-review time, so dragging it into the write path is not worth the cost. If incident-triage workflows turn out to need the DLQ context inline, a future PR can add a `--with-dlq-context` opt-in flag.

Two new Prometheus counters on the existing `monitoring.SQSMetrics` registry:

```
elastickv_sqs_admin_purge_queue_total{queue, outcome}
  outcome ∈ {"ok", "forbidden", "not_leader", "not_found", "purge_in_progress", "internal_error"}

elastickv_sqs_admin_peek_queue_total{queue, outcome}
  outcome ∈ {"ok", "forbidden", "not_leader", "not_found", "internal_error"}
```

The label cardinality budget is the same `sqsMaxTrackedQueues = 512` cap that already governs the depth gauges — the same `admitForCounterBudget` helper from PR #743 r3 handles the collapse to `_other`. Peek has no `purge_in_progress` outcome (no rate limit on peek itself; the per-queue read budget gates it at the request layer and a denial there returns 429 with `Retry-After` from the throttle code, surfacing in the existing `Throttling` outcome on the read-side counter).

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

1. **Unit — Purge:** `TestAdminPurgeQueue_*` in `adapter/sqs_admin_test.go`:
   - happy path bumps the generation
   - read-only principal → `ErrAdminForbidden`
   - follower → `ErrAdminNotLeader`
   - missing queue → `ErrAdminSQSNotFound`
   - second purge within 60 s → `ErrAdminSQSPurgeInProgress`
   - empty name → `ErrAdminSQSValidation`
2. **Unit — Peek:** `TestAdminPeekQueue_*` in `adapter/sqs_admin_test.go`:
   - happy path returns visible messages in `visible_at` order
   - peek does NOT change receive count (verify via subsequent Describe / ReceiveMessage)
   - peek does NOT issue receipt handles (verify the returned struct has no handle field; verify no receipt-handle record was committed)
   - in-flight messages (visibility timer not expired) are NOT returned
   - delayed messages (DelaySeconds > 0, visible_at > now) are NOT returned
   - Limit clamping: 0 → default 20; 500 → clamped to 100
   - Cursor round-trip: peek with empty cursor, then peek with returned cursor → second page non-overlapping with first
   - Cursor invalid (truncated base64, wrong version) → `ErrAdminSQSValidation`
   - BodyMaxBytes truncation: long body → `BodyTruncated=true`, `BodyOriginalSize` correct, returned `Body` length == `BodyMaxBytes`
   - FIFO queue → `GroupID` and `DeduplicationID` populated; standard queue → both empty
   - read-only principal → succeeds (peek is a read)
   - nil principal → `ErrAdminForbidden`
   - follower → `ErrAdminNotLeader`
   - missing queue → `ErrAdminSQSNotFound`
3. **Unit — DLQ awareness:** `TestAdminQueueSummary_IsDLQ_*` in `adapter/sqs_admin_test.go`:
   - queue with no inbound RedrivePolicy → `IsDLQ=false, DLQSources=nil`
   - queue referenced by one source → `IsDLQ=true, DLQSources=["source-a"]`
   - queue referenced by two sources → sorted slice
   - queue referenced by itself (paranoid edge) → `IsDLQ=true, DLQSources=["self"]`
4. **Integration:** `TestSqsHandler_Messages_*` in `internal/admin/sqs_handler_test.go`:
   - `GET /admin/api/v1/sqs/queues/orders/messages?limit=5` returns paginated JSON; `next_cursor` round-trips on a second call
   - `DELETE /admin/api/v1/sqs/queues/orders/messages` against a single-node fixture → 204, generation incremented
   - 429 with `Retry-After` header on the immediate second purge
   - peek + purge on same queue: peek shows N rows → purge → peek returns 0 rows
   - bridge bubbles `ErrAdminForbidden` through to 403 (peek under nil principal; purge under read-only principal)
   - path-segment validation: `/messages/` (trailing slash), `/messages/foo` (sub-resource), `/Messages` (case mismatch) all return 404
5. **Frontend:** Playwright / RTL test pinning:
   - Messages tab renders rows in `visible_at` order with the correct columns
   - row click opens detail modal with full body
   - Next / Previous cursor flow lands on the right pages
   - page-size selector clamps to {20, 50, 100}
   - Purge button label switches between "Purge messages" and "Purge DLQ" based on `is_dlq`
   - Purge confirmation modal requires the exact name (case-sensitive, trimmed)
   - Purge 429 keeps modal open and shows countdown
   - Peek after purge shows empty state (zero rows)
6. **Jepsen:** out of scope. Purge has been Jepsen-covered since Phase 2 via the SigV4 path; the admin RPC shares the same `tryPurgeQueueOnce` and inherits the coverage. Peek is a read-only operation against state the Jepsen visibility-index workload already exercises; no new Jepsen workload is warranted.

---

## 7. Rollout Plan

| Phase | Content                                                                                                                                       |
|-------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| 1     | This proposal doc lands (one PR). Operators have time to flag concerns.                                                                       |
| 2     | Backend: `AdminPurgeQueue` + sentinel errors + `IsDLQ` / `DLQSources` on `AdminQueueSummary` + tests.                                          |
| 3     | Backend: `AdminPeekQueue` + `AdminPeekedMessage` + cursor codec + tests.                                                                      |
| 4     | HTTP handlers in `internal/admin/sqs_handler.go` (peek GET + purge DELETE + path-segment matcher) + bridge in `main_admin.go` + integration tests. |
| 5     | SPA: Messages tab + detail modal + Purge button + DLQ chips + API client wiring + frontend tests.                                              |
| 6     | Doc rename `_proposed_` → `_implemented_`.                                                                                                    |

Phases 2 / 3 / 4 / 5 each ship as separate PRs so they review small. Purge (phase 2) lands first because it is the simpler write — peek (phase 3) builds on cursor / pagination infrastructure that is wholly new. Phase 5 lands the user-visible feature.

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

The proposal covers the full triage-then-clear DLQ workflow:

- **`AdminPeekQueue`** — non-destructive sample of currently-visible messages so operators can see what's actually in a DLQ before deciding what to do with it. Pure read, no receive-count bump, no visibility-timer change, no receipt handles minted. Cursor-paginated; leader-only to keep the visibility index snapshot consistent.
- **`AdminPurgeQueue`** — wraps the existing `purgeQueueWithRetry` in the same shape `AdminDeleteQueue` already uses for delete. Inherits the SigV4 path's 60-second meta-stored rate limit so the two surfaces stay in lockstep.
- **`AdminQueueSummary.IsDLQ` / `DLQSources`** — reverse-scan the catalog (prefix scan over `SqsQueueMetaPrefix`, one storage round-trip) so the SPA can frame the Messages tab and the Purge confirmation appropriately. Reverse-index deferred until profiling shows it.

The user-visible feature is a Messages tab + Purge button on the queue detail page. Each backend primitive is small (Peek is bounded-page reads; Purge is one OCC commit) and they reuse the existing admin auth/audit/forwarding pipeline, so the design surface is intentionally small relative to the operator-facing value.
