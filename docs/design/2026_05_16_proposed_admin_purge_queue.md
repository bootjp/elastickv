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
8. **Read-only role can peek but not purge.** Peek is gated by a **live `RoleStore` re-check** (not just the session-auth gate that List / Describe currently use), introduced as a new `principalForReadSensitive` helper alongside the existing `principalForWrite`. Purge stays gated by `principalForWrite` (live-role re-check), matching `AdminDeleteQueue` exactly. Codex r9 P1 flagged the security gap in the earlier draft: peek exposes full message bodies / attributes (not just metadata like List / Describe), so a session JWT that was revoked or whose role was downgraded after login could still read DLQ payloads via peek until the token's natural 1-hour TTL elapsed. The new `principalForReadSensitive` helper performs the same revocation check `principalForWrite` does, but classifies the call as a read in the audit pipeline — keeping the audit shape parallel to List / Describe while closing the confidentiality gap. List / Describe themselves remain on the session-auth-only gate because their output is metadata that is already shown on the SPA's queue list page; the divergence is intentional and is documented at the call site so a future reviewer does not "fix" the inconsistency by downgrading peek's gate. Claude r2 caught the earlier draft that implied a non-existent `principalForRead` helper; this paragraph spells out the actual gate with the security-class distinction.

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
    // response size. Clamped to [256, sqsMaximumAllowedMaximumMessageSize]
    // (= 256 KiB, matching AWS SQS's hard cap on stored message size
    // — adapter/sqs_catalog.go:38). 0 means "use default (4096)".
    // The full body is always retained on the server; only the wire
    // representation is truncated.
    //
    // The cap matches the maximum-possible stored body so the detail
    // modal can pass BodyMaxBytes=262144 and be guaranteed to see the
    // entire payload — Codex r3 flagged that an earlier 64-KiB cap
    // broke the triage guarantee for messages between 64 KiB and
    // 256 KiB (the modal would still show a truncated body and the
    // operator could purge without seeing what they were dropping).
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

**Implementation.** Peek opens a read transaction at the leader's next read timestamp (`nextTxnReadTS`) and walks the visibility index for the queue. There are two visibility keyspaces, mirroring how `receiveMessage` (`adapter/sqs_messages.go`) dispatches:

- Standard / non-partitioned: `!sqs|msg|vis|<queue>|<gen>|<visible_at>|<msg_id>`
- Partitioned FIFO (`PartitionCount > 1`): `!sqs|msg|vis|p|<queue>|<partition>|<gen>|<visible_at>|<msg_id>`

For non-partitioned queues the walk is a single prefix scan over the first keyspace. For partitioned FIFO queues peek does **sequential partition scanning**: scan partition 0 to exhaustion (or until `Limit` is reached), then continue with partition 1, etc. The choice of sequential-rather-than-fanout matches `receiveMessage`'s existing dispatch and makes the cursor format trivial. A per-partition fanout would force the cursor to carry one offset per partition — Claude r2 flagged this as a concrete pagination concern and sequential scanning is the simpler resolution.

**Cursor format.** The cursor is a versioned JSON envelope, base64url-encoded for URL safety:

```go
type peekCursor struct {
    V          int    `json:"v"`           // schema version, currently 1
    Generation uint64 `json:"gen"`         // queue generation at scan start
    Partition  uint32 `json:"p,omitempty"` // current partition (0 for non-partitioned)
    LastKey    []byte `json:"k"`           // last scanned visibility-index key
}
```

Single-partition queues encode `Partition=0` and walk the non-partitioned keyspace. The cursor is hard-capped at **512 bytes** after encoding; any client-supplied cursor exceeding that returns `ErrAdminSQSValidation`. Same cap rejects pathologically large or crafted-by-hand inputs.

The walk:

1. Decodes the cursor (or starts at `Partition=0, LastKey=nil, Generation=current` if empty).
2. If queue meta's current generation differs from `cursor.Generation`, returns `ErrAdminSQSValidation` (`"peek cursor is from a prior generation; restart from the front"`). The SPA surfaces this as an automatic "Refresh" on the front of the cursor flow.
3. Issues a bounded `ScanAt` over the appropriate visibility-index prefix up to `Limit - len(rows)` keys. For each key fetches the message record via `GetAt` (one round-trip per row — the visibility index stores only the (visible_at, msg_id) pair, not the body).
4. When a partition is exhausted and `len(rows) < Limit` and the queue is partitioned, advances `cursor.Partition++` and continues the scan from the start of the next partition's prefix.
5. Truncates bodies to `BodyMaxBytes`, records `BodyTruncated` / `BodyOriginalSize` for the SPA to render "showing first 4 kB of 12 kB" labels.
6. Builds the next cursor from the last scanned (`partition`, `key`) pair; returns it as the opaque base64url string for the SPA. Empty when the final partition was exhausted.

Cost is `O(Limit)` round-trips against Pebble at peek time — tiny for the bounded result sets the SPA uses. The bound on `Limit` (max 100) prevents an operator script from accidentally issuing million-row peeks against the leader.

**Throttle.** Peek consults a **distinct per-queue admin-peek bucket**, *not* the per-queue `ReceiveMessage` budget. An earlier draft of this design merged the two; Claude r2 flagged that an operator paginating through a 10k-message DLQ could exhaust the budget that real consumers depend on. The separate admin-peek bucket defaults to a lower steady-rate (`adminPeekRPS = 5`, `adminPeekBurst = 20`) so a pagination loop cannot starve consumers.

**Bucket key format.** The existing `bucketStore` (`adapter/sqs_throttle.go`) keys on a struct `bucketKey{queue, action, incarnation}`, not a string. The admin-peek bucket therefore uses `bucketStore.charge()` directly with `action = bucketActionAdminPeek` and the queue's current incarnation, exactly like the `SendMessage` / `ReceiveMessage` paths do. Claude r4 flagged an earlier draft that described the bucket as a free-standing string-keyed map; that would have required parallel rate-limiter infrastructure and would not have been swept by `invalidateQueue()` on queue re-creation. The `bucketStore.charge(adminPeekThrottle, queueName, bucketActionAdminPeek, meta.Incarnation, 1)` form participates in the existing incarnation reset machinery automatically.

**`resolveActionConfig` must learn the new action explicitly.** `resolveActionConfig` (`adapter/sqs_throttle.go:484`) currently switches on `bucketActionSend` / `bucketActionReceive` and falls through to a `("*", DefaultCapacity, DefaultRefillPerSecond)` branch for any unknown action. If admin-peek silently joined the `"*"` branch, the bucket key `bucketKey{queue, "*", incarnation}` would alias the same queue's `ReceiveMessage`-via-Default key — every interleaved peek / receive call would force `bucketStore.loadOrInit` to detect a `(capacity, refill)` mismatch and `CompareAndDelete + LoadOrStore` the bucket. The net effect: each caller perpetually wipes the other's state, both observed rates climb past their configured caps, and the new admin-peek bucket effectively does nothing. Claude r5 caught this exact aliasing.

The implementation adds an explicit case to `resolveActionConfig` keyed on a new constant `bucketActionAdminPeek = "admin_peek"`, returning `(bucketActionAdminPeek, cfg.AdminPeekCapacity, cfg.AdminPeekRefillPerSecond)` so the bucket key stays distinct from `"*"`. The constant is declared in `sqs_throttle.go` alongside the existing `bucketActionSend` / `bucketActionReceive` so the bucket action enumeration stays in one place and a future grep for `bucketAction` finds the new value. The `sqsAdminPeekThrottleConfig` struct gains `AdminPeekCapacity` / `AdminPeekRefillPerSecond` fields seeded from the `adminPeekRPS=5` / `adminPeekBurst=20` defaults at server boot.

Exhaustion returns `&adminPeekThrottledError{retryAfter: time.Duration}` from the bucket, which the admin wrapper converts into a 429-mapped admin sentinel parallel to `ErrAdminSQSPurgeInProgress` (call it `ErrAdminSQSPeekThrottled`); the HTTP handler maps it to `429 Too Many Requests` with `Retry-After` and the `throttled` metric outcome (§3.6).

**Why leader-only.** The visibility index is updated by the leader as part of every `SendMessage` / `ReceiveMessage` / `DeleteMessage` commit. A follower read could see an index entry whose underlying message record was already deleted (race between the leader's apply and the follower's lease-read fence). Peek is rare and bursty rather than steady-state, so the lease-read fast path's win doesn't apply; running it on the leader keeps the snapshot internally consistent.

### 3.2 Backend: AdminPurgeQueue RPC

```go
// AdminPurgeResult is the success return of AdminPurgeQueue. The
// generation values are captured from the committed OCC round
// (tryPurgeQueueOnce knows both lastGen and lastGen+1), so the audit
// log records the value that actually landed — not an extra Pebble
// read before/after that could race a concurrent purge.
type AdminPurgeResult struct {
    GenerationBefore uint64
    GenerationAfter  uint64
}

// PurgeInProgressError is the typed error returned when the 60-second
// rate-limit is active. RetryAfter carries the wall-clock duration the
// caller should wait before retrying; the HTTP handler renders it as
// both the Retry-After response header and the retry_after_seconds
// JSON field. Implements `error` so existing errors.Is checks against
// the ErrAdminSQSPurgeInProgress sentinel still work via wrapping:
// `errors.Is(err, ErrAdminSQSPurgeInProgress)` returns true on any
// PurgeInProgressError. The struct form lets the handler extract the
// duration without parsing strings.
type PurgeInProgressError struct {
    RetryAfter time.Duration
}

func (e *PurgeInProgressError) Error() string { ... }
func (e *PurgeInProgressError) Is(target error) bool {
    return target == ErrAdminSQSPurgeInProgress
}

// AdminPurgeQueue is the SigV4-bypass counterpart to purgeQueue.
// Bumps the queue's generation so every message under the old
// generation becomes unreachable, leaving the meta record (name,
// ARN, RedrivePolicy, tags, attributes) in place. The reaper
// eventually deletes the orphaned message keys via the existing
// tombstone path.
//
// Returns the captured generation pair so the admin handler's audit
// line can record the value that actually landed (a separate
// loadQueueMetaAt call would race a concurrent purge in the 60-second
// window).
//
// Sentinel errors:
//   - ErrAdminForbidden          — read-only principal
//   - ErrAdminNotLeader          — follower
//   - ErrAdminSQSNotFound        — queue absent
//   - *PurgeInProgressError      — last purge < 60 s ago; errors.Is matches
//                                  the sentinel ErrAdminSQSPurgeInProgress.
//                                  RetryAfter field carries the duration.
//   - ErrAdminSQSValidation      — empty / malformed name
func (s *SQSServer) AdminPurgeQueue(
    ctx context.Context,
    principal AdminPrincipal,
    name string,
) (AdminPurgeResult, error)
```

**Implementation.** `purgeQueueWithRetry`'s signature changes from `error` to `(oldGen, newGen uint64, err error)`. The change is contained: there is **one** existing caller, `purgeQueue` (the SigV4 HTTP handler at `adapter/sqs_purge.go:22`), and that caller currently ignores the success values — it can simply discard the new tuple's first two elements. `tryPurgeQueueOnce` already knows `lastGen` and `lastGen + 1` from the OCC commit; threading them out is a one-line change. Claude r2 flagged that an extra `loadQueueMetaAt` call to fetch generations races a concurrent purge in the 60-second window; returning them from the committed OCC round is the only race-free choice.

The 60-second rate-limit lives on the meta record (`tryPurgeQueueOnce` reads it) so it survives leader failover and is enforced uniformly whether the call came from the SigV4 path or the admin path.

**Caller audit (semantic-change rule).** `purgeQueueWithRetry`'s signature change has exactly one external caller (`purgeQueue` in `sqs_purge.go`); the implementation PR's grep + audit will confirm the only call site is updated to discard the generation pair. `tryPurgeQueueOnce` is internal to the same file; same audit applies.

**RetryAfter propagation (typed-error path, no second meta read).** Claude r3 + r4 flagged that the duration must travel **inside the error value itself** — not be recomputed by the admin wrapper via a second `loadQueueMetaAt` call, which would race a concurrent purge that resets `LastPurgedAtMillis` in the 60-second window.

The implementation:

1. `tryPurgeQueueOnce` already has both `meta.LastPurgedAtMillis` and `now` inside the OCC read at the rate-limit rejection point (`adapter/sqs_purge.go:63–76`). At rejection it computes the remaining duration as **`time.Duration(sqsPurgeRateLimitMillis - (now - meta.LastPurgedAtMillis)) * time.Millisecond`** (`sqsPurgeRateLimitMillis` is `60_000` from `adapter/sqs_catalog.go:47` — an int64 in milliseconds; the difference is also int64 in milliseconds, so the explicit `* time.Millisecond` is the unit-correct conversion to `time.Duration`. Claude r5 caught an earlier draft using a `*ms` pseudo-unit suffix that an implementer could easily mis-read as `* time.Millisecond` applied directly to the already-millisecond constant, which would produce a 60_000× too-large duration). It returns the typed internal error `&purgeRateLimitedError{remaining: <that duration>}`.
2. `AdminPurgeQueue` uses `errors.As(err, &purgeRateLimitedErr)` to extract the duration directly from the typed error and returns `&PurgeInProgressError{RetryAfter: purgeRateLimitedErr.remaining}`. No second meta read is needed.
3. **The SigV4 wire response shape is preserved only if `writeSQSErrorFromErr` learns the new error type.** `writeSQSErrorFromErr` (`adapter/sqs_catalog.go:228`) pattern-matches only on `*sqsAPIError` today; if `tryPurgeQueueOnce` stops emitting an `*sqsAPIError` for the rate-limit case, the SigV4 `purgeQueue` handler's `writeSQSErrorFromErr(w, err)` call falls through to the generic 500 branch — silently regressing the wire response from `400 PurgeQueueInProgress` to `500 InternalFailure`. Phase 2 therefore adds a new `errors.As(err, &purgeErr *purgeRateLimitedError)` branch to `writeSQSErrorFromErr` that renders `sqsErrPurgeInProgress` with the duration embedded in the message string (matching the format the previous `newSQSAPIError(...)` call produced character-for-character). Claude r5 flagged this as Phase-2-blocking; the doc now spells it out so the Phase 2 implementation PR includes the `writeSQSErrorFromErr` branch alongside the `purgeQueueWithRetry` signature change.

This is the only race-free path: the rate-limit decision and the duration that describes it are derived from the **same** `LastPurgedAtMillis` value in a single OCC read. The phrase "from the meta record" in earlier drafts was ambiguous; the doc now commits explicitly to the typed-error path.

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

`AdminDescribeQueue` populates `IsDLQ` / `DLQSources` by reverse-scanning the catalog. Implementation: a **paginated prefix scan** over the queue-meta prefix (`SqsQueueMetaPrefix`), reusing the exact loop shape from `scanQueueNamesAt` (`adapter/sqs_catalog.go:1234`). Each iteration calls `ScanAt(..., sqsQueueScanPageLimit, ...)` (1024 records per page) and advances the cursor via `nextScanCursorAfter(lastKey)` until either the scan returns an empty page, fewer than `sqsQueueScanPageLimit` rows, or the cursor passes the prefix end. The handler unmarshals each record, runs `parseRedrivePolicy` (already in `adapter/sqs_redrive.go:43`) against the queue's `RedrivePolicy` attribute, and matches the resolved DLQ target against `name`. The pagination loop is mandatory for correctness: a single `ScanAt` would cap at `sqsQueueScanPageLimit = 1024` and silently miss DLQ source links for any deployment with >1024 queues — Codex r4 P1 flagged that the earlier wording ("a single `ScanAt` … returns all queue meta records in one storage round-trip") was wrong in exactly that scenario, mislabeling the purge UI for the queues whose sources spilled to a later page. Cost is **`ceil(N / 1024)` Pebble prefix-scan round-trips + O(N) unmarshal-and-string-compare in memory** — same shape `scanQueueNames` already pays for the admin list endpoint, so the worst-case load is bounded by the existing per-request envelope.

A future optimization maintains a reverse-index (`!sqs|catalog|dlq_source|<dlq_name>|<source_name>`) updated transactionally by `CreateQueue` / `SetQueueAttributes` / `DeleteQueue`, reducing Describe-time cost to O(direct sources). Out of scope for this PR; the reverse-scan is correct and the prefix-scan formulation is fast enough that the reverse-index only pays for itself in deployments with many thousands of queues — Gemini's r1 review on this proposal flagged the scan cost, and the reverse-index is the documented next step if production profiling shows the Describe path is hot.

### 3.4 Backend: HTTP endpoints

| Method   | Path                                                                | Purpose                                |
|----------|---------------------------------------------------------------------|----------------------------------------|
| `DELETE` | `/admin/api/v1/sqs/queues/{name}` *(existing)*                      | Delete the queue itself                |
| `GET`    | `/admin/api/v1/sqs/queues/{name}/messages?limit=N&cursor=C` *(new)* | Peek a sample of visible messages      |
| `DELETE` | `/admin/api/v1/sqs/queues/{name}/messages` *(new)*                  | Purge all messages, keep meta intact   |

Method-based dispatch on the same `/messages` sub-resource: `GET` → peek, `DELETE` → purge.

**ServeHTTP restructure (existing dispatch must change).** The current `ServeHTTP` in `sqs_handler.go` extracts everything after `/queues/` and passes it to `handleDescribe` / `handleDelete` as the queue name. With sub-resources, `handleDescribe("foo/messages")` would happily try to describe a queue named `"foo/messages"`. The implementation PR must restructure dispatch into three explicit levels:

```text
/admin/api/v1/sqs/queues            → handleList (existing)
/admin/api/v1/sqs/queues/{name}     → handleDescribe / handleDelete (existing handlers,
                                       but receive bare {name} after restructure)
/admin/api/v1/sqs/queues/{name}/messages → handlePeek (GET) / handlePurge (DELETE) (new)
```

**Routing is a single ordered procedure** — the prior draft described two different mechanisms in two paragraphs and contradicted itself; the doc now commits to one. Claude r6 caught the inconsistency.

1. **Read the escaped path.** Use `r.URL.EscapedPath()`, NOT `r.URL.Path`. The latter is the percent-decoded form: Go has already turned `%2F` into `/` in the queue-name position, so a check on `r.URL.Path` cannot distinguish `…/queues/%2F/messages` from `…/queues//messages`. Codex r6 P1 flagged that working from `r.URL.Path` reopens the same confused-deputy vector even after the pre-`path.Clean` check is added. The router reads from `EscapedPath()` exactly.
2. **Dispatch the collection root before any splitting.** If the escaped path equals `/admin/api/v1/sqs/queues` (with or without trailing slash — i.e. equals the prefix exactly, or equals the prefix + `"/"`), the request is a list call; the handler returns immediately into `handleList`. Codex r9 P2 flagged that the earlier draft tried to detect this case via `len(segments) == 0` after trimming the prefix, but `strings.TrimPrefix` returns the input unchanged when the prefix does not match — and `strings.Split("", "/")` returns `[""]`, a one-element slice, not the empty slice — so the `len(segments) == 0` test never fired and a bare collection-root request would have flowed through to the queue-name validator and been rejected as "empty queue name segment". The explicit pre-split branch sidesteps the entire `Split` / `TrimPrefix` corner case.
3. **Trim the route prefix** (`/admin/api/v1/sqs/queues/`) — the trailing slash is now part of the prefix so a match guarantees there is at least one further path segment — and split the remainder on `/`. The result is a slice of **raw** (still-escaped) segments. (A failed trim — i.e. the path neither equals the collection root from step 2 nor starts with the prefix-with-slash — falls through to the admin router's default 404 handler; the SQS sub-router never sees it.)
4. **Validate the queue-name segment AND every other segment.** Reject with `400 Bad Request` + `{"code":"ValidationError","message":"empty queue name segment"}` when ANY of these hold:
   - `segments[0] == ""` — the raw URL had `…/queues//…` (the empty string between two consecutive slashes ends up as an empty segment after split).
   - `strings.ContainsRune(segments[0], '%')` — any percent sign in the queue-name segment is illegal regardless of what it would decode to. AWS queue names match `sqsQueueNamePattern` (`^[a-zA-Z0-9_-]{1,80}(\.fifo)?$`, `adapter/sqs_catalog.go:65`); none of those characters need percent-encoding, so a queue-name segment containing `%` could only be an attempt to bypass the pre-`path.Clean` empty-segment check via a single-decode (`%2F`), double-decode (`%252F`), or arbitrary-depth-nested decode (`%25252F`, …) round-trip. Rejecting all `%`-containing segments at the source forecloses the entire decoding-collision class without iterating `url.PathUnescape` and without enumerating the encoded variants. Codex r6 P1 round-2 flagged that the previous "single `PathUnescape` decodes to empty" check would have missed `%252F` (one decode yields the literal string `%2F`, not the empty string); the contains-`%` rule sidesteps the bypass entirely.
   - **Any segment equals `"."` or `".."`** — dot-segment traversal. `path.Clean` collapses `..` against the preceding segment, so an input like `/admin/api/v1/sqs/queues/orders/../messages` would pass an "only-validate-segments[0]" check (`segments[0] == "orders"`, a legal name), then `path.Clean` rewrites the trimmed path to `/messages` and step 6 dispatches against a queue literally named `messages` — a wrong-queue operation reachable through a single crafted URL. Codex r7 P1 flagged this as the next confused-deputy vector after the `%`-decoding class was closed. The fix is structural: ban `.` and `..` in **every** segment (queue name and sub-resource alike) at step 4, before `path.Clean` ever runs. AWS queue names cannot equal `.` or `..` anyway (the legal-character set excludes both), and the only sub-resource segment we recognise is the literal string `messages`, so the prohibition costs nothing for legitimate traffic.
5. **Apply `path.Clean` to the trimmed path** for everything downstream of step 4 (trailing slash normalisation only — every `.` / `..` has already been rejected at step 4, so `path.Clean` no longer has rewriting work to do beyond slash collapse). With the queue-name segment AND every other segment validated, `path.Clean` is safe — none of its rewrites can move an empty segment or a synthesised segment into the queue-name position.
6. **Decode the sub-resource segments.** Re-split the cleaned path; the first post-`queues/` segment is the queue name (decoded via `url.PathUnescape`), the rest (zero or more segments) is the sub-resource path.

**What the four canonical bad inputs do:**

| Input                                                              | Outcome                                                                       |
|--------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `/admin/api/v1/sqs/queues//messages`                               | step 4 rejects (`segments[0] == ""`) → 400 ValidationError                    |
| `/admin/api/v1/sqs/queues/%2F/messages`                            | step 4 rejects (contains `%`) → 400 ValidationError                           |
| `/admin/api/v1/sqs/queues/%252F/messages` (double-encode)          | step 4 rejects (contains `%`) → 400 ValidationError                           |
| `/admin/api/v1/sqs/queues/orders/../messages` (dot-segment escape) | step 4 rejects (segment equals `".."`) → 400 ValidationError                  |
| `/admin/api/v1/sqs/queues/./orders/messages` (leading `.`)         | step 4 rejects (segment equals `"."`) → 400 ValidationError                   |
| `/admin/api/v1/sqs/queues/orders/messages/` (trail slash)          | step 5 normalises via `path.Clean` to `/orders/messages` → 200 (GET peek) or 204 (DELETE purge) |

**What the four canonical good inputs do:**

| Input                                                  | Outcome                                                              |
|--------------------------------------------------------|----------------------------------------------------------------------|
| `/admin/api/v1/sqs/queues`                             | step 2 dispatches the bare collection root → `handleList`            |
| `/admin/api/v1/sqs/queues/orders`                      | `handleDescribe` / `handleDelete` on queue `orders`                   |
| `/admin/api/v1/sqs/queues/orders/messages`             | `handlePeek` / `handlePurge` on queue `orders`                       |
| `/admin/api/v1/sqs/queues/messages`                    | **valid** — `handleDescribe` / `handleDelete` on a queue literally named `messages` (legal AWS queue name; sub-resource path is empty so this is not `/queues/<name>/messages`) |

**Trailing slash on a valid path is accepted.** `/admin/api/v1/sqs/queues/orders/messages/` Cleans to `/admin/api/v1/sqs/queues/orders/messages` in step 5 and succeeds — Claude r4 originally caught the earlier draft's test-plan contradiction here, and the policy stays: accept trailing slashes via `path.Clean` normalisation rather than adding a `strings.HasSuffix(path, "/")` pre-check that would diverge from the HTTP `mux` family's convention.

This restructure is itself a small change — `handleDescribe` and `handleDelete` are unchanged internally, they just receive a bare name instead of a path tail.

**Bridge interface.** `QueuesSource` (defined in `internal/admin/sqs_handler.go:62-65`) gains two methods. The existing methods are named `AdminListQueues` / `AdminDescribeQueue` / `AdminDeleteQueue`; the new methods follow the same convention (Claude r4 flagged the earlier draft that showed shorter `List` / `Describe` / `Delete` names — those don't match the actual interface):

```go
type QueuesSource interface {
    // existing
    AdminListQueues(ctx context.Context) ([]string, error)
    AdminDescribeQueue(ctx context.Context, name string) (*QueueSummary, bool, error)
    AdminDeleteQueue(ctx context.Context, principal AuthPrincipal, name string) error
    // new
    AdminPeekQueue(ctx context.Context, principal AuthPrincipal, name string, opts AdminPeekMessageOptions) (AdminPeekResult, error)
    AdminPurgeQueue(ctx context.Context, principal AuthPrincipal, name string) (AdminPurgeResult, error)
}
```

`sqsQueuesBridge` in `main_admin.go` implements both new methods by translating between the admin-package types and `adapter.AdminPeek*` / `adapter.AdminPurge*`. The bridge wraps the adapter's 3-tuple return `(rows, nextCursor, err)` into a single `AdminPeekResult` struct so the `QueuesSource` interface stays consistent with the existing single-return shape of `AdminDescribeQueue` / `AdminDeleteQueue`:

```go
// AdminPeekResult is the admin-package projection of an
// AdminPeekQueue call. Mirrors the adapter's 3-tuple return
// (rows, nextCursor, error) but bundles the data into a single
// struct so QueuesSource's method signatures stay regular.
// Claude r6/r7 flagged that the earlier draft used the type
// name without defining it; this struct lives in the admin
// package alongside the existing QueueSummary / QueueCounters
// types.
type AdminPeekResult struct {
    Messages   []AdminPeekedMessage
    NextCursor string // empty when no further pages remain
}
```

`AdminPurgeResult` carries `GenerationBefore` / `GenerationAfter` for the audit line. The bridge inspects the returned error: a `*adapter.PurgeInProgressError` is rewrapped as `*admin.PurgeInProgressError{RetryAfter: e.RetryAfter}` so the admin package stays free of the adapter package's error types. Both struct errors implement `errors.Is(ErrQueuesPurgeInProgress)` so `writeQueuesError` can branch on the sentinel while the duration travels in the typed payload.

**Admin-package sentinel.** Two new sentinels in `internal/admin/queues_errors.go` (parallel to existing `ErrQueuesForbidden` / `ErrQueuesNotLeader` / `ErrQueuesNotFound` / `ErrQueuesValidation`):

```go
ErrQueuesPurgeInProgress = errors.New("admin: purge in progress")
// PurgeInProgressError is the typed counterpart that carries the
// remaining duration. errors.Is(err, ErrQueuesPurgeInProgress) returns
// true on any *PurgeInProgressError value.
type PurgeInProgressError struct{ RetryAfter time.Duration }
```

`writeQueuesError` gains a `429 Too Many Requests` branch with `Retry-After` header populated from `PurgeInProgressError.RetryAfter`. The JSON body matches the existing structured-error shape with an extra `retry_after_seconds` field.

**Peek-related path conventions.** Wire-side query parameters follow the existing admin API's snake_case convention: `limit`, `cursor`, `body_max_bytes` (not `bodyMaxBytes`). Claude r2 r1 flagged the inconsistency in an earlier draft. The frontend client's TypeScript field names (`bodyMaxBytes`, `nextCursor`) stay camelCase per JS convention; the client adapter does the case translation at the request boundary.

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

**Messages tab.** A new tab labelled "Messages" sits next to the existing "Overview" view. The tab header shows **"Showing N visible messages (M currently in-flight)"** where `N` = `peek.messages.length` (after applying any current cursor page) and `M` = `summary.counters.not_visible` — so an operator does not mistake an empty peek result on a busy queue for "the queue is empty". Claude r2 raised this; it is a Phase 5 UX detail but cheap to land at the same time as the rest of the tab. On mount the tab issues `GET /admin/api/v1/sqs/queues/{name}/messages?limit=20` and renders the result as a table:

| Column            | Source                                              | Rendering                                                                                                                          |
|-------------------|-----------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------|
| Message ID        | `message_id`                                        | first 8 chars in monospace; full ID in tooltip + copy-on-click                                                                     |
| Sent              | `sent_timestamp`                                    | relative ("2m ago") + absolute ISO in tooltip                                                                                      |
| Group ID          | `group_id` (FIFO only; column hidden for standard)  | as-is in monospace                                                                                                                 |
| Receive count     | `receive_count`                                     | plain integer; muted styling when 0                                                                                                |
| Body preview      | `body` (already truncated by backend)               | first 96 chars; "…" suffix when `body_truncated`. Row click opens detail modal.                                                    |
| Size              | `body_original_size`                                | human-readable ("1.4 kB") so operators can spot oversized messages                                                                 |

Below the table: a page-size selector (20 / 50 / 100), a Refresh button, and Next / Previous controls driven by the cursor. Detail modal shows full body + every attribute + the timestamps; a "Copy as JSON" button copies the row's full record to the clipboard for manual export.

**Copy as JSON payload schema.** The clipboard payload is the exact wire shape of a single `AdminPeekedMessage` entry (top-level keys: `message_id`, `body`, `body_truncated`, `body_original_size`, `sent_timestamp`, `receive_count`, `group_id`, `deduplication_id`, `attributes`) plus a wrapper `{"schema_version": 1, "queue": "<name>", "exported_at": "<ISO8601>", "message": { … }}`. The `schema_version` is what downstream tooling pins so a future change to the export format (e.g. multi-message JSONL bundle) does not silently break exporters. Operator workflows that pipe this into a recovery tool can rely on the schema not shifting under them.

**Full-body fetch in the detail modal.** `AdminPeekQueue` walks the visibility index from a cursor; it has no by-message-id targeted fetch.

**The list request itself fetches full bodies.** When the SPA issues the list-view peek it sets `body_max_bytes = sqsMaximumAllowedMaximumMessageSize` (= 262 144, the cap on SQS's stored body size) so every row arrives at full fidelity. Opening the detail modal then renders directly from the row already in memory — there is **no re-peek round-trip on modal open**. This eliminates the entire class of "the row might have disappeared between list and modal open" failure modes (concurrent purge, ReceiveMessage by another client, message becoming invisible because its visibility timer started, leader step-down, etc.). Codex r5 flagged the earlier draft's re-peek mechanism as not actually guaranteeing the row was still present.

Claude r4 raised the cost concern: a 100-row page with every message at 256 KiB stretches to a 25 MiB response. The SPA mitigates by:

1. **Defaulting page size to 20** (not 100), keeping the worst-case list response at 5 MiB — well under typical network and JSON-parse budgets.
2. **Tracking cumulative response size on the page-size selector.** When the operator selects 50 or 100, the SPA derives an estimate from the queue summary's `Counters.Visible × max(BodyOriginalSize from previous page, 4 KiB)` and shows a warning ("This page may exceed 25 MiB; messages > 256 KiB are not stored. Continue?") before issuing the larger request. Operators can dismiss and proceed if they understand the cost; the warning is informational, not blocking.
3. **Row body sizes are visible** in the list view (the `Size` column rendered from `body_original_size`) so an operator can scroll a 20-row page and decide whether to switch to size 50 / 100 or keep paginating.

The list-view full-body strategy trades initial fetch size for **modal-open consistency** — the operator sees the message exactly as it was at peek time, even if the queue churns underneath them. The trade is right for triage: the entire point of Peek is to capture a point-in-time view of what is currently in the DLQ, and a modal that fails to load the message because it has since been purged is worse than a slightly larger initial fetch.

A future PR can add a `?message_id=…` filter to `AdminPeekQueue` for targeted refresh-on-demand if production needs it (e.g., for a queue with many > 100 KiB messages where the operator wants to inspect one specific message without paying for the rest of the page). For now the full-page approach is simpler and consistency-correct.

Codex r3 flagged that an earlier 64-KiB cap on BodyMaxBytes broke the triage guarantee for messages between 64 KiB and 256 KiB; raising the cap to 262144 and committing to the eager-fetch model resolves the gap.

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

The audit line stays **lean** — `subject`, `role`, `queue`, and the two generations are everything an operator needs to reconstruct who-purged-what-when. Gemini's r1 review flagged that an earlier draft logged `is_dlq` / `dlq_sources` too; computing those would force every purge to pay the catalog reverse-scan cost (§3.3), turning an O(1) generation bump into O(N). The DLQ relationship is observable separately via `AdminDescribeQueue` at audit-review time, so dragging it into the write path is not worth the cost. If incident-triage workflows turn out to need the DLQ context inline, a future PR can add a `--with-dlq-context` opt-in flag.

**Sourcing the generation values.** `purgeQueueWithRetry` is modified to return `(oldGen, newGen uint64, err error)` so the audit log records the values from the **committed OCC round**, not a pre/post `loadQueueMetaAt` read that could race a concurrent purge in the 60-second window. `tryPurgeQueueOnce` already has both values inside the OCC transaction (`lastGen` and `lastGen + 1`); plumbing them out is one extra `return`. Claude r2 caught this — fetching the generations via extra Pebble reads would race the meta-stored rate-limit fields and could log values that never appeared as a single consistent state. The change is contained to `adapter/sqs_purge.go`; the only other caller (the SigV4 `purgeQueue` handler) discards the new return values via `_, _, err := ...`.

Two new Prometheus counters on the existing `monitoring.SQSMetrics` registry:

```
elastickv_sqs_admin_purge_queue_total{queue, outcome}
  outcome ∈ {"ok", "forbidden", "not_leader", "not_found", "purge_in_progress", "internal_error"}

elastickv_sqs_admin_peek_queue_total{queue, outcome}
  outcome ∈ {"ok", "forbidden", "not_leader", "not_found", "throttled", "internal_error"}
```

The label cardinality budget is the same `sqsMaxTrackedQueues = 512` cap that already governs the depth gauges — the same `admitForCounterBudget` helper from PR #743 r3 handles the collapse to `_other`.

Peek's `throttled` outcome covers exhaustion of the dedicated admin-peek bucket introduced in §3.1 (`adminPeekRPS = 5`, `adminPeekBurst = 20`). Codex r3 caught that an earlier draft of this section omitted the throttled outcome — implementers following the enum literally would have classified expected rate-limit rejections as `internal_error`, making the metric unable to distinguish backend faults from operator pagination loops. Peek has no `purge_in_progress` outcome (the 60-second rate-limit is purge-specific; peek is read-only and doesn't share that state).

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
   - `AdminPurgeResult.GenerationBefore` / `GenerationAfter` are the values from the **committed** OCC round (not the meta read pre/post): the test arranges a queue at generation N, runs `AdminPurgeQueue`, asserts the returned `GenerationBefore=N, GenerationAfter=N+1`, then runs a concurrent same-tick purge attempt that must hit the rate-limit (validating the values logged in the audit line are the ones that actually landed, not stale-read intermediates)
   - read-only principal → `ErrAdminForbidden`
   - follower → `ErrAdminNotLeader`
   - missing queue → `ErrAdminSQSNotFound`
   - second purge within 60 s → `*PurgeInProgressError{RetryAfter: d}`; `errors.Is(err, ErrAdminSQSPurgeInProgress)` returns true; `d` is in `(0, 60s]`
   - empty name → `ErrAdminSQSValidation`
   - SigV4 caller compatibility: `TestPurgeQueueSigV4_StillCompiles` exercises the SigV4 handler path through the modified `purgeQueueWithRetry` signature to prove the change does not regress the wire-level surface
2. **Unit — Peek:** `TestAdminPeekQueue_*` in `adapter/sqs_admin_test.go`:
   - happy path returns visible messages in `visible_at` order
   - peek does NOT change receive count (verify via subsequent Describe / ReceiveMessage)
   - peek does NOT issue receipt handles (verify the returned struct has no handle field; verify no receipt-handle record was committed)
   - in-flight messages (visibility timer not expired) are NOT returned
   - delayed messages (DelaySeconds > 0, visible_at > now) are NOT returned
   - Limit clamping: 0 → default 20; 500 → clamped to 100
   - Cursor round-trip: peek with empty cursor, then peek with returned cursor → second page non-overlapping with first
   - Cursor invalid (truncated base64, wrong version) → `ErrAdminSQSValidation`
   - Cursor too large (encoded length > 512 bytes) → `ErrAdminSQSValidation`
   - Cursor with stale generation (queue purged between calls) → `ErrAdminSQSValidation` with the "restart from the front" message; SPA refreshes from empty cursor
   - BodyMaxBytes truncation: long body → `BodyTruncated=true`, `BodyOriginalSize` correct, returned `Body` length == `BodyMaxBytes`
   - FIFO queue → `GroupID` and `DeduplicationID` populated; standard queue → both empty
   - **Partitioned FIFO pagination (`TestAdminPeekQueue_PartitionedFIFO_Pagination`):** queue with `PartitionCount=4` and N messages spread across partitions; cursor pagination eventually surfaces every message exactly once, in `(partition, visible_at)` order; cursor cleanly transitions from partition k to partition k+1 between pages
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
   - path-segment validation:
     - `/messages/foo` (undefined sub-resource), `/Messages` (case mismatch) → 404
     - `/messages/` (trailing slash) → 204 via `path.Clean` normalisation (pins the Claude r4 / r6 test-plan contradiction fix)
     - **`GET /admin/api/v1/sqs/queues//messages`** (omitted queue name, literal double slash) → 400 `{"code":"ValidationError","message":"empty queue name segment"}` (pins the Codex r5 / r6 routing pre-check)
     - **`DELETE /admin/api/v1/sqs/queues//messages`** (same) → 400 with the same body
     - **`GET /admin/api/v1/sqs/queues/%2F/messages`** (percent-encoded slash bypass) → 400 with the same body (pins the Codex r6 P1 `%2F` rejection — case-insensitive, including `%2f`)
     - **`GET /admin/api/v1/sqs/queues/%252F/messages`** (double-percent-encoded slash) → 400 with the same body (the `ContainsRune('%')` rule rejects ALL `%`-containing queue-name segments, so deeper nesting like `%25252F` is also caught without enumerating levels)
     - **`GET /admin/api/v1/sqs/queues/orders/../messages`** (dot-segment escape) → 400 with the same body (pins the Codex r7 P1: `path.Clean` would rewrite to `/messages` and dispatch on a queue literally named `messages` — the step 3 `..`-segment check forecloses this regardless of what `path.Clean` would do)
     - **`GET /admin/api/v1/sqs/queues/./orders/messages`** (leading single dot) → 400 with the same body
     - `GET /admin/api/v1/sqs/queues/messages` (no sub-resource, legal queue name "messages") → 200 Describe of the queue (legitimate name, NOT rejected — distinguishes "queue named messages" from "missing queue name")
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
- **`AdminQueueSummary.IsDLQ` / `DLQSources`** — reverse-scan the catalog with a **paginated prefix scan** over `SqsQueueMetaPrefix` (the same loop shape `scanQueueNames` uses today — `ceil(N / sqsQueueScanPageLimit)` round-trips, NOT a single page) so the SPA can frame the Messages tab and the Purge confirmation appropriately. Reverse-index deferred until profiling shows it. Codex r5 caught an earlier draft of this summary that said "one storage round-trip" — a regression to a single `ScanAt` would silently miss DLQ source links once the catalog exceeds 1024 queues; §3.3 has the full description.

The user-visible feature is a Messages tab + Purge button on the queue detail page. Each backend primitive is small (Peek is bounded-page reads; Purge is one OCC commit) and they reuse the existing admin auth/audit/forwarding pipeline, so the design surface is intentionally small relative to the operator-facing value.
