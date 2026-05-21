# Transactional secondary-commit idempotency

> **Status: proposed — not yet implemented.**
> Triggered by the 2026-05-21 Jepsen scheduled run
> [26198185540](https://github.com/bootjp/elastickv/actions/runs/26198185540)
> which surfaced a `:duplicate-elements` anomaly on the Redis list-append
> workload. This doc proposes a server-side dedup marker (and an optional
> client-side commit-status check) to make multi-shard txn commits
> exactly-once-applied at the FSM layer.

## Background

PR [#789](https://github.com/bootjp/elastickv/pull/789) (May 19) shipped the
NOTLEADER prefix for gRPC-wrapped leader errors, which stopped the Jepsen
Redis workers from crashing on `:prefix :rpc` exceptions. That fix
unmasked a deeper anomaly that had been silently classified as `:info`
(unknown outcome) under the old worker-crash regime: **`:duplicate-elements`**.

Jepsen's history.txt for the run shows:

```
1411  3  :invoke :txn  [[:r 16 nil] [:r 16 nil] [:append 14 230]]
1416  3  :ok     :txn  [[:r 16 [...]] [:r 16 [...]] [:append 14 230]]
1420  7  :ok     :txn  [..., [:r 14 [... 228 229 230 230]]]
```

Worker 3 sent the value `230` to key 14 exactly once. The very next read
(line 1420) shows two `230`s adjacent at the tail. The result.edn
analysis reports `:anomalies {:duplicate-elements [... :duplicates {230 2} ...]}`
across multiple ops.

This is a real consistency violation. Each Jepsen `:append k v` carries a
distinct, per-key monotonically-increasing value, so two `230`s in the read
must mean two distinct storage entries with that value — one logical
append applied twice.

## Problem

### Why a same-commit-ts replay does NOT produce a duplicate

elastickv's list-append mutations are content-addressed:

```go
// adapter/redis.go:3066-3071  (buildLPushOps / mirrored in RPush)
elems = append(elems, &kv.Elem[kv.OP]{
    Op:    kv.Put,
    Key:   listItemKey(key, seq),   // seq = current Tail + Len at attempt time
    Value: vCopy,
})
delta := store.MarshalListMetaDelta(...)
elems = append(elems, &kv.Elem[kv.OP]{
    Op:    kv.Put,
    Key:   store.ListMetaDeltaKey(key, commitTS, seqInTxn),  // commitTS in the key
    Value: delta,
})
```

Both the per-item key and the meta-delta key embed `commitTS` (directly or
via `seq` derived from a meta read pinned to `startTS`). Two applies with
the **same** `(startTS, commitTS)` therefore land on the **same** Pebble
keys — the second is a no-op overwrite, not a duplicate.

This is the same property TiKV relies on: writes to the Write CF at
`(user_key, commit_ts) → start_ts` are naturally idempotent because
`commit_ts` is part of the key.

### How a different-commit-ts replay DOES produce a duplicate

`adapter/redis.go:2824` wraps the entire `runTransaction` body in
`retryRedisWrite`:

```go
err := r.retryRedisWrite(dispatchCtx, func() error {
    startTS := r.txnStartTS()              // regenerated each iteration
    ...
    txn := &txnContext{... startTS: startTS, ...}
    for _, cmd := range queue {
        res, err := txn.apply(cmd)
    }
    if err := txn.validateReadSet(...); err != nil { return err }
    if err := txn.commit(); err != nil { return err }   // new commitTS via Clock().Next()
})
```

`retryRedisWrite` retries on `store.ErrWriteConflict` or `kv.ErrTxnLocked`
(see `adapter/redis_retry.go:43`). On retry the EXEC body re-runs from
scratch: fresh `startTS`, fresh meta read (which now reflects any earlier
commit), fresh `commitTS`, fresh `seq` for the list item, fresh meta-delta
key. Content-addressing protects against same-input replay; it does not
protect against this re-generation.

The duplicate shape:

1. Attempt 1: `startTS=T0`, reads `meta.Len = 229`, builds `listItemKey(14, 230)`
   and `ListMetaDeltaKey(14, T1, 0)` with `commitTS=T1`. Dispatch runs.
2. Some path inside dispatch lets Attempt 1 **actually commit at the primary
   shard** (key 14 now has `230` at idx 230, at commit_ts=T1) yet returns a
   `WriteConflict` / `TxnLocked` to the adapter. The retryRedisWrite
   classifier therefore retries.
3. Attempt 2: `startTS=T2 > T1`, reads `meta.Len = 230` (the just-committed
   entry from attempt 1 is now visible), builds `listItemKey(14, 231)` and
   `ListMetaDeltaKey(14, T3, 0)` with `commitTS=T3`. Dispatch commits.
4. End state: idx 230 has value `230` at T1, idx 231 has value `230` at T3.
   LRANGE returns `[... 229 230 230]`.

The specific path in (2) that lets the primary commit *and* return a
conflict has not been positively identified from the GH Actions log
(`tail -n 500 /tmp/elastickv-demo.log` truncates the window). Candidates:
partial multi-key commit in `applyCommitWithIdempotencyFallback`,
`commitSecondaryWithRetry` propagating a misclassified error, or a
LockResolver path. PR
[#795](https://github.com/bootjp/elastickv/pull/795) ("upload full demo
cluster log on Jepsen failure") was opened in parallel to obtain
forensic-quality logs from the next scheduled run; this doc does not
depend on that PR landing because the FIX is independent of *which* code
path produces the partial-commit — see "Why a server-side marker is the
right layer".

### Why the existing `applyCommitWithIdempotencyFallback` doesn't catch this

`kv/fsm.go:502` already has an idempotency fallback for the commit-phase
apply path:

```go
func applyCommitWithIdempotencyFallback(... applyStartTS, commitTS uint64) error {
    err := f.store.ApplyMutationsRaft(ctx, storeMuts, nil, applyStartTS, commitTS)
    if err == nil { return nil }
    if !errors.Is(err, store.ErrWriteConflict) { return errors.WithStack(err) }
    // Scan for a key already committed at >= commitTS; if found, retry
    // with commitTS as the conflict baseline.
    for _, mut := range uniq {
        latestTS, exists, lErr := f.store.LatestCommitTS(ctx, mut.Key)
        if exists && latestTS >= commitTS {
            return errors.WithStack(f.store.ApplyMutationsRaft(ctx, storeMuts, nil, commitTS, commitTS))
        }
    }
    return errors.WithStack(err)
}
```

This handles **same-commit_ts replay** of a single-key PUT correctly: the
second apply's WriteConflict is recognized as an idempotent retry and the
re-apply at `(commit_ts, commit_ts)` becomes a no-op MVCC overwrite.

It does *not* help here because Attempt 1's storage keys
(`listItemKey(14, 230)`, `ListMetaDeltaKey(14, T1, 0)`) and Attempt 2's
(`listItemKey(14, 231)`, `ListMetaDeltaKey(14, T3, 0)`) are **different
keys**. Neither attempt's apply sees a WriteConflict on the other's
storage key. The fallback can only catch identical-input replays; it
cannot catch logically-equivalent-but-recomputed retries.

## How TiKV solves this

TiKV's Percolator commit path is naturally idempotent because TiKV writes
are pure PUTs at content-addressed keys (Write CF `(user_key, commit_ts) →
start_ts`). Critically, **TiKV clients do not recompute the txn on a
transient error**: they call `CheckTxnStatus(primary_key, start_ts)` first
to learn the txn's actual fate. If the txn already committed, the client
returns success to the user without re-issuing any write. Three layers
combine:

1. **Content-addressed writes**: `(key, commit_ts)` PUT is idempotent at
   the FSM/MVCC layer. Replay with same `(key, commit_ts, value)` is a
   no-op.
2. **`CheckTxnStatus` before retry**: client reads the primary's Write CF
   for `start_ts` before deciding to retry. Eliminates blind-retry of an
   already-committed txn.
3. **Resolve-lock from readers**: a stale secondary lock seen at read
   time is resolved against the primary's commit record. Guarantees
   eventual commit of stranded secondaries without needing client retry.

elastickv has (1) (the `applyCommitWithIdempotencyFallback` fallback, plus
content-addressed list-delta keys) and (3) (`kv.LockResolver` at
`kv/lock_resolver.go`). It does **not** have (2): every retry path
(`runTransaction`'s `retryRedisWrite`, `listPushCore`'s `retryRedisWrite`,
`commitSecondaryWithRetry`'s loop) re-issues the write blindly without
first checking the primary's commit status. (1) protects against
identical-input replay but not against the regenerated-input replay that
`runTransaction`'s EXEC-body re-execution produces.

## Proposed design

Add a server-side **txn-applied marker** at FSM apply time, plus a kv-side
**check-then-retry** wrapper around the secondary commit path. The marker
is the primary fix; the check-then-retry is a performance optimisation.

### Why a server-side marker is the right layer

The duplicate cannot be deduped purely client-side because:

- The Redis client (Jepsen, Carmine, etc.) sends one `MULTI/EXEC` and
  expects one response. There is no client-supplied txn ID.
- The adapter could generate a UUID at EXEC entry, but to make `retryRedisWrite`
  dedup-safe, the UUID has to survive both:
  - regeneration of `startTS` / `commitTS` (purely server-side state), and
  - propagation through the gRPC `Forward` redirect when the local node
    is not the leader of all touched shards.
- A UUID generated at EXEC entry that travels through every internal
  retry and every Forward boundary IS the design. Storing it server-side
  in a small dedup table is the natural place to enforce uniqueness.

### Server-side marker (the fix)

New reserved key on each shard:

```
__txn_applied__<adapter_txn_id>  →  (commit_ts, applied_at_wall_time)
```

`adapter_txn_id` is a server-generated UUIDv7 (time-ordered for cheap
TTL-based GC), allocated in the adapter at EXEC entry (`runTransaction`
line 2820, before `retryRedisWrite`). A new field
`Request.AdapterTxnId [16]byte` propagates it through `pb.Request`. It
survives:

- `retryRedisWrite` retries: the UUID is captured outside the
  closure, so every iteration sees the same value.
- `commitSecondaryWithRetry` retries: the field is on the existing `req`
  object.
- Forward redirect: `Coordinate.buildRedirectRequests` already builds a
  `pb.Request`; just include the field.

FSM apply path (`kv/fsm.go:handleCommitRequest`,
`handleOnePhaseTxnRequest`):

1. **Before** building store mutations, read `__txn_applied__<id>` for the
   request's adapter_txn_id.
2. If present and the recorded commit_ts equals the request's commit_ts:
   no-op (return nil). The mutations would be a duplicate.
3. If present and the recorded commit_ts **differs**: return
   `ErrTxnAlreadyCommittedAtDifferentTS` (a new sentinel). This catches the
   bug shape — retry generated a NEW commit_ts after the original committed
   — and surfaces it as a hard error to the adapter, which can then return
   a deterministic outcome rather than producing a duplicate.
4. If absent: apply mutations normally **and** write
   `__txn_applied__<id> → (commit_ts, now)` in the **same Pebble batch**
   (`ApplyMutationsRaft`'s atomic guarantee). The marker is committed iff
   the mutations are committed.

### GC of the marker table

`__txn_applied__*` keys must survive long enough to catch the latest possible
retry. The retry envelope is bounded by:

- `redisDispatchTimeout` (handler-context timeout for one Redis command)
- `lockResolverInterval` (10 s) + `defaultTxnLockTTLms` (30 s) for lock-
  resolver triggered re-applies

A conservative TTL of `2 × (lockResolverInterval + defaultTxnLockTTLms)`
= 80 s covers both. Cleanup runs in `kv/compactor.go`'s existing periodic
sweep — add a new prefix scan that drops markers whose `applied_at` is
older than the TTL. Cost: ~40 bytes/txn × ~1000 txn/s × 80 s ≈ 3.2 MB
steady-state per shard. Negligible.

### Client-side commit-status check (optimisation)

This is the TiKV-style optimisation that *reduces* the marker's
load-bearing role from "absolute correctness" to "race-window guard". Not
required for correctness given the marker, but reduces wasted Raft traffic
on the retry-after-success case:

`commitSecondaryWithRetry` (`kv/sharded_coordinator.go:527`) checks the
primary's commit record (`txnCommitKey(primary_key, start_ts)`) before
deciding to retry:

```
commit_record = txnCommitTS(primary_key, start_ts)
match commit_record {
    Committed(ts) => {
        // Primary already committed. If our secondary's marker is
        // present, this is a true no-op — return success. Otherwise
        // retry the secondary commit (it really did fail).
        if secondary_marker_present(adapter_txn_id) { return Ok }
        else                                        { retry }
    }
    NotFound => { retry }   // primary not yet committed; safe to retry
    RolledBack => { return abort }
}
```

This is purely additive; if it is omitted, the FSM-side marker still
prevents the duplicate.

## Caller audit

The marker design changes the FSM apply semantics: a commit request whose
adapter_txn_id is already present becomes either a no-op (same commit_ts)
or an error (different commit_ts). Every caller of `handleCommitRequest`
/ `handleOnePhaseTxnRequest` (via `applyRequest`) is affected:

| Caller | Outcome with marker | Notes |
|---|---|---|
| `commitPrimaryTxn` first attempt | no marker yet → apply + write marker | normal path |
| `commitSecondaryTxns` per shard | no marker yet → apply + write marker | normal path |
| `commitSecondaryWithRetry` retry of same req | marker present, same commit_ts → no-op | the bug shape we are fixing |
| `runTransaction` retry-after-success (new commit_ts) | marker present, different commit_ts → `ErrTxnAlreadyCommittedAtDifferentTS` | adapter must translate this to a synthesized success result for the user-visible txn |
| `LockResolver.resolveExpiredLock` re-commit | same (start_ts, commit_ts) as original primary record → marker present, same commit_ts → no-op | safe |
| `applyTxnResolution` from lock cleanup | same | safe |

The adapter handles `ErrTxnAlreadyCommittedAtDifferentTS` by:

1. Reading the primary's `txnCommitKey(primary_key, start_ts)` to learn the
   ACTUAL commit_ts.
2. Reading back the user-visible state at that commit_ts (e.g.
   `resolveListMeta` at the actual `commit_ts`) to reconstruct what the
   client should see.
3. Returning success with the reconstructed result.

This is the same pattern TiKV uses when it discovers
"the txn already committed before the client knew about it" — `CheckTxnStatus`
result + read-after-commit-ts.

## Why not just remove `retryRedisWrite` from `runTransaction`?

Removing the EXEC-body retry would eliminate the trigger but trade
correctness for availability: every transient `WriteConflict` (which
happens under normal OCC contention) would surface to the client as a
hard error, requiring the client to retry. Jepsen/Redis clients typically
do not retry write-conflict errors and would interpret them as `:fail`
ops. Availability under contended workloads (BullMQ, list-append-heavy
patterns) would drop measurably.

Keeping the retry but making it idempotent at the FSM layer (the marker
design) preserves availability and adds correctness.

## Implementation milestones

### M1 — proto + adapter UUID plumbing

- Add `bytes AdapterTxnId = N;` to `pb.Request` (proto/service.proto)
- `runTransaction` (`adapter/redis.go:2820`): allocate UUIDv7 once,
  capture in closure
- `listPushCore` / `txn.commit()` / single-command write helpers: thread
  UUID through `kv.OperationGroup`
- `Coordinate.buildRedirectRequests`: propagate UUID through Forward
- `commitSecondaryTxns`: include UUID in each per-shard `req`
- Unit tests: assert UUID is stable across retryRedisWrite iterations

### M2 — FSM marker

- New reserved-key prefix `__txn_applied__<uuid>` in
  `internal/storage/keys` (or wherever the prefix table lives)
- `handleCommitRequest` / `handleOnePhaseTxnRequest`: marker check
  before `buildCommitStoreMutations`
- `buildCommitStoreMutations`: append marker mutation atomically
- New sentinel `ErrTxnAlreadyCommittedAtDifferentTS`
- Unit tests: same-commit_ts replay → no-op; different-commit_ts replay →
  sentinel error

### M3 — Adapter handling of `ErrTxnAlreadyCommittedAtDifferentTS`

- `runTransaction`: catch sentinel, read back actual commit state, return
  reconstructed success
- Unit test: simulated partial-commit + EXEC retry → no duplicate,
  client sees success

### M4 — GC

- Extend `kv/compactor.go` periodic sweep with a marker-prefix scan
- TTL: `2 × (lockResolverInterval + defaultTxnLockTTLms)` = 80 s
- Metric: `elastickv_txn_marker_gc_swept_total`

### M5 — Optional: client-side commit-status check

- `commitSecondaryWithRetry`: before each retry, read primary commit
  record + secondary marker
- Skip the retry if the secondary is already applied

### M6 — Validation

- Local Jepsen reproduction (with elevated trace logging on FSM)
- Scheduled Jepsen run goes 7 consecutive days without
  `:duplicate-elements`

Total scope estimate: M1-M4 are the load-bearing milestones (~600 LOC of
Go + tests + proto change). M5 is optional, ~50 LOC. M6 is verification
only, no code.

## Risks

### R1 — Marker GC timing

If a retry arrives after the marker is GC'd (>80 s after the original
apply), the dedup misses → duplicate could still happen. Mitigation: the
TTL is already 4× the maximum natural retry window (LockResolver + lock
TTL). Operators can raise it via a tunable if late-arriving retries are
ever observed.

### R2 — Proto field on every Request

`pb.Request.AdapterTxnId` adds 16 bytes per Raft entry on every write. At
the cluster's current scale this is negligible (<1% overhead). For very
small-value workloads (e.g. counter increments), the relative cost is
higher; a future optimisation could elide the field when empty.

### R3 — `ErrTxnAlreadyCommittedAtDifferentTS` reconstruction

The adapter's read-after-commit-ts reconstruction in M3 is the most
intricate part. For list operations the reconstruction reads `meta` at
the actual commit_ts; for string SET the reconstruction reads the value
at the actual commit_ts. Each Redis command needs its reconstruction
hook. Mitigation: scope M3 to the highest-priority commands (RPUSH,
LPUSH, SET, INCR) in the initial PR; expand coverage as needed.

### R4 — Interaction with `applyTxnResolution`'s value-less PUT

`kv/shard_store.go:1111` emits `Op_PUT` with no Value during lock
resolution, relying on the FSM to look up the staged intent. The marker
check fires BEFORE intent lookup, so if the marker is present and
commit_ts matches, the intent is never touched. This is the correct
behavior (the intent is already resolved). No interaction risk.

## Open questions

- **Is the partial-commit code path the actual trigger?** PR #795 will
  surface this in the next failing scheduled run via the full log
  artifact. The marker design works regardless of which code path
  produces the partial-commit; the artifact just confirms which fix
  belongs upstream of the marker (so an earlier fix may shrink the
  partial-commit window without needing the marker at all).
- **Should the adapter_txn_id also propagate to read paths?** No —
  reads don't dedup (idempotent). Field is write-only.

## Decision log

- (2026-05-21) initial proposal; open for review.
