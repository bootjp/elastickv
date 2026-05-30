# Transactional secondary-commit idempotency

> **Status: partial — option 2 reader landed everywhere; writer (emission)
> gated off by default pending cluster-wide rollout.** M2a (store exact-ts
> probe), M2b (`prev_commit_ts` in `TxnMeta` V2 + one-phase FSM probe), and
> M1+M3 for the list-push family (write-set reuse + `OperationGroup.PrevCommitTS`
> threading through both `ShardedCoordinator` and the legacy `Coordinate`
> redirect path) have shipped on this branch and are tested. Emission stays
> off by default (`RedisServer.onePhaseTxnDedup`), so the FSM is byte-identical
> to today until operators enable it after a full probe-aware rollout — see
> R5. Remaining: `runTransaction` EXEC-body reuse and M4 (Jepsen).
>
> Triggered by the 2026-05-21 Jepsen scheduled run
> [26198185540](https://github.com/bootjp/elastickv/actions/runs/26198185540)
> which surfaced a `:duplicate-elements` anomaly on the Redis list-append
> workload. This doc proposes **option 2 — stable-write-set reuse with an
> exact-`commit_ts` dedup probe at one-phase FSM apply** as the primary
> design, and records **option (a) — give the single-group one-phase path
> a lock + commit record so TiKV-style `CheckTxnStatus` applies** as a
> documented fallback. The decision hinges on one argument, written head-on
> below: *can a retry safely reuse the failed attempt's write set without
> re-committing at a stale `commit_ts`?* The answer is yes — by separating
> the **dedup identity** (the stale `commit_ts`, read-only) from the
> **commit ordering** (a fresh, monotonic `commit_ts`, write). Because the
> argument holds, option 2 is primary; if it fails in review or test, we
> fall back to (a).

## Background

PR [#789](https://github.com/bootjp/elastickv/pull/789) (May 19) shipped the
NOTLEADER prefix for gRPC-wrapped leader errors, which stopped the Jepsen
Redis workers from crashing on `:prefix :rpc` exceptions. That fix
unmasked a deeper anomaly that had been silently classified as `:info`
(unknown outcome) under the old worker-crash regime: **`:duplicate-elements`**.

Jepsen's history.txt for the run shows:

```text
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
2. Some path inside dispatch lets Attempt 1 **actually commit** (key 14 now
   has `230` at idx 230, at commit_ts=T1) yet returns a `WriteConflict` /
   `TxnLocked` (or a transient/ambiguous error) to the adapter. The
   retryRedisWrite classifier therefore retries.
3. Attempt 2: `startTS=T2 > T1`, reads `meta.Len = 230` (the just-committed
   entry from attempt 1 is now visible), builds `listItemKey(14, 231)` and
   `ListMetaDeltaKey(14, T3, 0)` with `commitTS=T3`. Dispatch commits.
4. End state: idx 230 has value `230` at T1, idx 231 has value `230` at T3.
   LRANGE returns `[... 229 230 230]`.

The trigger that lets attempt 1 commit *and* return a retryable error has
since been positively identified as a **leader-election storm racing the
in-flight commit** — see "Resolved" below. The fix does not depend on
*which* raft event produced the ambiguous commit; it depends only on the
adapter being able to recognise, on retry, that its own previous attempt
already landed.

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
cannot catch logically-equivalent-but-recomputed retries. Note also its
`latestTS >= commitTS` comparison is a *loose* check — option 2 below
needs an *exact*-ts probe, for reasons the correctness argument makes
precise.

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
`kv/lock_resolver.go`). It is missing (2) — but, crucially, **it cannot
adopt (2) as-is**, because of the cluster's topology:

> The cluster is **single-group**: `NewEngineWithDefaultRoute`
> (`distribution/engine.go:58`) maps the whole keyspace to group 1. Every
> transaction therefore takes the **one-phase** path
> (`handleOnePhaseTxnRequest`, `kv/fsm.go:393`), which writes **no lock and
> no `txnCommitKey`** — it applies mutations directly. TiKV's
> `CheckTxnStatus` reads the primary's lock / commit record; in the
> one-phase path there is nothing for it to read. `primaryTxnStatus`
> (`kv/shard_store.go:962`) — elastickv's `CheckTxnStatus` primitive — only
> ever fires on the multi-shard 2PC path, which this deployment does not
> exercise.

So the missing layer (2) cannot be added by "just probe the commit
record": **there is no commit record on the path that produces the bug.**
Two ways forward:

- **Option 2 (primary):** keep the one-phase path commit-record-free, and
  make the *retry itself* idempotent by reusing the failed attempt's write
  set and deduping on an exact-`commit_ts` probe of the data the attempt
  would have written. No commit record, no lock, no hot-path tax.
- **Option (a) (fallback):** give the one-phase path a lock + commit record
  (turning it into a mini-2PC), so the existing `primaryTxnStatus` /
  `CheckTxnStatus` machinery applies. Correct and built on a proven
  primitive, but it taxes the single-group hot path.

The rest of this doc develops option 2, proves it, and records (a) as the
fallback.

## Proposed design (primary) — option 2: write-set reuse + exact-ts dedup

### The idea

The duplicate is born the moment the retry **recomputes** its write set
from a fresh meta read: the list grew between attempts, so `seq` advances
(230 → 231) and the retry appends at a *new* index instead of overwriting
the old one. Kill the recomputation and you kill the duplicate.

So: on a retryable failure, **do not recompute**. Reuse the previous
attempt's logical write set verbatim — same `seq`, same `listItemKey`
index, same `seqInTxn` — but commit it at a **fresh** `commit_ts`. Carry
the previous attempt's `commit_ts` (`prev_commit_ts`) into the request as a
read-only probe key. At one-phase apply, the FSM uses `prev_commit_ts` to
ask the only question that matters:

> **Did the previous attempt actually land?**

and branches:

```text
one-phase apply of the retry entry E2 (commit_ts = T2, prev_commit_ts = T1):
  if committedVersionAt(primaryKey, T1):       # attempt 1 landed
      no-op the entire apply                    # dedup: do not write at T2
      → adapter reconstructs attempt 1's result (length at T1)
  else:                                         # attempt 1 did NOT land
      apply the reused write set at T2          # OCC still guards genuine conflicts
```

The probe is `committedVersionAt(key, exactTS)` — an MVCC point lookup for
a committed version stamped *exactly* `T1`, not `>= T1`. Exactness is
load-bearing; the correctness argument explains why.

### The three outcomes

Let attempt 1 be Raft entry **E1** (`commit_ts = T1`, builds
`listItemKey(14, 230)` + `ListMetaDeltaKey(14, T1, 0)`), and the retry be
entry **E2** (`commit_ts = T2 > T1`, **reusing** `seq = 230`, so it would
build `listItemKey(14, 230)` + `ListMetaDeltaKey(14, T2, 0)`).

1. **Attempt 1 landed (the bug case).** `committedVersionAt(14-primary, T1)`
   is true. E2 no-ops entirely — no second `listItemKey`, and critically no
   second meta-delta, so `meta.Len` is not double-counted. The adapter
   reconstructs the post-append length as of T1 and returns success. List
   tail stays `[... 229 230]`. **One element. Correct.**
2. **Attempt 1 did not land (truncated / never committed).** The probe
   misses. E2 applies the reused write set at T2. Exactly one delta
   (`@T2`), one item at idx 230. **One element. Correct.**
3. **Genuine conflict (another txn took idx 230).** Attempt 1 did not land,
   so the probe misses and E2 tries to apply at T2 — but `listItemKey(14, 230)`
   now holds a committed version from the *other* txn at some `Tx`, newer
   than E2's reused `startTS`. OCC fires `WriteConflict`. The adapter falls
   back to a full recompute (fresh meta read → `seq = 231`) and appends the
   *different* value at idx 231. Two distinct values at two indices is the
   correct, non-duplicate outcome.

Outcome 3 is why we still keep `retryRedisWrite`'s recompute path as the
*fallback* branch: write-set reuse is tried first (the common, self-conflict
case), and a genuine cross-txn conflict degrades cleanly to recompute.

### Correctness — commit_ts reuse vs stale-ts ordering

This is the crux the whole design turns on. State the tension first, then
resolve it.

**The tempting shortcut, and why it is wrong.** The simplest way to make
the retry idempotent would be to reuse not just the write set but the
*same* `commit_ts` T1 — then E2's keys are byte-identical to E1's
(`listItemKey(14,230)` + `ListMetaDeltaKey(14, T1, 0)`), and a second apply
is a trivial no-op overwrite. But **re-committing new data at a stale
`commit_ts` violates Snapshot Isolation.** Concretely: a reader picks a
snapshot timestamp `S` with `T1 < S < T2` and reads the list — sees
`Len = 229` (the retry has not landed yet). Later the retry commits a write
stamped `T1 < S`. A *re-read at the same `S`* now returns `Len = 230`. The
set of versions visible at a fixed snapshot changed after the snapshot was
chosen — a non-repeatable read inside a pinned snapshot. SI's core
invariant ("the version set at `S` is fixed the instant `S` is chosen") is
broken. So naive `commit_ts` reuse is off the table.

**The resolution: separate identity from ordering.** Option 2 reuses the
write *set* but **never** the `commit_ts` as a write timestamp:

- **New data is only ever written at a fresh, strictly-monotonic
  `commit_ts` T2** (`Clock().Next()`, which always advances). Any snapshot
  `S` that could have been chosen *before* the retry was issued satisfies
  `S ≤ last-issued-ts < T2`, so the T2 write is invisible to every such
  snapshot — precisely what SI requires of a write that lands after `S` was
  fixed. **No snapshot's version set is mutated retroactively. SI holds.**
- **The stale `commit_ts` T1 is used only as a read key** in
  `committedVersionAt(primaryKey, T1)`. A point read of an old version at an
  exact timestamp adds and removes nothing; it cannot perturb any snapshot's
  visible set. T1 is demoted from "the timestamp we re-commit at" to "the
  lookup key for *did attempt 1 land?*" — and that demotion is the entire
  trick.

This is why the probe must be **exact** (`== T1`), not loose (`>= T1`). A
loose `>= T1` check (as `applyCommitWithIdempotencyFallback` uses today)
would also fire for outcome 3, where the *other* txn committed at `Tx > T1`
— misclassifying a genuine conflict as a self-duplicate and silently
dropping our append. Exactness keys the dedup to *our own* attempt and
nothing else.

**Race-freedom: the Raft log already decided E1's fate.** The remaining
worry is a TOCTOU: between the probe reading "attempt 1 not landed" and E2
writing at T2, could E1 land and produce a double write? No — because the
probe runs *inside the deterministic FSM apply of E2*, and FSM apply is a
pure function of the committed Raft log. By the time E2 applies, the log
order has already fixed E1's outcome to exactly one of:

- **E1 committed at a lower index than E2.** Then E1 applied before E2; the
  T1 version exists; the probe hits; E2 no-ops (outcome 1).
- **E1 was truncated** (lost the log race during the election that produced
  the ambiguous error). Then E1 will *never* apply; the T1 version will
  never exist; the probe correctly misses; E2 applies at T2 (outcome 2).

There is **no committed log in which E1 applies after E2.** This rests on
one architectural property, stated explicitly because it is the linchpin:
*the adapter proposes E2 only after attempt 1's dispatch has returned,* and
a dispatch returns only after its entry has either applied or been
abandoned at a leader that subsequently lost leadership. Attempt 1's
proposal therefore went to a leader `L`; the retry's E2 is proposed to a
leader `L'` whose log, at the moment E2 is appended, already contains E1
either committed-at-a-lower-index or truncated. E1 cannot enter `L'`'s log
*after* E2, because the client-proposal for E1 was abandoned and is never
re-submitted to `L'`. (If `L == L'` and both proposals are queued at the
same leader, they are appended in call order — E1 first — because dispatch
is synchronous: attempt 2 begins only after attempt 1 returns.) The
fallback (a) does **not** rely on this property — it gets its
race-freedom from the lock + commit record instead — which is exactly what
makes (a) the safe retreat if this argument is ever doubted.

**Conclusion.** Writing only at fresh monotonic timestamps preserves SI;
reading the stale timestamp only (exactly) decides the dedup; the Raft log
order makes the decision race-free. The argument holds, so option 2 is the
primary design and we proceed to implement it.

### Where it lives — caller audit

The change is split between the adapter retry loop (reuse the write set;
carry `prev_commit_ts`; reconstruct the result on dedup) and the one-phase
FSM apply (the exact-ts probe + no-op branch). No new persistent state, no
proto dedup table, no GC.

| Call site | Behaviour change | Risk |
|---|---|---|
| `listPushCore` retry (`adapter/redis.go:3009`) | on retryable error, reuse prior attempt's write set (same `seq`) + carry `prev_commit_ts`; on dedup, return prior length reconstructed at `prev_commit_ts` | result reconstruction (R1); reuse-vs-recompute branch selection (R3) |
| `runTransaction` retry (`adapter/redis.go:2824`) | same write-set reuse for the EXEC body; carry `prev_commit_ts`; reconstruct each mop's result on dedup | multi-mop result reconstruction (R1) |
| `handleOnePhaseTxnRequest` (`kv/fsm.go:393`) | **add** exact-ts probe `committedVersionAt(primaryKey, prev_commit_ts)`; no-op the apply on hit | the only FSM-side change; covered by R2/R4 |
| `commitSecondaryWithRetry` (`kv/sharded_coordinator.go:527`) | **none** — already replays the same `req` (stable `start_ts`/`commit_ts`), content-addressed and idempotent today | — |
| `LockResolver` / `applyTxnResolution` | **none** — still idempotent via the existing lock-missing check | — |

`prev_commit_ts` is the one new request field on the one-phase path
(a single `uint64`, set only on retries; zero on first attempt → probe
skipped). It is *not* a dedup token in the UUID sense: it is the prior
attempt's own commit timestamp, which the adapter already holds in a local
across the retry loop.

## Fallback design — option (a): one-phase lock + commit record

If the option-2 correctness argument fails to convince in review, or a test
exposes a case the argument missed, fall back to making the single-group
one-phase path carry a Percolator-style commit record, so elastickv's
existing, proven `primaryTxnStatus` / `CheckTxnStatus` machinery applies
directly.

**Mechanism.** Extend `handleOnePhaseTxnRequest` to (1) write a lock at
prewrite and (2) write `txnCommitKey(primaryKey, startTS) =
encodeTxnCommitRecord(commitTS)` at the commit point — i.e. give the
one-phase path the same commit record `buildCommitStoreMutations`
(`kv/fsm.go:619`) already writes on the multi-shard path. Then the
adapter's pre-retry guard is the straightforward CheckTxnStatus probe:
before re-running, call `primaryTxnStatus(primaryKey, startTS_prev)`; if it
reports `Committed`, return the prior result instead of retrying.

**Why it's the fallback, not the primary.** This turns the one-phase fast
path into a mini-2PC: a lock write plus a commit-record write (and the
commit-point ordering they imply) on *every* single-group transaction —
which is *every* transaction in this deployment. One-phase exists precisely
to avoid that cost; (a) gives it back. It is correct and leans on a
battle-tested primitive (so it is the safe retreat), but it is a measurable
hot-path tax to defend against a rare retry race. Option 2 defends the same
race with cost paid *only on the retry path* (one extra `uint64` in the
request and one exact-ts point read at apply, both reached only after an
attempt has already failed). That asymmetry is why 2 is primary.

## Why not just remove `retryRedisWrite` from `runTransaction`?

Removing the EXEC-body retry would eliminate the trigger but trade
correctness for availability: every transient `WriteConflict` (which
happens under normal OCC contention) would surface to the client as a
hard error, requiring the client to retry. Jepsen/Redis clients
typically do not retry write-conflict errors and would interpret them
as `:fail` ops. Availability under contended workloads (BullMQ,
list-append-heavy patterns) would drop measurably.

Keeping the retry but making it idempotent (reuse + exact-ts dedup)
preserves availability and adds correctness.

## Implementation milestones (option 2)

### M1 — previous-attempt write-set + identity plumbing in the retry loop

- `retryRedisWrite` (`adapter/redis_retry.go`): expose the resolved
  `(writeSet, startTS, commitTS, primaryKey)` of the attempt to the loop
  driver so the *next* iteration can reuse the write set and carry
  `prev_commit_ts`. Today the closure hides this; thread it out via a small
  result struct or a pre-attempt callback.
- No proto change beyond the single `prev_commit_ts uint64` on the
  one-phase request (M2).
- Unit test: the loop driver observes the prior attempt's write set +
  `commit_ts` on the second iteration and reuses the same `seq`.

### M2 — exact-ts probe at one-phase apply — **LANDED**

- **M2a (landed).** `store.MVCCStore.CommittedVersionAt(ctx, key, commitTS)
  (bool, error)` — an *exact*-timestamp existence check (point lookup),
  distinct from `GetAt`'s newest-version-`<=ts` scan. Implemented on
  `pebbleStore` (point `Get` on `encodeKey(key, commitTS)`; a tombstone
  counts as landed, so the value is not decoded), on the in-memory
  `mvccStore` (binary search the TS-ascending version slice), and delegated
  by `ShardStore` / `LeaderRoutedStore` to the local group store (apply-local
  probe, no leader fence/proxy). Tested on both stores, including the
  load-bearing exactness case (a version at 300 must not satisfy a probe at
  200).
- **M2b (landed).** `prev_commit_ts` is carried in the existing `TxnMeta`
  blob, **not** a new proto field: a new `TxnMeta.PrevCommitTS` field + V2
  flag `0x04`. `EncodeTxnMeta` emits V2 *only* when `PrevCommitTS != 0`, so
  every non-retry caller stays on the V1 wire format (see R5). Wired into
  `handleOnePhaseTxnRequest` (`kv/fsm.go`): when `meta.PrevCommitTS != 0`,
  probe `CommittedVersionAt(meta.PrimaryKey, meta.PrevCommitTS)` and `return
  nil` (no-op the whole apply) on a hit. FSM tests pin: prior attempt landed
  at exactly T1 → no-op (no version at T2, newest stays T1); truncated /
  never-applied → applies at T2; `prev_commit_ts == 0` → probe skipped,
  byte-identical to today.
- **M2 other-txn FSM exactness (was: still open) — LANDED.**
  `TestOnePhaseDedup_OtherTxnVersionDoesNotMaskRetry`
  (`kv/fsm_onephase_dedup_test.go`) pins exactness at the apply layer with
  the third-party-version-at-T_other ≠ T1 scenario: a foreign version at
  T_other=20 must NOT satisfy the FSM's exact-T1=30 probe, so a retry
  carrying `prev_commit_ts=30` falls through and applies at the fresh
  `commit_ts=40`. The store layer pin
  (`store/committed_version_at_test.go`) covers the primitive; the new test
  covers the FSM dispatch path that uses it.

### M3 — write-set reuse + result reconstruction in the retry sites

- **Emission gating (R5) — LANDED.** `prev_commit_ts` is emitted only when
  `RedisServer.onePhaseTxnDedup` is on (`WithOnePhaseTxnDedup` /
  `ELASTICKV_REDIS_ONEPHASE_DEDUP`), **default off** until the whole cluster
  runs a probe-aware binary. While off, `listPushCore` keeps the legacy
  recompute-on-retry loop, no V2 meta is produced, the probe never fires, and
  the FSM is byte-identical to today — no mixed-version divergence window.
- **`listPushCore` (RPUSH/LPUSH) — LANDED.** When the gate is on, the retry
  loop tracks a `reusableListPush` (the prior attempt's `ops`, `startTS`,
  `commitTS`, and computed `length`). On a retryable error it REUSES that
  write set under a fresh `commit_ts` with `PrevCommitTS` set (threaded
  through `OperationGroup` → `ShardedCoordinator.dispatchSingleShardTxn` →
  `onePhaseTxnRequestWithPrevCommit`). On success it returns the reused
  `length`; on an OCC `WriteConflict` from a reuse it drops the reusable
  attempt and recomputes from a fresh meta (outcome 3). Tests cover all three
  outcomes end-to-end against real OCC + the real probe, plus the gate-off
  legacy path.
- **Result reconstruction (R1) — resolved, and simpler than feared.** See R1.
- **`runTransaction` (MULTI/EXEC) — LANDED via PR #887 (originally PR #884, re-landed against main).** When the gate is
  on, `runTransactionWithDedup` mirrors `listPushCoreWithDedup` at the EXEC
  granularity: the first attempt builds the txn, captures `nextResults`
  from attempt 1's `startTS` snapshot, dispatches; on a retryable failure
  the closure stashes a `reusableExecTxn` and the next iteration calls
  `dispatchExecReuse` with `PrevCommitTS`. Cached `results` are returned
  on both reuse-success and self-conflict-probe-hit; a non-self
  `WriteConflict` drops pending and rebuilds.
  - **Multi-mop EXEC — LANDED.**
    `TestExecDedup_MultiMopLandedPriorAttempt_ReturnsCachedResults`
    (`adapter/redis_exec_dedup_test.go`) pins a 3-command body (SET +
    SET + DEL) where attempt 1 lands then errors: the retry returns the
    cached results array as-is (OK, OK, 1) without re-executing — DEL
    would re-execute to 0 on a second pass, which the test rejects.
  - **Two intentional deviations from the M1/M2 template, noted per
    claude[bot] PR #884 review:**
    1. `prepareDispatch()` assembles `readKeys` unconditionally (before
       the empty-elems guard), versus the old `commit()` shape that
       assembled `readKeys` only after the guard. Harmless reorder —
       the slice is discarded in the empty-elems path — but the doc
       acknowledges the small semantic shift so a future reader does
       not flag it as an oversight.
    2. ~~The reuse path in `runTransactionWithDedup` derives a fresh
       per-attempt `reuseCtx` from `handlerContext()` rather than
       reusing the outer `dispatchCtx`.~~ **Reverted per PR #887 review:**
       the original "more conservative" framing ignored that a
       disconnected client cannot benefit from the extra fresh-timeout
       budget — the wasted 10 s reuse work would never reach a waiting
       caller. `reuseCtx` is now derived from `dispatchCtx` so an outer
       cancellation interrupts mid-attempt, matching
       `listPushCoreWithDedup`'s pattern (which threads the caller ctx
       through). Per-attempt `redisDispatchTimeout` still caps the
       dispatch the same way `commit()` does for the first attempt;
       what changes is that an expired outer ctx is now respected
       promptly instead of being ignored until the fresh budget
       elapses.
- **Standalone SET — LANDED.** The standalone `r.set` handler now routes
  through `runTransactionWithDedup` as a single-mop EXEC body when the gate
  is on (the dedup machinery's "free" extension to any command whose
  `applyXxx` already exists on `txnContext`). The fast-path optimization
  (`trySetFastPath`) is intentionally bypassed under the gate — dedup is
  opt-in, and a non-dedup'd fast path under a dedup-on cluster would split
  the idempotency contract. Tested by `TestStandaloneSetDedup_*` in
  `adapter/redis_set_dedup_test.go`.
- **Standalone INCR / HSET — still open.** Both lack a `txnContext.applyXxx`
  implementation, so the "route through single-mop EXEC" pattern that
  worked for SET cannot apply as-is. Bringing them into the dedup'd path
  requires implementing `applyIncr` / `applyHSet` first (each ~30–50 LOC
  for the txn-state-aware read-compute-write shape), then the standalone
  handler routing is a one-liner via `runTransactionWithDedup`. Tracked
  as separate follow-up PRs; until then, INCR and HSET keep today's
  buggy-under-churn behaviour, which is the design doc's stated default
  ("everything else keeps today's behaviour until its hook is added" —
  Open questions).

### M4 — Validation

- Local Jepsen reproduction. Because the trigger is election churn
  (see "Resolved" below), reproduce by running the 3-node demo under
  CPU pressure or with shortened election timeouts so leadership
  flaps during the workload.
- Scheduled Jepsen run goes 7 consecutive days without
  `:duplicate-elements` / `:G-single-item-realtime`.

Scope estimate: M1–M3 are adapter + one `store` helper + a one-field
one-phase request change (~250 LOC Go + tests), no FSM dedup table, no GC.
M4 is verification only.

## Risks

### R1 — Client-visible result reconstruction on the dedup no-op — resolved

The doc originally called this "the most intricate part." For the list-push
family it turns out to be trivial, and the reason is the heart of option 2:
**because the retry reuses the write set built from attempt 1's meta, the
client-visible result is invariant across reuse.** RPUSH/LPUSH returns the
post-push length `meta.Len + len(values)`, computed once from attempt 1's
meta read. Whether attempt 1 physically committed it (probe hit → no-op) or
the reuse re-applied the identical write set (probe miss → apply at T2), the
list ends in the same state and the return value is the same number. So the
adapter simply **returns the remembered `length`** — no store re-read, no
reconstruction at `prev_commit_ts`. (`listPushCoreWithDedup` keeps it in the
`reusableListPush.length` field.) The genuine-conflict branch is the only one
that produces a different length, and there the adapter recomputes from a
fresh meta anyway, so the length is naturally correct.

This invariance is specific to commands whose retry reuses a fixed write set.
A MULTI/EXEC body with read-dependent results (e.g. an `INCR` whose return is
the post-increment value) needs the same treatment — remember the
client-visible result computed on the first attempt and return it on a dedup
no-op — which is tractable but per-command. Mitigation unchanged: scope the
first PR to RPUSH/LPUSH (the commands the Jepsen workload exercises); commands
without a reuse path keep today's (buggy-under-churn, gate-off-by-default)
recompute, so the change is strictly an improvement, never a regression.

### R2 — Probe read cost on the retry path

Each retry now carries one `uint64` and does one extra MVCC point-read
(`committedVersionAt`) at apply before deciding to write. Retries only
happen on conflict, which is rare, and the read is a single key — cost is
negligible relative to the Raft round-trip the retry would otherwise pay.
The hot (no-conflict) path is untouched: `prev_commit_ts` is zero on the
first attempt, so the probe is skipped entirely.

### R3 — Reuse-vs-recompute branch selection

Write-set reuse is correct only for a *self*-conflict (our own prior
attempt). A genuine cross-txn conflict must degrade to recompute (outcome
3). The discriminator is the exact-ts probe plus the OCC check at reuse:
probe-miss + OCC-conflict ⇒ recompute. If the adapter ever reuses when it
should recompute, it would drop a legitimate distinct append; if it
recomputes when it should reuse, it reintroduces the duplicate. The unit
tests in M3 must pin both directions. Exactness of the probe (R-argument
above) is what keeps these from blurring.

### R4 — Exact-ts probe under deep churn

If the prior attempt's data version is itself truncated by a *subsequent*
election before E2 applies, the probe correctly returns false and E2
applies its reused write set at T2 — still one element, still correct
(outcome 2), because reuse keeps `seq` stable so no duplicate index is
created even when the probe misses. This is a strict improvement over the
original bug, which only duplicated *because* the retry recomputed `seq`.
The residual failure would require E1 to apply *after* E2 in the committed
log, which the race-freedom argument rules out under the synchronous-
dispatch property; if that property is ever violated in practice, fall back
to (a).

### R5 — FSM determinism across a rolling upgrade

The probe changes the apply *outcome* (no-op vs. apply at T2), so it must be
computed identically on every node — FSM apply is deterministic by
contract; a node that no-ops while another applies would diverge the
replicas. The decision depends only on (a) the presence of `prev_commit_ts`
in the replicated log entry (identical on every node) and (b)
`CommittedVersionAt(primaryKey, prev_commit_ts)`, which is itself
deterministic: at entry E2's apply, every node has applied exactly E1..E2-1,
so the probe result is the same everywhere. The only divergence risk is a
node that lacks the probe code yet receives an entry carrying
`prev_commit_ts` — it would ignore the dedup and apply at T2 while upgraded
nodes no-op.

Two design choices close this:

1. **`prev_commit_ts` rides in `TxnMeta` V2, and V2 is emitted only when
   `prev_commit_ts != 0`.** Every existing (non-retry) caller keeps emitting
   V1, so the default wire format is unchanged. A node that predates the V2
   flag would *reject* an unknown flag (`decodeTxnMetaV2` returns
   "unsupported flags") rather than silently diverge — fail-loud, not
   fail-silent.
2. **Emission is gated off by default** (M3): the leader only starts
   emitting a non-zero `prev_commit_ts` once the whole cluster runs a
   probe-aware binary. Until then, no V2-with-prev-flag entry is ever
   produced, the probe never fires, and the FSM is byte-identical to today.
   The probe code (M2b) is always present but inert at `prev_commit_ts == 0`.

The operational contract is therefore: roll out the probe-aware binary
everywhere first (behaves exactly as before), *then* enable emission. This
is the standard "ship the reader before the writer" sequencing for a
replicated-state-machine format change.

## Resolved: the trigger is leadership churn during commit

> **Closed 2026-05-24** with the full demo-cluster log from scheduled
> run
> [26340084100](https://github.com/bootjp/elastickv/actions/runs/26340084100)
> (the first failing run after PR #795 landed the full-log artifact).

The full log positively identifies the trigger as a **leader-election
storm racing an in-flight commit**, not a single localized code bug.
Run 26340084100 reproduced the SAME `:duplicate-elements` anomaly
(`{153 2}` on key 17, with adjacent values 154/155 lost) AND added a
downstream `:G-single-item-realtime` cycle — the latter is the
append-checker's report of the corrupted per-key version order the
duplicate causes, not an independent fault.

Server-side evidence, `elastickv-demo.log`:

- The 3-node single-process demo went through **20+ leadership
  changes in the ~3-minute workload** (terms 2→3→5→7→11→…→23), with
  repeated `found conflict at index N [existing term: X, conflicting
  term: Y]` — etcd raft truncating uncommitted log-tail entries on
  each leader change.
- At `18:21:48` — the exact instant `[:append 17 153]` was processed
  — a 3-way pre-vote/vote storm was in flight: two peers at
  `index: 888` campaigning while the incumbent leader sat at
  `index: 890` (two entries ahead) with a still-valid lease
  (`remaining ticks: 6`). The two-entry divergence is precisely the
  window where a freshly-proposed commit lands in the leader's log
  but has not yet committed to quorum.

Causal chain, now confirmed:

1. The `append 17 153` commit is proposed to the incumbent leader and
   lands at log index ~889.
2. The election storm makes that entry's fate ambiguous: the leader's
   lease keeps it serving briefly, but a successor at a higher term
   may truncate index 889 (`found conflict at index …`).
3. The adapter's `retryRedisWrite` observes a `WriteConflict` /
   transient error and **retries with a fresh `commitTS`** (the
   mechanism this doc's "Problem" section describes).
4. The original entry (if it survived the election) plus the retry
   both commit → two `153`s at two different list indices.

This confirms the doc's core hypothesis (retry regenerating
`commitTS` *and `seq`* defeats content-addressing) and pins the trigger
to leadership churn rather than `commitSecondaryWithRetry` or the
LockResolver specifically. Option 2 is the correct fix because it
deduplicates **regardless of which raft event produced the ambiguous
commit** — the retry reuses the prior write set (stable `seq`) and the
one-phase apply deduplicates on the prior attempt's exact `commit_ts`,
both of which are stable across the `retryRedisWrite` regeneration and
the leader change. Step (3) in the chain above maps directly to outcome 1
(probe hits → no-op) and step (2)'s truncation maps to outcome 2 (probe
misses → apply at T2, still one element because `seq` is reused).

### CI-environment amplifier (separate, non-blocking)

The 20+ elections in 3 minutes are abnormally frequent and are
amplified by the CI environment: the single-process 3-node demo runs
co-located with the JVM Jepsen client on a 2-core GitHub runner. The
`lease is not expired (remaining ticks: 6)` + pre-vote-storm pattern
is the signature of heartbeats delayed by CPU starvation, which trips
election timeouts. This makes the ambiguous-commit window far more
frequent in CI than in the 5-node production cluster on dedicated
VMs. It does NOT invalidate the bug — production leader churn during
rolling deploys and failures hits the same window — but it explains
why CI surfaces it on most scheduled runs while production has not
reported a user-visible duplicate. A follow-up may pin the demo's
raft tick/election timing or the runner's CPU budget to reduce CI
flakiness, tracked separately from the correctness fix.

## Open questions

- **Result reconstruction coverage.** The dedup no-op must reconstruct each
  command's client-visible result at `prev_commit_ts`. RPUSH/LPUSH (length)
  and single-mop EXEC are in scope for the first PR; multi-mop EXEC and
  other write commands (SET/INCR/HSET/…) need per-command reconstruction
  hooks. Which commands beyond RPUSH/LPUSH must land in the first PR vs.
  follow-ups? (Default: only the list-append family the Jepsen workload
  exercises; everything else keeps today's behaviour until its hook is
  added.)
- **Race-freedom linchpin — RE-VERIFIED (2026-05-30).** The race-freedom
  argument needs **E1 applies before E2, or never** — never the other way
  round. An earlier verification (2026-05-26) appealed to `applyRequests`
  using `context.Background()` so that `Engine.Propose` could not abandon on
  a caller timeout. That argument was based on a stale read; codex correctly
  pointed out (PR #796 review) that the current single-shard txn path
  threads the caller ctx through (`dispatchSingleShardTxn` →
  `TransactionManager.Commit(ctx, …)` → `applyRequests(ctx, …)` →
  `Engine.Propose(ctx, …)`), and `Engine.Propose` has a live `ctx.Done()`
  early-return that calls `cancelPendingProposal`. So "Background blocks"
  is *not* what guarantees ordering. Re-prove it on what actually does:
  the FIFO propose channel and monotonic Raft log indices.

  The relevant facts in `internal/raftengine/etcd/engine.go`:
  - `Engine.Propose` enqueues `req` onto `e.proposeCh` (`engine.go:626`)
    *before* awaiting `req.done`. The raft run loop dequeues in order and
    submits to the underlying raft node, which assigns log indices
    monotonically on a single leader. A request that has entered
    `proposeCh` cannot be un-enqueued: `cancelPendingProposal`
    (`engine.go:2326`) only removes the pending-proposal notification
    entry; the payload may still be appended to the leader's log.
  - On commit, `applyNormalEntry` (`engine.go:1769`) runs `fsm.Apply` at
    the entry's assigned log index *regardless* of whether the proposal
    notification has been drained, so a cancelled or leadership-drained
    proposal still applies if it was already committed by the log.
  - The adapter goroutine that issues E1 returns control *before* it
    issues E2 (the retry is sequential), and E1's Propose-submit
    happens-before E2's. With a single leader, E1 therefore lands in
    `proposeCh` ahead of E2 and is assigned a strictly lower log index.
    If leadership has changed in between: E2 is proposed to a *different*
    leader and gets a fresh-tail index in that leader's log; E1 in the
    deposed leader's log was either already committed (index < E2's,
    applies first) or truncated (never applies). Across leaders, E1's
    original payload is never re-submitted to the new leader — the
    `failPending(errNotLeader)` drain (`engine.go:2179-2190`) errors the
    *pending* entry on this engine, but the adapter creates a brand new
    request id on retry, so E2 is a different proposal, not a re-issue
    of E1.

  Therefore E1 either applies at a lower log index than E2, or never
  applies at all. It cannot apply *after* E2. This holds whether the
  caller ctx is `Background()` or a cancellable context; cancellation
  just lets the adapter learn about a fate Raft has already decided.
  The ambiguous-commit case (E1 committed by the successor leader while
  the adapter received `errNotLeader`) maps to outcome 1: E1 applies
  first, the exact-ts probe hits, E2 no-ops.

- **FSM probe determinism — retention guard reverted (2026-05-30, round-11).**
  Round-10 surfaced `ErrReadTSCompacted` from `CommittedVersionAt` when
  the probed `commit_ts` fell below `minRetainedTS`, mirroring
  `GetAt`/`ExistsAt` semantics (codex P2 round-10). Codex P1 round-11
  correctly flagged that branching the FSM dedup decision on this signal
  is non-deterministic across raft replicas: FSM compaction advances
  `minRetainedTS` from local wall clock and per-replica scheduler
  (`kv/compactor.go:safeMinTS` uses `time.Now()`), so two replicas
  applying the same log entry at the same log index can return different
  probe outcomes — one falls through and applies at the fresh
  `commit_ts`, the other no-ops, leaving diverging MVCC histories.

  The fix reverts the retention guard inside `CommittedVersionAt`. The
  probe is now a single `pebble.Get` (or sorted-slice lookup for the
  in-memory store) and never returns `ErrReadTSCompacted`. For the
  option-2 use case this is deterministic across replicas: every
  per-element key carries at most one MVCC version (`elem:N -> value`
  appended by attempt 1), so physical pebble compaction does not remove
  it — there is no superseding version to retire. The cluster-wide
  invariant `retentionWindow > max(adapter retry latency)` keeps the
  rare "real never-landed retry below the compacted floor" case out of
  reach in practice; future work can replace the invariant with a
  separate replicated commit-ts log if option 2 needs to support
  arbitrary key shapes.

  `ErrReadTSCompacted` continues to surface from `GetAt`, `ScanAt`,
  `RawGetAt`, `RawScanAt` for genuine historical-read callers (gRPC
  adapter, S3 adapter, dualwrite proxy) where the guard expresses the
  truthful answer "the value at this timestamp is no longer
  recoverable".

## Decision log

- (2026-05-21) initial proposal; open for review.
- (2026-05-24) open question closed: full log from run 26340084100
  confirms the trigger is an election storm racing an in-flight
  commit. Recorded the CI CPU-starvation amplifier as a separate
  non-blocking follow-up.
- (2026-05-24) design switched from the UUID + FSM-marker + GC approach to
  a TiKV-style `CheckTxnStatus` probe at the adapter retry boundary.
- (2026-05-26) **CheckTxnStatus-as-primary retracted**: the cluster is
  single-group, so every txn takes the one-phase path, which writes no lock
  and no commit record — `CheckTxnStatus` has nothing to read. Doc
  rewritten with **option 2 (write-set reuse + exact-ts dedup) as primary**
  and **option (a) (one-phase lock + commit record, enabling
  `CheckTxnStatus`) as the documented fallback**. The decision hinges on the
  "commit_ts reuse vs stale-ts ordering" argument, written head-on above:
  new data is written only at a fresh monotonic `commit_ts` (SI preserved),
  the stale `commit_ts` is read-only in an exact-ts probe (dedup identity),
  and Raft log order makes the probe race-free (E1 applies before E2 or
  never). Because the argument holds, option 2 is primary and implementation
  (M1–M4) is authorised to begin; (a) is the retreat if the synchronous-
  dispatch linchpin (Open questions) cannot be guaranteed.
- (2026-05-26) **synchronous-dispatch linchpin verified** against the
  propose/apply path (`kv/transaction.go:152`, `engine.go:606/1780/2179`):
  the txn dispatch proposes with `context.Background()` and blocks until
  apply or until the proposal is failed-and-drained on a Leader→non-Leader
  transition; a drained proposal is never re-proposed, and `fsm.Apply` runs
  at the entry's own (lower) log index. So attempt-1's entry applies before
  the retry's or never — never after. The race-freedom premise holds; option
  2 is cleared to implement.
- (2026-05-26) **M2 landed.** M2a: `CommittedVersionAt` exact-ts probe on
  both store implementations (+ ShardStore/LeaderRoutedStore delegation),
  tested. M2b: `prev_commit_ts` carried as a new `TxnMeta.PrevCommitTS` field
  + V2 flag `0x04` (no proto change), with `EncodeTxnMeta` emitting V2 only
  when set, so the default wire format stays V1; the probe is wired into
  `handleOnePhaseTxnRequest` (no-op the apply on an exact hit). Recorded R5
  (FSM determinism across a rolling upgrade): resolved by the V1-default wire
  format + fail-loud unknown-flag rejection + default-off emission gating
  (ship the reader before the writer). Next: M1 (thread the prior attempt's
  write set + commit_ts out of `retryRedisWrite`) and M3 (write-set reuse +
  gated emission + result reconstruction in `listPushCore` / `runTransaction`).
- (2026-05-27) **M1 + M3 (list-push) landed.** `OperationGroup.PrevCommitTS`
  threads through `ShardedCoordinator` → `dispatchSingleShardTxn` →
  `onePhaseTxnRequestWithPrevCommit`. `listPushCore` gained a write-set-reuse
  retry loop (`listPushCoreWithDedup` + `reusableListPush`) gated by
  `RedisServer.onePhaseTxnDedup` (default off; `WithOnePhaseTxnDedup` /
  `ELASTICKV_REDIS_ONEPHASE_DEDUP`). **R1 resolved and downgraded from "most
  intricate" to trivial for list-push:** because the retry reuses the write
  set built from attempt 1's meta, the post-push length is invariant across
  reuse, so reconstruction is just returning the remembered `length` — no
  store re-read. Adapter tests cover all three outcomes (landed→dedup→no
  duplicate; not-landed→apply; genuine cross-txn conflict→recompute) against
  real OCC + the real probe, plus the gate-off legacy path. Remaining:
  `runTransaction` EXEC-body reuse (per-command result memo) and M4 (Jepsen).
