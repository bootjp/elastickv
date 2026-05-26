# Transactional secondary-commit idempotency

> **Status: proposed — not yet implemented.**
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

```
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

### M2 — exact-ts probe at one-phase apply

- `store` helper `committedVersionAt(key, commitTS) bool` — MVCC point-read
  for a committed version stamped **exactly** `commitTS`.
- Wire it into `handleOnePhaseTxnRequest` (`kv/fsm.go:393`): when the
  request carries a non-zero `prev_commit_ts`, probe
  `committedVersionAt(primaryKey, prev_commit_ts)` and no-op the apply on a
  hit.
- Add `prev_commit_ts uint64` to the one-phase request (zero on first
  attempt → probe skipped).
- Unit tests: prior attempt landed at exactly T1 → probe true → no-op;
  truncated/never-applied → false → apply; *other* txn committed at Tx≠T1 →
  false (exactness) → apply → OCC conflict surfaces.

### M3 — write-set reuse + result reconstruction in the retry sites

- `listPushCore` (`adapter/redis.go:3009`): on a retryable error, reuse the
  prior attempt's write set (same `seq`) and set `prev_commit_ts`; on a
  dedup no-op, return the prior length reconstructed at `prev_commit_ts`. On
  an OCC `WriteConflict` after reuse (genuine cross-txn conflict), fall back
  to full recompute.
- `runTransaction` (`adapter/redis.go:2824`): same reuse + reconstruct for
  the EXEC body (single-mop first; see Open questions).
- Unit tests per site: simulated ambiguous-commit (apply succeeds, error
  returned) + retry → no duplicate, client sees success; genuine
  cross-txn conflict + retry → two distinct elements, no dedup.

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

### R1 — Client-visible result reconstruction on the dedup no-op

When the probe finds the prior attempt landed and the apply no-ops, the
retry must return what the client would have seen. For RPUSH/LPUSH that is
the post-append length (a meta read at `prev_commit_ts`); for a MULTI/EXEC
body it is each mop's result reconstructed at `prev_commit_ts`. This is the
most intricate part. Mitigation: scope the first PR to RPUSH/LPUSH (the
commands the Jepsen workload exercises and where the duplicate is observed)
and the single-mop EXEC case; expand coverage per-command as needed.
Commands whose reconstruction is not yet implemented keep today's
(buggy-under-churn) recompute, so the change is strictly an improvement,
never a regression.

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
- **Synchronous-dispatch linchpin — VERIFIED (2026-05-26).** The
  race-freedom argument assumes a dispatch returns only after its entry has
  applied or been abandoned at a now-deposed leader (so the retry's E2 is
  never overtaken by E1). Confirmed against the propose/apply path:
  - `applyRequests` (`kv/transaction.go:152`) proposes with
    `context.Background()`, so `Engine.Propose`
    (`internal/raftengine/etcd/engine.go:606`) blocks until the entry
    applies or the proposal is *failed* — it never abandons on a caller
    timeout (the `ctx.Done()` branch is dead under Background).
  - A pending proposal is failed only via (a) submission rejection (entry
    never appended → never applies) or (b) the Leader→non-Leader transition
    in `refreshStatus` (`engine.go:2179-2190`) calling
    `failPending(errNotLeader)`, which *drains* the proposal from the
    pending map so this engine never re-proposes it.
  - On commit, `applyNormalEntry` runs `fsm.Apply` at the entry's assigned
    log index *regardless* of whether the proposal notification is still
    pending; `resolveProposal` (`engine.go:1780`) no-ops the notify if the
    entry was already drained.

  Therefore E1 applies at its (lower) log index before E2, or never applies
  (truncated / never-appended) — it can never apply *after* E2. The
  ambiguous-commit case (E1 committed by the successor leader while the
  adapter received `errNotLeader`) maps to outcome 1: E1 applies first, the
  exact-ts probe hits, E2 no-ops. M2 should add a regression test that pins
  this ordering. The linchpin holds, so option 2 is cleared for
  implementation.

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
