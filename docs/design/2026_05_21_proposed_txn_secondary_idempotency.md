# Transactional secondary-commit idempotency

> **Status: proposed — not yet implemented.**
> Triggered by the 2026-05-21 Jepsen scheduled run
> [26198185540](https://github.com/bootjp/elastickv/actions/runs/26198185540)
> which surfaced a `:duplicate-elements` anomaly on the Redis list-append
> workload. This doc proposes a **TiKV-style `CheckTxnStatus` probe at
> the adapter's retry boundary**: before re-running an operation after a
> conflict, check whether the previous attempt already committed and, if
> so, return its result instead of re-appending. An earlier draft
> proposed a UUID + FSM dedup table; that is recorded below as
> "considered and rejected" — `CheckTxnStatus` is lighter (adapter-only,
> no proto/FSM/GC) and sufficient for the observed bug.

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
`runTransaction`'s EXEC-body re-execution produces — and the regeneration
is exactly what defeats (1), because RPUSH's target key (the list index)
is data-dependent and changes when the list grows between attempts.
**Adding (2) is this design.**

## Proposed design — CheckTxnStatus before retry (TiKV layer 2)

The fix is to add the one TiKV layer elastickv is missing: **before
re-running the operation, check whether the previous attempt actually
committed**. If it did, return its result instead of recomputing and
re-appending. This is exactly TiKV's `CheckTxnStatus`, applied at the
elastickv adapter's retry boundary. It needs no new wire field, no FSM
dedup table, and no GC — only a few bytes of per-attempt state held in
a local variable across the `retryRedisWrite` loop.

### Why a UUID / FSM marker is NOT needed (considered and rejected)

The earlier draft of this doc proposed a server-allocated UUIDv7
(`Request.AdapterTxnId`) written to a `__txn_applied__<uuid>` dedup
table at FSM apply time, GC'd on a TTL. That works, but it is
over-engineered for the observed bug:

- It adds a proto field on every write `Request`, an FSM-side marker
  read+write on every commit, and a periodic GC sweep — permanent
  cost on the hot path to defend a rare retry race.
- Its only capability beyond CheckTxnStatus is deduping a retry that
  spans *different client commands* (a client re-sending the same
  RPUSH as a brand-new request). But under Redis semantics that is a
  *legitimate* second append — `RPUSH k v` twice means two elements —
  so there is nothing to dedup there. The marker would be guarding a
  case that should not be guarded.

The observed Jepsen duplicate is always a retry *inside a single
`retryRedisWrite` loop* (one client op, internally retried). That is
precisely the scope CheckTxnStatus covers, so the UUID/marker buys no
additional correctness for this bug.

### The mechanism

The duplicate exists because, on a `WriteConflict`/`TxnLocked` retry,
`retryRedisWrite` cannot tell two cases apart:

- **Genuine conflict** — *another* txn took the list index we wanted.
  We MUST re-read and re-append at a fresh index. Recompute is correct.
- **Spurious conflict (the bug)** — *our own* previous attempt already
  committed (its entry survived the election that returned us an
  ambiguous error), and the conflict we see is against our own write.
  We must NOT re-append; we should return the prior attempt's result.

TiKV distinguishes these with `CheckTxnStatus(primary_key, start_ts)`.
elastickv can do the same by remembering the **previous attempt's
identity** — `(startTS_prev, commitTS_prev, primaryKey_prev)` — in a
local variable across the retry loop, then probing whether that attempt
landed before re-running:

```
retryRedisWrite loop, attempt N+1 (only entered after attempt N failed):
  if attempt N actually committed:        # CheckTxnStatus
      read back the result at commitTS_prev and return success
  else:
      genuine conflict → re-read state, recompute, dispatch (attempt N+1)
```

"Did attempt N commit?" is answered without any new persistent state:

- **Multi-shard 2PC** (`handleCommitRequest` path): the primary shard
  writes `txnCommitKey(primaryKey_prev, startTS_prev)` at the commit
  point (`buildCommitStoreMutations`, kv/fsm.go:619). Probe it; if
  present, the txn is committed (Percolator's commit point), and the
  pending secondaries are the LockResolver's job. Return success.
- **Single-shard one-phase** (`handleOnePhaseTxnRequest`, kv/fsm.go:393):
  this path does NOT write a commit record — it applies mutations
  directly. So instead probe the previous attempt's own data write
  content-addressed: does `listItemKey(key, seq_prev)` have a committed
  version at `commitTS_prev`? If yes, attempt N landed. This is a pure
  MVCC point-read on a key we already computed; still no new state.

Because the probe keys on the *previous attempt's* identity (held in a
local), it is unaffected by the very regeneration that causes the bug:
attempt N+1 recomputing a new `startTS`/`commitTS`/index does not change
what we are checking for attempt N.

### Where the check lives

Three call sites run the retry loop; each gets the same pre-retry
CheckTxnStatus guard:

- `runTransaction` (`adapter/redis.go:2824`) — MULTI/EXEC body. The
  primary key is `primaryKeyForElems` over the EXEC write set;
  multi-shard → probe `txnCommitKey`.
- `listPushCore` (`adapter/redis.go:3009`) — single-command RPUSH/LPUSH.
  Often single-shard → probe the previous attempt's `listItemKey` at
  `commitTS_prev`.
- `commitSecondaryWithRetry` (`kv/sharded_coordinator.go:527`) — already
  retries the same `req` (stable `start_ts`/`commit_ts`), so its replay
  is content-addressed and idempotent today; the CheckTxnStatus guard
  here is a cheap short-circuit that avoids re-proposing once the
  primary commit record shows the txn is done.

### Caller audit

The behaviour change is confined to the adapter retry loop. The FSM
apply path is **unchanged** — no new sentinel, no marker read/write.

| Call site | Behaviour change | Risk |
|---|---|---|
| `runTransaction` retry | before re-running EXEC body, probe prev attempt's `txnCommitKey`; if committed, read-back + return success | must reconstruct the client-visible result at `commitTS_prev` (see R1) |
| `listPushCore` retry | before re-RPUSH, probe prev attempt's `listItemKey@commitTS_prev`; if present, return prev length | length reconstruction is a meta read at `commitTS_prev` |
| `commitSecondaryWithRetry` | probe primary `txnCommitKey` before re-propose; short-circuit if committed | none — same `req`, already idempotent; this only saves a Raft round-trip |
| FSM `handleCommitRequest` / `handleOnePhaseTxnRequest` | **none** | — |
| `LockResolver` / `applyTxnResolution` | **none** — still idempotent via the existing lock-missing check | — |

No FSM-side caller is affected because the dedup decision moves entirely
to the adapter's pre-retry check; the FSM keeps applying whatever commit
requests reach it, and the adapter simply stops issuing the duplicate
one.

## Why not just remove `retryRedisWrite` from `runTransaction`?

Removing the EXEC-body retry would eliminate the trigger but trade
correctness for availability: every transient `WriteConflict` (which
happens under normal OCC contention) would surface to the client as a
hard error, requiring the client to retry. Jepsen/Redis clients
typically do not retry write-conflict errors and would interpret them
as `:fail` ops. Availability under contended workloads (BullMQ,
list-append-heavy patterns) would drop measurably.

Keeping the retry but teaching it to recognise its own prior success
(CheckTxnStatus) preserves availability and adds correctness.

## Implementation milestones

### M1 — previous-attempt identity plumbing in the retry loop

- `retryRedisWrite` (`adapter/redis_retry.go`): expose the attempt's
  resolved `(startTS, commitTS, primaryKey)` to the loop driver so it
  can be remembered between iterations. Today the closure hides this;
  thread it out via a small result struct or a pre-attempt callback.
- No proto change, no wire field.
- Unit test: the loop driver observes the prior attempt's identity on
  the second iteration.

### M2 — CheckTxnStatus probe helper

- `kv` helper `txnCommittedAt(primaryKey, startTS) (commitTS, bool)`
  reading `txnCommitKey` on the primary shard (multi-shard path).
- `store` helper `committedVersionAt(key, commitTS) bool` — MVCC
  point-read for the single-shard one-phase path.
- Unit tests: committed prior attempt → true; aborted / never-applied
  → false; cross-key probe isolation.

### M3 — wire the probe into the three retry sites

- `runTransaction` (`adapter/redis.go:2824`): before re-running the EXEC
  body, if the prior attempt's `txnCommitKey` is present, read back the
  EXEC's client-visible results at `commitTS_prev` and return success.
- `listPushCore` (`adapter/redis.go:3009`): before re-RPUSH, if the
  prior attempt's `listItemKey@commitTS_prev` exists, return the prior
  length (meta read at `commitTS_prev`).
- `commitSecondaryWithRetry` (`kv/sharded_coordinator.go:527`):
  short-circuit the re-propose when the primary commit record shows the
  txn is done.
- Unit tests per site: simulated ambiguous-commit (apply succeeds, error
  returned) + retry → no duplicate, client sees success.

### M4 — Validation

- Local Jepsen reproduction. Because the trigger is election churn
  (see "Resolved" below), reproduce by running the 3-node demo under
  CPU pressure or with shortened election timeouts so leadership
  flaps during the workload.
- Scheduled Jepsen run goes 7 consecutive days without
  `:duplicate-elements` / `:G-single-item-realtime`.

Scope estimate: M1–M3 are adapter + kv/store helper changes (~250 LOC
Go + tests), no proto change, no FSM change, no GC. M4 is verification
only.

## Risks

### R1 — Client-visible result reconstruction on the success short-circuit

When the probe finds the prior attempt committed, the retry must
return what the client would have seen. For RPUSH/LPUSH that is the
post-append length (a meta read at `commitTS_prev`); for a MULTI/EXEC
body it is each mop's result reconstructed at `commitTS_prev`. This is
the most intricate part. Mitigation: scope the first PR to RPUSH/LPUSH
(the commands the Jepsen workload exercises and where the duplicate is
observed) and the single-mop EXEC case; expand reconstruction coverage
per-command as needed. Commands whose reconstruction is not yet
implemented fall back to the current (buggy-under-churn) retry, so the
change is strictly an improvement, never a regression.

### R2 — Probe read cost on the retry path

Each retry now does one extra point-read (primary `txnCommitKey`, or a
single-key MVCC lookup) before deciding to recompute. Retries only
happen on conflict, which is rare, and the read is a single key — cost
is negligible relative to the Raft round-trip the retry would otherwise
pay. The hot (no-conflict) path is untouched: the probe is only reached
after an attempt has already failed.

### R3 — Probe false-negative under deep churn

If the prior attempt's commit record / data version has itself been
truncated by a *subsequent* election before the probe reads it, the
probe returns false and the retry recomputes → a duplicate could still
slip through. This is far narrower than the original bug (it needs two
elections to straddle a single retry) and converges: the LockResolver
and the OCC conflict check still bound the damage. If observed, the
mitigation is the heavier FSM-marker design (preserved in git history
of this doc) — but it is not warranted for the churn rates seen so far.

### R4 — Single-shard one-phase has no commit record

`handleOnePhaseTxnRequest` (kv/fsm.go:393) does not write a
`txnCommitKey`, so the multi-shard probe does not apply. M2's
`committedVersionAt(key, commitTS)` MVCC point-read covers this case by
checking the prior attempt's own data write directly. Verified against
the one-phase apply path; no new persistent state required.

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
`commitTS` defeats content-addressing) and pins the trigger to
leadership churn rather than `commitSecondaryWithRetry` or the
LockResolver specifically. The server-side marker (M1–M4) is the
correct fix because it deduplicates **regardless of which raft event
produced the ambiguous commit** — it keys on the adapter-allocated
txn UUID, which is stable across both the `retryRedisWrite`
regeneration and the leader change.

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

- **Result reconstruction coverage.** The success short-circuit must
  reconstruct each command's client-visible result at `commitTS_prev`.
  RPUSH/LPUSH (length) and single-mop EXEC are in scope for the first
  PR; multi-mop EXEC and other write commands (SET/INCR/HSET/…) need
  per-command reconstruction hooks. Which commands beyond RPUSH/LPUSH
  must land in the first PR vs. follow-ups? (Default: only the
  list-append family the Jepsen workload exercises; everything else
  keeps today's behaviour until its hook is added.)
- **Should `commitSecondaryWithRetry`'s short-circuit be in the first
  PR or deferred?** It is a pure optimisation (the same-`req` replay is
  already idempotent), so it can ship separately from the correctness
  fix in `runTransaction` / `listPushCore`.

## Decision log

- (2026-05-21) initial proposal; open for review.
- (2026-05-24) open question closed: full log from run 26340084100
  confirms the trigger is an election storm racing an in-flight
  commit. Recorded the CI CPU-starvation amplifier as a separate
  non-blocking follow-up.
- (2026-05-24) design switched from the UUID + FSM-marker + GC approach
  to a TiKV-style `CheckTxnStatus` probe at the adapter retry boundary.
  Rationale: the observed duplicate is always an internal retry within a
  single `retryRedisWrite` loop, which `CheckTxnStatus` fully covers
  without a wire field, FSM dedup table, or GC. The UUID design's only
  extra reach — deduping a retry across *different* client commands — is
  a case Redis semantics treat as a legitimate second append, so it
  guards nothing real. The marker design remains in this file's git
  history if R3 (probe false-negative under deep churn) is ever
  observed in practice.
