# DynamoDB item-write one-phase idempotency dedup

Status: Partial
Author: bootjp
Date: 2026-06-03

> **Status: partial — M1 (adapter dedup, gated off) ships in PR #920; M2
> (validation: add DynamoDB to the dedup-mode Jepsen workflow + 7-day green
> criterion) is open, and S3/SQS remain separate follow-ups.** Extends the
> option-2 one-phase dedup (write-set reuse + exact-`commit_ts` probe) from the
> Redis adapter to the DynamoDB adapter's single-item write path
> (`UpdateItem` / `PutItem` / `DeleteItem`). The FSM-side probe
> (`dedupProbeOnePhase`, `kv/fsm.go`) and the request plumbing
> (`OperationGroup.PrevCommitTS` → `onePhaseTxnRequestWithPrevCommit`) already
> exist and are shared across adapters; only the DynamoDB adapter retry loop
> and a default-off gate are new. Follows the parent design
> [`2026_05_21_proposed_txn_secondary_idempotency.md`](2026_05_21_proposed_txn_secondary_idempotency.md);
> read that first — this doc reuses its correctness argument verbatim and only
> develops the DynamoDB-specific differences.
>
> Triggered by the 2026-06-03 Jepsen scheduled run
> [26856696842](https://github.com/bootjp/elastickv/actions/runs/26856696842),
> which surfaced a `:duplicate-elements` anomaly on the **DynamoDB**
> list-append workload — the same anomaly class that run 26198185540 surfaced
> on the Redis workload and that motivated the parent design.

## Background — the failing run

The DynamoDB Jepsen workload (`elastickv.dynamodb-workload`, list-append model)
failed with:

```clojure
{:valid? false,
 :anomaly-types (:duplicate-elements),
 :anomalies {:duplicate-elements [{:op #Op{... :process 6 ...}
                                   :mop [:r 18 [... 71 72 72 74 75]]
                                   :duplicates {72 2}} ...]}}
```

Key `18`'s list contains the value `72` twice. Evidence that this is a
double-apply, not a generator artifact:

1. **The generator emitted `72` to key `18` exactly once.** Across the whole
   DynamoDB run, every one of the 250 invoke-appends to key 18 carries a
   distinct value (`elle.list-append/wr-txns` assigns a strictly increasing
   per-key counter; a retired key is replaced by `max+1` and never reused, so
   `(key, value)` pairs are unique). The single append of `72` is op
   `[[:append 18 72]]` (process 21, `00:47:49.875` invoke → `00:47:50.661`
   ok), a **single micro-op** routed through `dynamo-append!` (one `UpdateItem`
   with `SET #v = list_append(...)`).
2. **A Raft leader-election storm coincides with that op.** The demo-cluster
   log (`elastickv-demo.log`) shows terms 7→8→9→10→11 elected within
   `00:47:49`–`00:47:50`, with `hlc lease renewal failed: ... not leader`
   logged at the same instant — the CI CPU-starvation amplifier the parent
   doc's "CI-environment amplifier" section describes.

So a single client-issued `list_append(72)` was physically stored twice. This
is the parent doc's exact failure shape, on a different adapter.

## Problem — why the DynamoDB single-item write duplicates

The DynamoDB single-item write path is a **read-modify-write that recomputes on
retry**:

```text
adapter/dynamodb.go
  updateItem
    → updateItemWithRetry
      → retryItemWriteWithGeneration            // the retry loop
          for each attempt:
            readTS  := d.nextTxnReadTS()         // FRESH read snapshot each attempt
            plan    := prepareUpdateItemWrite(in, readTS)
                         readLogicalItemAt(readTS)          // re-READ current list
                         buildUpdatedItem → applyUpdateExpression
                                          → evalListAppendUpdateValue  // RE-APPEND value
                         buildItemWriteRequestWithSource    // Put(itemKey, encode(newList))
            plan.req.StartTS = readTS
            commitItemWrite(plan.req)  → coordinator.Dispatch
            if isRetryableTransactWriteError(err) { continue }  // WriteConflict / TxnLocked
```

The OCC snapshot is the caller-supplied `StartTS = readTS`
(`dynamodb.go:1249`). The one-phase FSM apply rejects the write if any
mutation key already has a committed version newer than `StartTS`
(`store/mvcc_store.go:533`, `latestVer.TS > startTS → WriteConflict`).
`retryItemWriteWithGeneration` retries on `store.ErrWriteConflict` /
`kv.ErrTxnLocked` (`dynamodb.go:4757`).

### The self-inflicted conflict, step by step

Let key `18` hold `[A, B]` (committed at some `T < R1`).

1. **Attempt 1.** `readTS = R1`, reads `[A, B]`, computes
   `Put(item18, encode([A, B, 72]))`, `StartTS = R1`. Dispatch proposes to the
   incumbent leader; the entry **commits at `commit_ts = C1`** and applies
   (`item18` now has a version at `C1 > R1`).
2. **Leadership churns** before the dispatch result is unambiguously returned.
   The commit is real, but the path surfaces a `WriteConflict` to the adapter.
   Two mechanisms produce this, and the fix does not depend on which:
   - the coordinator's internal leader-change retry re-proposes with the
     **same caller-supplied `StartTS = R1`** (it does not reset a
     caller-owned snapshot — `sharded_coordinator.go:449-477`); the new
     leader's OCC sees `item18@C1 > R1` and returns `WriteConflict`; or
   - the AWS SDK / a higher layer re-issues the `UpdateItem`.
3. **Attempt 2 (recompute).** `retryItemWriteWithGeneration` treats the
   `WriteConflict` as retryable, takes a **fresh** `readTS = R2 > C1`, re-reads
   — now `[A, B, 72]` (attempt 1's commit is visible) — re-appends `72`, and
   writes `Put(item18, encode([A, B, 72, 72]))`. This commits.
4. **End state:** `[A, B, 72, 72]`. **The element `72` appears twice.**

The duplicate is born exactly where the parent doc says: the retry
**recomputes** its write set from a fresh read, so the just-committed value is
folded back in and re-appended. The discriminator the adapter is missing is
"was the conflict caused by *my own* prior attempt?" — the self-inflicted
conflict the parent doc's outcome 1 handles.

### Why the existing dedup does not cover this today

The parent design landed the FSM-side probe and the request plumbing, but
gated **emission** behind the Redis adapter only:

- `OperationGroup.PrevCommitTS` (`kv/transcoder.go`) →
  `dispatchSingleShardTxn` → `onePhaseTxnRequestWithPrevCommit`
  (`kv/coordinator.go`) → `TxnMeta.PrevCommitTS` is shared by all adapters.
- The FSM probe `dedupProbeOnePhase` (`kv/fsm.go`) fires only when
  `meta.PrevCommitTS != 0`; it no-ops the apply when
  `store.CommittedVersionAt(meta.PrimaryKey, meta.PrevCommitTS)` hits.
- **But the DynamoDB adapter never sets `PrevCommitTS`.** Every
  `buildItemWriteRequestWithSource` returns an `OperationGroup` with
  `PrevCommitTS` at its zero value, so the probe is always skipped. The parent
  doc's M4 "Workflow scope rationale" states this explicitly: *"DynamoDB / S3
  / SQS do not route through the dedup loop."*

So the FSM is ready; the DynamoDB adapter is the only missing piece.

## Why DynamoDB is simpler than the Redis list-push case

The Redis list-push write set is **seq-addressed**: each element lands at
`listItemKey(key, seq)` where `seq = Tail + Len` at attempt time, plus a
`commitTS`-stamped meta-delta. The retry duplicates precisely because a fresh
meta read advances `seq` (230 → 231), so the re-append lands at a *new index*.
Reuse there must pin `seq` AND a boundary `readKeys` fence (the codex P1
shrink-race).

The DynamoDB item write is **whole-item content-addressed**: the write set is a
single `Put(itemKey, encode(nextItem))` (plus GSI delta puts/dels keyed by GSI
entry → `itemKey`). Two consequences:

1. **Same-`commit_ts` replay is already idempotent** — re-applying
   `Put(itemKey, V)` at the same `(startTS, commitTS)` is a no-op MVCC
   overwrite. The duplicate is purely a *recompute* artifact (the value `V`
   itself grows from `[A,B,72]` to `[A,B,72,72]`), never a key-divergence
   artifact.
2. **Reuse needs no `seq` pinning and no boundary fence.** Reusing the prior
   attempt's `Elems` verbatim under a fresh `commit_ts` is the whole fix; the
   single mutation key (`itemKey`) is its own OCC fence (it is a write key, so
   `checkConflictsLocked` already validates it against `StartTS`). The
   `reusableItemWrite` struct is therefore strictly smaller than
   `reusableListPush` (no `readKeys` field is required for correctness; see R3).

The correctness argument (SI preserved because new data is written only at a
fresh monotonic `commit_ts`; the stale `commit_ts` is read-only in an exact-ts
probe; Raft log order makes the probe race-free — E1 applies before E2 or
never) carries over **unchanged** from the parent doc §"Correctness" and
§"Open questions / Race-freedom linchpin". This doc does not re-derive it.

## Proposed design — option 2 for the DynamoDB item-write loop

Mirror `listPushCoreWithDedup` / `dispatchListPushReuse` at the
`retryItemWriteWithGeneration` granularity.

### Prerequisite: allocate `commit_ts` in the adapter

Today the DynamoDB item-write path leaves `OperationGroup.CommitTS = 0` and
lets the coordinator assign the commit timestamp internally. Option 2 requires
the adapter to **know** the `commit_ts` it dispatched, so it can pass it as
`PrevCommitTS` on the next attempt. The Redis adapter already does this:
`commitTS = r.coordinator.Clock().NextFenced()`, dispatched as
`OperationGroup.CommitTS`. The coordinator honors a caller-supplied
`CommitTS` **when `StartTS` is also caller-supplied** (`Dispatch` only clears
`CommitTS` on the `StartTS == 0` branch, `sharded_coordinator.go:466-477`),
which the DynamoDB path already satisfies (`StartTS = readTS != 0`).

So the prepare step gains one line: allocate `commitTS` via
`d.coordinator.Clock().NextFenced()` and set `plan.req.CommitTS`. `NextFenced`
(not `Next`) is mandatory — it honors the HLC physical-ceiling fence so a
stale-leader window cannot mint a colliding timestamp (HLC-4). `NextFenced`'s
`ErrCeilingExpired` must be classified **non-retryable** by the adapter loop
(surface to the client), matching the Redis call sites.

**Leader-only allocation (codex P1, PR #920).** This local allocation moves
timestamp issuance from the coordinator (which has the *leader* assign it on
the non-dedup path) into the adapter, so it is only safe **on the leader** —
otherwise two follower frontends could mint the same `commitTS` in one
millisecond (the HLC logical counter is process-local), and a retry carrying
that `prev_commit_ts` would make the FSM exact-ts probe dedup against the
*other* writer's version, losing an update. The dedup path is therefore gated
on `d.coordinator.IsLeader()` in addition to the feature flag: on the leader
the single HLC issues monotonic unique values and `NextFenced`'s ceiling fence
keeps a deposed leader's window disjoint from its successor's (so even a TOCTOU
leadership loss after the `IsLeader()` check cannot collide); a non-leader
falls back to the legacy path, where `Coordinator.Dispatch` redirects to the
leader and the **leader** allocates `commitTS`. In the normal multi-node
deployment the DynamoDB adapter already HTTP-proxies all follower ingress to
the leader (`proxyToLeader`), so `retryItemWriteWithGeneration` runs on the
leader and the dedup path is taken; the `IsLeader()` guard only matters for the
no-`leaderMap` topology that relies on `Coordinator.Dispatch` redirect, where
it correctly degrades to legacy (no dedup, no lost update) rather than minting
a follower-local timestamp.

### The retry loop (gated)

```text
retryItemWriteWithGenerationDedup(ctx, tableName, prepare):
    var pending *reusableItemWrite
    for each attempt (bounded by transactRetryMaxAttempts / MaxDuration):
        if pending != nil:
            plan, drop, err := dispatchItemWriteReuse(ctx, pending)
            if drop { pending = nil }
            if err == nil { return plan }            // reuse landed (or dedup no-op)
            if not retryable { return err }
            continue
        // first attempt (recompute)
        readTS  := d.nextTxnReadTS()
        plan    := prepare(readTS)                    // re-read + recompute write set
        if plan.req == nil { return plan }
        plan.req.StartTS  = readTS
        plan.req.CommitTS = d.coordinator.Clock().NextFenced()
        err := commitItemWrite(ctx, plan.req)
        if err == nil {
            verify generation fence; return plan      // unchanged
        }
        if isRetryableTransactWriteError(err):
            pending = &reusableItemWrite{
                elems:    plan.req.Elems,             // REUSE verbatim
                startTS:  plan.req.StartTS,
                commitTS: plan.req.CommitTS,          // becomes next PrevCommitTS
                probeKey: primaryKeyForElems(plan.req.Elems), // == FSM meta.PrimaryKey (R4)
                plan:     plan,                       // for result reconstruction (R1)
            }
        return/continue per the loop predicate
```

```text
dispatchItemWriteReuse(ctx, pending) -> (plan, drop, err):
    commitTS := d.coordinator.Clock().NextFenced()
    err := commitItemWrite(ctx, &OperationGroup{
        IsTxn:        true,
        StartTS:      pending.startTS,
        CommitTS:     commitTS,
        PrevCommitTS: pending.commitTS,               // FSM probes this exact ts
        Elems:        pending.elems,                   // REUSE verbatim — no recompute
    })
    if err == nil:
        return pending.plan, false, nil                // reuse applied (or FSM no-op'd)
    if errors.Is(err, store.ErrWriteConflict):
        // Self-inflicted-conflict guard (parent doc codex P1): the apply may
        // have landed at THIS commitTS but bubbled up as WriteConflict under
        // churn. Probe the store directly.
        landed, perr := d.store.CommittedVersionAt(ctx, pending.probeKey, commitTS)
        if perr == nil && landed:
            pending.commitTS = commitTS                // keep probe pointed at the landed ts
            return pending.plan, false, nil            // success, no double-apply
        return nil, true /*drop*/, err                 // genuine conflict → recompute
    if isRetryableTransactWriteError(err):
        pending.commitTS = commitTS                    // still ambiguous; next probe = this ts
    return nil, false, err
```

This is a structural copy of `dispatchListPushReuse`, minus the `readKeys`
fence and the meta re-read (DynamoDB's result is the item itself; see R1).

### The three outcomes (DynamoDB instantiation)

Reusing the parent doc's framing, with E1 = attempt 1 (`commit_ts = C1`,
writes `Put(item18, [A,B,72])`) and E2 = reuse (`commit_ts = C2 > C1`,
`PrevCommitTS = C1`, **reuses** `Put(item18, [A,B,72])`):

1. **E1 landed (the bug case).** `CommittedVersionAt(item18, C1)` hits at FSM
   apply → E2 no-ops. Final `[A,B,72]`. **One `72`. Correct.**
2. **E1 never landed (truncated by the election).** Probe misses → E2 applies
   `Put(item18, [A,B,72])` at C2. Final `[A,B,72]`. **One `72`. Correct.**
3. **Genuine conflict (another txn wrote `item18` at `Tx > startTS`).** Probe
   misses (E1 didn't land); OCC fires `WriteConflict` on E2's reuse; the
   self-conflict guard probes `CommittedVersionAt(item18, C2)` → misses → drop
   pending → recompute from a fresh read (`[A,B,X]`) → append once →
   `[A,B,X,72]`. **No duplicate, no lost update. Correct.**

### Result reconstruction (R1) — trivial, as for list-push

`dynamo-append!` uses default `ReturnValues=NONE`, so the dedup no-op returns
an empty body. For `ReturnValues=ALL_NEW`/`UPDATED_NEW` the result is
`plan.next`; for `ALL_OLD`/`UPDATED_OLD` it is `plan.current`. Both are
captured once on attempt 1 and are **invariant across reuse** (the write set is
fixed), so the adapter returns the remembered `plan` on a dedup no-op — no
store re-read. This is the parent doc's R1 invariance, instantiated for the
item write.

### Gating (R5) — default on, rollback switch remains

Emission is controlled by a DynamoDB flag, mirroring
`RedisServer.onePhaseTxnDedup`. The probe-aware FSM reader now ships on every
node, so the writer side can be the default behavior while retaining an
operator rollback switch:

- `DynamoDBServer.onePhaseTxnDedup bool`, set from
  `os.Getenv("ELASTICKV_DYNAMODB_ONEPHASE_DEDUP") != "0"` and/or a
  `WithDynamoOnePhaseTxnDedup(bool)` server option (distinct symbol name to
  avoid colliding with the Redis `WithOnePhaseTxnDedup` in the same `adapter`
  package).
- **On (default)** → the dedup loop above.
- **Off (`ELASTICKV_DYNAMODB_ONEPHASE_DEDUP=0` or
  `WithDynamoOnePhaseTxnDedup(false)`)** →
  `retryItemWriteWithGeneration` keeps its current
  recompute-on-retry behavior byte-for-byte; no `PrevCommitTS` is ever emitted;
  the FSM probe never fires.

The FSM-determinism argument (R5 in the parent doc) holds unchanged: the probe
decision depends only on the replicated `PrevCommitTS` and the deterministic
`CommittedVersionAt`, which for a whole-item single-version key is identical on
every replica applying the same log entry.

## Scope

- **In scope (M1):** the shared single-item write loop
  `retryItemWriteWithGeneration`, which backs `UpdateItem`, `PutItem`, and
  `DeleteItem`. The fix lives in the loop, so all three inherit it; the Jepsen
  workload exercises `UpdateItem`.
- **Out of scope (follow-up):** `TransactWriteItems` (multi-item) takes a
  separate path (`dynamo-transact-write!` with explicit `ConditionExpression`
  OCC). It already surfaces conflicts as `TransactionCanceledException`, which
  the workload classifies as a definite `:fail` (excluded from the history), so
  it does not produce the duplicate. Bringing it under reuse-dedup is tractable
  but doubles the test matrix; defer it.
- **S3 / SQS — audited (2026-06-03).** A handler-by-handler adversarial sweep
  found that the original "same latent bug class" claim was over-broad. The
  DynamoDB bug is specific to a read-modify-write that **recomputes a growing
  value** (`list_append`) at a **stable key** on a self-conflict retry. The
  audit verdicts:
  - **S3 — no duplicate possible.** Object writes are whole-value PUTs at a
    stable key (`PutObject`, `CompleteMultipartUpload` whose manifest is
    re-assembled deterministically from the immutable uploaded parts, not from
    a re-read of the object value). `PutObject` carries a caller-supplied
    `StartTS`, so a leader-move coordinator retry that races an interleaved
    write fails the OCC fence rather than clobbering it; it has no in-process
    re-read-recompute retry. No `list_append`-style growth, no recompute-into-a-
    different-value → no `:duplicate-elements`.
  - **SQS single `SendMessage` / FIFO — safe.** Single send keys the message by
    a random `MessageId` with no in-process retry (a conflict surfaces to the
    client as a normal at-least-once SDK retry). FIFO send is fenced by the
    `MessageDeduplicationId` dedup record (committed atomically with the
    sequence), whose hit short-circuits any retry.
  - **SQS standard `SendMessageBatch` — HAD the bug, FIXED here.** Unlike single
    send, the batch path added an **in-process** retry loop
    (`sendMessageBatchWithRetry`) that re-minted fresh random `MessageId`s (and
    send timestamps) per attempt, so a committed-but-conflicted attempt plus the
    retry double-sent every entry. Fixed by pre-generating one stable
    `sqsSendIdentity` per entry **before** the retry loop and reusing it on
    every attempt (`buildSendRecordWithIdentity`), so the retry overwrites the
    same keys idempotently. This needs **no** FSM probe / `PrevCommitTS` / gate —
    the keys become content-stable, so re-applying is a plain idempotent
    overwrite. The identity also pins `AvailableAtMillis` (vis-key input) and the
    per-entry validation re-runs against the current meta on every attempt, so a
    mid-retry `SetQueueAttributes` neither shifts the vis key (codex P2 round-1)
    nor lets a now-too-large body through (codex P2 round-2). Tests:
    `adapter/sqs_batch_send_dedup_test.go`.
    - **Residual edge (within at-least-once, no action):** if attempt 1
      *commits* (committed-but-conflicted) and a concurrent `SetQueueAttributes`
      tightens a limit (e.g. lowers `MaximumMessageSize`) before the retry, the
      retry's re-validation rejects the entry into `Failed[]` even though it is
      already in the queue — an inconsistent client view (message stored, client
      told it failed) but never a double-send. This is within the SQS
      standard-queue at-least-once contract; distinguishing committed-vs-not
      would need a dedup probe, which is out of proportion for this corner.

## Milestones

### M1 — DynamoDB adapter reuse-dedup (default on)

- Adapter `commit_ts` allocation in the prepare step
  (`d.coordinator.Clock().NextFenced()` → `plan.req.CommitTS`).
- `reusableItemWrite` struct + `dispatchItemWriteReuse` (mirror
  `reusableListPush` / `dispatchListPushReuse`).
- `retryItemWriteWithGeneration` gains the dedup loop; the explicit opt-out
  path is unchanged.
- `DynamoDBServer.onePhaseTxnDedup` + `WithDynamoOnePhaseTxnDedup` + env var;
  wire the env var in the server constructor with `0` as the rollback value.
- **Tests (`adapter/dynamodb_*_dedup_test.go`), all three outcomes against the
  real OCC + the real probe, plus the gate-off legacy path:**
  - `LandedPriorAttempt_ReturnsCachedResult` — attempt 1 lands then errors;
    reuse FSM-probe hits; final list has one copy; no second apply.
  - `PriorAttemptDidNotLand_Applies` — attempt 1 pre-rejected; reuse applies
    fresh; one copy.
  - `GenuineConflictRebuildsAndApplies` — concurrent write advances `item18`
    past `startTS`; reuse OCC-conflicts; probe misses; pending dropped; fresh
    recompute appends once (no duplicate, no lost update).
  - `SelfInflictedReuseConflict_ReturnsSuccess` — reuse lands then surfaces
    `WriteConflict`; self-conflict guard probes fresh `commitTS`; cached result
    returned (no double-apply).
  - `DisabledKeepsLegacyPath` — gate off; identical to today's
    `retryItemWriteWithGeneration`.
- Per the repo convention (failing-test-first for a review-identified defect):
  the `LandedPriorAttempt` test, written against the **gate-off** path first,
  reproduces the `:duplicate-elements` double-apply; flipping the gate on makes
  it pass.

### M2 — Validation

- Local reproduction: 3-node demo under CPU pressure / shortened election
  timeouts (`cmd/server/demo.go`) so leadership flaps during the DynamoDB
  workload. The default path has DynamoDB dedup enabled; set
  `ELASTICKV_DYNAMODB_ONEPHASE_DEDUP=0` only to reproduce the legacy path.
- **CI — LANDED.** The DynamoDB list-append workload was added to the
  dedup-mode workflow during rollout. After the default-on soak period, the
  dedicated dedup workflow was retired; the general scheduled workflow
  (`.github/workflows/jepsen-test-scheduled.yml`) now runs the default
  `DynamoDBServer.onePhaseTxnDedup` path without an env-var opt-out.
- Criterion to default-on: 7 consecutive days without `:duplicate-elements` in
  the dedup-mode DynamoDB workload, both workflows green. **Satisfied; this PR
  flips `DynamoDBServer.onePhaseTxnDedup`'s default and the env-var sense to
  default-on with `0` as the rollback value.**

Scope estimate: M1 is ~120–180 LOC Go in `adapter/dynamodb.go` + tests; no FSM
change (the probe already exists), no proto change, no new store primitive.

## Risks

- **R1 — result reconstruction.** Resolved as for list-push: the item-write
  result (`plan.current` / `plan.next`) is invariant across reuse; return the
  remembered `plan`. No store re-read.
- **R2 — probe cost.** One extra `uint64` and one `CommittedVersionAt` point
  read, only on the retry path; hot path (first attempt, `PrevCommitTS == 0`)
  untouched. Same as the parent doc R2.
- **R3 — reuse-vs-recompute discrimination.** The exact-ts probe plus the OCC
  check on the reuse is the discriminator (probe-miss + OCC-conflict ⇒
  recompute). For the whole-item PUT this is *cleaner* than list-push: there is
  a single write key, so no boundary `readKeys` fence is needed — the OCC check
  on `itemKey` itself distinguishes a self-conflict (probe hits) from a foreign
  write (probe misses). The tests pin both directions.
- **R4 — `PrevCommitTS` derivation correctness + probe-key agreement.**
  Because the adapter now allocates `commit_ts` locally, it holds the exact
  value to probe even when the dispatch returns only an error. The probe key,
  however, is **not** simply "the itemKey": the coordinator derives
  `meta.PrimaryKey` via `primaryKeyForElems` (`kv/coordinator.go:1110`), which
  selects the **lexicographically smallest** write key among the elems, not
  the first. Two consequences:
  - For the Jepsen `jepsen_append` table (HASH key only, **no GSI**) the write
    set is a single `Put(itemKey, …)`, so the min key *is* the itemKey and the
    FSM probe targets it directly — the failing workload is covered.
  - For a table with GSIs the write set also carries GSI delta puts/dels, so
    the min key may be a GSI entry key rather than the itemKey. This is still
    correct: every elem commits atomically in one Raft entry at `C1`, so
    `CommittedVersionAt(min_key, C1)` hits iff attempt 1 landed (a Del's
    tombstone at `C1` counts as landed per the parent doc's M2a). The
    requirement is only that the **adapter-side self-conflict guard probe the
    SAME key the FSM uses** — so `dispatchItemWriteReuse` must compute
    `probeKey = primaryKeyForElems(pending.elems)` (exported or duplicated for
    the adapter), not the first write key. (This is the one place the DynamoDB
    instantiation must deviate from `dispatchListPushReuse`, which probes
    `firstWriteKey`; for list-push the first write key and the min key coincide
    on the seq-addressed layout, but for the item write they need not.)
- **R5 — FSM determinism across rolling upgrade.** Unchanged from the parent
  doc: V1-compatible opt-out wire format (no `PrevCommitTS` ⇒ V1 meta),
  fail-loud unknown-flag rejection, and default-on emission only after the
  probe-aware FSM ships on every node.

## Decision log

- (2026-06-18) Default-on follow-up: `DynamoDBServer.onePhaseTxnDedup` now
  defaults on because the probe-aware FSM reader is everywhere. Operators can
  still set `ELASTICKV_DYNAMODB_ONEPHASE_DEDUP=0` or
  `WithDynamoOnePhaseTxnDedup(false)` for rollback.
- (2026-06-26) Post-flip CI cleanup: retired the legacy-path scheduled control
  by removing the `ELASTICKV_DYNAMODB_ONEPHASE_DEDUP=0` opt-out from the general
  scheduled Jepsen workflow and deleting the dedicated dedup-mode workflow.
- (2026-06-03, PR #920 round-1) **Leader-only dedup guard added** per codex P1:
  the adapter-local `commitTS` allocation is only safe on the leader, so the
  dedup path is gated on `d.coordinator.IsLeader()` (+ `NextFenced` ceiling
  fence for the cross-leader case); non-leaders fall back to the legacy path
  where the leader allocates. Covered by
  `TestItemWriteDedup_NonLeaderFallsBackToLegacy`. Also addressed gemini's
  redundant-`errors.WithStack` findings (the attempt helpers return the
  already-wrapped dispatch error raw; the outer loop wraps once).
- (2026-06-03) Initial proposal, triggered by Jepsen run 26856696842
  (`:duplicate-elements` on the DynamoDB list-append workload). Open for
  review. Diagnosis confirmed: same anomaly class as the parent design
  (`2026_05_21_proposed_txn_secondary_idempotency.md`); the DynamoDB
  single-item write recomputes its write set on a self-inflicted
  `WriteConflict` under leadership churn and double-applies. Fix: extend
  option-2 reuse-dedup to `retryItemWriteWithGeneration`.
