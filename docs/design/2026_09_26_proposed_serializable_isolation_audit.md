# Serializable isolation audit

Status: Proposed
Author: bootjp
Date: 2026-09-26
Related: `2026_09_26_proposed_positioning_and_roadmap.md` §5 and §6.3 (the
claim this audit backs), `2026_08_29_proposed_tso_batch_slot_claims.md` and
`2026_09_02_proposed_prephase_d_resolution_evidence.md` (timestamp
uniqueness, which the conflict check assumes).

## 1. Claim and scope

The positioning doc commits elastickv to this claim: **multi-key transactions
are atomic and serializable; write skew cannot occur.** The engine already
validates a transaction's read set at commit, so this is not a new isolation
mechanism. It is an audit that (a) finds every path where a read is not
surfaced to that validation, (b) closes the engine-level holes the analysis in §2 and §4 found (an
apply-order gap, G0, and an unprotected PREPARE-to-COMMIT window, G1), (c) makes
the TLA+ OCC model say what the code does, (d) adds Jepsen workloads that can
actually produce write skew, and (e) rewrites the documentation to match.

Out of scope: follower or learner reads, the leader-lease caveat (the
positioning doc keeps "strict serializable" unclaimed), and the TSO admission
proposals listed above (same correctness track, separate docs).

## 2. What the engine does today

Facts checked on `main` at `4ca7e90d`.

- **Conflict check.** `checkConflictsLocked` in `store/mvcc_store.go` (and the
  Pebble equivalent in `store/lsm_store.go`) rejects a transaction if any
  **write key or read key** has a committed version newer than `StartTS`.
  This runs under the store's apply lock from `ApplyMutations`, so validation
  and commit are atomic. With the read-set half, two transactions that each
  read what the other writes cannot both commit: the second to apply sees the
  first's version on its read key. That is the property that excludes write
  skew.
- **Single-shard transactions.** `handleOnePhaseTxnRequest` in `kv/fsm.go`
  applies the Raft entry that carries `ReadKeys`; the doc comment states that
  read-write conflicts are validated atomically with the commit.
- **Multi-shard transactions, write shards.** `prewriteTxn` in
  `kv/sharded_coordinator.go` attaches each shard's read keys to that shard's
  PREPARE entry, and `handlePrepareRequest` validates them under the apply
  lock at PREPARE apply. Nothing validates them again: the COMMIT entry
  built by `commitPrimaryTxn` carries no read keys and
  `handleCommitRequest` passes `nil` read keys to the store. Locks and
  intents written at PREPARE (`prepareTxnMutation`,
  `assertNoConflictingTxnLock`) cover **write** keys only. The
  `store/store.go` interface comment's "no TOCTOU window; full SSI" describes
  the PREPARE apply step, not the window between PREPARE and COMMIT (G1).
- **Multi-shard transactions, read-only shards.** `validateReadOnlyShards`
  issues a linearizable read barrier on each shard that holds read keys but no
  mutations, then compares `LatestCommitTS` per key against `StartTS`
  **outside** the apply lock. `LatestCommitTS` reads applied state, so a
  write that has applied by the time of the check is detected; what can be
  missed is a write that commits after the barrier and has not yet applied
  when the check runs. The code comment names the gap and its remedy: "full
  SSI for read-only shards … would require a dedicated 'read-validate' FSM
  request phase". These read keys are never part of any Raft entry.
- **Where read timestamps come from.** A transaction's `StartTS` and a
  plain read's snapshot are the store's applied watermark, `LastCommitTS()`,
  never `HLC.Next()`: `snapshotTS` in `adapter/ts.go`, `txnStartTS` in
  `adapter/redis_txn.go`, `nextTxnReadTS` in `adapter/dynamodb_locks.go`,
  and `BeginReadTimestampThrough` in `kv/tso.go`, which deliberately discards
  a fresh allocation because it "could run ahead of data-group apply". The
  comments justify this with the invariant "every version with commitTS ≤
  `LastCommitTS()` is visible", which holds only if entries apply in
  commit-timestamp order.
- **Where commit timestamps come from.** `resolveDispatchCommitTS` in
  `kv/coordinator.go` (and `settleTxnCommitTimestamp` in
  `kv/sharded_coordinator.go`) allocates `commitTS` from the HLC or the TSO
  **before** the entry is proposed, and the value travels inside the entry;
  `transactionManager.Commit` → `commitSequential` → `Propose` holds no lock
  from allocation to proposal. For 2PC the commit timestamp is allocated
  before PREPARE and carried by the later COMMIT entry. At apply,
  `alignCommitTS` (`store/mvcc_store.go`, `store/lsm_store.go`) and
  `stageLastCommitTSInBatch` only raise the watermark to
  `max(watermark, commitTS)`; no apply path rejects an entry whose
  `commitTS` is at or below the watermark. The one exception is the backup
  timestamp floor (`verifyBackupTimestampFloor` in `kv/fsm_backup.go`),
  which `2026_04_29_implemented_logical_backup.md` introduced precisely to
  "close the separate case where a writer already obtained a lower timestamp
  but has not reached Raft apply yet", and which applies only while a backup
  pin is active.
- **Which adapters populate `ReadKeys`.**
  - DynamoDB: only `TransactWriteItems` (`adapter/dynamodb_transact.go`,
    `plan.readKeys`). `ConditionCheck` on an existing item also re-writes the
    item (write set); on a missing item it relies on the read set alone,
    deliberately (a tombstone would shadow a concurrent higher-ts put).
    Single-item `PutItem` / `UpdateItem` / `DeleteItem` with a
    `ConditionExpression` validate the condition pre-Raft against the item
    read at `readTS`; the item itself is in the write set, so a concurrent
    write to that item is caught as a write-write conflict. DynamoDB
    conditions can only reference the item being written, so this is
    sufficient.
  - Redis: MULTI/EXEC tracks every key read inside the body
    (`trackReadKey`, `trackTypeReadKeys` in `adapter/redis_txn.go`); collection
    commands add type "fence" keys (`redisTxnWideCreateReadKeys`, and the
    list / hash / set / zset equivalents). Read-modify-write single commands
    (`INCR`, `LPUSH`, `HSET`, …) put the key they modify in the write set, so
    lost updates are caught. Lua (`adapter/redis_lua_context.go`,
    `luaWideFenceReadKeysForPlan`) records collection fence keys only; a
    script that reads a **string** value records no read key for it. `WATCH`
    is not implemented.
  - SQS: every write path records queue meta / generation fence keys and,
    for message operations, the data and visibility keys
    (`adapter/sqs_messages.go`, `sqs_fifo.go`, `sqs_catalog.go`, …).
  - S3: `ReadKeys` is set only in the upload-part path
    (`adapter/s3_upload_part.go`, the upload meta key). Every other handler
    reads at `readTS` and dispatches with `StartTS` set but no read keys.
- **Jepsen.** `redis_workload.clj`, `dynamodb_workload.clj`, and
  `dynamodb_multi_table_workload.clj` run Elle list-append under
  `:consistency-models [:strict-serializable]`, which would report G2-item.
  Every micro-op in list-append is a read of, or an append to, the same key,
  so every anti-dependency is accompanied by a write-write dependency on that
  key. The suite therefore passes because of write-write detection alone and
  cannot exercise read-set validation.
- **TLA+.** `tla/occ/OCC.tla` records `readObs` in `ReadKey` but neither
  `Prepare` nor `Commit` inspects it; OCC-2 quantifies over intersecting
  write sets only. The model describes snapshot isolation, not the code.

## 3. Gaps

| ID | Gap | Consequence if left open |
|---|---|---|
| G0 | **Apply order is not commit-timestamp order.** Commit timestamps are allocated before proposal with no lock to the proposal, so two writers can propose in the opposite order of their timestamps; the apply path never rejects an entry whose `commitTS` is at or below the watermark; reads take the watermark as their snapshot. Affects RAW, one-phase, and 2PC on every adapter. Established by code reading (§2); not yet reproduced. | Writer W1 allocates `c1`, W2 allocates `c2 > c1`, W2 proposes and applies first, the watermark becomes `c2`; T reads key `k` at `s = c2` and misses W1's version; W1 applies at `c1 < s`; T's validation (`TS > s`) sees nothing. If T also writes `k`, W1's write is lost; if T read `k` and `j` from W1 across W1's apply, T saw a torn snapshot. This is the same hazard the backup floor closes for backups. |
| G1 | 2PC read keys are unprotected between PREPARE and COMMIT on every shard: write shards validate them once at PREPARE apply, locks cover write keys only, and COMMIT does not re-validate; read-only shards are additionally validated outside the apply lock with read keys in no Raft entry | T reads `k` and writes `x` (one-phase); W reads `x` and writes `k` (2PC). W's PREPARE validates `x` before T applies; T applies (no committed version of `k` from W exists yet, and W's lock on `k` is not checked against T's read keys); W's COMMIT applies without re-validation. Both commit: write skew. On read-only shards the same window exists plus the barrier-to-check gap. |
| G2 | S3 handlers do not surface reads. Read-then-write sites (function names in `adapter/s3.go`, `s3_admin.go`, `s3_admin_objects.go`): `createBucket`, `deleteBucket`, `putBucketAcl`, `putObject`, `deleteObject`, `createMultipartUpload`, `uploadPart` (partly covered), `completeMultipartUpload`, `abortMultipartUpload`, and the admin equivalents. `If-Match` / `If-None-Match` are checked pre-Raft only (`validateS3PutPreconditions`). | Concrete anomaly: `deleteBucket` scans for emptiness at `readTS` while a concurrent `putObject` reads the bucket meta at its own `readTS`; both commit, leaving an object in a deleted bucket. Object-level races are mostly covered because the object head / manifest key is in the write set. |
| G3 | Lua scripts record no read key for string values | A script that reads string `a` and writes string `b`, racing with one that reads `b` and writes `a`, can produce write skew. |
| G4 | SQS fence-key coverage is asserted per call site, not audited as a table | Unknown; needs the same table as G2. |
| G5 | `tla/occ/OCC.tla` models SI | The model cannot catch a regression that removes read-set validation. |
| G6 | Docs: README's consistency bullet, `docs/architecture_overview.md` (no transaction section), `docs/review_todo.md` 4.4 ("or Del" is stale: the missing-item branch writes nothing) | Readers cannot tell what is guaranteed. |

## 4. Milestones

### A0. Apply-order fence (G0)

Reproduce first, per `CLAUDE.md`: a coordinator-level test with a hook that
delays one dispatch between `resolveDispatchCommitTS` and `Propose` while a
second dispatch with a later timestamp goes through, and a reader that takes
`StartTS` from the watermark in between; the test asserts the lost update.
A store-level test documents the underlying semantics (applying `c1` after
`c2 > c1` leaves a read at `c2` blind to `c1`). Both are expected to fail
on `main` today; if the coordinator-level test cannot be made to fail, G0 is
downgraded to "theoretical" and this milestone becomes documentation only.

Fix: generalise the backup timestamp floor into a permanent apply-order
fence. At FSM apply, a RAW or one-phase entry whose `commitTS` is at or below
the store's `LastCommitTS()` is rejected with `ErrWriteConflict` semantics
(the client already retries write conflicts with a fresh timestamp; the
one-phase dedup path keeps working because the rejected attempt's
`commitTS` becomes the retry's `PrevCommitTS`). For 2PC, the commit
timestamp is allocated **after** all PREPAREs succeed, immediately before
the primary COMMIT is proposed, and the primary COMMIT apply is fenced the
same way (rejection aborts the transaction before its commit point);
secondary COMMITs are not fenced because their intents are already visible
as locks to any reader at or above `StartTS`, and readers resolve them. The
fence makes "every version with `commitTS ≤ LastCommitTS()` is applied" a
guaranteed invariant instead of an assumption, which is what the read
timestamp path relies on. An alternative that avoids rejections, serialising
allocation and proposal under one lock per group, does not cover timestamps
allocated on other nodes (Internal.Forward) or handed out by the TSO batch
allocator to concurrent coordinators, so it is at most a complement.

### A1. Coverage table and adapter fixes

Deliverable: a table in this doc (promoted to `partial` when it lands) with
one row per read-then-write operation per adapter: keys read, keys written,
whether the read is protected by the write set, by `ReadKeys`, or by nothing.
Then:

- S3: populate `ReadKeys` with the bucket meta key on every object and
  multipart handler, the object head / manifest key on conditional puts and
  deletes, and the scanned range's fence on `deleteBucket` (or a bucket
  generation key, which `createBucket` already writes). `If-Match` /
  `If-None-Match` become read-set entries so the precondition is re-validated
  at apply.
- Lua: extend `luaWideFenceReadKeysForPlan` so string reads record the
  string key, capped by `kv.maxReadKeys`; scripts over the cap fail closed.
- SQS and DynamoDB: no code change expected; the table is the deliverable.

Each fix lands with a failing test first (per `CLAUDE.md`): a unit test that
drives the two-transaction interleaving through the coordinator and asserts
`ErrWriteConflict`.

### A2. Read locks at PREPARE (G1)

Analysis outcome: PREPARE-time validation is **not** sufficient. Between
PREPARE apply and COMMIT apply nothing protects a 2PC transaction's read
keys, on write shards or read-only shards, and re-validating at COMMIT apply
would not close it either, because the commit point cannot be atomic across
shards. The sound design is the classic one for serializable OCC over 2PC:
PREPARE installs a **read lock** record for every read key on that shard
(a marker keyed like `txnLockKey`, carrying `StartTS` and the primary key,
and shared rather than exclusive), and a writer's apply-time
`assertNoConflictingTxnLock` treats a foreign read lock like a foreign write
lock. COMMIT, ABORT, and the `LockResolver` clear read locks exactly as they
clear write locks. Read-only shards receive a PREPARE with an empty mutation
list and only read locks, which replaces `validateReadOnlyShards` and the
earlier idea of a separate `TXN_READ_VALIDATE` phase with a path that
already exists. Cost: one Raft entry per read-only shard per multi-shard
transaction (already paid by write shards) and one more lock row per read
key; writers to a read-locked key abort and retry instead of racing.

### A3. TLA+

Add to `tla/occ/OCC.tla`:

- Split `Commit(t)` into `Allocate(t)` and `Apply(t)` with a reorderable
  proposal queue between them (today one atomic step, so the model cannot
  express G0), with the A0 fence as `Apply`'s precondition; a gap
  configuration without the fence must fail OCC-3 / MVCC-4.
- `Prepare(t)` requires, for every key
  `k` the transaction read, that no version of `k` has `commitTs > startTs[t]`
  unless it was written by `t`.
- Property `OCC6_NoWriteSkew`: for any two committed transactions `t1`, `t2`
  with `t1` reading a key `t2` writes and `t2` reading a key `t1` writes,
  their commit order is consistent with at least one of them having seen the
  other's write (equivalently: no cycle of two rw anti-dependencies).
- A gap configuration (`MCOCC_gap_readset.cfg`) that disables the new
  precondition and must fail on `OCC6_NoWriteSkew`, wired into
  `scripts/tla-check.sh` like the existing gap checks.

### A4. Jepsen write-skew workloads

One workload per surface, all using Elle's rw-register model
(`jepsen.tests.cycle.wr`) under `:strict-serializable`, which reports
G2-item, with transactions of the shape "read `a`, write `b`" so
anti-dependencies are not accompanied by write-write edges:

- Redis: `MULTI; GET a; SET b; EXEC` (reads inside the body are tracked).
- Lua: `EVAL` script that reads `a` and writes `b`. Expected to **fail**
  before the A1 Lua fix and pass after; the failing run is committed as the
  regression evidence.
- DynamoDB: `TransactGetItems` for the reads, then `TransactWriteItems` with a
  `ConditionCheck` on each read item and a `Put` on the written item.
- gRPC TransactionalKV: there is no Jepsen client for gRPC today. Options are
  a Clojure gRPC client in `jepsen/` or a Go concurrency test in `kv/` that
  drives the same interleavings; see open question 2.
- Multi-shard variant for each: keys spread across at least two Raft groups so
  G1's path is exercised (Jepsen M5 already runs multi-group locally).

CI: added to `.github/workflows/jepsen-test.yml` next to the existing
workloads with the same `--local` topology.

### A5. Documentation

- README: replace the "Basic Consistency Behaviors" bullet with the three
  claims from the positioning doc §5 and the lease caveat.
- `docs/architecture_overview.md`: add a transactions section (one-phase, 2PC,
  read-validate, conflict predicate).
- `docs/review_todo.md`: 4.2 already points here; fix 4.4's stale "or Del".
- `CLAUDE.md` Conventions: one line stating that any adapter path that reads
  before it writes must surface the read keys, and that the coverage table in
  this doc is the checklist reviewers use.

Order: A0 reproduction → A0 fence → A1 (table first, then S3, then Lua) →
A2 → A3 → A4 → A5. A0's reproduction test is the first thing to run because
its outcome decides whether any path can be called serializable today; A3
and A4 are pure evidence and can move ahead of A2 if the read-lock work
proves larger than expected.

## 5. Evidence when complete

- The A0 reproduction test is red before the fence and green after; the
  same PR carries both.
- The coverage table has no row protected by "nothing".
- `go test -race ./kv/... ./adapter/... ./store/...` includes the new
  interleaving tests.
- `make tla-check` passes with `OCC6_NoWriteSkew` and fails the new gap
  config as expected.
- Every A4 workload is green in CI; the Lua workload's pre-fix red run is
  linked from the PR.

## 6. Risks

- **Read-set size.** Surfacing S3 bucket meta on every object write adds one
  key per transaction; `deleteBucket` on a large bucket needs a fence key
  rather than every object key (cap: `kv.maxReadKeys` = 10 000).
- **Conflict rate.** Bucket meta as a read key makes every object write in a
  bucket conflict with a concurrent `putBucketAcl` on that bucket. Acceptable:
  ACL changes are rare; measured in the A1 PR.
- **Fence rejections.** Under proposal reordering the fence turns a silent
  anomaly into a retried write conflict; the rate is exported as a new
  write-conflict kind and measured in the benchmark milestone. Allocating
  2PC commit timestamps at commit time adds one TSO or HLC call per
  multi-shard transaction.
- **Latency.** A2 adds one PREPARE entry per read-only shard and one lock
  row per read key. Measured in the benchmark milestone, which runs after
  this audit for that reason.

## 7. Decisions

- Serializable, not strict serializable, is the claim (positioning doc §5).
- Keep the Jepsen strict-serializable checkers as they are; add rw-register
  workloads rather than downgrading the model.
- The apply-order fence is permanent and unconditional; the backup floor
  stays as it is. 2PC commit timestamps move to commit time.
- Replace `validateReadOnlyShards` with read locks at PREPARE rather than
  keeping it as a fast path or adding a separate validate phase.
- Fixes land test-first, one adapter per PR.

## 8. Open questions

1. Whether the A0 reproduction can be driven by scheduling alone or needs a
   test-only delay hook between allocation and proposal; the hook is
   acceptable if it never ships in the binary.
2. gRPC write-skew evidence: Clojure gRPC client in `jepsen/` or a Go
   concurrency test. The former matches "Jepsen for every adapter"; the latter
   is a day of work instead of a week.
3. Whether `deleteBucket` should take a bucket generation read key
   (cheap, coarse) or a range fence (precise, new mechanism).
4. Read-lock representation: a separate key prefix or a flag on the existing
   lock record; readers of a read-locked key must not wait (read locks block
   writers only).
