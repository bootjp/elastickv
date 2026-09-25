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
surfaced to that validation, (b) closes the one engine-level hole, (c) makes
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
  PREPARE entry; the `store/store.go` interface comment records this as "no
  TOCTOU window; full SSI".
- **Multi-shard transactions, read-only shards.** `validateReadOnlyShards`
  issues a linearizable read barrier on each shard that holds read keys but no
  mutations, then compares `LatestCommitTS` per key against `StartTS`
  **outside** the apply lock. Its comment names the gap: a write committing
  between the barrier and the check goes undetected, and "full SSI for
  read-only shards … would require a dedicated 'read-validate' FSM request
  phase". These read keys are never part of any Raft entry.
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
| G1 | 2PC read-only shards validated outside the apply lock; read keys not in a Raft entry | A write to a read-only shard that commits between the barrier and the `LatestCommitTS` check is missed: write skew across shards. |
| G2 | S3 handlers do not surface reads. Read-then-write sites (function names in `adapter/s3.go`, `s3_admin.go`, `s3_admin_objects.go`): `createBucket`, `deleteBucket`, `putBucketAcl`, `putObject`, `deleteObject`, `createMultipartUpload`, `uploadPart` (partly covered), `completeMultipartUpload`, `abortMultipartUpload`, and the admin equivalents. `If-Match` / `If-None-Match` are checked pre-Raft only (`validateS3PutPreconditions`). | Concrete anomaly: `deleteBucket` scans for emptiness at `readTS` while a concurrent `putObject` reads the bucket meta at its own `readTS`; both commit, leaving an object in a deleted bucket. Object-level races are mostly covered because the object head / manifest key is in the write set. |
| G3 | Lua scripts record no read key for string values | A script that reads string `a` and writes string `b`, racing with one that reads `b` and writes `a`, can produce write skew. |
| G4 | SQS fence-key coverage is asserted per call site, not audited as a table | Unknown; needs the same table as G2. |
| G5 | `tla/occ/OCC.tla` models SI | The model cannot catch a regression that removes read-set validation. |
| G6 | Docs: README's consistency bullet, `docs/architecture_overview.md` (no transaction section), `docs/review_todo.md` 4.4 ("or Del" is stale: the missing-item branch writes nothing) | Readers cannot tell what is guaranteed. |

## 4. Milestones

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

### A2. Read-validate phase for 2PC read-only shards

First, an analysis note in this doc on the PREPARE-to-COMMIT window: write
shards validate read keys at PREPARE apply, before the primary commit;
whether a write that applies on that shard after PREPARE but with a smaller
commit timestamp is possible depends on commit-timestamp alignment in the
store (`alignCommitTS`) and on TSO monotonicity. The analysis decides whether
"validate at PREPARE apply" is sufficient or whether validation must be
re-run at COMMIT apply.

Then the mechanism: a new FSM request (working name `TXN_READ_VALIDATE`)
carrying `StartTS` and the shard's read keys, proposed to each read-only
shard and applied under the apply lock with the same predicate as
`checkConflictsLocked`. A conflict aborts the transaction before
`commitPrimary`. `validateReadOnlyShards` is replaced, not kept as a fast
path, so there is one code path to reason about. Cost: one extra Raft round
trip per read-only shard per multi-shard transaction, paid only by
transactions that already pay for 2PC.

### A3. TLA+

Add to `tla/occ/OCC.tla`:

- `Prepare(t)` (or `Commit(t)`, per the A2 analysis) requires, for every key
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

Order: A1 (table first, then S3, then Lua) → A2 analysis → A3 → A4 → A2
mechanism → A5. A3 and A4 are pure evidence and can be reordered ahead of
A2's mechanism if that proves larger than expected.

## 5. Evidence when complete

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
- **Latency.** A2 adds a Raft round trip for multi-shard transactions with
  read-only shards. Measured in the benchmark milestone, which runs after
  this audit for that reason.

## 7. Decisions

- Serializable, not strict serializable, is the claim (positioning doc §5).
- Keep the Jepsen strict-serializable checkers as they are; add rw-register
  workloads rather than downgrading the model.
- Replace `validateReadOnlyShards` rather than keeping it as a fast path.
- Fixes land test-first, one adapter per PR.

## 8. Open questions

1. A2 analysis outcome: validate at PREPARE apply, at COMMIT apply, or both.
2. gRPC write-skew evidence: Clojure gRPC client in `jepsen/` or a Go
   concurrency test. The former matches "Jepsen for every adapter"; the latter
   is a day of work instead of a week.
3. Whether `deleteBucket` should take a bucket generation read key
   (cheap, coarse) or a range fence (precise, new mechanism).
