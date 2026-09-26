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
| G0 | **Apply order is not commit-timestamp order.** Commit timestamps are allocated before proposal with no lock to the proposal, so two writers can propose in the opposite order of their timestamps; the apply path never rejects an entry whose `commitTS` is at or below the watermark; reads take the watermark as their snapshot. Affects RAW, one-phase, and 2PC on every adapter. **Reproduced**: `TestApplyOrder_SnapshotAtWatermarkIsStableUnderProposalReordering` in `kv/apply_order_repro_test.go` (branch `design/serializable-audit-a0`) fails on `main` in 10 of 10 runs under `-race`, using a parking proposer that holds one proposal while a later-timestamped one applies; a read at the watermark returns nothing, then returns the parked write after it applies. | Writer W1 allocates `c1`, W2 allocates `c2 > c1`, W2 proposes and applies first, the watermark becomes `c2`; T reads key `k` at `s = c2` and misses W1's version; W1 applies at `c1 < s`; T's validation (`TS > s`) sees nothing. If T also writes `k`, W1's write is lost; if T read `k` and `j` from W1 across W1's apply, T saw a torn snapshot. This is the same hazard the backup floor closes for backups. |
| G1 | 2PC read keys are unprotected between PREPARE and COMMIT on every shard: write shards validate them once at PREPARE apply, locks cover write keys only, and COMMIT does not re-validate; read-only shards are additionally validated outside the apply lock with read keys in no Raft entry | T reads `k` and writes `x` (one-phase); W reads `x` and writes `k` (2PC). W's PREPARE validates `x` before T applies; T applies (no committed version of `k` from W exists yet, and W's lock on `k` is not checked against T's read keys); W's COMMIT applies without re-validation. Both commit: write skew. On read-only shards the same window exists plus the barrier-to-check gap. **Reproduced**: `TestTwoPhaseCommit_ReadKeysUnprotectedBetweenPrepareAndCommit` in `kv/txn_read_key_window_repro_test.go` (branch `design/serializable-audit-a0`) drives exactly this interleaving on a two-group harness and fails on `main` in 3 of 3 runs under `-race`: T's one-phase entry applies after W's PREPAREs and before W's parked primary COMMIT, both dispatches return committed, and the final state is `k="w"`, `x="t"`. |
| G2 | S3 handlers rely on the write set alone (no `ReadKeys` except the upload-part meta key). Most paths are covered because the key they read is also the key they write, and object writers `Put` the bucket meta as a fence that `deleteBucket` deletes (Appendix A). The uncovered reads: `completeMultipartUpload` uses the part descriptors it loaded at its first `readTS` and never re-reads them at the commit snapshot; `uploadPart` reads the previous part at an older `readTS` than its `StartTS` and reads the bucket meta with no protection. `If-Match` / `If-None-Match` are checked pre-Raft only (`validateS3PutPreconditions`), on the head key that the write set covers. | `completeMultipartUpload` racing a re-upload of one part commits a manifest pointing at the superseded part version whose blobs the re-upload deletes asynchronously: an object with deleted chunks. `uploadPart` after a concurrent `deleteBucket` lands parts under a dead generation (leak); two uploads of the same part in the window leak blobs and double-clean the stale one. |
| G3 | Lua scripts record read keys only for stream reads and for the keys they write (fences); reads of any other key (`GET`, `HGET`, `LRANGE`, `SMEMBERS`, `ZRANGE`, …) are not surfaced (`adapter/redis_lua_context.go`) | `v = GET a; SET b v` commits over a concurrent `SET a`; two such scripts crossing `a` and `b` produce write skew. **Reproduced**: `TestLua_ReadOfUnwrittenKeyIsNotValidated` in `adapter/redis_lua_read_skew_repro_test.go` (branch `design/serializable-audit-a0`) holds the script after its `GET a` returned, lets `SET a x` commit below the script's commit timestamp, and observes `b = "none"` with `a` absent from the script's read set. |
| G4 | SQS: audited (Appendix A). Every message and catalog path carries the keys it reads plus the queue meta and generation keys. Two benign `N` rows remain: the DLQ existence check at `CreateQueue` / `SetQueueAttributes` (a policy can point at a deleted DLQ; redrive re-checks) and the reaper on the old-generation keyspace. | None beyond the two benign rows; closed by documentation. |
| G7 | Redis paths that bypass the collection fences (Appendix A): the emptying paths through `deleteLogicalKeyElems` (standalone `DEL`, `GETDEL`, `EXPIRE ≤ 0`, `LTRIM` to empty, a zset / set / HLL emptied to zero) neither read nor bump a fence; `HDEL` and update-only `HSET` do not bump the hash fence; `SETNX` and the legacy (dedup-off) `SET` / `INCR` / `HSET` creates read the key type with no fence; `MULTI` type probes for `GET` / `EXISTS` / `EXPIRE` on an absent key record no fence. | `DEL k` racing `RPUSH k v` where the push commits after `DEL`'s snapshot: `DEL` removes the base meta and the items it saw, the push's item and delta survive, the list has a hole. `SETNX k` racing `RPUSH k`: both commit, a string and a list coexist under one key. `MULTI { EXISTS k; SET a 1 }` misses a concurrent create of `k`. **Reproduced** for the `DEL` case: `TestDel_ListEmptyingDoesNotFenceConcurrentPush` in `adapter/redis_del_rpush_repro_test.go` (same branch) holds `DEL` after its item scan, lets `RPUSH v2` commit, and ends with `LLEN = 1`, `LRANGE = []`: the metadata points at the item `DEL` removed while the pushed item survives. |
| G8 | gRPC `TransactionalKV` has no transaction surface: there is no `Begin`, `PreWrite` / `Commit` / `Rollback` return not-implemented, and every `Put` / `Delete` is a single-key transaction with a coordinator-assigned `StartTS` and no read set (`adapter/grpc.go`, `adapter/grpc_transcoder.go`). | A gRPC client's `Get` followed by `Put` can lose an update; multi-key serializable transactions are reachable today only through Redis `MULTI`, DynamoDB `TransactWriteItems`, SQS, and the filesystem. An API gap rather than a validation bug; documented as such and out of this audit's scope. |
| G9 | DynamoDB `finalizeLegacyTableMigration` writes `TableMeta` from an earlier snapshot under a process-local lock only; table-generation checks for item writes are compensated after commit rather than validated at apply (`adapter/dynamodb_migration.go`, `adapter/dynamodb_transact.go`). | A stale schema can overwrite a concurrent schema change during legacy migration; low likelihood, migration-only. |
| G10 | **Single-key lost update: non-leader nodes issue commit timestamps, and the stale-reapply fast path treats an equal timestamp as "already applied".** In legacy TSO mode (the default) `ShardedCoordinator.Dispatch` picks both `StartTS` and `CommitTS` on whichever node runs it (`nextStartTS`, `settleTxnCommitTimestamp` → `nextFencedWithRecovery` on the local HLC; raw writes through `rawLogTimestamp`), then `LeaderProxy.Commit` forwards the pre-stamped request; `Internal.Forward` keeps a nonzero `CommitTS` and its validators are no-ops outside Phase D. The leader-only issuance in `Coordinate.dispatchOnce` applies only to the non-sharded coordinator, which only the demo uses. Concretely: `EVAL` / `EVALSHA` are not proxied to the leader (`adapter/redis_lua.go`, `redis_command_specs.go`); a follower runs the script, allocates `commitTS` from its own HLC (`redis_lua_context.go` → `kv.NextTimestampAfterThrough` → `nextFencedWithRecovery`), stamps it into the request, and `Internal.Forward` keeps a preset `commitTS` (`adapter/internal.go`). The physical half is the Raft-applied ceiling, identical on every node, and the logical half advances only through local `Next()` and FSM `Observe`, so two followers that applied the same log hand out the same next timestamp. This violates the `CLAUDE.md` invariant that followers never issue persistence timestamps. Then `staleRaftApplyFastPathLocked` / `raftApplyAlreadyLandedLocked` in `store/lsm_store.go` (added for crash replay) declare an entry already applied when every mutation key has a version at exactly `commitTS`, and return before `checkApplyConflicts`; the second transaction becomes a silent no-op and its client gets OK. **Reproduced** deterministically: `TestLua_TwoFollowersLoseUpdateOnCommitTSCollision` and `TestLua_LostUpdate_StoreTreatsCommitTSCollisionAsReplay` in `adapter/redis_lua_lost_update_repro_test.go` (branch `design/serializable-audit-g10`), a three-node harness where scripts on two followers read the same value and both return OK while one write vanishes; 5 of 5 under `-race`. `dedupProbeOnePhase` in `kv/fsm.go` (`CommittedVersionAt(primary, PrevCommitTS)`) rests on the same timestamp-as-identity assumption. Blast radius (audited): besides `EVAL` / `EVALSHA`, gRPC `RawKV` `RawPut` / `RawDelete` and `TransactionalKV` `Put` / `Delete` are served on every node with no leader check and stamp locally; the FUSE filesystem service (`internal/filesystem/service.go` `dispatchTxn`, its lease reaper, and startup intent recovery) runs on any mounting node with no leader check; asynchronous cleanups (S3 manifest / part cleanup, DynamoDB deleted-table `DEL_PREFIX`) keep running after a leader change; and in multi-group deployments the default-group leader stamps `CommitTS` for groups it does not lead (2PC, cross-group Redis commands, `FLUSHALL`). Safe because the raw command reaches the leader before any timestamp is chosen: every other Redis write (`proxyToLeader` re-executes on the leader), `MULTI` / `EXEC`, DynamoDB and SQS (HTTP proxy to the default-group leader), S3 (per-route-key verified proxy), `SplitRange`, the lock resolver, and the leader-gated Redis background loops. `Internal.Forward` never `Observe`s a forwarded timestamp, so a follower's stamp can also collide with one the leader issues next. The comment in `adapter/dynamodb_item_write.go` claiming the leader allocates `commit_ts` is true only for `Coordinate`. | A plain read-modify-write through `EVAL` on two nodes can lose one write with both clients told OK. Independent of the multi-key claim; it breaks the single-key guarantee. |
| G11 | **An indeterminate outcome is reported as a definite failure.** When a leader loses leadership while a proposal is pending, `refreshStatus` in `internal/raftengine/etcd/engine.go` fails the pending proposal with `errNotLeader`; the Redis adapter maps it to `NOTLEADER` (`writeRedisError`), and the entry may still commit under the new leader. Found by the A4 pause nemesis: Jepsen recorded the writes as `:fail`, then saw their values read, and reported G1a (aborted read) in two Lua runs; reclassifying `NOTLEADER` as indeterminate makes both histories valid. | A client that treats `NOTLEADER` as "not applied" and retries can apply a non-idempotent command twice. Fix: once a proposal has been handed to Raft, leadership loss must surface as an "outcome unknown" error distinct from `NOTLEADER` (and the Jepsen clients must record it as `:info`); the Redis, DynamoDB, S3, SQS, and gRPC error mappings each need the distinction. |
| G5 | `tla/occ/OCC.tla` models SI | The model cannot catch a regression that removes read-set validation. |
| G6 | Docs: README's consistency bullet, `docs/architecture_overview.md` (no transaction section), `docs/review_todo.md` 4.4 ("or Del" is stale: the missing-item branch writes nothing) | Readers cannot tell what is guaranteed. |
| G12 | **Transaction identity is `(primaryKey, StartTS)`, which is not unique.** Locks (`txnLock`), the commit and rollback records (`txnCommitKey`, `txnRollbackKey`), the ownership test in `handlePrepareRequest` (`lock.StartTS == startTS && PrimaryKey equal`), and the A2 read-lock rows all identify a transaction by its primary key and start timestamp. Adapter transactions supply `StartTS = readTS`, the shared read snapshot, so two concurrent 2PC transactions with the same primary key and the same snapshot alias each other: the second PREPARE passes the ownership check as a retry of the first and overwrites its intent; the first COMMIT then publishes the second's value under the first's identity, and the second's COMMIT finds no lock and reports success. A2's read-lock rows collide the same way (one transaction's COMMIT deletes the other's protection). Found in review; not yet reproduced (a two-group reproduction with two transactions sharing primary key and `StartTS` is the first A2 deliverable). | A cross-shard transaction can commit a mix of two transactions' writes (atomicity), and A2's read locks do not protect a transaction that shares identity with another. |

## 4. Milestones

### A0. Apply-order fence (G0)

Reproduce first, per `CLAUDE.md`: a coordinator-level test with a hook that
delays one dispatch between `resolveDispatchCommitTS` and `Propose` while a
second dispatch with a later timestamp goes through, and a reader that takes
`StartTS` from the watermark in between; the test asserts the lost update.
A store-level test documents the underlying semantics (applying `c1` after
`c2 > c1` leaves a read at `c2` blind to `c1`). Status: both tests exist in
`kv/apply_order_repro_test.go` on branch `design/serializable-audit-a0`. The
coordinator-level test fails deterministically on `main` (no production
hook was needed: the parking sits in a test-only `raftengine.Proposer`
wrapper around the real single-node engine, and `commitSequential` does not
serialise one-phase proposals); the store-level test passes. The fix PR
carries both tests, per the convention in `CLAUDE.md`.

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

Status of A0: implemented ahead of review on local branch
`design/serializable-audit-a0-fence` (six commits on top of the reproduction
branch, not pushed). What landed: replay detection by Raft index
(`raftApplyReplayLocked`: an index below the persisted `metaAppliedIndex` is a
pure no-op; at the persisted index the entry is a replay only if every key
already has a version at `commitTS`, because a raw batch carries several
requests under one index; anything above is live), the fence as
`MVCCStore.ApplyMutationsRaftAtFenced` (Pebble and in-memory stores,
forwarded by `ShardStore` and `LeaderRoutedStore`; runs under the apply lock
after replay detection and before the conflict checks; rejects with
`ErrCommitTSFenced`, which also satisfies `ErrWriteConflict`; counted as the
`commit_ts_fence` conflict kind), requested only by `handleRawRequest` and
`handleOnePhaseTxnRequest`. Two additions the fence forced: gRPC `RawKV`
writes are retried with a fresh timestamp when fenced (raw timestamps are
allocated before batching and the client has no retry), and raw batches are
proposed in timestamp order. Results: the G0 reproduction and both G10
reproductions pass (the parked or colliding entry is rejected and the client
sees a serial outcome), the G1 reproduction still fails as it must, `store`
and `adapter` are green under `-race`, `kv` fails only the G1 test, lint is
clean. Mutations and the applied index were already written in one Pebble
batch, so no new meta key was needed.

Constraints the fix PR must close before merge:

- **Replica determinism.** The fence compares against `LastCommitTS()`, which
  non-replicated direct writes also advance (the catalog bootstrap
  `CatalogStore.Save` writes straight to the FSM store at `LastCommitTS() + 1`).
  A replica that took such a write mid-replay would reject an entry the
  others accept. The fence must compare against a watermark that only
  replicated applies advance, or those writes must go through Raft.
- 2PC commit timestamps are still allocated before PREPARE, so the primary
  COMMIT is not fenced; moving the allocation after the PREPAREs and fencing
  the primary COMMIT is the remaining A0 item.
- `DEL_PREFIX` writes versions at its `commitTS` and is not fenced.
- The Lua path treats a forwarded write conflict as non-retryable, so a
  fenced `EVAL` surfaces the conflict to the client instead of retrying;
  acceptable (serial) but a behaviour change to document, and A0b's
  leader-side execution makes it moot.
- A possible pre-existing loss to verify: the engine's cold-start duplicate
  skip drops the whole entry at the durable index, so a crash between two
  requests of one raw batch could lose the second.
- Throughput under reordering is not measured; the benchmark milestone does.

### A0b. Commit-timestamp identity (G10)

Two fixes, neither weakening an existing check:

- **No non-leader-issued persistence timestamps.** `ShardedCoordinator.Dispatch`
  on a node that does not lead the target group forwards the request
  **unstamped** (`CommitTS = 0`, raw `Ts = 0`) and the leader allocates in
  `resolveDispatchCommitTS`, as `Coordinate.dispatchOnce` already does for
  the demo; `Internal.Forward` in legacy mode rejects a nonzero forwarded
  `CommitTS` instead of keeping it, so the invariant fails closed. Handlers
  whose keys embed the timestamp (Lua list / zset delta keys,
  `VersionedBlobKey` in S3 upload-part) must therefore run on the leader:
  `EVAL` / `EVALSHA` get the same `proxyToLeader` every other Redis write
  has; gRPC `RawKV` / `TransactionalKV` gain a leader check with forwarding;
  the filesystem service dispatches through the leader; the asynchronous
  cleanups stop when leadership is lost; multi-group stamping for foreign
  groups goes through each group's leader or the TSO. `CLAUDE.md`'s
  architecture note is corrected to say this is the intended invariant, not
  today's behaviour.
- **Harden the stale-reapply fast path.** "A version exists at
  `(key, commitTS)`" stops being proof that this entry was applied. The path
  compares against a unique identity (the entry's Raft index, or a unique
  transaction id persisted with the version; `(primary key, startTS)` is not
  one, because `startTS` is the watermark and many transactions share it;
  G12 and A2 give transactions a `TxnID` for exactly this reason) or
  is restricted to the crash-replay window; a live entry always runs
  `checkConflicts` and the read-set check. `dedupProbeOnePhase` is reviewed
  under the same rule.

A0 and A0b are one change at the store: the apply-order fence
(`commitTS > LastCommitTS()`) also rejects a second entry carrying an
already-applied timestamp, but only if it runs **before** the stale-reapply
fast path, which today short-circuits first. Replay detection therefore
moves to the Raft index (an entry at or below the applied index is a replay;
anything else runs the fence and the conflict checks). The reproduction
tests on `design/serializable-audit-g10` are red before and must be green
after; the fix PR carries them, plus tests for the cases the blast-radius
audit left open: two gRPC `RawPut`s to one key with equal `Ts`, the S3
`maybeProxyToLeader` fallback that serves locally when the route key cannot
be loaded, a deposed leader issuing at `physical = ceiling` next to the new
leader, and the dedup probe (`dedupProbeOnePhase`) matching another
transaction's version at `PrevCommitTS`.

Status of A0b, first slice: implemented ahead of review on local branch
`design/serializable-audit-a0b-leader-stamp` (five commits on top of the A2
branch, not pushed). What landed: `EVAL` / `EVALSHA` go through the same
leader proxy as every other Redis write (all keys led by one foreign node →
forwarded as `EVAL` with the cached body; a foreign key with an unknown
leader → fail closed; keys on several foreign leaders → still executed
locally, which the 2PC follow-up covers); `ShardedCoordinator.Dispatch` on a
node that does not lead the target group forwards single-shard one-phase,
raw, and `DEL_PREFIX` requests **unstamped** and the leader allocates
(`forwardSingleShardTxnUnstamped`, `rawLogTimestampForGroup`, per-group
`DEL_PREFIX` copies), a caller-supplied `CommitTS` on a non-leader is refused
with `ErrCommitTSNotLeaderIssued` (a `NOTLEADER`-class error), and
`leaseRefreshingTxn.Commit` stamps an unstamped one-phase request when the
node became leader between dispatch and commit; `Internal.Forward` rejects a
stamped raw or one-phase request in every TSO mode
(`ErrForwardedTimestampRejected`) while PREPARE / COMMIT / ABORT keep their
handling; the DynamoDB dedup path dispatches unstamped when the item's group
is led elsewhere and its false comment is rewritten. gRPC `RawKV` /
`TransactionalKV`, the filesystem, and the asynchronous cleanups needed no
change: none of them stamped before `Dispatch`. Results: all reproduction
tests (G0, G1, G3, G7, G10) pass; a follower-routed `RawPut` / `Put` test
shows leader-issued timestamps (it failed before the change); `store`, `kv`,
and `adapter` are green under `-race` (the adapter package needs
`-timeout 40m` on this machine because two pre-existing tests take about 8
minutes each); lint is clean.

Behaviour changes to review: a deposed leader that stamps and then fails
`VerifyLeader` now gets its forwarded request rejected instead of accepted;
in multi-group deployments, S3's local fallback when the route key cannot be
loaded and a keys-spanning Lua script whose writes all land in one foreign
group now fail closed instead of stamping locally.

Remaining for A0 / A0b: allocate the 2PC commit timestamp after all
PREPAREs succeed, from the primary group's leader or the TSO, fence the
primary COMMIT, and stop `Internal.Forward` from keeping a coordinator-stamped
COMMIT timestamp (cross-group Redis commands, `FLUSHALL`, spanning Lua
scripts, cross-group S3 / DynamoDB / SQS / filesystem transactions); make the
fence compare against a watermark that only replicated applies advance; fence
`DEL_PREFIX`; review `dedupProbeOnePhase`.

Status of A0, completion slice: implemented ahead of review on local branch
`design/serializable-audit-a0-complete` (four commits on top of the A0b
branch, not pushed). What landed:

- **Replicated-only watermark.** A second persisted meta value,
  `_meta_last_raft_commit_ts`, staged in the same Pebble batch as the write
  and advanced only by replicated applies (`ApplyMutationsRaft*`, the Raft
  `DeletePrefix*`, `ImportVersionsRaft`, `PromoteVersions`); direct writes
  (`ApplyMutations`, `PutAt`, `DeleteAt`, `ExpireAt`, `DeletePrefixAt`,
  `ImportVersions`) advance `LastCommitTS()` only. The fence compares against
  `LastRaftCommitTS()`; a store or snapshot without the key falls back to
  `LastCommitTS()`, capped. A two-replica test shows identical verdicts when
  one replica took a direct write at `watermark + 1` (it fails with the old
  fence). The only direct write on a running node is the catalog bootstrap
  `CatalogStore.Save`; its timestamp can equal a later replicated entry's,
  which then overwrites the seed on every replica (convergent); a local read
  at that timestamp before the entry applies, on an empty-catalog node at
  startup, is the one case left open and documented on
  `EnsureCatalogSnapshot`.
- **2PC commit timestamp after PREPARE.** `dispatchTxn` no longer allocates
  before `prewriteTxn`; the primary COMMIT is proposed unstamped and stamped
  by the primary group's leader (locally with `resolveTxnCommitTS`, or in
  `Internal.Forward`'s COMMIT branch, or by `leaseRefreshingTxn` if the node
  won leadership in between); the landed value comes back through
  `ForwardResponse.CommitTs` or the commit record and the secondaries are
  committed with it. The first apply of the primary COMMIT is fenced;
  secondary and repeated primary COMMITs are not. A hazard found on the way:
  a lost reply to an unstamped primary COMMIT makes `LeaderProxy` re-forward
  it, the leader stamps a second timestamp, and the transaction looks
  failed; `settleFailedPrimaryCommit` therefore aborts the primary group
  first and, if it reports "already committed", commits the secondaries at
  the recorded timestamp instead of rolling them back. **Exemption:**
  transactions whose keys embed the commit timestamp before PREPARE (Redis
  `MULTI` / `EXEC` and Lua delta keys, S3 `VersionedBlobKey`, any element
  with a `CommitTSValueOffset` patch) still pre-allocate, marked
  `txnCommitTSPreallocated`; their primary COMMIT is fenced too, so under
  contention they abort with a write conflict more often, and a
  multi-shard `MULTI` retry then meets `ErrTxnDedupRequiresSingleShard`.
- **`DEL_PREFIX` fenced** through a new `DeletePrefixesAtRaftAtFenced`; each
  prefix element now carries its own timestamp so a multi-prefix broadcast
  (the S3 bucket-delete safety net) does not fence itself.
- **Dedup probe.** `dedupProbeOnePhase` / `CommittedVersionAt` document why
  the probe is sound once timestamps are leader-issued, monotonic, and
  fenced; a test shows another transaction's entry at the attempt's exact
  timestamp is fenced.

Results: all reproduction tests (G0, G1, G3, G7, G10) and the A0b
leader-stamp tests pass; `store`, `kv`, `distribution`, `internal/filesystem`,
and `adapter` green under `-race` (one A0b test,
`TestGRPC_FollowerWritesCarryLeaderIssuedTimestamps`, is timing-dependent
under parallel suite load on both branches and needs its second case
decoupled from the first); lint clean.

Still open for the fix PRs:

- **Rolling upgrades.** An old FSM does not fence, so a mixed-version group
  can reach different verdicts on one entry; the fence needs a
  cluster-wide capability gate like the encryption envelopes, or a
  stop-the-cluster cutover.
- Behaviour changes to review: pre-allocated 2PC commits fenced under load;
  a pre-pin 2PC transaction whose primary COMMIT sits at or below a backup
  pin's read timestamp is now aborted, not committed; `DEL_PREFIX`
  (`FLUSHALL`, S3 and DynamoDB cleanups) can return a write conflict; 2PC
  route floors are checked against `startTS + 1`.
- `DEL_PREFIX` is fenced but still bypasses transaction locks (A2 blocker
  above).
- Not yet leader-issued: the 2PC ABORT timestamp (unfenced) and the
  pre-allocated paths above; making delta keys independent of the commit
  timestamp would remove the exemption.
- One more 8-byte meta key per replicated apply batch, not benchmarked; the
  streaming MVCC and in-memory snapshot formats do not carry the replicated
  watermark and fall back.

### A1. Coverage table and adapter fixes

Deliverable: the coverage matrix. Its initial version, produced by code
reading on 2026-09-26, is Appendix A; A1 re-verifies every row against the
code at fix time and turns the `N` rows into fixes:

- S3: populate `ReadKeys` with the bucket meta key on every object and
  multipart handler, the object head / manifest key on conditional puts and
  deletes, and the scanned range's fence on `deleteBucket` (or a bucket
  generation key, which `createBucket` already writes). `If-Match` /
  `If-None-Match` become read-set entries so the precondition is re-validated
  at apply.
- Lua: extend `luaWideFenceReadKeysForPlan` so string reads record the
  string key, capped by `kv.maxReadKeys`; scripts over the cap fail closed.
- Redis: bump the collection fence on every emptying path
  (`deleteLogicalKeyElems` callers, `HDEL`, update-only `HSET`) and read the
  fence keys in `SETNX`, the legacy creates, and the `MULTI` type probes for
  absent keys (G7). Two of the fixes are already known to be small: in a
  throwaway copy, adding `redisStrKey(key)` to the Lua context's read keys on
  a string read, and making `deleteListElems` `Put` the list fence, turned
  the G3 and G7 reproduction tests green (the first attempt conflicts, the
  retry sees the concurrent write).
- S3: re-read the part descriptors at the commit snapshot in
  `completeMultipartUpload`; in `uploadPart`, surface the bucket meta and
  **re-read the previous part at `StartTS`** (or validate it against the
  earlier `readTS` it was actually read at): merely listing it in
  `ReadKeys` does not help, because a commit between the old `readTS` and
  `StartTS` is not newer than `StartTS` and passes validation (G2, in
  addition to the bucket-meta read keys above).
- SQS: no code change; the matrix is the deliverable (G4). DynamoDB: none
  except the migration finaliser (G9): `finalizeLegacyTableMigration`
  receives the schema loaded at the earlier `legacyMigrationSnapshot`,
  migrates the items, checks emptiness, and only then allocates a fresh
  `readTS` for the final write, so a schema key in `ReadKeys` alone is
  insufficient for the same reason as `uploadPart`: a change committed
  between the snapshot and that `StartTS` is not newer than `StartTS`. The
  finaliser re-reads the schema at its `StartTS` (or validates the passed
  schema against the timestamp it was actually read at) and lists the
  schema key in `ReadKeys`.
- gRPC: none in this audit; the API gap (G8) is documented in the positioning
  doc and README.

Status of the Redis half: landed on branch `design/serializable-audit-a1-redis`
(commits `19c7cffa` "surface Lua string reads as OCC read keys" and
`0a044364` "bump collection fences on emptying paths"; not pushed), on top
of the reproduction tests. `stringState` records the string key and the bare
key on first load; `EXISTS` / `TYPE` / `PTTL` go through a `probedKeyType`
that records the string, HLL, and bare keys for a string and every type
anchor plus the four wide fences for an absent key; `deleteListElems` and
the hash / set / zset logical-delete helpers `Put` their type's fence, and
`deleteLogicalKeyElems` `Put`s all four. Both reproduction tests pass (the
first attempt now fails with a write conflict and the retry sees the
concurrent write), two table-driven tests pin the read sets and fence
`Put`s, `go test -race ./adapter/` is green, lint is clean. Decisions for the
fix PR: three call sites in `redis_txn.go` and `redis_collection_ttl.go`
now carry duplicate fence `Put`s (the FSM's `uniqueMutations` drops them;
removing the appends restores the old entry size); every logical delete,
including `DEL` of a string or a missing key, now writes four fences; the
TTL-inline migrator's expired-collection delete now conflicts with a
concurrent push and retries on its next pass; an absent-key probe adds 13
read keys and a `GET` adds 2, so a script with roughly 770 absent probes
hits `kv.maxReadKeys` and fails closed. The Jepsen run on this branch shows the cross-key skew gone but a single-key
lost update through `EVAL` remaining (G10, under investigation). Still open in
Redis after this change: a `GET` that finds the key absent records no
collection fences (a
concurrent `HSET` creating it is undetected, which matters only in a
two-script cycle), `SET NX` / `SET XX` / `SETNX` check existence through
`logicalExists`, which records nothing, and probes that resolve to a
collection type record nothing.

Each fix lands with a failing test first (per `CLAUDE.md`): a unit test that
drives the two-transaction interleaving through the coordinator and asserts
`ErrWriteConflict`.

### A2. Read locks at PREPARE (G1)

Analysis outcome, confirmed by the reproduction test: PREPARE-time
validation is **not** sufficient. Between
PREPARE apply and COMMIT apply nothing protects a 2PC transaction's read
keys, on write shards or read-only shards, and re-validating at COMMIT apply
would not close it either, because the commit point cannot be atomic across
shards. The sound design is the classic one for serializable OCC over 2PC:
PREPARE installs a **read lock** record for every read key on that shard
(a marker keyed like `txnLockKey`, carrying `StartTS` and the primary key,
and shared rather than exclusive), and a writer's apply-time
`assertNoConflictingTxnLock` treats a foreign read lock like a foreign write
lock. Symmetrically, **installing a read lock fails when the key carries a
foreign write lock** (an intent): otherwise two cross-shard transactions
that each read the other's write key can interleave their per-shard
PREPAREs so that both write locks land before either read lock, and both
commit (write skew); the reader aborts or resolves the lock, as a plain
read does through `maybeResolveTxnLock`. COMMIT, ABORT, and the
`LockResolver` clear read locks exactly as they clear write locks.

**Transaction identity (G12).** Read locks, write locks, and the commit /
rollback records need an identity that two concurrent transactions cannot
share, and `(primaryKey, StartTS)` is not one because adapters take
`StartTS` from the shared read snapshot and `StartTS` cannot be made unique
without breaking validation (the read set must be validated at exactly the
snapshot it was read at). A2 therefore adds an explicit `TxnID`: 16 random
bytes drawn once per transaction by the dispatching coordinator (no
coordination, unique with overwhelming probability, stable across the
retries of one transaction so an idempotent re-PREPARE still matches its
own lock), carried in `pb.Request`, stored in the `txnLock` payload (a new
encoding version; a lock without one is treated as foreign by everything
except the resolver, which settles it from its primary as today), used as
the owner test in `handlePrepareRequest` and the read-lock checks, and as
the suffix of the read-lock row key and of the commit / rollback record
keys (`primaryKey` stays in the key so the resolver still finds the
primary's status). This is a wire and key-format change: old and new
nodes must not run mixed 2PC traffic, so it ships behind the same
rolling-upgrade capability gate as the fence.
`DEL_PREFIX` is a range read-modify-write, not a blind write, so its apply
must conflict with every transaction lock, write or read, under the prefix
(the `DEL_PREFIX` is rejected as retryable, or the prepared transactions
are resolved first); today `handleRawRequest` routes it to
`handleDelPrefixWithFloorSnapshot` before any per-key lock validation, so a
prepared transaction can read `k`, `FLUSHALL` can delete the committed `k`
while missing the uncommitted intent on `x`, and the transaction then
commits `x`: a result that serialises neither before nor after the flush. `handlePrepareRequest` is extended to accept a lock-only PREPARE (today it
rejects an empty mutation list with `ErrInvalidRequest`) and to create read
locks from `ReadKeys`; read-only shards then receive a PREPARE with an empty
mutation list and only read locks, which replaces `validateReadOnlyShards`
and the earlier idea of a separate `TXN_READ_VALIDATE` phase. Cost: one Raft entry per read-only shard per multi-shard
transaction (already paid by write shards) and one more lock row per read
key; writers to a read-locked key abort and retry instead of racing.

Status of A2: implemented ahead of review on local branch
`design/serializable-audit-a2-readlocks` (six commits on top of the A0 fence
branch, not pushed). What landed: read-lock rows under a new prefix
`!txn|rlock|` keyed `uvarint(len(key)) key startTS primaryKey` (per key and
transaction, transaction identity `(primaryKey, startTS)` as the commit and
rollback records already use, value = the existing `txnLock` payload);
`handlePrepareRequest` installs one row per read key and accepts a lock-only
PREPARE (read keys, no mutations); `assertNoForeignTxnReadLocks` makes a
one-phase apply and a PREPARE fail with `TxnLockedError` ("read lock") when a
write key carries another transaction's read lock, through one paged scan
over the write keys' prefixes; COMMIT and ABORT entries carry the read keys
and delete the rows; the `LockResolver` sweeps both lock namespaces and
settles an expired read lock from the primary's status; `prewriteTxn`
prepares the read-only groups with lock-only PREPAREs and the commit / abort
fan-out includes them; `validateReadOnlyShards` and its helpers are deleted;
migration export skips the rows; readers never consult them. Results: the G1
reproduction passes (3 of 3 under `-race`), fifteen new tests cover install,
release, foreign versus own locks, reader transparency, resolver expiry, and
end-to-end read-only-shard transactions on two Raft groups; `store`, `kv`,
and `adapter` are green under `-race`; lint is clean.

The review finding that read-lock installation ignored foreign write locks
is fixed on `design/serializable-audit-a2-readlock-vs-writelock` (two
commits on top of the A0 completion branch, not pushed): `54cc0541` adds
`kv/txn_read_lock_vs_write_lock_repro_test.go`, a two-group test that parks
T2's lock-only PREPARE and both primary COMMITs so both write locks land
before either read lock; before the fix both transactions committed
(`x="t2"`, `y="t1"`). `8223f042` adds
`assertNoForeignTxnWriteLocksOnReadKeys` to `handlePrepareRequest`, run
after the mutations are built and before the read-lock rows are appended,
for lock-only and write-shard PREPAREs alike; it fails with
`TxnLockedError` ("write lock") for the first read key whose write lock
belongs to another transaction, using the same ownership test as
`txnLockOwnedBy`, and the write-key check was refactored into
`assertNoConflictingTxnLockTargets` so both use the batch-get path on
Pebble and the scan path otherwise. Checking the lock row is equivalent to
checking the intent because PREPARE writes both in one batch and COMMIT,
ABORT, and the resolver delete both in one batch. No new cleanup path:
`prewriteTxn` already aborts the prepared groups of the failed transaction,
and the reproduction asserts no lock rows remain on either group. After the
fix T1 aborts with the lock conflict and T2 commits. Four new subtests
(in-memory and Pebble, lock-only and write-shard PREPARE) cover rejection,
no partial rows on rejection, own locks accepted, and success after the
writer's ABORT; one existing test that read-locked a key under a foreign
write lock was changed to a key the writer only read-locked. `store` and
`kv` are green under `-race`; `adapter` is green except the pre-existing
A0b timing flake `TestGRPC_FollowerWritesCarryLeaderIssuedTimestamps` (4 of
15 runs fail on the base commit, 3 of 15 with the fix). Cost: one batch-get
of the read keys' lock rows per PREPARE that carries read keys, no extra
Raft round-trip. Limits: like the other lock checks it ignores lock TTL, so
an orphaned write lock blocks readers' PREPAREs until the resolver settles
it, and staged-migration lock aliases are not checked (as for the existing
checks). The one-phase path is unchanged: it installs no read locks, and
its reads and a conflicting 2PC read lock on the same group are ordered by
that group's apply.

Constraints the fix PR must close before merge:

- **Read-lock installation versus foreign write locks (found in review):
  closed.** Reproduced and fixed on
  `design/serializable-audit-a2-readlock-vs-writelock` (status above); the
  TLA+ half is `design/serializable-audit-a3-shards`, whose
  `MCOCC_gap_readlock_intent.cfg` fails `OCC8` on this interleaving without
  the rejection (A3). Still open from it: the orphaned-write-lock wait is
  bounded only by the resolver's TTL sweep.
- **Transaction identity (G12, found in review).** The implemented A2
  keys and owns read locks by `(primaryKey, StartTS)`; add the `TxnID`
  described in the design, with a two-group reproduction (two transactions
  sharing primary key and `StartTS`, expected red first: a mixed commit
  today) and tests that a same-identity retry still matches its own locks.
- **`DEL_PREFIX` ignores locks (found in review).** The implemented A2
  leaves raw `DEL_PREFIX` outside lock validation; add range-aware conflict
  handling against write and read locks under the prefix, with a test that
  prepares a transaction reading a key the flush deletes.
- **Tombstone accumulation.** Rows are per `(key, transaction)` and MVCC
  compaction keeps a key's last tombstone, so every released read lock leaves
  a permanent tombstone that every later writer of that key scans: about
  2 µs per check with no history, 28 µs after 100 released locks, 290 µs
  after 1000 (`kv/txn_read_lock_benchmark_test.go`). Options: let compaction
  drop tombstone-only `!txn|rlock|` keys below `minRetainedTS` (a duplicate
  PREPARE arriving after that could recreate a stale lock that then lasts
  TTL plus one resolver interval), or keep one shared row per key holding
  the set of holders. Needs a decision.
- **Migration.** Read locks are not drained or carried across a cutover; a
  read lock taken on the source does not protect the key on the target.
- **Cost.** A read-only shard now takes two Raft entries (PREPARE, COMMIT)
  where it took one read barrier; the earlier "one entry" estimate is wrong.
- **Behaviour change.** A read key routing to a group the coordinator does
  not know now aborts the transaction (before, its validation was silently
  skipped).
- Pre-existing and untouched: RAW writes and raw `DEL_PREFIX` ignore every
  lock; a coordinator stalled past the TTL can have its primary lock treated
  as a rollback (true for write locks too).

### A3. TLA+

Status: done on three stacked local branches (not pushed):
`design/serializable-audit-a3` (commit `e11e3dd1`, the G0 half),
`design/serializable-audit-a3-g1` (commit `f0ba0cbf`, the G1 half), and
`design/serializable-audit-a3-shards` (commit `74c15fda`, PREPARE split per
key so the cross-shard interleaving found in review is modelled). What
remains is documentation and the modelling limits listed below.

Landed, in `tla/occ/OCC.tla` and its configs:

- `Commit(t)` is split into `Allocate(t)` (fresh timestamp, state
  `Proposed`, no version written), `Apply(t)` (versions written, watermark
  raised by max, enabled for **any** proposed transaction so TLC explores
  reorderings), and `AbortProposed(t)` (the FSM rejects the entry).
  `BeginTxn` takes `startTs = watermark`, as `snapshotTS` does; reads are
  recorded in a `readSet` so "read nothing" is distinct from "not read".
- Constant `ApplyFence` (the A0 fence): `Apply(t)` requires
  `commitTs[t] > watermark`, otherwise `AbortProposed(t)` fires.
- Read-set validation, `ReadConflictFree(t)`: every read key has no version
  newer than `startTs[t]`, checked by the one-phase `Apply` and by the 2PC
  `Prepare` (once, never again at the 2PC `Apply`), exactly as
  `checkConflictsLocked` and `handlePrepareRequest` do. Before this was
  added the model was plain snapshot isolation: `OCC8` failed on it with two
  one-phase transactions crossing two keys.
- Constant `ReadLocks` (the A2 read locks): PREPARE adds `t` to a shared
  `readLocks[k]` for each read key; a writer's PREPARE is disabled and a
  one-phase `Apply` is rejected while a foreign read lock sits on one of
  its write keys; `Apply`, `Abort`, and `AbortProposed` release them;
  readers are never blocked.
- PREPARE is per key and per role, not one atomic step: `PrepareWrite(t,
  k)` (no lock on `k`, no version newer than `startTs[t]`, no foreign read
  lock) installs the write lock and `PrepareRead(t, k)` (per-key version
  check, then the read lock when `ReadLocks` is on) stands for the
  lock-only PREPARE of a read-only shard. The first step moves the
  transaction to a new `Preparing` state (no further reads, writes, or
  one-phase `Allocate`), the last to `Prepared`; `Abort` is enabled from
  `Preparing` and releases partial locks. One key per shard is the finest
  sharding, so every per-shard interleaving is reachable and a pass holds
  for any key-to-shard assignment. Read validation runs when the read lock
  is installed, as each shard's PREPARE apply does; COMMIT does not
  re-validate.
- Constant `ReadLockRejectsWriteLock` (the review finding): `PrepareRead`
  is disabled while a foreign write lock sits on the key. With it off,
  `MCOCC_gap_readlock_intent.cfg` fails `OCC8` at depth 15 on exactly the
  review's interleaving (both write locks, then both read locks over the
  other's intent, then both apply). With it on, a scratch check confirmed
  the state with both write locks placed is still reachable and both
  `PrepareRead` steps are disabled there, over all 203,379 distinct states.
- Invariants `OCC6_SnapshotStableAtWatermark` (what a read at snapshot `s`
  returned is still what is visible at `(k, s)`), `OCC7_NoLostUpdate` (a
  committed writer saw every version it overwrote), and `OCC8_NoWriteSkew`
  (no two committed transactions each read a key the other wrote without
  seeing the other's write; two-transaction cycles only).
- Results, `make tla-check`, all 18 runs matching the contract: `MCOCC.cfg`
  (3 transactions, 2 keys, `MaxOps = 4`, all guards on) passes OCC1 to OCC8
  over 203,379 distinct states at depth 23 in about 10 seconds (124,064 at
  depth 21 before the per-key split) with every guard exercised (canary
  invariants confirmed each rejection path is reachable).
  `MCOCC_gap_applyorder.cfg` (fence off) fails `OCC6` at depth 11 with the
  same schedule as `kv/apply_order_repro_test.go`, fails `OCC7` at depth 14
  when `OCC6` is removed, and `OCC8` at depth 15 when `OCC7` is also
  removed. `MCOCC_gap_readlocks.cfg` (read locks off) fails `OCC8` at depth
  13 with the G1 schedule: W prepares (lock on `k`), T allocates and applies
  (its write key `k1` has no lock; its read key carries W's lock but no
  version), W allocates and applies without re-checking its read key.
  `MCOCC_gap_readlock_intent.cfg` fails `OCC8` at depth 15 as above. All
  three gap configs are wired into `scripts/tla-check.sh`; the HLC, MVCC,
  Routes, and Composed configs are unchanged.

Modelling limits, recorded so nobody over-reads the result:

- One store stands for all shards; sharding shows up only as the per-key
  PREPARE interleaving, so shard-local state (a leader change on one shard,
  a partial PREPARE that is never resolved) is not modelled. A refused
  PREPARE step is modelled as disabled (the transaction waits or aborts),
  not as an error returned to the client, and `PrepareRead` still requires
  a non-empty write set.
- The 2PC commit timestamp is allocated after `Prepare` (the A0 ordering);
  today's allocate-before-PREPARE is not modelled. G1 does not depend on it.
- `OCC8` checks two-transaction cycles only.
- `2026_05_28_implemented_tla_safety_spec.md` still lists OCC-1 to OCC-5 and
  one gap configuration; it is refreshed in the A3 PR.

### A4. Jepsen write-skew workloads

Status: the three single-shard workloads are on local branch
`design/serializable-audit-a4` (commit `dc880d73`, not pushed), with unit
tests, registered in `jepsen_test.clj`. Not yet done: the multi-shard
variant, CI wiring, and the "green after the fix" run.

All use Elle's rw-register model (`jepsen.tests.cycle.wr`) under
`:consistency-models [:strict-serializable]` with `:wfr-keys? true`, which
reports G2-item, with transactions of the shape "read `a`, write `b`" so
anti-dependencies are not accompanied by write-write edges. Every run uses
a per-run key prefix so the three workloads can share one cluster.

- `elastickv.redis-wr-workload`: `MULTI; GET / SET in transaction order;
  EXEC` (reads inside the body are tracked as OCC read keys).
- `elastickv.redis-lua-wr-workload`: one `EVAL` per transaction that
  performs the reads and writes in order and returns the read values.
- `elastickv.dynamodb-wr-workload`: `TransactGetItems` for the reads, then
  one `TransactWriteItems` with a `ConditionCheck` on every read-only key
  (the read value) and a conditioned `Put` on every written key;
  `TransactionCanceledException` is recorded as `:fail`.
- gRPC: `TransactionalKV` cannot express a read set (G8), so no client-side
  workload can drive write skew through it; the engine-level evidence for
  that path is the Go reproduction tests on `design/serializable-audit-a0`.

First results, on a binary built from `main` at `4ca7e90d`, three local
nodes, `--time-limit 30 --rate 50 --concurrency 10`:

| Workload | Result |
|---|---|
| Lua | `:valid? false`, four G2-item cycles, `:not #{:repeatable-read}`. One cycle is textbook write skew: T1 `[[:r 53 1] [:r 53 1] [:w 54 2]]`, T2 `[[:r 53 1] [:w 53 2] [:r 54 nil]]`. This is G3 observed through Jepsen; the fix on `design/serializable-audit-a1-redis` is expected to turn it green. |
| Redis `MULTI` | `:valid? true` (678 ok). The race did not trigger at this rate; not evidence of correctness. |
| DynamoDB | `:valid? true` (1088 ok, 148 `:fail` from conditional checks). Same caveat. |

Elle note, recorded so the next person does not chase it: with
`:linearizable-keys? true` the first Redis run reported a `:cyclic-versions`
anomaly on one key whose version edges are supported by no realtime order
in the history; Elle 0.2.7 also drops the realtime version order for every
key when this fires. Re-checking the same history with `:wfr-keys?` only is
valid. The workloads therefore do not set `:linearizable-keys?`, at the
cost that version order comes only from the initial state and from
transactions that read then write the same key, so reads of absent keys
are the main way write skew is detected.

Second results, on a binary built from `design/serializable-audit-a1-redis`
(the G3 and G7 fixes), same topology and rate:

| Workload | Result |
|---|---|
| Redis `MULTI` | `:valid? true` (707 ok, 0 fail, 0 info). |
| Lua, two runs | `:valid? false` in both, but with **no cross-key cycle**: every G2-item is the single-key rw / rw pair under a `:lost-update` on the same key (keys 144, 43, 140 in one run; 71 in the other). The cross-key write skew that G3 targets did not appear. |
| Lua control on `main`, two runs | one run `:valid? true`, the other `:valid? false` with two single-key lost updates and one **cross-key** cycle (keys 55 / 56, the G3 shape). |

Reading: the G3 fix removes the cross-key shape (0 of 2 fixed runs versus
2 of 3 `main` runs showing it), and exposes a distinct single-key lost
update that exists on `main` as well (G10). A 30-second run is not enough
to separate the binaries statistically; the fix PR should run the Lua
workload longer or at a higher rate and report the counts.

Third results, on a binary built from `design/serializable-audit-a0-complete`
(the full stack: G3 / G7 adapter fixes, A0 fence with replicated watermark,
A2 read locks, A0b leader-only stamping, 2PC commit-time timestamp), same
topology, 60 s at rate 50 / concurrency 10:

| Workload | Result |
|---|---|
| Lua, three runs | `:valid? true` in all three (1466 / 1495 / 1512 ok, 0 fail, 0 info). No G2-item, no lost update, in about 4,470 transactions. |
| Redis `MULTI`, two runs | `:valid? true` (1341 / 1237 ok). |
| DynamoDB, two runs | `:valid? true` (2142 / 2237 ok; 339 / 312 `:fail` are `TransactionCanceledException` from condition checks). |
| Existing CI set at CI settings | all `:valid? true` (see the harness findings below for three vacuous passes). |
| Existing list-append, Redis and DynamoDB, 60 s at rate 50 on a fresh cluster | `:valid? true` (1014 and 2181 ok). |

Reading: the three anomaly shapes seen on `main` (Lua cross-key G2-item,
single-key lost update, the `MULTI` `:cyclic-versions` artifact) did not
appear on the full-stack binary under the load that produced them. Every
run was `--local`: no partitions, kills, or leader changes, on one host;
the paths the fixes target under leadership changes (forwarding while
leadership moves, the 2PC commit timestamp after PREPARE) are not yet
exercised under faults. Node logs contained no `panic` or `fatal` lines;
the server does not log conflicts, fences, or read locks at the default
level, so their frequency is visible only client-side.

Cost observed: Lua latency rose from roughly 39 to 72 ms mean (p50 about
28 ms, p99 0.2 to 0.4 s) on `main` to about 380 ms mean (p50 about 162 ms,
p99 2.7 to 2.9 s) on the full stack, while client-visible Lua write conflicts
fell from 68 to 157 per 30 s run to 0: `EVAL` now runs on the leader through
the proxy and conflicts are retried server-side. The benchmark milestone
measures this properly; the Lua proxy path (a shared go-redis client with a
3 s read timeout) is the first place to look.

Harness findings, to be filed as their own follow-ups (they hold for any
server binary):

- `elastickv.sqs-htfifo-workload` tests nothing, in CI too: `setup!` stores
  the queue URL on the record it returns, Jepsen 0.3.13 discards that
  value (`with-client+nemesis-setup-teardown` runs `open!` / `setup!` /
  `close!` once per node and drops the result), workers get clients from
  `open!`, so every request carries a nil `QueueUrl` and the server answers
  `MissingParameter` (HTTP 400); the checker sees 0 sends and 0 receives
  and reports valid. **Fixed** on local branch
  `fix/jepsen-sqs-htfifo-queue-url` (from `main`, not pushed): `7542e939`
  makes the checker report `:valid? :unknown` with `:vacuous-reasons`
  (`:no-sends`, `:no-receives`) when nothing was sent or received (a real
  `:lost` still wins with `false`; `cli/fail-on-invalid!` already fails on
  `:unknown`), and `13395f8a` keeps the queue URL in an atom created once
  per test (the pattern `seq-counters` already uses; an `open!`-time lookup
  was rejected because Jepsen re-opens a client after every `:info`, and a
  promise would hang `invoke!`), with an op failing locally as
  `:no-queue-url` if it is unset. Eighteen unit tests (57 assertions; the
  checker and lifecycle tests were red first). On a fresh three-node
  cluster with the CI flags (30 s, rate 5, concurrency 5): `main` 0 `:ok`
  sends and `:valid? true`; the checker commit alone `:unknown`
  `[:no-sends :no-receives]`, exit 1; both commits 80 `:ok` sends, 118
  `:ok` receives, 80 received, `:valid? true`. Not yet run under faults.
- Found while fixing it, not yet fixed: `sqs-invoke!` reads the error code
  from `:__type`, but this cognitect SQS client (query protocol) puts it
  under `:cognitect.aws.error/code`, so every SQS error is `:info` with
  `:cognitect.anomalies/incorrect`, the `:fail` classification and the
  `QueueAlreadyExists` fallback never trigger, and the missing-queue code is
  `AWS.SimpleQueueService.NonExistentQueue` (conservative: no false pass);
  the drain generator is not wrapped in `gen/clients`, so the nemesis worker
  takes `:recv` ops and logs `:jepsen.nemesis/invalid-completion` about a
  dozen times per run (noise only); a comment in
  `dynamodb_multi_table_workload.clj` says `setup!` runs once per test, but
  it runs once per node.
- The list-append workloads reuse plain integer keys across runs, so a
  second run on the same cluster makes Elle abort (`No transaction wrote
  11 = 2`); CI runs each workload once per cluster and is unaffected.
- The DynamoDB per-type `binary` and `binary-set` runs cannot decode reads
  (every read is `:info`), so those two types are write-only checks.

Fourth results, under faults. A local process nemesis
(`jepsen/src/elastickv/local_nemesis.clj`, branch
`design/serializable-audit-a4-nemesis`, commit `cb267c46`, not pushed; 15
unit tests) kills or pauses the node whose `/healthz/leader` answers 200
every 10 s and heals it 5 s (kill) or 3 s (pause) later, restarting a killed
node with its original flags and waiting for its ports. Same full-stack
binary, 90 s at rate 50 / concurrency 10, fresh cluster per run:

| Run | Result |
|---|---|
| Lua, Redis `MULTI`, DynamoDB rw-register, two runs each, leader kill | all `:valid? true`; 9 kills and 9 restarts per run, every kill hit the leader, every node came back (ports open within 143 to 291 ms); no `:ok` write vanished; no Elle anomaly. The `:fail` / `:info` counts (about 800 per Redis run) are connection refusals to the dead node and in-flight `:eof`s. |
| Redis and DynamoDB list-append, leader kill | `:valid? true`. |
| Lua, leader pause, prescribed run | `:valid? true`; no pause changed the leader (failover takes about 2.7 to 3 s, so a 3 s pause sits on the boundary) and the cluster stalled for the pause windows. |
| Lua, leader pause, two additional runs | `:valid? false`, **G1a** in both, traced to G11 above (a write that committed after the leader stepped down was reported `NOTLEADER` and recorded as a definite failure); with `NOTLEADER` reclassified as indeterminate both histories are valid. |

Node logs: no `panic`, `fatal`, `data race`, `inconsistent`, or `diverg`
lines in any run. Two availability observations: failover after a leader
stop takes about 2.7 to 3 s; and a demoted leader's Lua retries keep
forwarding the request stamped with its old timestamp, which the A0b check
rejects each time until the retries run out (safe, wasted work; the retry
should re-dispatch unstamped).

What the fault runs do not cover: partitions, clock skew, disk or fsync
faults (SIGKILL keeps the page cache), more than one faulted node, a
non-leader target, more than one Raft group (no cross-shard 2PC under
faults), more than one host, more than two runs per configuration, pause
for `MULTI` / list-append, and the snapshot-install path on restart.

Remaining:

- Fix G11 server-side (A6) and record the outcome-unknown class as `:info`
  in the Jepsen clients; re-run the pause configuration for every workload.
- Multi-group cluster under faults (cross-shard 2PC); partition nemesis.
- Fix the three harness findings above (separate PRs).
- Multi-shard variant for each workload: keys spread across at least two
  Raft groups so G1's path is exercised (Jepsen M5 already runs multi-group
  locally).
- CI: add the three workloads to `.github/workflows/jepsen-test.yml` next
  to the existing ones once the A1 and A0 / A2 fixes land, so the workflow
  stays green; until then they run locally through `scripts/run-jepsen-local.sh`.
- Environment facts for the runner: `/tmp/lein` from `CLAUDE.md` does not
  exist on the dev machine; `/opt/homebrew/bin/lein` with Java 21 works
  (Java 17 fails on `java.util.SequencedCollection`); the `jepsen/redis`
  submodule must be initialised.

### A6. Indeterminate outcomes (G11)

Status: proposed; implementation starts on
`design/serializable-audit-a6-outcome-unknown` (from `main`, independent of
the A0 to A2 stack). Numbered after A5 because it was found by A4's pause
nemesis after the milestone list was written.

Today the etcd engine fails a proposal in three places with the same
`errNotLeader`: `handleProposal` before `node.Propose` (nothing was
proposed: a definite failure), `ErrProposalDropped` during a leadership
transfer (also definite), and `failPending` from `refreshStatus` when the
node stops being leader (the entry is in the log and may commit under the
successor: the outcome is unknown). `kv.isLeadershipLossError` treats all
three as transient, so `LeaderProxy` and the coordinator retry, which
double-applies a non-idempotent command when the first proposal did commit,
and every adapter reports a definite failure (`NOTLEADER`, DynamoDB 4xx,
gRPC `Unavailable`) that a later read can contradict. The Jepsen pause runs
see this as G1a.

Design:

- **Engine.** A new sentinel `raftengine.ErrProposalOutcomeUnknown`, not
  marked with `ErrNotLeader`. `failPending` uses it for pending proposals
  and pending admin / config changes; pending reads keep `errNotLeader`
  (nothing was applied). Pre-proposal rejections are unchanged. The engine
  records, per drained proposal, the last known index and term so a later
  milestone can resolve the outcome by watching whether that index commits
  with that term or is overwritten; this milestone only reports it.
- **kv.** `isLeadershipLossError` and `isTransientLeaderError` return false
  for it, so no server-side path retries a proposal whose outcome is unknown
  (a retry re-stamps a fresh timestamp and applies a second time when the
  first committed; the A0 fence does not detect that). A new
  `kv.IsOutcomeUnknown(err)` names the class for adapters. The lease
  invalidation on leadership loss is unchanged.
- **Forwarding.** `Internal.Forward` carries the class across the gRPC
  boundary with a distinct status code (`codes.Aborted`) and the message
  prefix `proposal outcome unknown`, and the client side of `Forward`
  re-marks it with the sentinel; the phrase classifier's closed list does
  not include it, so a forwarder never reclassifies it as transient.
- **Adapters.** Each surface reports the class the way its upstream does
  for an internal error whose effect is unknown, never as a retry-safe
  failure: Redis `-OUTCOMEUNKNOWN <message>` (a distinct first token, so
  clients and the Jepsen client classify it separately from `NOTLEADER`);
  DynamoDB HTTP 500 `InternalServerError`; S3 HTTP 500 `InternalError`;
  SQS HTTP 500 `InternalFailure`; gRPC `codes.Aborted` (clients must not
  treat it as `Unavailable`).
- **Jepsen.** The A4 clients record it as `:info` (Redis by the first
  token, the HTTP adapters by status 500 plus the code), on the A4 branch.

Tests: an engine test that proposes, forces leadership loss with the
proposal pending, and asserts the sentinel (and that `errors.Is(err,
ErrNotLeader)` is false), next to one that a pre-proposal rejection is
still `ErrNotLeader`; the raftenginetest conformance suite gains the same
case if it has a leadership-loss hook; a `LeaderProxy` table test that the
class is not retried; per-adapter mapping tests; a `Forward` round-trip
test. Merge blocker: none beyond these; the timestamp and read-lock work is
independent.

### A5. Documentation

- README: replace the "Basic Consistency Behaviors" bullet with the three
  claims from the positioning doc §5 and the lease caveat.
- `docs/architecture_overview.md`: add a transactions section (one-phase, 2PC,
  read-validate, conflict predicate).
- `docs/review_todo.md`: 4.2 already points here; fix 4.4's stale "or Del".
- `CLAUDE.md` Conventions: one line stating that any adapter path that reads
  before it writes must surface the read keys, and that the coverage table in
  this doc is the checklist reviewers use.

Order: A0 reproduction → A0 fence → A0b (timestamp identity) → A1 (table
first, then S3, then Lua) → A2 → A3 → A4 → A5. A0's reproduction test is the first thing to run because
its outcome decides whether any path can be called serializable today; A3
and A4 are pure evidence and can move ahead of A2 if the read-lock work
proves larger than expected.

## 5. Evidence when complete

- The A0 reproduction test is red before the fence and green after; the
  same PR carries both. The G1 reproduction test is red before the read
  locks and green after; the A2 PR carries it. The G3 and G7 reproduction
  tests are red before the A1 Redis fixes and green after; the A1 PR carries
  them.
- The coverage table has no unresolved in-scope row protected by
  "nothing": every `N` that remains is marked benign (with the reason) or
  out of scope (G8, the gRPC API gap), so completion cannot be reached by
  relabelling.
- `go test -race ./kv/... ./adapter/... ./store/...` includes the new
  interleaving tests.
- `make tla-check` passes with `OCC6_SnapshotStableAtWatermark`,
  `OCC7_NoLostUpdate`, and `OCC8_NoWriteSkew`, and fails the two gap
  configurations as expected.
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
2. Whether to add a read-set-bearing transaction API to gRPC
   `TransactionalKV` (`Begin` / `Get` / `Commit`) so the surface can carry
   serializable multi-key transactions at all (G8). Out of this audit; a
   separate proposal if wanted.
3. Whether `deleteBucket` should take a bucket generation read key
   (cheap, coarse) or a range fence (precise, new mechanism).
4. Read-lock representation: a separate key prefix or a flag on the existing
   lock record; readers of a read-locked key must not wait (read locks block
   writers only).
5. Observation to verify, not yet a gap: `leaderFenceTS` in
   `kv/leader_routed_store.go` reads the local store at
   `max(ts, fenceTS)` where `fenceTS` comes from
   `Coordinate.LinearizableReadForKey`, which returns the engine's Raft
   applied index rather than an HLC timestamp. Harmless while timestamps
   dwarf log indexes; a unit mix worth removing.

## Appendix A. Read-then-write coverage matrix (initial, 2026-09-26)

Produced by code reading on `main` at `4ca7e90d`; every row is re-verified
during A1. Protection means what stops a concurrent writer from invalidating
the read at FSM apply: **W** the key read is also written, so the write-write
check (`latest > StartTS`) catches it; **R** the key is in `ReadKeys`;
**F** a fence key that every conflicting writer also writes is in `ReadKeys`
or the write set; **N** nothing. `StartTS` is the snapshot the handler read
at unless noted. A read key on a shard that receives no mutations is checked
once at prewrite and not again (G1).

| Adapter | Operation (handler) | Keys read | Keys written | `ReadKeys` | Protection | Notes |
|---|---|---|---|---|---|---|
| S3 | `createBucket` (`s3.go`), `AdminCreateBucket` (`s3_admin.go`) | bucket meta (absent), generation | bucket meta, generation | none | W | |
| S3 | `deleteBucket`, `AdminDeleteBucket` | bucket meta | bucket meta (Del), then non-txn prefix delete | none | W | |
| S3 | `deleteBucket` emptiness scan | object manifests (limit 1) | none | none | F via W | every manifest writer `Put`s the bucket meta as a fence (`s3_put_object.go`, `s3_multipart_complete.go`, `s3_admin_objects.go`); uploads and parts are not scanned |
| S3 | `putBucketAcl`, `AdminPutBucketAcl` | bucket meta | bucket meta | none | W | |
| S3 | `putObject` incl. `If-Match` / `If-None-Match` (`s3_put_object.go`, `validateS3PutPreconditions`) | bucket meta, head manifest | chunk refs, bucket meta (fence), head | none | W | `StartTS` is the prepare `readTS`, held across the body upload; `AdminPutObject` flushes chunks non-txn first |
| S3 | `deleteObject`, `AdminDeleteObject` | bucket meta, head | head (Del; no write if absent) | none | head W; bucket meta N (benign) | |
| S3 | `createMultipartUpload` | bucket meta | bucket meta (fence), upload meta, GC upload | none | W | |
| S3 | `uploadPart` (`s3_upload_part.go`): upload meta | upload meta, re-read at a fresh `readTS` | part key, chunk refs | upload meta | R | conflicts with abort / complete, which delete the upload meta |
| S3 | `uploadPart`: bucket meta | bucket meta | none | none | **N** | after a concurrent `deleteBucket` the part and chunks land under a dead generation (leak) |
| S3 | `uploadPart`: previous part | part key at the prepare `readTS`, older than `StartTS` | part key | none | **N** (window) | a same-part upload committing in the window is unseen; its blobs are never cleaned and the stale one is cleaned twice |
| S3 | `completeMultipartUpload` (`s3_multipart_complete.go`): meta / upload | bucket meta, generation, upload meta, head, all re-read at the second `readTS` | bucket meta, head, upload meta (Del), GC upload | none | W | |
| S3 | `completeMultipartUpload`: parts | part descriptors at the first `readTS` | none | none | **N** | never re-read at the commit snapshot; a later `uploadPart` is allowed (upload meta unchanged) and the manifest points at superseded part versions whose blobs are deleted asynchronously |
| S3 | `abortMultipartUpload` | bucket meta, upload meta | upload meta (Del), GC upload | none | upload meta W; bucket meta N (benign) | |
| S3 | chunk blob push / backfill (`s3_blob_fetch.go`) | blob key latest commit | local Put | none | W (local store) | content-addressed |
| S3 | async cleanup | prefix scans | non-txn Del | none | N | garbage only |
| DynamoDB | `PutItem` / `UpdateItem` / `DeleteItem` with or without `ConditionExpression`, `BatchWriteItem`, admin item writes (`dynamodb_item_write.go`, `dynamodb_transact.go`) | target item; old GSI entries derived from it | target (Put / Del), stale GSI keys (Del), new GSI keys (Put) | none | W | `DeleteItem` on a missing item writes nothing |
| DynamoDB | same, legacy migration source key (`dynamodb_item_read.go`) | legacy source when target absent | source (Del) if found | none | W if found, N if absent | the only other writer `Put`s the target, caught by W there |
| DynamoDB | same plus `TransactWriteItems`: table schema | table meta / generation | none | none | N at apply | compensated after commit: `verifyTableGeneration` + `cleanupCommittedKeys` + retry |
| DynamoDB | `TransactWriteItems` Put / Update / Delete on an existing item | item | as above | item key | W + R | |
| DynamoDB | `TransactWriteItems` Delete or `ConditionCheck` on a missing item | item (absent) | none | item key | R | validated only if another element writes; an all-no-op transaction returns without dispatch |
| DynamoDB | `ConditionCheck` on an existing item | item | same value re-put | item key | W + R | |
| DynamoDB | `CreateTable` (`dynamodb_schema.go`) | table meta (absent), generation | both | none | W | |
| DynamoDB | `DeleteTable` | table meta | table meta (Del); async item prefix delete | none | W | |
| DynamoDB | legacy migration start / item (`dynamodb_migration.go`) | meta + generation; target + source | meta + generation; target (Put), source (Del) | none | W | "target exists → Del source only" reads the target unprotected (benign) |
| DynamoDB | `finalizeLegacyTableMigration` | schema from an earlier snapshot; source-generation emptiness | table meta with a fresh `StartTS` | none | **N** | only a process-local lock; G9 |
| Redis | `MULTI` type probe (`stagedKeyType`, `redis_txn.go`) | raw key type: string / HLL / bare, list meta + deltas, wide hash / set / zset prefixes, legacy blobs, stream meta | none | list meta, legacy blobs, stream meta, HLL, string, bare; **no fences, no wide or delta prefixes** | R partial → **N** for `GET` / `EXISTS` / `EXPIRE` on an absent key | `MULTI { EXISTS k; SET a 1 }` misses a concurrent `RPUSH` / `HSET` / `SADD` / `ZADD` creating `k`; commands that write `k` add fences |
| Redis | `MULTI SET` (with `NX`, `XX`, or `GET`), standalone `SET` (dedup path, default) | type; string + bare | logical delete, 4 fences, string, TTL | anchors + 4 fences | R + W + F | |
| Redis | `MULTI INCR`, standalone `INCR` (dedup) | type, string + bare, TTL | string / TTL | anchors (+ 4 fences if absent) | R + W | |
| Redis | `MULTI HSET` / `HMSET`, standalone (dedup) | type, hash fence, fields written | fields; fence + delta only if new fields | anchors + hash fence (+ 4 fences if absent) | R / W | update-only `HSET` does not bump the fence |
| Redis | `MULTI RPUSH` | list meta / deltas | items, list fence, delta | list fence, boundary items | R + W + F | |
| Redis | `MULTI LRANGE` | meta, items | none | list fence + boundary items (fence only if absent) | F | subject to the fence gaps below |
| Redis | `MULTI DEL`, `EXPIRE ≤ 0` | list / zset / hash / legacy / stream state | every scanned key (Del) + fence Puts | 4 fences, list fence / boundaries, zset fence, anchors, stream meta | R + F | |
| Redis | `MULTI ZINCRBY` | members | member / score, fence | zset fence + legacy zset key | R + W | |
| Redis | `MULTI EXPIRE > 0` | type, TTL, string | string; or meta + 4 fences for collections | anchors | W | |
| Redis | `SETNX` (`redis_expire_cmds.go` → `executeSet`); legacy `SET` / `INCR` / `HSET` with `ELASTICKV_REDIS_ONEPHASE_DEDUP=0` | type, string / fields | string / TTL or fields; `SET` over a collection deletes the keys it scanned | none | W on string / field; **N** for type-absent | no fences read or written: `SETNX` racing `RPUSH` / `HSET` / `SADD` / `ZADD` on a new key lets a string and a collection coexist |
| Redis | `GETDEL` | type, string | logical delete | none | W (string) | |
| Redis | standalone `EXPIRE` / `PEXPIRE` | existence, TTL, type, string / HLL | string / HLL + TTL, or meta + fences | none | W; the `≤ 0` path is N (no fence) | |
| Redis | standalone `DEL` (`redis_strings.go`, `redis_compat_helpers.go`) | anchors; scans of list items / deltas / claims, hash / set / zset members, stream entries | every key seen (Del); **no fence** | none | **N** | an `RPUSH` committing after `DEL`'s snapshot keeps its item and delta: a list with a hole. Same gap in `LTRIM` to empty, `ZREM` / `ZREMRANGE*` that empty the zset, a set or HLL emptied to zero |
| Redis | `LPUSH` / `RPUSH` (`redis_lists.go`) | type, TTL, meta + deltas | items, list fence, delta | absent: 4 fences; else list fence + head / tail items | R + W + F | |
| Redis | `LPOP` / `RPOP` | type, meta, items in range | claim keys, items (Del), list fence, delta | none | W (fence / claims) | |
| Redis | `LTRIM`, non-empty result | all items | delete + rebuild (rebuild `Put`s the fence) | none | W | |
| Redis | `HDEL` (`redis_hash_cmds.go`) | field existence | fields (Del), delta; **no fence** | none | W | neither `HDEL` nor update-only `HSET` bumps the hash fence |
| Redis | `HINCRBY` | type, field | field; fence + delta if new | hash create read keys | R + W | |
| Redis | `SADD` / `SREM` (`redis_set_cmds.go`) | type, members | changed members; fence + delta if the length changes | set create read keys | R + W + F | |
| Redis | `PFADD`, HLL-kind `SADD` | kind, HLL anchor | HLL + TTL | none | W; N if type-absent | |
| Redis | `ZADD` / `ZINCRBY` / `ZREM` / `ZREMRANGEBYRANK` / `BZPOPMIN` (`redis_zset_cmds.go`) | type, scores / ranks | member / score keys, zset fence, delta | zset create read keys or fence | R + W + F | emptying falls into the `DEL` gap |
| Redis | `XADD` / `XTRIM` (`redis_stream_cmds.go`) | type, stream meta, trim candidates | entry + stream meta, trimmed (Del) | none | W (stream meta); N if type-absent | |
| Redis | `EVAL` / `EVALSHA` and Lua-backed commands (`RENAME`, `LREM`, `LSET`, `RPOPLPUSH`, `ZPOPMIN`, `ZREMRANGEBYSCORE`): keys the script writes (`redis_lua_context.go`) | any | not preserving: delete + 4 fences + rewrite; preserving (list / zset deltas): own fence | 4 fences or the own-type fence | W + R + F | |
| Redis | Lua reads of keys the script does not write | values (`GET`, `HGET`, `LRANGE`, `SMEMBERS`, `ZRANGE`, …) | none | only stream reads are tracked | **N** | `v = GET a; SET b v` commits over a concurrent `SET a` (G3) |
| Redis | delta compactor (`redis_delta_compactor.go`) | base meta + deltas | meta; folded deltas (Del) | none | W | |
| SQS | `CreateQueue` (`sqs_catalog.go`) | meta, generation | both | meta, generation | R + W | |
| SQS | `CreateQueue` / `SetQueueAttributes` DLQ existence check (`sqs_redrive.go`) | DLQ meta | none | none | N (benign) | a policy can point at a deleted DLQ; redrive re-checks |
| SQS | `DeleteQueue` | meta, generation | meta (Del), generation + 1, tombstone | meta, generation | R + W | |
| SQS | `SetQueueAttributes`, `TagQueue` / `UntagQueue` (`sqs_tags.go`) | meta | meta | meta | R + W | |
| SQS | `SendMessage` / `SendMessageBatch`, standard (`sqs_messages.go`, `sqs_messages_batch.go`) | meta (generation) | data, visibility, by-age | meta, generation | F | delete, purge, and set-attributes all write meta or generation |
| SQS | FIFO send (`sqs_fifo.go`) | meta, dedup record, sequence | data, visibility, by-age, dedup, sequence | meta, generation, dedup, sequence | R + W | a dedup hit is read-only |
| SQS | receive rotation | visibility (range scan), data, lock | visibility (Del / Put), data, lock ops | visibility, data, meta, generation (+ lock) | R + W | |
| SQS | receive-time expiry, `DeleteMessage`, `ChangeMessageVisibility` | visibility / data (token check), lock | data / visibility / by-age (Del), or old visibility (Del) + new visibility + data | data, visibility, meta, generation (+ owned lock) | R + W | |
| SQS | `PurgeQueue` (`sqs_purge.go`) | meta, generation | meta, generation + 1, tombstone | meta, generation | R + W | |
| SQS | redrive to DLQ (`sqs_redrive.go`) | source visibility / data, source + DLQ meta / generation, DLQ sequence, lock | source (Del), DLQ Puts, sequence, lock (Del) | all of those | R + W | |
| SQS | reaper record / dedup (`sqs_reaper.go`) | by-age, data, lock / dedup expiry | Dels | same keys + meta / generation | R + W | |
| SQS | reaper orphan by-age and tombstone | data absence; prefix emptiness | by-age / tombstone (Del) | by-age / tombstone only | N (benign) | old-generation keyspace only |
| Filesystem | `InitializeRoot`, `Create` / `Mkdir` (`internal/filesystem/service.go`) | parent inode, entry (absent), candidate inode (absent), usage | inode, home, entry, parent inode, dir version, usage, ref, intent (Del) | all keys read | R + W | `dispatchTxn` keeps `StartTS` |
| Filesystem | `Open` | inode, ref (absent) | ref, ref fence | inode, ref, ref fence | R + W | |
| Filesystem | `Write`, `Truncate` / `SetAttr` | inode, home, partially overwritten chunks, usage | chunks, inode, usage | inode, home, partial chunks, usage | R + W | |
| Filesystem | unlink file | parent, entry, inode, ref scan, chunk page | entry (Del), parent, dir version, inode / home / ref fence, chunks | includes ref fence and chunk keys | R + F | `Open` `Put`s the ref fence, which covers the ref scan |
| Filesystem | `Rmdir`, rename over a directory | child entries (emptiness scan) | child inode / home / dir version (Del) | child inode, home, dir version | F | every creator in the child `Put`s the child inode and dir version |
| Filesystem | `Rename` | parent, old / new entries, replaced inode | entry Del / Put, parent, dir version, GC | yes | R + W | same-parent only |
| Filesystem | release / lease reaper, orphan finalise | ref, inode, refs, chunk emptiness | ref / inode / home / ref fence (Del) | include ref fence and inode | R + F | `Write` `Put`s the inode, which covers chunk emptiness |
| Filesystem | move / recovery (`migration.go`, `recovery.go`) | job, intent, home, inode, chunk page | Puts | explicit lists covering every read | R + W | |
| gRPC | `TransactionalKV` `Put` / `Delete` / `Get` (`grpc.go`, `grpc_transcoder.go`) | `Get`: snapshot at the global watermark | single-key transaction with coordinator-assigned `StartTS` | never | **N** | no `Begin`; `PreWrite` / `Commit` / `Rollback` return not-implemented; a client `Get` then `Put` can lose an update (G8) |

Notes:

1. Not supported at the top level and therefore not rows: `APPEND`,
   `SETRANGE`, `GETSET`, `INCRBY`, `PERSIST`, `RENAMENX`, `HSETNX`, `SPOP`,
   `SMOVE`, `ZPOPMAX`, `XDEL`, `LMOVE`. `MULTI` accepts only `SET`, `DEL`,
   `GET`, `EXISTS`, `INCR`, `HSET`, `HMSET`, `RPUSH`, `LRANGE`, `ZINCRBY`,
   `EXPIRE`, `PEXPIRE`.
2. F protection assumes every writer of a collection bumps its fence. The
   writers that do not: the emptying paths through `deleteLogicalKeyElems`
   (`DEL`, `GETDEL`, `EXPIRE ≤ 0`, `LTRIM` to empty, emptied zset / set),
   `HDEL`, and `HSET` that only updates existing fields (G7).
   `redisTxnReadFenceKeys` only chooses the read snapshot and route; those
   are not OCC read keys.
3. S3 relies on W alone with `StartTS = readTS` everywhere;
   `kv/sharded_coordinator.go` acknowledges these sites supply `StartTS`
   without `ReadKeys`.
4. Highest-risk `N` rows, in the order A1 takes them: S3
   `completeMultipartUpload` parts; Redis standalone `DEL` and the other
   fence-less emptying paths; `SETNX` and legacy type-absent creates;
   `MULTI` `GET` / `EXISTS` type probes; Lua reads of keys the script does
   not write; DynamoDB `finalizeLegacyTableMigration`; gRPC client
   read-modify-write (API gap, G8).
