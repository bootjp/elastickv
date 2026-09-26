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
| G12 | **Transaction identity is `(primaryKey, StartTS)`, which is not unique.** Locks (`txnLock`), the commit and rollback records (`txnCommitKey`, `txnRollbackKey`), the ownership test in `handlePrepareRequest` (`lock.StartTS == startTS && PrimaryKey equal`), and the A2 read-lock rows all identify a transaction by its primary key and start timestamp. Adapter transactions supply `StartTS = readTS`, the shared read snapshot, so two concurrent 2PC transactions with the same primary key and the same snapshot alias each other: the second PREPARE passes the ownership check as a retry of the first and overwrites its intent; the first COMMIT then publishes the second's value under the first's identity, and the second's COMMIT finds no lock and reports success. A2's read-lock rows collide the same way (one transaction's COMMIT deletes the other's protection). Found in review, **reproduced** deterministically (3 of 3 under `-race`): `TestTwoPhaseCommit_SharedPrimaryAndStartTSAliasTransactions` in `kv/txn_identity_aliasing_repro_test.go` (branch `design/serializable-audit-a2-txnid-repro`, commit `255bd0ba`, on the read-lock fix). Two groups; T1 writes `p` and `s1`, T2 writes `p` and `s2`, both with `StartTS` from `ShardStore.LastCommitTS()` and primary `p`. T2's PREPARE on `p` passes `txnLockOwnedBy` and overwrites T1's intent; the first primary COMMIT publishes whatever intent is there and records its `CommitTS`; the second primary COMMIT fails with `commit_ts mismatch`, its ABORTs fail with `ErrTxnAlreadyCommitted`, and `completeCommittedTxn` then commits the loser's secondary at the winner's recorded `CommitTS` and reports success. Final state `p="t2"`, `s1="t1"`, `s2="t2"`; both clients told committed with the same `CommitTS`; T1's write of `p` is lost. In the read variant T2's COMMIT also deletes T1's read-lock row on `s2` and T1 still commits with its read of `s2` overwritten. | A cross-shard transaction can commit a mix of two transactions' writes (atomicity), and A2's read locks do not protect a transaction that shares identity with another. |
| G13 | **A 2PC PREPARE whose `StartTS` equals the previous commit's timestamp is skipped as a replay, and the transaction's writes are silently lost (found by A4, diagnosed).** No faults, one node, one HLC, no split: the DynamoDB multi-table list-append workload (two groups) loses 11 to 21 acknowledged appends per 30-second run with the route-shuffle nemesis on or off. Mechanism: T1's COMMIT deletes each key's lock and intent rows, leaving tombstones at exactly `c1`; T2 starts at `nextTxnReadTS` = `ShardStore.LastCommitTS()` = `c1` when nothing committed since; T2's PREPARE writes its lock and intent at version `StartTS` (`handlePrepareRequest`, `commitTS = startTS`), `staleRaftApplyFastPathLocked` → `raftApplyAlreadyLandedLocked` finds a version (the tombstone) at `c1` on every key and declares the live PREPARE a replay before `checkApplyConflicts`, so no lock is written and validation is skipped; T2's COMMIT finds no lock, treats the key as already resolved, applies nothing, and the client gets OK. A collision on the secondary group alone tears the transaction (primary lands, secondary lost); a collision on both groups loses everything. **Reproduced** deterministically (`TestTwoPhaseCommit_PrepareAtPreviousCommitTSIsSkippedAsReplay` in `kv/prepare_replay_fastpath_repro_test.go`, branch `design/serializable-audit-g13-repro`, commit `10846a6c`, on `main` plus the harness fixes; 3 of 3 red under `-race`, control green; an instrumented binary logged 26 skipped live PREPAREs in one run, every lost transaction matching a skip at its `StartTS`). Excluded: the split and migration path (two runs without route shuffle lose appends too, group-1 keys whose route never moved lose them, the reproduction has no split); the A0 cross-group snapshot (the write is never applied, nothing is in flight); cross-node stamping (one HLC). It is the G10 stale-reapply sink reached by a single node, and **the A0 completion branch's Raft-index replay detection makes the reproduction pass**. Both ABORT variants **reproduce** too (`d0ad09ae`, same file, table-driven, 4 of 4 red 3 times under `-race`, all green with the fast path disabled and all green on the A0 completion branch): a coordinator ABORT after a read-only-shard conflict leaves the same tombstones at its `abortTS`, and `tryAbortExpiredPrimary` resolving an expired lock persists an ABORT at `startTS + 1`, a timestamp nothing allocated; a transaction starting at either value loses its write the same way. The Redis, SQS, and S3 2PC paths start at the watermark too and are expected to share the defect. | The default 2PC path loses committed writes on a healthy single node with both clients told OK; the fix is A0's index-based replay detection, and the regression test travels with the A0 PR. |
| G14 | **PREPARE order lets a reader roll back a secondary before the primary lock exists (found while implementing A2, reproduced).** `prewriteTxn` prepares groups in group-id order (`groupMutations` sorts the ids), not primary group first. A reader that meets a secondary's lock before the primary PREPARE has applied asks `primaryTxnStatus`, finds no primary lock and no record, concludes rolled back **before any TTL check**, and aborts that secondary without writing a rollback record (`handleAbortRequest` writes one only when the ABORT names the primary); the primary then prepares and commits, and the secondary COMMIT finds no lock, treats the key as already resolved (`commitTxnKeyMutations`), and `handleCommitRequest` returns nil with nothing written. **Reproduced** (`TestTwoPhaseCommit_ReaderRollsBackSecondaryBeforePrimaryPrepare` in `kv/txn_prepare_order_repro_test.go`, branch `design/serializable-audit-g14-repro`, commit `6b90173b`, on the `TxnID` branch; 3 of 3 red under `-race`, three controls green): with the route layout flipped so the primary sits on the higher group id, a plain read of the secondary key in the window (a live 30 s lock, no TTL involved) aborts it, and the `LockResolver` does the same once the secondary lock has expired; with the primary prepared first the reader gets `txn locked` and T commits both keys. The window is at least one Raft round-trip on the primary group per cross-shard transaction. The scan read path shares `primaryTxnStatus` and very likely the flaw (not exercised). | A cross-shard transaction commits with one of its writes silently missing while the client is told OK; pre-existing on `main`, independent of the identity fix. |
| G15 | **A forwarded RPC that times out after it was sent is retried (found while implementing A6, not yet reproduced).** The leader-forward breaker in `kv/leader_proxy.go` treats `Unavailable` and `DeadlineExceeded` as retryable even when the request had already reached the leader, which may have proposed and committed it; the resend is a second proposal. | A non-idempotent write applied twice across a forward timeout with the client told OK once. Fix direction: after a forwarded write has been sent, a timeout or `Unavailable` is outcome unknown unless the request is idempotent by identity (2PC requests with a `TxnID` are; raw one-phase requests are not); the A0 fence does not detect the second apply because it carries a fresh timestamp. |

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
timestamp path relies on.

**Multi-group deployments (found in review).** The fence is per group, but
the adapters' read snapshot is not: `globalSnapshotTS` takes
`GlobalLastCommitTS`, and `ShardStore.LastCommitTS()` is the maximum over
all groups. With group A's watermark at `c2` and group B's at `c0`, a write
`c1` (`c0 < c1 < c2`) still in flight on B, and a transaction reading B at
the global snapshot `s = c2`: the read misses `c1`, B later accepts `c1`
(above its own watermark), and validation ignores it (`c1 ≤ s`). That is G0
across groups, and it stays open with a per-group fence. **Reproduced**
(`TestApplyOrder_GlobalSnapshotMissesLaggingGroupWrite` in
`kv/apply_order_multigroup_repro_test.go`, branch
`design/serializable-audit-a0-multigroup-repro`, commit `ae582d7f`, on the
A0 completion branch; 3 of 3 red under `-race`, two control variants
green): two groups, `W_B` at `c1` parked before its proposal, `W_A` at `c2`
applied, T takes `s = ShardStore.LastCommitTS() = c2`, reads `b-k = "old"`,
`W_B` lands on B (`c1` is above B's watermark), T commits `f("old")` with
`b-k` in `ReadKeys`, both as a 2PC transaction writing on A and as a
one-phase write on B; afterwards `b-k` at `s` is `"new"`. Validation passes
for two reasons: the check is `latest > StartTS` and `c1 ≤ s`, and both
stores skip `checkConflictsLocked` entirely when the group's
`lastCommitTS ≤ StartTS` (`store/mvcc_store.go`, `store/lsm_store.go`).
Every adapter read and transaction start uses that global maximum
(`adapter/ts.go` `snapshotTS` / `globalSnapshotTS`; `redis_txn.go`
`txnStartTS`; `dynamodb_locks.go` and `sqs_catalog.go` `nextTxnReadTS`;
`s3.go` `readTS`; the gRPC reads), `ShardStore` has no `GlobalLastCommitTS`,
and no adapter uses a per-group value as a snapshot. The unprotected window
is from T's read to T's PREPARE or one-phase apply on B (a lock-only
PREPARE at `StartTS` already advances B's replicated watermark to `s`
through `alignRaftCommitTS`).

The fix keeps one snapshot timestamp per transaction and makes every group
honour it: **a read at `s` on a group whose replicated watermark is below
`s` first raises that group's watermark to `s` with a Raft entry** (a
watermark-advance entry carrying no mutation, applied like a lock-only
PREPARE's `alignRaftCommitTS`; the backup timestamp floor,
`persistBackupTimestampFloor` / `verifyBackupTimestampFloor`, is the
precedent). After it applies, the fence rejects any later apply at
`≤ s` on that group (a delayed `c1` retries with a fresh timestamp above
`s`, the fence's normal path), so the read at `s` is stable and the
"every version at or below the watermark is applied" invariant holds
per group for `s`. Three rules bound it (each a review finding). **Provenance.** `GetAt` and
`ScanAt` take a bare timestamp and cannot tell a coordinator snapshot from
a gRPC client's `Ts`, so the advance does not live in them: it lives in a
separate snapshot-read API on `ShardStore` whose snapshot argument is a
type only the coordinator can construct (unexported constructor), so a
client-supplied timestamp can never reach it. A forwarded snapshot read to
another node's group leader carries the marker on the node-to-node
`Internal` RPC only, never on the public `RawKV` / `TransactionalKV`
request types, whose handlers cannot set it (the public gRPC listener
having no authentication is §6.2's problem, not this one's). Only such a
**cluster-issued** snapshot may advance a watermark, and the advance is
bounded by the leader's HLC (a timestamp above what the local clock could
have issued is refused, never persisted). A client timestamp above the
group's watermark on the gRPC reads is **rejected** (`FailedPrecondition`,
"read timestamp above the group's applied watermark"; a client that wants
the latest state passes `Ts = 0`), not silently served at a lower snapshot,
because the responses carry no effective read timestamp and a later write
at or below the requested `ts` could change the answer; without the
rejection one request with a far-future timestamp would fence every write
on the group until wall time caught up. **The empty-store sentinel.**
`snapshotTS` returns `^uint64(0)` ("latest") when `LastCommitTS()` is 0;
that value is a non-advancing read sentinel, normalised before the
advance, so the first read against a fresh store returns empty instead of
failing the HLC bound. **Authoritative snapshots.** The snapshot itself
must come from the touched groups' **leaders**, not from the coordinating
node's local replicas: a local replica of group B can lag behind B's
leader, which has already applied and acknowledged `c1`, and a proxied
leader read at a stale `s < c1` passes its ReadIndex barrier and still
returns the pre-`c1` value, a stale read that began after the write
completed. So a transaction's `s` is the maximum of the leader watermarks
of the groups it touches, each obtained after that group's read barrier
(the lease read already returns the leader's `lastCommitTS`;
`GroupCommittedTimestampFloor` / `RawLatestCommitTS` are the authoritative
per-group values the TSO floor uses), and only then are the lagging groups
advanced to `s`. A lagging-replica regression test (coordinator on a
follower of B, B's leader ahead) is required. The coordinator's own
snapshot reads (the adapters' `snapshotTS` / `globalSnapshotTS`,
`txnStartTS`, `nextTxnReadTS`, `readTS`) are the trusted callers; the
group leader proposes the advance when `s > group.LastRaftCommitTS()`,
waits for its local apply, then reads; single-group reads (`s` is that group's own watermark) never pay,
and an idle group pays one entry per distinct `s` it is read at, in the
same RPC as the read when the leader is remote. The rejected alternative,
`StartTS` = the minimum of the touched groups' watermarks, is stable
without extra entries but livelocks against an idle group: its watermark
never advances, every read on a busy group at that stale `s` fails
validation, and transactions spanning the two never commit. A second
alternative, per-group read timestamps (each read key validated against
the watermark its group had when it was read, carried on the wire),
avoids the entry but gives up the single-snapshot model the TLA+ spec and
this document rely on; it is recorded here in case the entry cost matters.
Needed before merge: the advance entry and its apply, the `ShardStore`
read path, a regression run of the reproduction (green with `W_B` fenced
or seen), and a test that a single-group read proposes nothing. An alternative that avoids rejections, serialising
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
- Cross-group snapshots: the adapters' global-maximum snapshot is not
  honoured by a lagging group's fence (reproduced above); the
  watermark-advance read path is the fix, on
  `design/serializable-audit-a0-multigroup` once it exists.
- Not yet leader-issued: the 2PC ABORT timestamp (unfenced) and the
  pre-allocated paths above; making delta keys independent of the commit
  timestamp would remove the exemption.
- One more 8-byte meta key per replicated apply batch, not benchmarked.
- **The streaming MVCC and in-memory snapshot formats must carry the
  replicated watermark (found in review).** Today they omit
  `_meta_last_raft_commit_ts` and the restore falls back to
  `LastCommitTS()`, which direct writes such as `CatalogStore.Save` advance
  on their own; a replica restored from such a snapshot then fences a
  committed entry at or below that direct-write timestamp while replicas
  holding the true replicated watermark apply it, a divergent FSM. Every
  Raft snapshot format carries the key, and the fallback survives only for
  a snapshot produced before the fence existed, which the rolling-upgrade
  gate keeps from being restored after activation. A merge blocker for
  A0, with a restore-then-apply test on each format.

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
  string key, and collection reads record what a transaction would: `HGET` /
  `HMGET` / `HEXISTS` the field keys, `LINDEX` / `LRANGE` / `LLEN`,
  `SMEMBERS` / `SISMEMBER` / `SCARD`, `ZSCORE` / `ZRANGE` / `ZCARD`, and the
  stream reads the collection's fence key (a concurrent field write or
  emptying path bumps it, per the G7 fence rules), all capped by
  `kv.maxReadKeys`; scripts over the cap fail closed. The A1 Redis branch
  covers string reads only, so collection reads stay a blocker (a script
  reading a hash field and writing another key still commits against a
  concurrent change of that field).
- Redis: bump the collection fence on every emptying path
  (`deleteLogicalKeyElems` callers, `HDEL`, update-only `HSET`) and read the
  fence keys in `SETNX`, the legacy creates, the `MULTI` type probes for
  absent keys, and the two absent-key creators the matrix marks `N` that the
  first pass omitted, `PFADD` (HLL key) and `XADD` (stream meta): each
  probes the type and then writes only its own encoding, so an absent-key
  `XADD` racing `HSET` or `RPUSH`, or `PFADD` racing `SADD`, commits two
  encodings under one logical key (G7). Two of the fixes are already known to be small: in a
  throwaway copy, adding `redisStrKey(key)` to the Lua context's read keys on
  a string read, and making `deleteListElems` `Put` the list fence, turned
  the G3 and G7 reproduction tests green (the first attempt conflicts, the
  retry sees the concurrent write).
- S3: in `completeMultipartUpload`, re-read the part descriptors at
  `StartTS` **and list every consumed part key in `ReadKeys`** (or, if the
  part count can exceed `kv.maxReadKeys`, a per-upload parts fence that every
  `uploadPart` `Put`s and the completion reads): the re-read alone leaves the
  window between it and apply, in which a same-part re-upload commits
  without touching the upload meta. In `uploadPart`, surface the bucket meta
  and **re-read the previous part at `StartTS`** (or validate it against the
  earlier `readTS` it was actually read at), listing both keys in
  `ReadKeys`: listing alone does not help when the value was read before
  `StartTS`, because a commit between the old `readTS` and `StartTS` is not
  newer than `StartTS` and passes validation (G2, in addition to the
  bucket-meta read keys above).
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

Status of the S3 half: landed on branch `design/serializable-audit-a1-s3`
(from `main`, not pushed): `3a975625` adds
`adapter/s3_multipart_occ_repro_test.go`, four cases through the real FSM
and `ShardedCoordinator` with a test-only store wrapper that parks a
handler after a chosen `GetAt` (no production hook), all red on `main` 5 of
5: `completeMultipartUpload` racing a same-part re-upload, parked after the
part-descriptor read and again between its last read and its commit,
commits a manifest whose chunk is missing and serves the object as HTTP 200
with an empty body; `uploadPart` racing `deleteBucket` lands the part and
chunks under the deleted generation (only when the delete's best-effort
cleanup sweep misses; when it succeeds the upload-meta read key already
protects `main`); two same-part uploads in the window leak one set of blobs
and clean the other twice. `b3277347` fixes all three: the completion has
no early read phase any more, each attempt reads bucket meta, upload meta,
every requested part, and the head manifest at its `StartTS`, then
allocates the commit timestamp and dispatches with every consumed part key
in `ReadKeys` (a per-upload parts fence was rejected because every
`uploadPart` would write it and parallel parts of one upload would conflict
with each other); `kv.maxReadKeys` is raised so a 10,000-part completion
plus its bucket-generation fence and upload meta fit (today the cap is
exactly 10,000 and `dispatchTxn` checks the total, so the maximum-size
upload would fail as an invalid transaction; the staged-migration alias
doubling still halves what fits during a migration); a conflict retries
the attempt and either commits the new part version (same ETag) or fails
`InvalidPart`. `uploadPart` re-reads the bucket meta in the commit phase
(404 `NoSuchBucket` / `NoSuchUpload` when the generation changed) and reads
the previous part at that same watermark, listing upload meta, bucket
meta, and the part key in `ReadKeys`. `0ef1098a` pins the read sets in 15
table cases. Undoing each part of the fix turns its reproduction red
again; `adapter`, `kv`, `store` green under `-race`; lint clean; one
pre-existing gosec `//nolint` removed. Cost: `uploadPart` 4 → 5 point reads
and +2 read keys; a completion with N parts N+5 → N+3 reads on the first
attempt and N+3 per retry (was 3), +N read keys on the entry. Three follow-ups
landed after review of the result. `de3eeb95`: the bucket-meta read key
made `uploadPart` conflict with every concurrent writer in the bucket
(`putObject`, `createMultipartUpload`, completion, ACL changes) for the
whole body upload, unacceptable for multipart, so the fence is now
`BucketGenerationKey`: `deleteBucket` and `AdminDeleteBucket` share one
write set (`bucketDeleteTxnElems`) that deletes the bucket meta, still the
emptiness fence, and re-puts the generation key at
`max(stored, deleted generation)` (never deleted, never lowered; a legacy
bucket without one gets it at the deleted generation so its recreate no
longer restarts at 1), and `uploadPart`'s `ReadKeys` are the upload meta,
the generation key, and the part key. The bucket meta is still re-read at
the commit snapshot but no longer listed: a delete does not change the
generation key's value, so a delete committed between the prepare read and
`StartTS` leaves no version newer than `StartTS`, and only the re-read
catches it (404 `NoSuchBucket`, or `NoSuchUpload` after a recreate);
deletes and recreates after `StartTS` conflict on the key (503). A
regression test holds a part upload while a `putObject` and another
`createMultipartUpload` commit in the bucket (red before, 200 now), and
the delete race covers both orders with mutation checks. `659cc2df`: an
OCC conflict that survives the internal retries is reported as HTTP 503
`SlowDown` by `uploadPart` (was 500) and `completeMultipartUpload` (was
409 `OperationAborted`), the classes `retryS3Mutation` already treats as
transient; other handlers keep 409. `7f84c4cf`: `kv.maxReadKeys` becomes
10,000 + 240 headroom (a 10,000-part completion's read set is exactly the
part keys, the upload meta being in its write set, so it fitted the old
cap at the boundary; the headroom is defensive), documented with the
migration-alias halving, and a real 10,000-part completion commits through
Raft in a test. `adapter` and `kv` green under `-race` after each commit;
lint clean. Point reads per request against `main`: `uploadPart` 4 → 5;
completion with N parts N+5 → N+3 on the first attempt and 3 → N+3 per
retry; bucket delete +1. A maximum-size completion's Raft entry grows by
about N × 70 B plus the object key (up to about 11 MiB at 10,000 parts),
within the gRPC limit but on etcd/raft's oversized-entry path. Known
limits: in a multi-group deployment where the
bucket generation lives on another group, `uploadPart` becomes a 2PC
transaction and its generation check is subject to G1 until A2 lands; a
completion of more than about 4,998 parts during a staged-visibility
migration exceeds `kv.maxReadKeys` (the migration aliases every key) until
the migration ends; the Jepsen S3 workload does only PUT / GET and was not
run.

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

The collection half of the Lua and creator work is on
`design/serializable-audit-a1-redis-collections` (five commits on the Redis
branch, not pushed). `b69ce8f2` adds two reproductions through the real
FSM: a script that reads a collection and `SET`s another key from what it
read, racing an update of that collection (`HGET` versus an update-only
`HSET`, `HGETALL` versus a new field, `LRANGE` versus `RPUSH`, `SMEMBERS`
and `SISMEMBER` versus `SADD`, `ZSCORE` versus a score update; all
committed the stale value, `XLEN` was already covered by the stream meta
key), and two absent-key creators held after their first read while the
other commits (`XADD` versus `HSET` and `RPUSH`, `PFADD` versus `SADD`, and
the reverse orders; each ended with two encodings under one key, e.g.
`[list stream]`). `c6056fc8` records collection reads as read keys, once
per key per script: the list fence for `LLEN` / `LINDEX` / `LRANGE` /
`LPOS`; the set fence for `SMEMBERS` / `SCARD`, plus the member key for
`SISMEMBER`; the zset fence and the legacy zset blob key for the range and
count reads, plus the member key for `ZSCORE`; the hash fence and the
legacy hash blob key for `HLEN` / `HGETALL`, plus the field key for `HGET`
/ `HEXISTS` / each `HMGET` field; the thirteen-key absent set (four fences
and nine type anchors, shared with `EXISTS` / `TYPE` / `PTTL`) for any
collection read that finds the key absent and for an empty
`ZRANGEBYSCORE` fast path, which cannot tell absent from empty. The
legacy blob keys are recorded because some legacy-blob writes skip the
fence. `5eb20a7e` gives `PFADD` and `XADD` (dedup, legacy, and the Lua
`XADD` create path) the `redisAbsentKeyCreateGuard` the dedup `SET`
create uses: read the absent set and `Put` the four fences (the `Put` is
what makes a `SADD` / `RPUSH` committing second conflict). `a7f2dafe` and
`7b23b0fb` pin the read sets in 39 cases and the creator fences in 8.
Results: all 15 interleavings green (the first attempt fails with a write
conflict and the retry sees the new value, or returns `WRONGTYPE`);
`adapter` green under `-race` (19 min); the two expected-red A0 / A2
reproductions the branch carries still fail; lint clean. Cost: 1 read key
per list / set read, 2 per zset / hash read, +1 per distinct element for
point reads, 13 per absent key; a creator adds 13 read keys and 4 `Put`s;
appends add nothing; an `HGET`-heavy script that writes now reaches
`kv.maxReadKeys` at about 9,998 distinct fields of one hash, 3,333 present
hashes, or 769 absent collection keys and fails closed; the empty
`ZRANGEBYSCORE` poll (BullMQ's delayed-queue loop) costs 13 read keys.
What range reads can and cannot detect, checked against every write
path: lists, sets, zsets, and streams are complete (every push, pop, trim,
membership change, score change, logical delete, type change, and stream
write touches the type's fence or meta); **hashes are not**: `HGETALL` and
`HLEN` cannot see an update of an existing field (`HSET`, `HMSET`,
`HINCRBY`) or a wide-column `HDEL` that leaves the hash non-empty, because
those write only the field key and a per-commit delta key. Closed by
`01e1bdb9`: `HGETALL` records every field key it returns (`HMGET` already
did per field), so value updates of existing fields are seen; every
`HDEL` that removes a field, emptying or not, on the wide-column and
legacy-blob paths, `Put`s the hash fence, so `HLEN` (which depends only on
the field set) and `HGETALL` see removals; value-only writers (`HSET` of an
existing field, `HMSET`, `HINCRBY`) stay fence-free. Three more
interleavings red then green (`HGETALL` versus update-only `HSET`,
`HGETALL` and `HLEN` versus a non-emptying `HDEL`), three read-set pins,
five `HDEL` fence pins; removing only the fence `Put`s turns the `HLEN`
case and the fence pins red again. `adapter` green under `-race`; lint
clean. Read keys on a present hash: `HGETALL` of N fields 2 + N, `HLEN` 2,
`HGET` 2 + 1 per field, `HMGET` of M fields 2 + M. A script that
`HGETALL`s one hash and `SET`s one new string key fits up to 9,994 fields
and fails closed from 9,995 (hashes can hold 100,000 fields, so a large
`HGETALL` in a writing script now fails where it used to commit without
validation). Cost on the write side: `HDEL` now conflicts with every other
writer of the same hash that `Put`s or reads its fence (concurrent
`HDEL`s, `HSET` adding a field, the dedup `HSET` / `HMSET` of an existing
field and standalone `HINCRBY`, which read the fence), which retry when an
`HDEL` commits first. This is the balance between penalising only the
scripts that read whole hashes and serialising every writer of a hot
hash. Standalone `HGETALL` outside scripts and hash reads in `MULTI` are
outside this change. Still open after this branch: `SETNX`, `SET NX` /
`SET XX`, and the legacy string creates record nothing (a held `SETNX`
against a `PFADD` / `XADD` that commits first still creates two
encodings; the reverse order is now caught), reads that hit `WRONGTYPE`
under `pcall` record nothing, and Lua probes that resolve to a collection
type record nothing.

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
encoding version), used as
the owner test in `handlePrepareRequest` and the read-lock checks, and as
the suffix of the read-lock row key and of the commit / rollback record
keys (`primaryKey` stays in the key so the resolver still finds the
primary's status). Ownership is decided per request: a request with a `TxnID` owns exactly
the locks and records carrying that `TxnID`; a request without one (a
legacy transaction) owns locks and records without one under the old
`(primaryKey, StartTS)` test, permanently, not only while the gate is off,
so a transaction prepared before activation finishes its COMMIT, ABORT,
or retry normally and the resolver settles legacy locks from their
primary as today; a legacy lock against a `TxnID` request, or the
reverse, is foreign. The gate exists for FSM determinism, not ownership:
every replica must write the same row for the same entry, so the
coordinator starts assigning `TxnID`s only once every node runs the new
binary, and transactions already in flight at that moment complete under
legacy identity. This is a wire and key-format change, so it ships behind
the same rolling-upgrade capability gate as the fence.
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

`DEL_PREFIX` now conflicts with transaction locks, on
`design/serializable-audit-a2-delprefix-locks` (two commits on the read-lock
fix, not pushed): `e0c839bf` adds `kv/txn_delprefix_lock_repro_test.go`
(T prepares with read key `k` and write key `x`; a flush over both applied,
deleted the committed `k`, kept the intent, and T then committed
`x`), `71334583` adds `assertNoTxnLocksUnderPrefix` to
`handleDelPrefixWithFloorSnapshot`, after the reserved-prefix, route-fence
and floor checks and before `DeletePrefixesAtRaftAtFenced`: one range scan
of `txnLockKey(prefix)` for write locks (one seek when none is held), then a
paged scan of the whole `!txn|rlock|` namespace filtered by prefix (the
rows are length-prefixed, so a prefix cannot be seeked; pages 8 → 1024).
The first matching row fails the apply with `TxnLockedError` ("write lock"
or "read lock") and nothing is deleted; a key under `!txn|` never counts,
matching what the delete skips. No coordinator change: nothing resolves or
retries `TxnLockedError` for raw requests today, `DEL_PREFIX` is a
per-group broadcast whose copies apply independently (re-running it would
re-apply the landed copies), and every caller (Redis `FLUSHDB` /
`FLUSHALL` through `retryRedisWrite`, the DynamoDB deleted-table cleanup,
the S3 bucket-delete safety net) already retries the whole flush on that
error. Tests: 28 table subtests (write lock, read lock, both, lock outside
the prefix, nil prefix, `!txn|` user keys; resolved by COMMIT and by
ABORT; in-memory and Pebble), a paging test with a read lock behind 1124
unrelated rows, and a single-group Raft test that a dispatched flush fails
with `ErrTxnLocked` and deletes nothing, then succeeds after COMMIT.
`store`, `kv`, `adapter` green under `-race` (the A0b flake did not fire);
lint clean. Limits: the read-lock scan grows with the group's in-flight
read sets (FLUSHDB runs it five times, the S3 safety net seven), accepted
because a prefix delete already scans everything under its prefix; locks on
migration-staged keys are skipped as on the raw point path; and a
rejection that crosses `Internal.Forward` loses its type as a gRPC status,
which Redis re-parses but the DynamoDB cleanup and the S3 safety net do not
(they give up and log), a pre-existing property of every forwarded
`TxnLocked` or fenced conflict that the A0b `Forward` work should carry
across the wire.

The transaction identity (G12) is implemented on
`design/serializable-audit-a2-txnid` (the G12 reproduction cherry-picked
onto the `DEL_PREFIX` branch, then three commits, not pushed). `047daa2d`
adds `bytes txn_id = 8` to `Request` (`buf breaking` clean against the
base; present on every 2PC PREPARE / COMMIT / ABORT and on the resolver's
COMMIT / ABORT, which reuse the lock's own id; `abortRequestFor` copies it;
the FSM accepts an empty id or exactly 16 bytes). `b36972e6` makes locks,
read-lock rows, and records identity-aware: `txnLock` version 2 appends
the id (a lock without one is still written as version 1, byte-identical
to before); read-lock rows with an id are keyed `uvarint(len(key)) key
txnID` and the holder is read from the row value, so a release deletes a
row only if the value names the releasing transaction (both attempts of
one `Dispatch` share the row, and a late ABORT of the first attempt must
not free the second's lock); commit and rollback records with an id live
in two new namespaces, `!txn|cmtid|` and `!txn|rbid|` (`primaryKey
startTS txnID`), because a legacy record key is `primaryKey || startTS`
with an unbounded primary key and no layout inside `!txn|cmt|` can be told
apart from a legacy key with a longer primary, and `txnRouteKey` must
recover the primary from both shapes for routing, migration filtering, and
backup ownership; the two prefixes are wired into `isTxnInternalKey`, two
migration families appended to the enum, the known-internal prefix list,
and the write-conflict metric buckets, and user keys under them become
reserved. Ownership requires id, primary key, and `StartTS` all to match
(the composed-1 retry reuses the id under a fresh `StartTS`, so a leftover
lock from the earlier attempt must stay foreign); a legacy request owns a
legacy lock under the old pair test, permanently; legacy against id, or
the reverse, is foreign; `WithTransactionIDs` (default on, an atomic read
once per `Dispatch`) gates only whether the coordinator assigns ids, and
plugs into the capability-monitor pattern of `main.go`
(`startStorageEnvelopeV2CapabilityMonitor`, the encryption fan-out) with
the migration families covered by the same gate. `9822ec6d` makes the
first primary COMMIT require its own lock on the primary key (otherwise
`TxnLockedError` "primary lock lost", nothing written;
`settleFailedPrimaryCommit` then writes the transaction's own rollback
record and aborts the other groups), for legacy transactions too;
`completeCommittedTxn` runs only for the transaction's own record. The
reproduction is green 3 of 3 (T2's PREPARE on the primary fails
`TxnLocked`, its cleanup ABORT leaves T1's lock alone, T1 commits, final
`p="t1"`, no rows left) and renamed
`TestTwoPhaseCommit_SharedPrimaryAndStartTSStayDistinctTransactions`;
`kv/txn_identity_test.go` covers the ownership matrix (as a function and
through PREPARE), both codec versions and malformed input, key shapes and
routing, idempotent re-PREPARE, a foreign record never taken as one's own,
the primary-lock rule for id and legacy transactions, the late-ABORT
release check, one id per `Dispatch` including the composed-1 retry, none
with the option off or for one-phase, `settleFailedPrimaryCommit` aborting
the secondaries on a foreign record, a legacy transaction in flight at
activation completing while a new one with the same pair conflicts, and
the resolver settling both shapes from their own records. `store`, `kv`,
`adapter`, `proto`, `distribution` green under `-race` (one earlier run
hit only the known A0b flake); lint clean. Cost: one 16-byte
`crypto/rand` read per multi-shard `Dispatch`, +24 bytes per lock payload,
no extra Raft round-trips; `BenchmarkAssertNoConflictingTxnLocks` 2.2 /
29.7 / 328 µs at 0 / 100 / 1000 released locks, in line with the tombstone
numbers above. `dedupProbeOnePhase` is unchanged. Rollback records now
also cover the cleanup ABORT after a failed PREPARE (records were never
collected before either). Found while implementing, recorded as G14:
`prewriteTxn` prepares groups in group-id order rather than primary first.

Constraints the fix PR must close before merge:

- **Read-lock installation versus foreign write locks (found in review):
  closed.** Reproduced and fixed on
  `design/serializable-audit-a2-readlock-vs-writelock` (status above); the
  TLA+ half is `design/serializable-audit-a3-shards`, whose
  `MCOCC_gap_readlock_intent.cfg` fails `OCC8` on this interleaving without
  the rejection (A3). Still open from it: the orphaned-write-lock wait is
  bounded only by the resolver's TTL sweep.
- **Transaction identity (G12, found in review, reproduced): closed** on
  `design/serializable-audit-a2-txnid` (status above). Still open from it:
  the rolling-upgrade gate (A7).
- **PREPARE order (G14, reproduced).** Three changes, on
  `design/serializable-audit-a2-prepare-order` over the reproduction:
  (1) `prewriteTxn` prepares the **primary group first** and sends the
  secondaries (write groups, then the lock-only read groups) only after the
  primary PREPARE has applied, so a secondary lock never exists without the
  primary lock or a record on the primary group; (2) `primaryTxnStatus`
  and `backgroundPrimaryTxnStatus` conclude "rolled back" only from a
  rollback record, or from an expired primary lock that
  `tryAbortExpiredPrimary` then aborts (writing the record); "no lock and
  no record" is **pending**: the reader returns `txn locked` (and retries
  as for any live lock) and the resolver skips the row, since with
  primary-first ordering that state is either a transient the reader
  should wait out or an orphan whose coordinator died between the primary
  PREPARE and the secondaries, which the primary lock's TTL then settles;
  (3) a secondary COMMIT that finds no lock verifies the primary's commit
  record and **re-applies the write from the COMMIT entry**, which
  therefore carries the mutation values (today it carries keys only), and
  fails loudly if it cannot, instead of returning success with nothing
  written; the primary's commit record is the commit point, so any earlier
  abort of a secondary was wrong and re-installing is correct. Tests: the
  reproduction green (both read and resolver variants), the scan path
  variant added, a coordinator-crash-between-PREPAREs case settled by the
  primary TTL, and the COMMIT re-apply path.
- **`DEL_PREFIX` ignores locks (found in review): closed** on
  `design/serializable-audit-a2-delprefix-locks` (status above).
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
- Pre-existing: raw `DEL_PREFIX` bypassed lock validation (closed above).
  Ordinary RAW `PUT` / `DEL` already check write locks
  (`validateRawMutationForApply` → `assertNoConflictingTxnLock`) and
  deliberately skip read locks: a raw write carries no read set, so it can
  always be serialised after the transaction holding the read lock (a raw
  write derived from an earlier adapter read is a G7-class bug on its own,
  fixed by making that path a transaction with `ReadKeys`). A coordinator
  stalled past the TTL can have its primary lock treated as a rollback (true
  for write locks too).

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
- Found while fixing it and **fixed** on `fix/jepsen-sqs-error-codes`
  (three commits on the branch above, not pushed): `sqs-invoke!` read the
  error code from `:__type`, but this cognitect SQS client (query protocol)
  puts it under `:cognitect.aws.error/code`, so every SQS error was `:info`
  with `:cognitect.anomalies/incorrect`, the `:fail` classification and the
  `QueueAlreadyExists` fallback never triggered, and the missing-queue code
  is `AWS.SimpleQueueService.NonExistentQueue` (conservative: no false
  pass). `53116eb8` reads the query-protocol key first with a `:__type`
  fallback and records `:fail` only for an allow-listed 4xx code
  (`MissingParameter`, `InvalidParameterValue`, the two missing-queue codes,
  the two already-exists codes, `ReceiptHandleIsInvalid`,
  `InvalidIdFormat`), everything else staying `:info` (5xx, throttling,
  transport faults, anomalies without a code), the same shape as the
  DynamoDB workloads; the tests stub `aws/invoke` and build the error map
  with aws-api's own `parse-http-error-response` from the XML elastickv
  writes (24 assertions red first). `8c78b611` wraps the drain generator in
  `gen/clients` (the nemesis worker was taking `:recv` ops and logging
  `:jepsen.nemesis/invalid-completion`, 50 lines and 25 crashes per run
  before, 0 after; reproduced with a small virtual-time copy of the
  interpreter loop since 0.3.13 has no `jepsen.generator.test`).
  `a3914bfb` corrects the `dynamodb_multi_table_workload.clj` comment:
  `setup!` runs once per node, concurrently; that is safe because
  `verify-multi-group-routing!` only reads and the losers of the
  concurrent `CreateTable` race get the ignored `ResourceInUseException`.
  Full `lein test`: 156 tests, 428 assertions, green. A live probe against
  a never-created queue now records `:fail` with
  `AWS.SimpleQueueService.NonExistentQueue`; a 30 s run is `:valid? true`
  with 66 `:ok` sends and 66 received. Not yet run under faults.
- Found in that pass and **fixed** on `fix/jepsen-final-generator` (two
  commits on the branch above, not pushed): Jepsen 0.3.13 never reads
  `:final-generator` (verified in the jar: only producers such as
  `nemesis/combined.clj` and `tests/cycle/append.clj` set it, nothing in
  `core`, `cli`, the interpreter, or the compiled `generator` classes
  consumes it, and `store` even serialises it to disk, which is where the
  "can't fressian-serialize some combined final gens" note came from). So
  no elastickv workload ever ran its nemesis heal (`:stop-partition`,
  `:resume :all`, `:start :all`): a faulted run could end mid-partition and
  the HT-FIFO drain could run on a partitioned cluster; the zset-safety
  final `:zrange-all` never ran, so every mutation acknowledged after the
  last main-phase read began was unchecked and a lost tail write under
  faults passed (main-phase reads, about 35% of ops, did exercise the other
  properties); and the `append/test` final reads were dropped by the same
  merge. `09344f11` adds `elastickv.cli/with-final-phases` (time-limited
  main phase, then the nemesis package's final generator on the nemesis
  worker, a 10 s recovery wait when a client phase follows, then the
  workload's final client generator) and routes all seven workloads
  through it (the HT-FIFO drain becomes the final client phase after the
  heal); `0954de30` makes the zset-safety checker require a successful
  `:zrange-all` that began after every mutation completed (`:valid?
  :unknown` with a reason otherwise) and retries the final read for up to
  30 s. Virtual-time interpreter moved to `generator_simulation.clj`; 22
  assertions red first; full `lein test` 163 tests, 495 assertions, green.
  A 30 s zset-safety run passes with the final read last in the history
  and seeing two mutations the last main-phase read missed. Still off: the
  `append/test` final reads in the list-append workloads (they need
  `:wrap-generator max-key-tracker` around the whole generator, which
  changes Elle's input; enabling them is the recorded follow-up, since
  without them Elle cannot see the final state). Also found: in the HT-FIFO
  `:recv` branch a `DeleteMessage` that errors but actually committed (an
  ambiguous timeout under faults) drops its tuple, so the message is never
  redelivered and the checker would report a false `:lost` (not yet
  fixed).
- **A real anomaly found while verifying that fix (G13 in §3).** The
  DynamoDB multi-table list-append workload in the M5 topology (one
  process, two groups, route-shuffle nemesis, `--local`, 30 s) fails with
  `G-single-item`, `G0`, and `incompatible-order`: a read of key 11 misses
  an earlier-read append. Diagnosed as the stale-reapply fast path
  swallowing a 2PC PREPARE at the previous commit's timestamp (G13; the
  split plays no part, and the workload fails without the nemesis too),
  fixed by A0's index-based replay detection. Stores:
  `jepsen/store/elastickv-dynamodb-append-multi-table/20260926T235756.006+0900`
  and `.../20260927T000015.355+0900` in that worktree.
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

Status: implemented ahead of review on
`design/serializable-audit-a6-outcome-unknown` (five commits from `main`,
independent of the A0 to A2 stack, not pushed): `05d1091d` (the sentinel),
`c1d40b56` (no server-side retry), `7dba0cac` (every wire protocol),
`32e34b4b` (resolution by log position), `c74920c2` (an abandoned
proposal's unknown outcome stays visible; the raw batch proposes under a
30 s timeout instead of `context.Background()`, without which a batch
pending on an isolated former leader blocked the single flusher goroutine
and every later raw commit behind it until the partition healed). Numbered
after A5 because it was found by A4's pause nemesis after the milestone
list was written. What landed, and where it refines the design below:

- Engine (`internal/raftengine/etcd/pending_writes.go`):
  `trackAppendedEntries` records each pending proposal's and membership
  change's `(index, term)` from `Ready.Entries` before anything in that
  `Ready` applies; an entry is ours only if it carries the request id
  **and** the term it was proposed in, because ids are per-engine counters
  and a successor's entry can reuse one (the old id-based `resolveProposal`
  went away). `settlePendingAt` runs on every applied entry: same term at
  the index is success (with the FSM response or the conf-change peer);
  a **committed and applied** entry of a different term is the definite,
  retry-safe `errNotLeader`. A local overwrite of the index is deliberately
  **not** enough: with five or more voters the old leader's entry `(i, t)`
  can be overwritten on the old leader by `(i, t+1)` from a successor that
  then dies uncommitted, and a third leader with the more up-to-date log
  can still commit `(i, t)`, so answering `errNotLeader` at the overwrite
  would let a client retry and apply twice; waiting for the committed
  entry is always safe and only later. A snapshot covering the index is
  unknown; a locally taken snapshot cannot cover an unresolved index
  because it is taken at the applied index. Leadership loss now fails only
  pending reads; shutdown and `fail` give pending writes the unknown class
  with the cause kept in the chain; a context that expires after the entry
  reached the log yields unknown, before it a plain context error, with no
  race against resolution (stores refuse an ended context, sends happen
  under the pending lock). Rejections before the log stay definite.
- kv (`kv/outcome_unknown.go`): `IsOutcomeUnknown`, the wire prefix,
  `markForwardedOutcomeUnknown`; `isLeadershipLossError` and
  `isTransientLeaderError` check the class first so the "not leader" text
  match cannot reclassify it; `finalDispatchErr` never swaps an unknown
  outcome for an earlier `NOTLEADER`; the forwarding clients
  (`LeaderProxy.forward`, `Coordinate.redirect`,
  `leaderAdminProposer.forwardAdmin`) re-mark a `codes.Aborted` status
  with the prefix and `forwardFailureDecision` / `runAdminForwardCycle`
  treat it as terminal (they used to resend any non-transient error three
  times); lease invalidation also fires on it; a TSO control entry's
  unknown outcome becomes a definite `ErrTSONotLeader` because no
  timestamp was handed out; `commitRaw` reports an abandoned batched item
  as unknown.
- Adapters: Redis `-OUTCOMEUNKNOWN <message>` checked before `NOTLEADER`
  (a follower relays a leader's reply from `MULTI` / `EXEC` as the
  top-level reply); gRPC `RawPut` / `RawDelete` / `Put` / `Delete`,
  `Internal.Forward`, `ForwardAdminProposal`, and the encryption admin
  (which used to fall through to the retryable `Unavailable`) return
  `codes.Aborted` with `proposal outcome unknown: …` as a raw status so the
  prefix survives; DynamoDB (JSON `__type`), S3 (XML `<Code>`), and SQS
  (JSON, query-protocol `<Code>`, batch entries with `SenderFault=true`)
  return HTTP 400 `RequestOutcomeUnknown` with the message "the request may
  or may not have been applied; read the current state before retrying",
  engine detail logged, not echoed.
- Evidence: engine integration tests (committed by the successor →
  success with the FSM response; a different-term entry committed at the
  index → `errNotLeader`; pending past leadership loss → unknown only at
  context expiry; shutdown → unknown with `errClosed` kept; follower
  propose → definite `ErrNotLeader`) and unit tests (settle by position,
  the config variant, a colliding foreign id ignored, snapshot → unknown,
  the abandon cases), all red first (`main` returned "is not leader" / "is
  closed"); kv classifier tables, forward re-mark, `finalDispatchErr`,
  lease invalidation, the local and forwarded `LeaderProxy` paths and the
  admin proposer each sending exactly once (before: "leader forward failed
  after 3 retries"), TSO sanitisation, context precedence and the raw
  batch; adapter per-surface mappings, a real `Internal.Forward` round
  trip over gRPC sent exactly once with the prefix not stacked, and a Redis
  end-to-end on a real three-node cluster where the followers' gRPC is cut
  between the handler's read fences and its `Dispatch` (the client gets
  `OUTCOMEUNKNOWN`; on the old engine an i/o timeout, because `Dispatch`
  kept retrying `NOTLEADER`; cutting the quorum before the write is
  correctly a `NOTLEADER`, since nothing reached the log). `internal`, `kv`,
  `adapter` green under `-race`; lint clean. Cost: one map insert and
  delete per proposal plus one lock per `Ready`; no extra round-trips;
  leadership loss no longer fails fast, a write stuck on an isolated old
  leader waits for resolution or its context (`Dispatch` 5 s, Redis 30 s,
  raw batch 30 s).
- Still open: the Jepsen clients recording `OUTCOMEUNKNOWN` /
  `RequestOutcomeUnknown` / `Aborted` as `:info` (A4 branch); the forward
  breaker's retry on `Unavailable` / `DeadlineExceeded` after an RPC was
  sent is a separate double-apply risk (G15); the startup encryption
  rotation now fails on an unknown outcome instead of retrying; the Redis
  migration proxy's leader-aware backend does not refresh its leader on
  `OUTCOMEUNKNOWN` (it never replays commands, so not a safety issue); one
  stale mention of `resolveProposal` in `internal/raftengine/statemachine.go`.

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
  marked with `ErrNotLeader`. Leadership loss no longer fails pending
  proposals at all: the engine **resolves** them. Each proposal is tracked
  by the `(index, term)` it received when its entry appeared in
  `Ready.Entries`; it resolves as success when the entry applies (the
  normal pop by id, whichever leader committed it), as a definite,
  retry-safe `errNotLeader` only when a **committed** entry of a different
  term applies at that index (a local overwrite is not proof, see the
  five-voter case in the status above), and as
  `ErrProposalOutcomeUnknown` when the request context expires, the engine
  stops, or a snapshot that covers the proposal's index is installed before
  either happens: a snapshot proves nothing about the entry (the successor
  may have committed it and compacted), so it stays unknown unless the FSM
  can answer from durable state whether that proposal was applied, which
  this milestone does not add. Pending
  admin / config changes follow the same rule; pending reads keep
  `errNotLeader` (nothing was applied). Pre-proposal rejections are
  unchanged. So the unknown class is the residue after a bounded wait, not
  the first answer.
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
- **Adapters.** The residual unknown class must never be something a
  standard client resubmits automatically: Redis `-OUTCOMEUNKNOWN
  <message>` (a distinct first token, so clients and the Jepsen client
  classify it separately from `NOTLEADER`); gRPC `codes.Aborted` (not
  `Unavailable`, which retry policies treat as safe); DynamoDB, S3, and SQS
  HTTP **400** with the code `RequestOutcomeUnknown` (JSON `__type` for
  DynamoDB, the XML `<Code>` for S3 and the query-protocol `<Code>` for
  SQS), because the AWS SDKs retry every 5xx automatically and would
  resubmit a non-idempotent `UpdateItem`, `SendMessage`, or part upload
  that may already have committed. This deliberately departs from upstream,
  which returns 500 and relies on client idempotency tokens; honouring
  those tokens (`TransactWriteItems` `ClientRequestToken`, FIFO
  `MessageDeduplicationId`, the idempotent S3 `PUT`) is the recorded
  follow-up before any 5xx mapping could be reconsidered. The message says
  the outcome is unknown and that the client should read before retrying.
- **Jepsen.** The A4 clients record it as `:info` (Redis by the first
  token, the HTTP adapters by the `RequestOutcomeUnknown` code), on the A4
  branch.

Tests: an engine test that proposes, forces leadership loss with the
proposal pending, and asserts the sentinel (and that `errors.Is(err,
ErrNotLeader)` is false), next to one that a pre-proposal rejection is
still `ErrNotLeader`; the raftenginetest conformance suite gains the same
case if it has a leadership-loss hook; a `LeaderProxy` table test that the
class is not retried; per-adapter mapping tests; a `Forward` round-trip
test. Merge blocker: none beyond these; the timestamp and read-lock work is
independent.

### A7. Rolling upgrade of the transaction format

Status: proposed; implementation on
`design/serializable-audit-a7-upgrade-gate` on top of the A2 `TxnID`
branch. Every FSM-side change in A0 and A2 is a **semantic** change to
how an entry is applied: the fence rejects an entry, `DEL_PREFIX` refuses
under a lock, read-lock rows are written, `TxnID` rows and records use new
keys and namespaces, the watermark-advance entry exists. A replica on the
old binary applies the same entry with the old semantics, so a mixed
cluster diverges as soon as one such verdict differs, whatever the
coordinator proposes; and a lock-only PREPARE or an advance entry is not
even decodable by the old binary. The capability gate that the earlier
sections defer to is therefore two-sided:

- **Per-entry format flag.** `pb.Request` gains `txn_format` (an enum,
  `TXN_FORMAT_V1` = legacy, `TXN_FORMAT_V2` = this audit's semantics).
  The FSM applies the fence, the `DEL_PREFIX` lock check, read locks,
  `TxnID` ownership and records, and the advance entry **only for V2
  entries**; a V1 entry is applied exactly as `main` applies it today, on
  both binaries, so the two never disagree on a V1 entry. The
  replay-by-index detection is local bookkeeping, not a verdict, and is
  not gated. Snapshot formats always carry the replicated watermark once
  the new binary writes them (a V1-only cluster simply never consults it).
- **Cluster capability.** Every node advertises `txn_format_v2` in the
  capability report the storage-envelope and encryption monitors already
  read; the coordinator flips one atomic `txnFormatV2` (`WithTxnFormatV2`,
  subsuming `WithTransactionIDs`) only when every voter and learner of
  every group reports it, through a monitor built like
  `startStorageEnvelopeV2CapabilityMonitor` /
  `buildEncryptionCapabilityFanout`, and never flips it back. From then on
  it stamps V2 on every request; transactions already in flight keep V1
  and finish under V1 semantics (legacy ownership is permanent, A2). An
  operator flag pins the format to V1 for a deployment that must keep an
  old binary around, with a startup warning that the audit's guarantees
  are off.
- **Refusal on the old side.** A V2 entry must never reach an old binary;
  the monitor makes that a matter of timing only. As a belt to the braces,
  the new binary refuses to *start* a group whose peers include a node
  without the capability while the format is pinned V2 in its data dir
  (`raft-engine` marker, the same file that already records the backend),
  and records the format in the marker once V2 is first proposed, so a
  downgrade after V2 traffic fails loudly (`ErrTxnFormatDowngrade`) instead
  of applying V2 entries with V1 semantics.
- **Tests.** A two-binary simulation is out of reach in unit tests, so the
  evidence is: table tests that every gated verdict is a no-op for V1
  entries and active for V2; a coordinator test that the flag flips only on
  full capability and stays flipped; in-flight V1 transactions finishing
  after the flip; the marker's downgrade refusal; and a Jepsen run of the
  rolling-update script (`rolling-update.sh`, the deploy runbook) across
  the format change under the list-append workload.

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
  `OCC7_NoLostUpdate`, and `OCC8_NoWriteSkew`, and fails all three gap
  configurations as expected: `MCOCC_gap_applyorder.cfg` (`OCC6`),
  `MCOCC_gap_readlocks.cfg` (`OCC8`), and `MCOCC_gap_readlock_intent.cfg`
  (`OCC8` on the foreign-write-lock interleaving).
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
