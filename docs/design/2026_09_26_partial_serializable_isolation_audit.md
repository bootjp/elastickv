# Serializable isolation audit

Status: Partial (read watermark advance, opt-in; see "Partial implementation")
Author: bootjp
Date: 2026-09-26
Related: `2026_09_26_proposed_positioning_and_roadmap.md` §5 and §6.3 (the
claim this audit backs), `2026_08_29_proposed_tso_batch_slot_claims.md` and
`2026_09_02_partial_prephase_d_resolution_evidence.md` (timestamp
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
| G15 | **A forwarded RPC whose reply is lost after it reached the leader is retried (found while implementing A6, reproduced, fixed).** The leader-forward path (`LeaderProxy.forward`, `Coordinate.redirect`, the admin `forwardAdmin`) treated `Unavailable`, `DeadlineExceeded`, a dropped connection, and a reply held past the deadline as retryable even when the request had already reached the leader, which proposed and committed it; the resend was a second proposal with a fresh timestamp, which the A0 fence cannot detect. **Reproduced** (`adapter/forward_lost_response_test.go`, branch `design/serializable-audit-a6-forward-timeout`, commit `9d8cde4d`, on A6): the real `Internal` server over gRPC with a counting transaction manager and a server interceptor that applies the write and then loses the first reply in five ways, against three forward paths, 15 of 15 red ("the forwarded write was applied 2 times (err=<nil>)" for `LeaderProxy` and admin; `redirect` applied once but told the client a retryable error). **Fixed** by `8d1d77a4`: `classifyForwardedWriteError(err, sent)` keeps a never-sent failure retryable, marks a sent-then-lost reply (`Unavailable`, `DeadlineExceeded`, `Canceled`, `Internal`, a context error, a non-gRPC error) as `ErrProposalOutcomeUnknown` while keeping the gRPC code so the breaker still counts it, and passes the leader's definite answers through; "sent" comes from a per-call `grpc.Peer` probe, which grpc-go fills only after `transport.NewStream` succeeded (an empty peer means the final attempt never opened a stream); the startup gate's `Unavailable` is matched by an exact message constant (`kv.StartupGateUnavailableMessage`, now used by `main.go`'s gate interceptors) and stays retryable, so startup rotation and encryption cutover keep working. Green 15 of 15 plus 25 classification cases, a never-sent retry that succeeds once the leader listens, breaker accounting, the caller-deadline case, and the admin path; `kv`, `adapter`, `internal`, and the root package green under `-race`; lint clean. Every write is treated as non-idempotent on this branch (a lost reply on a forwarded PREPARE / COMMIT / ABORT is now unknown rather than retried inside `LeaderProxy`); `commitSecondaryWithRetry` one layer up still re-sends a secondary COMMIT on any error, relying on the per-transaction commit idempotence the `TxnID` branch formalises; the admin HTTP forward got the same classification in `5815c790` (the probe and the lost-reply rule moved into a leaf package `internal/forwardoutcome` that both `kv` and `internal/admin` use; the forward client marks a sent-then-lost reply; the Dynamo, S3, and SQS admin handlers and the leader-side forward responses answer HTTP 400 `RequestOutcomeUnknown` with no `Retry-After` where they answered 503 `leader_unavailable` "retry shortly", and `main_admin.go`'s leader-churn suffix match no longer catches the class; the admin SPA never retries and shows the message; an end-to-end test through the real `AdminForward` gRPC path applies the create once and returns the 400; the startup gate still gets its retryable 503); a gRPC service-config retry policy, if ever enabled, would invalidate the probe (none is). | Closed for the data-plane and admin forwards. A write reaching the leader is applied at most once across a lost reply; the client sees success or the outcome-unknown class. |
| G16 | **A caught `WRONGTYPE` in a Lua script poisons the key's cached state (found while closing G7, reproduced, fixed).** Every Lua state loader (`redis_lua_context.go`) cached an empty, unloaded key state before checking the type and left it in place when the read failed with `WRONGTYPE` under `pcall` / `redis.pcall`; the next command of that type in the same script saw the key as absent, so a second `HGET` or `GET` returned nil and `HSET` / `RPUSH` / `SADD` / `ZADD` / `XADD` on a string, or `INCR` on a hash, succeeded and replaced the value at commit (`pcall(HGET)` then `HSET` turned string `"s"` into a hash). Reproduced (`TestLua_CaughtWrongTypeDoesNotPoisonKeyState`, eight cases red, `e1ff72d7`) and fixed (`fd97de18`, drop a key state whose first load failed) on `design/serializable-audit-a1-redis-setnx`. | A script that tolerates a type error can silently overwrite a value of another type on `main`; independent of concurrency. |
| G17 | **A crash between two requests of one batched Raft entry loses the tail on the restarted replica (found in the A0 review, confirmed from the code, reproduced by the crash test on #1269, closed on `main` by #1269, merged 2026-10-15 as `99a82156`).** `kvFSM.Apply` applies a `raftEncodeBatch` entry request by request (`kv/fsm.go`); each request's `ApplyMutationsRaftAt` persists the entry index in its own Pebble batch (`store/lsm_store.go`), so the durable applied index names the whole entry after the first request; the engine's cold start seeds the applied index from that durable value and drops a non-volatile entry at or below it without calling the FSM (`rawNodeAppliedForOpen`, `applyNormalCommitted` in `internal/raftengine/etcd/engine.go`). The batched entry is the raw multi-key write (`kv/transaction.go` encodes it; `kv/coordinator.go` and `kv/leader_proxy.go` build it). | The restarted replica permanently misses part of a committed entry that its peers applied in full, and a later leader change can serve either state. Closed on `main` by #1269: one entry in one indexed Pebble batch with one applied-index write, the crash test (two-put and `PUT` then `DEL_PREFIX` entries) travelling with it; no longer a merge blocker for the A0 PR, which builds on that shape. |

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
Landed on `design/serializable-audit-a0-multigroup` (four commits over
the reproduction, not pushed): `51de638c` the advance entry (a reserved
FSM tag `0x0f` plus an 8-byte timestamp, chosen because `0x0f` is an
invalid proto wire byte and sits outside the encryption band, applied
through `ApplyMutationsRaftAt` with no mutations so both watermarks become
`max(current, ts)` in the same batch as the applied index; unfenced,
idempotent, a no-op on replay, refused for 0 and `^uint64(0)`, observed by
the HLC so a fenced write retries above it, carried by every snapshot;
**open, a merge blocker for this branch:** the apply must raise the
replicated physical ceiling to the advance's physical half **before** the
HLC observes it, because `NextFenced` floors the physical half at the
ceiling and rejects any value above it, so a value observed above the
ceiling makes every subsequent persistence allocation on the group fail
with `ErrCeilingExpired` until a lease renewal overtakes it, which
frequent cross-group reads from a group whose clock runs ahead would keep
from ever happening (found in review; see the read path below). The raise
is to the observed physical half exactly, not one millisecond above it
(an earlier revision added a millisecond of rollover headroom; withdrawn
in review): every fenced allocation sits at the ceiling's physical
millisecond, because `fencedNowMillis` floors the wall at the ceiling and
the fence rejects a wall at or past it, so a receiver whose ceiling is
raised one millisecond above the sender's value issues above the sender,
the next snapshot taken across both groups carries the receiver's value
back, and alternating cross-group traffic staircases both ceilings by a
millisecond per operation with no bound (the staircase the peer-only
`ForwardRead` token guards against, reached by ordinary traffic); raised
to the value itself, the receiver shares the sender's millisecond and the
ceilings move only with renewals. The rule is not specific to the advance
entry: the same observation happens for every replicated entry that
carries a leader-issued timestamp (PREPARE, COMMIT, one-phase, raw,
`DEL_PREFIX`), through `observeAppliedCommitTS` after each apply and
`observeStoreLastCommitTS` at open, and a 2PC secondary whose process
hosts no member of the primary group never sees the primary group's lease
entries, so a primary on a faster clock poisons the secondary's `last`
with every COMMIT it stamps and starves the secondary's own writes for as
long as the skew lasts (found in review); `kvFSM` therefore raises the
ceiling to the physical half of every timestamp it is about to observe,
on apply, and at open from the replicated watermark alone
(`LastRaftCommitTS`, which a store carries once the ledger epoch has
applied on it; a store without it raises nothing at open and
its first renewal establishes the fence), never from `LastCommitTS()`
(found in review: `observeStoreLastCommitTS` observes `LastCommitTS()`,
which the replica-local `CatalogStore.Save` direct write advances at
`LastCommitTS() + 1` on the one member that took it, so that member
opened with a ceiling and a `last` its peers did not have, and the
applied log said nothing about the difference); the raise is then
deterministic across members, a function of the applied log and the
restored snapshot, so a new leader floors at the same value (`last` at
open may still take the capped `LastCommitTS()`, since a higher floor
only skips values). For that to hold, the ceiling is **one
per Raft group** (found in review: the HLC is one per process and
`SetPhysicalCeiling` kept a single value for every group the process
hosts, so a raise carried by group B's log, its own lease or a value it
observed, lifted the fence for group A on the same process while A's
replicas had agreed to nothing above A's own lease; A's leader then
issued above A's replicated ceiling, and once A's leadership moved to a
process that never applied B's entries, the new leader floored at A's
lower ceiling and could re-issue a value the old leader had handed to a
transaction still in flight, the identity collision G10 describes;
`main` has the same shape in a narrower window, since the groups a
process leads renew out of phase by up to one tick). `HLC` keeps a
ceiling per group id: each group's FSM applies its lease entries, its
advances, and the timestamps it observes to that group's ceiling only;
`NextFenced(G)` takes the issuing group and fences at that group's
ceiling, inside that group's grant (below), so a persistence timestamp
never exceeds a ceiling the issuer's replicas agreed to (corrected in
review: an earlier revision fenced at the lowest ceiling over every
participant group and so required every participant's grant, which the
2PC issuer, the primary group's leader, holds only for the groups it
leads, so a cross-group COMMIT from a leader of the primary alone would
have been refused; the secondaries need no grant from the issuer: a
secondary COMMIT at `c` is bound by A0b's apply-order fence, `c` above
the secondary's replicated watermark, and the secondary's FSM raises its
own ceiling to `physical(c)` as it applies the COMMIT, which is what
keeps that group's next leader from re-issuing below `c`); the
single-group `Coordinate`
and the TSO group keep one ceiling; `PhysicalCeiling()` for monitoring
reports the minimum over the led groups under legacy TSO mode and
`ceiling(0)` under Phase D, and every other reader of the former scalar
names its group (the FSM's snapshot metadata through `hlcCeilingFromHLC`
carries its own group's ceiling; the TSO paths read `ceiling(0)`, see
the read path below). A transaction whose snapshot
came from a faster group therefore waits, on its slower participants,
for their own logs to carry the raise (the advance entry on a read, the
PREPARE on a write, or the renewal below), never for a raise another
group carried. Tests: a process leading A and B where B observes a value
far ahead refuses an allocation for A alone until A's own renewal lands
and never issues for A above A's replicated ceiling; after A's
leadership moves to a process that never applied B's entries, the values
the old leader issued for A and the values the new leader issues are
disjoint; a member that took a `CatalogStore.Save` direct write above
the replicated watermark opens with the same ceiling as its peers. The
disjointness needs one more rule, because a floor is not a grant (found
in review): a leader whose ceiling for `G` was reached by observation,
or by re-applying the previous leader's lease, shares that ceiling's
millisecond with the previous leader, whose allocations at it reach the
new one only inside a later entry (a `StartTS` issued before the
election rides a PREPARE proposed after it), so the new leader would
hand out values the old one already had, the G10 collision across a
leader change (`main` has it: `NextFenced` fences on the wall alone and
the ceiling a new leader applies from the log is the old leader's).
`HLC` therefore keeps per group the ceiling of the last lease entry
this process proposed in its current term, once applied (`grant(G)`,
cleared on leadership loss), and a fenced allocation for `G` requires
`grant(G)` to be at or above `ceiling(G)`: a newly elected leader's
first fenced allocation enters the on-demand recovery below, whose
target clears every floor, and a raise that a later observation carries
above the grant blocks `G` until the next tick or that recovery, one
Raft round trip either way; so a leader only ever issues inside a
window no earlier leader could issue in. Tests: a newly elected leader
whose ceiling came from the previous leader's lease refuses its first
fenced allocation until its own lease entry applies, and the two
leaders' values are disjoint even when the old leader kept issuing
after losing leadership. Open, found in the same review and not
decided here: a raise to the observed value lets the leaders of two
coupled groups issue in one millisecond from independent logical
counters, so a `(physical, logical)` pair is not unique across issuers,
which the dedup probe and the pre-epoch timestamp fast path assume, and
which the OCC boundary assumes too (a snapshot one leader issued equal
to a commit timestamp another leader issued, with the read served
before that commit applied, is a lost update the snapshot check cannot
see); the candidates are an issuer lane in the logical half (a few high
bits per issuing process, the counter below them) and Phase D's single
issuer, and the A0 PR must settle it before the README claim. The
raise and the observation are one
HLC operation, `ObserveApplied(ts)`, not `SetPhysicalCeiling` followed by
`Observe` (found in review): `nextLocked` loads `last` and then the
ceiling, so an allocator running between the two stores would see the
raised ceiling with the old `last` and issue `(physical(ts), 0)`, the
very value a secondary COMMIT at logical zero carries, and during the
ledger-era V1 interval a same-key write at that timestamp is taken for
the already-landed version by the timestamp replay shortcut and
acknowledged without applying. `ObserveApplied` publishes `last` first
and the ceiling second, and `nextLocked` loads the ceiling before `last`,
so a fenced allocation that sees the raised ceiling also sees `last ≥
ts` and issues above it; an allocation that sees the new `last` with the
old ceiling computes a physical half above the ceiling and re-reads the
ceiling once and retries before returning `ErrCeilingExpired`, which
closes the nanosecond window between the two stores without a lock on
the issuance path. Test: a spinning fenced allocator against repeated
`ObserveApplied` calls at logical `0` and at logical `0xffff`, under
`-race`, never returns a value equal to or below an observed one. Two
consequences follow. One, the lease
renewal target must clear a raised ceiling (found in review): one ceiling
value holds 65,536 timestamps and a group's budget is one fresh window
per renewal tick (`hlcRenewalInterval`, 1 s), whatever
`hlcPhysicalWindowMs` (20 s) is; `SetPhysicalCeiling` keeps the maximum,
so a renewal at `wall + hlcPhysicalWindowMs` below a raised ceiling is
ignored, and a busy receiver with no further raise would exhaust its one
window and fail every write with `ErrCeilingExpired` until its wall
caught up, for up to the clock skew between the two groups, which the
admission bound below caps at about one window. Decision: the renewal
target on every group a node renews, in `renewHLCLease` and in the
single-group `Coordinator`, becomes, for each led group `G`,
`max(wall + hlcPhysicalWindowMs, ceiling(G) + 1, physical(last) + 1)`,
so each tick opens one fresh window above a raised ceiling, exactly the
budget an unskewed group has, and every led group agrees within one tick
to everything the process has observed (the per-group ceiling above
makes that agreement the condition for issuing on `G` at all); the
ceiling then drifts by one millisecond per tick
per group while it sits ahead of the wall (coupled groups pass each
tick's raise to each other through observation, so the drift is one
millisecond per tick per coupled group against a wall that moves a
thousand, and the coupled ceilings track the fastest group's
`wall + hlcPhysicalWindowMs`) and returns to its own
`wall + hlcPhysicalWindowMs` as soon as the wall overtakes; a renewal is
a ceiling move, which `tla/hlc` already models as `ApplyCeiling`. Two,
on-demand recovery covers the exhausted window: `RecoverHLCLease` today
proposes only once the wall has passed the ceiling, so a `last` at
logical `0xffff` of the ceiling's millisecond, which 65,536 allocations
in one leased millisecond or an observed maximum-logical snapshot produce
legitimately, would fail writes until the next tick; it is entered on
every `ErrCeilingExpired` instead (its callers already are the fenced
allocation paths), proposes the same per-group target on every led
group whose ceiling is below the needed value, and reports success once
the fenced path can issue again (the issuing group's ceiling is above
`last`'s millisecond, or `last` has logical room at it), so the
exhausted case costs one Raft round trip, serialized by `hlcRecoveryMu`
as the expired-wall case is today. Tests: a receiver advanced ahead of its
wall by more than one tick and given no further advance allocates past
65,536 fenced timestamps across two ticks; its target falls back to
`wall + hlcPhysicalWindowMs` once the wall passes the raised value; a
maximum-logical snapshot issues again after one on-demand renewal; two
groups on skewed clocks exchanging cross-group snapshot reads and 2PC
commits in both directions over many operations keep their ceilings
within one tick of each other and of the faster group's
`wall + hlcPhysicalWindowMs`, which pins the absence of the staircase; a
2PC secondary whose process hosts no primary-group member keeps issuing
its own writes while a primary on a faster clock commits through it
repeatedly; a restart from a snapshot whose replicated watermark's
physical half exceeds its ceiling issues after the one recovery that
grants it);
`21a11ddc` the read path: every leader-local `ShardStore` read (eleven
entry points: get, exists, the version probes, every scan shape) goes
through `leaderReadAt`, whose behaviour depends on the intent carried in
the context: a **snapshot** intent exists only on a `ReadTimestamp` minted
inside `kv` by `BeginSnapshotReadTimestamp` (an unexported capability
field, bound with `WithReadSnapshot` or through `WithDispatchVoucher`),
applies only at its own timestamp, proposes an advance through the
wrap-aware proposer when the group's replicated watermark is below it and
waits for the local apply, is refused with `ErrReadTSAheadOfClock` when the
physical half exceeds `max(physical(HLC.Current), ceiling) +
hlcPhysicalWindowMs`, with `physical(x) = x >> HLCLogicalBits`
(corrected in review: the earlier text compared the packed 64-bit
`HLC.Current` against a ceiling in milliseconds, a bound 65,536 times
too wide that every snapshot passed; the test pins the boundary with a
nonzero logical counter in `HLC.Current`), the ceiling being the
serving group's own under legacy TSO mode and the
TSO group's, `ceiling(0)`, under Phase D (no wall clock; that bound sits
above the local ceiling on purpose, which is what makes the ceiling raise
at advance apply necessary: a group's issued timestamps sit at its own ceiling, `wall +
hlcPhysicalWindowMs` at the last renewal, so under any positive clock skew
a legitimate snapshot from a faster group lies above this group's ceiling,
and refusing at the ceiling instead would fail every cross-group read from
that group; the apply therefore extends the ceiling through the replicated
advance entry, which is Raft-agreed exactly like a lease renewal, and the
admission bound caps how far a snapshot can pull the ceiling ahead, by at
most the window plus the skew; `tla/hlc` models the advance as a ceiling
move so HLC-4 still holds; under Phase D the bound measures
against the TSO group's ceiling, read explicitly (corrected in review:
with the scalar ceiling the shared clock made this automatic, but the
ceilings are per group above, `TSOStateMachine.applyLeaseCeiling`
advances `ceiling(0)` alone, and a bound read through the minimum over
the led groups would see the retired data group's ceiling, frozen at its
last renewal, and refuse every TSO-issued snapshot after one window): a
Phase-D data-group leader stops renewing its own ceiling
(`shouldRenewHLCGroup` retires data groups once `PhaseDActive`), Phase D
requires hosting the dedicated TSO group (`configureLegacyCoordinatorTSO`
refuses `--tsoModeFile` and Phase-D mode with `ErrTSOGroupRequired`), and
`applyLeaseCeiling` advances `ceiling(0)` on every member of that group
as each lease entry applies, so an idle data-group leader's `ceiling(0)`
trails group 0's renewals only by its apply lag and every TSO-issued
snapshot, issued at or below it, is admitted; the same explicit read
serves the TSO allocator's own checks (`hlcCeilingExpired` for recovery
and `rejectMinimumWindowBeyondCeiling`) and the fenced allocation under
Phase D; a node without a group-0 member is not Phase-D-aware
(`tsoCutoverState` is nil) and keeps renewing its own groups; a group-0
member cut off from group 0's quorum for longer than the window refuses
snapshot reads until it catches up, and it cannot allocate TSO timestamps
for its writes in that state either, so no new unavailability. Test: a
Phase-D data-group leader that only follows group 0 and has been idle for
more than one window, its own group's ceiling frozen at its last renewal
below the TSO's, admits a busy group's snapshot through the per-group
API and refuses one above `ceiling(0)` plus the window), and skips groups
without an engine, the TSO group, and stores without a replicated
watermark; a **client**
intent (`WithClientReadTimestamp`) never proposes and fails with
`ErrReadTSAboveWatermark` above the watermark; a bare timestamp reads as
before. The snapshot comes from the leaders: `BeginSnapshotReadTimestamp`
calls the new `LeaderSnapshotCoordinator` capability, which
`ShardedCoordinator` implements by fencing the leaders of the groups the
operation **routes to** (the owning groups of a point or range read's
keys, the participants of a transaction) and taking the maximum of their
replicated watermarks (`LastRaftCommitTS`, never `LastCommitTS`; see the
seed rule below); every data group is fenced only when the key set is not
known up front (a keys-spanning Lua script), so an unavailable or
leaderless group fails only the operations that touch it instead of the
whole data plane (the implemented branch fences every data group,
including for the public gRPC `Ts = 0` reads, and must be narrowed to the
routed participants: a merge blocker). A group the operation discovers
only after `s` was minted is not a gap in the rule (found in review): an
S3 object handler learns the bucket generation from the bucket-meta read
and only then builds `s3keys.RouteKey`, and `BucketMetaKey` and the
object key can route to different groups, so the object group is not a
participant at Begin; the participants at Begin only seed `s`, and every
read at `s` on any group goes through the read path above, which
advances that group's replicated watermark to `s` when it is below and
waits for the apply, after which the fence rejects every entry at or
below `s` on it, while a group already above `s` serves the read at `s`
as an ordinary MVCC read below its watermark. A delayed object write at
`c ≤ s` therefore either applied before the advance, and the manifest
read at `s` sees it, or is fenced after it and retried above `s`, never
the G0 schedule, with no restart of the snapshot and no fencing of every
S3 group; the generation the handler uses is the one its bucket-meta
read at `s` returned, and the `proxyRouteKey` read stays what it is,
proxy routing outside the snapshot. Test: the bucket-meta group ahead
supplies `s`, the object group behind, an object write at `c ≤ s`
proposed on it around the manifest read at `s` either lands before the
advance and is read, or is fenced after it, and the read never misses a
write that was acknowledged. No value from outside the touched
groups seeds `s` (found in review): the adapters' local seeds today are
process-wide, `ShardStore.LastCommitTS` behind `snapshotTS`,
`GlobalLastCommitTS` behind `nextTxnReadTS`, and
`GlobalCommittedTimestampFloor` for a keyless begin, and keeping such a
value when it is higher would turn every point read on an idle group into
an advance proposal whenever any other group moves, which is exactly the
Raft write the routed-participant narrowing exists to avoid. `s` is the
maximum over the touched groups of the fenced leader answer and that
group's own replica watermark, both the **replicated-only** watermark
(found in review: `CommittedTimestampFloorForKeys` and the
`RawLatestCommitTS` RPC behind it sample `Store.LastCommitTS`, which a
direct write advances, so a catalog bootstrap `CatalogStore.Save` at `d`
on a replica whose replicated watermark is `r < d` would advertise
`s = d`, and a later entry stamped at `d` still passes the `commitTS > r`
fence and changes the snapshot after the read returned; the floor helpers
and the RPC therefore return `LastRaftCommitTS`, and the floor is a guard
against a regressed answer over the same key set, not a second source),
the keyless helpers
pass their keys or take the snapshot without a seed, and a keys-spanning
operation fences every data group anyway, where the global floor is the
same value. Tests: with group B busy and group A idle, point reads and
single-table transactions on A propose zero advances while B's watermark
moves; a direct write above the replicated watermark raises neither the
answer nor the floor. An all-zero answer (every touched group fresh)
still issues a capability (corrected in review: issuing none left the
read on the `^uint64(0)` "latest" sentinel, so a multi-key reader on a
fresh store could read `a` as absent, let a single-group transaction
write `a` and `b` atomically, and then read the new `b`, an outcome no
serial order allows): `s` is then one fenced allocation minted by the
leader of one touched group for that group (the lowest group id among
them; the TSO group's leader under Phase D), inside that leader's
current-term grant, and returned in the fenced answer the capability is
built from (corrected in review: the coordinating process may lead none
of the touched groups, as the public gRPC, Lua, and filesystem paths
allow, and then holds no grant for them, while an allocation under an
unrelated group it leads would be fenced at none of the touched groups'
ceilings), and
each touched group advances to
`s` on its first read, one Raft write per group once, since its
watermark is nonzero from then on. The single-group `Coordinate` takes
the same snapshot and the same advance (corrected in review: exempting
it kept the apply-order hole open on a fresh store, where a writer that
allocated `c` and stalled before proposing would land after a snapshot
minted at `s > c` had read a key as absent, the zero watermark fencing
nothing, and a later read at `s` would see the write); its one group
advances to `s` once when `s` was minted above its watermark, and
otherwise `s` is the watermark itself and no advance is proposed. Test:
on a fresh store a reader of `a` then `b`, interleaved with a
single-group transaction writing both, sees neither or both in every
interleaving, through `ShardedCoordinator` and through `Coordinate`,
including the writer that allocated its timestamp before the snapshot
was minted and proposed after the first read). Forwarded reads use a new node-to-node `Internal.ForwardRead`
RPC carrying a `ForwardedReadMode` and an authenticated **peer-only**
bearer token. The token is a node-only secret from a required, non-empty
`--internalTokenFile` (or the node-to-node mTLS the security milestone
introduces, once it exists), never the `--adminTokenFile` value: that
file is the read-only Admin bearer token handed to operator clients, and
a holder of it who can reach a group listener could otherwise call
`ForwardRead` in snapshot mode with a chosen timestamp and, since each
accepted advance extends the ceiling the next admission bound is measured
against, staircase the replicated watermark arbitrarily far ahead and
stall timestamp allocation (found in review; the implemented branch
sourced the token from `--adminTokenFile` and must change). `ForwardRead`
is explicitly protected by the server interceptor, which accepts only the
internal token, its client always attaches that token, and V2 activation
refuses to proceed unless every voter and learner has the credential and
an authenticated peer-forward probe succeeds. Absent or wrong credentials
fail closed, and a request carrying the admin token is rejected like one
carrying none (a test pins that). An `Unimplemented` answer
from an older leader may fall back to public `RawKV` only while V1 is
pinned and cannot satisfy a V2 snapshot capability. A remote authenticated
read still costs one round trip. A per-group `watermarkAdvancer` allows one
proposal in flight, folds higher targets into the next proposal, re-checks
under the lock, and never hands a waiter another caller's cancellation.
`a2c51771` the public gRPC reads: no provenance field on the request
types; a nonzero client `Ts` above the group's watermark on `RawGet` /
`RawScanAt` / the version probes returns `FailedPrecondition`; `Ts = 0`,
`Get`, and `Scan` begin a leader snapshot and read under it, and they
carry the same catalog pin as `TransactGetItems` (found in review): the
groups to fence and the point or range read are resolved from one
`RouteEngine` snapshot, the read carries that version, a leader whose
applied catalog moved the key or range, or whose M2 cutover read fence is
armed for it, rejects it with the retryable route-moved error, and the
server restarts the read on a fresh snapshot (bounded retries), so a read
cannot fence the old owner and then resolve through the new one, where a
migration can still install versions at their original `commitTS ≤
snapshotTS` after the response. `93b5623b`
the adapters: the Redis, DynamoDB, SQS, and S3 transaction-begin helpers,
the Redis `EXEC` fence path, and the delta compactor begin leader
snapshots and read under the capability (about 60 call sites; the
DynamoDB legacy-migration check that runs on every request stays local
and begins a leader snapshot only when a table needs migrating). The
reproduction is green (`W_B` is fenced and retries above `s`, or lands
before the snapshot and is seen). Tests: the entry on both stores through
real Raft; one proposal per lagging read entry point; zero proposals for
the single-group, at-or-below-watermark, `^uint64(0)`, and `ts = 0`
cases; a fresh empty store; 32 concurrent readers producing one entry;
rising targets folding into exactly two proposals; a waiter surviving its
owner's cancellation; the fenced delayed write; six clock-bound cases;
forwarded reads keeping their intent in one RPC with no proposal on the
follower and the legacy fallback; the lagging-replica regression (this
node follows group B with a stale replica while B's leader acknowledged
`c1`: the local snapshot misses `c1`, the leader snapshot is at or above
it); the gRPC client-`Ts` matrix; `ForwardRead` in each mode and its
token; each adapter's begin helper issuing the capability. `store`, `kv`,
`adapter` green under `-race` (one pre-existing `store` flake,
`TestPebbleStore_RestoreWaitsForMaintenanceLock`, passed on rerun); lint
clean. Cost: single-group reads at their own watermark pay nothing; an
idle group pays one entry per distinct snapshot; a hot multi-group
workload at most one entry per lagging group per Raft round trip; every
multi-group transaction begin now fences all group leaders (a local lease
check where the lease is valid, one `ForwardLeaseRead` per group this node
does not lead), which the benchmark milestone measures.

**Partial implementation (branch
`design/serializable-audit-a0-multigroup-read-advance`, reimplemented from
this section because the four commits above were never pushed).** What
ships: the advance entry (FSM tag `0x17`, since `0x0f` has since been taken
by `raftEncodeTargetReadiness`; 8-byte big-endian timestamp; refused for 0
and `^uint64(0)`; applied through `ApplyMutationsRaftAt` with no mutations;
idempotent; the apply raises the process HLC ceiling to the timestamp's
physical half exactly, then observes it), `ShardStore.ensureReadWatermark`
on every group-level read entry point (point reads, version-presence
probes, forward, reverse and physical-limit scans, keys-only scans,
`ScanGroupOwnedAt`), forwarding to a remote leader through
`Internal.ForwardAdminProposal` with a dedicated proposer wired whether or
not the admin server runs, and a leader-side admission bound
(`ReadWatermarkAdvanceAdmissible`: physical half at most the leader's wall
clock plus two lease windows, never derived from the HLC or the ceiling,
which an accepted advance raises, so repeated requests cannot staircase
it). A persistence failure halts the apply loop, and the entry is
classified volatile so a restart replays it and restores the ceiling. The
whole path is opt-in (`--readWatermarkAdvance`, default off; without it a
forwarded advance is refused) until every replica understands tag `0x17`,
which is the rolling-upgrade gate. Regression
tests: `kv/read_watermark_advance_test.go` (the reproduction's shape on two
single-node groups, red on `main`). Provenance is the interim rule "advance
only when `ts` is at or below the maximum watermark this node has applied
over its local groups" (a client `Ts` above that is served as before and
advances nothing), not the typed snapshot API above. Still open from this
section: the typed coordinator-only snapshot argument and the
`FailedPrecondition` rejection of client timestamps above a group's
watermark; leader-authoritative snapshots and the lagging-replica test;
per-group HLC ceilings (the raise lands on the process-wide ceiling, the
shape `main` already has for every observed commit timestamp); a
cluster-verified capability gate in place of the operator flag;
node authentication for the forwarded advance when no admin token is
configured, and a server-verifiable snapshot voucher; the per-group
`watermarkAdvancer` that keeps one in-flight advance per lagging group
(concurrent reads at the same snapshot each propose today; the entries are
idempotent but cost log entries and latency); skipping the repeated no-op
forward a follower sends on every page of a long scan while its local
apply still lags `ts`; and the read-side benchmark.

**Raw and 2PC apply-order fence (branch
`design/serializable-audit-a0-raw-commit-fence`).** Extends the one-phase
commit-ts fence, which is all `main` carries of A0's fence, to the other two
paths G0 names, behind a replicated activation.

- **Raw.** `handleRawRequest` applies through `ApplyMutationsRaftAtFenced`,
  so a raw entry whose timestamp is at or below the group's watermark is
  rejected with a write conflict. Raw timestamps are coordinator-allocated
  (never caller-chosen), so the fence only fires on reordering;
  `ShardedCoordinator` re-dispatches a fenced single-group raw write with a
  fresh timestamp (raw writes carry no read set, and nothing was applied).
  `DEL_PREFIX` stays unfenced: a multi-prefix broadcast shares one
  timestamp and would fence itself.
- **2PC: fence the PREPARE, not the COMMIT.** The section above moves the
  commit-timestamp allocation after the PREPAREs and fences the primary
  COMMIT. This branch keeps the allocation where it is (settled before
  PREPARE, carried in `TxnMeta.CommitTS` or, when durable PREPARE evidence
  is enabled, `TxnMeta.PrepareCommitTS`) and fences each PREPARE against
  the same timestamp used for its route floor and durable lock proof. A
  PREPARE whose settled timestamp is at or below its group's
  watermark is rejected with a write conflict, and the coordinator aborts
  the transaction as for any PREPARE conflict. Once a PREPARE applies, its
  write locks (at `start_ts < CommitTS`) are visible, so a reader at
  `s >= start_ts` meets the lock and resolves it, and a reader that
  completed before the PREPARE read at `s` no higher than the watermark
  the PREPARE was checked against, below `CommitTS`. Neither can see the
  COMMIT land below its snapshot, so the COMMIT needs no fence and keeps
  its idempotent re-apply. With the read watermark advance on, a reader
  advances a secondary group before reading, so the same check covers 2PC
  across groups. TLC agrees: OCC.tla's `CommitFence` constant adds the
  check to `Prepare`; `MCOCC_g0.cfg` (fence on) passes OCC-3 where
  `MCOCC_g0_gap.cfg` (fence off) fails it, and with `MultiGroup` and 2PC
  the check passes with `ReadAdvance` and fails without it.
- **Activation.** A fence changes the FSM verdict, so a per-node flag would
  let replicas of one group disagree. A replicated activation entry (FSM
  tag `0x1f`, an invalid protobuf prefix; `0x18` would be a legacy
  `Request`'s `ts` tag) persists a marker key per group, reloaded at open and after
  a snapshot restore; the FSM fences raw entries and PREPAREs only once the
  marker is set. With `--commitTSFence`, each group's leader proposes the
  activation once it finds the marker missing. The operator enables the
  flag only after every node runs a binary that knows tag `0x1f`; an older
  replica fails to decode the entry and does not fence, which is the same
  rolling-upgrade rule as `--readWatermarkAdvance`.
- **Still open:** `DEL_PREFIX`; the replica-determinism caveat above (the
  fence compares against `LastCommitTS()`, which the catalog bootstrap's
  direct write also advances), shared with the one-phase fence; the 2PC
  ABORT timestamp; and G1, which the fence does not touch.

Open after this branch:

- **2PC commit timestamps versus advanced secondaries.** The commit
  timestamp is allocated on the primary group's leader, whose HLC and
  watermark may sit below an `s` that a secondary group was advanced to;
  the secondary COMMIT is unfenced, so it can land at or below `s` on that
  group if its PREPARE applied after the read. Fix: the primary allocates
  the commit timestamp above every participant's watermark (each PREPARE
  reply returns its group's replicated watermark; the allocation goes
  through the HLC's after-through path). A merge blocker for A0.
- **Non-transactional reads stay plain.** Redis `GET` and the scans,
  DynamoDB `GetItem` / `Query` / `Scan`, S3 `GET` / `LIST`, and the SQS
  peek read at the local watermark without the capability. A single-key
  read is a leader read on one group and unaffected; a range read whose
  page spans two groups can be torn. Two paths to close: a DynamoDB
  `Query` / `Scan` page under `ConsistentRead` whose range crosses a split
  (upstream serves it from one snapshot), and **`TransactGetItems`**
  (`adapter/dynamodb_transact.go`), which reads up to 100 point keys at one
  `nextTxnReadTS()` snapshot and can span groups: a lagging participant
  can later apply `c ≤ readTS`, so the supposedly atomic result is torn.
  Both must begin a leader snapshot over the groups they route to and read
  under the capability, with tests in the completion set. Until the page's
  route subset is pinned as described next, a `ConsistentRead` `Query` /
  `Scan` page may conservatively cover every active data group; this adds
  leader snapshot work for groups outside the eventual page but cannot
  weaken the read. `TransactGetItems` validates its keys before fencing and
  covers the distinct groups to which those keys and any result-affecting
  schema records route, and it binds that group selection and every read
  to **one catalog version**: the groups are resolved from a single
  `RouteEngine` snapshot (`ResolveRoutesWithVersion`), each point read
  carries that version, and a leader whose applied catalog has a different
  owner for the key, or an armed M2 cutover read fence for its interval,
  rejects the read with a retryable route-moved error, on which the
  adapter's `TransactGetItems` handler discards the snapshot and restarts
  the whole request from the group resolution, re-reading every key
  (bounded retries, then a retryable 5xx). Fencing the old owning group and
  then reading through a newer catalog that routes the key to the
  migration target would otherwise let the target, which never received
  the advance, apply a delayed `c ≤ readTS` after the response (found in
  review); the per-group advance that a snapshot read proposes on an
  unfenced group does not close it on its own, because migration imports
  and promotions copy versions with their original, older timestamps, so
  the pin keeps the snapshot independent of the M2 cutover protocol. A
  `ConsistentRead` `Query` / `Scan` page is pinned the same way. Invalid
  requests retain their existing 4xx ordering. The Redis
  adapter implements no `MGET` and the DynamoDB adapter no `BatchGetItem`,
  so these two are the multi-key reads that exist.
- **Rolling upgrade.** Older followers skip the `0x0f` entry (a fence
  verdict divergence of the A0 class), an older node forwarding a snapshot
  read through the public `RawGet` gets `FailedPrecondition` from an
  upgraded leader, and a snapshot read forwarded to an older leader is
  served without an advance: all covered by the A7 gate, whose V2 set now
  includes the advance entry and `ForwardRead`.
- `hlcPhysicalWindowMs` is 20 s in code; `CLAUDE.md` said 3 s and is
  corrected. An alternative that avoids rejections, serialising
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
- **Batched entries were not applied atomically (G17, closed on `main`
  by #1269).** `kvFSM.Apply` applied the requests of one `raftEncodeBatch` entry
  one at a time, and each request's `ApplyMutationsRaftAt` persists the
  entry index in its own Pebble batch, so after the first request the
  durable applied index already names the whole entry. A crash before a
  later request leaves that replica with the entry's head applied and its
  tail not; on restart the engine seeds `e.applied` from the durable index
  (`rawNodeAppliedForOpen`) and `applyNormalCommitted` drops the entry as a
  non-volatile duplicate without calling the FSM, so the tail is never
  applied on that replica while every other replica applied all of it: a
  permanent divergence that A0's per-request index replay detection cannot
  repair, because the engine discards the entry before the FSM sees it. The
  fix applies every request of one entry in one **indexed** Pebble batch
  with one applied-index write, so the entry lands whole or not at all,
  and keeps the sequential semantics of the per-request path (found in
  review: a plain batch whose later requests read only the DB would let a
  `DEL_PREFIX prefix(k)` after a `PUT k` in the same entry miss the staged
  put and leave `k` visible while both requests report success): the
  replay probe, the OCC conflict checks, and the prefix-delete scan of a
  later request read through the batch, so they observe the earlier
  requests' staged writes exactly as they observed the per-request
  commits (#1269, merged as `99a82156`, implements it this way). A crash test that kills the
  process between two requests of one batched entry, restarts, and
  asserts the replica converges with one that never crashed travels with
  it, run on a two-put entry and on a `PUT k` followed by `DEL_PREFIX
  prefix(k)` entry, whose re-delivery must leave `k` deleted. The loss is
  pre-existing on `main` (the cold-start skip
  predates this audit) and is listed in §3.
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
`DEL_PREFIX`; review `dedupProbeOnePhase`; apply every request of a batched
entry in one Pebble batch (G17, landed on `main` as #1269); raise the physical ceiling at advance apply
before the HLC observes the advance (the multi-group branch's open item).

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
  secondary and repeated primary COMMITs are not (a secondary's HLC
  observes the primary's stamped value on apply; a secondary whose process
  hosts no primary-group member and runs on a slower clock would be
  starved by it, found in review, which the ceiling raise before
  observation in the A0 advance notes closes for every replicated apply,
  not only the advance entry). A hazard found on the way:
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
  Fencing the primary alone is **not** enough once a secondary group can
  be advanced by a snapshot read (A0 multi-group): a reader advances group
  B to `s > c` and reads there, the pre-allocated transaction prepares
  afterwards, its primary group accepts `c` because that group's watermark
  is below `c`, and the unfenced secondary COMMIT lands at `c ≤ s` on B,
  invisible to the reader's validation. So, before the primary COMMIT is
  proposed, a pre-allocated `c` must exceed every PREPARE response's
  replicated watermark (the same participant watermarks the follow-up in
  the multi-group section collects); otherwise the transaction aborts and
  retries with a fresh pre-allocation. This is the pre-allocated half of
  the "commit timestamp above every participant's watermark" blocker. The
  non-offloaded `UploadPart` cannot stay pre-allocated under it (found in
  review): `allocateS3UploadPartVersion` chooses `c` before
  `uploadS3Chunks`, whose chunk batches are unstamped dispatches on the
  same object group and raise its replicated watermark above `c`, so the
  descriptor PREPARE's watermark exceeds `c` on every non-empty part, the
  request body is already consumed, the retry is impossible, and the
  failure path deletes the chunks. It therefore adopts the offloaded
  path's shape: the first allocation becomes the chunk namespace only
  (the version in `VersionedBlobKey` and the descriptor's `PartVersion`),
  and the descriptor's commit timestamp is allocated after the chunks are
  durable, as `finalizeS3UploadPartCommitTS` already does for offloaded
  parts, so the descriptor transaction is not pre-allocated at all and
  takes the ordinary post-PREPARE allocation. A test drives a non-empty
  non-offloaded part through the fenced rule.
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
  watermark-advance read path is the fix; its first part is on
  `design/serializable-audit-a0-multigroup-read-advance` (see "Partial
  implementation" above for what remains).
- Not yet leader-issued: the 2PC ABORT timestamp (unfenced) and the
  pre-allocated paths above; making delta keys independent of the commit
  timestamp would remove the exemption.
- One more 8-byte meta key per replicated apply batch, not benchmarked.
- **Every snapshot format carries the replicated watermark (found in
  review, closed).** The streaming MVCC and in-memory formats omitted
  `_meta_last_raft_commit_ts` and the restore fell back to
  `LastCommitTS()`, which direct writes such as `CatalogStore.Save` advance
  on their own; a replica restored from such a snapshot then fenced a
  committed entry at or below that direct-write timestamp while replicas
  holding the true replicated watermark applied it, a divergent FSM.
  Fixed on `design/serializable-audit-a0-snapshot-watermark` (two commits
  on the A0 completion branch, not pushed): `c973e232` adds the
  reproductions (`TestSnapshotFormats_CarryReplicatedWatermark` red for
  the streaming and in-memory formats; a variant with no Raft write before
  the direct one red for all four formats, since a store with only direct
  writes snapshotted nothing; and the two-replica kv test
  `TestFSMSnapshot_RestoredReplicaFencesLikeLogReplica`, "replicas diverge
  on one committed entry"); `7d6e6a97` fixes it. Pebble native and SST
  ingest keep their byte format but the key is now guaranteed to exist in
  every Pebble DB this version opens or restores (`loadLastRaftCommitTS`
  persists it with `Sync` when missing or above `lastCommitTS`, at open and
  after every restore swap; an empty-snapshot restore writes 0; **to be
  gated on the applied ledger epoch row before merge**: a store on which
  the ledger epoch has not applied (pre-epoch, V1-pinned, or marker
  already `txn-ledger` with the epoch row not yet applied) must not
  carry the key at all, and the open-time seeding above goes away, the
  epoch's apply batch being the key's only creator, see A7); the
  streaming MVCC format gains layout V4 (V3 plus a trailing
  `lastRaftCommitTS`; the reader still accepts V1 to V3); the backup
  encoder writes the key as a raw entry and `ReadSnapshotWithHeader` skips
  it. The fallback is one shared decision (`resolveReplicatedWatermark`):
  a present value is used exactly (capped at `lastCommitTS`); an absent
  one falls back to `lastCommitTS` with a warning carrying the stable
  attribute `replicated_watermark_fallback=<open|restore_pebble_native|
  restore_sst_ingest|restore_mvcc_stream>`, and the tests assert no such
  warning on any restore of a snapshot this version wrote, including a
  second hop from the restored replica. Legacy V1 to V3 and key-less
  native / SST snapshots restore with exactly one warning; a legacy store
  warns once at open and is silent thereafter. `store`, `kv`, `internal`,
  `distribution`, `cmd`, the root package, and `adapter` green under
  `-race`; lint clean. Limits: pre-fence sources still fall back (the
  cutover requirement A7 covers); the in-memory store's V4 snapshots are
  rejected by pre-V4 binaries (production nodes do not use the in-memory
  store); and, separately and older, `elastickv-snapshot-decode` /
  `ReadSnapshot` could not decode any live Pebble `.fsm` (it failed on
  `_meta_last_commit_ts`, "encoded value length 8 < 9"), a tooling defect
  left as found by this branch. #1267 (merged 2026-10-15 as `95889d1f`)
  removed that first failure, by skipping every reserved metadata key a
  live store writes through a mirror of the store's classifier pinned by
  cross-check tests, and #1276 (merged 2026-10-16 as `184351ae`) closed
  the rest, found in review on #1275: the decoder now reads an on-disk
  `.fsm` (the FSM snapshot header in front, the engine's CRC32C footer
  behind), keeps only the newest version of each key, and accepts the
  SST-ingest snapshot format and the store's stored-key budget, with a
  test on a file written through the engine's snapshot path. Encrypted
  entries are still rejected; the roadmap's §4 row records that limit.

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

#### S3 A1 completion plan (2026-10-30)

The unpublished branch described below is historical evidence, not a merge
or acceptance receipt. Implementation now proceeds from main `c316b829e` on
`design/serializable-audit-a1-s3-validation`; the open A0 PR #1355 is separate.

- Every completion attempt acquires one read timestamp, loads the bucket,
  upload metadata, requested part descriptors and replaced manifest at that
  snapshot, then commits with every consumed descriptor key in `ReadKeys`.
  A retry repeats those reads and ETag checks rather than reusing a manifest
  assembled from an earlier snapshot.
- UploadPart keeps its streamed blob namespace and read voucher, but checks
  the upload, bucket generation and superseded descriptor again at exactly
  the dispatched `StartTS` before its descriptor commit, using the original
  pre-body read timestamp voucher and retained snapshot. Do not replace that
  snapshot with a local/latest watermark or rename the already-streamed blob
  and chunkref namespaces. Allocate the descriptor commit timestamp after
  chunk durability in both inline and offloaded modes; keep the reserved
  inline blob version separate from the descriptor's MVCC commit timestamp.
  The read set contains the upload metadata, generation
  key and part key. Bucket deletion re-puts the generation key, preserving
  its maximum value, in both public and admin deletion transactions. Other
  object writers do not write that key, so parallel unrelated uploads do
  not acquire an accidental bucket-wide conflict.
- Support all 10,000 requested parts during staged-visibility migration as
  well as normal routing. The physical read-key cap is bounded at
  `2 * (10_000 + 240)` to include migration aliases and control-key headroom.
  Retain every existing cap check and every alias; do not accept the older
  branch's approximately 4,998-part migration limitation as completion.
- Multipart conflicts that are proven noncommits can use the existing
  bounded completion retry or a 503 SlowDown response for the consumed
  upload body. Unknown outcomes must not become permission to replay.
  Track whether descriptor dispatch was attempted: clean new blobs after
  pre-dispatch failure or a proven noncommit, but preserve them after an
  uncertain dispatch result. Clean superseded blobs only after confirmed
  success. The unknown marker takes precedence over any joined conflict,
  lock or route-fence cause. This prevents a lost success reply from deleting
  blobs that a committed descriptor may already reference.
- Red/green regressions run through the production ShardedCoordinator/FSM,
  including both part-read windows, deletion/recreation, superseded blob
  cleanup, unrelated bucket writes, maximum part counts and alias capacity.
  Test server-side data and cleanup effects, not only HTTP status codes.
- Add an opt-in S3 multipart register mode to the existing Jepsen workload.
  Each write races same-body part replacement with completion under one
  upload ID; the expected object remains the register value regardless of
  which immutable part version wins. Preserve the existing linearizability
  checker and honest indeterminate outcomes. Require complete successful
  final reads so lost referenced chunks cannot be hidden by failed GETs.
  Keep the default PUT/GET workload unchanged and run both modes in CI.
  Run the new multipart mode for 30 seconds at rate 10 and concurrency 10
  in each encryption job; a five-second sample produced no confirmed writes
  under contention in one local run and must not be accepted as evidence.
  Preserve both named S3 histories, source SHA, encryption mode and step
  outcomes as unconditional CI artifacts. Do not weaken the nonvacuity or
  final-read gate to obtain a passing result.
- Security acceptance: reject public S3 object keys exceeding 1,024 decoded
  UTF-8 bytes before proxying, body consumption or mutation. Apply the same
  creation limit to admin PUT; admin GET/delete remain available for recovery
  of previously stored nonconforming keys. Before allocating completion
  descriptor/read-key arrays or reading descriptors, enforce a 24 MiB logical
  aggregate read-key budget, including escaped segment lengths and fixed key
  encoding overhead. Use division/subtraction rather than unchecked byte-count
  multiplication. This supports 10,000 maximum-length conforming keys, including
  zero-byte escaping; migration alias expansion remains separately bounded by
  the existing physical key-count cap. The budget bounds this dependency set,
  not total request/protobuf/Raft heap or concurrency, and does not change RawKV
  value limits. Existing overlong public S3 keys require the admin recovery API.
  Add runtime-red HTTP/admin and pre-allocation/no-descriptor-read regressions
  before the fix, then repeat latest-push code/security review and CI.
- Verify all changed helper callers, full adapter/KV/store race suites,
  unfiltered lint, relevant Jepsen checks, and latest-push review/CI. A1 does
  not establish cross-group serializability before A2's read-lock protocol,
  nor close any other audit milestone.

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
outside this change.

The string creates and the type probes are on
`design/serializable-audit-a1-redis-setnx` (eight commits on the
collections branch, not pushed). `590d525e` adds the reproductions
(`adapter/redis_setnx_absent_key_repro_test.go` and new cases in the Lua
and `MULTI` files): `SETNX` held after its existence check while `XADD` /
`RPUSH` / `HSET` commits, the legacy `SET NX`, plain `SET`, and `INCR`
held while `XADD` or `RPUSH` commits (seven red, "both creates committed:
the key holds 2 encodings"); Lua `TYPE` / `EXISTS` / `PTTL` of a
collection racing `DEL`, caught `WRONGTYPE` reads (`pcall` `GET` of a
hash, `redis.pcall` `HGET` of a string, `pcall` `LLEN` of a set) racing
`DEL`, and branches that skip the write (`SET NX` on a present key, `SET
XX` / `DEL` / `PEXPIRE` / `RENAME` on an absent one), fourteen red;
`MULTI` probes (`EXISTS` / `GET` / `EXPIRE`) of an absent key racing
`HSET` / `ZADD` / `SADD`, three red. `SET NX` against `SET NX`, `SET XX`
against `DEL`, and `SETNX` against `PFADD` were already rejected by the
write-write check (both write the same key or the TTL index) and stay as
pins. `6707a671` gives `SETNX`, `SET NX` / `XX` / `GET`, the legacy plain
`SET`, and the legacy `INCR` the absent-key guard on create (thirteen
read keys and four fence `Put`s) and the string, HLL, and bare-key reads
on an existing string; `7052a141` routes `SETEX` through the same guarded
path (it was a raw blind write with no reads and left a hash / list /
stream / HLL next to the string; it now takes a read timestamp, the type
probe, the TTL filter, and an OCC dispatch, and deletes the HLL or bare
anchor it overwrites); `793d6ba4` records Lua type probes and `WRONGTYPE`
reads on collection keys (the collection's fence, plus the legacy blob for
hash and zset and the meta for streams); `5d356eda` makes the `MULTI`
probes of an absent key read all four fences (nine to thirteen keys);
`6a9ab4f3` pins the read sets (26 Lua rows, 15 string-write rows, 5
`MULTI` rows). All 39 reproductions green; `adapter` green under `-race`;
lint clean. Found on the way and recorded as **G16**: every Lua state
loader cached an empty, unloaded key state before checking the type and
left it there on `WRONGTYPE`, so after a caught error the next command of
that type treated the key as absent (a second `HGET` or `GET` replied nil;
`HSET` / `RPUSH` / `SADD` / `ZADD` / `XADD` on a string, or `INCR` on a
hash, succeeded and replaced the value at commit); `e1ff72d7` reproduces
it (eight red) and `fd97de18` drops a key state whose first load failed.
Still open in G7 after this branch: the legacy plain `SET` over an
existing string is still a blind write (a `DEL` plus a create of another
type between its probe and its write leaves two encodings; dedup-off mode
only); Lua `GET` / `GETDEL` / `INCR` on an absent key record only the
string and bare keys (a concurrent collection create is unseen); Lua
`PTTL` of a collection validates existence but not the TTL value (a
positive `EXPIRE` writes the meta and TTL index, not the fence; not
reproduced); and, not G7, an emptied wide-column hash still answers
`EXISTS` / `TYPE` as a hash until compaction because its length deltas
survive. (The list above is the complete set of G7 items still open; the
`SETNX` / `SET NX` / `SET XX` creates, the caught `WRONGTYPE` reads, and
the collection-type probes that an earlier revision of this paragraph
listed as open are closed by the branch just described.)

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
`!txn|rlock|` (moved in review under `!txn|ok|` tag `0x04`, see the
identity status below; the layout after the tag is unchanged) keyed
`uvarint(len(key)) key startTS primaryKey` (per key and
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
paged scan of the whole read-lock namespace filtered by prefix (the
rows are length-prefixed, so a prefix cannot be seeked; pages 8 → 1024).
The first matching row fails the apply with `TxnLockedError` ("write lock"
or "read lock") and nothing is deleted; a key under `!txn|` never counts,
matching what the delete skips. No coordinator change: nothing resolves or
retries `TxnLockedError` for raw requests today, `DEL_PREFIX` is a
per-group broadcast whose copies apply independently (re-running it would
re-apply the landed copies), and every caller (Redis `FLUSHDB` /
`FLUSHALL` through `retryRedisWrite`, the DynamoDB deleted-table cleanup
through `isRetryableTransactWriteError`, the S3 bucket-delete safety net
through `retryS3Mutation`) retries the whole flush on that error when it
reaches them typed, which today it does only when the rejecting group is
led locally (found in review; corrected under Limits below: the forwarded
form lands with this branch, not with A0b). Tests: 28 table subtests (write lock, read lock, both, lock outside
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
which Redis re-parses (`redis_retry.go`) but the DynamoDB cleanup and the
S3 safety net do not: `dispatchDeletedTableCleanupPrefix` returns the
untyped error and `launchDeletedTableCleanup` logs it, with no sweeper
behind it, and `runBucketDeleteSafetyNet` logs "orphan sweep incomplete"
and returns nil. Before this branch a remote group never rejected a
`DEL_PREFIX`, so the gap was latent; with the lock check, a transient
read lock on a remote group would abandon the deletion of a removed
table or bucket for good and leak everything under the prefix (found in
review). So the typed class crosses the wire in this branch: `Forward`
attaches a `ForwardError` status detail (`kind`: txn locked, write
conflict, route fenced; the key and the detail string), `LeaderProxy`
rebuilds the typed error from it, the Redis string re-parse stays as the
fallback for a leader that predates the detail, and the two cleanups
retry on the rebuilt class exactly as they do locally. Tests: a
forwarded `DEL_PREFIX` blocked by a read lock on a remote group comes
back typed, the DynamoDB cleanup and the S3 safety net retry it, and
after COMMIT nothing remains under the prefix; a forwarded write conflict
and a forwarded route fence come back typed too. The one-shot cleanup
goroutine itself (a crash between the table delete and the sweep leaks
the same way) is pre-existing and out of scope here.

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
not free the second's lock); commit and rollback records with an id, and
the A2 read-lock rows, live under the success-marker family `!txn|ok|`,
which the shipped binary already reserves by prefix, hides from reads,
refuses for raw writes, and parses by a leading tag byte
(`txnSuccessMarkerVersion`, `0x01`; `txnSuccessLockedKey` rejects every
other tag): tag `0x02` is the commit record and `0x03` the rollback
record (`primaryKey startTS txnID` after the tag), and tag `0x04` the
read-lock row (the layout A2 gave `!txn|rlock|`, after the tag). They
cannot live in `!txn|cmt|` or `!txn|rb|`, because a legacy record key
is `primaryKey || startTS` with an unbounded primary and no layout
inside those families can be told apart from a legacy key with a longer
primary; and they cannot take prefixes of their own (corrected in
review: `!txn|cmtid|`, `!txn|rbid|`, and `!txn|rlock|` are ordinary
user keyspace to the shipped binary, which reserves exactly the lock,
intent, commit, rollback, success, and meta families and the backup
floor key, so reserving them later would hide a user's rows under them,
an exact collision would overwrite one, and a collision scan with a
migration gate would have to precede the reservation). `txnRouteKey`
dispatches on the tag to recover the primary (tags `0x02`, `0x03`) or
the locked key (`0x04`) for routing, migration filtering, and backup
ownership; every tagged layout is written only after the identity
capability is on, which the capability monitor grants once every member
advertises it, so no shipped binary ever routes or migrates one; the
tags are wired into the migration families appended to the enum and
the write-conflict metric buckets, and a test pins that
`isTxnInternalKey` reserves no new prefix. Ownership requires id,
primary key, and `StartTS` all to match
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
- **PREPARE order (G14, reproduced): closed** on
  `design/serializable-audit-a2-prepare-order` (four commits over the
  reproduction, not pushed): `a9387b26` prepares in `txnPrepareOrder`
  (primary group, then the other write groups, then the read-only groups;
  the PREPAREs were already sequential, so no round-trip is added, and the
  primary's `Commit` returns only after its FSM applied, so no secondary
  lock exists before the primary lock); `c5b94ac5` makes `primaryTxnStatus`
  and `backgroundPrimaryTxnStatus` treat "no primary lock and no record" as
  pending (the reader gets `txn locked`, the resolver skips the row;
  justification in the `primaryWithoutLockOrRecordIsPending` comment);
  `9dbdb9df` adds `mutations_carry_values` to `Request` (additive, `buf
  breaking` clean) so a `TxnID` transaction's secondary COMMITs carry the
  group's real writes (PUT with value, DEL; the flag is needed because
  proto3 cannot tell an empty value from a missing one; primary COMMITs and
  legacy transactions stay keys-only); `3346ba67` makes a secondary COMMIT
  whose lock is gone install the carried write at `commitTS` when the entry
  carries values, has a `TxnID`, and no version of the key is newer than
  `startTS`, treat an existing version at exactly `commitTS` as already
  landed, treat a version newer than `commitTS` as landed and since
  overwritten (released without writing; corrected in review, see the
  lock-GC decision below), and otherwise fail with the new
  `ErrTxnWriteLost` (logged with
  `key`, `commit_ts`, `start_ts`, `primary_key`, `txn_id`) instead of
  returning success with nothing written; `commitSecondaryTxns` surfaces it
  after still committing the other secondaries and does not retry it. Two
  things found on the way: the TransactionManager's cleanup ABORT also
  fired after a failed secondary COMMIT and released the committed
  transaction's secondary locks (`txnCleanupRequests` now skips COMMITs
  that do not name their primary), and a keys-only COMMIT that finds the
  lock present but the intent gone now fails loudly (the primary's then
  aborts through `settleFailedPrimaryCommit`) where it silently wrote
  nothing. Tests: the reproduction, renamed
  `TestTwoPhaseCommit_PrimaryIsPreparedBeforeAnySecondary`, parks T's
  second PREPARE whichever group it goes to and covers read, scan, and
  resolver (live and expired; the four higher-group variants fail with the
  old order restored); `SecondaryWithoutPrimaryLockOrRecordStaysPending`
  (read, scan, expired resolver, `TxnID` and legacy, all red under the old
  rule); the coordinator stopped after the primary PREPARE (the reader of
  the secondary reads the old value, one resolver pass writes the rollback
  record); the FSM install / fail-loudly / already-committed /
  primary-lock-lost cases on both stores; and the coordinator-level
  reinstall (`TxnID` commits both keys; legacy gets `ErrTxnWriteLost` after
  one unretried COMMIT). `store`, `kv`, `adapter`, `distribution` green
  under `-race`; lint clean. Cost: the Raft log carries each secondary value
  twice (PREPARE and COMMIT) for `TxnID` transactions; keyviz double-counts
  those bytes. Limits: a secondary lock whose primary has neither lock nor
  record is never resolved (only an old-binary coordinator whose primary
  PREPARE never applied can produce it), closed by the follow-up below;
  readers on an old binary can still wrongly abort secondaries during the
  A7 rolling upgrade, `TxnID` transactions heal through the valued COMMIT
  and legacy ones fail loudly; `ErrTxnWriteLost` means the transaction is
  half-committed (the same posture as
  `ErrTxnSecondaryRouteShiftedAfterPrimaryCommit`), which is data loss to
  report, not indeterminacy: the adapters must return an error, never OK,
  and never retry the whole transaction on it. Closed by `3e9c61b5` on
  the same branch: for an **expired** secondary lock whose primary has
  neither lock nor record, `backgroundPrimaryTxnStatus` runs
  `unpreparedPrimaryTxnStatus`, which sends an ABORT naming the identity's
  primary (writing the rollback record on the primary group; if the primary
  committed in between the ABORT fails `ErrTxnAlreadyCommitted`, the
  records are re-read, and the secondary is committed; if the primary
  PREPARE applied in between the same ABORT releases its lock) and then
  aborts the secondary as rolled back; live secondary locks stay pending. A
  late primary COMMIT was already rejected by `commitApplyStartTS` once the
  rollback record exists; a late primary PREPARE is now rejected too
  (`rejectPrepareAfterRollback`, `ErrTxnAlreadyAborted`, nothing written,
  checked only by PREPAREs naming the primary key, including the
  migration-staged copy of the record). Tests on both stores for `TxnID`
  and legacy identities: one resolver pass leaves the rollback record and
  removes the secondary lock, a late PREPARE and a late COMMIT fail, T's own
  dispatch fails, another transaction on the same primary key prepares
  normally; both new checks red without them. `kv` green under `-race`;
  lint clean. Cost: one `ExistsAt` on the rollback key per primary PREPARE
  apply (almost always a miss). Limits: a coordinator whose primary PREPARE
  arrives more than one lock TTL after its secondary PREPARE (only the
  old group-id order can produce that) loses its transaction with
  `ErrTxnAlreadyAborted`; point reads and scans still get `txn locked` on
  an expired orphan until the next resolver pass. Reconciliation with the
  lock-GC branch, decided: a PREPARE whose identity has its **rollback**
  record is rejected (`ErrTxnAlreadyAborted`); one whose identity has its
  **commit** record is a no-op success (the coordinator's COMMIT is
  idempotent). The original design of the three changes follows.
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
  after 1000 (`kv/txn_read_lock_benchmark_test.go`). Decision: **the lock
  namespaces are exempt from MVCC history.** The compactor physically
  removes any `!txn|lock|` or read-lock key whose latest version is a
  tombstone with a commit timestamp below `minRetainedTS` (no snapshot
  read ever targets a lock row at an old timestamp; the retention floor
  already accounts for backup pins), and a PREPARE whose identity already
  has a commit or rollback record is a no-op, so a late duplicate PREPARE
  cannot recreate a released lock after its tombstone is gone on the
  **primary** group (the records keep their history: they are the
  identity's memory; their own growth is the pre-existing "records are
  never collected" limit). Secondary groups hold no record, so a delayed
  duplicate secondary PREPARE arriving after its tombstone was collected
  does recreate the lock and intent there; that is a liveness cost, not a
  safety hole: the recreated lock is settled by the resolver from the
  primary's record within one TTL, a commit record resolves it as already
  landed and a rollback record drops it. Landed is decided without the
  exact version (corrected in review: retention compaction removes the
  version at exactly `commitTS` once a newer version supersedes it, at
  the same floor at which the tombstone is collected, so a recreated
  lock on an overwritten key found no exact version, the valued COMMIT
  saw a version newer than `StartTS`, and `ErrTxnWriteLost` left the key
  locked for good): a version at exactly `commitTS`, or any version newer
  than `commitTS`, means the write landed, because under V2 the lock is
  released only by the transaction's own COMMIT or ABORT, so a newer
  version can exist only after the COMMIT that the record proves; the
  lock and the recreated intent are then dropped without writing. No
  version above `StartTS` installs the write from the recreated intent
  (or the entry's carried values), and a version in `(StartTS,
  commitTS)` is the loud `ErrTxnWriteLost`, impossible under V2. Test:
  commit, overwrite, compact past both the version and the tombstone,
  then the delayed PREPARE, settled by the resolver without a write and
  without a lock left behind; and no server path resends a PREPARE after the transaction
  completed (a lost reply is outcome unknown since G15, a composed-1
  retry changes the identity), so the duplicate can only be a transport
  artefact. A test proves the settlement in both directions. The
  shared-row alternative (one row per key holding the holder set) was
  rejected because it serialises every reader of a hot key on one row.
  Target: the check cost returns to the no-history figure after
  compaction, pinned by the benchmark. Implementation on
  `design/serializable-audit-a2-lock-gc`.
- **Migration (found in review, reopens G1 under hotspot M2).** Read
  locks are not drained or carried across a cutover: a transaction can
  PREPARE and install its read lock on the source, the range can cut over
  without it, and a writer can commit on the target before the original
  transaction commits, a write skew that survives every read-lock check
  because the two consult different groups. Decision: the migrator's
  existing write-lock drain (`PendingTxnLocksInRoute` in
  `kv/migrator_lock_drain.go`, which blocks the cutover while prepared
  write locks remain in the range) is extended to read locks, so a cutover
  waits until every read lock in the range is released or resolved, with
  the same TTL-bounded wait; a read lock is never transferred (a
  transaction that spans the cutover is aborted by its own COMMIT finding
  the range moved, as today). **The drain covers only transactions that
  have already PREPAREd (found in review).** A cutover can land after an
  adapter fenced or read the old owner and before its PREPARE, when no
  lock exists to drain; `maybeAutoPinObservedRouteVersion`
  (`kv/sharded_coordinator.go`) deliberately leaves requests that carry
  `ReadKeys` or a caller-supplied `StartTS` unpinned, and the DynamoDB,
  S3, SQS, and filesystem paths that do not set `ObservedRouteVersion`
  themselves flow with version 0, so the Composed-1 gate short-circuits
  and the transaction can PREPARE on the new owner while carrying an
  old-owner snapshot; a target-side version imported at `commitTS ≤
  StartTS` is invisible to the `latest > StartTS` predicate, so OCC cannot
  catch it either. Decision: the catalog pin the A0 snapshot read path
  uses for `TransactGetItems` applies to every read-write operation. The
  adapter resolves its groups from one `RouteEngine` snapshot and carries
  that version on its reads, its PREPAREs, and its COMMIT: every
  transactional path sets `ObservedRouteVersion` from the snapshot it read
  at, the auto-pin exemptions for `ReadKeys` and a caller-supplied
  `StartTS` go away (a pinned read set is exactly what the gate must
  protect), and the resolver-routed exemption stays until the M5
  resolver-aware gate, which keeps resolver-routed keys outside the claim
  (the roadmap's publication gate, §6.1, names that exclusion next to the
  V1 and Phase-D ones, since the pin cannot fence a route the catalog does
  not own).
  The FSM rejects a PREPARE or one-phase entry whose pinned version
  predates a cutover that moved any of its keys **or read keys** (the
  existing Composed-1 fence, extended to read keys), and the operation
  restarts on a fresh snapshot where the reads can be repeated (corrected
  in review: `Dispatch` receives only the computed `StartTS`, `ReadKeys`,
  and `Elems` and has no way to repeat the adapter's reads, so a
  coordinator-level restart would commit the old mutations against a
  newer snapshot and miss every write between the two). The existing
  `dispatchTxnWithComposed1Retry` split therefore stands: a
  coordinator-allocated, read-free transaction retries inside the
  coordinator; a read-bearing one (non-empty `ReadKeys`, or a
  caller-supplied `StartTS`) surfaces the retryable route-moved sentinel
  to the adapter, which re-executes the whole operation (resolves its
  groups from a fresh `RouteEngine` snapshot, re-reads at a fresh leader
  snapshot, recomputes its mutations, re-dispatches) within bounded
  retries, the same contract the M4 loop already imposes on adapters.
  The streaming S3 writes are the exception that needs its own rule
  (found in review): `putObject` and `uploadPart` stream the request
  body into chunk rows (`BlobKey` / `ChunkRefKey` under the bucket
  generation, the object, and a per-request upload id) through raw
  dispatches before the manifest or part transaction is dispatched, and
  by the time a cutover rejects that PREPARE the body is gone, so a
  re-execution cannot rebuild the chunks, and spooling the body is ruled
  out (unbounded memory or disk per in-flight upload). Their retry is
  metadata-only: every object-scoped key family (manifest, blob, chunk
  ref, upload part) maps to one route key per object
  (`s3keys.ExtractRouteKey`), so the drain carries an object's chunk rows
  to the new owner with its manifest; the handler re-resolves the route,
  re-reads the bucket meta and the previous manifest or the upload meta
  at a fresh leader snapshot, fails the request and deletes the chunks if
  the bucket generation changed (the rows then belong to a dead
  generation), re-runs the request's preconditions against the fresh
  manifest (`validateS3PutPreconditions`: `If-Match` / `If-None-Match`;
  found in review: an `If-None-Match: *` upload that saw no object, lost
  its PREPARE to a cutover, and raced a successful create would otherwise
  overwrite it, since the fresh `StartTS` no longer conflicts with that
  create), failing with `PreconditionFailed` and deleting the chunks when
  they no longer hold, takes the fresh manifest as the previous version
  the post-commit blob cleanup (`cleanupManifestBlobsAsync`) acts on, and
  otherwise re-dispatches the same transaction, the same chunk-ref
  elements included, with a fresh commit timestamp; the chunk rows are
  never written twice. Today a rejected manifest PREPARE deletes
  the chunks and fails the request (`cleanupS3PutObjectChunks`), which is
  safe but turns a cutover into a failed upload. Tests: a cutover between
  the read and the PREPARE of a read-write transaction is rejected,
  surfaced to the adapter, and re-executed from the read, with a write
  that landed between the two snapshots visible to the second attempt; a
  cutover of an object's route between the chunk upload and the manifest
  PREPARE of a `PutObject`, and between a part's chunk upload and its
  part PREPARE, completes on the new owner with every chunk readable and
  the body read once; a bucket generation change in that window fails
  the request and leaves no chunk rows; an `If-None-Match: *` `PutObject`
  that loses its first PREPARE to a cutover while another create lands
  fails with `PreconditionFailed` and leaves no chunk rows, and an
  overwrite retried after a cutover cleans up the manifest that was
  current at the retry; a resolver-routed transaction is documented as
  outside the claim. Until both the drain and the pin land,
  deployments running hotspot M2 migrations are excluded from the
  serializability claim (§6.1 of the roadmap). Implementation on
  `design/serializable-audit-a2-migration-drain`.
- **Cost.** A read-only shard now takes two Raft entries (PREPARE, COMMIT)
  where it took one read barrier; the earlier "one entry" estimate is wrong.
- **Behaviour change.** A read key routing to a group the coordinator does
  not know now aborts the transaction (before, its validation was silently
  skipped).
- Pre-existing: raw `DEL_PREFIX` bypassed lock validation (closed above).
  Ordinary RAW `PUT` / `DEL` already check write locks
  (`validateRawMutationForApply` → `assertNoConflictingTxnLock`); **they
  must check foreign read locks too (found in review)**. The earlier
  reasoning, that a raw write carries no read set and can therefore always
  be serialised after the transaction holding the read lock, is wrong once
  snapshot readers exist: T reads `k` and PREPAREs a write to `x` (read
  lock on `k`, write lock on `x`); a raw `PUT k` applies while T's read
  lock is held; a read-only transaction R then observes the new `k` and
  the old `x` before T commits. The order constraints are T before the PUT
  (T read the old `k`), the PUT before R (R saw it), and R before T (R
  missed `x`), so no serial history exists although every operation
  succeeded, and T's COMMIT does not re-validate its read set because the
  read lock was meant to protect it. A raw `PUT` / `DEL` on a key under a
  foreign read lock is therefore rejected with `TxnLockedError` exactly as
  one under a foreign write lock is; the coordinator already retries
  `TxnLockedError` for raw requests, so the client-visible effect is a
  retry, not an error. The A2 test set gains this interleaving as a Go
  reproduction (T PREPARE, raw PUT, R read, T COMMIT: red with the skip,
  green with the check), and A3 adds it as an OCC.tla gap configuration
  (`MCOCC_gap_rawwrite_readlock.cfg`, expected to fail `OCC8`), so write
  skew through a raw write is covered by the model as well. A raw write
  derived from an earlier adapter read is still a G7-class bug on its own,
  fixed by making that path a transaction with `ReadKeys`. A coordinator
  stalled past the TTL can have its primary lock treated as a rollback
  (true for write locks too).

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
  `TransactionCanceledException` is recorded as `:fail`. **Not yet the
  claimed shape (found in review):** on `main` the adapter turns a
  `ConditionCheck` on an existing item into a `Put` of the item's current
  value (`buildConditionCheckLockRequest` in `adapter/dynamodb_transact.go`,
  a pre-A2 way of protecting the read with a write lock), so every
  read-only key of an existing item is also in the write set, two crossing
  transactions collide as ordinary write-write conflicts, and the
  PREPARE-to-COMMIT read-lock window is never exercised; the green result
  below is explained by that, not by correctness. A2 changes the adapter so
  that a `ConditionCheck` on an existing item is a read key under a read
  lock and writes nothing (which also stops a condition check from
  rewriting the item at a new commit timestamp), with a unit test on the
  plan builder asserting that no mutation is emitted. A request whose
  every action is a `ConditionCheck` then carries read keys and no
  mutation and dispatches nothing (found in review): `dispatchTxn` takes
  its primary key from the write set and rejects an empty one with
  `ErrTxnPrimaryKeyRequired`, and `runTransactWriteAttempt` already
  answers a request with no elements without calling it, which is how a
  `ConditionCheck` on a missing item works today; with no write there is
  no commit point to protect, the conditions are evaluated at the A0
  leader snapshot, a serializable read-only transaction at `s`, and a
  violated condition cancels as today. Endpoint tests: an
  all-`ConditionCheck` request on existing items succeeds and leaves
  every item's version unchanged, one with a violated condition returns
  `TransactionCanceledException`, and a mixed request keeps its
  `ConditionCheck` keys out of the write set. The workload's
  harness asserts before a run counts as evidence that its read-only keys
  stay outside the write set (the plan-builder test plus an adapter-level
  check that the item's version is unchanged after a `ConditionCheck`-only
  transaction). Until then the DynamoDB workload is not DynamoDB
  serializability evidence either way.
- gRPC: `TransactionalKV` cannot express a read set (G8), so no client-side
  workload can drive write skew through it; the engine-level evidence for
  that path is the Go reproduction tests on `design/serializable-audit-a0`.

First results, on a binary built from `main` at `4ca7e90d`, three local
nodes, `--time-limit 30 --rate 50 --concurrency 10`:

| Workload | Result |
|---|---|
| Lua | `:valid? false`, four G2-item cycles, `:not #{:repeatable-read}`. One cycle is textbook write skew: T1 `[[:r 53 1] [:r 53 1] [:w 54 2]]`, T2 `[[:r 53 1] [:w 53 2] [:r 54 nil]]`. This is G3 observed through Jepsen; the fix on `design/serializable-audit-a1-redis` is expected to turn it green. |
| Redis `MULTI` | `:valid? true` (678 ok). The race did not trigger at this rate; not evidence of correctness. |
| DynamoDB | `:valid? true` (1088 ok, 148 `:fail` from conditional checks). Not evidence: the `ConditionCheck` keys were in the write set (see the workload's note above), so the crossings were rejected as write-write conflicts and the read-lock window was never reached. |

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
  and seeing two mutations the last main-phase read missed. The `append/test`
  final reads in the three list-append workloads are enabled by
  `fix/jepsen-append-final-reads` (`f7cc098c`, on the outcome-unknown
  client change, not pushed): `with-final-phases` takes the package's
  `:wrap-generator` and wraps the whole composed generator so
  `max-key-tracker` sees every main-phase transaction and hands `:max-key`
  to the final phase. The package's own `final-gen` in the 0.3.13 jar reads
  `(range max-key)` in chunks of 8 and so never reads the highest key and
  drops a last chunk shorter than 8 (confirmed: with max-key 7 it issued no
  final read at all), so it is replaced by `cli/append-final-reads`, which
  reads every key 0 to max-key, 8 per transaction, each retried one at a
  time until `:ok` for up to 60 s, and throws if `:max-key` is missing. The
  evidence test (Elle 0.2.7, `:strict-serializable`): a history whose last
  append to a key is acknowledged after that key's last main-phase read is
  `:valid? true` without a final read and `G-single-item-realtime` with a
  final read that misses the append. 24 assertions red first (16 with the
  package's final reads: missed keys); full `lein test` 187 tests, 729
  assertions, green, and 50 repeated runs in one JVM. A 30 s Redis run
  against the A6 binary: 151 main-phase transactions, then three final
  read transactions covering keys 0 to 17 exactly once, five keys whose
  last append was acknowledged after their last main-phase read all seen
  only by the final read, `:valid? true`. `fix/jepsen-append-final-reads-required` (`a06fc0e9`) then makes the
  list-append checker refuse a run whose final reads left any key unread:
  every final-phase transaction carries `:final? true`, and
  `AppendFinalReadsChecker` (wired once in `cli/append-test`, so all three
  workloads get it) runs Elle, computes `max-key` from the history, counts a
  key as read only by an `:ok` transaction whose invocation carries the
  marker, and merges with `checker/merge-valid` (false beats `:unknown`
  beats true), returning `:valid? :unknown` with `:reason
  :missing-final-reads` and the unread keys otherwise; an Elle anomaly still
  wins. 51 assertions red first; full `lein test` 190 tests, 816
  assertions, green; a 30 s Redis run passes with `:final-reads {:max-key
  15, :complete? true}`, and the same run with the final-read timeout
  overridden to 50 ms returns `:unknown` with keys 8 to 22 unread and exits
  1 through `fail-on-invalid!`. Not yet done: the DynamoDB and multi-table
  final reads (8-key `TransactGetItems`) were not run against a server; no
  CLI option for the final-read timeout; the Redis list-append keys are
  plain integers without a per-run namespace. Also found: in the HT-FIFO
  `:recv` branch a `DeleteMessage` that errors but actually committed (an
  ambiguous timeout under faults) dropped its tuple, so the message was
  never redelivered and the checker would report a false `:lost`;
  **fixed** in `c6057c44` (under A6): a delete whose failure is classified
  `:info` (outcome unknown, a transport fault, a 5xx) puts the tuple in the
  receive op's `:in-doubt` list, which the checker counts as delivered for
  the no-loss check and ignores for duplicates and ordering
  (`:in-doubt-receives` is reported); a definitely rejected delete still
  drops it.
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
  with the cause kept in the chain; a context that expires after
  `rawNode.Propose` returned success yields unknown, because `Propose`
  appends the entry synchronously while the `(index, term)` is learned only
  from the next `Ready.Entries`, so "not yet tracked" is not "not in the
  log" (the implemented branch classified that gap as a plain context
  error, which a caller could retry: a fix to carry into the branch); a
  context that expires before `Propose` was called, or when `Propose`
  returned an error, is a plain context error, with no race against
  resolution (stores refuse an ended context, sends happen under the
  pending lock). Rejections before the log stay definite.
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
- The Jepsen clients record the class as `:info` with `:error
  :outcome-unknown` and the server text under `:message`, on
  `fix/jepsen-outcome-unknown-info` (`c6057c44`, on the harness-fix
  branches, not pushed): a shared `client_errors.clj` (the code constant,
  the Carmine `:prefix :outcomeunknown` check, the AWS error-code reader
  moved from the SQS fix, an S3 XML `<Code>` parser); the Redis append
  client catches it around `invoke!` (the submodule's `with-exceptions`
  did not know the code and the exception used to escape), for single ops,
  a top-level `EXEC` reply, and an element inside the `EXEC` array; the
  zset-safety client covers all five ops and a `ZINCRBY` error reply
  handed back as a value; the three DynamoDB clients check it first;
  S3 reads the XML body clj-http leaves in the exception; SQS checks it
  first in a new `error->op`. `NOTLEADER` and the allow-listed 4xx codes
  stay `:fail`. Tests use loopback Redis and HTTP servers speaking the real
  bytes (48 failures and 3 errors red first across seven namespaces; full
  `lein test` 180 tests, 643 assertions, green); a 30 s Redis run against
  the A6 binary passes without faults (no live `OUTCOMEUNKNOWN` observed:
  the local nemesis lives on the A4 branch). Not handled because no client
  uses them: the SQS batch form and gRPC `Aborted`. Still open: the forward
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
  both binaries, so the two never disagree on a V1 entry. That includes
  the replay detection: for a live V1 PREPARE whose `StartTS` equals a
  tombstone timestamp (the G13 shape) the old replica's timestamp fast path
  skips the entry while the index-based path would apply it, so a
  **pre-epoch** V1 entry keeps the timestamp fast path on both binaries
  (and keeps G13 until the flip). Once the ledger epoch has applied, every
  replica runs the new binary, and a ledger-era V1 PREPARE uses the
  index-based detection like a V2 entry (found in review: the timestamp
  shortcut would let a second transaction with the same `(primaryKey,
  StartTS)` and the same primary mutation but a different participant set
  be classified as the first one's replay, after which its secondary
  PREPARE lands on a group the durable row never named and the drain
  confirms the wrong participants). Every ledger-era V1 request also
  carries an `attempt_id` the coordinator generates once per transaction
  attempt (all binaries are new after the epoch, so the field needs no
  compatibility path), the ledger row records it next to the participant
  set, and a PREPARE whose row already exists with a different participant
  set **or a different `attempt_id`** is rejected as an aliased identity
  (the G12 class); only a PREPARE carrying the row's own `attempt_id` is a
  replay (found in review: with the same identity, participants, and
  primary mutation, a second transaction's delayed PREPARE would otherwise
  be indistinguishable from a replay and the first one's commit record
  would make its PREPARE and COMMIT succeed without applying its writes).
  Tests cover the different-participants pair and the same-participants,
  different-`attempt_id` pair. V2 entries use the
  index-based detection and never the timestamp fast path. The replicated
  watermark key
  `_meta_last_raft_commit_ts` and the snapshot layouts that carry it exist
  **only once the ledger epoch's row has applied** on that replica (the
  marker is a downgrade fence only, written before the node advertises
  and therefore before the epoch applies, so it cannot gate the key;
  corrected in review: gated on the marker, a node that had advertised
  and not yet applied the epoch would have created the key at open from
  its local `lastCommitTS`, the very seed the next sentences rule out,
  and carried it into snapshots that pre-epoch replicas then restored);
  on a pre-epoch or V1-pinned node the new binary writes
  neither (found in review: the shipped `isPebbleMetaKey` does not know the
  key, so the shipped binary's `findMaxCommitTS` decodes its last eight
  bytes as an inverted MVCC timestamp and, at every open, raises
  `_meta_last_commit_ts` to that bogus value with `Sync`, and whole-keyspace
  scans surface it as a user key; a V1-only cluster that downgraded, or
  installed a snapshot the new binary wrote, would therefore break OCC and
  snapshot reads). Concretely: the epoch's apply batch creates the key,
  `loadLastRaftCommitTS` only reads it back and never creates it (a
  missing key means pre-epoch, not "seed me"), the streaming MVCC layout
  moves from V3 to V4 and the backup encoder writes the raw entry only
  when the applied epoch row is present (the snapshot header's format
  field, `reserve`, `ledger`, `v2`, follows the applied epoch rows too,
  never the marker). The key's
  initial value is **Raft-agreed, not local** (found in review): the seed
  is the ledger epoch entry's own leader-issued commit timestamp, carried
  in the entry, and every replica sets `_meta_last_raft_commit_ts` to it in
  the same Pebble batch as the epoch's apply, so the value is persisted,
  travels in every later snapshot, and a restarted replica, or one whose
  log was compacted, recovers it from its store or its snapshot rather than
  from anything it would have to reconstruct (a pre-epoch store has no
  replicated watermark key, its `_meta_last_commit_ts` mixes Raft applies
  with direct writes, and its applied index records no timestamps, so
  "the highest commit timestamp of the entries it applied" is not
  recoverable there, found in review). The seed is safe: the admission stop
  drains every V1 transaction before the epoch, V1 entries never consult
  the watermark, and every V2 entry's timestamp is leader-issued after the
  activation epoch and therefore above the ledger epoch's timestamp, which
  every replica's HLC observed when it applied the entry. Seeding each
  replica from its own `lastCommitTS` would not do, because that value
  includes direct writes such as `CatalogStore.Save` that differ between
  replicas, and a replica with a direct write at `r + 1` would fence the
  first entry at `r + 1` while its peers, seeded at `r`, applied it. A
  replica whose `lastCommitTS` is above the seed keeps `_meta_last_commit_ts`
  as it is (the fence reads only the replicated watermark); the epoch
  entry's own commit timestamp then advances the watermark identically on
  every replica, and a test applies the epoch on two replicas, one holding
  a direct write above the seed, and asserts the same verdict on the next
  entry. A pre-epoch or V1-pinned store, snapshot, or backup stays
  byte-compatible with the shipped binary (a ledger-era node is already
  downgrade-incompatible through its marker and header, see the
  ledger-aware transition), and a test opens such a store and such a
  snapshot of each format through the shipped reader logic (the
  `isPebbleMetaKey` / `findMaxCommitTS` pair as they are on `main`) and
  asserts `_meta_last_commit_ts` is unchanged. A V1 entry never consults
  the watermark, so V1 mode loses nothing by not carrying it.
- **Ledger-aware transition.** The shipped binary cannot record V1
  participants or obey a new ledger epoch. Before the first V2 activation
  entry, the rollout stops new V1 transaction admission at every ingress,
  waits for every in-flight coordinator to reach a **definitive** outcome
  for each of its transactions, upgrades every voter, learner, and
  coordinator, and applies a
  Raft-serialized ledger-aware epoch to every **data** group (the
  dedicated TSO group runs `TSOStateMachine`, not `kvFSM`, and gets
  neither epoch: its `Apply` halts on any unexpected tag, so sending the
  envelope there would stop timestamp issuance instead of activating
  anything; the capability monitors watch the members of the data groups,
  and the watermark-advance read path already skips the TSO group for the
  same reason). "Definitive" excludes an unknown outcome (found in
  review): a pre-epoch V1 transaction has no ledger row, so if its primary
  decision committed, its secondary was not published, and the
  coordinator then crashed, the primary lock is gone and the only complete
  participant list went with the coordinator; the later lock and ledger
  scan cannot name the missing secondary and would publish `txn-v2` over
  a torn transaction. The upgraded coordinator therefore admits no
  transaction that the later scan cannot settle, by closing on its own
  side the three ways a V1 write goes missing without leaving a lock
  (rewritten in review: the earlier revision recorded each transaction's
  participant set in a reserved-key mutation of the primary PREPARE and
  confirmed every named secondary through a per-key receipt carried in
  the secondary PREPARE, and review found each piece defeatable under V1
  apply semantics: the receipt was a lock of its own, and the shipped
  `handleCommitRequest` and `LockResolver.resolveExpiredLock` settle one
  lock at a time, so on the G14 schedule the user key's lock was removed
  while the receipt's survived, the receipt landed alone, and the probe
  deleted the record over a torn secondary; a receipt per secondary key
  repeated the primary key once per key, so a transaction whose
  per-group PREPAREs fit could stop fitting once augmented while the
  bound on the aggregate record held; a receipt-bearing PREPARE could
  not take the G13 skip at all, so the G13 completion test expected an
  outcome that coordinator cannot produce; and two transactions sharing
  `(primaryKey, StartTS)` overwrote one record). The intermediate
  release changes only the coordinator, so every entry it proposes is a
  V1 entry the shipped binary applies exactly as today. (1) **Primary
  first.** It prepares in `txnPrepareOrder` (the A2 prepare-order
  branch, coordinator-only: the primary group's PREPARE has applied
  before any secondary PREPARE is sent, and the PREPAREs were sequential
  already). The shipped `primaryTxnStatus` then reaches "no primary lock
  and no record" only after a decision record exists: the shipped COMMIT
  writes the commit record in the batch that deletes the primary lock
  (`buildCommitStoreMutations`), an ABORT naming the primary writes the
  rollback record (`appendRollbackRecord`), `tryAbortExpiredPrimary`
  writes it before a resolver rolls a secondary back, and the status read
  reaches the primary group's leader (`getGroupAt` serves it through
  `leaderGetAt` or forwards it), so no reader or resolver removes a
  secondary lock except on the primary's recorded decision, which the
  shipped COMMIT honours (`ErrTxnAlreadyAborted` on a rollback record,
  the recorded timestamp otherwise). (2) **One fresh key per per-group
  PREPARE**, the attempt marker: `!txn|cmt|` + the group's lowest write
  key + the attempt id (16 random bytes) + `StartTS` with bit 62 set,
  with an empty value (no real timestamp sets bit 62 or bit 63, the
  physical half being 48 bits of Unix milliseconds). The attempt id is
  part of the key (corrected in review: keyed by the group's key and
  `StartTS` alone, two transactions with different primaries, the same
  `StartTS`, and the same lowest write key on one group, which the
  reservations let through, shared the marker; once the first had
  completed, every row of the second's delayed PREPARE on that group had
  a version at `StartTS`, so it was skipped as the first one's replay and
  its write was lost, the G13 shape again, and a value cannot help an
  existence probe). The shipped timestamp fast path
  (`raftApplyAlreadyLandedWith`) skips a PREPARE only when every store
  mutation has a version at `StartTS`, and the marker's lock and intent
  rows have one only if this PREPARE itself applied, so the G13 skip
  cannot take a marker-bearing PREPARE on either binary while a genuine
  re-proposal is still skipped, and the delayed PREPARE above now fails
  `validateConflicts` on the key the first transaction committed. The
  coordinator appends the marker to the group's PREPARE itself, so where
  the router would place the key does not matter (the FSM's
  route-ownership check skips transaction-internal keys); the shipped
  `txnRouteKey` strips eight bytes and routes the marker by the group's
  key followed by the attempt id, which sits in the group's range unless
  a split boundary falls inside that suffix, and either way nothing
  depends on the marker once its PREPARE applied: it is hidden and
  refused for raw writes as the commit-record family is, nothing reads
  it, it is committed or rolled back with the group's keys, and the
  post-activation sweep removes it. Its lock and intent rows route, once
  `routeKey` strips their one wrapper, by the marker's own bytes, so a
  range move inside the PREPARE-to-COMMIT window can leave them behind
  (true of every `!txn|cmt|`-keyed mutation, the earlier revision's
  record included); harmless, since an orphaned row is settled by the
  resolver like any lock. (3) **Identity reservation.** Before the
  primary PREPARE the coordinator runs a one-key V1 2PC on `!txn|cmt|` +
  primary key + `StartTS` with bit 63 set (the earlier revision's record
  key, now the reservation; the shipped readers of the family look up
  exact keys only), as a transaction of its own with identity (that key,
  `StartTS`) and the attempt id as value. The reservation transactions
  of two coordinators that admitted `(primaryKey, StartTS)` share the
  legacy identity (reservation key, `StartTS`) themselves, so the second
  PREPARE passes the ownership test as the first one's retry and
  overwrites the intent; the arbiter is the COMMIT, not the PREPARE
  (found in review). The shipped `validateConflicts` rejects a PREPARE
  whose key has a committed version above `StartTS`, and the shipped
  COMMIT rejects a second commit of one identity at another timestamp
  (`commit_ts mismatch`, `ErrTxnInvalidMeta`) and a commit after a
  rollback record (`ErrTxnAlreadyAborted`), so at most one reservation
  COMMIT lands and the key then holds exactly one committed version,
  carrying the attempt id of whichever PREPARE wrote the intent last.
  That committed value is the arbiter, not the COMMIT's own outcome
  (found in review: the outcome is not unique, because a second COMMIT
  stamped with the same timestamp, which `main`'s any-node stamping and
  a leadership change between the first COMMIT's Raft commit and its
  apply both produce, is accepted as an idempotent replay of the first,
  so both coordinators would see success). The coordinator drives its
  COMMIT to a definite outcome (an ambiguous proposal is retried with
  the same timestamp until a commit record or a rollback record
  exists), then performs a leader-fenced read of the reservation key on
  the primary group and admits the transaction only if the committed
  value names its own attempt; a foreign value, or no committed version
  (a rollback record), retries the whole transaction under a fresh
  snapshot before any PREPARE. The value is final once a version
  exists, because a later PREPARE on the key fails `validateConflicts`
  and nothing else writes it, and it names exactly one attempt, so
  exactly one coordinator is admitted however the PREPAREs and COMMITs
  interleave, with no dependence on timestamp uniqueness. The
  reservation path never enters the generic completion by which the V1
  coordinator finishes a transaction whose primary COMMIT failed but
  whose identity turns out committed (the G12 behaviour, where the
  loser's secondaries are committed at the winner's timestamp and
  success is reported), and it never aborts a reservation. No two
  transactions of the intermediate release therefore share an identity,
  which is what G12 needs. A raw or one-phase write cannot serve, since
  both refuse transaction-internal keys (`validateRawMutationForApply`,
  `buildOnePhaseStoreMutations`). Cost: two Raft round trips on the
  primary group per cross-group transaction, in the intermediate release
  only; the ledger era reserves through the ledger row instead. A
  coordinator that dies between the reservation and the primary PREPARE
  leaves a reservation with neither a lock nor a decision under its
  identity, which the scan ignores and the sweep removes; one that dies
  inside the reservation leaves a lock the resolver expires. A
  shipped-binary coordinator still admitting during the rolling restart
  neither reserves nor sees reservations, so an identity shared with it
  would alias with no crash for the attestation to name (found in
  review: bounding that by the restart window is not fail-closed). The
  intermediate release therefore admits a cross-group transaction only
  once a **reservation epoch** has applied on every participant group:
  once every node advertises the intermediate capability, the
  capability monitor proposes the epoch through each data group exactly
  as the V2 activation below is proposed (the same fatal-to-old-binaries
  envelope, the validated conf state the FSM checks at apply, the
  apply-enforced membership exclusion for a joiner without the
  capability attestation), and a node that applied it persists the
  `txn-reserve` form of its `raft-engine` marker, which the shipped
  binary refuses at startup, so the epoch is the intermediate release's
  first downgrade-incompatible step (corrected in review: a capability
  poll alone races a member restarted on the shipped binary and an
  incapable joiner, both of which could admit an unreserved transaction
  after one coordinator had seen every node capable, exactly the race
  the V2 transition records below). A coordinator observes the epoch
  once per group and caches it, since it is never revoked; until then
  its cross-group admission fails with a retryable upgrading error
  (single-group transactions carry no shared identity and are
  unaffected), so before the epoch only shipped coordinators admit
  cross-group transactions, and the epoch is proposed only after the
  last of them has drained and exited, which its graceful stop
  guarantees before the new process advertises, so no shipped-era lock
  is left for an intermediate transaction to alias. Two more guards
  (found in review): a node persists the `txn-reserve` marker form
  before it advertises the intermediate capability, as the ledger era
  does with `txn-ledger`, so a coordinator that advertised and was then
  restarted on the shipped binary is refused at startup rather than
  admitting unreserved transactions against groups whose epoch already
  applied; and once the epoch applies on a group, that group's FSM
  rejects a PREPARE that carries no attempt marker
  (`ErrTxnReservationRequired`, a definite error the coordinator reports
  as the upgrading retry), deterministic because every member of the
  group is the intermediate binary by then, so a coordinator the barrier
  missed still cannot land an unreserved cross-group PREPARE there. The
  guard recognises the reservation by shape (corrected in review: the
  reservation is itself a one-key V1 2PC that carries no marker and
  runs only after the epoch, so a guard on the marker alone would have
  rejected every reservation and with it every cross-group transaction
  of the intermediate release): a PREPARE whose only mutation is a
  bit-63 reservation key under `!txn|cmt|` and whose primary key is
  that same key is admitted without a marker, and any other marker-less
  PREPARE, a reservation key next to a user key included, is rejected.
  The ledger epoch supersedes the guard: once it has applied on a
  group, that group's FSM admits a V1 PREPARE only in the ledger-era
  form (the request-level `attempt_id` on every PREPARE, the ledger row
  on a primary one) and rejects the marker-bearing and the
  reservation-shaped forms of the intermediate release with the same
  definite error; that strands nothing, since the ledger epoch waits
  for every node to advertise `txn_v1_drain_ledger`, which an
  intermediate binary never does, so the last intermediate coordinator
  has drained and exited before it, and a reservation leaves no lock
  and no decision under its identity for a later PREPARE to depend on.
  Tests: a node restarted on the shipped binary after advertising is
  refused; on a group whose epoch has applied, a reservation PREPARE
  and COMMIT followed by marker-bearing primary and secondary PREPAREs
  apply end to end, an unreserved marker-less PREPARE is rejected, and
  a marker-less PREPARE carrying a reservation key next to a user key
  is rejected; after the ledger epoch a reservation-shaped PREPARE and
  a marker-bearing one are rejected while a ledger-era PREPARE
  applies. Shipped-against-shipped aliasing during the
  restart is `main`'s own G12, not the drain's to find; the runbook
  removes it by routing cross-group traffic away from nodes awaiting
  restart or by pausing it at the ingress for the window. Test: a
  cross-group transaction admitted on an upgraded node while a shipped
  coordinator is still reported is refused with the retryable error, and
  one admitted after the last report flips reserves and proceeds. (4) **Sizing.** The coordinator sizes every per-group
  PREPARE after augmentation against the transport limit before issuing
  any PREPARE and refuses the transaction with a client-visible size
  error (found in review: sizing one aggregate misses a per-group
  request that no longer fits once augmented); the adapters' transaction
  shapes sit far below it (DynamoDB caps a transaction at 100 items,
  Redis `MULTI` and Lua carry their keys). (5) **No cleanup ABORT after
  the primary commits** (found in review): on `main`,
  `TransactionManager.commitSequential` treats a failed secondary COMMIT
  as needing cleanup, so a cancellation or a proposal error after the
  primary COMMIT is durable fires a detached ABORT that removes that
  secondary's lock and intent, and `commitSecondaryTxns` still reports
  success, leaving a missing write with no lock for the scan to find;
  the A2 status's `txnCleanupRequests` exclusion (no cleanup for a COMMIT
  that does not name its primary) is coordinator-only and therefore
  ships in the intermediate release too, a failed or cancelled secondary
  COMMIT after a durable primary COMMIT is reported as outcome unknown
  and left to the resolver, never cleaned up, and the test runs primary
  commit, cancelled secondary commit, activation drain, and asserts the
  lock is found and the write published. With the closures, every
  write of a transaction the intermediate release admitted either holds
  a lock the epoch scan settles from the primary's recorded decision or
  has already become the version or the rollback that decision produced,
  so the epoch proposer scans every data group for pre-epoch locks, as
  before, settles them through the `LockResolver`, ignores reservations
  and markers with no lock and no decision under their identity, and
  needs no participant list, no confirmation probe, and no
  exact-timestamp version probe (the `CommittedVersionAt` exact mode of
  the earlier revision is withdrawn). Tests: the G13 and G14
  reproductions run under the intermediate coordinator against the
  shipped FSM and **pass** (every write lands, the reader gets `txn
  locked`); the G12 aliasing reproduction under it, run once with the
  two reservation PREPAREs overlapped before either COMMIT, once with
  the second PREPARE after the first COMMIT, and once with the second
  COMMIT stamped at the first one's timestamp across a leadership change
  between the first COMMIT's Raft commit and its apply, ends with
  exactly one coordinator admitted, the other retried under a fresh
  snapshot, and no mixed outcome; a coordinator that loses its data
  directory
  after the primary commit and before the secondary COMMIT is replaced
  by an empty one, the scan finds the secondary lock, the resolver
  publishes it from the commit record, and the epoch applies; the same
  with the coordinator merely restarted; a coordinator crashed between
  the reservation and the primary PREPARE leaves nothing the epoch waits
  for; the resolver settles a marker's lock and the user key's lock in
  either order to the same outcome; a per-group PREPARE that fits only
  before augmentation is refused before any PREPARE is sent; and two
  transactions with different primaries, the same `StartTS`, and the
  same lowest write key on a secondary group, the second's PREPARE there
  delayed past the first's COMMIT, see that PREPARE fail with a write
  conflict instead of being skipped.
  Transactions admitted by a shipped-binary coordinator carry neither
  marker nor reservation, so the rollout is two steps (corrected in
  review: calling such a transaction pre-existing damage did not make the
  drain fail closed). First, an intermediate release that keeps V1
  semantics and adds only the coordinator-side closures above is rolled
  onto every node by graceful restart, which the shipped binary's gRPC
  and HTTP listeners already honour (`GracefulStop` and
  `http.Server.Shutdown` drain in-flight requests, and a `Dispatch` runs
  inside its request), and that release makes the Redis listener's stop
  drain the same way (checked in review: a Lua or `MULTI` transaction
  cut mid-2PC is exactly the case the closures exist for); admission
  stops only once every member and coordinator advertises the
  intermediate capability, so every transaction in flight at the stop
  was admitted with them. What that cannot cover is a shipped-binary
  coordinator that died ungracefully (a crash, a kill past the drain, a
  lost data directory) before its upgrade: nothing on the server can
  name its transactions. The activation command therefore refuses to
  propose the epoch unless the operator passes an explicit attestation
  that every such incident has been reconciled; the attestation and the
  incident list travel in the epoch entry, and the runbook lists how to
  find the incidents (restart reasons, the lock namespace,
  application-level checks). The disk-loss test is also run with the
  transaction begun on the shipped binary before the upgrade, and asserts
  the refusal without the attestation and the activation with it. The
  ledger epoch is the first downgrade-incompatible step,
  not a soft poll: a node advertises `txn_v1_drain_ledger` only after
  persisting the downgrade-incompatible `txn-ledger` form of its
  `raft-engine` marker (the marker forms are described under **Refusal on
  the old side**), the epoch entry itself travels under the
  fatal-to-old-binaries envelope, and a ledger-era node writes the
  discriminating snapshot header, so a member that attested, applied the
  epoch, and was restarted on the shipped binary is refused at startup
  rather than admitting V1 transactions that record no participants and
  reserve no identity (found in review: such a transaction could reach a
  partial outcome that the later V2 gate, seeing neither a ledger row nor a
  lock, would miss and declare V2 in effect over). V1 admission reopens
  only after that durable epoch is applied on every data group; thereafter
  every accepted V1 transaction records its participants before PREPARE.
  The epoch carries the validated conf state, and membership admission
  after it checks the ledger capability at apply as the V2 epoch does.
  Pre-epoch transactions with visible unresolved locks must settle before
  V2 activation; an outcome whose participants cannot be reconstructed
  requires operator reconciliation and keeps the gate closed. Historical
  partial transactions whose locks and participant evidence are already
  gone cannot be detected retrospectively and remain outside the
  forward-looking claim.
- **Cluster capability.** Every node advertises separate
  `txn_v1_drain_ledger` and `txn_format_v2` capabilities in the report the
  storage-envelope and encryption monitors already
  read; a monitor built like `startStorageEnvelopeV2CapabilityMonitor` /
  `buildEncryptionCapabilityFanout` watches for every voter and learner of
  every data group to report each capability before its corresponding epoch.
  The flip itself is **not** a per-process
  atomic set from that poll: a membership change admitted while this
  process still sees V1 could race another coordinator that finished its
  poll and flipped, which would then send a V2 entry to the newly admitted
  old member and halt it, costing quorum. Activation is therefore a
  **Raft-serialized epoch**: the monitor proposes a `txn_format_v2`
  activation entry through each data group (the same fatal-to-old-binaries
  envelope, so an old member that slipped in halts here, loudly, before any
  data entry), carrying the conf state it validated against; the FSM
  rejects the entry if the group's current conf state differs (the
  proposer re-validates and retries). The membership exclusion is
  **durable and enforced at apply**, not by a leader-local "pending"
  flag, which a leader change would lose (an activation appended but not
  yet applied, then a new leader admitting an incapable member after it):
  every `AddLearner` / `AddVoter` conf change carries the joiner's
  capability attestation in its context, and at apply the engine cancels
  the conf change (etcd/raft applies an empty `ConfChange` as a no-op) when
  the group's applied activation epoch is active and the attestation is
  missing, and **resolves the pending admin request with a definite
  rejection** (`ErrTxnFormatJoinerNotCapable`) rather than the success the
  config-change apply path returns today from the committed entry's
  context, so an operator or the rollout script never believes a member
  was added when the configuration did not change (a test covers the
  ordering: activation applies, then an already-queued `AddLearner` is
  cancelled and its caller gets the rejection); a leader also waits until
  its applied index reaches its commit index before proposing any
  membership change, so it reasons from the log it has already applied. **Activation drains V1 work before the
  guarantees are claimed.** Once the activation entry applies on a group,
  that group's FSM rejects every V1 **mutating** entry: a V1 PREPARE, a V1
  one-phase entry, and (found in review) a V1 raw `PUT` / `DEL` or
  `DEL_PREFIX` entry, which are separate non-transactional apply paths
  whose V1 semantics carry no apply-order fence, no foreign-read-lock
  check, and no `DEL_PREFIX` lock check, so a V1 raw entry proposed before
  activation and applied after it could write a key under a V2
  transaction's read lock and recreate the raw-write cycle recorded under
  A2 (the coordinator treats every such rejection as a pause and retry
  signal, waits until the V2 activation epoch has applied on every
  participant group, revalidates membership and capability, and then
  re-proposes a raw write as V2 itself, while a read-bearing transaction
  is returned to the adapter as a retry sentinel (`ErrTxnFormatRetry`)
  rather than restarted by the coordinator (corrected in review: the
  coordinator holds the computed mutations and the read-key names, not
  the observations that produced them, so re-running them under a fresh
  V2 `StartTS` would let a write committed between the original read and
  the retry fall below the new snapshot and pass validation, and keeping
  the old `StartTS` keeps the unfenced V1 read's G0 exposure); the
  adapter repeats its reads under a V2 snapshot and recomputes the
  mutations, exactly as the composed-1 retry stays in the adapter under
  A2, with the same metadata-only retry and precondition re-validation
  for a streaming S3 request whose body is consumed, and a test drives a
  read-bearing DynamoDB transaction, a Redis `MULTI`, and a streaming S3
  put through activation and asserts each re-read before retrying; it
  sends no V2 entry to any participant before this barrier; a delayed V1
  one-phase or raw entry can no longer bypass the fence, and a V1
  transaction that read before activation can no longer lock after it,
  which is the G1 shape a V1 PREPARE racing a V2 one-phase transaction
  would reopen), and does not
  let an in-flight V1 transaction **commit** under V1 semantics either,
  since those semantics carry the reproduced G12 and G14 failures (an
  aliased or half-prepared V1 transaction whose COMMIT publishes the
  primary and skips a secondary, removing the last lock row as it does so,
  which an empty lock namespace would then mistake for a clean finish): a
  V1 **primary** COMMIT is rejected with a retryable abort (the coordinator
  aborts the transaction's V1 locks and, for a read-bearing transaction,
  returns it to the adapter as the same `ErrTxnFormatRetry` a PREPARE
  rejected by activation produces, so the adapter repeats its reads under
  a V2 snapshot and recomputes the mutations; corrected in review: at
  commit time the coordinator holds only the computed mutations and the
  read-key names, so a V2 retry under a fresh `StartTS` would let a
  write committed after the original read fall below the new snapshot
  and pass validation, and a retry under the old `StartTS` would keep
  the unfenced V1 read's G0 exposure; only a transaction with raw writes
  and no reads is re-proposed by the coordinator itself as V2, after the
  same participant-wide activation barrier. Test: a read-bearing V1
  transaction whose PREPAREs applied before activation and whose primary
  COMMIT arrives after it sees its locks aborted and its adapter re-read
  and recompute, and a write committed between the original read and
  the retry is observed by the retry), a V1 ABORT is accepted against an
  existing legacy lock, and a V1 **secondary** COMMIT is accepted only for
  an identity whose legacy commit record already exists on the primary
  group (the coordinator verifies the record before proposing, as the
  resolver does), so a transaction that reached its commit point before
  activation is published, and a missing lock there fails loudly with
  `ErrTxnWriteLost` rather than skipping. The resolver settles every
  remaining legacy lock from its primary's record, and V2 transactions
  run next to those settlements without waiting for the drain (checked
  in review: a legacy transaction with a durable primary record at `c`
  and a secondary lock not yet settled cannot give a V2 transaction a
  stale observation, because the lock row, written at the legacy
  `StartTS` below `c`, is visible to every V2 snapshot read at `s ≥ c`,
  and the read settles it before returning: the point read resolves the
  lock from the primary's record and lands the secondary COMMIT at `c`
  before reading the key (`leaderGetAt` → `maybeResolveTxnLock`, which
  A0's `leaderReadAt` keeps), the scan paths plan the resolutions,
  apply them, and re-read every locked key (`resolveScanLocks` →
  `materializeScanLockResults`), a pending primary fails the read with
  `txn locked`, and a non-leader proxies to the leader; so an
  observation at `s ≥ c` includes `c`, an observation at `s` between
  the legacy `StartTS` and `c` excludes `c` and its PREPARE finds
  `latest = c > s`, and a read below the legacy `StartTS`, which sees no
  lock, is followed by a PREPARE that meets the legacy lock, foreign
  under A2, or finds `latest = c > s`: a conflict every way. Pausing V2
  admission until the drain completes would stall every write for the
  drain's duration, bounded below by the lock TTL, for no further
  safety. Test: a legacy transaction whose primary COMMIT at `c` is
  durable and whose secondary lock is unsettled, then activation; a V2
  transaction reads the secondary key at `s ≥ c`, observes the version
  at `c` with the secondary COMMIT landed through the read, and its
  PREPARE on that key succeeds; a V2 transaction that read at `s` below
  `c` has its PREPARE refused; and the scan form of both). The
  coordinator flips
  its atomic `txnFormatV2` (`WithTxnFormatV2`, subsuming
  `WithTransactionIDs`) when it applies the activation entry on every group
  it coordinates, and never flips it back; the point at which the audit's
  guarantees hold, recorded in the `raft-engine` marker as "V2 in effect"
  and the precondition for the roadmap's README claims, is when every
  group reports no legacy-shape lock row left **and** every legacy commit
  record written since activation has had its secondaries confirmed. That
  confirmation is a durable Raft-replicated drain ledger. Every V1
  transaction admitted after the ledger-aware epoch records its stable
  identity, primary group, and complete participant set **in the primary
  group's PREPARE entry, atomically with the primary lock** (found in
  review: a row committed before PREPARE would be orphaned by a
  coordinator crash right after it, with no lock, commit record, or
  rollback to make it terminal, and the gate below would then block the
  upgrade for good). The row therefore exists exactly when the primary
  lock does: a crash before the primary PREPARE leaves nothing, and a
  crash after it leaves a lock the resolver already settles (TTL expiry,
  rollback), whose settlement makes the row terminal in the same entry. A
  secondary PREPARE delayed past that rollback is **not** fenced at apply
  (corrected in review: `rejectPrepareAfterRollback` runs only for a
  PREPARE naming the primary key, and the rollback record lives on the
  primary group alone, so a secondary group's FSM cannot see it): it
  creates a lock and intent under an identity whose row is already
  terminal, an orphan the drain treats like any other legacy lock. It
  can never commit, because the primary's rollback record makes every
  COMMIT under the identity fail (`commitApplyStartTS`,
  `ErrTxnAlreadyAborted`) and the identity stays reserved until
  activation; the resolver, or any reader that meets the lock, settles it
  from that record with an ABORT; and the row's per-key confirmation is
  not the gate's last word for it, since the completion step reads the
  lock namespace itself ("no legacy-shape lock row left") after the
  activation epoch has applied on every data group, and after activation
  the FSM rejects a delayed V1 PREPARE outright, so a delayed secondary
  PREPARE either lands before activation, where that scan finds its lock
  and the resolver aborts it before the marker is written, or is
  rejected. Test: a ledger-era secondary PREPARE delayed past the
  primary's TTL rollback and the row's terminal confirmation lands, the
  resolver aborts it from the rollback record, the completion step's scan
  finds the lock and withholds the marker until it is gone, and the same
  PREPARE arriving after activation is rejected. Because
  the row rides in the primary PREPARE, the coordinator sizes that
  PREPARE with the row against `maxMarshaledCommandSize` before admission
  and refuses the transaction with the client-visible size error when it
  does not fit (found in review: every per-group request can fit while
  the aggregate participant set pushes the primary command past the
  64 MiB bound, and such a transaction would otherwise be admitted and
  then fail to replicate for the whole ledger era); the row names each
  participant by group id and key, the adapters' transaction shapes sit
  far below the bound, and a test drives a transaction spread over many
  groups whose per-group requests fit while the row does not. The
  transaction then replicates its decision and each secondary
  confirmation. The V1
  legacy identity `(primaryKey, StartTS)` is reserved on the primary group
  **until V2 activation**, not merely until the row is terminal and
  confirmed: a terminal row is kept, never released (found in review: a
  released identity would let a second transaction's delayed PREPARE with
  the same identity and participants ride the first one's commit record
  as a replay), so an aliased V1 transaction can neither take over an
  outstanding outcome nor reuse a settled one; the rows are small and the
  ledger era is bounded by activation, which removes them. The primary COMMIT
  or ABORT record and the ledger decision are applied atomically in one
  Raft entry; a separate later decision write would leave a crash gap.
  Confirmations are per locked key because the resolver can settle one
  key at a time. The coordinator's outstanding
  list is only a cache rebuilt from this ledger after restart; the resolver
  updates it as secondaries settle. A V1 transaction without a reconstructible
  participant record cannot cross the activation boundary, and the marker
  cannot be written while any ledger entry is incomplete or any visible
  pre-epoch legacy outcome remains unresolved. The gate is tested by
  crashing the coordinator after a primary decision and before secondary
  publication:
  restart must reconstruct the pending participant and keep V2 out of effect
  until confirmation is replicated; and by crashing it between the primary
  PREPARE and the secondary PREPAREs: the orphaned primary lock expires,
  the resolver rolls it back, the row becomes terminal with the rollback,
  a delayed secondary PREPARE then lands as an orphan (the secondary
  FSM cannot see the primary group's rollback record), the completion
  scan withholds `txn-v2` while that lock exists, and the resolver aborts
  it from the primary's rollback record. This verifies outcomes, rather than
  inferring them from an empty lock namespace. A V1 transaction that
  completed in a partial state
  before activation is pre-existing damage that activation cannot detect;
  the claims are forward-looking from "V2 in effect". An operator flag pins the format to V1
  for a deployment that must keep an old binary around, with a startup
  warning that the audit's guarantees are off.
- **Refusal on the old side.** A V2 entry must never be *applied* by an
  old binary, and the monitor alone cannot guarantee that: after the
  one-way flip, the supported fresh-learner path can add an old binary
  with an empty data directory, which has no marker, ignores the unknown
  `txn_format` field, and applies legacy semantics. Two guards close it.
  First, V2 entries are **fatal to the old binary**. A merely unknown tag
  byte is not enough: `kv/fsm_encryption_test.go` pins that unknown tags
  above the reserved band fall through as non-halting errors, and the
  engine (`internal/raftengine/etcd/engine.go`, the apply loop) advances
  the applied index on such a response, so an old member would *skip*
  every V2 entry and diverge silently; and changing the new binary's
  dispatch cannot change what the shipped binary does. V2 entries are
  therefore encoded under a tag and subtype the **shipped** release
  already handles fatally: the `raftEncodeBackup` tag with a new subtype
  byte (`backupSubtypeTxnV2`), which the shipped `applyDecodedBackup`
  rejects with `ErrBackupWireSubtype` and `applyBackup` turns into
  `haltErr` (the same path halts on a missing timestamp tracker). The new
  binary's `applyBackup` dispatches that subtype to the transaction apply
  path before the backup decoder sees it, and **`IsVolatileOnlyPayload`
  becomes subtype-aware**: today it answers true for the whole
  `raftEncodeBackup` tag (HLC leases and backup pins are the two
  volatile-only payloads the cold-start skip gate must still deliver past
  the snapshot index), so a V2 transaction under that tag would be
  re-applied from the WAL tail after a restart whose durable applied index
  is ahead of its snapshot, re-running transaction side effects with
  `SetApplyIndex` suppressed and feeding the index-based replay detection
  an index it never recorded; the classifier returns true only for the
  genuine backup-pin subtypes and false for `backupSubtypeTxnV2`, it stays
  a pure function of the bytes, and a cold-start-tail test applies a V2
  entry past the snapshot index exactly once. A halted learner is a loud
  failure, not a divergent one; the alternative, an intermediate release
  that installs a fail-closed dispatch before V2 can activate, is recorded
  as the cleaner path for a later format bump. Second, **membership
  admission** checks capability: `AddLearner` / `AddVoter` on a V2 cluster
  ask the joining process for its capability report before proposing the
  conf change and refuse a node that does not advertise `txn_format_v2`.
  The `raft-engine` marker (the same file that already records the
  backend) has four forms: the legacy `etcd` form, the `txn-reserve`
  form the intermediate release writes before advertising its capability
  (added in review: the reservation epoch introduced the form without
  listing it here, and a node restarted after the epoch would have
  rejected its own directory), the `txn-ledger` form, and the `txn-v2`
  form, moving only forward in that order; the shipped binary rejects
  the last three at
  startup with the error it already has, `ErrUnsupportedRaftEngine`
  wrapped as "invalid raft engine marker" (`readRaftEngineMarker` →
  `parseRaftEngineType` in `multiraft_runtime.go`; the shipped binary has
  no `ErrTxnFormatDowngrade` sentinel, so the rollout runbook and the
  compatibility test match on that message, found in review), and the new
  binary reads all four and raises `ErrTxnFormatDowngrade` itself when a
  process pinned to V1 by the operator flag is started on a
  `txn-reserve`, `txn-ledger`, or `txn-v2` directory. A
  process advertises `txn_v1_drain_ledger` or `txn_format_v2` only after
  it has persisted the downgrade-incompatible `txn-ledger` form, so a
  capability report that can satisfy either epoch is backed by durable
  state rather than by a process poll: a voter that advertised and is then
  restarted on the shipped binary before an epoch entry applies is refused
  at startup instead of starting, matching the still-V1 polled state, and
  halting on the epoch envelope, or, worse for the ledger epoch, admitting
  V1 transactions the ledger never sees. The V1 pin suppresses both
  advertisements and the marker write, so a pinned cluster stays
  downgradable. (Downgrading two of three voters after they advertised
  still costs the group its quorum until they are upgraded again; the
  marker turns that into a startup refusal with a named error rather than
  a halt mid-apply, and the rollout runbook says so.) The `txn-ledger`
  form is also the downgrade fence for V2 entries and V2 snapshots: a
  replica applies its first V2 entry, or installs / restores a ledger-era
  or V2 snapshot, only while its marker is at least `txn-ledger` (a fresh
  replica writes that form before the restore mutates the FSM), and the
  `txn-v2` form means something else, **V2 in effect**: it is written by
  the drain's completion step once the activation epoch has applied on
  every data group and every ledger row is terminal and confirmed, and it
  is the publication gate the roadmap's README claims wait on. The two
  states are distinct on purpose (found in review): writing `txn-v2`
  before the activation entry applies would make a crash at that point,
  or an activation rejected for a conf-state mismatch, indistinguishable
  from a drained V2 group, while delaying the fence to the drain would
  leave V2 entries applied on a downgradable directory. A crash between
  activation apply and the `txn-v2` write restarts as fenced, active, and
  not in effect: the marker says `txn-ledger`, the applied activation row
  says active, the ledger says what remains, and the drain resumes from
  it; a rejected activation leaves the marker at `txn-ledger`. The
  receiver must be able to tell a ledger-era or V2 snapshot apart
  **before** restoring it:
  Raft snapshot metadata carries only index, term, and the conf state, and
  an epoch row inside the FSM payload is discoverable only after
  installation, so a fresh replica could otherwise restore such state
  without writing the marker and then be restarted under the shipped
  binary with no fatal envelope in its WAL. Reservation-era, ledger-era,
  and V2 snapshots are therefore written under a new authenticated FSM
  snapshot header magic (`EKVTHLC3`, whose payload carries the
  transaction format, `reserve`, `ledger`, or `v2`, next to the ceiling
  and the cutover index; `reserve` added in review, since a
  reservation-era snapshot in the old format could be installed by a
  fresh learner or a restore under the shipped binary once the epoch
  entry was compacted, which would then admit unreserved transactions),
  which the new binary reads and fences on before mutating the FSM (the
  marker moves to the format's form first, `txn-reserve` for a
  reservation-era snapshot and `txn-ledger` for a ledger one, whenever
  the directory's marker is behind it) and which the shipped
  `ReadSnapshotHeader` rejects fail-closed with
  `ErrSnapshotHeaderUnknownMagic` (a trailing V2 extension would not do:
  the shipped reader skips unknown trailing bytes); the external restore
  formats (the streaming MVCC V4 layout, the backup encoder's archive, the
  SST ingest and physical snapshot manifests) carry the same format field,
  and `elastickv-snapshot-prepare-restore` and the physical restore path
  fence on it the same way. A node before the reservation epoch, or a
  V1-pinned one, keeps writing the `EKVTHLC2` header, so such clusters
  stay restorable by the shipped binary. The marker update is atomic and fsyncs the file and directory. A marker-write failure halts apply
  or restore before the durable applied index can advance or a read can
  be served. This covers a follower that never proposes a V2 entry: its
  WAL may omit an already-applied V2 envelope on restart, so the fatal
  envelope alone cannot prevent a downgrade. A downgrade after V2
  traffic fails loudly (`ErrUnsupportedRaftEngine` on the shipped binary,
  `ErrTxnFormatDowngrade` on a V1-pinned new binary) instead of applying V2
  entries with V1 semantics, and the new binary refuses to start a group
  whose configured peers include a node without the capability while
  the marker says V2.
- **Tests.** A two-binary simulation is out of reach in unit tests, so the
  evidence is: table tests that every gated verdict is a no-op for V1
  entries and active for V2; a coordinator test that the flag flips only on
  full capability and stays flipped; in-flight V1 transactions finishing
  after the flip; a follower applying V2 before it ever proposes, then
  restarting under the shipped binary and rejecting downgrade; V2
  snapshot install/restore persisting the `txn-ledger` marker before
  serving, and before any FSM mutation when the snapshot arrives at a
  fresh replica; a crash between activation apply and the `txn-v2` write
  restarting as fenced and active but not in effect, resuming the drain,
  and publishing only when the drain completes; an activation rejected
  for a conf-state mismatch leaving the marker at `txn-ledger`;
  the shipped `ReadSnapshotHeader` rejecting an `EKVTHLC3` header; a
  capability report (either capability) refused until the `txn-ledger`
  marker is durable, and the shipped startup check rejecting a data
  directory that holds it; the ledger epoch entry halting the shipped
  binary; neither epoch proposed through the dedicated TSO group;
  marker fsync failure preventing applied-index advance; restart after the marker
  write but before V2 apply; staggered activation of two participant groups
  (the coordinator waits rather than sending V2 to either group), including
  a membership change during that wait that forces capability revalidation;
  and a Jepsen run of the
  rolling-update script (`rolling-update.sh`, the deploy runbook) across
  the format change under the list-append workload.

### A5. Documentation

The encryption design doc's stale header (Stage 5E marked deferred while
shipped and wired) is corrected on `docs/encryption-5e-header`
(`0168e290`, from `main`, not pushed), the first A5 item to land.

- README: replace the "Basic Consistency Behaviors" bullet with the three
  claims from the positioning doc §5, the lease caveat, and the three
  exclusions the roadmap's §6.1 gate names (V1-pinned or mid-upgrade
  clusters, Phase-D clusters, resolver-routed keys).
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
- `go test -race ./...` (`make test`, the repository-wide suite
  `CONTRIBUTING.md` requires) is green and includes the new interleaving
  tests; A6 and A7 change `internal/raftengine/etcd` and the root package's
  capability and activation wiring, so a package subset is not completion
  evidence.
- `make tla-check` passes with `OCC6_SnapshotStableAtWatermark`,
  `OCC7_NoLostUpdate`, and `OCC8_NoWriteSkew`, and fails all four gap
  configurations as expected: `MCOCC_gap_applyorder.cfg` (`OCC6`),
  `MCOCC_gap_readlocks.cfg` (`OCC8`), `MCOCC_gap_readlock_intent.cfg`
  (`OCC8` on the foreign-write-lock interleaving), and
  `MCOCC_gap_rawwrite_readlock.cfg` (`OCC8` on a raw write ignoring a
  transaction's read lock, the A2 cycle).
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
