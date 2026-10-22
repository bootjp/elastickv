# DynamoDB snapshot seal for read-modify-write transactions

Status: Partial
Author: bootjp
Date: 2026-10-11

## Failure and scope

Jepsen run [37207021947](https://github.com/bootjp/elastickv/actions/runs/37207021947) observed a successful DynamoDB list append disappear from a later read. A one-phase write can allocate commit timestamp `C`, then pause before proposing its Raft entry. Another write to a different key can advance the applied watermark beyond `C`. A later transaction fences the already-proposed log, reads the old item at snapshot `R > C`, and plans an overwrite. If the first entry arrives after this read, ordinary OCC only checks for versions with commit timestamp greater than `R`; it cannot identify the unseen write at `C`.

This applies to UpdateItem, PutItem, DeleteItem, BatchWriteItem, admin writes, and TransactWriteItems, including ConditionCheck. It also applies when the two requests enter different servers or leadership changes. Process-local item locks reduce races within one server but do not establish a distributed safety boundary.

## Required invariants

1. A read snapshot at timestamp `R` includes every previously proposed write that can commit at or before `R` in each group it reads.
2. A write allocated before the snapshot but proposed later cannot commit at or before `R`. It must fail with a retryable conflict and recompute against a new snapshot; ambiguous retries retain the existing exact-commit-timestamp dedup probe.
3. The boundary survives follower ingress, leader election, crash recovery, and a table spanning data groups. A local mutex or a lease-only read is insufficient.
4. All single-item entry points derive their local contention key from only the table's hash and range key attributes. ConditionCheck participates because an existing item can produce a same-value Put and a missing item still contributes a read dependency.

## Implemented protocol

Before planning a DynamoDB read-modify-write transaction, obtain the strict committed floor for the touched table routes and write a reserved snapshot-seal key for every item the operation touches. Each seal key uses the internal `!ddb|seal|` family and includes the canonical table, generation, and a two-byte digest of the primary item key. It routes to the same data group as that table's items. The digest bounds live seals to 65,536 keys per table generation, at the cost of occasional false contention between items in one bucket.

Dispatch the first single-table seal transaction without a commit timestamp so the owning Raft leader assigns `R`. A follower ingress forwards that request to the leader and uses the returned timestamp; it never persists a follower HLC timestamp. Dispatch each remaining table-route seal transaction with explicit `CommitTS = R`. Wait for every response to confirm `R` before reading items at `R` and dispatching the user transaction with `StartTS = R`. If any route fails or returns an unconfirmed timestamp, fail the request; seals already applied are harmless. A multi-shard transaction response alone is insufficient because secondary commit errors may be deferred by the coordinator. After sealing, verify that each table still has the generation used to derive its seal key. A changed generation requires fresh lock and seal derivation before retrying.

Every item-writing transaction includes each affected item's seal key in `ReadKeys`. If a write was allocated before the seal but proposed afterward, the seal's committed version is newer than that write's original `StartTS`; the existing Raft-apply OCC check rejects it as a retryable conflict. If the write was proposed before the seal, the seal's Raft ordering makes it visible to the read at `R`. A later seal similarly conflicts with any earlier transaction that has not yet committed. The seal is ordinary MVCC data, persisted with the Raft applied index and restored with snapshots; no new wire format or independent floor state is needed. Live and logical snapshot backups exclude seals, while table-generation cleanup removes them. Full Raft snapshots retain seals for OCC recovery.

The seal must be a Raft write rather than a client-side check of `LastCommitTS`: the latter cannot see an allocated, unproposed timestamp. A ReadIndex establishes the committed floor before the seal, but by itself does not prevent an older timestamp from arriving after the read. Local item locks can remain as a contention optimization and must never be relied on for correctness across nodes. Seal buckets allow writes to different buckets in the same table to proceed independently; hot-item contention, digest collisions, and the extra Raft transaction must be measured under Jepsen and a benchmark.

## Failure handling and validation

- Failure to allocate `R`, reach any target leader, or apply and confirm a seal fails the request closed; it must not use a follower-local persistence timestamp. An unrelated shard is outside the touched-route floor and does not block a healthy table.
- A leader change does not clear a seal. The successor restores the seal key before accepting item writes. The HLC physical-ceiling fence prevents the successor from issuing timestamps in the old leader's window.
- Tests must hold a write after allocation and before proposal, advance another key's watermark, seal and plan on a second server, then release the first write. Repeat with a leader change and with two data groups. The final item must contain both successful appends, or one operation must return a retryable failure.
- Jepsen should exercise follower ingress without an HTTP leader map and leader changes as well as the existing three-node append workload.

## Implementation and verification status

The protocol landed in [PR #1259](https://github.com/bootjp/elastickv/pull/1259), merged on 2026-10-11 as `b6c6c917f4197f7ca2fb1951b232b587644d3368`.
Its final branch commit was `30dc3498159773423af4029745ba7233e7ba4fdb`.
The implementation is present in the audited main revision `429f5ee74e36590a18358cccce25849dc95b418a`.
The former Proposed status and instruction to keep #1259 open were stale.
No replacement seal implementation is needed on the basis of this audit.
The design remains Partial because the acceptance evidence below does not yet cover every required failure mode or the performance cost.

### Invariant audit

| Requirement | Implementation evidence | Existing regression evidence and limits |
| --- | --- | --- |
| 1. Previously proposed writes are included in the sealed snapshot of each touched route. | [`beginTxnReadTimestamp`, `snapshotSealGroups`, and `tryTxnWriteTimestamp`](../../adapter/dynamodb_locks.go) fence the owning routes, dispatch each route separately, and require every response to confirm the same seal timestamp before selecting the snapshot. [`CommittedTimestampFloorForKeys`](../../kv/shard_store.go) fences only the owning groups. | `TestDynamoDBUpdateItemFencesBeforeSnapshotWatermark`, `TestDynamoDBSnapshotSealFailsWhenSecondTableFails`, and `TestDynamoDBSnapshotSealRejectsMissingCommitTimestamp` in [`dynamodb_read_fence_test.go`](../../adapter/dynamodb_read_fence_test.go); `TestShardStoreCommittedTimestampFloorForKeysSkipsUnrelatedUnavailableGroup` in [`tso_floor_test.go`](../../kv/tso_floor_test.go). The two-table seal test uses one in-memory store and a recording coordinator, not two Raft groups. |
| 2. An allocated but later-proposed write conflicts with the seal and recomputes; ambiguous retries retain exact-timestamp dedup. | [`buildItemWriteRequestWithSource` and `prepareDeleteItemWrite`](../../adapter/dynamodb_item_write.go) add the item seal to `ReadKeys`; [`buildTransactWriteItemsRequest`](../../adapter/dynamodb_transact.go) adds every affected seal. [`handleOnePhaseTxnRequest` and `handlePrepareRequest`](../../kv/fsm.go) pass `ReadKeys` into the atomic store apply. [`checkApplyConflictsWith`](../../store/lsm_store.go) checks read dependencies against `StartTS`. The existing retry and `PrevCommitTS` probe remain in [`dynamodb_item_write.go`](../../adapter/dynamodb_item_write.go) and `dedupProbeOnePhase` in [`fsm.go`](../../kv/fsm.go). | `TestDynamoDBTwoServersSealPendingItemWrite` exercises the allocated/unproposed-write schedule across two server objects with separate item locks. It shares an in-memory store and coordinator. [`dynamodb_onephase_dedup_test.go`](../../adapter/dynamodb_onephase_dedup_test.go) covers landed, not-landed, and genuinely conflicting retries; [`fsm_occ_test.go`](../../kv/fsm_occ_test.go) covers atomic one-phase and PREPARE read-key conflicts. |
| 3. Follower ingress, leadership changes, restart, and multiple groups preserve the boundary. | [`DispatchWithLeaderIssuedCommitTS`](../../kv/tso.go), [`dispatchSingleShardTxnLeaderStamped` and `leaseRefreshingTxn`](../../kv/sharded_coordinator.go), and [`commitWithLocalStamp`](../../kv/leader_proxy.go) defer the first timestamp until the verified owning leader assigns it. Subsequent table routes confirm the same timestamp. Seals are normal MVCC records in [`dynamodb_keys.go`](../../adapter/dynamodb_keys.go), with the normal Raft apply and FSM snapshot persistence path. | [`sharded_coordinator_txn_test.go`](../../kv/sharded_coordinator_txn_test.go) has follower timestamp, promotion, and demotion regressions. `TestDynamoDBSnapshotSealSurvivesLeaderSwitch` switches between mocked coordinators over one store. Generic snapshot and HLC-ceiling round trips exist in [`snapshot_test.go`](../../kv/snapshot_test.go). These do not establish a seal-specific crash/restart test, a real Raft election during the pending-write schedule, or that schedule across two data groups. |
| 4. All item entry points use schema-derived hash/range identity, including ConditionCheck. | [`itemWriteIdentity`, `lockItemForWrite`, and `transactWriteLockIndexes`](../../adapter/dynamodb_locks.go) project only the schema primary key before deriving the local lock. Batch writes use [`batchWriteRequestKey`](../../adapter/dynamodb_transact.go). The entry-point map below records every caller. | `TestDynamoDBItemLockNormalizesKeyAndHonorsCancellation`, `TestDynamoDBConditionCheckUsesItemLock`, and `TestDynamoDBAdminPutUsesItemLock` in [`dynamodb_read_fence_test.go`](../../adapter/dynamodb_read_fence_test.go) cover key normalization and shared contention. This is not a separate concurrent regression for every entry point and composite-key schema. |

### Item entry-point audit

The current routing granularity is a whole table: `dynamoRouteKey` in [`shard_key.go`](../../kv/shard_key.go) normalizes metadata, items, indexes, and seals to the same table route.
`TestRouteKey_NormalizesDynamoKeysToTable` in [`shard_key_test.go`](../../kv/shard_key_test.go) records that contract.
Multiple touched table routes can belong to different data groups; a single table split across independently owned item routes is not established by this implementation or its tests.
The multi-group requirement must therefore be validated at the supported table-route granularity without claiming arbitrary within-table sharding.

| Entry point | Local identity and sealed write path |
| --- | --- |
| HTTP PutItem | [`putItem`](../../adapter/dynamodb_item_write.go) calls `lockItemForWrite`, then `putItemWithRetry`; both retry modes obtain `sealedItemReadTimestamp` before planning. |
| HTTP UpdateItem | [`updateItem`](../../adapter/dynamodb_item_write.go) calls `lockItemForWrite`, then `updateItemWithRetry` and the same sealed retry path. |
| HTTP DeleteItem | [`deleteItem`](../../adapter/dynamodb_item_write.go) calls `lockItemForWrite`, then `deleteItemWithRetry`; a present item adds its seal to the delete request's `ReadKeys`. An absent item needs no user mutation. |
| BatchWriteItem Put/Delete | [`executeBatchWriteRequest`](../../adapter/dynamodb_transact.go) uses `batchWriteRequestLockKey` with `primaryKeyAttributes`, then delegates to `putItemWithRetry` or `deleteItemWithRetry`. |
| AdminPutItem / AdminDeleteItem | [`dynamodb_admin_items.go`](../../adapter/dynamodb_admin_items.go) calls `lockItemForWrite` and the corresponding shared sealed retry method after authorization and leader verification. |
| TransactWriteItems Put/Update/Delete/ConditionCheck | [`lockTransactWriteItems`](../../adapter/dynamodb_locks.go) derives schema primary keys, locks unique stripes in order, and derives all seals. [`buildTransactWriteItemsRequest`](../../adapter/dynamodb_transact.go) seals before planning and includes all seals in `ReadKeys`, including ConditionCheck. An existing ConditionCheck may write the same item value; an absent item remains a read dependency. |
| Legacy item migration | [`resolveLegacyMigrationTarget` and `migrateLegacyItem`](../../adapter/dynamodb_migration.go) use the target schema primary key for the local lock, seal the target item, and attach that seal to `ReadKeys`. |

Generation changes are checked after sealing by `verifySealedTableGeneration`.
The single-item retry loop derives a new seal on retry; the transaction loop calls `refreshTransactWriteLocks` to rebuild locks, generations, and seals.
`TestDynamoDBSnapshotSealDetectsTableRecreation` covers the generation mismatch.

### Persistence, cleanup, and backup boundaries

- [`dynamoItemSnapshotSealKey`](../../adapter/dynamodb_keys.go) uses two digest bytes, represented as four hexadecimal characters, per table generation. `TestDynamoDBSnapshotSealRoutesWithItem` checks the routing, generation separation, and key shape. The bound applies to distinct live seal keys, not the number of retained MVCC versions or the total disk usage.
- [`cleanupDeletedTableGeneration`](../../adapter/dynamodb_schema.go) removes the seal prefix with the deleted generation. `TestDynamoDBCleanupDeletedTableGeneration_RetriesRouteFence` in [`dynamodb_cleanup_retry_test.go`](../../adapter/dynamodb_cleanup_retry_test.go) covers cleanup retry. `TestRoutePrefixRangeDynamoSealCleanupIsExactTable` in [`shard_key_test.go`](../../kv/shard_key_test.go) prevents a neighboring table prefix from being included.
- Logical snapshot decoding drops seals as internal records in [`decode.go`](../../internal/backup/decode.go); live backup scope excludes them through `ScopeForKey` in [`live.go`](../../internal/backup/live.go). `TestDecodeSnapshot_DynamoSealDroppedAsInternal`, `TestAdapterForKeyRecognizesDynamoSnapshotSeal`, and the scoped-key cases in [`live_test.go`](../../internal/backup/live_test.go) cover those boundaries. These logical-backup tests do not prove a full Raft snapshot/restart scenario.

### Jepsen evidence

The [Jepsen CI run for #1259's final commit](https://github.com/bootjp/elastickv/actions/runs/38128904501) completed successfully at `30dc3498159773423af4029745ba7233e7ba4fdb`.
Its log records `:valid? true` for the DynamoDB append workload and successful per-type workload steps for all ten DynamoDB attribute types.
The append invocation used `--local --time-limit 5 --rate 5 --concurrency 5` against three local node endpoints.
The launch supplied `--raftDynamoMap`, which allows HTTP ingress to be redirected to the leader.

This is three-node smoke evidence, not evidence of the follower path with the HTTP leader map disabled or of an injected election.
[`elastickv-dynamodb-test`](../../jepsen/src/elastickv/dynamodb_workload.clj) disables the nemesis in local mode.
The multi-table workload exists in [`dynamodb_multi_table_workload.clj`](../../jepsen/src/elastickv/dynamodb_multi_table_workload.clj), but that existence and its unit tests are not a recorded execution of the seal failure schedule across data groups.

### Remaining acceptance work

- Run the allocated/unproposed-write regression through real Raft with follower ingress and an election, retaining the history and final item assertion.
- Repeat the delayed-write schedule with two touched data groups, and cover seal persistence through snapshot restore and process restart.
- Exercise follower ingress with the HTTP leader map disabled and leader changes in Jepsen, retaining the exact revision, topology, nemesis configuration, histories, and checker results.
- Measure the extra seal transaction under hot-item contention, digest collisions, and independent buckets. No seal-specific benchmark function or recorded contention measurement was found in the audited tracked files, and #1259 explicitly reports production throughput as unmeasured. Existing Redis benchmarks are not evidence for this requirement.

These are validation and measurement gaps. This audit found the seal protocol and all item entry-point wiring already present; it does not add a second implementation or claim that the remaining acceptance work has passed.
