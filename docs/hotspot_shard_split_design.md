# Hotspot Shard Split Design for Elastickv

## 1. Background

Elastickv already has shard boundaries, but it does not yet have the control-plane needed for safe automatic hotspot splitting.

Current implementation status (at proposal time):

- `distribution/engine.go` has per-range access counters and threshold-based `splitRange`.
- However, `RecordAccess` is not wired into real request paths.
- The route table is in-memory only and is lost on restart.
- Route updates are append-based via `UpdateRoute`, with no conflict detection, deduplication, or versioning.
- There is no data movement workflow to relocate part of a split range to another Raft group.

So practical “hotspot splitting” is currently not connected end-to-end.

## 2. Goals and Non-goals

### 2.1 Goals

1. Detect hot ranges automatically and execute range splits.
2. Move split children to other Raft groups to distribute load.
3. Preserve consistency during split (no lost writes, no MVCC/Txn breakage).
4. Support resumable job-based operations after failures.

### 2.2 Non-goals

1. Automatic Raft group creation or membership orchestration (operate within existing `--raftGroups`).
2. Automatic merge (inverse of split) in this design.
3. Deep LSM/Pebble optimization in the first phase (in-memory MVCC is the baseline first).

## 3. Requirements

### 3.1 Functional requirements

1. Collect read/write load by range.
2. Select split candidates with thresholds and hysteresis.
3. Choose split keys from observed key distribution (not midpoint-only).
4. Execute phased backfill and cutover.
5. Propagate post-split routing to all nodes.

### 3.2 Consistency requirements

1. No write loss at cutover boundaries.
2. Correct migration of internal keys including `!txn|...` and `!lst|...`.
3. Ability to reject writes delivered to the old group using stale routes.

### 3.3 Operational requirements

1. Toggle auto-split ON/OFF.
2. Provide manual split API.
3. Expose job status, failure reason, and migration throughput.

## 4. High-level Architecture

New components:

1. `Split Controller`
2. `Hotspot Detector`
3. `Route Catalog` (durable + versioned)
4. `Range Migrator`
5. `Route Watcher`

Responsibilities:

| Component | Responsibility | Placement |
|---|---|---|
| Hotspot Detector | Load aggregation and split candidate extraction | Every node + leader aggregation |
| Split Controller | Split state machine and retries | Default-group leader |
| Route Catalog | Durable route table and split jobs | Internal metadata in default group |
| Range Migrator | Backfill and delta copy | Between source/target leaders |
| Route Watcher | Route update distribution and local refresh | All nodes |

## 5. Data Model

`RouteDescriptor`:

- `route_id uint64`
- `start []byte` (inclusive)
- `end []byte` (exclusive, nil=+inf)
- `group_id uint64`
- `state` (`ACTIVE`, `WRITE_FENCED`, `MIGRATING_SOURCE`, `MIGRATING_TARGET`)
- `parent_route_id uint64` (lineage)

`CatalogSnapshot`:

- `version uint64` (global catalog generation)
- `routes []RouteDescriptor`

Versioning model note:

- The current implementation uses a single global catalog version.
- Per-route version fields are not used.
- Store-level OCC with per-record version checks is not the mechanism here; consistency is coordinated by leader-side sequencing and catalog-level compare-and-swap.

`SplitJob`:

- `job_id string`
- `source_route_id uint64`
- `target_group_id uint64`
- `split_key []byte`
- `snapshot_ts uint64`
- `fence_ts uint64`
- `phase` (`PLANNED`, `BACKFILL`, `FENCE`, `DELTA_COPY`, `CUTOVER`, `CLEANUP`, `DONE`, `ABORTED`)
- `cursor []byte` (resume cursor)
- `retry_count uint32`
- `last_error string`

`AccessWindow`:

- `route_id uint64`
- `window_start_unix_ms uint64`
- `read_ops uint64`
- `write_ops uint64`
- `p95_latency_us uint64` (future)
- `top_keys_sample` (for split-key selection)

## 6. Hotspot Detection

### 6.1 Instrumentation points

Read path:

- `kv/ShardStore.GetAt`
- `kv/ShardStore.ScanAt`

Write path:

- `kv/ShardedCoordinator.groupMutations` (increment per key)

### 6.2 Scoring

Range score:

`score = write_ops * Ww + read_ops * Wr`

Initial parameters:

- `Ww=4`, `Wr=1`
- `threshold=50_000 ops/min`
- Candidate after 3 consecutive windows over threshold
- Cooldown 10 minutes after split

### 6.3 Split-key selection

1. Default to p50 of sampled key distribution.
2. If single-key skew is high (`top_key_share >= 0.8`), isolate the hot key.
3. For `end=nil` ranges, derive split key from observed keys.

## 7. Split Execution Flow

```mermaid
flowchart LR
  A[Detect hotspot] --> B[Create SplitJob: PLANNED]
  B --> C[BACKFILL at snapshot_ts]
  C --> D[FENCE writes on moving range]
  D --> E[DELTA_COPY snapshot_ts+1..fence_ts]
  E --> F[CUTOVER route version+1]
  F --> G[CLEANUP source old copies]
  G --> H[DONE]
```

### 7.1 BACKFILL

1. Record `snapshot_ts = source.LastCommitTS()`.
2. Export MVCC versions for the moving range from source.
3. Import into target idempotently.
4. Persist `cursor` continuously for resumability.

### 7.2 FENCE

1. Mark moving range as `WRITE_FENCED`.
2. In `ShardedCoordinator.Dispatch`, reject writes in fenced range with retryable errors.
3. Use `raft.Barrier` to confirm fence visibility.

### 7.3 DELTA_COPY

1. Acquire `fence_ts = source.LastCommitTS()`.
2. Copy delta versions in `(snapshot_ts, fence_ts]`.
3. Mark cutover-ready when complete.

### 7.4 CUTOVER

1. Replace parent range with two active child ranges.
2. Switch moving child `group_id` to target.
3. Increment route catalog `version`.

### 7.5 CLEANUP

1. Keep old copies on source for a grace period (read safety).
2. Garbage-collect moved data after grace period.

## 8. Routing Consistency

### 8.1 Route version in requests

- Add `route_version` to `pb.Request` (`proto/internal.proto`).
- Coordinator stamps current route version into each request.

### 8.2 Ownership validation

- Inject `groupID` + route resolver into `kvFSM`.
- Validate ownership for each mutation key (via `routeKey`) before apply.
- Return `ErrWrongShard` on mismatch.

### 8.3 Stale-route handling

- Old-group leader rejects stale-route writes.
- Client retries after refreshing routes.

## 9. Key Coverage for Migration

Range membership must be based on logical route key, not raw storage key.

Keys to include:

1. User keys
2. List keys (`!lst|meta|...`, `!lst|itm|...`)
3. Txn keys (`!txn|lock|...`, `!txn|int|...`, `!txn|meta|...`, `!txn|cmt|...`, `!txn|rb|...`)

Required changes:

- Export route-key extraction from `kv/txn_keys.go` for migrator reuse.
- Add MVCC version export/import APIs in `store`.

## 10. API Changes

### 10.1 `proto/distribution.proto`

Add RPCs:

1. `ReportAccess(ReportAccessRequest) returns (ReportAccessResponse)`
2. `ListRoutes(ListRoutesRequest) returns (ListRoutesResponse)`
3. `WatchRoutes(WatchRoutesRequest) returns (stream WatchRoutesResponse)`
4. `SplitRange(SplitRangeRequest) returns (SplitRangeResponse)`
5. `GetSplitJob(GetSplitJobRequest) returns (GetSplitJobResponse)`

### 10.2 `proto/internal.proto`

Add RPCs:

1. `ExportRangeVersions(ExportRangeVersionsRequest) returns (stream ExportRangeVersionsResponse)`
2. `ImportRangeVersions(ImportRangeVersionsRequest) returns (ImportRangeVersionsResponse)`

## 11. Implementation Touchpoints

1. `distribution/engine.go`
- Extend route data with state/version.
- Replace append-only updates with CAS update APIs.

2. `kv/shard_store.go`
- Add read-path access recording.
- Define behavior for moving/fenced ranges.

3. `kv/sharded_coordinator.go`
- Add write-path access recording.
- Return retryable errors for `WRITE_FENCED` writes.

4. `kv/fsm.go`
- Add route-ownership checks.
- Return `ErrWrongShard` when ownership mismatches.

5. `adapter/distribution_server.go`
- Implement split/job/watch/report APIs.

6. `store/mvcc_store.go` and `store/lsm_store.go`
- Implement range version export/import.

## 12. Phased Rollout

### Milestone 1: Control plane

1. Durable route catalog
2. Route version + watcher
3. Manual split API (same-group split, no migration)

### Milestone 2: Migration plane

1. MVCC range export/import
2. Job phases: BACKFILL/FENCE/DELTA/CUTOVER
3. Manual split with target-group relocation

### Milestone 3: Automation

1. Access aggregation
2. Hotspot detector
3. Auto-split scheduler (cooldown/hysteresis)

### Milestone 4: Hardening

1. Stale-route reject path
2. Cleanup GC
3. Jepsen hotspot workloads

## 13. Test Strategy

### 13.1 Unit tests

1. Hotspot scoring and split-key selection
2. Route catalog CAS/version transitions
3. Split state-machine phase transitions
4. Ownership validation

### 13.2 Integration tests

1. Consistency under concurrent read/write during split
2. Retryable behavior during FENCE
3. Job resume after leader change/restart
4. Split with cross-shard transactions

### 13.3 Jepsen

1. Hotspot load + partition nemesis
2. Hotspot load + kill nemesis
3. Linearizability checks during split

## 14. Risks and Mitigations

1. Risk: Long write pause during split.
- Mitigation: Chunked delta copy, narrow fence to moving child only.

2. Risk: Misrouting via stale route cache.
- Mitigation: Leader-side ownership validation + route version checks.

3. Risk: Internal key omission causing unresolved transactions.
- Mitigation: Logical route-key based filtering + dedicated txn/list migration tests.

4. Risk: Cleanup too early impacting old readers.
- Mitigation: Grace period + route-version observability + staged GC.

## 15. Acceptance Criteria

1. Manual split divides one range into two and moves one child to another group.
2. No write loss during split.
3. Stale-route writes are rejected after cutover.
4. Split jobs resume after failure and eventually reach `DONE`.
5. Existing tests + new split tests + extended Jepsen scenarios pass.
