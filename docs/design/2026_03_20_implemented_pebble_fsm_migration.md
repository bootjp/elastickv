# Pebble FSM Migration Plan

## Objective

Replace the current in-memory FSM store with a Pebble-backed `MVCCStore` that:

1. Preserves FSM state across process restarts.
2. Replays Raft logs safely without duplicate-apply anomalies.
3. Scales snapshot and restore behavior to large datasets.
4. Keeps read/write semantics identical to the current MVCC store.

## Current State

1. Raft log and Raft stable state are already persisted in Pebble under `raft.db`.
2. The FSM used by `main.go` and `cmd/server/demo.go` is still `store.NewMVCCStore()`, which is in-memory.
3. `store.NewPebbleStore()` exists and passes basic MVCC and snapshot/restore tests.
4. `pebbleStore.Snapshot()` currently serializes the full store into a `bytes.Buffer`, which is not safe for large state.
5. `pebbleStore.Compact()` is still a no-op, so MVCC versions can grow without bound.
6. `pebbleStore` persists `LastCommitTS`, but it does not yet track Raft recovery metadata such as the last applied log index.

## Non-Goals

1. Changing client-visible MVCC semantics.
2. Introducing a new storage engine other than Pebble.
3. Solving cross-group shard migration as part of this work.
4. Replacing the existing Raft log store, stable store, or snapshot store.

## Success Criteria

1. A node restart does not require rebuilding the FSM from the entire retained Raft log in normal cases.
2. Replaying already-applied Raft entries cannot produce false write conflicts or duplicate logical effects.
3. Snapshot creation and restore do not require loading the full FSM into memory at once.
4. Long-running clusters have a defined MVCC garbage-collection path.
5. `main.go`, multi-Raft runtime, and demo mode can all run with the Pebble-backed FSM.

## Implementation Principles

1. Raft remains the source of truth for ordering and safety.
2. The persisted FSM must be recoverable only through Raft-approved state transitions or Raft snapshots.
3. Startup and restore logic must be explicit about whether the local FSM is reused, rebuilt, or replaced.
4. Snapshot behavior must be streaming-oriented and bounded in memory.

## Work Breakdown

## PR1: Recovery Model and FSM Metadata

### Goal

Define and implement the recovery contract between the persisted FSM and Raft replay.

### Main files

1. `store/store.go`
2. `store/lsm_store.go`
3. `kv/fsm.go`
4. `multiraft_runtime.go`
5. `main.go`
6. `cmd/server/demo.go`

### Tasks

1. Define recovery invariants for a persisted FSM:
   - what metadata must be stored locally
   - when local FSM state may be trusted
   - when a full restore from Raft snapshot is required
2. Add persistent FSM metadata, at minimum:
   - last applied Raft log index
   - last applied Raft log term if needed
   - snapshot metadata needed to validate local state reuse
3. Decide the startup path for each case:
   - empty FSM directory
   - clean restart with matching metadata
   - stale or incompatible local FSM state
   - snapshot restore followed by log replay
4. Make `Apply`/replay idempotence explicit for the persisted FSM path.
5. Add tests for:
   - clean restart without data loss
   - restart after partial progress
   - replay of already-persisted entries
   - mismatch between local FSM metadata and Raft state

### Done criteria

1. Recovery behavior is documented and deterministic.
2. Local FSM reuse cannot silently diverge from Raft state.

## PR2: Streaming Snapshot and Restore

### Goal

Remove full-buffer snapshot behavior and make FSM snapshot/restore memory-bounded.

### Main files

1. `store/store.go`
2. `store/lsm_store.go`
3. `kv/fsm.go`
4. `kv/snapshot.go`
5. Related tests in `store/` and `kv/`

### Tasks

1. Replace the current `bytes.Buffer` snapshot implementation with a streaming design.
2. Revisit the `MVCCStore` snapshot interface if needed so that stores can write directly to a sink or stream reader/writer pair.
3. Implement Pebble snapshot iteration with bounded buffers and chunked writes.
4. Make restore stream entries incrementally instead of assuming the whole payload is resident in memory.
5. Add integrity checks for streamed snapshots:
   - version header
   - checksum or digest
   - metadata validation
6. Add tests for:
   - large snapshot generation
   - snapshot round-trip after restart
   - truncated or corrupted snapshot input

### Done criteria

1. Snapshot memory use is proportional to chunk size, not database size.
2. Restore can handle large datasets without full in-memory buffering.

## PR3: MVCC Compaction and Retention

### Goal

Implement a real garbage-collection strategy for old MVCC versions in Pebble-backed FSM state.

### Main files

1. `store/lsm_store.go`
2. `store/lsm_store_test.go`
3. `store/mvcc_store_compact_test.go`
4. Runtime wiring where compaction is scheduled or triggered

### Tasks

1. Define the retention rule for historical versions:
   - preserve versions still needed for snapshot visibility
   - preserve versions needed for transactional correctness
   - remove versions older than the safe watermark
2. Implement `Compact(ctx, minTS)` for the Pebble store.
3. Ensure compaction never removes the newest version at or before the retention watermark for any key.
4. Define when compaction runs:
   - background loop
   - operator-triggered path
   - snapshot-driven trigger
5. Add tests for:
   - correctness after compaction
   - tombstone handling
   - TTL interaction
   - transactional metadata keys

### Done criteria

1. Old MVCC versions no longer grow without bound.
2. Compaction preserves read correctness and transaction semantics.

## PR4: Wire Pebble FSM into Server Startup

### Goal

Make the Pebble-backed FSM selectable and then default for real deployments.

### Main files

1. `main.go`
2. `multiraft_runtime.go`
3. `cmd/server/demo.go`
4. Config parsing and deployment scripts as needed

### Tasks

1. Add configuration for the FSM data directory and migration behavior.
2. Instantiate `store.NewPebbleStore()` in the production startup path.
3. Instantiate the same path in demo mode so behavior matches production.
4. Keep an explicit fallback or feature flag during rollout.
5. Ensure lifecycle management is correct:
   - close the store on shutdown
   - create directories explicitly
   - surface startup errors clearly
6. Document the on-disk layout for:
   - Raft log store
   - Raft snapshots
   - Pebble FSM state

### Done criteria

1. All server entrypoints can run with the Pebble-backed FSM.
2. Rollout does not require source edits per environment.

## PR5: Migration, Validation, and Operations

### Goal

Provide a safe rollout path and operational guidance for existing clusters.

### Main files

1. `docs/redis-proxy-deployment.md`
2. `docs/docker_multinode_manual_run.md`
3. Jepsen or integration test areas affected by restart/recovery behavior
4. Deployment scripts if migration support is automated

### Tasks

1. Define the migration strategy for clusters currently using the in-memory FSM:
   - cold restart from Raft snapshot/log only
   - rolling restart expectations
   - failure handling if one node rebuilds slowly
2. Add restart and recovery integration tests for:
   - leader restart
   - follower restart
   - large backlog replay
   - snapshot restore on a lagging node
3. Add operational metrics and logging for:
   - FSM open time
   - snapshot duration and size
   - restore duration
   - compaction duration
   - replay progress
4. Add runbooks for:
   - clearing incompatible FSM state
   - forcing rebuild from Raft
   - diagnosing slow restore or replay
5. Run Jepsen and sustained-load tests focused on restart and recovery.

### Done criteria

1. Operators have a documented recovery and migration path.
2. Restart behavior has test coverage under realistic load.

## Open Design Questions

1. Should the FSM store the last applied log index internally, or should that metadata live beside the Pebble DB in a separate manifest?
2. Do we want a one-time rebuild from Raft on every restart at first, before enabling local FSM reuse?
3. Should the Pebble FSM and Raft log store stay in separate directories, or should their layout be unified under one group-specific root?
4. Is the existing `MVCCStore` snapshot interface sufficient, or should it be redesigned around streaming primitives?
5. How should compaction coordinate with active snapshots and transactional lock records?

## Validation

```bash
GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./store/... ./kv/...
GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./...
GOCACHE=$(pwd)/.cache GOLANGCI_LINT_CACHE=$(pwd)/.golangci-cache golangci-lint run ./... --timeout=5m
```
