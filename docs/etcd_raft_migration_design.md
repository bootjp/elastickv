# etcd/raft Migration Design

## Status

Draft planning document. This document is intentionally decision-oriented and does not commit the project to migration.

## Objective

Evaluate whether Elastickv should migrate its consensus runtime from HashiCorp Raft to `etcd/raft`, and define a concrete migration plan if the project decides to proceed.

The immediate trigger for this discussion is linearizable read support. The current codebase already implements a repo-local read fence on top of HashiCorp Raft, so the purpose of this document is broader than "replace one API with another". The decision should be based on the full platform impact: read path correctness, scaling, operational control, testability, persistence format, and maintenance cost.

## Executive Summary

`etcd/raft` would make true `ReadIndex` support easier to implement and would give Elastickv tighter control over read-only queries, per-group scheduling, and transport/storage behavior. It also creates room for follower reads, more deterministic simulation tests, and a more explicit multi-Raft runtime.

However, moving from HashiCorp Raft to `etcd/raft` is not a library swap. It is a substantial runtime rewrite. The current codebase depends on HashiCorp Raft for:

1. process-local Raft runtime construction
2. proposal submission and future handling
3. leader verification and leader discovery
4. membership management and leadership transfer
5. snapshot store integration
6. metrics and observer hooks
7. admin and health RPCs built around the current runtime
8. test helpers that directly instantiate `*raft.Raft`

Because of that, migration is justified only if Elastickv wants the broader benefits of owning a lower-level Raft runtime, not just native `ReadIndex`.

## Current State

### Runtime construction

The server and tests instantiate HashiCorp Raft directly:

1. `main.go` builds one Raft runtime per shard group via `newRaftGroup`.
2. `multiraft_runtime.go` wires `raft.DefaultConfig`, the Pebble-backed log/stable store, the snapshot store, and the gRPC transport manager.
3. `adapter/test_util.go` and several integration tests build in-memory `*raft.Raft` nodes directly.

### Write path coupling

The transactional path is built around HashiCorp Raft proposal futures:

1. `kv/transaction.go` calls `raft.Apply(...)` and inspects `ApplyFuture`.
2. `kv/leader_proxy.go` uses `State()`, `LeaderWithID()`, and leader verification before local commit.
3. Commit indexes are surfaced to APIs and tests through the current runtime.

### Read path coupling

The current linearizable read path is a repo-local fence built on HashiCorp Raft:

1. `kv/raft_leader.go` wraps `VerifyLeader`.
2. `kv/read_barrier.go` uses `VerifyLeader`, `CommitIndex()`, `State()`, and bootstrap polling.
3. `kv/coordinator.go` and `kv/sharded_coordinator.go` expose leader verification APIs.
4. `kv/leader_routed_store.go` decides whether to read locally or proxy to the leader.

### Snapshot and FSM integration

The FSM and snapshot flow are expressed in HashiCorp Raft interfaces:

1. `kv/fsm.go` implements `raft.FSM`.
2. `kv/snapshot.go` implements `raft.FSMSnapshot`.
3. `multiraft_runtime.go` uses `raft.NewFileSnapshotStore(...)`.

### Persistence and migration

Durable Raft state is already partly abstracted, but only for HashiCorp Raft contracts:

1. `internal/raftstore/pebble.go` implements `raft.LogStore` and `raft.StableStore`.
2. `internal/raftstore/migrate.go` migrates legacy BoltDB state into the current HashiCorp-compatible Pebble format.

This helps with local durability, but it does not make the runtime portable. `etcd/raft` uses different storage primitives (`HardState`, `Entry`, `Snapshot`, `ConfState`) and would need a new storage layer or an adapter.

### Observability and operations

Operational tooling is also built around HashiCorp Raft:

1. `monitoring/raft.go` polls `Stats()`, `LeaderWithID()`, `GetConfiguration()`, and observer events.
2. `main.go` and test setup register `leaderhealth` and `raftadmin`.
3. `scripts/rolling-update.sh` depends on `raftadmin` for leadership transfer and membership inspection.

## Why Consider Migration

### 1. Native linearizable reads

The strongest technical reason is that `etcd/raft` exposes `ReadIndex` as a first-class protocol instead of requiring a repo-local approximation. This gives a cleaner implementation of linearizable reads and a better base for follower-served reads later.

### 2. Better fit for many-Raft-group scheduling

Elastickv is moving toward a stronger multi-Raft shape. HashiCorp Raft is convenient per group, but it hides the scheduler and transport internals inside each instance. `etcd/raft` gives explicit control over:

1. ticking
2. batching
3. outbound message flushing
4. disk persistence timing
5. backpressure

That control matters if the number of shard groups per process grows.

### 3. Clearer transport and persistence ownership

`etcd/raft` forces the application to own the event loop, transport, and stable persistence behavior. That is more work, but it also means Elastickv can make those choices explicitly instead of adapting to library behavior.

This could help in areas such as:

1. batching outbound replication messages across groups
2. aligning disk sync behavior with Pebble usage
3. applying group-level flow control
4. tuning snapshot send/receive behavior for large state

### 4. Better future path for follower reads

Today the read path is mostly leader-local or leader-proxied. If Elastickv wants follower-served linearizable reads later, `ReadIndex` is the standard foundation.

### 5. More deterministic testing

`etcd/raft` is easier to drive in simulation because the application owns time progression and message delivery. That improves fault-injection and deterministic unit tests for election, reconfiguration, and read-index behavior.

## Benefits Beyond ReadIndex

The migration is only worth considering if these benefits matter.

### Potential benefits

| Area | Potential benefit to Elastickv |
| --- | --- |
| Linearizable reads | Native `ReadIndex` protocol, cleaner than the current verify-plus-wait fence |
| Follower reads | A realistic path to serving linearizable reads from followers after local apply catches up |
| Multi-Raft scaling | One explicit reactor model can schedule many groups more efficiently than many opaque runtimes |
| Batching control | More control over disk flush cadence and network message batching |
| Transport flexibility | Easier to swap or specialize replication transport without depending on HashiCorp-specific adapters |
| Deterministic tests | Easier in-memory simulations for elections, partitions, and lagging followers |
| Operational introspection | The application can expose precisely the status it needs instead of reverse-parsing `Stats()` |
| Future protocol work | Easier to experiment with read-only modes, admission control, and scheduler policies |

### Important nuance

These are platform benefits, not free wins. `etcd/raft` exposes control, but Elastickv must then build and maintain the machinery needed to use that control safely.

### What migration does not solve by itself

Moving to `etcd/raft` does not automatically improve:

1. cross-shard transaction semantics
2. MVCC or HLC correctness
3. snapshot size or compaction cost
4. adapter-level retry and proxy behavior
5. operational tooling availability

Those areas still require separate application work.

## Costs and Drawbacks

### 1. Major rewrite cost

Migration requires replacing core runtime machinery, not only the read path.

### 2. Higher maintenance burden

HashiCorp Raft currently owns a large amount of correctness-sensitive behavior:

1. internal replication flow
2. snapshot handling contracts
3. transport integration assumptions
4. configuration-change sequencing
5. failure handling around leadership changes

After migration, Elastickv owns more of that surface directly.

### 3. Operational feature regression risk

Current operational workflows depend on:

1. `raftadmin`
2. `leaderhealth`
3. `GetConfiguration`
4. `LeadershipTransferToServer`
5. the current monitoring based on `Stats()` and observer hooks

All of those would need equivalents before migration is production-safe.

### 4. Data migration complexity

The current durable Raft data format is HashiCorp-specific. A direct wire-format conversion to `etcd/raft` is possible in theory but expensive and risky.

### 5. No mixed-cluster compatibility

There is no realistic expectation that a single Raft group can run some nodes on HashiCorp Raft and others on `etcd/raft`. This strongly pushes rollout toward cold cutover, blue/green cutover, or group-by-group rebuild from logical snapshot.

### 6. Long path to feature parity

The current read fence already solves the immediate correctness gap. That means migration competes with other roadmap work and must justify itself against simpler incremental improvements.

## Alternatives

### Alternative A: Stay on HashiCorp Raft and keep the current read fence

Pros:

1. Lowest risk
2. No storage or operational migration
3. Already implemented

Cons:

1. No true `ReadIndex` API
2. Harder path to follower reads
3. Multi-Raft scheduling remains opaque per runtime

### Alternative B: Fork or extend HashiCorp Raft with a `ReadIndex`-style API

Pros:

1. Lower migration cost than a full engine replacement
2. Keeps current transport, admin, and storage integration

Cons:

1. Still becomes a long-lived fork
2. Does not unlock the broader scheduler and runtime-control benefits of `etcd/raft`

### Alternative C: Introduce a Raft engine abstraction first, then decide

Pros:

1. Reduces lock-in
2. Creates a measurable prototype stage
3. Makes the no-migration path useful too

Cons:

1. Upfront abstraction work
2. Some duplication while two engines coexist

This document recommends Alternative C as the entry point, even if the project eventually decides not to migrate.

## Recommendation

Do not commit immediately to full migration.

Instead:

1. build a narrow Raft engine abstraction around the surfaces Elastickv actually needs
2. implement a single-group `etcd/raft` prototype behind that abstraction
3. measure read latency, write throughput, goroutine count, memory cost, and operational parity
4. only then decide whether to continue to production rollout

This keeps the project from paying the full migration cost before the platform benefits are proven.

## Proposed Target Architecture

Introduce an internal engine boundary, for example `internal/raftengine`, with two implementations:

1. `hashicorp`
2. `etcd`

The boundary should be shaped around Elastickv behavior, not around one library's API.

### Engine responsibilities

The engine layer should own:

1. proposal submission
2. commit index reporting
3. leader identity and local leadership checks
4. linearizable read fence or ReadIndex
5. membership changes
6. leadership transfer
7. snapshot trigger and snapshot restore plumbing
8. transport start/stop
9. durable state open/close
10. metrics/status export
11. logical clock progression (ticking)

### Callers that should stop depending on concrete HashiCorp Raft types

1. `main.go`
2. `multiraft_runtime.go`
3. `kv/coordinator.go`
4. `kv/leader_proxy.go`
5. `kv/read_barrier.go`
6. `kv/raft_leader.go`
7. `monitoring/raft.go`
8. adapter test helpers that create clusters directly

## Migration Plan

## Phase 0: Decision Gate and Abstraction

### Goal

Create a minimal engine boundary and move the application off direct `*raft.Raft` usage where possible.

### Main tasks

1. Define an internal engine interface for:
   - propose/apply
   - linearizable read
   - leader lookup
   - local state
   - config changes
   - leadership transfer
   - snapshot coordination
   - metrics/status
2. Refactor callers to depend on this boundary instead of `*raft.Raft`.
3. Keep the existing HashiCorp implementation as the default engine.
4. Add conformance tests shared across engine implementations.

### Exit criteria

1. The main server path can boot from the abstraction with no behavior change.
2. Existing tests still pass on the HashiCorp implementation.
3. The project can implement an `etcd/raft` prototype without changing adapters again.

## Phase 1: Single-Group etcd/raft Prototype

### Goal

Prove that a single Raft group can run correctly with `etcd/raft` behind the new abstraction.

### Main tasks

1. Implement an in-process reactor loop:
   - tick progression
   - proposal intake
   - `Ready` handling
   - persistence
   - outbound message send
   - committed-entry apply
2. Implement leader tracking and linearizable reads using `ReadIndex`.
3. Keep the current KV FSM command encoding so higher layers remain unchanged.
4. Provide a basic gRPC replication transport for Raft messages.
5. Support bootstrap and restart for one group.

### Exit criteria

1. Single-group KV tests pass.
2. Linearizable read tests pass through native `ReadIndex`.
3. The engine can restart from disk and preserve state.

## Phase 2: Durable Storage and Snapshot Format

### Goal

Define how `etcd/raft` state is persisted and migrated safely.

### Main tasks

1. Choose the durable storage model:
   - reuse Pebble with a new `etcd/raft` storage adapter
   - or adopt an etcd-style WAL/snapshot split persisted on local disk
2. Define how committed entries, hard state, and snapshots are stored.
3. Define snapshot send/receive plumbing compatible with the existing KV FSM snapshot format.
4. Build a migration tool.

### Recommended migration strategy

Start with logical-state migration, not Raft-log translation.

Recommended first cut:

1. stop a cluster or fail traffic over to a new cluster
2. export a logical FSM snapshot plus membership configuration
3. bootstrap a new `etcd/raft` group from that snapshot
4. verify data and cut traffic over

This is safer than attempting to convert HashiCorp log/stable state into `etcd/raft` durable state in place.

### Exit criteria

1. Crash recovery is deterministic.
2. Snapshot restore works on realistic datasets.
3. A migration tool exists and is tested.

## Phase 3: Operations and Admin Parity

### Goal

Replace the operational capabilities currently provided by HashiCorp-specific tooling.

### Main tasks

1. Replace `raftadmin`-based workflows with a native admin service:
   - leader query
   - configuration query
   - add/remove voter
   - leadership transfer
2. Replace `leaderhealth` integration with an engine-native health view.
3. Rebuild Prometheus metrics using engine status and explicit counters.
4. Update `scripts/rolling-update.sh` and manual runbooks.

### Exit criteria

1. Operational commands used today still exist.
2. Dashboards and alerts remain meaningful.
3. Rolling update documentation is rewritten for the new engine.

## Phase 4: Multi-Raft Integration

### Goal

Run multiple shard groups per process under the new engine.

### Main tasks

1. Decide whether each group owns its own reactor or all groups share a scheduler.
2. Implement fairness and backpressure across groups.
3. Measure CPU, memory, goroutine count, and tail latency as group count increases.
4. Validate shard-aware read and write paths, including coordinator routing.

### Exit criteria

1. Multi-group tests pass.
2. Resource usage is not worse than the current engine without a clear benefit.
3. The scheduler behavior is observable and debuggable.

## Phase 5: Rollout

### Goal

Ship migration safely with an explicit rollback plan.

### Main tasks

1. Add a runtime flag such as `--raft-engine=hashicorp|etcd`.
2. Gate new deployments behind the flag first.
3. Run shadow benchmarks and failure tests.
4. Run Jepsen and restart/recovery suites on the new engine.
5. Decide whether migration is:
   - opt-in for new clusters only
   - or a supported migration path for existing clusters

### Exit criteria

1. Performance and correctness targets are met.
2. Operational playbooks are complete.
3. Rollback is tested.

## Benefits and Drawbacks Summary

| Dimension | HashiCorp Raft today | etcd/raft after migration |
| --- | --- | --- |
| ReadIndex | Not exposed directly | Native protocol support |
| Follower linearizable reads | Harder | Natural extension |
| Runtime abstraction | Higher-level, convenient | Lower-level, more explicit |
| Per-group setup | Simple | More custom code |
| Multi-group scheduling control | Limited | Strong |
| Transport/admin ecosystem | Existing in repo | Must be rebuilt |
| Storage format continuity | Existing | New format and migration work |
| Short-term delivery cost | Low | High |
| Long-term control | Moderate | High |
| Correctness ownership | More library-owned | More application-owned |

## Risks

1. The migration may consume substantial engineering time without enough runtime benefit in current workloads.
2. Rebuilding admin and operational tooling may delay production readiness more than the consensus core itself.
3. A custom transport or persistence bug could create correctness issues in the most sensitive part of the system.
4. Mixed-engine rolling upgrades are likely infeasible, which increases migration coordination cost.
5. The current read fence may already satisfy practical needs, weakening the case for migration.

## Open Questions

1. What shard-group density per process do we expect over the next two milestones?
2. Is follower-served linearizable read actually a product requirement or only a technical preference?
3. Would a HashiCorp Raft fork with `ReadIndex` cover enough of the need at lower cost?
4. Do we want to preserve existing on-disk FSM snapshots as-is during migration, or redefine snapshot packaging at the same time?
5. Is migration only for new clusters acceptable, or must existing clusters be migratable in place?

## Decision Criteria

Proceed beyond Phase 0 only if at least one of the following is true:

1. native `ReadIndex` is required for an important product capability, such as follower-served reads
2. multi-Raft scaling with the current engine becomes a measurable bottleneck
3. operational or testing benefits from explicit runtime ownership materially improve development velocity

Otherwise, the recommended path is to keep HashiCorp Raft and continue iterating on the current read fence and surrounding abstractions.
