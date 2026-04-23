# Hotspot Split Milestone 1 PR Plan

## Objective

Milestone 1 delivers the control plane only:

1. Durable route catalog.
2. Route versioning and watcher-based refresh.
3. Manual split API (same-group split, no cross-group migration).

## Out of Scope

1. Data migration to a different Raft group.
2. Automatic hotspot detection and auto-split scheduling.
3. Stale-route write rejection in FSM (`ErrWrongShard`) and route-version stamping on every internal request.

## Implementation Strategy

Use the existing default Raft group as the source of truth for route metadata.

1. Persist route catalog entries as reserved internal keys in replicated MVCC state.
2. Keep `distribution.Engine` as the in-memory read path used by routers/coordinators.
3. Run a route watcher that refreshes `Engine` from durable catalog version changes.

This keeps rollout small and avoids introducing a new external metadata service.

## PR Breakdown

## PR1: Route Catalog Model and Persistence

### Goal

Introduce a durable route catalog format and storage operations.

### Main files

1. `distribution/catalog.go` (new)
2. `distribution/catalog_test.go` (new)
3. `kv/` metadata helper file for reserved distribution keys (new)
4. `store/` tests if key encoding helpers are shared there

### Tasks

1. Define `RouteDescriptor` and `CatalogSnapshot` structs for durable representation.
2. Define reserved key prefixes, for example:
   - `!dist|meta|version`
   - `!dist|route|<route_id>`
3. Implement encoding/decoding helpers (stable binary or protobuf-based).
4. Implement catalog read/write helpers with optimistic version checks.
5. Add unit tests for:
   - encode/decode round-trip
   - version mismatch behavior
   - deterministic ordering in snapshots

### Done criteria

1. Catalog structs and key format are stable and documented.
2. Versioned read/write helpers are covered by tests.

### Validation

```bash
GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./distribution/... ./kv/...
```

## PR2: Engine Refactor for Versioned Snapshot Apply

### Goal

Make `distribution.Engine` apply a full route snapshot atomically with catalog version.

### Main files

1. `distribution/engine.go`
2. `distribution/engine_test.go`

### Tasks

1. Add engine metadata:
   - current route catalog version
   - route IDs and states needed for control-plane operations
2. Add APIs:
   - `ApplySnapshot(snapshot CatalogSnapshot) error`
   - `Version() uint64`
3. Keep existing `GetRoute` and scan behavior unchanged for data path callers.
4. Add tests:
   - snapshot apply replaces old routes atomically
   - old version snapshot is rejected
   - lookup behavior remains correct after snapshot apply

### Done criteria

1. Engine can be fully refreshed from durable catalog.
2. Existing routing tests remain green.

### Validation

```bash
GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./distribution/...
```

## PR3: Distribution Admin Service (ListRoutes and SplitRange)

### Goal

Add manual control-plane APIs for route introspection and same-group split.

### Main files

1. `proto/distribution.proto`
2. Generated files:
   - `proto/distribution.pb.go`
   - `proto/distribution_grpc.pb.go`
3. `adapter/distribution_server.go`
4. `adapter/distribution_server_test.go`

### Tasks

1. Add RPCs:
   - `ListRoutes`
   - `SplitRange`
2. Add message fields needed for route version and route state.
3. Implement `ListRoutes` by reading the durable catalog.
4. Implement `SplitRange` for same-group split only:
   - validate range ownership and boundaries
   - create two child ranges in same `group_id`
   - bump catalog version atomically
5. Return clear validation errors for:
   - unknown route
   - invalid split key
   - split key at boundaries
   - version conflict

### Done criteria

1. Manual split updates durable catalog atomically.
2. APIs are covered with success and failure-path tests.

### Validation

```bash
cd proto && make gen
cd ..
GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./adapter/... ./proto/...
```

## PR4: Route Watcher and Runtime Refresh

### Goal

Make every node refresh in-memory routes when durable catalog version changes.

### Main files

1. `distribution/watcher.go` (new)
2. `distribution/watcher_test.go` (new)
3. `main.go`
4. `cmd/server/demo.go` if demo mode should support this path

### Tasks

1. Add a watcher loop with periodic poll (initially simple and robust):
   - read durable catalog version
   - if version changed, fetch full snapshot
   - call `engine.ApplySnapshot(...)`
2. Add lifecycle hooks in server startup and graceful stop.
3. Add tests:
   - watcher applies new version
   - no-op when version unchanged
   - retry on transient read error

### Done criteria

1. Route updates made by one leader become visible in all nodes’ engines.
2. Watcher failure does not crash serving path; it retries.

### Validation

```bash
GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./distribution/... ./...
```

## PR5: End-to-End Milestone 1 Tests and Docs

### Goal

Add integration tests for manual split and update operator docs.

### Main files

1. `kv/sharded_integration_test.go`
2. `adapter/grpc_test.go` and/or dedicated split integration tests
3. `README.md`
4. `docs/hotspot_shard_split_design.md`

### Tasks

1. Add integration scenario:
   - start with one range
   - write keys on both sides of planned split point
   - call `SplitRange`
   - verify routing + reads/writes continue correctly after refresh
2. Add restart scenario:
   - apply split
   - restart node(s)
   - confirm routes reload from durable catalog
3. Document manual split API request/response examples.

### Done criteria

1. Milestone 1 behavior is reproducible via tests and docs.
2. Restart does not lose route table changes.

### Validation

```bash
GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./...
GOCACHE=$(pwd)/.cache GOLANGCI_LINT_CACHE=$(pwd)/.golangci-cache golangci-lint run ./... --timeout=5m
```

## Merge Order

1. PR1
2. PR2
3. PR3
4. PR4
5. PR5

Each PR should be independently testable and mergeable.

## Operational Rollout Checklist (Milestone 1)

1. Deploy binaries with watcher enabled but manual split unused.
2. Validate `ListRoutes` output on staging.
3. Execute one manual same-group split in staging.
4. Confirm all nodes converge to the same route catalog version.
5. Restart one node and verify route version is preserved.

## Exit Criteria for Milestone 1

1. Route catalog is durable across restart.
2. Manual split API works for same-group split.
3. Route changes propagate to all nodes via watcher refresh.
4. Full test and lint pass in CI.
