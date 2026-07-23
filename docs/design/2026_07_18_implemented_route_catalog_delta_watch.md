# Versioned route catalog delta and streaming watch

Status: Implemented
Author: bootjp
Date: 2026-07-18

## 1. Scope

This design replaces full-catalog refresh as the steady-state propagation path
for route catalog changes. It owns:

- a durable, versioned route delta log;
- bounded retention with full-snapshot reset for stale cursors;
- atomic publication into each node's in-memory route engine;
- capability negotiation and a server-streaming gRPC watch;
- reconnect from the last published version;
- rolling-upgrade fallback to the existing full-snapshot poll.

The route index and batched catalog mutation designs remain separate. This
change preserves the existing one-version-per-catalog-commit contract and the
route engine's slice representation.

## 2. Durable representation

Each catalog transition from version `v` to `v+1` writes all of the following
in the same MVCC transaction:

- changed route rows;
- deleted route rows;
- `!dist|delta|<big-endian uint64 version>` containing the ordered route
  mutations;
- `!dist|meta|delta_floor`, the oldest retained delta version;
- `!dist|meta|version`, the new catalog version.

A delta records exactly one contiguous transition:

```text
CatalogDelta {
  previous_version: v
  version:          v + 1
  mutations:        UPSERT(RouteDescriptor) | DELETE(route_id)
}
```

Mutation route IDs are unique within a delta. Upserts carry a validated route
whose ID matches the mutation ID. Deletes carry no route payload. The codec is
versioned and rejects trailing bytes, oversized lengths, unknown operations,
zero route IDs, and non-contiguous versions.

`CatalogStore.Save` derives the delta from the old and desired route sets.
`DistributionServer.SplitRange` constructs the equivalent parent delete and
two child upserts and includes them in the coordinator transaction that commits
the route rows and catalog version. A successful catalog version therefore
cannot be visible without its matching delta.

## 3. Retention and reconnect

The default retention window is 1,024 catalog transitions. Delta version keys
sort by version and the floor advances as old entries are deleted in the same
catalog commit.

`CatalogStore.ChangesSince(after_version, limit)` reads the version, floor,
and deltas at one MVCC timestamp. It returns one of:

- no change when `after_version` equals the durable version;
- a bounded contiguous delta batch;
- a full `CatalogSnapshot` reset when the requested next version predates the
  retained floor or the catalog predates delta logging.

A cursor ahead of the durable version is rejected. A missing or malformed
delta inside the advertised retained range is an integrity error, not a silent
snapshot fallback.

## 4. Atomic mirror publication

`Engine.ApplyDelta` builds the complete next route table while holding the
engine write lock, validates route order and overlap, and only then replaces the
published slice and version. Readers observe either the complete previous
version or the complete next version. A gap, malformed mutation, or invalid
route leaves both the route table and version unchanged.

The first durable delta removes the process bootstrap route with ID zero.
Upserts preserve the current in-memory load counter for an existing route.
Successful publication records the same immutable history snapshot used by the
cross-version read gate.

The local `CatalogWatcher` consumes bounded delta batches. Retention reset uses
the existing atomic `ApplySnapshot` path. Stale work is harmless when the local
poller and remote stream race because both publication paths reject version
regression.

## 5. Streaming protocol

`proto.Distribution` exposes:

- `GetCatalogCapabilities`, which advertises supported watch protocol versions,
  the current version, oldest retained delta, and maximum batch size;
- `WatchCatalog`, which accepts a protocol version, reconnect cursor, and batch
  limit and streams either `CatalogDeltaRecord` or `CatalogSnapshotReset`.

Protocol version 1 requires contiguous one-version deltas. The server caps a
client batch request at 1,024 and uses 128 when the client omits a limit.
Unsupported protocols fail with `FailedPrecondition`; future cursors fail with
`InvalidArgument`.

Production resolves the current catalog-group leader before each stream
attempt. The leader closes the stream with `Unavailable` after it loses
leadership, causing the client to resolve the new leader and reconnect from the
last version atomically published in its engine.

The existing 100 ms local durable watcher remains active while the stream is
connected and while it is unavailable. It bounds staleness during elections,
network failure, and mixed-version rollout. A server without capability or
watch support causes the client to use `ListRoutes` polling and negotiate again
on the next attempt, so it can adopt streaming after the rolling upgrade
finishes.

## 6. Failure and rollout behavior

- Stream disconnect loses no catalog transition because reconnect uses the
  mirror's published version and deltas are durable.
- Falling behind retention produces an explicit snapshot reset.
- Corruption or a hole within retention fails closed and is surfaced for
  operator investigation.
- A follower does not retain leader-targeted watch streams.
- Old clients continue using `ListRoutes`; old servers return `Unimplemented`
  for the new RPCs and new clients retain full-snapshot polling.
- The stream is an optimization. Route correctness continues to depend on the
  durable catalog-group state and monotonic engine publication.

## 7. Verification

Focused package tests:

```sh
go test ./distribution -run 'Test(CatalogDelta|CatalogStore|EngineApplyDelta|CatalogWatcherAppliesBounded)' -count=1
go test ./adapter -run 'Test(DistributionServerCatalogWatch|GRPCCatalogWatcher|CatalogStoreMutationsToOps)' -count=1
```

The focused coverage includes:

- codec round trip and invalid record rejection;
- contiguous `CatalogStore.Save` deltas;
- retention deletion and snapshot reset;
- missing retained-delta integrity failure;
- engine gap and overlap rejection without partial publication;
- bounded local watcher batches;
- split transaction delta visibility;
- protocol negotiation, reconnect cursor, and snapshot stream events;
- disconnect followed by endpoint re-resolution;
- follower stream rejection;
- old-server full-snapshot fallback.

Repository gates:

```sh
go test ./...
go test -race ./distribution ./adapter
GOCACHE=$(pwd)/.cache GOLANGCI_LINT_CACHE=$(pwd)/.golangci-cache golangci-lint run ./... --timeout=5m
git diff --check
```
