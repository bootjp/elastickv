# Elastickv Architecture Overview

Elastickv exposes multiple protocol APIs over one sharded, replicated KV engine.
Start with the [high-level diagram in the README](../README.md#architecture) for the request flow, or the [Getting started guide](getting_started.md) to run a node.
The diagrams below expand that view into runtime components and cluster topology.

## Architectural layers

| Layer | Responsibility |
| --- | --- |
| Protocol adapters | Translate Redis, DynamoDB, S3, SQS, gRPC, and filesystem requests into KV operations. Each API has its own compatibility scope. |
| Routing and transactions | Resolve key ranges to Raft groups and coordinate transactional operations over those groups. |
| Raft replication | Order and replicate each group's changes through its leader and voting replicas. A group needs a majority of voters to commit. |
| MVCC storage | Apply committed changes to versioned local data, persisted in Pebble by the normal server. Each replica owns its local storage. |
| Route catalog | Persist range ownership in the default Raft group and refresh each node's routing cache. |

A **shard range** maps a portion of the keyspace to a **Raft group**.
A **node** is a server process that can host replicas for several groups; those groups can have different leaders.
Replicas of the same group contain replicated data, while different data groups serve their assigned ranges.
The single-node quick start runs one voting replica per configured group. To tolerate a host failure, place a three-voter group across independent hosts; the in-process demo does not provide those failure domains.
The shared engine does not imply cross-API record interoperability or a transaction spanning different protocol requests.

## 1. Component Diagram

```mermaid
flowchart TB
  subgraph Clients["Clients"]
    RC["Redis Client"]
    DC["DynamoDB Client"]
    GC["gRPC RawKV/Transactional Client"]
    OC["Operator Client (grpcurl/SDK)"]
    SC["S3 Client (AWS CLI / SDKs / rclone)"]
    SQSC["SQS Client (AWS CLI / SDKs)"]
    FC["FuseFS Client (POSIX Apps / Kernel VFS)"]
    RP["redis-proxy (cmd/redis-proxy)"]
  end

  subgraph Node["Elastickv Node Process"]
    subgraph Adapters["Adapters"]
      RS["Redis Server (adapter/redis.go)"]
      DBS["DynamoDB Server (adapter/dynamodb.go)"]
      GS["gRPC Server (adapter/grpc.go)"]
      DS["Distribution Server (adapter/distribution_server.go)"]
      S3S["S3 Server (adapter/s3.go)"]
      SQSS["SQS Server (adapter/sqs.go)"]
      FS["FuseFS (internal/filesystem/fuseadapter)"]
    end

    subgraph DataPlane["Data Plane"]
      SCC["Sharded Coordinator (kv/sharded_coordinator.go)"]
      SS["Shard Store (kv/shard_store.go)"]
      SR["Shard Router (kv/shard_router.go)"]
    end

    subgraph ControlPlane["Control Plane"]
      DE["Route Engine Cache (distribution/engine.go)"]
      CW["Catalog Watchers (distribution/grpc_watcher.go, watcher.go)"]
      CS["Catalog Store (distribution/catalog.go)"]
    end

    subgraph Replication["Replication and Storage"]
      RG["Raft Group Runtime(s)"]
      FSM["KV FSM (kv/fsm.go)"]
      MV["MVCC Store (store/mvcc_store.go or store/lsm_store.go)"]
    end

    subgraph Clock["Timestamp Oracle"]
      HLC["HLC (kv/hlc.go)"]
    end
  end

  RC --> RS
  DC --> DBS
  GC --> GS
  OC --> DS
  SC --> S3S
  SQSC --> SQSS
  FC --> FS
  RP --> RS

  RS --> SCC
  RS --> SS
  DBS --> SCC
  DBS --> SS
  GS --> SCC
  GS --> SS
  S3S --> SCC
  S3S --> SS
  SQSS --> SCC
  SQSS --> SS
  FS --> SCC
  FS --> SS
  SCC --> SR
  SCC --> DE
  SS --> DE

  DS --> SCC
  DS --> CS

  SR --> RG
  SCC --> RG
  SS --> RG
  RG --> FSM
  FSM --> MV

  CS -->|"Read catalog snapshot"| MV
  CW -->|"Local polling"| CS
  CW -->|"WatchCatalog from catalog leader"| DS
  CW --> DE

  SCC -- "Fenced timestamp allocation (legacy mode)" --> HLC
  FSM -- "apply HLC lease: SetPhysicalCeiling" --> HLC
  SCC -- "propose HLC lease (leader)" --> RG
```

Catalog mutations follow the coordinator → Raft → FSM path; the catalog store reads the applied snapshot.
The HLC arrows show the legacy timestamp mode. The optional dedicated group-0 timestamp oracle is described in the [TSO operations guide](centralized_tso_operations.md).

## 2. Overall Runtime Architecture

```mermaid
flowchart LR
  subgraph Cluster["Elastickv Cluster"]
    subgraph N1["Node A"]
      AIN["Ingress (gRPC/Redis/DynamoDB/S3/SQS/FuseFS)"]
      ADE["Route Engine"]
      ACW["Catalog Watcher"]
      ARG0["Default Group Runtime (Catalog Keys)"]
      ARG1["User Group Runtime(s)"]
    end

    subgraph N2["Node B"]
      BIN["Ingress (gRPC/Redis/DynamoDB/S3/SQS/FuseFS)"]
      BDE["Route Engine"]
      BCW["Catalog Watcher"]
      BRG0["Default Group Runtime (Catalog Keys)"]
      BRG1["User Group Runtime(s)"]
    end

    subgraph N3["Node C"]
      CIN["Ingress (gRPC/Redis/DynamoDB/S3/SQS/FuseFS)"]
      CDE["Route Engine"]
      CCW["Catalog Watcher"]
      CRG0["Default Group Runtime (Catalog Keys)"]
      CRG1["User Group Runtime(s)"]
    end
  end

  AIN --> ADE
  BIN --> BDE
  CIN --> CDE

  ADE --> ARG1
  BDE --> BRG1
  CDE --> CRG1

  ARG0 <-. "Raft Replication" .-> BRG0
  BRG0 <-. "Raft Replication" .-> CRG0

  ARG1 <-. "Raft Replication" .-> BRG1
  BRG1 <-. "Raft Replication" .-> CRG1

  ACW --> ARG0
  BCW --> BRG0
  CCW --> CRG0

  ACW --> ADE
  BCW --> BDE
  CCW --> CDE
```

The topology illustrates separate catalog and user-group roles. Depending on configuration, the default group can also own user ranges; the quick start does not require a separate catalog-only group.

## 3. Same-group Control Plane Split Path

```mermaid
sequenceDiagram
  participant Op as "Operator"
  participant DS as "DistributionServer"
  participant SC as "ShardedCoordinator"
  participant DG as "Default Raft Group"
  participant CW as "CatalogWatcher (all nodes)"
  participant DE as "RouteEngine (all nodes)"

  Op->>DS: "SplitRange(expectedVersion, routeId, splitKey)"
  DS->>SC: "Dispatch catalog mutations as txn"
  SC->>DG: "Replicate catalog key updates"
  DG-->>DS: "Commit new catalog version"
  DS-->>Op: "SplitRangeResponse(left,right,catalogVersion)"
  Note over CW,DE: Routing caches refresh asynchronously
  CW->>DS: "WatchCatalog / polling fallback"
  DS-->>CW: "Catalog deltas or snapshot"
  CW->>DE: "ApplyDelta / ApplySnapshot"
```

## 4. Hybrid Logical Clock (HLC)

In legacy timestamp mode, the coordinator uses a Hybrid Logical Clock (`kv/hlc.go`).
A 64-bit timestamp packs a 48-bit physical component (Unix milliseconds) and a
16-bit in-memory logical counter. Raft commits the physical-time ceiling.
Fenced allocation rejects an expired ceiling or an exhausted logical window;
callers must handle that error. The unfenced `Next()` helper is not a persistence safety check.

```mermaid
sequenceDiagram
  participant L as "Leader Coordinator"
  participant RG as "Locally led Raft Group"
  participant F as "KV FSM (all nodes)"
  participant H as "HLC (all nodes)"
  participant Tx as "Txn / MVCC read-write path"

  loop every hlcRenewalInterval (=1s, <20s)
    L->>RG: Propose HLC lease (now + hlcPhysicalWindowMs)
    RG-->>F: Apply HLC lease entry
    F->>H: SetPhysicalCeiling(ms)
  end

  Tx->>H: NextFenced() / NextBatchFenced(n)
  alt Ceiling expired or logical window exhausted
    H-->>Tx: ErrCeilingExpired
  else Allocation permitted
    H-->>Tx: Monotonic timestamp / reserved batch
  end
```

## 5. Notes

1. Route catalog is persisted in reserved internal keys in the default Raft group.
2. `distribution.Engine` is an in-memory routing cache. The normal server consumes the catalog leader's `WatchCatalog` stream (with a polling fallback) and also polls its local applied catalog. Watchers apply deltas or replacement snapshots; the split response does not wait for every node's cache to refresh.
3. `SplitRange` is same-group only. Cross-group split migration uses a separate job path (`StartSplitMigration`), omitted from this diagram. The opt-in `--autoSplitCrossGroup` scheduler uses that path for eligible right-child moves; see the [automation design](design/2026_06_11_implemented_hotspot_split_milestone3_automation.md).
4. In legacy mode, the sharded coordinator renews HLC ceilings through each
   locally led group. The logical counter advances in memory with no Raft
   round-trip. The normal server shares a `*HLC` instance between its
   coordinator and KV FSMs; see `internal/server/run.go` and
   `internal/server/shard_groups.go` for wiring.
