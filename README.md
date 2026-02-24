# Elastickv

## Overview
Elastickv is an experimental project undertaking the challenge of creating a distributed key-value store optimized for cloud environments, in a manner similar to DynamoDB. This project is currently in the planning and development phase, with the goal to incorporate advanced features like Raft-based data replication, dynamic node scaling, and automatic hot spot re-allocation. Elastickv aspires to be a next-generation cloud data storage solution, combining efficiency with scalability.

**THIS PROJECT IS CURRENTLY UNDER DEVELOPMENT AND IS NOT READY FOR PRODUCTION USE.**

## Implemented Features (Verified)
- **Raft-based Data Replication**: KV state replication is implemented on Raft, with leader-based commit and follower forwarding paths.
- **Shard-aware Data Plane**: Static shard ranges across multiple Raft groups with shard routing/coordinator are implemented.
- **Durable Route Control Plane (Milestone 1)**: Durable route catalog, versioned route snapshot apply, watcher-based route refresh, and manual `ListRoutes`/`SplitRange` (same-group split) are implemented.
- **Protocol Adapters**: gRPC (`RawKV`/`TransactionalKV`), Redis (core commands + `MULTI/EXEC` and list operations), and DynamoDB-compatible API (`PutItem`/`GetItem`/`UpdateItem`/`TransactWriteItems`) implementations are available (runtime exposure depends on the selected server entrypoint/configuration).
- **Basic Consistency Behaviors**: Write-after-read checks, leader redirection/forwarding paths, and OCC conflict detection for transactional writes are covered by tests.

## Planned Features
- **Dynamic Node Scaling**: Automatic node/range scaling based on load is not yet implemented (current sharding operations are configuration/manual driven).
- **Automatic Hot Spot Re-allocation**: Automatic hotspot detection/scheduling and cross-group relocation are not yet implemented (Milestone 1 currently provides manual same-group split).

## Development Status
Elastickv is in the experimental and developmental phase, aspiring to bring to life features that resonate with industry standards like DynamoDB, tailored for cloud infrastructures. We welcome contributions, ideas, and feedback as we navigate through the intricacies of developing a scalable and efficient cloud-optimized distributed key-value store.

## Architecture

Architecture diagrams are available in:

- `docs/architecture_overview.md`


## Example Usage

This section provides sample commands to demonstrate how to use the project. Make sure you have the necessary dependencies installed before running these commands.

### Starting the Server
To start the server, use the following command:
```bash
go run cmd/server/demo.go
```

### Starting the Client

To start the client, use this command:
```bash
go run cmd/client/client.go
```

### Working with Redis
To start the Redis client:
```bash
redis-cli -p 63791
```

#### Setting and Getting Key-Value Pairs
To set a key-value pair and retrieve it:
```bash
set key value
get key
quit
```

### Connecting to a Follower Node
To connect to a follower node:
```bash
redis-cli -p 63792
get key
```

### Redirecting Set Operations to Leader Node
```bash
redis-cli -p 63792
set bbbb 1234
get bbbb
quit

redis-cli -p 63793
get bbbb
quit

redis-cli -p 63791
get bbbb
quit
```

### Manual Route Split API (Milestone 1)

Milestone 1 includes manual control-plane APIs on `proto.Distribution`:

1. `ListRoutes`
2. `SplitRange` (same-group split only)

Use `grpcurl` against a running node:

```bash
# 1) Read current durable route catalog
grpcurl -plaintext -d '{}' localhost:50051 proto.Distribution/ListRoutes

# 2) Split route 1 at user key "g" (bytes are base64 in grpcurl JSON: "g" -> "Zw==")
grpcurl -plaintext -d '{
  "expectedCatalogVersion": 1,
  "routeId": 1,
  "splitKey": "Zw=="
}' localhost:50051 proto.Distribution/SplitRange
```

Example `SplitRange` response:

```json
{
  "catalogVersion": "2",
  "left": {
    "routeId": "3",
    "start": "",
    "end": "Zw==",
    "raftGroupId": "1",
    "state": "ROUTE_STATE_ACTIVE",
    "parentRouteId": "1"
  },
  "right": {
    "routeId": "4",
    "start": "Zw==",
    "end": "bQ==",
    "raftGroupId": "1",
    "state": "ROUTE_STATE_ACTIVE",
    "parentRouteId": "1"
  }
}
```

Notes:

1. `expectedCatalogVersion` must match the latest `ListRoutes.catalogVersion`.
2. `splitKey` must be strictly inside the parent range (not equal to range start/end).
3. Milestone 1 split keeps both children in the same Raft group as the parent.


### Development

### Running Jepsen tests

Jepsen tests live in `jepsen/`. Install Leiningen and run tests locally:

```bash
curl -L https://raw.githubusercontent.com/technomancy/leiningen/stable/bin/lein > ~/lein
chmod +x ~/lein
(cd jepsen && ~/lein test)
```

These Jepsen tests execute concurrent read and write operations while a nemesis
injects random network partitions. Jepsen's linearizability checker verifies the
history.



### Setup pre-commit hooks
```bash
git config --local core.hooksPath .githooks
```
