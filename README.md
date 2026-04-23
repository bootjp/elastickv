# Elastickv

## Overview
Elastickv is an experimental project undertaking the challenge of creating a distributed key-value store optimized for cloud environments, in a manner similar to DynamoDB. This project is currently in the planning and development phase, with the goal to incorporate advanced features like Raft-based data replication, dynamic node scaling, and automatic hot spot re-allocation. Elastickv aspires to be a next-generation cloud data storage solution, combining efficiency with scalability.

**THIS PROJECT IS CURRENTLY UNDER DEVELOPMENT AND IS NOT READY FOR PRODUCTION USE.**

## Implemented Features
- **Raft-based Data Replication**: KV state replication is implemented on Raft, with leader-based commit and follower forwarding paths.
- **Shard-aware Data Plane**: Static shard ranges across multiple Raft groups with shard routing/coordinator are implemented.
- **Durable Route Control Plane (Milestone 1)**: Durable route catalog, versioned route snapshot apply, watcher-based route refresh, and manual `ListRoutes`/`SplitRange` (same-group split) are implemented.
- **Protocol Adapters**: gRPC (`RawKV`/`TransactionalKV`), Redis-compatible server, DynamoDB-compatible HTTP API, and S3-compatible HTTP API implementations are available (runtime exposure depends on the selected server entrypoint/configuration).
- **Redis Compatibility Scope**: Strings, hashes, lists, sets, sorted sets, HyperLogLog, streams (`XADD`/`XREAD`/`XRANGE`/`XREVRANGE`/`XTRIM`/`XLEN`), Pub/Sub (`PUBLISH`/`SUBSCRIBE`), transactions (`MULTI`/`EXEC`/`DISCARD`), TTL/expiry (`EXPIRE`/`PEXPIRE`/`TTL`/`PTTL`), key scanning (`KEYS`/`SCAN`), and Lua scripting (`EVAL`/`EVALSHA`) are implemented. A Redis-protocol reverse proxy (`cmd/redis-proxy`) supports phased zero-downtime migration from existing Redis deployments.
- **DynamoDB Compatibility Scope**: `CreateTable`/`DeleteTable`/`DescribeTable`/`ListTables`/`PutItem`/`GetItem`/`DeleteItem`/`UpdateItem`/`Query`/`Scan`/`BatchWriteItem`/`TransactWriteItems` are implemented.
- **S3 Compatibility Scope**: `ListBuckets`, `CreateBucket`, `HeadBucket`, `DeleteBucket`, `PutObject`, `GetObject`, `HeadObject`, `DeleteObject`, and `ListObjectsV2` (path-style) are implemented. AWS Signature Version 4 authentication with static credentials is supported. The server exposes an S3-compatible HTTP endpoint via `--s3Address`.
- **Basic Consistency Behaviors**: Write-after-read checks, leader redirection/forwarding paths, and OCC conflict detection for transactional writes are covered by tests.
- **Hybrid Logical Clock (HLC)**: Transactions are ordered by a 64-bit HLC split into an upper 48-bit physical component (Unix milliseconds) and a lower 16-bit logical counter. The logical half advances in memory with atomic CAS on every `Next()` call — no Raft round-trip per timestamp — so timestamp issuance stays in the nanosecond range. The physical half is bounded by a leader-lease style ceiling: the leader periodically commits a lease entry (`hlcRenewalInterval ≈ 1s`, window `hlcPhysicalWindowMs = 3s`) so that a newly elected leader inherits a safe lower bound and never issues timestamps overlapping the previous leader's window, without blocking per-request on consensus. See `docs/architecture_overview.md` §4 for details.

## Planned Features
- **Dynamic Node Scaling**: Automatic node/range scaling based on load is not yet implemented (current sharding operations are configuration/manual driven).
- **Automatic Hot Spot Re-allocation**: Automatic hotspot detection/scheduling and cross-group relocation are not yet implemented (Milestone 1 currently provides manual same-group split).

## Development Status
Elastickv is in the experimental and developmental phase, aspiring to bring to life features that resonate with industry standards like DynamoDB, tailored for cloud infrastructures. We welcome contributions, ideas, and feedback as we navigate through the intricacies of developing a scalable and efficient cloud-optimized distributed key-value store.

## Architecture

Architecture diagrams are available in:

- `docs/architecture_overview.md`

Deployment/runbook documents:

- `docs/docker_multinode_manual_run.md` (manual `docker run`, 4-5 node cluster on multiple VMs, no docker compose)
- `docs/etcd_raft_migration_operations.md` (offline HashiCorp-to-etcd cutover runbook and verification checklist)
- `docs/redis-proxy-deployment.md` (Redis-protocol reverse proxy for zero-downtime Redis-to-Elastickv migration)

Design documents:

- `docs/s3_compatible_adapter_design.md` (S3-compatible object storage adapter design, data model, routing, and rollout plan)

## Metrics and Grafana

Elastickv now exposes Prometheus metrics on `--metricsAddress` (default: `localhost:9090` in `main.go`, `127.0.0.1:9090` in `cmd/server/demo.go` single-node mode). The built-in 3-node demo binds metrics on `0.0.0.0:9091`, `0.0.0.0:9092`, and `0.0.0.0:9093`, and uses the bearer token `demo-metrics-token` unless `--metricsToken` is set.

The exported metrics cover:

- DynamoDB-compatible API request rate, success/system-error/user-error split, latency, in-flight requests, and per-table read/write activity
- Raft local state, leader identity, membership, leader changes seen, failed proposals, last-log/commit/applied/snapshot index, FSM backlog, and leader contact lag

Provisioned monitoring assets live under:

- `monitoring/prometheus/prometheus.yml`
- `monitoring/grafana/dashboards/elastickv-cluster-overview.json`
- `monitoring/grafana/dashboards/elastickv-dynamodb.json`
- `monitoring/grafana/dashboards/elastickv-raft-status.json`
- `monitoring/grafana/dashboards/elastickv-redis-summary.json`
- `monitoring/grafana/dashboards/elastickv-pebble-internals.json`
- `monitoring/grafana/provisioning/`
- `monitoring/docker-compose.yml`

The provisioned dashboards are organized by operator task:

- `Elastickv Cluster` is the landing page for leader identity, cluster-wide latency/error posture, and per-node Raft health
- `Elastickv DynamoDB` is the DynamoDB-compatible API drilldown for slow operations, noisy nodes, and hot/erroring tables
- `Elastickv Raft Status` is the control-plane drilldown for membership, leader changes, failed proposals, node state, index drift, backlog, and leader contact
- `Elastickv Redis` is the Redis-compatible API drilldown for per-command throughput/latency/errors, with a collapsible `Hot Path` row for GET fast-path (PR #560) verification
- `Elastickv Pebble Internals` is the storage-engine drilldown for block cache, L0 pressure, compactions, memtables, and store write conflicts

If you bind `--metricsAddress` to a non-loopback address, `--metricsToken` is required. Prometheus must send the same bearer token, for example:

```yaml
scrape_configs:
  - job_name: elastickv
    authorization:
      type: Bearer
      credentials: YOUR_METRICS_TOKEN
```

To scrape a multi-node deployment, bind `--metricsAddress` to each node's private IP and set `--metricsToken`, for example `--metricsAddress "10.0.0.11:9090" --metricsToken "YOUR_METRICS_TOKEN"`.

For the local 3-node demo, start Grafana and Prometheus with:

```bash
cd monitoring
docker compose up -d
```

`monitoring/prometheus/prometheus.yml` assumes the demo token `demo-metrics-token`. If you override `--metricsToken` when running `go run ./cmd/server/demo.go`, update `authorization.credentials` in that file to match.


## Example Usage

This section provides sample commands to demonstrate how to use the project. Make sure you have the necessary dependencies installed before running these commands.

### Starting the Server
To start a single node with the default `etcd/raft` runtime, use:
```bash
go run . \
  --address "127.0.0.1:50051" \
  --redisAddress "127.0.0.1:6379" \
  --raftId "n1" \
  --raftBootstrap
```

To expose metrics on a dedicated port:
```bash
go run . \
  --address "127.0.0.1:50051" \
  --redisAddress "127.0.0.1:6379" \
  --dynamoAddress "127.0.0.1:8000" \
  --s3Address "127.0.0.1:9000" \
  --s3Region "us-east-1" \
  --s3CredentialsFile "/etc/elastickv/s3creds.json" \
  --metricsAddress "127.0.0.1:9090" \
  --raftId "n1"
```

### Running with the etcd/raft backend

`etcd/raft` is the default backend:

```bash
go run . \
  --address "127.0.0.1:50051" \
  --redisAddress "127.0.0.1:6379" \
  --raftId "n1"
```

Elastickv writes a `raft-engine` marker into each Raft data directory and refuses
to reopen a directory with a different backend. Do not point the default etcd
runtime at an existing HashiCorp Raft directory, or vice versa. Use
`--raftEngine hashicorp` only for legacy clusters that have not migrated yet.

For existing stores, seed a fresh etcd data dir with the offline migrator:

```bash
go run ./cmd/etcd-raft-migrate \
  --fsm-store /var/lib/elastickv/n1/fsm.db \
  --dest /var/lib/elastickv-etcd/n1 \
  --peers n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053
```

The full cutover procedure, validation, and rollback constraints are documented
in `docs/etcd_raft_migration_operations.md`.

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

#### Sorted Sets, Streams, and Other Data Structures
The Redis adapter supports the full range of Redis data structures including sorted sets (`ZADD`/`ZRANGE`/`ZSCORE`), HyperLogLog (`PFADD`/`PFCOUNT`), streams (`XADD`/`XREAD`/`XRANGE`), sets (`SADD`/`SMEMBERS`), hashes (`HGET`/`HSET`/`HGETALL`), and Pub/Sub (`PUBLISH`/`SUBSCRIBE`). Lua scripts can be executed via `EVAL` and `EVALSHA`.

#### Migrating from Redis

A Redis-protocol reverse proxy (`redis-proxy`) enables phased zero-downtime migration. It supports dual-write, shadow-read comparison, and primary cutover modes. See `docs/redis-proxy-deployment.md` for the full deployment guide.

```bash
# Run redis-proxy in dual-write mode (writes to both Redis and Elastickv)
# The proxy listens on :6479 inside the container, exposed as :6379 on the host
# so existing clients can connect without changing their configuration.
docker run --rm \
  -p 6379:6479 \
  ghcr.io/bootjp/elastickv/redis-proxy:latest \
  -listen :6479 \
  -primary redis.internal:6379 \
  -secondary elastickv.internal:6380 \
  -mode dual-write
```

### Working with S3-compatible Storage

Elastickv exposes an S3-compatible HTTP API when `--s3Address` is set (for example `127.0.0.1:9000`). Any S3 client or SDK that supports path-style requests and AWS Signature Version 4 can connect to it.

```bash
# Configure the AWS CLI to point at Elastickv
aws configure set aws_access_key_id YOUR_ACCESS_KEY
aws configure set aws_secret_access_key YOUR_SECRET_KEY

# Create a bucket
aws --endpoint-url http://localhost:9000 s3api create-bucket --bucket my-bucket

# Upload an object
aws --endpoint-url http://localhost:9000 s3api put-object \
  --bucket my-bucket --key hello.txt --body hello.txt

# Download an object
aws --endpoint-url http://localhost:9000 s3api get-object \
  --bucket my-bucket --key hello.txt /tmp/hello.txt

# List objects
aws --endpoint-url http://localhost:9000 s3api list-objects-v2 \
  --bucket my-bucket
```

#### Public Bucket Access

Buckets support a bucket-level ACL that allows anonymous (unauthenticated) read access. Supported canned ACL values are `private` (default) and `public-read`.

```bash
# Create a public bucket
aws --endpoint-url http://localhost:9000 s3api create-bucket \
  --bucket public-assets --acl public-read

# Change an existing bucket to public
aws --endpoint-url http://localhost:9000 s3api put-bucket-acl \
  --bucket my-bucket --acl public-read

# Check a bucket's ACL
aws --endpoint-url http://localhost:9000 s3api get-bucket-acl \
  --bucket my-bucket

# Anonymous download (no credentials required)
curl http://localhost:9000/public-assets/hello.txt

# Revert to private
aws --endpoint-url http://localhost:9000 s3api put-bucket-acl \
  --bucket my-bucket --acl private
```

Public buckets allow unauthenticated `GetObject`, `HeadObject`, `HeadBucket`, and `ListObjectsV2`. Write operations (`PutObject`, `DeleteObject`, multipart uploads) always require authentication. `ListBuckets` and ACL management also always require authentication.

See `docs/s3_compatible_adapter_design.md` for the full data model, consistency guarantees, multipart upload design, and rollout plan. See `docs/s3_public_bucket_design.md` for the public bucket ACL design.

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
