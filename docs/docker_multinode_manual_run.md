# Run a 4-5 Node Elastickv Cluster on Multiple VMs (Docker `run`, No Docker Compose)

This guide explains how to run Elastickv as a Raft cluster across multiple VMs using only `docker run`.

- No Docker Compose
- Not a single-VM deployment
- One Elastickv node per VM

## English Summary

Use `docker run` on 4 or 5 separate VMs and bootstrap the cluster from a fixed voter list via `--raftBootstrapMembers`.
Start all initial nodes (`n1` to `n4`/`n5`) with both `--raftBootstrapMembers` and `--raftBootstrap`, then wait for quorum before sending write traffic.
Use private VM IPs for all bind addresses and lock down network access because gRPC, Redis, and DynamoDB-compatible endpoints are unauthenticated by default.

## Target Topology

- 1 node = 1 VM
- Total nodes: 4 or 5
- VMs must be able to reach each other over TCP (at minimum `50051/tcp`)
- S3-compatible API clients must be able to reach `9000/tcp` on each node
- Prometheus must be able to reach each node's metrics endpoint if you want centralized monitoring (`9090/tcp` in the examples below)
- Docker Engine installed on every VM

Example (5 nodes):

| Node ID | VM | IP |
| --- | --- | --- |
| n1 | vm1 | 10.0.0.11 |
| n2 | vm2 | 10.0.0.12 |
| n3 | vm3 | 10.0.0.13 |
| n4 | vm4 | 10.0.0.14 |
| n5 | vm5 | 10.0.0.15 |

For a 4-node cluster, remove `n5`.

## Fault Tolerance

| Nodes | Quorum | Tolerated Simultaneous Failures |
| --- | --- | --- |
| 4 | 3 | 1 |
| 5 | 3 | 2 (recommended) |

If fault tolerance is a priority, use 5 nodes.

## Security Requirements (Stronger Defaults)

These examples prioritize operational clarity, not hardening. Treat them as baseline only.

- Mandatory isolation: do not expose any Elastickv cluster port to the public Internet or to shared/multi-tenant networks.
- Restrict inbound sources with firewall/security groups/NACLs so only Elastickv nodes and tightly controlled admin hosts can connect.
- Endpoints are unauthenticated and plaintext by default: gRPC (including `RaftAdmin`), Redis API, DynamoDB-compatible API, and S3-compatible API (unless `--s3CredentialsFile` is configured).
- Any principal with network access to these ports can read/modify data and reconfigure cluster membership.
- Run Elastickv on dedicated private subnets/VPCs for the cluster, without direct Internet routing and without cross-tenant sharing.
- Enforce network segmentation and, where possible, add TLS/mTLS and authentication via Elastickv features or a trusted terminating proxy/service mesh.
- Do not bind advertised service addresses to `0.0.0.0` or `localhost` in cluster flags; use each VM's routable private IP (for example, `10.0.0.11`).

## 1) Pull the Image on All VMs

```bash
docker pull ghcr.io/bootjp/elastickv:latest
```

## 2) Prepare Data Directory on All VMs

```bash
sudo mkdir -p /var/lib/elastickv
```

## 3) Define Shared Cluster Variables

`RAFT_TO_REDIS_MAP` and `RAFT_TO_S3_MAP` are for Redis/S3 leader routing only.
They are not used for Raft transport membership.

Raft node-to-node communication uses `--address` (gRPC transport).

Shared `RAFT_TO_REDIS_MAP` (5-node example):

```bash
RAFT_TO_REDIS_MAP="10.0.0.11:50051=10.0.0.11:6379,10.0.0.12:50051=10.0.0.12:6379,10.0.0.13:50051=10.0.0.13:6379,10.0.0.14:50051=10.0.0.14:6379,10.0.0.15:50051=10.0.0.15:6379"
```

Shared `RAFT_TO_S3_MAP` (5-node example):

```bash
RAFT_TO_S3_MAP="10.0.0.11:50051=10.0.0.11:9000,10.0.0.12:50051=10.0.0.12:9000,10.0.0.13:50051=10.0.0.13:9000,10.0.0.14:50051=10.0.0.14:9000,10.0.0.15:50051=10.0.0.15:9000"
```

Shared fixed bootstrap voters (5-node example):

```bash
RAFT_BOOTSTRAP_MEMBERS="n1=10.0.0.11:50051,n2=10.0.0.12:50051,n3=10.0.0.13:50051,n4=10.0.0.14:50051,n5=10.0.0.15:50051"
```

Shared metrics bearer token (required because the examples bind `--metricsAddress` to non-loopback VM IPs):

```bash
ELASTICKV_METRICS_TOKEN="$(openssl rand -hex 32)"
```

For a 4-node cluster, remove the `n5` entry from all variables above.

## 4) Start Nodes with `docker run`

This guide uses `--network host` and explicit VM private IPs.

Binding guidance:

- Set `--address`, `--redisAddress`, `--dynamoAddress`, and `--s3Address` to the VM private IP.
- Set `--metricsAddress` to the VM private IP if Prometheus scrapes from another host.
- Set `--metricsToken` to the same shared bearer token on every node whenever `--metricsAddress` is non-loopback.
- Do not use `0.0.0.0` as the advertised address.
- Do not use `localhost` for cluster communication.

`n1` (initial voter):

```bash
docker rm -f elastickv 2>/dev/null || true

docker run -d \
  --name elastickv \
  --restart unless-stopped \
  --network host \
  -v /var/lib/elastickv:/var/lib/elastickv \
  ghcr.io/bootjp/elastickv:latest /app \
  --address "10.0.0.11:50051" \
  --redisAddress "10.0.0.11:6379" \
  --dynamoAddress "10.0.0.11:8000" \
  --s3Address "10.0.0.11:9000" \
  --s3Region "us-east-1" \
  --s3PathStyleOnly=true \
  --metricsAddress "10.0.0.11:9090" \
  --metricsToken "${ELASTICKV_METRICS_TOKEN}" \
  --raftId "n1" \
  --raftDataDir "/var/lib/elastickv" \
  --raftRedisMap "${RAFT_TO_REDIS_MAP}" \
  --raftS3Map "${RAFT_TO_S3_MAP}" \
  --raftBootstrapMembers "${RAFT_BOOTSTRAP_MEMBERS}" \
  --raftBootstrap
```

`n2` (initial voter):

```bash
docker rm -f elastickv 2>/dev/null || true

docker run -d \
  --name elastickv \
  --restart unless-stopped \
  --network host \
  -v /var/lib/elastickv:/var/lib/elastickv \
  ghcr.io/bootjp/elastickv:latest /app \
  --address "10.0.0.12:50051" \
  --redisAddress "10.0.0.12:6379" \
  --dynamoAddress "10.0.0.12:8000" \
  --s3Address "10.0.0.12:9000" \
  --s3Region "us-east-1" \
  --s3PathStyleOnly=true \
  --metricsAddress "10.0.0.12:9090" \
  --metricsToken "${ELASTICKV_METRICS_TOKEN}" \
  --raftId "n2" \
  --raftDataDir "/var/lib/elastickv" \
  --raftRedisMap "${RAFT_TO_REDIS_MAP}" \
  --raftS3Map "${RAFT_TO_S3_MAP}" \
  --raftBootstrapMembers "${RAFT_BOOTSTRAP_MEMBERS}" \
  --raftBootstrap
```

Start `n3` to `n5` the same way by replacing:

- `--address`
- `--redisAddress`
- `--dynamoAddress`
- `--s3Address`
- `--metricsAddress`
- `--raftId`

## Startup Order and Quorum Caution

Recommended startup sequence:

1. Start `n1`.
2. Immediately start enough peers to form quorum (`n2` and `n3` at minimum).
3. Start remaining nodes (`n4`, `n5`).
4. Send write traffic only after quorum is confirmed and leader election is stable.

Important behavior:

- In a 5-node cluster, at least 3 voters must be up for writes to commit.
- In a 4-node cluster, at least 3 voters must be up for writes to commit.
- If fewer than quorum nodes are running, leader election/commit may stall and writes may fail or hang.

## 5) Verify Cluster Convergence

Run on `n1`:

```bash
# Pin to an immutable digest.
GRPCURL_IMG="fullstorydev/grpcurl@sha256:085e183ca334eb4e81ca81ee12cbb2b2737505d1d77f5e33dabc5d066593d998"

# Optional: re-check the current digest for v1.9.3 before use.
# TOKEN=$(curl -fsSL 'https://auth.docker.io/token?service=registry.docker.io&scope=repository:fullstorydev/grpcurl:pull' | jq -r .token)
# curl -fsSI -H "Authorization: Bearer ${TOKEN}" -H 'Accept: application/vnd.docker.distribution.manifest.list.v2+json' \
#   https://registry-1.docker.io/v2/fullstorydev/grpcurl/manifests/v1.9.3 | tr -d '\r' | grep -i docker-content-digest

# Wait for every node gRPC endpoint
for ip in 10.0.0.11 10.0.0.12 10.0.0.13 10.0.0.14 10.0.0.15; do
  until docker run --rm --network host "${GRPCURL_IMG}" \
    -plaintext "${ip}:50051" list >/dev/null 2>&1; do
    sleep 1
  done
done

# Check Raft members
docker run --rm --network host "${GRPCURL_IMG}" \
  -plaintext -d '{}' 10.0.0.11:50051 RaftAdmin/GetConfiguration
```

For a 4-node cluster, remove `10.0.0.15` from the loop.

## 6) Validate Read/Write

Check leader:

```bash
docker run --rm --network host "${GRPCURL_IMG}" \
  -plaintext -d '{}' 10.0.0.11:50051 RaftAdmin/Leader
```

Write/read through Redis endpoints:

```bash
redis-cli -h 10.0.0.11 -p 6379 SET health ok
redis-cli -h 10.0.0.12 -p 6379 GET health
```

## 6.5) Validate Metrics

Check the local Prometheus endpoint on any node:

```bash
curl -fsS -H "Authorization: Bearer ${ELASTICKV_METRICS_TOKEN}" \
  http://10.0.0.11:9090/metrics | grep '^elastickv_'
```

Prometheus must send the same bearer token when scraping:

```yaml
scrape_configs:
  - job_name: elastickv
    authorization:
      type: Bearer
      credentials: ${ELASTICKV_METRICS_TOKEN}
```

Grafana/Prometheus provisioning examples are available under `monitoring/`.

## 7) Fault Tolerance Drill

With 5 nodes, the cluster should continue serving with up to 2 node failures.

Example: stop `n4` and `n5`:

```bash
docker stop elastickv
```

Then verify writes/reads still succeed from remaining nodes:

```bash
redis-cli -h 10.0.0.11 -p 6379 SET survive yes
redis-cli -h 10.0.0.12 -p 6379 GET survive
```

## DynamoDB Compatibility Notes

Current DynamoDB-compatible API coverage includes:

- `CreateTable`
- `DeleteTable`
- `DescribeTable`
- `ListTables`
- `PutItem`
- `GetItem`
- `DeleteItem`
- `UpdateItem`
- `Query`
- `TransactWriteItems`

Currently unsupported commands:

- `Scan`
- `BatchWriteItem`

If you migrate existing DynamoDB data, use key-based reads (`GetItem`/`Query`) and write with `PutItem`/`TransactWriteItems` instead of `Scan`/`BatchWriteItem`.

## Stop and Cleanup

Stop/remove on each VM:

```bash
docker rm -f elastickv 2>/dev/null || true
```

Remove persisted data (if required):

```bash
sudo rm -rf /var/lib/elastickv/*
```
