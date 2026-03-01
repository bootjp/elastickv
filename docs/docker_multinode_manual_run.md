# Run a 4-5 Node Elastickv Cluster on Multiple VMs (Docker `run`, No Docker Compose)

This guide explains how to run Elastickv as a Raft cluster across multiple VMs using only `docker run`.

- No Docker Compose
- Not a single-VM deployment
- One Elastickv node per VM

## English Summary

Use `docker run` on 4 or 5 separate VMs and bootstrap the cluster from a fixed voter list via `--raftBootstrapMembers`.
Start `n1` with `--raftBootstrap`, then start enough additional nodes to reach quorum before sending write traffic.
Use private VM IPs for all bind addresses and lock down network access because gRPC, Redis, and DynamoDB-compatible endpoints are unauthenticated by default.

## Target Topology

- 1 node = 1 VM
- Total nodes: 4 or 5
- VMs must be able to reach each other over TCP (at minimum `50051/tcp`)
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

- Do not expose cluster ports to public networks.
- Restrict inbound sources with firewall/security groups/NACLs.
- Endpoints are unauthenticated by default: gRPC (including `RaftAdmin`), Redis API, and DynamoDB-compatible API.
- Prefer private subnets and trusted east-west networking only.
- Use network segmentation and, where possible, mTLS/TLS termination via your platform or proxy layer.
- Do not bind advertised service addresses to `0.0.0.0` or `localhost` in cluster flags.
  - Use each VM's routable private IP instead (for example, `10.0.0.11`).

## 1) Pull the Image on All VMs

```bash
docker pull ghcr.io/bootjp/elastickv:latest
```

## 2) Prepare Data Directory on All VMs

```bash
sudo mkdir -p /var/lib/elastickv
```

## 3) Define Shared Cluster Variables

`RAFT_TO_REDIS_MAP` is only for Redis leader routing.  
It is not used for Raft transport membership.

Raft node-to-node communication uses `--address` (gRPC transport).

Shared `RAFT_TO_REDIS_MAP` (5-node example):

```bash
RAFT_TO_REDIS_MAP="10.0.0.11:50051=10.0.0.11:6379,10.0.0.12:50051=10.0.0.12:6379,10.0.0.13:50051=10.0.0.13:6379,10.0.0.14:50051=10.0.0.14:6379,10.0.0.15:50051=10.0.0.15:6379"
```

Shared fixed bootstrap voters (5-node example):

```bash
RAFT_BOOTSTRAP_MEMBERS="n1=10.0.0.11:50051,n2=10.0.0.12:50051,n3=10.0.0.13:50051,n4=10.0.0.14:50051,n5=10.0.0.15:50051"
```

For a 4-node cluster, remove the `n5` entry from both variables.

## 4) Start Nodes with `docker run`

This guide uses `--network host` and explicit VM private IPs.

Binding guidance:

- Set `--address`, `--redisAddress`, and `--dynamoAddress` to the VM private IP.
- Do not use `0.0.0.0` as the advertised address.
- Do not use `localhost` for cluster communication.

`n1` (bootstrap node):

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
  --raftId "n1" \
  --raftDataDir "/var/lib/elastickv" \
  --raftRedisMap "${RAFT_TO_REDIS_MAP}" \
  --raftBootstrapMembers "${RAFT_BOOTSTRAP_MEMBERS}" \
  --raftBootstrap
```

`n2` (non-bootstrap):

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
  --raftId "n2" \
  --raftDataDir "/var/lib/elastickv" \
  --raftRedisMap "${RAFT_TO_REDIS_MAP}"
```

Start `n3` to `n5` the same way by replacing:

- `--address`
- `--redisAddress`
- `--dynamoAddress`
- `--raftId`

## Startup Order and Quorum Caution

Recommended startup sequence:

1. Start `n1` (bootstrap).
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
GRPCURL_IMG="fullstorydev/grpcurl:v1.9.3"

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
docker run --rm --network host fullstorydev/grpcurl:v1.9.3 \
  -plaintext -d '{}' 10.0.0.11:50051 RaftAdmin/Leader
```

Write/read through Redis endpoints:

```bash
redis-cli -h 10.0.0.11 -p 6379 SET health ok
redis-cli -h 10.0.0.12 -p 6379 GET health
```

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

## Stop and Cleanup

Stop/remove on each VM:

```bash
docker rm -f elastickv 2>/dev/null || true
```

Remove persisted data (if required):

```bash
sudo rm -rf /var/lib/elastickv/*
```

