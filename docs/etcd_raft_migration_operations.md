# etcd/raft Migration Operations

## Scope

This runbook covers the supported migration path from the legacy HashiCorp Raft
runtime to the default `etcd/raft` runtime.

Use it when:

1. migrating an existing cluster that already has HashiCorp-backed data dirs
2. validating a fresh etcd-backed deployment before production cutover
3. rolling out nodes with the repo's default runtime and operational scripts

This runbook does not support mixed-engine Raft groups. A single group must run
entirely on one runtime at a time.

## Current Defaults

As of the etcd rollout:

1. `go run .` starts with `--raftEngine=etcd` by default
2. Jepsen workloads default to `--raft-engine etcd`
3. `scripts/rolling-update.sh` defaults `RAFT_ENGINE=etcd`

If you still operate a legacy HashiCorp cluster, set the engine explicitly and
keep using the original data dirs until the offline migration below is complete.

## Preconditions

Before migrating an existing cluster:

1. Confirm the source cluster is healthy and fully replicated.
2. Inventory every Raft group, node ID, and advertised raft address.
3. Record the Redis and S3 routing maps if you use leader-local routing.
4. Stop application writes and plan for a full maintenance window.
5. Back up the existing data dirs before creating the new etcd dirs.

## Important Constraints

1. Elastickv writes a `raft-engine` marker into each data dir.
2. Reusing the same data dir across `hashicorp` and `etcd` is intentionally rejected.
3. The supported migration is offline. Stop the old cluster first, seed fresh
   etcd dirs, then start the new cluster.
4. Rollback means restarting the old HashiCorp cluster from the untouched old
   dirs. There is no mixed-engine rollback in place.

## Fresh Cluster Bootstrap

For a brand-new cluster, use the default runtime directly:

```bash
go run . \
  --address "10.0.0.11:50051" \
  --redisAddress "10.0.0.11:6379" \
  --raftId "n1" \
  --raftBootstrapMembers "n1=10.0.0.11:50051,n2=10.0.0.12:50051,n3=10.0.0.13:50051"
```

Start one process per node with the same `--raftBootstrapMembers` set and a
node-local `--raftDataDir`.

## Offline Migration from HashiCorp Raft

### Single-group cluster

1. Stop every Elastickv process in the cluster.
2. Keep the old HashiCorp data dirs intact.
3. Create a fresh etcd data dir for each node.
4. Run the migrator once per node:

```bash
go run ./cmd/etcd-raft-migrate \
  --fsm-store /var/lib/elastickv/n1/fsm.db \
  --dest /var/lib/elastickv-etcd/n1 \
  --peers n1=10.0.0.11:50051,n2=10.0.0.12:50051,n3=10.0.0.13:50051
```

5. Repeat for `n2`, `n3`, and every other cluster member with the same peer map.
6. Start the new cluster with `--raftDataDir` pointing at the new etcd dir.

### Multi-group cluster

Run the same procedure once per node and once per group directory. For example,
if a node has `group-1` and `group-2`, migrate both:

```bash
go run ./cmd/etcd-raft-migrate \
  --fsm-store /var/lib/elastickv/n1/group-1/fsm.db \
  --dest /var/lib/elastickv-etcd/n1/group-1 \
  --peers n1=10.0.0.11:50051,n2=10.0.0.12:50051,n3=10.0.0.13:50051

go run ./cmd/etcd-raft-migrate \
  --fsm-store /var/lib/elastickv/n1/group-2/fsm.db \
  --dest /var/lib/elastickv-etcd/n1/group-2 \
  --peers n1=10.0.0.11:50061,n2=10.0.0.12:50061,n3=10.0.0.13:50061
```

Use the group-specific raft addresses for each peer set.

## Startup Validation

After migration:

1. Start all nodes with `--raftEngine etcd` or rely on the default.
2. Verify the cluster forms with the expected membership.
3. Confirm reads and writes succeed through the primary protocol you use.

Useful checks:

```bash
go run ./cmd/raftadmin 10.0.0.11:50051 state
go run ./cmd/raftadmin 10.0.0.11:50051 leader
go run ./cmd/client/client.go
```

For Redis:

```bash
redis-cli -h 10.0.0.11 -p 6379 SET migration:smoke ok
redis-cli -h 10.0.0.12 -p 6379 GET migration:smoke
```

## CI and Jepsen Validation

The repo now validates the default etcd runtime in CI:

1. `.github/workflows/jepsen-test.yml` launches a 3-node etcd-backed cluster
2. Jepsen local workloads default to `--raft-engine etcd`

Before production rollout, run at least:

```bash
GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./...
GOCACHE=$(pwd)/.cache GOLANGCI_LINT_CACHE=$(pwd)/.golangci-cache golangci-lint run ./... --timeout=5m
```

For local Jepsen validation:

```bash
cd jepsen
HOME=$(pwd)/tmp-home \
LEIN_HOME=$(pwd)/.lein \
LEIN_JVM_OPTS="-Duser.home=$(pwd)/tmp-home" \
/tmp/lein run -m elastickv.redis-workload --local --raft-engine etcd
```

## Rolling Operations After Cutover

After a cluster is already on etcd:

1. `scripts/rolling-update.sh` defaults to `RAFT_ENGINE=etcd`
2. `cmd/raftadmin` can be used for `add_voter`, `remove_server`, and leadership transfer
3. Keep `RAFT_ENGINE=hashicorp` only for legacy nodes that have not migrated yet

## Rollback

Rollback is operationally simple but requires the old cluster to be preserved:

1. Stop the etcd-backed cluster.
2. Do not reuse the etcd data dirs with the HashiCorp runtime.
3. Restart the old HashiCorp cluster from the original pre-migration dirs.

If the old dirs were modified or discarded, rollback is no longer supported.
