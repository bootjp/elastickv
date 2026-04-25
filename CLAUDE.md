# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Elastickv is an experimental, cloud-oriented distributed key-value store written in Go (module `github.com/bootjp/elastickv`, Go 1.25+). It exposes multiple wire protocols (gRPC RawKV/Transactional, Redis, DynamoDB-compatible HTTP, S3-compatible HTTP) on top of a Raft-replicated, MVCC/OCC storage engine. **Not production-ready.**

## Common Commands

```bash
make test          # go test -v -race ./...
make lint          # golangci-lint --config=.golangci.yaml run --fix
make run           # go run cmd/server/demo.go (built-in 3-node single-process demo)
make client        # go run cmd/client/client.go
make gen           # regenerate protobufs (cd proto && make gen)
```

Run a single test or package:

```bash
go test -run TestName ./store/...
go test -race ./kv/...
```

If `$GOCACHE` is sandbox-blocked (macOS), prefix with `GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp`. The lint cache analog is `GOLANGCI_LINT_CACHE=$(pwd)/.golangci-cache`.

Single-node server (etcd/raft is the default backend):

```bash
go run . --address "127.0.0.1:50051" --redisAddress "127.0.0.1:6379" --raftId "n1" --raftBootstrap
```

The local Jepsen runner (builds, starts a 3-node cluster on `5005{1,2,3}` / `6379{1,2,3}` / `6380{1,2,3}` / `6390{1,2,3}`, runs DynamoDB workloads):

```bash
./scripts/run-jepsen-local.sh                     # full cycle
./scripts/run-jepsen-local.sh --no-rebuild --no-cluster  # reuse running cluster
```

Direct Jepsen invocation requires isolating Leiningen state from `$HOME`:

```bash
cd jepsen && HOME=$(pwd)/tmp-home LEIN_HOME=$(pwd)/.lein \
  LEIN_JVM_OPTS="-Duser.home=$(pwd)/tmp-home" /tmp/lein test
# Same pattern under jepsen/redis/ with HOME=$(pwd)/../tmp-home etc.
```

Protobuf regeneration is version-pinned and will fail unless the toolchain matches: `libprotoc 29.3`, `protoc-gen-go v1.36.11`, `protoc-gen-go-grpc 1.6.1` (see `proto/Makefile`).

Pre-commit hook (runs `make lint`) is opt-in: `git config --local core.hooksPath .githooks`.

## Architecture

The full diagrams live in `docs/architecture_overview.md` — read it before non-trivial changes touching coordination, replication, or routing. Big picture:

- **Adapters (`adapter/`)** — Per-protocol ingress: `redis.go`, `dynamodb.go`, `grpc.go`, `s3.go`, `distribution_server.go` (operator/control plane). `redis_proxy.go` and the standalone `cmd/redis-proxy/` implement a phased Redis-to-Elastickv migration proxy with dual-write/shadow-read modes (see `proxy/`).
- **Data plane (`kv/`)** — `ShardedCoordinator` (`sharded_coordinator.go`) is the entry point all adapters dispatch into. It resolves keys via `ShardRouter` (`shard_router.go`) against the in-memory `RouteEngine` cache, then drives `ShardStore` (`shard_store.go`) per Raft group. Transactions live in `transaction.go` / `txn_codec.go`; OCC and lock resolution in `lock_resolver.go`. Leader-only reads go through `lease_state.go`.
- **Replication (`internal/raftengine/`, `kv/fsm.go`)** — Default backend is `etcd/raft` (`internal/raftengine/etcd`). Each Raft data dir contains a `raft-engine` marker; the process refuses to reopen a dir under a different backend. Use `cmd/etcd-raft-migrate` for offline migration (procedure in `docs/etcd_raft_migration_operations.md`); legacy hashicorp clusters set `--raftEngine hashicorp`. The KV FSM (`kv/fsm.go`) applies committed entries to the storage layer and to the HLC ceiling.
- **Storage (`store/`)** — MVCC over Pebble (`mvcc_store.go`, `lsm_store.go`); OCC, TTL/expiry, snapshots (`snapshot_pebble.go`), and per-type helpers for Redis collections (`hash_helpers.go`, `list_helpers.go`, `set_helpers.go`, `zset_helpers.go`, `stream_helpers.go`).
- **Control plane (`distribution/`)** — Durable route catalog persisted in reserved keys of the **default Raft group**. `engine.go` is the read-side cache; `watcher.go` polls the catalog and applies versioned snapshots into the engine; `catalog.go` is the storage layer. Operator RPCs (`ListRoutes`, `SplitRange` — same-group split only) are on `proto.Distribution`. **All routing decisions read from the cached `RouteEngine`, not from the catalog directly.**
- **Timestamp Oracle (`kv/hlc.go`, `kv/hlc_wall.go`)** — 64-bit HLC: 48-bit physical (Unix ms) + 16-bit logical. The logical half advances in-memory via atomic CAS — no Raft round-trip per `Next()`. The physical half is bounded by a leader-lease ceiling: leader periodically (`hlcRenewalInterval ≈ 1s`, window `hlcPhysicalWindowMs = 3s`) commits a lease entry replicated through the default Raft group; FSM apply calls `SetPhysicalCeiling`. The coordinator and FSM **must share the same `*HLC`** (wired via `WithHLC` / `NewKvFSMWithHLC`).
- **Process entrypoints** — `main.go` is the multi-binary server (gRPC + Redis + DynamoDB + S3 + admin + metrics + pprof). `cmd/server/demo.go` is a single-process 3-node demo. `cmd/client/`, `cmd/redis-proxy/`, `cmd/elastickv-admin/`, `cmd/raftadmin/`, and `cmd/etcd-raft-migrate/` are standalone tools. `multiraft_runtime.go` and `shard_config.go` wire shard groups to addresses for multi-group deployments.

## Conventions

- `gofmt` + the linters in `.golangci.yaml` (`gocritic`, `gocyclo`, `gosec`, `wrapcheck`, `errorlint`, `mnd`, etc.) are enforced. Avoid `//nolint` — refactor instead.
- Errors: wrap with `github.com/cockroachdb/errors` (the `wrapcheck` linter enforces wrapping at boundaries).
- Logging: structured `slog` with stable keys (`key`, `commit_ts`, `route_id`, …).
- Test files are co-located (`*_test.go`); prefer table-driven tests. `pgregory.net/rapid` is available for property tests (see `kv/transcoder.go` callers, `store/mvcc_store_prop_test.go`).
- After changes to replication, MVCC, OCC, or the Redis adapter, run the relevant Jepsen suite — these are the integration-level safety net.
- HLC: do **not** issue persistence timestamps from non-leader nodes; OCC decisions assume leader-issued ts. Keep wall clocks reasonably synchronized across nodes.
- Route catalog mutations must go through `SplitRange` (or future control-plane RPCs) so the catalog version bumps and watchers fan out — never write catalog keys directly.

## Design Documents

`docs/design/` is dated proposals and as-implemented records (`*_proposed_*.md` / `*_implemented_*.md`). Check it before designing anything new — there is likely a recent precedent (HLC lease, FSM compaction, S3 adapter, lease reads, Lua commit batching, TTL inline value, centralized TSO proposal, etc.). `docs/design/README.md` indexes them.
