# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

Elastickv is an experimental, cloud-oriented distributed key-value store written in Go (module `github.com/bootjp/elastickv`, Go 1.25.0 with `toolchain go1.26.2`). It exposes multiple wire protocols (gRPC RawKV/Transactional, Redis, DynamoDB-compatible HTTP, S3-compatible HTTP) on top of a Raft-replicated, MVCC/OCC storage engine. **Not production-ready.**

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
- **Replication (`internal/raftengine/`, `kv/fsm.go`)** — Only backend is `etcd/raft` under `internal/raftengine/etcd` (the hashicorp backend was dropped in `a35245a`; the `--raftEngine` flag still advertises `hashicorp` in `main.go` but `newRaftFactory` rejects anything other than `etcd`). Each Raft data dir contains a `raft-engine` marker so the process refuses to reopen a dir under a different backend. Note: README and `docs/etcd_raft_migration_operations.md` still reference `go run ./cmd/etcd-raft-migrate`, but that directory was deleted in `a35245a` — the migrator is no longer in-tree. The KV FSM (`kv/fsm.go`) applies committed entries to the storage layer and to the HLC ceiling.
- **Storage (`store/`)** — MVCC over Pebble (`mvcc_store.go`, `lsm_store.go`); OCC, TTL/expiry, snapshots (`snapshot_pebble.go`), and per-type helpers for Redis collections (`hash_helpers.go`, `list_helpers.go`, `set_helpers.go`, `zset_helpers.go`, `stream_helpers.go`).
- **Control plane (`distribution/`)** — Durable route catalog persisted in reserved keys of the **default Raft group**. `engine.go` is the read-side cache; `watcher.go` polls the catalog and applies versioned snapshots into the engine; `catalog.go` is the storage layer. Operator RPCs (`ListRoutes`, `SplitRange` — same-group split only) are on `proto.Distribution`. **All routing decisions read from the cached `RouteEngine`, not from the catalog directly.**
- **Timestamp Oracle (`kv/hlc.go`, `kv/hlc_wall.go`)** — All HLC timestamps are **issued exclusively by the Raft leader** via `ShardedCoordinator` / `Coordinator` — followers never call `HLC.Next()` for persistence. The 64-bit value splits into an upper 48-bit **physical** half (Unix ms) and a lower 16-bit **logical** counter, and the two halves take very different paths:
  - **Physical (upper 48 bits) — Raft-agreed.** The leader periodically (`hlcRenewalInterval = 1s`, window `hlcPhysicalWindowMs = 3s`) proposes a ceiling entry through the default Raft group; FSM apply on every node calls `SetPhysicalCeiling`. `Next()` clamps the physical half to `max(wall_ms, ceiling_ms)`, so a newly elected leader can never issue a timestamp inside the previous leader's lease window.
  - **Logical (lower 16 bits) — in-memory only.** Advanced by atomic CAS on each `Next()` call; **no Raft round-trip and no consensus per timestamp**. This is what keeps issuance in the nanosecond range.
  - The coordinator and FSM **must share the same `*HLC`** instance (wired via `WithHLC` / `NewKvFSMWithHLC`) so the in-memory counter and the replicated ceiling stay coupled.
- **Process entrypoints** — `main.go` is the multi-binary server (gRPC + Redis + DynamoDB + S3 + admin + metrics + pprof). `cmd/server/demo.go` is a single-process 3-node demo. `cmd/client/`, `cmd/redis-proxy/`, `cmd/elastickv-admin/`, and `cmd/raftadmin/` are standalone tools. `multiraft_runtime.go` and `shard_config.go` wire shard groups to addresses for multi-group deployments.

## Conventions

- `gofmt` + the linters in `.golangci.yaml` (`gocritic`, `gocyclo`, `gosec`, `wrapcheck`, `errorlint`, `mnd`, etc.) are enforced. Avoid `//nolint` — refactor instead.
- Errors: wrap with `github.com/cockroachdb/errors` (the `wrapcheck` linter enforces wrapping at boundaries).
- Logging: structured `slog` with stable keys (`key`, `commit_ts`, `route_id`, …).
- Test files are co-located (`*_test.go`); prefer table-driven tests. `pgregory.net/rapid` is available for property tests (see `kv/transcoder.go` callers, `store/mvcc_store_prop_test.go`).
- After changes to replication, MVCC, OCC, or the Redis adapter, run the relevant Jepsen suite — these are the integration-level safety net.
- HLC: do **not** issue persistence timestamps from non-leader nodes; OCC decisions assume leader-issued ts. **Never use the local wall clock (`time.Now()` / `hlc_wall.go` directly) for snapshot reads, MVCC visibility checks, OCC validation, lease/expiry decisions, or any other ordering-sensitive read** — always go through `HLC.Next()` (writes/commits) or the leader-issued read timestamp pipeline. Local wall clocks are only valid for diagnostics/metrics and as the input that bounds the physical ceiling. Keep wall clocks reasonably synchronized across nodes.
- Route catalog mutations must go through `SplitRange` (or future control-plane RPCs) so the catalog version bumps and watchers fan out — never write catalog keys directly.
- Commits: short imperative summary, optional scope prefix matching the touched area (`store:`, `adapter:`, `kv:`, `docs:`, …). PR descriptions should call out behavior change, risk, and the test evidence (`go test`, `make lint`, relevant Jepsen suite).

## Design Documents

`docs/design/` is dated proposals and as-implemented records (`*_proposed_*.md` / `*_implemented_*.md`). Check it before designing anything new — there is likely a recent precedent (HLC lease, FSM compaction, S3 adapter, lease reads, Lua commit batching, TTL inline value, centralized TSO proposal, etc.). `docs/design/README.md` indexes them.
