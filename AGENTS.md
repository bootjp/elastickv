# Repository Guidelines

## Project Structure & Modules
- `cmd/server`, `cmd/client`: entrypoints for running the KV server and client.
- `store/`: MVCC storage engine, OCC/TTL, and related tests.
- `kv/`: hybrid logical clock (HLC) utilities and KV interfaces.
- `adapter/`: protocol adapters (e.g., Redis), plus integration tests.
- `jepsen/`, `jepsen/redis/`: Jepsen test harnesses and workloads.
- `proto/`, `distribution/`, `internal/`: supporting protobufs, build assets, and shared helpers.

## Build, Test, and Development Commands
- `go test ./...` — run unit/integration tests. If macOS sandbox blocks `$GOCACHE`, prefer `GOCACHE=$(pwd)/.cache GOTMPDIR=$(pwd)/.cache/tmp go test ./...`.
- `GOCACHE=$(pwd)/.cache GOLANGCI_LINT_CACHE=$(pwd)/.golangci-cache golangci-lint run ./... --timeout=5m` — full lint suite.
- `HOME=$(pwd)/jepsen/tmp-home LEIN_HOME=$(pwd)/jepsen/.lein LEIN_JVM_OPTS="-Duser.home=$(pwd)/jepsen/tmp-home" /tmp/lein test` (from `jepsen/` or `jepsen/redis/`) — Jepsen tests.
- `go run ./cmd/server` / `go run ./cmd/client` — start server or CLI locally.

## Coding Style & Naming
- Go code: `gofmt` + project lint rules (`golangci-lint`). Avoid adding `//nolint` unless absolutely required; prefer refactoring.
- Naming: Go conventions (MixedCaps for exported identifiers, short receiver names). Filenames remain lowercase with underscores only where existing.
- Logging: use `slog` where present; maintain structured keys (`key`, `commit_ts`, etc.).

## Testing Guidelines
- Unit tests co-located with packages (`*_test.go`); prefer table-driven cases.
- TTL/HLC behaviors live in `store/` and `kv/`; add coverage when touching clocks, OCC, or replication logic.
- Integration: run Jepsen suites after changes affecting replication, MVCC, or Redis adapter.
  - `cd jepsen && HOME=$(pwd)/tmp-home LEIN_HOME=$(pwd)/.lein LEIN_JVM_OPTS="-Duser.home=$(pwd)/tmp-home" /tmp/lein test`
  - `cd jepsen/redis && HOME=$(pwd)/../tmp-home LEIN_HOME=$(pwd)/../.lein LEIN_JVM_OPTS="-Duser.home=$(pwd)/../tmp-home" /tmp/lein test`

## Commit & Pull Request Guidelines
- Messages: short imperative summary (e.g., “Add HLC TTL handling”). Include scope when helpful (`store:`, `adapter:`).
- Pull requests: describe behavior change, risk, and test evidence (`go test`, lint, Jepsen). Add repro steps for bug fixes.

## Security & Configuration Tips
- Hybrid clock derives from wall-clock millis; keep system clock reasonably synchronized across nodes.
- Avoid leader-local timestamps in persistence; timestamp issuance should originate from the Raft leader to prevent skewed OCC decisions.
