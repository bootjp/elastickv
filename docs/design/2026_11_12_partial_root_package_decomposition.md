# Root package decomposition

Status: Partial (M1, part of M2 and part of M4 implemented)
Author: bootjp
Date: 2026-11-12

## Problem

The server binary is the repository-root `package main`. It holds about
25,000 lines of non-test code in 107 files and about 28,000 lines of tests
in 125 files. Splitting `main.go` into `main_<area>.go` files made the
files easier to read, but every file still shares one namespace.
Nothing in that namespace marks which parts are private to an area, so any
area can depend on any other.

The dependency survey that led to this proposal (an AST scan of
cross-file identifier references, 2026-11-12) found three root causes:

1. **Flag globals.** 92 `flag.*` package variables are dereferenced deep
   inside wiring helpers (`*raftId`, `*adminTokenFile`, `*filesystemMount`, …).
   A helper's real inputs are invisible in its signature, and tests
   mutate globals to drive it. `main_server_runner.go`, `main_tso_wiring.go`,
   `main.go`, `main_filesystem.go` and the admin files hold most of the
   dereferences.
2. **Shared runtime types live in `main`.** `raftGroupRuntime`,
   `groupSpec`, `raftBootstrapConfig` and `runtimeConfig` are used by
   every area (bootstrap, encryption, snapshot offload, leader balance,
   split migration, filesystem). Because those types cannot be imported,
   no area can leave `main`.
3. **No leaf areas have been extracted.** Even self-contained code, such
   as the admin bridges that convert `adapter` DTOs to `internal/admin`
   DTOs, stays in `main` by default. The bootstrap area (48 files, about
   9,600 lines) and the encryption wiring (9 files, about 3,600 lines) are
   the largest blocks.

`internal/serverwiring` already shows the target shape: the keyviz,
autosplit, credential and monitoring wiring there is shared by `main.go`
and `cmd/server/demo.go` through explicit parameters.

## Goals

- Make each wiring area an importable package with an explicit,
  documented API, and keep its unexported details inside that package.
- Make every input to a wiring helper visible in its signature. No helper
  below `main` reads a flag global.
- Shrink the root package to flag registration, `main()` and the
  top-level `run()` assembly.
- Change no runtime behavior: the flags, defaults, start-up order, wire
  formats and on-disk formats all stay as they are.

## Non-goals

- Redesigning any subsystem (TSO, encryption, bootstrap protocol,
  filesystem). Code moves, and signatures change only to make inputs
  explicit.
- Changing the CLI surface, flag names or defaults.
- Merging `cmd/server/demo.go` with `main.go` (possible later, once
  both can call the same packages).

## Target layout

```text
main.go, main_flags.go          flag registration, main(), run()
internal/adminbridge/           adapter <-> internal/admin DTO bridges (SQS, S3, DynamoDB, keyviz)
internal/noderuntime/           raftGroupRuntime, groupSpec, raft bootstrap/peer config, runtimeConfig parsing
internal/serverwiring/          (existing) plus TSO wiring, distribution start-up, startup gate, node auth
internal/leaderbalance/         leader-balance scheduler wiring
internal/fswiring/              FUSE filesystem start-up and lifecycle
internal/snapshotoffload/       (existing package) gains the start-up wiring now in main_snapshot_offload*.go
internal/splitmigration/        split-migration capability gate, clients, membership/ownership wiring
internal/encryptionwiring/      KEK loading, start-up guards, registration, rotation, fan-out
internal/clusterbootstrap/      the `elastickv bootstrap ...` operator CLI and runtime
```

Package names are working names. Each milestone may adjust them, and
the doc is updated when that happens.

## Milestones

Each milestone is one PR, except M2 and M4, which land one area (or one
group of areas) per PR. Every PR compiles on its own and is
behavior-neutral.
Tests move with the code they cover.

- **M1 — Admin bridges.** Move `sqsQueuesBridge`, `bucketsBridge`,
  `dynamoTablesBridge`, their converters, `convertAdminPrincipal`,
  `isLeaderChurnError` and the keyviz source/fan-out helpers to
  `internal/adminbridge`. These are leaf code: they depend only on
  `adapter`, `internal/admin` and `keyviz`. This milestone sets the
  pattern for later milestones and needs no global-state changes.
- **M2 — Typed flag config.** Collect the flag globals into structs
  grouped by area (`adminFlags`, `filesystemFlags`, `tsoFlags`, …) that
  are registered on a `*flag.FlagSet`. `run()` builds plain config values
  from those structs and passes them down. Helpers stop dereferencing
  globals. Tests construct config values instead of mutating globals.
  About 130 flags across some 25 files is too much for one reviewable
  PR, so M2 lands one group of areas per PR.
- **M3 — `internal/noderuntime`.** Move `raftGroupRuntime`, `groupSpec`,
  the raft bootstrap/peer config resolution, `runtimeConfig` parsing and
  the shard-group builder. This unblocks every remaining milestone.
- **M4 — Mid-sized areas.** Move leader balance, filesystem wiring,
  snapshot-offload wiring, split-migration wiring, TSO wiring, the startup
  gate and node gRPC auth. Each area may be its own PR.
- **M5 — Encryption wiring.** Move it to `internal/encryptionwiring`.
- **M6 — Bootstrap.** Move the bootstrap operator CLI and runtime to
  `internal/clusterbootstrap`. `main` keeps only the
  `dispatchBootstrapOperator` call.

## Progress

- M1: admin bridges moved to `internal/adminbridge`.
- M4 (partial): the leader-balance scheduler moved to `internal/leaderbalance`
  and FUSE mounting to `internal/fsmount`. Both see the node through narrow
  inputs (a `Group` interface with `GroupID` / `Engine`, and a resolved
  `fsmount.Config`) rather than `*raftGroupRuntime` or flag globals, so
  neither waited for M3.
- M2 (partial): `serverFlags` in `main_flags.go` groups the flags of the
  health listener, FUSE mount, leader balance, TSO and snapshot offload.
  Each area's struct (`filesystemFlags`, `leaderBalanceFlags`, `tsoFlags`,
  `snapshotOffloadFlags` with its nested `snapshotRetentionFlags`)
  registers itself on a `*flag.FlagSet` and sits next to the wiring that
  reads it. `main()` passes `cliFlags` to `run()`, which hands each area
  its own value. `TestCLIFlagsReadOnlyByMain` fails if any other file
  reads `cliFlags`. Tests build values with `defaultFlags[T]()`, so the
  TSO and snapshot-offload tests no longer overwrite process globals.
  Still package globals: listener addresses, Raft and shard topology,
  admin HTTP/gRPC and node auth, split migration, backups, encryption and
  KEK, keyviz/auto-split, and the bootstrap peer-config dir.
- Snapshot-offload wiring can now move (M4): its helpers take
  `snapshotOffloadFlags` instead of reading about twenty flags.

## Decisions

- **Internal packages, not exported library packages.** These packages
  serve the server binaries in this module, so they go under
  `internal/`, and their APIs can keep changing.
- **Move code without rewriting it.** A moved function keeps its body.
  Only identifiers are exported, and signatures change only where M2
  makes inputs explicit, so reviewers can follow a move with
  `git diff --color-moved`.
- **Narrow inputs instead of shared types.** An extracted area declares
  the small interface or config it needs (for example
  `leaderbalance.Group`) and `main` adapts its runtime to it. This lets an
  area leave `main` without first moving `raftGroupRuntime`, which embeds
  bootstrap types and would otherwise drag the bootstrap area along.
- **No design changes inside a milestone.** If a move exposes a defect,
  fix it in a separate PR with a failing test first, per the repository
  convention.

## Verification

For every milestone:

- `go build ./...`, `go vet`, `make lint`
- `go test -race` for the root package and each package that gains code
- Jepsen is not required for M1 and M2, which leave start-up order and
  runtime paths unchanged. M3–M6 touch Raft/encryption/bootstrap start-up
  wiring, so they run `./scripts/run-jepsen-local.sh`, plus `--encrypted`
  for M5 and M6.

## Risks

- **Test churn.** Most root tests reach unexported helpers. Moving those
  tests next to the code keeps that access. Tests that exercise
  several areas at once stay in `main` and use the new exported APIs.
- **Import cycles.** `internal/noderuntime` must not import any of the
  area packages. M3 lands before M4 so a cycle shows up as a compile
  error early.
- **Merge conflicts with in-flight branches** (serializable audit
  stack). Each milestone is kept small and rebased right before it is
  merged.
