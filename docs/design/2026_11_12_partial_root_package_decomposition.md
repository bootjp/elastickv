# Root package decomposition

Status: Partial (M1, M2, M3 and part of M4 implemented)
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
- Leave only `main.go` in the root package: `main()`, the flag
  registration call, the `adminBuildVersion` link variable and a thin
  `run()` that delegates to `internal/server`. Every other root file,
  production or test, moves to a package whose allowed importers the
  import guard enforces.
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

Layers, bottom to top (each layer may import only the layers below it):

```text
libraries            kv, store, adapter, distribution, internal/encryption,
                     internal/raftengine[/etcd], raftbootstrap, raftadmission, ...
node layer           internal/noderuntime   group spec, runtime config, peer config, GroupRuntime core
                     internal/nodeauth      node/admin gRPC auth flags, token files, interceptors
leaf components      internal/adminbridge, internal/leaderbalance, internal/fsmount,
                     internal/snapshotoffloadwiring, internal/tsowiring, internal/serverwiring
encryption wiring    internal/encryptionwiring   KEK loading, start-up guards, registration,
                                                 rotation, fan-out, raft-envelope runtime
protected bootstrap  internal/clusterbootstrap   `elastickv bootstrap ...` CLI, protected
                                                 bootstrap runtime, startup gate, GroupRuntime
                                                 with its bootstrap attachments
server assembly      internal/server        shard-group builder, distribution start-up, raft
                                            and public servers, admin, shutdown, monitoring,
                                            run() body, server flag aggregation
root                 main.go                main(), flag registration, thin run()
```

Split-migration peer clients, membership gates and the readiness gate
implement interfaces that `adapter` declares, so they live in `adapter`
next to those interfaces rather than in a feature-named package.

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
  the shard-group builder. It lands in two PRs:
  - **M3a — configuration.** `groupSpec`, the shard-range and address-map
    parsing, the HT-FIFO partition map, `runtimeConfig` and its parsing,
    and `raftBootstrapConfig` with the bootstrap / join peer resolution.
    This is pure configuration that every area reads.
  - **M3b — runtime.** The group runtime core moves as
    `noderuntime.GroupRuntime`: engine, store, state machine, transport
    hooks, `Close`, the engine-type data-dir marker checks and
    `BuildRuntimeForGroup`. `raftGroupRuntime` stays in `main` as a thin
    wrapper that embeds `*noderuntime.GroupRuntime` and adds the three
    protected-bootstrap fields (`bootstrapDomain`, `bootstrapPeerConfig`,
    `bootstrapNetwork`); those leave with the bootstrap area (M6). The
    shard-group builder also stays in `main`: it composes the encryption
    applier, the protected bootstrap specs and the protected TSO group,
    so it moves with M5/M6.
- **M4 — Mid-sized areas.** Move leader balance, filesystem mounting,
  snapshot-offload wiring, TSO wiring and node gRPC auth
  (`internal/nodeauth`); re-home the split-migration peer code into
  `adapter`; encode every extracted package in the import guard. Each
  area may be its own PR. The startup gate moves with M6 because it
  holds the bootstrap domain.
- **M5 — Encryption wiring.** M5a cuts the two encryption → bootstrap
  edges in place (the registration source becomes an interface owned by
  encryption; the capability fan-out takes a dial-option provider) and
  moves assembly code out of encryption-named files. M5b moves the
  encryption wiring to `internal/encryptionwiring`.
- **M6 — Bootstrap.** M6a moves assembly code out of bootstrap-named
  files and folds the startup gate and `raftGroupRuntime` into the
  bootstrap set. M6b moves the bootstrap operator CLI and runtime to
  `internal/clusterbootstrap`, with `raftGroupRuntime` as
  `clusterbootstrap.GroupRuntime`. An optional M6c splits the offline
  operator CLI into a subpackage.
- **M7 — Server assembly and thin root.** Move every remaining root file
  (shard-group builder, distribution start-up, raft and public servers,
  admin, shutdown, monitoring, the `run()` body, the flag aggregation and
  the root tests) to `internal/server`, leaving `main.go` alone in the
  root, and make the import guard allow the root to import only
  `internal/server`.

## Progress

- M1: admin bridges moved to `internal/adminbridge`.
- M4 (partial): the leader-balance scheduler moved to `internal/leaderbalance`
  and FUSE mounting to `internal/fsmount`. Both see the node through narrow
  inputs (a `Group` interface with `GroupID` / `SnapshotEngine`, and a resolved
  `fsmount.Config`) rather than `*raftGroupRuntime` or flag globals, so
  neither waited for M3.
- M2: `serverFlags` in `main_flags.go` groups the flags of the
  health listener, FUSE mount, leader balance, TSO and snapshot offload.
  Each area's struct (`filesystemFlags`, `leaderBalanceFlags`, `tsoFlags`,
  `snapshotOffloadFlags` with its nested `snapshotRetentionFlags`)
  registers itself on a `*flag.FlagSet` and sits next to the wiring that
  reads it. `main()` passes `cliFlags` to `run()`, which hands each area
  its own value. `TestCLIFlagsReadOnlyByMain` fails if any other file
  reads `cliFlags`. Tests build values with `defaultFlags[T]()`, so the
  TSO and snapshot-offload tests no longer overwrite process globals.
  Part 2 adds the admin HTTP listener with its keyviz fan-out
  (`adminHTTPFlags`), live backup (`backupFlags`, carried to the Admin
  service through `adminBackupDependencies`), and the keyviz / auto-split
  flag groups that `internal/serverwiring` already defined, which now hang
  off `serverFlags` instead of package globals.
  Part 3 adds node auth (`nodeAuthFlags`: Admin gRPC token and insecure
  mode, split-migration token and enforcement, peer gRPC TLS, S3 blob peer
  token). The split-migration readiness gate becomes a method on it; the
  distribution server, background runners and raft gRPC servers receive
  its method value (the raft servers through `adminGRPCInterceptors`),
  and a missing gate fails closed. The protected-bootstrap check that the
  startup token differs from the service tokens gets the token files
  through `raftBootstrapConfig`, so it still runs before any group opens.
  Part 4 adds encryption (`encryptionFlags`: sidecar path, opt-in,
  rotate-on-startup, KEK file and URI). The KEK load, start-up guards,
  KEK preflight and EncryptionAdmin mutator gate take it as a parameter.
  Protected bootstrap admission only needs two facts from it (encryption
  configured, rotation requested), so those travel as
  `bootstrapEncryptionRequest` in `raftBootstrapConfig` and are kept on the
  bootstrap domain for WAL recovery. The bootstrap e2e fixtures pass the
  request instead of overwriting process globals.
  Part 5 adds the client-facing listeners (`listenerFlags`: Redis and its
  Lua pool size, DynamoDB, S3 and SQS addresses, regions and credential
  files, and the metrics and pprof endpoints with their tokens). Runtime
  config parsing, the server runner and the admin listener (which reuses
  the S3 credentials file) receive it as a value.
  Part 6 adds the node's Raft identity (`raftFlags`: address, Raft ID,
  engine, data dir), the cluster layout (`topologyFlags`: Raft groups,
  shard ranges, Raft-to-protocol address maps, HT-FIFO partition map) and
  the cross-group ordering rollout gates (`consistencyFlags`:
  `commitTSFence`, `readWatermarkAdvance`). The runtime-input resolvers
  take `*serverFlags`.
  Part 7 adds the bootstrap and join flags (`raftBootstrapFlags`:
  `raftBootstrap`, `raftBootstrapMembers`, `raftGroupPeers`,
  `raftJoinMembers`, `raftJoinAsLearner`, `raftBootstrapPeerConfigDir`).
  The peer-config dir and learner expectation travel in
  `raftBootstrapConfig` to protected-domain loading and the shard-group
  builder, including the dedicated TSO group. The protected-bootstrap e2e
  fixtures pass the dir instead of overwriting a process global.
  With part 7 the root package registers no flag as a package global:
  every server flag hangs off `serverFlags`, and M2 is complete.
- M3a: `internal/noderuntime` holds `GroupSpec`, `RuntimeConfig` and
  `ParseRuntimeConfig` (Raft groups, shard ranges, leader address maps,
  HT-FIFO partition map), `BootstrapConfig` with `ResolveRaftPeerConfig`
  and `BootstrapSettingsForGroup`, and `BootstrapEncryptionRequest`. The
  root package keeps only the flag structs and the resolver that feeds
  them in. The package is restricted to binaries in the import guard, and
  its tests moved with it (`shard_config_test.go`, the peer-resolution
  tests, the DynamoDB leader-map and SQS resolver tests).
- M3b: `noderuntime.GroupRuntime` holds the engine (behind `EngineMu`,
  read by long-lived goroutines through `SnapshotEngine`), store, state
  machine and transport hooks, with `Close` and `RegisterGRPC`.
  `BuildRuntimeForGroup`, `GroupDataDir`, `RaftEngineType` and the
  data-dir marker checks moved with it. `main`'s `raftGroupRuntime`
  embeds it and keeps only the bootstrap attachments.
  `leaderbalance.Group` now asks for `SnapshotEngine()` instead of
  `Engine()`: a wrapper method named `Engine` would shadow the promoted
  `Engine` field, and `SnapshotEngine` already reads the engine under the
  lock the same way.
- M4, snapshot offload: the start-up wiring moved to
  `internal/snapshotoffloadwiring` (a sibling of the `snapshotoffload`
  library rather than part of it, so flag and start-up code stays out of
  the library and clear of its test fakes). It owns `Flags` /
  `RetentionFlags`, endpoint and spool validation, the TLS-enforcing HTTP
  client, store construction, stale-spool cleanup and the scheduler and
  retention workers. `Start` takes `[]snapshotoffload.OffloadGroup` and the
  build version; `main` keeps only `snapshotOffloadGroups`, which reduces
  the runtimes to offload groups with their leadership callbacks. The
  package is restricted to binaries in the import guard.
- M4, split migration: the capability gate and peer probe, the promotion /
  migration / group / voter / apply-member gRPC client factories, the
  membership and namespace admission gates, the ownership-refresh runner,
  the readiness gate and the gRPC coverage interceptor moved to `adapter`
  (`adapter/split_migration_peer_*.go`), next to the interfaces they
  implement; `adapter` gains an `internal/raftadmin` import. (An earlier
  revision of this PR put them in a feature-named
  `internal/splitmigration` package; that fails the extraction criterion
  because every export took or returned an `adapter` type.) `main` keeps the glue that needs the runtime
  or protected-bootstrap types: the capability peer source over runtimes,
  the Internal protocol options, the ownership-refresh root resolver and
  option, the membership interceptor (which adds the prepare-evidence
  gate and bootstrap dial options), and the two `nodeAuthFlags` readiness
  methods. The namespace gate takes the node-ID derivation as a function
  so the package does not import the concrete etcd backend, and the
  uncached peer dial helper moved to `kv.NewGRPCClient` next to the
  connection cache that already dials the same way.
- M4, TSO: the timestamp-oracle wiring moved to `internal/tsowiring`
  (its own package rather than `internal/serverwiring`, whose tests would
  collide with the moved fakes). It owns `Flags`, mode and mode-file
  validation, the legacy local bridge, the dedicated allocator with leader
  routing and the runtime mode controller, `Wiring.AuthorizeActivation`,
  the mode reloader and the HLC lease renewal helpers. `Configure` takes the
  dedicated group ID and group explicitly plus extra routing options;
  `main`'s `configureCoordinatorTSOWithObserver` looks the group up by
  `noderuntime.DedicatedTSORaftGroupID` and turns the protected-bootstrap
  networks into those routing options.

- M4, node auth: `internal/nodeauth` holds the node / admin gRPC
  authentication: `Flags` (admin token, insecure mode, split-migration
  token and enforcement, peer gRPC TLS, S3 blob peer token) with
  `ServiceTokenFiles`, `LoadGRPCAuth` and the split-migration readiness
  gates, the token-file loaders and validators, `ConfigureGRPCTransport`,
  the bearer-token interceptor helper and `AdminGRPCInterceptors`. It
  imports only `internal` and `adapter`. Its unit tests moved with it;
  the wiring tests that build the admin service or call `run()` stay in
  `main`.

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
- **Extraction criterion.** A package is a real cut when it imports only
  lower layers, takes narrow inputs (no runtime or bootstrap wrapper
  types), and the code left above it only adapts the runtime to those
  inputs. Code with wide inputs is assembly and belongs in
  `internal/server`, not in a per-feature package.
- **Implementations live with the interface owner.** Default
  implementations of interfaces declared by a library package (for
  example the split-migration client factories and membership gates for
  `adapter`) live in that package. This is the one exception to "internal
  packages": `adapter` gains them because it owns their interfaces.
- **`<lib>wiring` siblings.** An `internal/<lib>wiring` package is
  justified only when it imports just that library (plus leaves) and
  exposes `Flags` and one `Start`/`Configure` (`snapshotoffloadwiring`,
  `tsowiring`, `encryptionwiring`).
- **Dependency direction.** noderuntime / nodeauth → encryptionwiring →
  clusterbootstrap → server → root. An edge that points the other way is
  inverted with an interface or callback owned by the lower package.
- **Exported fields on moved types.** Types moved as-is may export their
  fields, including a mutex (as `noderuntime.GroupRuntime.EngineMu`
  does); encapsulating them is a separate, non-move change.
- **`adminBuildVersion` stays in `package main`** because release builds
  set it with `-ldflags -X main.adminBuildVersion=...`; `main` passes it
  down.
- **Every extracted package is guarded.** Each one has a
  `restrictedPackages` entry naming the layers that may import it, and
  the root may import only `internal/server` once M7 lands.

## Verification

For every milestone:

- `go build ./...`, `go vet`, `make lint`
- `go test -race` for the root package and each package that gains code
- `-help` output (and `elastickv bootstrap` help from M6 on) is
  byte-identical to the base, and the set of test names is unchanged
  apart from documented guard tests
- `./scripts/check-production-imports.sh`
- Jepsen is not required for M1 and M2, which leave start-up order and
  runtime paths unchanged. M3–M7 touch Raft/encryption/bootstrap start-up
  wiring, so they run `./scripts/run-jepsen-local.sh`, plus `--encrypted`
  for M5–M7.

## Risks

- **Test churn.** Most root tests reach unexported helpers. Moving those
  tests next to the code keeps that access. Tests that exercise
  several areas at once stay in `main` and use the new exported APIs.
- **Import cycles.** `internal/noderuntime` must not import any of the
  area packages. M3 lands before M4 so a cycle shows up as a compile
  error early.
- **End-to-end fixtures span every layer** (the bootstrap e2e fixtures
  build shard groups, the distribution catalog and the raft servers), so
  they stay in the assembly package; tests move to the lowest package
  that owns everything they touch.
- **CI test shards** (`scripts/go-test-shard.sh`) name the root package;
  M7 points the root shard at `internal/server`.
- **Merge conflicts with in-flight branches** (serializable audit
  stack). Each milestone is kept small and rebased right before it is
  merged.
