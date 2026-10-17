# Production code commonization: coordinator wrappers, admin authorization, S3 paths, backup mirrors, server wiring

Status: Implemented (all milestones, PR #1277)
Author: bootjp
Date: 2026-10-16

## Context

The 2026-10-16 code audit (see `2026_10_16_proposed_test_suite_cleanup.md` for the test-side findings) found that most production duplication is small. The valuable cases are the ones where two hand-maintained copies of the same logic have already drifted apart and changed behaviour. This document covers five of them. Each milestone below fixes the drift by making both call sites use one implementation, and pins the result with tests that fail on the current code where a behaviour defect exists.

| # | Duplication | Drift already visible |
|---|---|---|
| M1 | `startupGatedCoordinator` (`main.go`, 33 forwarding methods) and `keyVizLabeledCoordinator` (`kv/keyviz_label.go`, 27) wrap a `kv.Coordinator` and hand-forward optional capabilities. | They forward different method sets. In production, every adapter's coordinator is `WithKeyVizLabel(startupGatedCoordinator{sharded})`. The gate does not forward `LocalLeaderGroupIDs`, and the label wrapper returns nil when its inner value lacks it. So the Redis delta compactor's TTL-inline migration builds zero scan plans and never runs (regression since `a36dac7f`). The label wrapper also lacks `EnsureMutationsWriteAllowed`, `ValidateDurableTimestamp`, `PhaseD*`, `ObserveForwardedRequests` and `RaftMembers*`; the gate lacks `LinearizableReadForKey`. |
| M2 | Six `principalFor*` helpers in `internal/admin` (DynamoDB write and item read, S3 write and object read, SQS sensitive read and write). | DynamoDB and S3 require both the JWT role and the live role. SQS checks only the live role when a role store is configured, so a JWT minted `read_only` is allowed to write after a live promotion. `s3_handler.go` documents that exact behaviour as a security regression (Codex P2 and CodeRabbit on PR #669). SQS also answers a missing session principal with 500 where the others answer 401. |
| M3 | S3 `ListObjectsV2` (`adapter/s3.go`) and `AdminListObjects` (`adapter/s3_admin_objects.go`) are line-for-line copies; `adminPutObjectStream` re-implements the PUT pipeline. | When a continuation token's read timestamp has been compacted, the bucket-metadata load returns `ErrReadTSCompacted`. The admin copy translates that to an invalid-token error (Codex r2 P2 on PR #811), but SigV4 still answers 500 `InternalError`. Admin PUT skips admission control, blob offload, chunk references and the shared cleanup path that SigV4 PUT uses. |
| M4 | `internal/backup` re-implements adapter encoders (DynamoDB ordered numbers and keys, SQS keys and FIFO dedup records, Redis string/HLL value headers), so the offline tools link no live-cluster code. | The logical-encoder design (`2026_05_25_implemented_snapshot_logical_encoder.md`) promises a byte-identical cross-check against the live adapter for each copy. Only the store-classifier mirror has one. The "links no live-cluster code" rule is already broken: `internal/backup/redis_stream.go` imports `store` (since `5ef05d4d`), which pulls Pebble and `internal/encryption` into every backup CLI, and no test caught it. |
| M5 | `cmd/server/demo.go` re-implements about 370 lines of root `main`: autosplit flags/config/validators/scheduler/splitter, KeyViz sampler, metrics/pprof servers, and SigV4 credential loading. | The demo's sampler ignores `--keyvizLabelsEnabled`, and its credential loader accepts duplicate access-key IDs (root rejects them, pinned by `main_s3_test.go`). Both autosplit route converters drop `StagedVisibilityActive`, `MigrationJobID` and `MinWriteTSExclusive`, which `distribution`'s own converter copies; this is latent today. |

## Goals

- One implementation per duplicated behaviour, used by every call site.
- For every drift that is a behaviour defect, a test that fails on `main` first, in the same commit series as the fix (CLAUDE.md).
- Guards that stop the copies drifting again: a capability-parity test for coordinator wrappers, an import-boundary test for `internal/backup`, and a cross-check test per backup mirror.

## Non-goals

- Merging `kv.Coordinate` and `ShardedCoordinator`. Their timestamp-issuance semantics differ on purpose (audit gap G10 / fix A0b).
- Moving the backup mirrors into shared leaf packages. M4 adds the cross-checks the design already requires; the move is a later step once the checks exist.
- New S3 or admin features.

## M1: One forwarding base for coordinator wrappers

**Design.**
- Add an exported `kv.CoordinatorForwarder` struct (`Inner Coordinator`) in a new `kv/coordinator_forwarder.go`. It implements `Coordinator` and every optional capability that `*ShardedCoordinator` exposes to adapters, by forwarding to `Inner`.
- When `Inner` lacks a capability, each method falls back exactly as the two wrappers do today. Examples:
  - `EnsureMutationsWriteAllowed` returns nil;
  - `RaftMembers` returns `ErrLeaderNotFound`;
  - `LeaseReadForGroup` falls back to `LeaseReadThrough`;
  - `LocalLeaderGroupIDs` returns nil.
- Errors are wrapped with `errors.WithStack`, which preserves `errors.Is`/`As` and gRPC status extraction. This replaces the gate's ~20 `//nolint:wrapcheck` pass-throughs.
- `startupGatedCoordinator` embeds the forwarder and keeps only its interceptions: `Dispatch`, `Next` and `NextAfter` refuse while the startup gate is closed, and `TimestampAllocator` returns the gate itself.
- `keyVizLabeledCoordinator` embeds it and keeps only its interceptions: `Dispatch` stamps the label; `Next`, `NextAfter`, `RecoverHLCLease`, `LinearizableReadForKey`, `LeaseReadForKey` and `LeaseReadForGroup` put the label on the context.

**Behaviour change.**
- `LocalLeaderGroupIDs` now reaches the sharded coordinator through both wrappers, so the TTL-inline migration scans the local leader groups again.
- The label wrapper gains the capabilities only the gate had, and vice versa; each forwards to the real implementation instead of failing a type assertion.

**Tests.**
1. Failing first: build `WithKeyVizLabel(startupGatedCoordinator{inner: <coordinator with LocalLeaderGroupIDs>})` and assert the group IDs survive. Also assert that `DeltaCompactor.ttlInlineMigrationScanPlans` returns one plan per local leader group through that chain.
2. Parity guard: by reflection, list every exported method of `*kv.ShardedCoordinator` that appears in a kv capability interface, and assert both wrappers expose it. Methods that deliberately stay unforwarded are listed explicitly in the test, so a new capability added to `ShardedCoordinator` without forwarding fails CI.
3. The existing gate tests (`main_internal_write_gate_test.go`, `main_tso_routing_test.go`) and keyviz-label tests must keep passing unchanged.

## M2: One admin authorization helper

**Design.**
- Add `authorizePrincipal(w, r, roles RoleStore, access roleAccess, deny string) (AuthPrincipal, bool)` in `internal/admin/authorize.go`. `roleAccess` is read or write.
- Policy (the one DynamoDB and S3 already use):
  1. A missing session principal → 401.
  2. The JWT-embedded role must allow the access → 403 otherwise.
  3. When a `RoleStore` is configured, the live role must exist and allow the access → 403 otherwise.
  4. The principal is passed downstream with the live role.
- All six `principalFor*` helpers become one-line calls, keeping their per-endpoint deny messages.
- `ForwardServer.validatePrincipal` is unchanged. Forwarded requests carry no JWT and are re-validated against the leader's live role.

**Decision.** The JWT freezes the role at login, and the live role can only narrow it. That is the documented policy (PR #669 review), it is the least-privilege option, and two of the three handler families already follow it. As a result, a key promoted from `read_only` to `full` must log in again before SQS writes succeed, as it already must for DynamoDB and S3.

**Behaviour change (SQS only).**
- JWT `read_only` with live `full` → 403 on delete queue, set queue attributes and purge (previously allowed).
- JWT without read with live read → 403 on peek (previously allowed).
- Missing principal → 401 (previously 500).

**Tests.**
- Failing first: SQS write and sensitive read reject a JWT role that does not allow the access even when the live role does.
- `TestSqsHandler_DeleteQueue_LivePromotion` pins the old policy; it is rewritten to assert the unified one.
- A table test runs `authorizePrincipal` over {JWT role} × {live role, missing, no store} × {read, write}.

## M3: Shared S3 listing core and PUT pipeline

**Listing.**
- `scanS3ObjectListing(ctx, s3ObjectListingRequest) (s3ObjectListingPage, error)` owns everything both paths share:
  - token decode and validation;
  - the read-timestamp choice;
  - the bucket-metadata load, including the `ErrReadTSCompacted` → expired-token translation;
  - the read pin;
  - the scan loop with delimiter collapsing and next-token encoding.
- It returns neutral entries: an object key with its manifest, or a common prefix.
- Typed sentinels distinguish a malformed token, a token that does not match the request, an expired token and a missing bucket.
- `listObjectsV2` maps the page to the XML result and the sentinels to S3 errors (400 `InvalidArgument`, 404 `NoSuchBucket`). `AdminListObjects` maps them to the admin listing and the existing `ErrAdmin*` sentinels.
- `start-after` stays SigV4-only, as a request field the admin path leaves empty.

**PUT.**
- `prepareS3PutObject`, `uploadS3ObjectData` and `commitS3PutObject` stop reading `*http.Request` directly. They take an `s3PutObjectInput` with the precondition headers, content headers, user metadata, expected payload hash and admission protocol, built by a small adapter for the SigV4 request.
- `adminPutObjectStream` becomes a call into the same pipeline with an admin input:
  - no preconditions;
  - its content type;
  - its 100 MiB cap;
  - its admission check, which returns `errS3PutAdmissionExhausted` instead of writing an HTTP response; the admin bridge maps it to a retryable 503.
- Admin uploads therefore get admission control, blob offload (when enabled), chunk references, and the same cleanup on failure as SigV4 uploads.

**Behaviour change.**
- SigV4 `ListObjectsV2` with a compacted continuation token answers 400 `InvalidArgument` "continuation token has expired" instead of 500.
- Admin uploads are subject to the PUT admission limiter and blob offload.

**Tests.**
- Failing first: SigV4 `ListObjectsV2` with a continuation token whose read timestamp is below the store's `MinRetainedTS` returns 400, not 500.
- Admin PUT respects an exhausted admission limiter, and writes offloaded chunks when offload is enabled.
- The existing SigV4 and admin listing/PUT tests keep passing; they now exercise one implementation.
- The removed `//nolint:cyclop,gocognit,gocyclo,nestif` directives stay removed.

## M4: Backup mirror cross-checks and import boundary

**Design.**
- `internal/backup` must not depend on `store`, `adapter`, `kv`, `distribution`, `internal/encryption` or Pebble. `proto` and the leaf key packages (`internal/s3keys`) remain allowed.
  - A test in `internal/backup` walks the package's first-party import closure with `go/build` and fails on any forbidden package.
  - `redis_stream.go` gets a local copy of `store.StreamMetaTrimBinarySize`, pinned against `store` by a cross-check in `internal/backup`'s own tests (the existing `snapshot_reader_live_test.go` pattern).
- Adapter-side cross-check tests (package `adapter`, which already imports `internal/backup`) build the same logical object with the live adapter builders and with the backup encoder. They decode the backup output with `backup.DecodeLiveEntries` and assert byte-identical keys and values for:
  - DynamoDB: table meta, item keys with string, binary and number hash/range keys (including negative numbers, zero and exponents, through the ordered-number encoding and canonicalisation), and GSI rows;
  - SQS: classic and partitioned queue meta, message data, visibility, by-age and FIFO dedup records;
  - Redis: string and HLL value headers.

**Behaviour change.** Backup CLIs no longer link `store`, Pebble or `internal/encryption`. Dump output is unchanged.

**Tests.** The boundary test fails on `main` (because of `redis_stream.go`), and the cross-checks pin every mirror the design claims is checked.

## M5: Shared server wiring for `cmd/server` and root `main`

**Design.** A new `internal/serverwiring` package, imported by both `main` packages, owns:
- `AutoSplitFlags`:
  - registration on a `*flag.FlagSet`;
  - `SchedulerConfig(leader)`, `Validate()` and the explicit-flag bookkeeping that today is `recordExplicitRuntimeFlags` / `recordExplicitDemoFlags`;
  - `StartScheduler` and the distribution splitter.
- `KeyVizFlags` and `BuildSampler` / `SeedRoutes` / `StartFlusher`, honouring `--keyvizLabelsEnabled`.
- `PrepareMetricsServer` and `PreparePprofServer` (address and token validation).
- `LoadSigV4StaticCredentials`, which rejects duplicate access-key IDs.

Also:
- `distribution` exports `RouteDescriptorFromProto`, the existing complete converter, and the autosplit splitter uses it.
- Root `main` keeps its flag names and defaults. The demo keeps its own flag set, but registers the shared groups through the package.

**Behaviour change (demo only).**
- The demo honours `--keyvizLabelsEnabled`.
- The demo rejects duplicate SigV4 access-key IDs.
- Autosplit route conversion keeps all route fields.

**Tests.**
- Failing first:
  - the demo credential loader rejects a duplicate access key;
  - the demo sampler honours the labels flag;
  - autosplit route conversion preserves `MinWriteTSExclusive`, `MigrationJobID` and `StagedVisibilityActive`.
- The existing `main_autosplit_test.go`, `main_s3_test.go`, `main_metrics_test.go`, `main_pprof_test.go` and `cmd/server/demo_test.go` cases move to the package or keep passing against it.

## Rollout

Milestones are independent and land as separate commit series in the PR that carries this document: M1 and M2 first, then M3, M4 and M5. Each series:
- starts with its failing test where a defect exists;
- runs `go test -race` for the touched packages and the root package;
- passes `golangci-lint` with zero issues;
- records the five self-review lenses in the PR description.

M1 touches the Redis adapter's coordinator path, so the Redis Jepsen workload is the integration check. The CI Jepsen job covers it.

## As built

All five milestones shipped in PR #1277. Each defect fix landed as a failing-test commit followed by the fix. Differences from the plan above, and additions found during implementation:

**M1.**
- The forwarder also answers `IsLeaderForGroup` / `RaftLeaderForGroup` from the wrapped coordinator's own `IsLeader` / `RaftLeader` when that coordinator has no group-keyed leadership (a single-group `Coordinate`), the same reasoning as the `LeaseReadForGroup` fallback (CodeRabbit on PR #1277).
- The startup gate's errors now carry a stack; gRPC codes are unchanged (`status.FromError` unwraps).
- The keyviz label wrapper's `LeaseReadForGroup` falls back to a lease read instead of a linearizable read when the inner coordinator has no group-keyed lease read, matching the gate.

**M2.**
- The SQS endpoints affected are delete queue, set queue attributes, purge and peek. There is no admin redrive endpoint.

**M3.**
- AdminListObjects also returned a bare decode error for a continuation token that is not base64 JSON, which the admin handler answered with 500. The shared scan classifies it, so admin now answers 400 like SigV4.
- An exhausted PUT admission limiter surfaces as `ErrAdminUploadThrottled`; the admin API answers 503 with `Retry-After: 1`.
- Admin uploads fill chunks to the chunk size before writing them, as SigV4 uploads do, instead of writing one chunk per `Read` call.

**M4.**
- The cross-checks round-trip the records the live write paths stage through the backup decoder (`NewLiveDecoder`) and encoder (`EncodeSnapshot`, `DecodeLiveEntries`) and require byte-identical keys and values. That covers both the decoder's and the encoder's mirrors.
- Where a restore differs from live state by design, the live side is built in the documented restored state, and each test says so:
  - generation 1;
  - SQS messages never received and fully visible;
  - the FIFO dedup send timestamp sharing the record's millisecond;
  - no-TTL HLLs restoring as the legacy raw payload.
- Mutation checks on both the backup copy and the live counterpart fail every family's test.
- `elastickv-backup`, `elastickv-snapshot-decode`, `elastickv-snapshot-encode` and `elastickv-snapshot-archive` no longer link `store`, the Pebble engine or `internal/encryption`. `elastickv-snapshot-prepare-restore` still does, through `internal/raftengine/etcd`, which the restore path needs.
- #1276 merged while PR #1277 was open. It made the decoder read SST ingest bundles with `github.com/cockroachdb/pebble/v2/sstable` (`internal/backup/snapshot_sst.go`), as the decoder design records. With both on `main`, the boundary test failed on that import.
  - The test now exempts exactly that package (`backupAllowedDeps`), and fails if the exemption goes unused.
  - The Pebble engine (`github.com/cockroachdb/pebble/v2`) imports `sstable`, so it cannot come in through it. Importing the engine or any other Pebble package still fails the test.
  - The four CLIs above therefore link the sstable reader and the Pebble packages it depends on, about 5 MiB per linux/amd64 binary. Only `elastickv-snapshot-decode` and the self-test of `elastickv-snapshot-encode` reach it; `elastickv-backup` and `elastickv-snapshot-archive` carry it unused.

**M5.**
- The autosplit splitter converts SplitRange children with `distribution.RouteDescriptorFromProto`, which rejects a nil child, an unspecified state or invalid bounds. Such a response now fails the split call instead of becoming a write-fenced placeholder.
- The single-group `Coordinate` used to hand the sampler `LabelLegacy` for every observation, so the demo's new `--keyvizLabelsEnabled` produced no per-adapter rows. It now passes the adapter label stamped by `WithKeyVizLabel` and leaves the decision to the sampler's `KeyVizLabelsEnabled` option (CodeRabbit on PR #1277).
- The demo's KeyViz flags use the server binary's help text.

## Decisions

- Coordinator wrappers share one forwarding base, and a capability-parity test is the guard against future drift (M1).
- Admin writes and sensitive reads require both the JWT role and the live role (M2).
- Admin S3 uploads go through the same PUT pipeline as SigV4 uploads, including admission control and blob offload; admin keeps its own size cap (M3).
- `internal/backup`'s import boundary is enforced by a test, and every backup mirror of an adapter encoder has an adapter-side byte-for-byte cross-check (M4). The boundary keeps the Pebble engine out but allows exactly Pebble's `sstable` reader, because SST ingest bundles are Pebble sstable files (#1276).
- Shared server wiring lives in `internal/serverwiring`; the demo stays a separate binary (M5).
