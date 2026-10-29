# Validation lint follow-up

Status: Implemented
Author: bootjp
Date: 2026-10-30
Base: c316b829ec722a2cee8af8fbd07dfd1d0b827756

## Context

The unfiltered baseline lint reported cyclomatic complexity 11 (maximum 10)
for `run` and `startRaftServers` in `main.go`. The optional
`--readWatermarkAdvance` wiring added one branch to each function. This
follow-up restored the validation baseline separately from the S3 work.
Its three milestones are complete; this validation prerequisite does not
complete the parent isolation goal or the separate S3 work.

## Landed Implementation

[PR #1356](https://github.com/bootjp/elastickv/pull/1356) merged into `main`
at `1912a4014b3980cb64540cb96d5548dc42443dbd`, with final reviewed HEAD
`3847493bae526a86f04227740b60ba5edd831d57`. The merge's first parent is the
base above and its second parent is that implementation HEAD.

The parent verified the live merge, code and security review, and all CI
passing for the final HEAD before this documentation update. Those are
existing implementation results, not checks rerun for this record.

## Decisions

The implementation follows the original decisions below. The private helpers
are `newStartupShardStore` in `run` and `internalReadWatermarkOptions` in
`startRaftServers`, both in `main.go`.

- Extract ShardStore construction and its optional read-watermark setting
  into a private helper in `main.go`, called at the original construction site.
- Extract the optional Internal read-watermark proposer options into a
  private helper in `main.go`, appended at the original option position.
  Resolve the proposer only when enabled, still through `proposerForGroup`
  so the shard group's encryption-aware proposer and engine fallback survive.
- Keep the exact startup order, cleanup registration, flags, defaults, errors,
  and optional proposer semantics. Do not change Raft behavior, TLS settings,
  protocol handlers, storage, or public interfaces.
- Do not add modes, packages, lint exclusions, suppressions, or broad refactors.

## Validation And Risk

The risks were accidentally enabling the option by default, losing the
wrapped proposer, or moving startup side effects. The extracted helpers
have focused enabled/disabled tests through observable ShardStore/Internal
behavior in `main_startup_options_test.go`:

- `TestNewStartupShardStoreReadWatermarkOption`: disabled reads remain
  unchanged; enabled reads propose the watermark and propagate proposal errors.
- `TestInternalReadWatermarkOptions`: disabled forwarding rejects watermark
  proposals even with an admin proposer present; enabled forwarding preserves
  engine fallback, the wrapped group proposer, commit indexes, and errors.
- `TestReadWatermarkStartupDefaultIsDisabled`: the flag remains false by
  default and disabled startup does not resolve a proposer.

The merged PR records the following existing validation evidence:

| Validation | Recorded result |
|---|---|
| Focused startup-option parity tests with the race detector | Pass |
| `go test -race -count=1 .` | Pass, exit 0; complete root package, 179.728s |
| `golangci-lint run --config=.golangci.yaml ./... --timeout=5m` | Pass, exit 0; 0 issues, unfiltered |
| `git diff --check` | Pass, exit 0 |

Jepsen was not rerun for the extraction because replication, MVCC, and
protocol semantics were unchanged. This documentation-only promotion does
not rerun Go tests, lint, or Jepsen and makes no new runtime validation claim.

## Separate Review Lenses

The merged PR records five separate review passes:

1. **Data loss:** no persistence, mutation, snapshot, or cleanup behavior
   changed.
2. **Concurrency / distributed failures:** startup order and conditional
   proposer lookup were unchanged; the root race tests passed.
3. **Performance:** the helpers are startup-only; no request-path work or
   Raft round-trips were added.
4. **Data consistency:** the same option still controls both sites; wrapped
   proposer selection, disabled behavior, errors, and TLS were preserved.
5. **Test coverage:** the tests above cover enabled/disabled reads, disabled
   forwarding with an admin fallback present, engine fallback, wrapped
   proposer selection, proposal errors, and the default flag value.

## Milestones

1. **Complete:** the design was committed before code changes in
   `ad93f4f7f9830591f8eadbc67e298bec03898fc8`.
2. **Complete:** both branches were extracted and focused parity coverage
   was added in `3847493bae526a86f04227740b60ba5edd831d57`.
3. **Complete:** validation and parent review were completed, followed by
   publication, final-HEAD review/CI, and merge through PR #1356 as recorded
   above. No implementation work remains in this bounded follow-up.
