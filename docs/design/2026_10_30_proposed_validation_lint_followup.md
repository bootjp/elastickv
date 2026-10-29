# Validation lint follow-up

Status: Proposed; implementation authorized for local review only
Author: bootjp
Date: 2026-10-30
Base: c316b829ec722a2cee8af8fbd07dfd1d0b827756

## Context

The unfiltered baseline lint reports cyclomatic complexity 11 (maximum 10)
for `run` and `startRaftServers` in `main.go`. The optional
`--readWatermarkAdvance` wiring added one branch to each function. Restore
the validation baseline separately from the S3 work; this is a prerequisite,
not completion of the parent goal.

## Decisions

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

The risk is accidentally enabling the option by default, losing the wrapped
proposer, or moving startup side effects. Add focused enabled/disabled tests
through the extracted helpers and observable ShardStore/Internal behavior;
review both call sites against the base for ordering and error parity.

Run focused tests, the full root package with `go test -race -count=1 .`,
and one unfiltered `golangci-lint run --config=.golangci.yaml ./...` after
the parent lint lock is available. Record commands, original exits, logs,
and lint timing. Review data loss, concurrency/distributed failures,
performance, consistency, and coverage separately.

## Milestones

1. Commit this design before changing code.
2. Extract the two branches and add focused parity coverage.
3. Run validation and present the local diff and commit IDs to the parent.

Keep this proposed filename while awaiting parent design/diff review.
Commits use bootjp <contact@bootjp.me> with DCO sign-off. Do not push or
open a PR before parent review. Stage only intended files; report unexpected
hook edits without reverting other work.
