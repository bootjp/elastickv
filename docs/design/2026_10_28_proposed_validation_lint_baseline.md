# Restore the full validation lint baseline

Status: Proposed
Author: bootjp
Date: 2026-10-28

## Context

The test-suite cleanup design requires a clean lint result. On main
`64fc9e62cda6d6a9f38fbc9e2bebb4ea0430ba23`, unfiltered golangci-lint v2.15.0
reports eight findings even though differential CI is green. The WP-1 and WP-3
PRs must not be called fully accepted by treating differential lint as a full
repository pass. This bounded maintenance slice removes that prerequisite gap.

The work is separate from the test-cleanup WPs, whose production edits are
limited to removing orphaned code. It does not implement new filesystem
behavior or supersede PR #1344's journal accounting investigation.

## Scope

1. `internal/filesystem/fstest/memory.go`: separate mutation construction and
   commit-timestamp patch validation from the serialized dispatch orchestration.
   Preserve the timestamp choice, copied bytes, accepted operations, OCC read
   keys, and timestamp-offset bounds. Do not replace this fake with a different
   dispatcher whose optional capabilities change test coverage.
2. `internal/filesystem/fuseadapter/adapter.go`: extract journaled-write handling
   and share the repeated unlink/rmdir lifecycle. Preserve operation-specific
   capability assertions: a core implementing only one operation must still
   support that operation. Check capability/session validity before allocating
   an operation ID. Keep mutation admission, handle refresh, receipts, unknown
   outcome handling, errors, and diagnostic logging in their current order.
3. `internal/filesystem/operation_journal_exec.go`: consolidate repeated attempt
   identity validation or another existing publication invariant without moving
   read validation, route checks, snapshot acquisition, proof construction,
   pending accounting, commit, or duplicate resolution across each other.
4. `kv/fsm.go`: split the raw apply verdict predicate into named protocol
   categories, preserving the initial `errTargetReadinessUnproven` exclusion and
   the exact accepted sentinel set and evaluation order. Operational errors
   must still halt the entry rather than becoming per-request verdicts.
5. `kv/shard_store_prepare_evidence_test.go`: extract the existing migration
   protection case body, retaining every enabled/tombstone/control-key case,
   all whole-batch rejection/state-preservation assertions, and the successful
   reuse of the rejected batch sequence for ordinary data.
6. `internal/filesystem/operation_journal.go`: remove the unused `extra` argument
   from `storeRecordAndSession` and audit every caller. They currently all pass
   nil. Keep the general `storeRecordAndSessionAtRoute` effects argument because
   publication uses it; do not change effect or OCC key order.

## Invariants and Non-goals

- No wire/on-disk formats, timestamp issuance, authorization policy, membership,
  retries, error classification, transaction semantics, or default flags change.
- No new lint exclusions, `nolint` directives, disabled checks, test skips,
  iteration reductions, or weakened assertions.
- Do not duplicate or merge PR #1344's trace and scheduled-workflow changes.
- Add focused parity/boundary tests where an extracted decision needs a direct
  guard. Preserve existing integration and real-Raft safety tests unchanged.
- This slice does not close the serializable audit, TSO slot claims, TTL legacy
  migration, or any other unfinished design.

## Acceptance

1. Unfiltered `golangci-lint run ./... --timeout=10m` on v2.15.0 reports zero
   issues, without a differential filter or suppressions.
2. Race tests cover the full filesystem subtree, KV, and root packages. Include
   existing journal, cancellation, unknown-outcome, mutation barrier, receipt,
   snapshot, and imported-control-row regressions; do not substitute short mode.
   Kernel FUSE checks additionally require the Linux CI environment.
3. Focused tests pin the raw verdict allowlist/exclusion, timestamp patch
   boundaries/copying, and FUSE partial-capability/session/admission behavior.
4. Inspect the caller audit and diff for preserved ordering before publishing.
   Latest-HEAD review and CI must pass before claiming the PR ready to merge.

## Rollout

One independent PR, with this design committed before implementation. Review
the behavior-preservation plan first. Existing WPs can reference its full lint
result once the relevant changes are incorporated; they must not retroactively
claim a clean baseline for their earlier commits.
