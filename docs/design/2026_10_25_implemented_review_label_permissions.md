# Review label permissions

Status: Implemented
Author: bootjp
Date: 2026-10-25

## Goal and evidence

Restore label reconciliation for pull requests with completed zero-finding
reviews at HEAD. PR #1331 satisfies the evidence checks, but run 40112872960
failed to add its label with HTTP 403. The job had issues/write and
pull-requests/read; creating the repository label succeeded.

## Decisions

Grant pull-requests/write to the trusted default-branch updater. Retain
issues/write for repository label creation and contents/read for checkout.
Do not change approval criteria, checkout source, or the read-only relay.
Before deployment, reconcile #1331 using the existing helper and the
maintainer's authenticated CLI, preserving all HEAD/evidence rechecks.

## Validation and milestones

- [x] Validate workflow syntax and the existing reconciliation suite.
- [x] Verify #1331's label through the same reconciliation helper.
- [x] Merge the permission change (#1332) and verify a live Actions write (`pull-requests: write` is in `review-approval-label.yml`; the updater runs successfully on `main`, e.g. run 40666978702).

The live Actions failure is the reproduction: POST issues/1331/labels returns
403 despite a completed clean review. Workflow syntax checks cannot establish
runtime authorization; a successful Actions write after merge is still required.

All 83 Node tests pass, actionlint passes for the updater, relay, and review
workflows, and git diff --check passes. The existing reconciler applied the label
to #1331 at b28f3030089601c0e47dae053efb2d9263b98457 through the authenticated CLI,
then re-read the HEAD and evidence successfully. A separate API read confirmed it.

## Self-review

1. Data loss: only workflow permissions and documentation change; no storage paths.
2. Concurrency: existing serialized execution and HEAD/evidence rechecks remain.
3. Performance: no additional CI steps or API calls in the workflow.
4. Consistency: approval evidence and trusted default-branch checkout remain intact.
5. Coverage: existing 83 tests and syntax checks pass; live Actions authorization
   remains a deployment check, rather than a claim established by unit tests.
