# Resolved review findings

Status: Proposed
Author: bootjp
Date: 2026-10-26

## Problem

PR #1337 has a completed zero-finding review at HEAD, but a resolved finding
from an older SHA remains mapped to HEAD. The updater vetoes it indefinitely.
PR #1335 demonstrated that an unresolved remapped finding must still veto a
conflicting clean summary. Neither REST commit binding alone establishes whether
an old finding remains open.

## Decision

Read paginated GraphQL review threads and match their root comment database IDs
to REST inline comments. Ignore a remapped finding only when its original SHA is
valid, differs from HEAD, and GitHub explicitly marks its thread resolved.
Current-HEAD original findings still veto approval even if resolved. Unresolved,
unknown-resolution, and malformed-origin findings retain the conservative veto.
A completed zero-finding review at HEAD is independently required; resolving a
thread cannot itself establish approval. Re-read resolution evidence after writes
alongside the existing HEAD and review rechecks. API failures revoke approval.

No manual label addition is used. The privileged workflow continues to execute
default-branch code. Merge is required before the deployed CI can use this fix.

## Validation

Cover resolved old versus original-HEAD findings, unresolved/unknown threads,
both reviewers, pagination, API failure, and resolution changing around a write.
Replay #1337 and #1335 evidence to establish that the former can approve while
the latter cannot approve with an unresolved mapped finding.

Five regressions fail before implementation; all 98 tests pass afterward.
Actual #1337 REST and GraphQL evidence changes from rejected to approved, while
removing the resolved root IDs still rejects it. Missing resolution, mismatched
root IDs, bad pagination, API failure, and reopening during writes fail closed.
Actionlint and git diff --check pass. The existing review/comment events and
hourly/manual reconciliation read current resolution state; thread resolution
alone is not assumed to trigger an Actions workflow.

## Self-review

1. Data loss: only label evidence changes; unrelated labels are preserved.
2. Concurrency: resolution is re-read during the post-write evidence check;
   reopening in that interval revokes approval. Event delivery remains asynchronous.
3. Performance: GraphQL is queried only for trusted remapped root comments;
   threads are paginated with only root ID and resolution fields requested.
4. Consistency: original-HEAD findings always veto; resolved old findings only
   stop vetoing and cannot establish approval without a fresh clean HEAD review.
5. Coverage: both bots, missing state, root ID mapping, pagination, API errors,
   actual evidence replay, and post-write reopening are verified.
