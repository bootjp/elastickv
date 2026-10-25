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
