# Review label inline findings

Status: Proposed
Author: bootjp
Date: 2026-10-25

## Problem and decision

PR #1335 retained approval despite a trusted root finding whose commit_id
matched HEAD and whose original_commit_id referred to an older revision.
The updater preferred original_commit_id and ignored the finding. A clean
summary therefore incorrectly established approval.

Treat a trusted root finding as current-HEAD evidence when either commit_id
or original_commit_id equals HEAD. Such evidence vetoes a clean summary.
Both hashes must differ from HEAD before a finding is treated as stale.
Replies and untrusted identities retain their existing handling. No formal
GitHub APPROVED state is required or introduced.

## Validation

Reproduce the old/current hash combination before changing the implementation.
Cover both trusted bots, a wholly stale finding, and revocation of an existing
label. Verify the decision against #1335's actual REST evidence. The privileged
workflow continues to execute only default-branch code; deployment requires
merging this fix. The erroneous live label is removed separately.

Validation: three regressions fail before the fix; all 87 tests pass afterward.
The actual #1335 REST evidence returns approved/zero findings before the fix and
not approved/current-HEAD inline findings afterward. Actionlint and diff checks
pass. The existing moved-finding test had encoded the incorrect behavior and is
updated to require a veto. Both wholly stale hashes still permit a clean review.

## Self-review

1. Data loss: only managed label decisions change; no application data paths.
2. Concurrency: existing pre/post HEAD and evidence checks remain unchanged.
3. Performance: constant additional comparison per inline comment; no new API calls.
4. Consistency: current and original commit bindings both veto conflicting summaries.
5. Coverage: both bots, wholly stale comments, original HEAD binding, live evidence,
   and revocation preserving unrelated labels are covered.
