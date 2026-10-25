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
