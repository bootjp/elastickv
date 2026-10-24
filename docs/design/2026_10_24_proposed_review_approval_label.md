# Review approval label

Status: Proposed
Author: bootjp
Date: 2026-10-24

## Goal

Maintain `agent-review-approved` only when the current pull request HEAD has a
completed Codex or Claude code review with zero findings. A new HEAD requires a
new review. Review requests, reactions, successful workflow runs, security-only
reviews, and the absence of comments do not establish approval.

The maintainer requested this behavior and accepted the SHA-bound approach before
implementation. This document remains proposed until the implementation lands.

## Evidence and decisions

- Accept only the known GitHub bot accounts, checking both login and numeric ID.
- Codex publishes `codex-review-state:v1` JSON in its summary comment. Require
  `code_review.commit_sha == HEAD`, `status == completed`,
  `has_any_findings == false`, and `has_unresolved_findings == false`.
  Within each reviewed SHA and review kind, the latest result is authoritative;
  do not fall back to an older clean result for that SHA.
  The format was observed in PR #1321; an unknown format fails closed.
- Claude adds an `agent-review-result:v1` JSON marker to its final PR review
  comment. Pin the requested SHA before invoking Claude and instruct it to emit
  a result only for an actual, complete review of that diff. The fields are
  `commit_sha`, `status`, and `has_any_findings`; every concern, including optional
  suggestions, counts as a finding. Non-review tasks and incomplete or scoped
  reviews cannot establish approval. Ordinary prose such as "no blocking issues"
  is insufficient.
- Either reviewer can establish approval. Current-HEAD findings from either bot,
  including Codex security findings, veto it. Trusted inline findings, change
  requests, and dismissed reviews for that HEAD also veto it. Resolving a finding
  alone does not turn a review that had findings into a zero-finding review.
- Choose the latest result per reviewer, reviewed SHA, and review kind. A later
  result for an old SHA cannot replace either approval or findings for HEAD.
  Code and security results are selected independently, even when bundled in
  the same summary. Require one unambiguous marker and strictly typed fields.
  Never infer a SHA from comment time or a short prefix.

## Workflows

The updater handles PR creation, pushes, reopening, closing, manual labeling,
trusted bot comment creation/edit/deletion, and manual dispatch. A periodic sweep
reconciles all open PRs and closed PRs retaining the label after missed or
token-suppressed events. Manual dispatch runs the same reconciliation.

A read-only event workflow relays review and inline-comment events through
`workflow_run`. The updater reads GitHub's current API state, not artifacts from
the triggering workflow. This also permits updates for fork PRs, whose review
workflows cannot receive a write token. The event workflow runs the local Node
tests for changes to the workflows, scripts, and Claude configuration.

All privileged execution uses scripts checked out from the repository's default
branch, never the PR branch. The updater has only contents/read, pull-requests/read,
and issues/write permissions and modifies only its own label. It does not request
reviews, run models, change merge rules, or merge PRs.

Before changing a label, fetch the PR again and confirm the HEAD and open state.
After adding it, read both HEAD and review evidence again and remove it if either
invalidates approval. Serialize
updater runs and reconcile all open PRs on each run so replacement of pending
Actions runs cannot lose an update for a different PR. Labels are asynchronous
metadata; this is not an atomic merge protection rule.

## Rollout and validation

Merge the workflow and helper into the default branch to activate it, then run
the updater manually to reconcile existing open PRs (or wait for the next sweep).
The updater creates the label if necessary. Existing Claude prose is not
retroactively approved; request a fresh review to obtain the marker.

Run `node --test .github/scripts/review-approval-label.test.cjs` and actionlint on
both new workflows and the changed Claude workflow. Cover trusted identities,
stale SHAs, incomplete and malformed results, review replacement/deletion,
security-only results, findings from either reviewer, revoked reviews, pagination,
label preservation, and HEAD changes around label writes.

## Milestones

- [x] Implement evidence parsing and reconciliation.
- [x] Add the privileged updater and read-only event/test workflow.
- [x] Include SHA-bound review results in Claude responses.
- [x] Validate local regression tests and workflow syntax.
- [ ] Merge to the default branch and verify a live reconciliation.

## Validation and self-review

The 74 Node regression tests pass. Actionlint passes for the updater, event
workflow, and changed Claude workflow. A read-only check against PR #1321's actual
REST comments recognizes its full-SHA Codex result as a completed zero-finding
code review. That PR is now closed; reconciliation does not approve closed PRs.
No remote labels were changed during validation.

The five review lenses were checked separately:

1. Data loss: no storage or application code changes. Label removal targets only
   `agent-review-approved`; regression tests preserve unrelated labels.
2. Concurrency and distributed failures: sweeps include closed labeled PRs and
   continue after individual PR failures. HEAD/state and evidence are checked
   again after approval; tests cover pushes, closure, and new findings during the
   label write. GitHub event delivery remains asynchronous.
3. Performance: paginated API reads run concurrently within each evidence fetch;
   PRs are reconciled serially to limit request bursts. No model invocations are
   added, and the periodic sweep runs hourly.
4. Data consistency: full SHAs, strict bot identities, explicit false booleans,
   current-HEAD finding vetoes, and an authoritative latest result per SHA prevent stale
   or ambiguous evidence from establishing approval. Code-fenced examples and
   unsubmitted reviews cannot establish approval.
5. Test coverage: the Node suite exercises both reviewers, malformed and stale
   evidence, revocation, API failures, pagination use, idempotence, and mutations
   around label writes. Application and Jepsen tests are not required for these
   CI-only changes. Activation and live write verification require merging the
   workflow into the default branch.

The PR review identified an ordering defect: a late result for an old SHA could
hide current-HEAD findings or approval from the same reviewer. Five regressions
failed before the fix. Results are now scoped by SHA before selecting the latest
code and security result independently; eight added regressions cover both bots,
mixed-SHA summaries, and label removal when stale results arrive late.
