# DCO sign-off checks

Status: Proposed
Author: bootjp
Date: 2026-10-24

## Goal

Require contributors to certify the Developer Certificate of Origin (DCO) 1.1
by signing off every commit submitted in a pull request. Document `git commit
--signoff` and how contributors can repair their own unsigned commits.

## Decisions

- Keep the unmodified DCO 1.1 in `DCO` and explain the policy in `CONTRIBUTING.md`.
- Require a `Signed-off-by: Name <email>` trailer matching the commit's author
  name and email. A different committer is allowed; a sign-off by a different
  person alone does not satisfy the check. A sign-off is a contributor's DCO
  certification, separate from a cryptographic commit signature.
- Check every commit reachable from the PR head but not its base, including
  merge commits. Do not exempt maintainers or bots. Existing base history is
  outside this range and does not need to be rewritten.
- Add a read-only `pull_request` workflow, without path or branch filters, for
  opened, synchronized, reopened, and edited PRs. Check the event's exact base
  and head SHAs rather than GitHub's temporary merge commit. Fetch full history
  and reject shallow or missing history rather than accepting a partial check.
- Use a small Python standard-library checker with Git's trailer parser, so the
  check needs no package installation or third-party DCO action. Never execute
  commands or code from commit messages; keep token permissions read-only and
  disable persisted checkout credentials.
- Run the checker's Git-fixture tests in the same workflow. Tests cover the
  accepted identity, missing/mismatched/prose sign-offs, multiple signers,
  merges, diverged bases, full ranges, and unavailable history.
- Enable Renovate's `:gitSignOff` preset. Its existing go-mod-fix workflow amends
  without changing the commit author or message, preserving that sign-off.
- Keep the check name `DCO`. After merging, maintainers can make `DCO` a required
  status check in the repository's branch protection or ruleset. This change
  does not alter repository settings or rewrite existing contributor commits.

## Rollout and validation

The first new PR event after the workflow is present checks all commits in that
PR, including commits authored before this policy. Authors must sign off only
work they can certify; maintainers must not add another person's certification.

Run `python3 -m unittest discover -s scripts -p 'test_check_dco.py' -v`,
`actionlint .github/workflows/dco.yml`, and `git diff --check`. Validate this
branch's signed-off commits against its base using the checker. No server,
storage, replication, or wire-protocol behavior changes.

## Self-review

- Data loss: only commit metadata is read; repository history is not mutated.
- Concurrency / distributed failures: immutable event SHAs determine the range;
  a subsequent PR update starts a new check.
- Performance: Git metadata is inspected once per new PR commit; no Go build
  or external dependency installation is needed.
- Data consistency: author identity uses the commit's original name and email,
  without mailmap rewriting; Git parses the final trailer block.
- Test coverage: real temporary repositories exercise commit ranges and trailer
  parsing, including unsigned merges and incomplete history.

## References

- [DCO 1.1](https://developercertificate.org/)
- [Git sign-off option](https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---signoff)
- [Renovate sign-off preset](https://docs.renovatebot.com/presets-default/#gitsignoff)
