# DCO sign-off checks

Status: Implemented

Shipped in #1306 (`DCO`, `.github/dco.yml`, Renovate `:gitSignOff`). No implementation work follows.
Author: bootjp
Date: 2026-10-24

## Goal

Require contributors to certify the Developer Certificate of Origin (DCO) 1.1
by signing off their commits. Use the Probot DCO App to check pull requests and
document `git commit --signoff` and repair instructions.

## Decisions

- Keep the unmodified DCO 1.1 in `DCO` and explain the policy in `CONTRIBUTING.md`.
- Require a `Signed-off-by: Name <email>` trailer matching the commit's author
  name and email. A different committer is allowed; a sign-off by a different
  person alone does not satisfy the check. A sign-off is a contributor's DCO
  certification, separate from a cryptographic commit signature.
- Install the [Probot DCO App](https://probot.github.io/apps/dco/) for
  `bootjp/elastickv`. The hosted App owns the `DCO` check; do not add a custom
  checker or GitHub Actions workflow with the same name.
- Follow the App's standard behavior, which excludes bot and merge commits.
  Its PR checks do not require rewriting existing base-branch history.
- Add `.github/dco.yml` with `require.members: true`, so the repository owner
  and maintainers are subject to the same checks as other contributors.
- Disable individual and third-party remediation commits explicitly. Authors
  must repair the unsigned commits themselves rather than adding a follow-up
  certification. The App reads configuration from the default branch.
- Enable Renovate's `:gitSignOff` preset. Its existing go-mod-fix workflow amends
  without changing the commit author or message, preserving that sign-off.
- Make the App's `DCO` check required through branch protection or a repository
  ruleset when rolling out enforcement. Select the DCO App as the expected check
  source. This PR does not change repository settings or rewrite history.

## Rollout and validation

The user has installed the App. Push this branch to trigger its check, and verify
that the check was produced by the DCO App rather than GitHub Actions. The new
configuration matches the App's documented defaults, which apply until this
configuration reaches the default branch. Existing open PRs may need authors
to sign off earlier commits. Contributors must sign off only work they can
certify; maintainers must not add another person's certification.

Validate the YAML configuration and Renovate JSON, run `git diff --check`, and
verify the branch's commit trailers. No server, storage, replication, or
wire-protocol behavior changes; no new executable checker needs unit tests.

## Self-review

- Data loss: documentation and App configuration do not modify runtime data.
- Concurrency / distributed failures: check execution and PR update handling
  belong to the hosted DCO App; no repository runner handles credentials.
- Performance: no runtime or repository CI runner cost for DCO validation.
- Data consistency: the App validates author sign-offs; the policy documents
  its bot and merge exclusions and keeps maintainer checks enabled.
- Test coverage: validate configuration and verify the App's actual PR check;
  no custom checking implementation remains to test.

## References

- [DCO 1.1](https://developercertificate.org/)
- [Probot DCO App](https://probot.github.io/apps/dco/)
- [DCO App configuration](https://github.com/dcoapp/app#usage)
- [Git sign-off option](https://git-scm.com/docs/git-commit#Documentation/git-commit.txt---signoff)
- [Renovate sign-off preset](https://docs.renovatebot.com/presets-default/#gitsignoff)
