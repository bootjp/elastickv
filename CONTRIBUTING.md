# Contributing to elastickv

Thanks for your interest. This file is the map; the rules themselves live in `CLAUDE.md` (coding standards, architecture, review passes, design-doc workflow) and `AGENTS.md` (build, style, and testing requirements that reviewers enforce). Where the two overlap they say the same thing; if they ever disagree, fix the disagreement rather than picking one.

## Coding standards

Read **`CLAUDE.md`**:

- **Conventions**: `gofmt` plus the linters in `.golangci.yaml`, `cockroachdb/errors` wrapping, structured `slog` keys, table-driven tests, and the HLC and routing invariants.
- **Self-review of code changes**: the five review lenses (data loss, concurrency / distributed failures, performance, data consistency, test coverage). Run them one at a time and record each result in the PR description.

Reviewers, human or automated, check changes against those two sections.

## Workflow

1. **Design doc first** for anything beyond a single-file edit, and for any
   new feature, new adapter, new control-plane RPC, schema or wire-format
   change, or modification touching replication / MVCC / OCC / HLC / routing
   even when it fits in one file. Write `docs/design/YYYY_MM_DD_proposed_<slug>.md` (see `docs/design/README.md` for the header block and lifecycle) and get it reviewed before implementing. A PR may carry the doc and the implementation, doc commit first.
2. **Branch** `design/<slug>`; the PR body starts with `Design: docs/design/<file>`.
3. **Review-found defects**: add a failing test that reproduces the issue first, then the fix, in the same PR.
4. **Test evidence** in the PR description: `make test` (all packages under `-race`), `make lint`, and the relevant Jepsen suite for replication / MVCC / OCC / Redis changes.

## Commands

```bash
make test    # go test -v -race ./...
make lint    # golangci-lint --config=.golangci.yaml run --fix
make run     # single-process 3-node demo
```

`make gen` regenerates protobufs and is toolchain-pinned (see `proto/Makefile`).

## Commits

Short imperative summary with an optional scope prefix matching the touched area (`store:`, `adapter:`, `kv:`, `docs:`, …).

### Developer Certificate of Origin

Every commit submitted in a pull request must include a `Signed-off-by` trailer
with the commit author's name and email, certifying the [DCO 1.1](DCO). This
applies to maintainers, bots, and merge commits within the PR. Existing commits
already on the target branch are outside the check.

Read the DCO before signing off. Configure your own Git identity, then use:

```bash
git commit --signoff -m "store: handle expired values"
```

Git adds a trailer using your committer identity:

```text
Signed-off-by: Your Name <your-email@example.com>
```

Your author and sign-off identities must match exactly. When applying another
person's commit, preserve their author and existing sign-off; your own sign-off
alone does not certify their authorship. A DCO sign-off is separate from a GPG
or SSH commit signature; a verified signature alone does not satisfy this check.

To repair your latest commit, **only if you are its author and can certify it**:

```bash
git commit --amend --no-edit --signoff
```

For several commits you authored and can certify, use an interactive rebase,
mark each unsigned commit as `edit`, amend it with `--signoff`, and continue with
`git rebase --continue`. Coordinate history rewrites with collaborators; after
amending an already-pushed branch, use `git push --force-with-lease`. Do not add
another contributor's sign-off on their behalf.

The `DCO` workflow checks all commits in the event's `base..head` range, including
unsigned earlier commits even when the latest commit is signed off. Check the
same range locally with:

```bash
python3 scripts/check-dco.py origin/main HEAD
```

Renovate uses `:gitSignOff` to sign off its dependency updates. After this
workflow is merged, add `DCO` as a required status check in branch protection or
the repository ruleset to block merges when it fails.

## Issues

Bug reports and questions are welcome as GitHub Issues. They are triaged with the labels in `docs/agents/triage-labels.md`. Maintainer work is planned in design docs, not issues.

## Vocabulary

`CONTEXT.md` is the glossary. Use its terms in identifiers, doc titles, and test names.
