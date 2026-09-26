# Issue tracker: GitHub (external reports only)

Issues for this repo are GitHub Issues on `bootjp/elastickv`, operated with the `gh` CLI. They are used **only for externally reported bugs, questions, and requests**. The maintainer's own work is never filed as an issue: a spec is a design doc under `docs/design/`, and that doc's milestone list is the plan. `gh` infers the repo from `git remote -v` inside a clone.

## When a skill says "publish to the issue tracker"

Do **not** create an issue. Write the spec as a design doc:

1. Create `docs/design/YYYY_MM_DD_proposed_<slug>.md` with the header block from `docs/design/README.md` (`Status: Proposed`, `Author`, `Date`).
2. Work on branch `design/<slug>`.
3. Open a PR whose body starts with `Design: docs/design/<file>`.

Decisions made while writing the spec go in the doc's `## Decisions` section. There is no `docs/adr/`.

## When a skill says "fetch the relevant ticket"

- Given a design doc path: read the file.
- Given an issue number (an external report): `gh issue view <number> --comments`.
- Given a PR number: `gh pr view <number>`; the `Design:` line in the body points at the spec.

## Conventions for external issues (used by `triage`)

- **Read an issue**: `gh issue view <number> --comments`.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'`, with `--label` / `--state` filters as needed.
- **Comment**: `gh issue comment <number> --body "..."`.
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`.
- **Close**: `gh issue close <number> --comment "..."`.

Labels follow `docs/agents/triage-labels.md`.

## Pull requests as a triage surface

**PRs as a request surface: no.** _(Set to `yes` if this repo starts treating external PRs as feature requests; `/triage` reads this flag.)_

## Skills not used in this repo

`to-tickets` and `wayfinder` are not part of the workflow, so there are no wayfinding operations and no ticket-creation conventions here. If that changes, restore the sections from the plugin's `issue-tracker-github.md` template.
