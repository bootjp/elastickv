# Contributing to elastickv

Thanks for your interest. This file is the map; the rules themselves live in `CLAUDE.md`, which is the single source of truth for coding standards.

## Coding standards

Read **`CLAUDE.md`**:

- **Conventions**: `gofmt` plus the linters in `.golangci.yaml`, `cockroachdb/errors` wrapping, structured `slog` keys, table-driven tests, and the HLC and routing invariants.
- **Self-review of code changes**: the five review lenses (data loss, concurrency / distributed failures, performance, data consistency, test coverage). Run them one at a time and record each result in the PR description.

Reviewers, human or automated, check changes against those two sections.

## Workflow

1. **Design doc first** for anything beyond a single-file edit. Write `docs/design/YYYY_MM_DD_proposed_<slug>.md` (see `docs/design/README.md` for the header block and lifecycle) and get it reviewed before implementing. A PR may carry the doc and the implementation, doc commit first.
2. **Branch** `design/<slug>`; the PR body starts with `Design: docs/design/<file>`.
3. **Review-found defects**: add a failing test that reproduces the issue first, then the fix, in the same PR.
4. **Test evidence** in the PR description: `go test -race`, `make lint`, and the relevant Jepsen suite for replication / MVCC / OCC / Redis changes.

## Commands

```bash
make test    # go test -v -race ./...
make lint    # golangci-lint --config=.golangci.yaml run --fix
make run     # single-process 3-node demo
```

`make gen` regenerates protobufs and is toolchain-pinned (see `proto/Makefile`).

## Commits

Short imperative summary with an optional scope prefix matching the touched area (`store:`, `adapter:`, `kv:`, `docs:`, …).

## Issues

Bug reports and questions are welcome as GitHub Issues. They are triaged with the labels in `docs/agents/triage-labels.md`. Maintainer work is planned in design docs, not issues.

## Vocabulary

`CONTEXT.md` is the glossary. Use its terms in identifiers, doc titles, and test names.
