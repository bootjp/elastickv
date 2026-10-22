# Domain Docs

How the engineering skills consume this repo's domain documentation when exploring the codebase.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root: the glossary. Vocabulary only, no implementation detail.
- **`docs/design/`**: the design docs that touch the area you are about to work in. Find them by slug or keyword; `docs/design/README.md` explains the `YYYY_MM_DD_<status>_<slug>.md` naming and the proposed / partial / implemented lifecycle. An `_implemented_` doc is the as-built record; its `## Decisions` section (where present) is where decisions live.
- **`docs/architecture_overview.md`**: the reference diagrams. Read it before touching coordination, replication, or routing.

This repo does **not** use ADRs. There is no `docs/adr/` and none should be created. `CONTEXT-MAP.md` does not exist either: this is a single-context repo.

## File structure

```
/
├── CONTEXT.md            ← glossary
├── CLAUDE.md             ← conventions and cross-cutting invariants
├── docs/
│   ├── architecture_overview.md
│   └── design/           ← proposals and as-built records; decisions live here
└── (Go packages)
```

## Use the glossary's vocabulary

When your output names a domain concept (in a design doc title, a refactor proposal, a hypothesis, a test name, an identifier), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary avoids.

If the concept you need isn't in the glossary yet, that's a signal: either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`, which extends `CONTEXT.md`).

## Recording decisions

- A decision tied to one feature goes in that feature's design doc, under `## Decisions`.
- A cross-cutting invariant (an HLC rule, a routing rule, an MVCC visibility rule) goes in the **Conventions** section of `CLAUDE.md`.
- When `domain-modeling` offers to write an ADR, write the decision into the relevant design doc instead.

## Flag conflicts with existing design docs

If your output contradicts the Decisions of an implemented design doc or a Convention in `CLAUDE.md`, surface it explicitly rather than silently overriding:

> _Contradicts `docs/design/2026_04_20_implemented_lease_read.md` (lease reads are leader-only), but worth reopening because…_
