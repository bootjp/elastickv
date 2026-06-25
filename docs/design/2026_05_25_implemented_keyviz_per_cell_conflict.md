---
status: implemented
phase: 2-C+
parent_design: docs/admin_ui_key_visualizer_design.md
date: 2026-05-25
---

# KeyViz Per-Cell Conflict Wire Format (Phase 2-C+ PR-3d)

## 1. Background

Phase 2-C+ PR-3c (#822) replaced the legacy §4.2 row-level max-merge
with the canonical §9.1 `(raftGroupID, leaderTerm)` per-cell dedupe:
the fan-out aggregator now collapses write samples to one value per
`(bucketID, raftGroupID, leaderTerm, windowStart)` and sums across
distinct terms for the same `(group, window)`. When two sources
report different non-zero values for the **same** `(group, term,
column)` tuple — a Raft-invariant violation, since at most one leader
exists per term per group — the merge flags a conflict.

That conflict detection already happens **per cell** inside the merge
accumulator (`cellMergeAcc.hasConflict()` in
`internal/admin/keyviz_fanout.go`). But the wire format only carries a
single **row-level** `conflict bool`: `resolveRowMergeAcc` ORs every
cell's conflict into one boolean for the whole row. The SPA's
`ConflictOverlay` then hatches the **entire row**, even when the
disagreement was confined to one column during a brief leadership
flip.

Parent design §9.1 actually describes the conflict signal at cell
granularity — *"if they differ, the cell is surfaced with
`conflict=true`"* and *"The heatmap hatches rows or time windows whose
expected source node failed."* The row-level collapse was an explicit
PR-3c simplification (see the deferral note in
`keyviz_handler.go`'s `KeyVizRow.Conflict` doc comment); PR-3d closes
that gap.

## 2. Scope

### 2.1 In scope

- Add a per-cell `Conflicts []bool` field to the **JSON** `KeyVizRow`
  (`internal/admin/keyviz_handler.go`), parallel to `Values[]` —
  `Conflicts[j]` is true when column `j` saw a within-term
  disagreement during fan-out merge.
- Stamp `Conflicts[j]` from the existing per-cell
  `cellMergeAcc.hasConflict()` in `resolveRowMergeAcc`.
- Keep the row-level `Conflict bool` on the wire as the OR of all
  cells, so an **older SPA** that only reads `conflict` keeps hatching
  the whole row (no behavioural regression).
- SPA `ConflictOverlay`: hatch the **individual cells** where
  `conflicts[j]` is true. When a response carries only row-level
  `conflict` (legacy server, no `conflicts` array), fall back to the
  current whole-row hatch.
- Tests: per-cell conflict assertions in `keyviz_fanout_test.go`.

### 2.2 Out of scope / non-goals

- **No proto / gRPC change.** The `proto.KeyVizRow` (gRPC
  `GetKeyVizMatrix`) has **never** carried a conflict field. Conflict
  is a fan-out *merge* artifact, and fan-out is implemented as
  JSON-over-HTTP peer aggregation (`KeyVizFanout.Run`); the single-node
  gRPC RPC never merges and therefore never produces a conflict.
  Adding `conflicts` to the proto would introduce a field with no
  producer and no consumer — speculative, so explicitly excluded.
- No change to the merge *algorithm* — PR-3c's `(group, term)` dedupe
  and fallback-max path are unchanged. PR-3d only widens how the
  already-computed per-cell conflict bit reaches the client.
- No new `--flag`; conflict reporting is intrinsic to fan-out and was
  already on by default whenever fan-out is configured.

## 3. Wire format

`KeyVizRow` (JSON) gains:

```go
// Conflicts[j] is true when fan-out merge saw two sources report
// different non-zero values for the same (bucket, raft_group_id,
// leader_term, column j) tuple. Parallel to Values[]; allocated
// lazily so it is nil whenever the row had no conflict (single-node,
// legacy server, OR a cleanly merged row) — omitempty then keeps it
// off the wire. Otherwise len == len(Values). The row-level Conflict
// bool remains the OR of this slice for older clients.
Conflicts []bool `json:"conflicts,omitempty"`
```

Compatibility matrix:

| Server | Client (SPA) | Behaviour |
|---|---|---|
| new (emits `conflicts[]` + `conflict`) | new | per-cell hatch |
| new | old (reads only `conflict`) | whole-row hatch (unchanged) |
| old (emits only `conflict`) | new | falls back to whole-row hatch |
| old | old | whole-row hatch (unchanged) |

`omitempty` keeps the field off the wire for the single-node /
no-fan-out path (the merge that produces conflicts only runs with ≥2
matrices), so quiet deployments see no payload growth.

## 4. Merge changes

`resolveRowMergeAcc` (`keyviz_fanout.go`) already iterates every cell
to resolve writes. It will additionally:

1. On the first conflicting cell, **lazily** allocate
   `row.Conflicts = make([]bool, width)` — never up front, so a
   cleanly merged row keeps `Conflicts == nil` and `omitempty` drops
   it from the wire (a non-nil `[]bool` is never "empty" for
   `omitempty`, so eager allocation would emit `[false,...]` on every
   merged write row and balloon the payload).
2. Set `row.Conflicts[j] = true` and `row.Conflict = true` for that
   cell.

The read path (`useGroupTermDedupe == false`) never sets conflict, so
it leaves `Conflicts` nil — consistent with today's row-level
behaviour.

## 5. SPA changes

`ConflictOverlay` currently emits one full-width `<rect>` per
conflicting row. PR-3d:

- When `row.conflicts` is present, emit one `<rect>` per conflicting
  **cell** at `x = j * cellW`, `width = cellW`, `y = i * cellH`.
- When `row.conflicts` is absent but `row.conflict` is true (legacy
  server), keep the full-row rect.
- The `RowDetail` "conflict" pill keeps using the row-level
  `row.conflict` (it is a row-scoped summary, unchanged).

The hatch `<pattern>` is unchanged. Per-cell rects reuse the same
`fill="url(#keyviz-conflict-hatch)"`.

## 6. Self-review (per CLAUDE.md five lenses)

1. **Data loss** — none. Read-only admin path; no Raft / FSM / Pebble
   interaction. `conflicts[]` is derived from existing merge state.
2. **Concurrency / distributed** — none new. Merge runs synchronously
   after the parallel peer fetch; no shared mutable state added.
3. **Performance** — one `make([]bool, width)` only for rows that
   actually conflict (lazy allocation), bounded by the 1024-row
   budget; the conflict bit was already computed per cell, so no extra
   passes. Lazy alloc + `omitempty` means clean rows add zero wire
   bytes and zero allocations.
4. **Data consistency** — the row-level `conflict` stays the exact OR
   of `conflicts[]`, so the coarse signal is unchanged; per-cell is
   strictly more precise. No change to `(group, term)` dedupe.
5. **Test coverage** — new table cases assert `Conflicts[j]` is true
   only at the conflicting column and that the row-level OR still
   fires. SPA change is rendering-only; covered by the existing
   manual heatmap check.

## 7. Rollout

Forwards- and backwards-compatible (see §3 matrix). No flag, no schema,
no state on disk. Mixed-version clusters: a new aggregator merging an
old peer's matrix simply gets no `conflicts[]` from that peer and ORs
its row-level `conflict` as before; a new SPA against an old server
falls back to whole-row hatch.
