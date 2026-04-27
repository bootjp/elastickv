---
status: proposed
phase: keyviz / follow-up
parent_design: docs/admin_ui_key_visualizer_design.md
date: 2026-04-28
---

# KeyViz adapter / namespace labels

## 1. Background

Phase 2-A through 2-C ship a fully functional KeyViz heatmap, but
the smallest unit of attribution is a **Raft route** — a contiguous
key range owned by one group. When multiple adapters share a route
(the default single-group config has every adapter writing into a
single `[-∞, +∞)` route), the heatmap shows one row with the
combined traffic and the operator cannot tell whether the spike
came from DynamoDB, Redis, S3, SQS, or RawKV.

A user observed this in production: 6-node cluster, fan-out
returning a single row at `route:1` with `Total = 378` writes,
covering all five adapters indistinguishably.

The hotspot-shard-split design and the multi-group startup flags
(`--raftRedisMap` / `--raftDynamoMap` / `--raftS3Map` /
`--raftSqsMap`) already give operators a way to split traffic
across distinct Raft groups so per-route attribution becomes
per-adapter attribution. But that is an **operational** workaround:
single-group deployments — the most common shape for first-time
operators and small clusters — still get the all-traffic-in-one-row
view.

This proposal adds an **independent label dimension** to the
sampler so a single Raft group can still surface per-adapter
breakdown in the heatmap.

## 2. Goals and non-goals

### 2.1 Goals

- Attribution **inside a single route**: a row that today reads
  `route:1, total=378` should optionally split into sub-rows like
  `route:1 / dynamo`, `route:1 / s3`, `route:1 / redis`, …
- Zero hot-path penalty when labels are **not** configured: a
  default deployment continues to behave exactly as today.
- Adapter-side wiring is the natural place to set labels (every
  adapter already has the dispatch entry into
  `ShardedCoordinator.Observe…` — see `kv/sharded_coordinator.go`).
  No global key-prefix table on the operator side.
- Cluster fan-out merges per-(route, label) cells, not just
  per-route — operators see the same per-adapter breakdown across
  the whole cluster.

### 2.2 Non-goals (deferred)

- **Free-form custom labels** (per-table, per-bucket, per-queue).
  This proposal stops at one level: the adapter family. A
  follow-up can add a second dimension (e.g. `dynamo / users`,
  `dynamo / orders`) once the wire format settles.
- **Persistence**. Labels are in-memory like the rest of the
  Phase 2 sampler.
- **Per-key-byte attribution**. Sub-route classification is done
  at the dispatch entry (the adapter knows its identity); we do
  not attempt to classify by inspecting the key bytes.
- **Backwards-incompatible wire-format changes**. The label is
  added as an optional field; old SPAs against new servers keep
  working.

## 3. Surface

The user-visible delta is one extra label on every heatmap row.
Default config emits the empty label (legacy behaviour); when
adapters set labels, the heatmap splits a single route into one
row per (route, label) pair:

```
Today (single group, no labels):
  Row 0  route:1  Total 378

After (adapters tag their traffic):
  Row 0  route:1 / dynamo   Total 142
  Row 1  route:1 / redis    Total 200
  Row 2  route:1 / sqs      Total 18
  Row 3  route:1 / s3       Total 11
  Row 4  route:1 / rawkv    Total  7
```

## 4. Options for label propagation

Four ways to get the label from the adapter to the sampler. Pick
one based on hot-path cost vs. plumbing weight.

### 4.1 (Recommended) Per-Observe label string

Extend the sampler signature:

```go
Observe(routeID uint64, op Op, keyLen, valueLen int, label string)
```

Empty `label` → existing behaviour, single row per route. Adapters
set their own constant string (`"dynamo"`, `"redis"`, …) at the
dispatch site they already own.

Cost on hot path: one extra map lookup per Observe — slot is now
keyed by `(routeID, label)` instead of just `routeID`. The map
key is a struct of `{uint64, string}`; the `label` is a small
interned constant (`"dynamo"`), so allocation should be zero.

Storage: each (route, label) gets its own `routeSlot`. With 5
adapters and a 1024-route budget, the worst case is 5120 slots
per node — still well below the existing `MaxTrackedRoutes` cap
when operators raise it for label use.

### 4.2 Per-adapter sampler instance

Wire one `*MemSampler` per adapter, each with a fixed label. The
admin handler queries every sampler and concatenates the results.

- Pro: zero hot-path code change in the existing sampler.
- Con: every adapter gets its own ring buffer, history, and
  retention machinery. Memory is N× higher and per-route
  metadata duplicates across samplers.

### 4.3 Per-key-prefix taxonomy (operator-configured)

Static `{prefix → label}` map registered at startup. Sampler
classifies each key at Observe time by prefix-matching.

- Pro: no adapter wiring; works with any caller that goes through
  `ShardedCoordinator`.
- Con: prefix-match per Observe is a hot-path cost, and the
  taxonomy is a new operator-facing config the design has been
  careful to avoid.

### 4.4 Hash the adapter into the route catalog

Make `distribution.Route` carry an adapter label and route by
adapter at the catalog layer. The sampler stays single-keyed.

- Pro: solves attribution at the catalog level, where it actually
  belongs.
- Con: the catalog is the wrong place for this — adapters share
  Raft groups by design, and forcing `Route` to carry adapter
  identity bakes a different separation into the route topology.
  Reverses the multi-group startup-flag story.

**Recommendation: Option 4.1.** Lowest plumbing weight, smallest
hot-path delta (one map lookup), and the label originates where
it is most naturally available (the adapter's dispatch entry).
The `routeSlot` map shape changes from `map[uint64]*routeSlot` to
`map[slotKey]*routeSlot` with `slotKey = {uint64, string}`.

## 5. Wire format extension

`MatrixRow` (Go) and `KeyVizRow` (proto + JSON):

```diff
 type MatrixRow struct {
   RouteID      uint64
+  Label        string  // optional; empty for legacy unlabelled traffic
   Start, End   []byte
   …
 }
```

The SPA reads the label and renders rows as
`route:<id> / <label>` when label is non-empty; legacy rows
(empty label) render as `route:<id>` exactly like today.

`bucket_id` formatting in the wire shape stays as-is; the label
is a separate field. This keeps the `route_ids` / `aggregate`
semantics from §5 of the parent design unchanged.

Forward compatibility: an old SPA against a new server ignores
the unknown `label` field and shows just the route as it always
did. A new SPA against an old server sees no `label` field and
falls back to the legacy formatting. Both directions are
non-breaking.

## 6. Aggregator merge changes

The fan-out aggregator's per-cell merge key gains the label:

- Phase 2-C (current): `(bucketID, raftGroupID, leaderTerm,
  windowStart)` per design `2026_04_27_proposed_keyviz_cluster_fanout.md`
  §4.
- With labels: `(bucketID, label, raftGroupID, leaderTerm,
  windowStart)`.

Reads still sum, writes still max-with-conflict; nothing about
the merge rules changes other than the key-tuple width.

## 7. Implementation plan

| PR | Scope |
|---|---|
| **PR-A** | Land this design doc. |
| **PR-B** | Sampler API extension: `Observe(... label string)`, `RegisterRoute` no-op (label is per-Observe, not per-route). `MatrixRow.Label`. Update the existing tests to pass empty labels. |
| **PR-C** | Adapter wiring: each adapter sets its own label at the dispatch entry into `ShardedCoordinator.Observe…`. Default constants live in one file (`adapter/keyviz_labels.go`) so a future audit lists every label in one place. |
| **PR-D** | Wire-format extension: proto + JSON `label` field; SPA renders `route:N / <label>` when present. |
| **PR-E** | Aggregator merge key gains `label`. Mirrors PR-3 of the Phase 2-C+ track. |

PRs B and C are independent of the wire format; the heatmap will
keep showing one row per route until D ships, but the per-label
counts are already accumulating in the sampler so the wire
extension is "switch on the field".

## 8. Five-lens checklist

1. **Data loss** — n/a; per-Observe label is metadata. The
   existing "no counts lost across flush" invariant
   (`keyviz/sampler_test.go`) extends straightforwardly with the
   label dimension; the `(routeID, label)` slot is still atomic-
   add updated like the current `routeID` slot.
2. **Concurrency / distributed** — slot-key change is contained
   in the routesMu COW path; the hot-path Load + map lookup keeps
   the same shape (one lookup, one atomic add). Burst test
   updates: parametrise on (route, label) instead of just route.
3. **Performance** — One extra map lookup per Observe via the
   wider key. `BenchmarkObserveParallel` already pins the hot-path
   cost; the new bench should land within run-to-run variance of
   the current 4 ns/op. If it doesn't, the design is wrong and we
   fall back to Option 4.2.
4. **Data consistency** — Cluster fan-out merge gains a tuple
   field; the dedup invariant (per-cell, per-(route, label,
   group, term, window)) still holds. Old SPA against new server
   sees the label-collapsed view; new SPA against old server
   sees the legacy view; both are coherent.
5. **Test coverage** — New test categories:
   - `Observe(label="dynamo")` and `Observe(label="redis")`
     against the same routeID produce two distinct rows in
     `Snapshot`.
   - Empty `label` matches no other rows (legacy behaviour pinned).
   - Burst test: many goroutines hitting the same route with
     different labels — exact-counting invariant must hold per
     (route, label).
   - Aggregator merge: same route, two labels, two nodes —
     each label dedupes correctly without bleeding into the
     other.

## 9. Open questions

1. **Should the label be hierarchical** (`dynamo / users`) from
   day one, or restricted to a single segment now and extended
   later? Proposal: single segment now (cheapest sampler change),
   extend with a `/`-delimited convention later if adapters want
   sub-tenant attribution.
2. **Label allocation discipline** — who owns the canonical label
   set? Proposal: `adapter/keyviz_labels.go` exports
   `LabelDynamo`, `LabelRedis`, `LabelS3`, `LabelSQS`,
   `LabelRawKV` constants. Adapters refer to these; nothing
   stops a future adapter from inventing its own label, but the
   review burden of adding to the central file catches accidental
   variants like `"DynamoDB"` vs `"dynamo"`.
3. **Should the aggregator collapse same-route different-label
   rows for operators who don't want the breakdown?** Proposal:
   no — the SPA already lets operators pick which row to
   examine; the wire form should always carry the breakdown so
   the data is queryable.

## 10. Out of scope (explicit deferrals)

- Per-table / per-bucket / per-queue / per-Redis-DB sub-labels.
- Operator-configurable label taxonomy.
- Persistence of labelled rows (Phase 3 covers persistence
  generally; labels ride along once the persistence path lands).
- Adapter-aware splitting of routes (`SplitRange` triggered by
  adapter-label hotspots) — that is a Phase 3+ idea.
