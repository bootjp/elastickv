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

```text
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

#### 4.1.1 Slot lifecycle and the lockless invariant

The current `Observe` is `Load → map-lookup → atomic.Add` with no
fallback: a miss is silently dropped. This is intentional —
`RegisterRoute` pre-creates every slot before traffic arrives, so
the hot path never needs to allocate. We keep that invariant by
making `RegisterRoute` **pre-create one slot per known label** at
registration time, but only on the individual-tracking path. The
label set is the canonical `keyviz/labels.go` constants (§9 Q2);
per-route slot count is `len(labels) + 1` (the +1 is the empty-
label legacy slot, kept so callers that pass `label=""` still hit
a slot). Total individual slots in the table is therefore
`len(routes) × (len(labels) + 1)`.

#### Pre-allocation is gated on PR-C, not PR-B

PR-B alone changes the slot-key type from `uint64` to `slotKey`
but still only pre-creates the **legacy empty-label slot**. The
labeled siblings are created by PR-C, when adapters actually
start passing non-empty labels. This split is deliberate and
preserves PR-B as a behavior-neutral refactor:

- After PR-B alone, slot count per route is **1** (just the
  legacy slot, identical to today's `routeID`-only behavior).
  A deployment running 1024 routes today still supports 1024
  routes after PR-B — no early coarsening, no virtual-bucket
  fold, no per-operator action required. (Codex P1 on round-5.)
- After PR-C, slot count per route grows to `len(labels) + 1`.
  Operators raise `--keyvizMaxTrackedRoutes` before deploying
  PR-C per the §7 rollout note.

Concretely PR-B's `RegisterRoute` body looks like:

```go
// PR-B body:
slot := newSlot(routeID, "", start, end, groupID)
next.slots[slotKey{RouteID: routeID, Label: ""}] = slot

// PR-C extends to:
for _, label := range allLabelsWithLegacy() { // AllLabels ∪ {""}
    next.slots[slotKey{RouteID: routeID, Label: label}] = newSlot(routeID, label, start, end, groupID)
}
```

When `RegisterRoute` decides a route will be coarsened into a
virtual bucket (i.e. it returns `false`), **no labeled slots are
created** for that route — the `tbl.virtualForRoute[routeID]`
fallback in `Observe` already handles all labels uniformly per
§6.5 (virtual buckets always emit `Label = ""`). Pre-creating
labeled slots that would never be hit would just burn allocator
work.

#### `MaxTrackedRoutes` semantics: count routes, not slots

After the slot-key widens to `slotKey{uint64, string}`, the live
table contains `len(AllLabels) + 1` slots per individually-tracked
route — bare `len(next.slots)` no longer equals "number of
routes." We pick **Option A (route-counting)**: the coarsening
check at `keyviz/sampler.go:416` divides by `len(AllLabels) + 1`
so `MaxTrackedRoutes` continues to count routes, not slots. This
preserves the operator-visible meaning of the existing flag —
`--keyvizMaxTrackedRoutes=1024` still means "1024 individual
routes," not "1024 slots that may be ~170 routes." Total slot
memory is `MaxTrackedRoutes × (len(AllLabels) + 1)`, which we
document but never count against the cap.

```go
// keyviz/sampler.go:416 (PR-B)
slotsPerRoute := len(keyviz.AllLabels) + 1 // 1 today (empty AllLabels), 6 after PR-C
if len(next.slots) / slotsPerRoute < s.opts.MaxTrackedRoutes {
    // ... pre-create slot(s) for this route
}
```

The alternative (Option B: `MaxTrackedRoutes` redefined as
"slots") would silently halve the route capacity at PR-C
rollout — an operator setting 1024 today would suddenly track
~170 routes after PR-C deploys with five labels, even though
the flag value is unchanged. We reject this on the
"behavior-preserving for legacy single-group deployments"
goal in §2.1. (Claude bot moderate on PR #694 round-5.)

New labels added after a route is registered are not auto-bound;
an operator deploying a new adapter must **deploy a new binary**
that includes the new constant (a process restart alone does
nothing — the canonical set lives at compile time). Matches the
current "ApplySplit / ApplyMerge in a future PR" semantics for
the static route catalog.

**`virtualForRoute` stays `map[uint64]*routeSlot`** (no widening
to `slotKey`). Virtual buckets aggregate across labels per §6.5,
so the fallback lookup keys on `RouteID` alone — widening it to
`slotKey` would make every coarsened-route `Observe` miss both
the labeled lookup AND the virtual fallback, silently dropping
all traffic for those routes. The new `Observe` lookup chain is:

```go
slot, ok := tbl.slots[slotKey{RouteID: routeID, Label: label}]
if !ok {
    slot, ok = tbl.virtualForRoute[routeID] // coarsened route: label irrelevant, aggregate all traffic
    if !ok {
        return // unknown route — drop, same as today
    }
}
// atomic.Add as today
```

`routeSlot` gains a `Label string` field so `Flush` /
`appendDrainedRow` can read the label off the slot rather than
threading it through a separate channel:

```go
type routeSlot struct {
    metaMu  sync.RWMutex
    RouteID uint64
    Label   string         // new — empty for the legacy unlabeled slot
    GroupID uint64         // Phase 2-C+, see PR-3a
    Start, End []byte
    ...
}
```

`reclaimRetiredSlot` (`keyviz/sampler.go:710`) must dedupe on
`(RouteID, Label)`, not `RouteID` alone — a slot retired as
`(routeID, "dynamo")` and re-registered as `(routeID, "redis")`
is **not** a reclaim candidate; they are different slots.

#### Symmetric teardown in `RemoveRoute`

`RemoveRoute(routeID)` must be updated to remove **every**
`slotKey{routeID, label}` for `label ∈ AllLabels ∪ {""}` — not
just the legacy `(routeID, "")` entry. The current code at
`keyviz/sampler.go:532`:

```go
delete(next.slots, routeID)
s.retiredSlots = append(s.retiredSlots, retiredSlot{slot: individual, retiredAt: retiredAt})
```

is a `map[uint64]*routeSlot` delete; after key widening this
becomes a compile error. The PR-B fix loops over `AllLabels`
plus the empty legacy label and retires N+1 slots in one pass:

```go
for _, label := range allLabelsWithLegacy() { // AllLabels ∪ {""}
    key := slotKey{RouteID: routeID, Label: label}
    if slot := next.slots[key]; slot != nil {
        delete(next.slots, key)
        s.retiredSlots = append(s.retiredSlots, retiredSlot{slot: slot, retiredAt: retiredAt})
    }
}
```

If only the legacy slot is retired, every labeled
`(routeID, "dynamo")` / `(routeID, "redis")` / … remains in the
live `routeTable` receiving Observe traffic forever — orphaned
slots accumulating until process restart. The §8 lens-5 test
"`RemoveRoute` retires N+1 slots" pins this. (Claude bot moderate
on PR #694 round-4.)

#### `RegisterRoute` idempotency check after key widening

The current idempotency guard is `if _, ok := cur.slots[routeID]`
which becomes a type mismatch after the key widens to `slotKey`.
PR-B replaces it with a check on the legacy slot:
`if _, ok := cur.slots[slotKey{RouteID: routeID}]; ok { return true }`
— the legacy slot's presence implies the labeled siblings were
already created by an earlier `RegisterRoute` call (the
pre-creation loop is atomic with respect to the routesMu lock).

Two alternatives we explicitly reject:

- **First-seen miss-fall-back to empty label**: surprising silent
  data-loss for the first Observe per (route, label) pair. Hard
  to debug.
- **First-seen creates a slot under mutex inside `Observe`**:
  breaks the zero-mutex hot-path contract. Non-starter.

(Reviewer notes from PR #694: Claude bot critical, Gemini medium.)

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

Three layers gain a label field. They are kept in sync at PR-D+E
(both shipping together) so an aggregator running PR-D against an
upstream still on PR-C never sees a wire shape mismatch:

```diff
 // keyviz/sampler.go — internal type, available from PR-B
 type MatrixRow struct {
   RouteID      uint64
+  Label        string  // empty for legacy unlabeled traffic
   Start, End   []byte
   …
 }

 // proto/admin.proto — wire form, PR-D+E.
 //
 // Reuse the **existing** field 4 (`string label`), which is
 // already declared on KeyVizRow but currently unused — no
 // schema migration, no field-number bump. The existing comment
 // already reserves it for "future per-Observe label", which is
 // exactly this proposal's payload. (CodeRabbit critical on PR
 // #694 caught this — an earlier draft proposed adding a
 // duplicate `string label = 13;` which would have collided.)
 message KeyVizRow {
   string bucket_id = 1;
   bytes start = 2;
   bytes end = 3;
-  string label = 4;        // currently unused
+  string label = 4;        // PR-D+E: filled with the per-Observe label
   …
 }

 // internal/admin/keyviz_handler.go — JSON struct, PR-D+E
 type KeyVizRow struct {
   BucketID  string `json:"bucket_id"`
   …
+  Label     string `json:"label,omitempty"`
 }
```

We carry the label as a **dedicated field** (not just embedded in
`bucket_id`) so SPA consumers do not need to parse the composite
to recover it. The SPA renders `route:<id> / <label>` directly
from `KeyVizRow.Label` when non-empty; legacy rows (empty label)
render as `route:<id>` exactly like today. Carrying both
`bucket_id` and `label` is intentional redundancy: `bucket_id`
remains the globally-unique row identifier (used by
`pivotKeyVizColumns` and merge-side dedupe), while `label`
serves the SPA's render-and-filter ergonomics.

**`bucket_id` is composite when `Label` is non-empty**:

- Legacy (empty label): `bucket_id = "route:<id>"` — unchanged
  from the parent design.
- Labelled: `bucket_id = "route:<id>:<label>"` (e.g.
  `"route:1:dynamo"`).

Composite `bucket_id` keeps the field globally unique across
labeled rows. Without it, `pivotKeyVizColumns` (currently keyed
by `RouteID` alone in `internal/admin/keyviz_handler.go`) would
collapse `(routeID=1, label="dynamo")` and `(routeID=1,
label="redis")` into the same map entry — silently re-merging
the rows the feature is trying to split. The same uniqueness
property is what `applyKeyVizRowBudget` and `sortKeyVizRowsByStart`
need for deterministic tiebreak (both sort on `BucketID` last).

`pivotKeyVizColumns` widens its pivot key from `uint64` (RouteID)
to a struct of `(uint64, string)` to mirror the sampler's slot
key — same shape, same reasoning as §4.1.1.

We use `:` (not `/`) as the route-vs-label separator so a future
hierarchical label (§9 Q1) can use `/` without ambiguity.
`route:1:dynamo` is unambiguous; if labels later become
`dynamo/users` the composite becomes `route:1:dynamo/users` and
parsers split on the *first* `:` after `route`.

**Hard constraint on canonical labels**: a label MUST NOT contain
`:`. The constants in `keyviz/labels.go` (§9 Q2) are the only
source of label values, and we enforce this at the constants
file with a `func TestAllLabelsAvoidSeparator(t *testing.T)` test
that asserts `!strings.ContainsRune(l, ':')` for every member of
`AllLabels`. Without this constraint a future label like
`"redis:db0"` would silently break `bucket_id` parsing — the
parser would split at the wrong `:`. (CodeRabbit major on PR
#694.)

`route_ids` / `aggregate` / virtual-bucket semantics from §5 of
the parent design are unchanged — labels are an orthogonal axis
to the route-coarsening machinery (and §6.5 below pins the
virtual-bucket interaction).

Forward compatibility: an old SPA against a new server sees a
new `bucket_id` shape (`route:1:dynamo`) and renders it
literally — operators get a less-pretty label but no
correctness bug. A new SPA against an old server sees no `label`
field and `bucket_id = "route:1"`, falling back to the legacy
formatting. Both directions are non-breaking. (Reviewer notes
from PR #694: Claude bot critical, Gemini high.)

## 6. Aggregator merge changes

The fan-out aggregator's per-cell merge key gains the label:

- Phase 2-C (current): `(bucketID, raftGroupID, leaderTerm,
  windowStart)` per design `2026_04_27_proposed_keyviz_cluster_fanout.md`
  §4.
- With labels: same tuple — but `bucketID` itself now carries the
  label via the §5 composite (`route:1:dynamo`). The merge key
  width does **not** change; the new label dimension is
  encoded into `bucketID` so the aggregator already separates
  same-route different-label rows correctly.

Reads still sum, writes still max-with-conflict; nothing about
the merge rules changes other than the wire shape of `bucketID`.
This is what makes PR-D and PR-E shippable in either order
(see §7) — the wire-format change in PR-D *is* the merge-key
change.

### 6.5 Virtual buckets and labels

Routes that overflow `MaxTrackedRoutes` fold into a virtual
bucket (parent design §5.3). Virtual buckets aggregate counters
across every route they swallow, including across **labels** —
attempting to per-label-key a virtual bucket would defeat the
"single coarsened slot" property the bucket exists for.

Resolution: **virtual buckets always emit `Label = ""`**.
Operators who want per-adapter breakdown for their hot routes
must keep them under `MaxTrackedRoutes` (§7 PR-C calls this out).
Virtual-bucket rows in the heatmap render as
`virtual:<lineageID>` (no label suffix), exactly like today.

## 7. Implementation plan

| PR | Scope |
|---|---|
| **PR-A** | Land this design doc. |
| **PR-B** | Sampler API extension: `Observe(... label string)`, `routeSlot.Label`, slot-key type widens to `slotKey{uint64, string}`. **`RegisterRoute` only pre-creates the legacy empty-label slot** — labeled-sibling pre-creation lands in PR-C so PR-B is a behavior-neutral refactor (per-route slot count is unchanged at 1). PR-B creates `keyviz/labels.go` with the `slotKey` struct + an **empty `AllLabels` slice**; the five adapter constants (`LabelDynamo`, `LabelRedis`, …) are added in PR-C alongside the adapter wiring, so PR-B's pre-creation loop is a no-op against the empty slice and the existing single slot per route is created exactly as today. `MatrixRow.Label` field present but always empty. `reclaimRetiredSlot` dedupes on `(RouteID, Label)` (forward-prep; only `Label=""` exists). The `MaxTrackedRoutes` coarsening check (`keyviz/sampler.go:416`) divides by `len(AllLabels)+1` so the cap continues to count **routes**, not slots; with `AllLabels` empty in PR-B the divisor is 1 and behavior is identical to today. Update existing tests **and the coordinator call sites in `kv/sharded_coordinator.go`** (the only non-test caller of `Sampler.Observe`) to pass `label = ""`. |
| **PR-C** | Adapter wiring: each adapter sets its own label at the dispatch entry into `ShardedCoordinator.Observe…`. Extends `RegisterRoute` to pre-create the labeled siblings (one slot per `AllLabels` member) alongside the legacy empty-label slot from PR-B. Canonical constants live in `keyviz/labels.go` (NOT `adapter/keyviz_labels.go` — sampler-side imports must not climb back to the adapter package; sampler's `RegisterRoute` reads the canonical set at registration time). **Operator note**: with N labels, slot count grows from M routes to ~N×M; raise `--keyvizMaxTrackedRoutes` proportionally or hot routes will fold into the virtual bucket and lose per-label breakdown. PR-C emits a structured `slog.Warn` from inside `RegisterRoute` at the moment a route is coarsened — `slog.Warn("route folded into virtual bucket", route_id=…, slots_used=…, max_tracked_routes=…)` — so the operator sees the symptom in the log when it actually fires. A startup-time check would not catch this because routes are added dynamically (the catalog watcher calls `RegisterRoute` whenever a `SplitRange` lands), so a process that started below the cap can cross it later. **Rollout discipline**: raise `--keyvizMaxTrackedRoutes` to at least `current_routes × (len(labels) + 1)` *before* deploying the PR-C binary — the `+ 1` accounts for the legacy empty-label slot that lives alongside the N labeled siblings on every individually-tracked route. Operators sizing only with `× len(labels)` (one short per route) hit coarsening immediately and lose labeled breakdown anyway. (Codex P2 on round-5.) Otherwise an already-misconfigured node prints a wave of warnings on startup as the watcher re-registers every catalog route — loud but not incorrect, only painful for log retention. |
| **PR-D+E** | **Ship together.** Wire-format extension (proto + JSON `bucket_id` composite + optional `label`) plus the SPA `route:N / label` rendering AND the four single-node-path code changes the labeled wire format requires in `internal/admin/keyviz_handler.go`: <br>(a) `pivotKeyVizColumns` `rowsByID` map AND `order` slice both widen from `uint64` to the composite `BucketID string` (or equivalently `(uint64, string)`) — widening only the map without the `order` slice is a compile error and an obvious mismatch, but worth naming; <br>(b) `newKeyVizRowFrom` (`keyviz_handler.go:368`) copies `mr.Label → row.Label`, mirroring the `mergeRowInto` change for the fan-out path; <br>(c) `bucketIDFor` (`keyviz_handler.go:383`) returns the composite `"route:<id>:<label>"` when `mr.Label != ""`, falling back to the legacy `"route:<id>"` for empty labels — without this `BucketID` is non-unique and `applyKeyVizRowBudget` / `sortKeyVizRowsByStart` lose their deterministic tiebreak; <br>(d) `mergeRowInto` (`internal/admin/keyviz_fanout.go:509`) adds `dst.Label = row.Label` so the fan-out path doesn't drop the label on merge. Splitting any subset of (a)–(d) leaves a window where labeled rows collapse — they're a single PR. The fan-out merge key (`bucketID` composite) picks up the per-label separation automatically once `bucketIDFor` is updated. (Claude bot moderate on PR #694 round-4 + round-5.) |

PRs B and C are independent of the wire format; the heatmap will
keep showing one row per route until D+E ship, but the per-label
counts are already accumulating in the sampler so the wire
extension is "switch on the field". (Reviewer notes from PR
#694: Claude bot critical / moderate.)

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
   - **Slot pre-creation count**: register 1 route, assert
     `len(tbl.slots) == len(labels) + 1` (N labeled + 1 legacy
     empty-label slot). Catches a PR-B regression that skips a
     label constant in the pre-creation loop — without this the
     missing slot only surfaces when that adapter fires traffic.
   - **`RemoveRoute` symmetric teardown**: register 1 route,
     `RemoveRoute(routeID)`, assert `len(tbl.slots) == 0` and
     `len(retiredSlots) == len(labels) + 1`. Catches a PR-B
     regression where only the legacy slot is retired and the
     labeled siblings leak into the live table.
   - **`mergeRowInto` Label copy** (PR-D+E): two nodes, one with
     `KeyVizRow{Label: "dynamo"}`, one with `KeyVizRow{Label:
     "redis"}` for the same routeID. The merged response must
     have **two rows** with `Label="dynamo"` and `Label="redis"`
     respectively — not two rows with `Label=""`. Catches the
     mergeRowInto regression where Label is omitted from the
     `dst := &KeyVizRow{...}` construction.
   - **Canonical labels avoid `:`** (PR-B regression guard):
     range `AllLabels`, assert `!strings.ContainsRune(label,
     ':')` for every constant. Pins the §5 hard constraint at
     compile time; a future "redis:db0" would otherwise silently
     break `bucket_id` parsing.
   - **`reclaimRetiredSlot` (RouteID, Label) dedupe**: retire
     `(routeID, "dynamo")`, re-register `(routeID, "dynamo")`,
     assert reclaim succeeds; then retire `(routeID, "dynamo")`
     again, re-register as `(routeID, "redis")`, assert reclaim
     does **not** fire — the dynamo slot must stay in the
     retired list to be drained by its own grace window. Catches
     a PR-B regression where the implementor forgets to widen
     the dedupe key from `RouteID` alone.
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
   - Coarsened-route + label: a route folded into the virtual
     bucket emits `Label = ""` regardless of which label the
     `Observe` carries (§6.5).

## 9. Open questions

1. **Should the label be hierarchical** (`dynamo/users`) from
   day one, or restricted to a single segment now and extended
   later? Proposal: single segment now (cheapest sampler change),
   extend with a `/`-delimited convention later if adapters want
   sub-tenant attribution. The route-vs-label separator in
   `bucket_id` is `:` (`route:1:dynamo`), not `/`, so a future
   `dynamo/users` extension produces `route:1:dynamo/users`
   without parser ambiguity (split on the *first* `:` after
   `route`). (Gemini round-1 minor on PR #694.)
2. **Label allocation discipline** — who owns the canonical label
   set? Proposal: `keyviz/labels.go` exports `LabelDynamo`,
   `LabelRedis`, `LabelS3`, `LabelSQS`, `LabelRawKV` constants
   alongside an `AllLabels []string` slice that ranges over them
   in stable order. `RegisterRoute` iterates `AllLabels` to
   pre-allocate the labeled slots — exposing the slice (rather
   than reflecting over the file) keeps the iteration deterministic
   and the test surface small. Constants live in the `keyviz`
   package (not `adapter/`) so the sampler can read the full set
   at registration time without an import cycle back to `adapter`.
   Adapters refer to these constants; nothing stops a future
   adapter from inventing its own label, but the review burden
   of adding to the central file catches accidental variants
   like `"DynamoDB"` vs `"dynamo"`.
3. **Should the aggregator collapse same-route different-label
   rows for operators who don't want the breakdown?** Proposal:
   no — the SPA already lets operators pick which row to
   examine; the wire form should always carry the breakdown so
   the data is queryable.

## 10. Out of scope (explicit deferrals)

- Per-table / per-bucket / per-queue / per-Redis-DB sub-labels.
- Operator-configurable label taxonomy.
- Persistence of labeled rows (Phase 3 covers persistence
  generally; labels ride along once the persistence path lands).
- Adapter-aware splitting of routes (`SplitRange` triggered by
  adapter-label hotspots) — that is a Phase 3+ idea.
