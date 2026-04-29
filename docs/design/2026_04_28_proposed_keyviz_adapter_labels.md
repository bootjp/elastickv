---
status: proposed
phase: 2-C+
parent_design: docs/admin_ui_key_visualizer_design.md
author: bootjp
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
- Minimal hot-path penalty when labels are **not** configured: a
  default deployment incurs **no additional lookups and no
  additional allocations**, with only the slightly wider hash
  and equality work that comes from the `slotKey{uint64, Label}`
  map key compared to today's bare `uint64`. (Copilot
  round-15-4th-pass observed that the original "zero penalty"
  framing was overstated — the key shape widens regardless of
  whether labels are enabled.)
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

### 4.1 (Recommended) Per-Observe label

Extend the sampler signature with a typed `Label` (defined as
`type Label string` in `keyviz/labels.go`, see §9 Q2):

```go
Observe(routeID uint64, op Op, keyLen, valueLen int, label Label)
```

Empty `label` (`LabelLegacy`) → existing behaviour, single row
per route. Adapters set their own canonical constant
(`LabelDynamo`, `LabelRedis`, …) at the dispatch site they
already own — typed constants prevent typos like `"DynamoDB"` /
`"dynamo"` drift at compile time.

Cost on hot path: still one `slots` map lookup per `Observe` on
the hit path (the existing code already does a single
`tbl.slots[routeID]` lookup, with a fallback into
`virtualForRoute` only on miss). What changes is the **key
shape**: `(routeID, label)` instead of just `routeID`, so the
map key is a `{uint64, Label}` struct — slightly wider hash and
equality work per lookup. `label` is a small interned constant
(`"dynamo"`), so the lookup itself allocates nothing. The miss
rate is unchanged because PR-C+D+E pre-creates a slot for every
member of `AllLabels` at `RegisterRoute` time, so labeled
`Observe` calls hit the same way today's unlabeled calls do.
(Copilot round-15 caught the earlier "one extra map lookup"
phrasing — the lookup count is unchanged; only the key shape
widens.)

Storage: each (route, label) gets its own `routeSlot` **when
labels are enabled** (`--keyvizLabelsEnabled=true`). Total slot
memory is `MaxTrackedRoutes × (len(AllLabels)+1)` with labels
enabled, and `MaxTrackedRoutes × 1` with labels disabled (legacy
slot only — same number of slots as today's baseline). The
pre-allocation cost is gated on the flag, so a cluster that
deploys the PR-C+D+E binary but leaves the flag off avoids the
**multiplicative** `len(AllLabels)+1` slot-count increase, though
each slot's map key is `slotKey{uint64, Label}` (a struct with a
string header) instead of a bare `uint64`, so there is a small
per-slot overhead vs today even with the flag off. The dominant
memory cost — the multiplicative slot count — is the part the
flag actually gates (Codex round-15-4th-pass P1; Copilot round-23
caught the "no memory penalty" overstatement).
`MaxTrackedRoutes` retains its route-counting semantics under
Option A (§4.1.1), so no operator action is required at PR-C+D+E
rollout. (Vestigial "when operators raise it for label use"
framing was rejected — Codex round-10 P2 + Claude bot round-11.)

#### 4.1.1 Slot lifecycle and the lockless invariant

The current `Observe` is a lockless `Load → lookup → atomic.Add`
hot path. On a miss in `tbl.slots[...]`, it may fall back to
`tbl.virtualForRoute[...]`; if both lookups miss, the sample is
silently dropped. The invariant is **not** "no fallback" — it is
"`Observe` never allocates and never creates slots on the hot
path". `RegisterRoute` pre-creates every slot before traffic
arrives, so the hot path never needs to allocate. We keep that
invariant by making `RegisterRoute` **pre-create one slot per
known label** at registration time when `--keyvizLabelsEnabled=true`,
but only on the individual-tracking path. (Copilot round-15-3rd-pass
caught the earlier "no fallback" wording — the fallback exists today
and must be preserved; what stays unchanged is the no-allocate
property.) The label set is the canonical `keyviz/labels.go`
constants (§9 Q2); per-route slot count is `len(labels) + 1` when
labels are enabled (the +1 is the empty-label legacy slot, kept so
callers that pass `label=""` still hit a slot) and `1` (legacy slot
only) when labels are disabled. Total individual slots in the table
is therefore `len(routes) × (len(labels) + 1)` with labels enabled
and `len(routes) × 1` with labels disabled. (Codex round-15-4th-pass
P1 caught the earlier framing that pre-allocated labeled siblings
even when the flag was off — that imposed an unnecessary multi-x
memory penalty on clusters that never enabled labels.)

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
- After PR-C with `--keyvizLabelsEnabled=true`, slot count per
  route grows to `len(labels) + 1`. Operators **do not** need to
  raise `--keyvizMaxTrackedRoutes` — the §4.1.1 Option A
  coarsening check divides slot count by `slotsPerRoute` (which
  is `len(AllLabels)+1` when the flag is on, `1` when off), so
  the operator-set cap continues to mean "individual routes."
  (An earlier draft told operators to scale the cap; that
  contradicted Option A. Codex round-10 P2.)
- After PR-C with `--keyvizLabelsEnabled=false` (the default
  immediately after deploying the bundled binary), slot count per
  route is **1** (legacy slot only) — `RegisterRoute` skips
  labeled-sibling pre-creation, so memory matches today's
  baseline. The flag-flip restart introduces the labeled siblings.
  (Codex round-15-4th-pass P1.)

Concretely PR-B's `RegisterRoute` body looks like:

```go
// PR-B body:
slot := s.reclaimRetiredSlot(routeID, "") // (RouteID, Label) dedupe — PR-B widened the signature
if slot == nil {
    slot = newSlot(routeID, "", start, end, groupID)
}
next.slots[slotKey{RouteID: routeID, Label: ""}] = slot

// PR-C extends to (flag-gated, Codex round-15-4th-pass P1):
labelsToCreate := []Label{LabelLegacy} // legacy slot always
if s.opts.KeyVizLabelsEnabled {
    labelsToCreate = allLabelsWithLegacy() // AllLabels ∪ {LabelLegacy}, fresh slice
}
for _, label := range labelsToCreate {
    slot := s.reclaimRetiredSlot(routeID, label) // reclaim per (routeID, label), not just legacy
    if slot == nil {
        slot = newSlot(routeID, label, start, end, groupID)
    }
    next.slots[slotKey{RouteID: routeID, Label: label}] = slot
}
```

`allLabelsWithLegacy()` is an unexported helper in
**`keyviz/labels.go`** (same package as `sampler.go`) that returns
a **freshly allocated slice** containing `AllLabels` followed by
`LabelLegacy`. Concretely: `result := make([]Label,
len(AllLabels), len(AllLabels)+1); copy(result, AllLabels[:]);
result = append(result, LabelLegacy)` — pre-size the backing
array via the capacity hint so the final length is exactly
`len(AllLabels)+1` with one allocation. The explicit `AllLabels[:]`
re-slice is required because §9 Q2 declares `AllLabels` as a
fixed-size array (`[5]Label`) and Go's `copy` builtin needs slice
arguments. Naively returning `append(AllLabels[:], LabelLegacy)`
would be a footgun — Go's `append` against a slice-of-array may
share storage with the underlying array, which would then visibly
mutate the canonical `AllLabels` (silently overwriting the legacy
entry into the array's tail or, more commonly, panicking because
the array is full). Co-locating the helper with `AllLabels` keeps the
canonical set in one file and avoids `sampler.go` reconstructing
the same expansion at every call site. The function is
unexported because no caller outside `package keyviz` needs it.
(Claude bot round-9 minor; Copilot round-15-4th-pass added the
fresh-slice safety requirement; Copilot round-19 caught the
off-by-one in the prior "length len(AllLabels)+1, then append"
phrasing — that produced length `len(AllLabels)+2`. Now uses
length `len(AllLabels)`, capacity `len(AllLabels)+1`, then
append, yielding the intended length `len(AllLabels)+1`.)

When `RegisterRoute` decides a route will be coarsened into a
virtual bucket (i.e. it returns `false`), **no labeled slots are
created** for that route — the `tbl.virtualForRoute[routeID]`
fallback in `Observe` already handles all labels uniformly per
§6.5 (virtual buckets always emit `Label = ""`). Pre-creating
labeled slots that would never be hit would just burn allocator
work.

#### `MaxTrackedRoutes` semantics: count routes, not slots

After the slot-key widens to `slotKey{uint64, Label}`, the live
table contains `len(AllLabels) + 1` slots per individually-tracked
route when `--keyvizLabelsEnabled=true` (and `1` slot per route
when the flag is off) — bare `len(next.slots)` no longer equals
"number of routes." We pick **Option A (route-counting)**: the
coarsening check at `keyviz/sampler.go:416` divides by
`slotsPerRoute`, which is `len(AllLabels) + 1` when the flag is
enabled and `1` when off. `MaxTrackedRoutes` continues to count
routes, not slots, in both regimes. This preserves the operator-
visible meaning of the existing flag — `--keyvizMaxTrackedRoutes=1024`
still means "1024 individual routes," not "1024 slots that may be
~170 routes." Total slot memory is `MaxTrackedRoutes ×
slotsPerRoute` (i.e. `MaxTrackedRoutes × (len(AllLabels) + 1)`
when labels are enabled, `MaxTrackedRoutes × 1` when off), which
we document but never count against the cap.

```go
// keyviz/sampler.go:416 (PR-B + PR-C+D+E flag-gated, Codex round-15-4th-pass P1)
slotsPerRoute := 1 // legacy slot only (PR-B, or PR-C+D+E with flag off)
if s.opts.KeyVizLabelsEnabled {
    slotsPerRoute = len(AllLabels) + 1 // 6 after PR-C with flag on (in-package, no keyviz. qualifier)
}
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
    Label   Label          // new — empty for the legacy unlabeled slot, typed via keyviz/labels.go
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
pre-creation loop runs under the `routesMu` lock).

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

**Recommendation: Option 4.1.** Lowest plumbing weight, and the
label originates where it is most naturally available (the
adapter's dispatch entry). The hot path remains a **single** map
lookup per `Observe`; only the lookup key widens (so the change
is wider hash and equality work, not an additional lookup). The
`routeSlot` map shape changes from `map[uint64]*routeSlot` to
`map[slotKey]*routeSlot` with `slotKey = {uint64, Label}`.
(Copilot round-19 caught the earlier "smallest hot-path delta
(one map lookup)" wording, which could be misread as adding an
extra lookup.)

## 5. Wire format extension

Three layers gain a label field. They are kept in sync at PR-D+E
(both shipping together) so an aggregator running PR-D against an
upstream still on PR-C never sees a wire shape mismatch:

```diff
 // keyviz/sampler.go — internal type, available from PR-B.
 // MatrixRow.Label uses the typed `keyviz.Label` so adapters
 // referencing it via the `Sampler` interface can't accidentally
 // hand-write a string literal that drifts from the canonical
 // constant set. The proto and JSON shapes below use plain `string`
 // because the wire form is operator-visible and we want to allow
 // unknown labels through without a schema bump (an old binary
 // returning a label a new SPA hasn't seen yet still renders).
 type MatrixRow struct {
   RouteID      uint64
+  Label        Label   // typed; see keyviz/labels.go
   Start, End   []byte
   …
 }

 // proto/admin.proto — wire form, PR-D+E.
 //
 // Reuse the **existing** field 4 (`string label`), which is
 // already declared on KeyVizRow but currently unused and
 // undocumented (no doc comment on the field) — no schema
 // migration, no field-number bump.
 // The current proto declares `string label = 4;` with no
 // doc comment, and the nearby `bucket_id` comment only mentions
 // the legacy `route:<id>` / `virtual:<id>` forms. PR-D+E starts
 // filling the field AND updates the proto comments in the same
 // commit (both the new label-field doc comment and the
 // `bucket_id` comment so it lists the composite
 // `route:<id>:<label>` form). (CodeRabbit critical on PR
 // #694 caught the duplicate-field-13 mistake — an earlier draft
 // proposed adding `string label = 13;` which would have collided
 // with the existing field 4. Copilot review surfaced the
 // related "comment already reserves it" misstatement — there is
 // no such reserving comment yet; PR-D+E adds it.)
 message KeyVizRow {
   string bucket_id = 1;
   bytes start = 2;
   bytes end = 3;
-  string label = 4;        // (no doc comment today)
+  string label = 4;        // PR-D+E: per-Observe label; empty for legacy / virtual rows
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
- Labeled: `bucket_id = "route:<id>:<label>"` (e.g.
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
to the **composite `BucketID` string** — matching the fan-out's
existing `rowsByBucket map[string]*KeyVizRow` so both paths key on
the same value (`"route:1:dynamo"`). An equivalent struct-of-
`(uint64, string)` would work but adds a conversion step at the
fan-out boundary; the string form is simpler and consistent with
the rest of the handler's surface area. Same shape, same
reasoning as §4.1.1.

We use `:` (not `/`) as the route-vs-label separator so a future
hierarchical label (§9 Q1) can use `/` without ambiguity.
`route:1:dynamo` is unambiguous; if labels later become
`dynamo/users` the composite becomes `route:1:dynamo/users` and
parsers split on the *first* `:` after `route`.

**Hard constraint on canonical labels**: a label MUST NOT contain
`:`. The constants in `keyviz/labels.go` (§9 Q2) are the only
source of label values, and we enforce this at the constants
file with a `func TestAllLabelsAvoidSeparator(t *testing.T)` test
that asserts `!strings.ContainsRune(string(l), ':')` for every member of
`AllLabels`. Without this constraint a future label like
`"redis:db0"` would silently break `bucket_id` parsing — the
parser would split at the wrong `:`. (CodeRabbit major on PR `#694`.)

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
the merge **rules** changes other than the wire shape of
`bucketID`. The merge **key** logic is unchanged: composite
`bucketID` already separates same-route different-label rows
correctly, so the aggregator's bucketing/grouping path needs no
edits.

The merge **value** path, however, still needs one explicit
field copy: `mergeRowInto` (`internal/admin/keyviz_fanout.go:509`)
must add `dst.Label = row.Label` so the label propagates from the
per-node `KeyVizRow` into the merged output. Without that copy,
labels work on single-node responses but are silently dropped
(`Label = ""`) after fan-out merge — the feature would be invisible
in clustered deployments. This is item (d) ("second-level Label
copy") in §7 PR-C+D+E. PR-D and PR-E are still bundled as a single
PR-D+E because the wire-format and pivot-key changes are
inseparable; the `mergeRowInto` value copy is part of the same
bundle. (An earlier draft claimed PR-D and PR-E were "shippable in
either order" — contradicted §7's "ship together" requirement;
resolved in favour of §7. Codex round-6. Codex round-13 caught
the related "merge logic needs no changes" wording, which had
implied no `mergeRowInto` edit at all.)

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
| **PR-B** | Sampler API extension: extend the `Sampler` interface declaration (`keyviz/sampler.go:74`) AND the `MemSampler.Observe` implementation to `Observe(routeID uint64, op Op, keyLen, valueLen int, label Label)` (Claude bot round-8 minor: surfaces immediately as a compile error if any mock implementer is missed, but naming it keeps the scope complete). `routeSlot.Label`, slot-key type widens to `slotKey{uint64, Label}`. **`RegisterRoute` only pre-creates the legacy empty-label slot when `--keyvizLabelsEnabled=false` (the default)** — labeled-sibling pre-creation is gated on the flag (round-17), so PR-B with the default flag is a behavior-neutral refactor (per-route slot count is unchanged at 1). **PR-B creates `keyviz/labels.go`** with the `slotKey` struct, the `type Label string` definition, the `LabelLegacy`/`LabelDynamo`/`LabelRedis`/etc. **typed constants**, and `AllLabels` as a **fixed-size array** `[5]Label{LabelDynamo, ..., LabelRawKV}` (round-23 — array form prevents importing-package mutation; see §9 Q2). The constants must exist at compile time so `RemoveRoute`'s `allLabelsWithLegacy()` teardown loop and `RegisterRoute`'s pre-creation loop both compile; with the flag default to false, `RegisterRoute` skips the labeled-sibling loop body so per-route slot count stays at 1 (Claude bot round-6 + round-23 array correction). `MatrixRow.Label` field present. **L0 label copy** (Claude bot round-8 moderate): update `appendDrainedRow` (`keyviz/sampler.go:828`) to include `Label: slot.Label` in the `MatrixRow` struct literal — `slot.Label` is set once at `RegisterRoute` time and immutable, so read it directly rather than threading through `snapshotMeta`. This is the first link in the propagation chain: `slot.Label` → `MatrixRow.Label` (L0, here) → `KeyVizRow.Label` via `newKeyVizRowFrom` (L1, PR-D+E) → merged `KeyVizRow.Label` via `mergeRowInto` (L2, PR-D+E). Missing L0 silently truncates the chain and leaves `MatrixRow.Label = ""` even after PR-C+D+E wires the adapters to pass labels; L1 and L2 then faithfully copy the empty string and the feature collapses. With PR-B's flag-default-false (only legacy slots pre-created), `slot.Label` is `""` so the emitted row is `""`-labeled (identical to today's behavior), but the wiring is in place for PR-C+D+E to flip the flag and start emitting labels. `reclaimRetiredSlot` dedupes on `(RouteID, Label)` (forward-prep; only `Label=""` exists). The `MaxTrackedRoutes` coarsening check (`keyviz/sampler.go:416`) divides by `slotsPerRoute` (= `len(AllLabels)+1` when the flag is on, `1` when off; round-17 + round-23) so the cap continues to count **routes**, not slots; with the flag default to false in PR-B the divisor is 1 and behavior is identical to today (Claude round-24 fix to the prior "AllLabels empty in PR-B" framing — round-23 made AllLabels populated from PR-B, so the divisor=1 reason is the flag, not the array length). Update existing tests **and the coordinator call sites in `kv/sharded_coordinator.go`** (the only non-test caller of `Sampler.Observe`) to pass `label = ""`. |
| **PR-C+D+E** | **Bundled — must ship together.** PR-C (adapter wiring + `AllLabels` population) and PR-D+E (wire format + pivot-key widening) are not separately shippable: once an adapter calls `Observe(..., LabelDynamo)`, the sampler emits multiple `MatrixRow` per route in a single column (one per label); without the pivot-key widening from PR-D+E, `pivotKeyVizColumns` and `matrixToProto` collapse those rows back into a single `RouteID`-keyed entry where each labeled row overwrites the previous one — non-deterministic data loss in the intermediate state. Earlier drafts framed PR-C and PR-D+E as separately shippable; that was wrong (Codex round-10 P1). The bundle covers: <br>**Adapter wiring (was PR-C)**: each adapter sets its label at the `ShardedCoordinator.Observe…` dispatch entry. The five canonical constants (`LabelDynamo`, `LabelRedis`, `LabelS3`, `LabelSQS`, `LabelRawKV`) are already declared in `keyviz/labels.go` from PR-B as part of the `[5]Label` array (round-23 — see §9 Q2 and the PR-B row above); PR-C+D+E only wires the adapters to pass them, no `AllLabels` modification needed (Claude round-24 fix to the prior "populate empty slice" framing). Extend `RegisterRoute` to pre-create the labeled siblings (one slot per `AllLabels` member) **only when `--keyvizLabelsEnabled=true`**; with the flag off, `RegisterRoute` continues to pre-create just the legacy empty-label slot, identical to today's behavior (Codex round-15-4th-pass P1). **MaxTrackedRoutes is unchanged in semantics** — the §4.1.1 coarsening check divides slot count by `slotsPerRoute` (`len(AllLabels)+1` when the flag is on, `1` when off), so the existing operator-set cap continues to mean "individually-tracked routes" exactly as before; **operators do not need to raise `--keyvizMaxTrackedRoutes`** (an earlier draft told operators to scale the cap by `× (len(labels)+1)` — that contradicted Option A; Codex round-6). Memory growth: `MaxTrackedRoutes × (len(AllLabels) + 1)` slots when labels are enabled, `MaxTrackedRoutes × 1` when off (today's baseline); documented but not capped. Emit `slog.Warn` from inside `RegisterRoute` when a route coarsens. <br>**Wire-format extension (was PR-D+E)**: proto + JSON `bucket_id` composite + optional `label` field, plus the SPA `route:N / label` rendering AND the **five** code changes across the three response paths: <br>**Single-node JSON path (`internal/admin/keyviz_handler.go`)**: <br>(a) `pivotKeyVizColumns` `rowsByID` map AND `order` slice both widen from `uint64` to the composite `BucketID string` — widening only the map without the `order` slice is a compile error; <br>(b) `newKeyVizRowFrom` (`keyviz_handler.go:368`) copies `mr.Label → row.Label` via an explicit `string(mr.Label)` cast (`MatrixRow.Label` is the typed `keyviz.Label`; `KeyVizRow.Label` is plain `string` for wire-format flexibility — Claude bot round-10 minor) — **first-level** Label copy, affects single-node and cluster deployments alike; <br>(c) `bucketIDFor` (`keyviz_handler.go:383`) returns the composite `"route:<id>:<label>"` when `mr.Label != ""`, falling back to the legacy `"route:<id>"` for empty labels — without this `BucketID` is non-unique and `applyKeyVizRowBudget` / `sortKeyVizRowsByStart` lose their deterministic tiebreak; <br>**Fan-out JSON path (`internal/admin/keyviz_fanout.go`)**: <br>(d) `mergeRowInto` (`keyviz_fanout.go:509`) adds `dst.Label = row.Label` — **second-level** Label copy, only the cluster fan-out path touches this; <br>**gRPC path (`adapter/admin_grpc.go`)**: <br>(e) `matrixToProto` (`admin_grpc.go:599`) and the per-row conversion it drives: (e1) **widen `rowsByID` (line 603) and `order` (line 604) from `uint64` to the composite `BucketID string` key** — same widening as item (a); without it `(routeID=1, label="dynamo")` and `(routeID=1, label="redis")` collapse to the same map entry; (e2) copy `MatrixRow.Label → KeyVizRow.label` (proto field 4) via `string(mr.Label)` cast (same typed→untyped reasoning as item (b)); (e3) emit composite `bucket_id` (`"route:<id>:<label>"`). Without (e1)–(e3), `GetKeyVizMatrix` gRPC clients receive collapsed unlabeled rows even though HTTP/SPA responses now show per-label rows. <br>All five copies are required; missing any one leaves a flavour of deployment with empty labels. Splitting the bundle into separate PR-C and PR-D+E was the original framing but is now rejected (see opening paragraph of this row). <br>**Operator-controlled rollout gate (rolling-upgrade safety)**: a normal rolling upgrade temporarily mixes nodes that emit legacy `route:<id>` rows with nodes that emit labeled `route:<id>:<label>` rows. The fan-out aggregator keys strictly by `BucketID` in `mergeRowInto`, so those rows do **not** merge — operators would see fragmented unlabeled-plus-labeled data per route until every node converges. PR-C+D+E adds `--keyvizLabelsEnabled` (default `false`) on every node; when false, the **`ShardedCoordinator` overrides the adapter-supplied label to `keyviz.LabelLegacy` at the single `sampler.Observe(...)` call site** in `kv/sharded_coordinator.go` (one `if !s.keyvizLabelsEnabled { label = keyviz.LabelLegacy }` guard, not a 5-file flag-read duplication across adapters; Claude bot round-12 moderate). The bundled binary is therefore safe to roll out one node at a time — every node, mixed or fully upgraded, emits the legacy format. <br>**Flag-flip activation**: once the fleet is fully on the new binary, the operator flips `--keyvizLabelsEnabled=true`. The flag is a startup-only `flag.Bool` (no live-toggle / config-reload path; Claude bot round-12 minor); changing it requires a process restart. There are two restart strategies: (a) **simultaneous restart of all nodes** — KeyViz heatmap is briefly unavailable during the restart window but no mixed-format fragmentation occurs; (b) **rolling restart** — for the duration of the restart (typically minutes), the heatmap shows a transient mixed view because the legacy `route:N` rows from not-yet-restarted nodes don't merge with `route:N:label` rows from restarted nodes. Since KeyViz is a monitoring view (not a consistency-sensitive system), the rolling-restart fragmentation is acceptable and clears as the final node restarts; operators who want zero fragmentation should use the simultaneous restart. (Claude bot round-12 moderate.) <br>**Flag also gates pre-allocation**: `--keyvizLabelsEnabled` is **both** the traffic-routing toggle (override label to `LabelLegacy` at the coordinator) **and** the slot-pre-creation toggle. When `false`, `RegisterRoute` pre-creates only the legacy empty-label slot (`len(next.slots) += 1` per route, identical to today's behavior); labeled siblings are not allocated. When `true`, `RegisterRoute` pre-creates the legacy slot **and** one labeled sibling per `AllLabels` member (`len(next.slots) += len(AllLabels)+1` per route). Memory therefore stays at the today's level (`MaxTrackedRoutes × 1`) for clusters that deploy the bundled binary but leave the flag off, and grows to `MaxTrackedRoutes × (len(AllLabels)+1)` only when the operator opts in. The flag-flip activation requires a process restart (see preceding paragraph), and `RegisterRoute` re-runs at startup, so the new pre-creation regime is in effect immediately after the restart — there is no live re-allocation path needed. **An earlier draft separated memory and traffic-routing into two regimes ("memory-vs-flag separation", round-12) where labeled slots were pre-created regardless of the flag; that was wrong (Codex round-15-4th-pass P1) — it imposed a multi-x memory penalty on clusters that never enable labels, contradicting the §2.1 minimal-penalty goal.** (Codex round-11 P2 originated this rollout gate.) |

PR-B is independent of the bundled PR-C+D+E: PR-B widens types
and prepares the slot machinery without changing observable
behavior (legacy slots only, `AllLabels` is the populated
`[5]Label` array but `--keyvizLabelsEnabled` defaults to false
so `RegisterRoute` skips the labeled-sibling loop and the
`MaxTrackedRoutes` divisor stays at 1; round-23 array form, see
§9 Q2). The bundled PR-C+D+E then ships everything
operator-visible — adapter wiring (constants are already
declared in `keyviz/labels.go` from PR-B; PR-C+D+E only wires
adapters to pass them at the dispatch entry, no change to
`AllLabels` itself), all five wire-format copies — in a single
atomic deploy. (Reviewer notes from PR #694: Claude bot critical
/ moderate + Codex round-10 + Claude round-24 fix.)

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
3. **Performance** — The hot path still does one map lookup per
   `Observe` (same as today); the only extra work is wider hash
   and equality on the `{uint64, Label}` key vs the current
   `uint64` key. `BenchmarkObserveParallel` already pins the
   hot-path cost. The acceptance check is **no statistically
   significant regression** versus a same-run baseline (or a
   recorded benchmark run attached to the PR), not an absolute
   ns/op target — `BenchmarkObserveParallel` results vary by
   machine, Go version, and CPU thermal state. If the benchmark
   regresses materially, the design is wrong and we fall back to
   Option 4.2.
4. **Data consistency** — Cluster fan-out merge gains a tuple
   field; the dedup invariant (per-cell, per-(route, label,
   group, term, window)) still holds. Old SPA against new server
   sees the label-collapsed view; new SPA against old server
   sees the legacy view; both are coherent.
5. **Test coverage** — New test categories:
   - **Slot pre-creation count (PR-C, post-`AllLabels`
     population)**: register 1 route, assert
     `len(tbl.slots) == len(AllLabels) + 1`. Catches a PR-C
     regression that skips a label constant in the pre-creation
     loop — without this the missing slot only surfaces when
     that adapter fires traffic. **PR-B counterpart** (with
     empty `AllLabels`): register 1 route, assert
     `len(tbl.slots) == 1` (just the legacy slot); pins
     PR-B's behavior-neutral refactor goal.
   - **`RemoveRoute` symmetric teardown (PR-C)**: register 1
     route, `RemoveRoute(routeID)`, assert `len(tbl.slots) == 0`
     and `len(retiredSlots) == len(AllLabels) + 1`. Catches a
     PR-C regression where only the legacy slot is retired and
     the labeled siblings leak into the live table. **PR-B
     counterpart** (empty `AllLabels`): register 1 route,
     `RemoveRoute(routeID)`, assert
     `len(retiredSlots) == 1` — the loop body executes once
     for the empty-label slot only.
   - **`mergeRowInto` Label copy** (PR-D+E): two nodes, one with
     `KeyVizRow{Label: "dynamo"}`, one with `KeyVizRow{Label:
     "redis"}` for the same routeID. The merged response must
     have **two rows** with `Label="dynamo"` and `Label="redis"`
     respectively — not two rows with `Label=""`. Catches the
     mergeRowInto regression where Label is omitted from the
     `dst := &KeyVizRow{...}` construction.
   - **Canonical labels avoid `:`** (PR-B regression guard —
     the `[5]Label` array is populated from PR-B (round-23, see
     §9 Q2) so the test is live immediately, not a no-op until
     PR-C): range `AllLabels`, assert
     `!strings.ContainsRune(string(label), ':')` for every
     constant — explicit `string(label)` cast required because
     `AllLabels` is the typed `[5]Label` array and
     `strings.ContainsRune` takes `string`. Prevents anyone
     from later changing a constant to a `:`-containing literal
     without the test turning red. (Claude round-24 corrected
     the stale "PR-C regression guard, written in PR-B" label
     — with the populated array, the test is a live PR-B test.)
     Pins the §5 hard constraint at compile time; a future
     "redis:db0" would otherwise silently break `bucket_id`
     parsing.
   - **All declared label constants are in `AllLabels`**
     (PR-B regression guard — the `[5]Label` array is populated
     from PR-B (round-23, see §9 Q2), so this is a live test
     from PR-B onward, not a no-op): range over the named
     constants (`LabelDynamo`, `LabelRedis`, …) and assert each
     appears in `AllLabels`. Catches the mistake where a future
     adapter PR adds `LabelNewAdapter = "newadapter"` to
     `keyviz/labels.go` but forgets to update `AllLabels`'s
     literal — the slot pre-creation loop wouldn't iterate over
     it, `Observe(..., LabelNewAdapter)` would miss `tbl.slots`,
     and all KeyViz samples for that adapter would silently drop
     in production. (Claude bot round-9; Claude round-24
     removed the dead `len(AllLabels) == 0` skip — with the
     populated array, length is always 5 at compile time, so
     the guard is dead code.)
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
   set? Proposal: `keyviz/labels.go` defines a **typed string**
   `type Label string` and exports the canonical constants of that
   type:

   ```go
   type Label string
   const (
       LabelLegacy Label = ""        // legacy / unlabeled traffic, coordinator default
       LabelDynamo Label = "dynamo"
       LabelRedis  Label = "redis"
       LabelS3     Label = "s3"
       LabelSQS    Label = "sqs"
       LabelRawKV  Label = "rawkv"
   )
   // Fixed-size array prevents the most common mutation pattern
   // (`append(keyviz.AllLabels, extra)` is a type error since
   // AllLabels is [5]Label, not []Label). Element reassignment
   // (`keyviz.AllLabels[0] = ...`) and full-var reassignment
   // (`keyviz.AllLabels = [5]Label{...}`) still compile against
   // an exported `var`, so they are not a compile-time guarantee
   // — but they require deliberate intent and would be caught
   // in code review and by the TestAllLabelConstantsInAllLabels
   // regression guard from §8 (which fails if any declared
   // constant is missing from AllLabels). The combination of
   // type-system block on `append` + runtime test on contents
   // is the round-23 enforcement story; an unexported var with
   // an accessor function returning a fresh copy was considered
   // and rejected because the array gives the same `append`
   // protection at zero per-iteration allocation cost.
   // (Copilot round-23 caught the exported-mutable-slice
   // footgun; Claude round-24 corrected the overstated
   // "mutation is a compile error" claim.)
   var AllLabels = [5]Label{LabelDynamo, LabelRedis, LabelS3, LabelSQS, LabelRawKV}
   ```

   `Observe` takes `label Label` (not `string`) and `RegisterRoute`
   iterates `AllLabels` for pre-allocation. Constants live in the
   `keyviz` package (not `adapter/`) so the sampler can read the
   full set at registration time without an import cycle back to
   `adapter`. `AllLabels` is intentionally a **fixed-size array**
   (`[5]Label`) rather than a slice — the most common mutation
   pattern (`append(keyviz.AllLabels, extra)`) becomes a
   compile-time type error since `AllLabels` is `[5]Label`, not
   `[]Label`. Element reassignment
   (`keyviz.AllLabels[0] = keyviz.Label("hacked")`) and full-var
   reassignment (`keyviz.AllLabels = [5]Label{...}`) still
   compile against an exported `var`, so the type system is not
   a complete guard — but they require deliberate intent, would
   be caught in code review, and the runtime
   `TestAllLabelConstantsInAllLabels` from §8 fails if any
   declared constant goes missing. (Claude round-24 corrected
   the prior overstated "mutation is a compile error" claim.)
   Iteration uses `range AllLabels` (works identically for
   arrays and slices) and `len(AllLabels)` returns 5 at compile
   time. The
   `allLabelsWithLegacy()` helper converts to a freshly allocated
   slice via `make([]Label, len(AllLabels), len(AllLabels)+1);
   copy(result, AllLabels[:]); result = append(result, LabelLegacy)`
   — the explicit `AllLabels[:]` re-slice is required because Go's
   `copy` builtin needs slice arguments. Bumping `AllLabels` length
   from 5 (when adding a new adapter) requires a deliberate doc +
   array-size edit, which is the intended friction; the
   `TestAllLabelConstantsInAllLabels` regression guard from §8
   then verifies the new constant is present in `AllLabels`.

   **PR sequencing note**: §7 PR-B previously declared `AllLabels`
   as a *deliberately-empty slice* (`var AllLabels []Label{}`) to
   make the pre-creation loop a no-op until PR-C populated it.
   With the round-23 array form, AllLabels is non-empty from PR-B
   onward — but the round-17 flag-gating means PR-B's
   `RegisterRoute` only pre-creates labeled siblings when
   `--keyvizLabelsEnabled=true`, which defaults to false. So
   PR-B remains behavior-neutral via the flag, not via an empty
   slice. The §7 PR-B row already mentions the flag default; this
   is the same mechanism reused.

   **Why a typed `Label` rather than plain `string`** (Codex round-9):
   slot pre-creation iterates `AllLabels`, and `Observe` drops slot
   misses silently — so any adapter that emits a label not in
   `AllLabels` would silently lose its KeyViz samples. Earlier
   drafts said "review burden catches accidental variants
   (`DynamoDB` vs `dynamo`)" — that's a social contract, exactly
   the kind of invariant a type system should enforce. With
   `Label` as a named type, the natural call shape is
   `s.Observe(..., keyviz.LabelDynamo)`; a typo
   `keyviz.LabelDynamoDB` is a compile error because no such
   constant exists. (Untyped string literals still auto-convert
   to `Label`, so `s.Observe(..., "dynamo")` still compiles —
   the type system doesn't fully prevent that path. Closing it
   completely needs an additional vet-style static check that
   greps `Observe(...)` call sites for non-constant labels; that
   is a Phase-2-C+-or-later follow-up, not blocking PR-B.)

   The result: typo'd constants fail at compile time (the high-
   probability mistake), and the `AllLabels` slice acts as the
   single source of truth for both `RegisterRoute`'s pre-creation
   loop and any future "is this label known?" check.
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
