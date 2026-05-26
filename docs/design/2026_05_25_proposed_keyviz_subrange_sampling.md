---
status: proposed
phase: 2-A+
parent_design: docs/admin_ui_key_visualizer_design.md
author: bootjp
date: 2026-05-25
---

# KeyViz Sub-Range (Hot-Key) Sampling (Phase 2-A+)

## 1. Background

The Key Visualizer's purpose is the one BigTable's tool serves:
surface **hot keys / hot sub-ranges** so an operator can see *where*
in the key space load concentrates and react (re-shard, salt a
sequential prefix, change a schema). The current sampler cannot do
this at the granularity the name implies.

Today the sampler keys every counter on **RouteID**
(`keyviz.MemSampler`, `Observe(routeID, op, keyLen, valueLen)` —
`keyviz/sampler.go`). A *route* is the key range a Raft group owns in
the distribution catalog. The heatmap's Y axis is therefore
**route-granular**:

- The hot path never sees the key — `ShardedCoordinator.observeMutation`
  passes `len(mut.Key)`, and `observeRead` passes `len(key)`
  (`kv/sharded_coordinator.go:1082`, `:800`, `:822`). The bytes are in
  scope at all three call sites but are discarded.
- Before any hotspot split, a single route can cover a huge contiguous
  range. All of its traffic collapses onto **one heatmap row**, so a
  hot key *inside* that range is invisible — exactly the case the tool
  exists to expose.

BigTable's Key Visualizer divides the key space into many fine,
**contiguous, order-preserving** buckets regardless of tablet
boundaries; brightness within a bucket reveals the hotspot. This
proposal brings that granularity to elastickv by subdividing each
route into `K` order-preserving sub-range buckets.

This extends the Phase 2-A in-memory sampler. It does not touch
fan-out (Phase 2-C / 2-C+), the SPA's structural rendering (the
heatmap already keys rows by `Start`/`End`), the proto, or Raft/FSM.

## 2. Goals / non-goals

### 2.1 Goals

- Subdivide each **individual** route's `[Start, End)` into `K`
  order-preserving sub-range buckets and accumulate the existing four
  series (reads, writes, read bytes, write bytes) per sub-bucket.
- Keep the hot path **allocation-free and lockless**, matching the
  current `Observe` contract (single atomic load + map lookup + atomic
  adds).
- Emit one `MatrixRow` per **non-empty** sub-bucket at `Flush`, with
  `Start`/`End` narrowed to the sub-bucket bounds. The SPA's heatmap
  rendering needs no structural change (it already keys rows by
  `Start`/`End`), but the server-side **pivot must be re-keyed** from
  bare `RouteID` to a composite `(RouteID, subBucket)` id, because
  sub-rows of one route share its `RouteID` and would otherwise collide
  in the pivot map — see §5.
- Bound memory and payload: `K` is modest and configurable; the
  existing `MaxTrackedRoutes` coarsening and per-request row budget
  still cap totals.
- Be **backward compatible**: `K = 1` reproduces today's exact
  route-level behaviour, and that is the value the feature flag
  defaults to until the operator opts in.

### 2.2 Non-goals

- **No exact Top-K hot-key ranking.** A heavy-hitters / count-min
  sketch that names the single hottest *keys* (rather than the hottest
  *sub-range*) was considered and explicitly deferred — see §8. The
  chosen base model is the BigTable-faithful range heatmap; Top-K can
  layer on later as a drill-down.
- **No sub-division of virtual aggregate buckets.** A virtual bucket
  already means "too many routes, coarsened into one row"; subdividing
  it would fight the coarsening. Aggregate slots stay single rows.
- **No new sampling of adapter-direct read paths** (Redis / DynamoDB /
  S3 reads that hit `MVCCStore.GetAt` without going through the
  coordinator). That gap is unchanged and tracked separately.
- **No proto / gRPC change.** Like the per-cell-conflict work, this is
  a sampler + JSON concern; the gRPC `GetKeyVizMatrix` path can adopt
  the finer rows for free because they are still ordinary `MatrixRow`s.
- **No persistence.** History remains the in-memory ring (Phase 3
  persistence is unaffected and out of scope).

## 3. Bucketing function (the central design decision)

We need a function `bucketIndex(key, Start, End, K) -> [0, K)` that is:

1. **Order-preserving**: `key1 < key2 ⇒ idx1 ≤ idx2`. This is what
   makes a sequential-write hotspot render as a moving diagonal and a
   point hotspot as a single bright cell — the BigTable visual
   vocabulary. (A hash bucketing would scatter a contiguous hot range
   across the whole axis and is therefore rejected, see §8.)
2. **Cheap and allocation-free** on the hot path.
3. **Bounded** to `K` buckets per route.

### 3.1 Even division of `[Start, End)` over a fixed byte window

Routes frequently share a long common prefix between `Start` and `End`
(e.g. a table/tenant prefix); the entropy that distinguishes keys
lives in the bytes *after* that prefix. So:

1. **At `RegisterRoute` (off the hot path), precompute and store on the
   slot** (immutable thereafter for individual routes):
   - `subPrefixLen` = length of the common prefix of `Start` and `End`.
   - `subStart` = the `W` bytes of `Start` at `[subPrefixLen, +W)`,
     right-padded with `0x00`, read big-endian into a `uint64`.
   - `subEnd` = same window of `End` (unbounded end handled in §3.2).
   - `subSpan = subEnd - subStart` (guaranteed `> 0` for bounded
     routes after §3.2's fallback).

   `W = 8` (one `uint64` window) is the proposed default — eight bytes
   past the common prefix is enough resolution for `K` up to a few
   thousand. `W` is an internal constant, not a flag.

2. **On the hot path** (`Observe`), with the key in hand, a single
   shared helper `subBucketIndex(key, slot) int` (also used by `Flush`
   for bounds reconstruction — see §4.3 — so the two paths can never
   diverge) does:
   - Read the `[subPrefixLen, +W)` window of `key` big-endian into `k`
     (bytes beyond the key's length read as `0x00`; a key shorter than
     `subPrefixLen` clamps to bucket 0).
   - **Underflow guard first**: if `k <= subStart` return `0`; if
     `k >= subEnd` return `K-1`. Only the strictly-interior case does
     arithmetic, so `k - subStart` can never wrap.
   - `idx = K * (k - subStart) / subSpan`, computed with an exact
     128-bit intermediate via `math/bits`:
     `hi, lo := bits.Mul64(uint64(K), k-subStart); idx, _ := bits.Div64(hi, lo, subSpan)`
     (`uint64(K)` cast shown explicitly — `K` is an `int`/`uint32`
     config value while `bits.Mul64` takes `uint64`).
     `bits.Div64` panics only when `hi >= subSpan`; the interior guard
     guarantees `k - subStart < subSpan`, so `hi < K`, and `K` is
     capped well below `subSpan` whenever `subSpan > 0` (the degenerate
     `subSpan == 0` route is forced to `K=1` at register — §3.2 — and
     never reaches this branch). With the `k >= subEnd` guard already
     handling the top edge, the interior case (`k <= subEnd - 1`)
     yields at most `K-1` exactly, so a final clamp to `[0, K-1]` can
     **never actually fire** — it is kept only as defensive
     correctness, not because any guarded path can overflow it.
   - `atomic.Add` into `slot.subBuckets[idx]`'s counter family.

   No allocation, no lock; one `Mul64`+`Div64` and a handful of byte
   reads versus today. `bits.Mul64`/`Div64` compile to a couple of
   instructions and keep the math exact, so we drop the earlier
   shift-and-approximate scheme entirely — bucket assignment is exact,
   monotone, and stable.

### 3.2 Degenerate and unbounded bounds

- **`Start == nil`** (first route / open low end): treat as all-`0x00`,
  i.e. `subStart = 0`. Natural — `0x00…` is the true low end of the
  byte-order key space.
- **`End == nil`** (last route / open high end): **sub-divide over
  `[subStart, MaxUint64]`** (revised — see the note below). There is no
  `End` window, so set `subPrefixLen = 0`, `subStart =
  windowUint64(start)`, `subEnd = math.MaxUint64`, `subSpan =
  MaxUint64 - subStart`, and divide that finite span into `K`. Keys
  bucket by their leading `W`-byte window across the open high end; the
  **last** sub-bucket keeps the route's unbounded `End` (`nil`). The
  `subBucketIndex` upper edge clamp is skipped for this slot (its
  `subHi` snapshot is `nil` — the unbounded sentinel), relying on the
  window math + the `w >= subEnd` guard instead.

  *Why this reverses Open Q1.* The first review chose "single row until
  split" because the original fallback used `subSpan = 1 << (8*W)`,
  which with `W = 8` overflows `uint64` to `0` and divides by zero.
  Using `MaxUint64` (= `2^64 − 1`) as the effective top makes the span
  a valid `uint64` with no overflow, so the original objection no
  longer applies. The practical driver: the **single-route cluster and
  every cluster's tail route are unbounded**, and that is exactly where
  an operator most wants hot-key visibility — keeping them at one row
  made the feature inert for the most common topology. The "split
  changes the heuristic" discontinuity is accepted: a split is a
  discrete event that already reshuffles routes. **Caveat:** with
  `subPrefixLen = 0` the resolution is leading-`W`-byte-coarse, so keys
  sharing a long prefix (e.g. `user:0001…`/`user:0002…`) still collapse
  into one bucket — finite, tight route bounds remain the way to get
  fine intra-prefix resolution.
- **`subEnd <= subStart`** (the `W`-byte window captures no difference —
  e.g. `Start`/`End` differ only beyond `subPrefixLen + W`): the route
  is likewise a **single bucket** (`K_effective = 1`, `subSpan` left 0
  and never used because the §3.1 guard short-circuits). It means "this
  route's interesting variation is past our resolution window," which a
  wider `W` or a route split resolves. Surfaced via a debug metric so
  we can tell whether `W = 8` is too small in practice.

Both single-bucket cases reduce that slot to today's exact route-level
behaviour, so they are always safe fall-backs rather than error paths.

## 4. Sampler changes (`keyviz/`)

### 4.1 `Observe` signature

```go
// before
Observe(routeID uint64, op Op, keyLen, valueLen int)
// after
Observe(routeID uint64, key []byte, op Op, valueLen int)
```

`keyLen` is dropped in favour of the key itself (`len(key)` recovers
it for the `*Bytes` counters). This is a **semantic-touching signature
change**: per CLAUDE.md, every caller of `Sampler.Observe` /
`MemSampler.Observe` will be grep-audited so each passes the real key
(not, say, a nil placeholder that would silently bucket all traffic
into bucket 0). The full audit:

- `ShardedCoordinator.observeMutation` — already holds `mut.Key`; pass
  it through.
- `ShardedCoordinator.observeRead` — **its own signature also changes**
  from `observeRead(routeID uint64, keyLen int)` to
  `observeRead(routeID uint64, key []byte)`, since it is the
  intermediate helper that wraps `sampler.Observe`
  (`kv/sharded_coordinator.go:1100`). Both of its call sites,
  `LinearizableReadForKey` (`:800`) and `LeaseReadForKey` (`:822`),
  already have the full `key` in scope and pass `len(key)` today, so
  the fix is local. This intermediate helper is the easy one to miss —
  calling it out explicitly so the audit doesn't stop at the two
  RPC entry points.

The nil-receiver and typed-nil no-op contract is preserved.

### 4.2 `routeSlot`

Individual slots gain (all set when the slot's range is established —
see the re-registration note below — and read by the hot path without
a lock):

```go
subPrefixLen int
subStart     uint64
subEnd       uint64           // subStart + subSpan; the §3.1 guard reads it
subSpan      uint64           // 0 ⇒ single-bucket slot (§3.2)
subBuckets   []subCounter     // len == effK (1 for aggregate / degenerate)
```

`subCounter` holds the same four `atomic.Uint64` the slot holds today.
For `K = 1` (flag default) and for aggregate / degenerate slots,
`subBuckets` has length 1 and `subBucketIndex` short-circuits to 0, so
the code path collapses to exactly today's behaviour with one extra
indirection. (An unbounded-`End` route with `K > 1` does sub-divide —
§3.2 — over `[subStart, MaxUint64]`, capped at the span.) The `subShift` field from the
first draft is gone — `math/bits` (§3.1) makes the multiply/divide
exact without it.

**Immutability & re-registration (revised after review).** The
sub-bucket layout (`subPrefixLen` / `subStart` / `subEnd` / `subSpan`
and the `subBuckets` array itself) is established **once at the route's
first registration and is immutable for the slot's lifetime**. So both
the hot path and `Flush` read it **lock-free**, with no `metaMu` — it
genuinely never changes, which is the cleanest way to keep the index
computation off any lock (resolving the lock-discipline question raised
in review).

Two consequences for the grace-window re-registration path
(`reclaimRetiredSlot`), both deliberate:

- **Counters are preserved, never zeroed and never re-sized.** The slot
  is reused precisely so pre-removal and in-flight `Observe`
  increments survive until the next `Flush` drains them — the
  sampler's no-loss contract (Codex P1). `K` is a sampler-wide
  constant, so `subBuckets` is already the right length on reuse; there
  is nothing to resize. The per-sub-bucket counters keep accumulating
  in place exactly as the slot's scalar `reads`/`writes` counters do
  today. An earlier draft said to "re-size and zero" here — that was
  wrong; it would drop counts recorded between the last flush and
  `RemoveRoute`.
- **The layout is *not* recomputed on re-registration.** A
  same-`RouteID` re-registration carries the **same** `[Start, End)` in
  practice: a split/merge mints *new* `RouteID`s rather than re-ranging
  an existing one, and a watcher re-sync re-registers the identical
  range. So the original layout stays valid. In the unexpected event
  that a same-ID re-registration ever carried a *different* range
  within the grace window, the route keeps its original sub-ranges for
  ≤ one grace window — a bounded **cosmetic** misattribution of sub-row
  labels, with **no** lost or double-counted traffic (every increment
  still lands in some bucket and is drained). This is strictly safer
  than mutating immutable layout fields the lock-free hot path is
  reading.

(`reclaimRetiredSlot` still refreshes the mutable `Start` / `End` /
`MemberRoutesTotal` under `metaMu` as today; those remain
`snapshotMeta`-guarded. The sub-bucket layout is simply not among the
mutated fields, so `Flush` can read it without taking the lock.)

**Memory**: `K * 4 * 8` bytes per individual route. `K = 16` →
512 B/route; at the default `MaxTrackedRoutes = 10_000` that is ~5 MB
worst case (every route fully populated). Bounded by construction.
Decision (Open Q3, confirmed in review): **eager** allocation at
register for hot-path simplicity and lock-freedom — lazy (CAS on first
non-zero Observe) adds hot-path branches for negligible benefit at the
target `K ≤ 16`.

### 4.3 `Flush`

`appendDrainedRow` becomes a loop over the slot's `subBuckets`: for
each sub-bucket with any non-zero counter, emit one `MatrixRow` whose
`Start`/`End` are the **sub-bucket bounds**, reconstructed by inverting
the §3.1 interpolation: bucket `i` spans
`[subStart + i*subSpan/K, subStart + (i+1)*subSpan/K)` mapped back into
the key byte space at offset `subPrefixLen`. The interior boundary
`i*subSpan/K` **must use the same 128-bit `math/bits` path as the
forward computation** — `hi, lo := bits.Mul64(uint64(i), subSpan);
bound, _ := bits.Div64(hi, lo, uint64(K))` — not plain `uint64`
arithmetic: `i*subSpan` overflows a `uint64` once `subSpan > 2^63` and
`i ≥ 2` (e.g. `K=3, i=2, subSpan=2^63` → `2^64` wraps to `0`),
producing a nonsensical interior bound and a visible gap in the
heatmap. To guarantee the sub-rows **tile the parent route exactly**
(the "contiguous" property the heatmap advertises) despite integer
truncation in the inverse:

- sub-bucket `0`'s `Start` is pinned to the route's actual `Start`
  (not the reconstructed `subStart` window value, which dropped the
  bytes past `subPrefixLen + W`);
- sub-bucket `K-1`'s `End` is pinned to the route's actual `End`;
- interior boundaries use the reconstructed value, and each bucket's
  `Start` is set equal to the previous bucket's `End` so there are no
  gaps or overlaps.

The parent `RouteID`, `RaftGroupID`, `LeaderTerm`, `Aggregate`, and
member metadata are copied onto every sub-row so fan-out dedupe
(`(bucketID, group, term, column)`) and the SPA's route attribution
keep working. Idle sub-buckets are skipped, so a route with one hot
sub-range emits one row, not `K`.

`MatrixRow` gains **two** fields, not one:

- `SubBucket int` — this row's index within its route, `0` for the
  first (or only) sub-bucket.
- `SubBucketCount int` — how many sub-buckets the parent route is
  divided into: `1` for a `K = 1` / aggregate / degenerate slot, `> 1`
  for a genuinely sub-bucketed route (including an unbounded-`End` route
  with `K > 1`).

`SubBucketCount` is the disambiguator `bucketIDFor` needs (Claude bot
gap): a `K = 1` slot's single row and a `K > 1` slot's first sub-row
*both* have `SubBucket == 0`, so the index alone cannot tell them
apart. `bucketIDFor` appends the `#<subIdx>` suffix **only when
`SubBucketCount > 1`**, so a `K = 1` slot keeps the exact legacy
`route:<id>` id (§5.2) and the change stays inert at the default.

Because `MatrixRow` is the shared shape behind both the JSON pivot and
the gRPC `GetKeyVizMatrix` path, these are struct additions the
gRPC/proto layer ignores for now; if the gRPC path later adopts
sub-rows, `proto.KeyVizRow` gains the matching fields then. This
proposal does **not** change the proto (the fields are unused on that
path), but the dependency is flagged so the future proto edit isn't a
surprise.

## 5. Pivot re-keying, wire format & SPA

### 5.1 Server-side pivot must key on `(RouteID, subBucket)` — required

The single most important code change beyond the sampler is in the
**pivot**. Both `pivotKeyVizColumns` (`internal/admin/keyviz_handler.go`,
the `rowsByID map[uint64]*KeyVizRow` keyed on `mr.RouteID` around
`:340`) and the gRPC twin `adapter.matrixToProto` collapse all
`MatrixRow`s sharing a `RouteID` into one output row. With sub-rows,
every sub-bucket of a route carries the **same `RouteID`**, so the
current map would collide them — only the last sub-bucket seen per
column would survive, silently dropping the others (Gemini HIGH).

Fix: re-key the pivot map on the composite `(RouteID, SubBucket)` (a
small struct key, or the `BucketID` string). Both the JSON pivot and
`adapter.matrixToProto` must change together; they are intentionally
kept as parallel implementations today, so this is a two-file edit with
matching table-driven tests on each. The row-budget and Start-order
sort that run after the pivot are unaffected (they already operate on
the row list, not the map).

### 5.2 Wire format

The JSON `KeyVizRow` already carries `Start`/`End`; sub-rows simply
have narrower bounds. To keep `BucketID` unique per row within a
column (the fan-out merge and the SPA both key on it), `bucketIDFor`
gains a sub-range discriminator keyed on `SubBucketCount` (§4.3):

- sub-bucketed route (`SubBucketCount > 1`, including an unbounded-`End`
  route with `K > 1`): `route:<id>#<subIdx>`
- whole route (`SubBucketCount == 1`, the `K = 1` / aggregate /
  degenerate case): `route:<id>` (unchanged)
- virtual bucket: `virtual:<id>` (unchanged)

### 5.3 SPA

The SPA heatmap renders rows by `Start`/`End` and already supports an
arbitrary number of rows under the row budget, so **no structural SPA
change is required** to see hot sub-ranges. Two small polish items
(optional, can be a follow-up PR): the `RowDetail` panel labels a
sub-row with its narrowed range, and tooltips show "route N · sub-range
i/K". The fan-out wire fields (`conflicts[]`, `raft_group_ids[]`,
`leader_terms[]`) are unaffected — they are per-column and travel on
each sub-row unchanged.

## 6. Configuration

```
--keyvizKeyBucketsPerRoute int   (default 1)
```

- `1` → today's exact route-level behaviour (no sub-bucketing,
  zero added memory beyond the length-1 slice). This is the default,
  so the change is inert until opted into.
- `> 1` → subdivide each individual route into that many sub-ranges.
- Clamped to `[1, keyvizKeyBucketsCap]`, **cap = 256**. The cap must be
  an *operationally* safe bound, not merely a finite one: memory is
  `K × 4 × 8` B/route, so the cap interacts with `MaxTrackedRoutes`.
  At the default `MaxTrackedRoutes = 10_000`:

  | `K` | counter memory (worst case) | pre-budget rows/column |
  |---|---|---|
  | 16 (typical) | ~5 MB | 160 k |
  | 256 (cap) | ~80 MB | 2.56 M |
  | 4096 (rejected) | ~1.28 GB | 41 M |

  `4096` (the first draft's cap) is "bounded" only mathematically —
  1.28 GB of counters and 41 M materialised rows is not acceptable for
  a monitoring subsystem (Codex P1, R4). `256` keeps the worst case at
  ~80 MB / 2.56 M rows, still generous versus the `K ≤ 16` target. The
  general rule, which the flag help text should state, is
  `K_max ≈ memBudget / (32 × MaxTrackedRoutes)`; an operator raising
  `MaxTrackedRoutes` should lower `K` to stay within budget. Values
  above the cap clamp down; values `< 1` clamp to 1.

Threaded through `MemSamplerOptions` exactly like the existing
`MaxTrackedRoutes` / `HistoryColumns` knobs (`buildKeyVizSampler` in
`main.go`).

**Interaction with the row budget.** Today `Flush` emits ≤1 row per
slot. With sub-bucketing a single route can emit up to `K` rows, so the
*worst-case* pre-budget row count rises from `MaxTrackedRoutes` to
`MaxTrackedRoutes × K` (e.g. `10_000 × 16 = 160_000` rows materialised
in a column before `applyKeyVizRowBudget` prunes to the per-request cap
of 1024). Two consequences to document for operators:

- The per-request `rows` budget (`keyVizRowBudgetCap`, 1024) now buys
  roughly `budget / active_subbuckets_per_route` *routes* shown at full
  resolution — i.e. raising `K` trades route breadth for intra-route
  depth at a fixed payload size. The doc/flag help text should say so.
- The intermediate 160k-row materialisation is bounded and transient
  (one column build), but if it proves heavy, `Flush` can apply a
  cheap per-slot top-sub-bucket cap before the global budget. Noted as
  a follow-up, not required for the first cut — only non-empty
  sub-buckets are emitted, and hot traffic concentrates in a few of
  them, so the typical count is far below the worst case.
- **Bias toward concentrated traffic (intended).** `applyKeyVizRowBudget`
  ranks rows by per-row `total`. Today a uniform route with 1,000
  writes/step always shows as one row valued 1,000. With `K = 16` that
  same route emits ~16 sub-rows of ~62 each, while a route whose 1,000
  writes land in one hot sub-range emits one sub-row valued ~1,000. So
  under budget pressure the *concentrated* route's hot sub-row
  out-ranks the *uniform* route's diluted sub-rows and is more likely
  to be retained. This is the **desired** behaviour — hot sub-ranges
  are exactly what should survive truncation — but it is a behavioural
  change from today (where a uniform route is guaranteed one row
  regardless of absolute activity), worth stating so the operator
  mental model is complete.

## 7. Backward compatibility & rollout

- Flag defaults to `1`: existing deployments behave identically and
  pay no extra memory (length-1 sub-bucket slice, index hard-wired to
  0). No wire change for `K = 1` rows.
- `Observe` signature change is internal (all callers in-tree); audited
  per §4.1.
- Mixed fan-out clusters (decision adopted from Open Q2): a peer
  running `K = 1` emits `bucketID = route:42`, a peer running `K = 16`
  emits `route:42#0 … route:42#15`. Because the aggregator dedupes by
  `bucketID`, these **do not merge** — the route-level row and the
  sub-rows **coexist as distinct rows** in the aggregate; the `K = 1`
  row does not "win over" or absorb the sub-rows, and vice-versa. That
  is the correct behaviour (they represent different granularities of
  the same range), but it is surprising enough to (a) state explicitly
  here and (b) lock down with a dedicated mixed-`K` fan-out test. The
  intended operational posture is therefore a **uniform `K` across the
  cluster**; mixed `K` is tolerated (no crash, no lost data) but
  produces a visually doubled range until the rollout converges.
  No peer needs to agree on `K` for correctness.
- `make run` demo + a load generator can set `--keyvizKeyBucketsPerRoute`
  to demonstrate a hot sub-range filling in under one Step.

## 8. Alternatives considered

- **Hash bucketing** (`hash(key) % K`): bounded and trivially even, but
  **destroys contiguity** — a sequential or point hotspot scatters
  across the axis, defeating the entire visual purpose. Rejected.
- **Fixed-width key-prefix / radix bucketing**: bucket by the first
  `W` bytes of the key directly. Order-preserving, but the bucket count
  is data-dependent (up to `256^W`) and needs its own coarsening layer
  on top of the route coarsening we already have. More moving parts for
  no visual gain over §3. Kept as a fallback if even-division's
  unbounded-tail handling (§3.2) proves unsatisfactory in practice.
- **Exact Top-K heavy hitters** (count-min sketch + min-heap of hot
  keys): names the literal hottest keys, which is *more* precise than a
  range. But it is a different visualization (a ranked list, not a
  heatmap row), and the user selected the range heatmap as the base.
  Best added later as a **drill-down** when a hot cell is clicked —
  noted as future work, not in this proposal.

## 9. Resolved decisions (from first review round)

The four questions opened in the first draft converged in review
(Codex, Gemini, Claude) and are now decided; recorded here so the
implementation has no ambiguity.

1. **Unbounded-tail bucketing (§3.2): ~~single row until split~~ →
   sub-divide over `[subStart, MaxUint64]` (revised post-merge).** The
   first-round decision kept an open-`End` route at one row. That made
   the feature inert for the single-route cluster and every cluster's
   tail route — the most common place hot-key visibility is wanted. The
   original blocker (`1 << (8*W)` overflow) is avoided by using
   `MaxUint64` as the effective top, so the open end now sub-divides by
   the leading-`W`-byte window (§3.2). The last sub-bucket keeps the
   unbounded `End`. Resolution is leading-byte-coarse for `prefixLen 0`;
   tight route bounds still give finer intra-prefix detail.
2. **Mixed-`K` fan-out (§7): coexist, don't merge.** Rows keyed by
   `bucketID` (now embedding `#subIdx`) mean a `K = 1` row and the
   `K = 16` sub-rows appear side by side rather than merging. Tolerated
   for correctness; uniform `K` is the intended posture. A dedicated
   mixed-`K` fan-out test is required.
3. **Eager allocation (§4.2).** Allocate `subBuckets` at register.
   At the target `K ≤ 16` the worst case is ~5 MB and the hot path
   stays lock-free; lazy CAS allocation is not worth its hot-path
   branches.
4. **Fixed window `W = 8` (§3.1).** Not configurable. Routes whose
   distinguishing bytes sit past `subPrefixLen + 8` degrade to a single
   bucket (§3.2) and are surfaced via a debug metric, which is an
   adequate safety valve; a configurable `W` would complicate the
   precompute for no clear operator benefit.

## 10. Self-review (per CLAUDE.md five lenses)

1. **Data loss** — none. Read-only sampling/visualization path; no
   Raft / FSM / Pebble / TTL interaction. Sub-bucketing only changes
   how already-counted traffic is *attributed* across rows; the
   existing retired-slot / grace-window machinery that prevents
   lost counts on route churn is untouched (sub-buckets live inside
   the same slot lifecycle).
2. **Concurrency / distributed** — hot path stays lockless and now
   provably so: the sub-bucket layout is **immutable for the slot's
   lifetime** (§4.2), so `Observe` reads `subPrefixLen`/`subStart`/
   `subEnd`/`subSpan` with no lock and no race — there is no writer.
   Per-sub-bucket counters are `atomic.Uint64`; `Flush` drains with
   `atomic.Swap` exactly as today and reads the immutable layout
   lock-free to reconstruct bounds. Grace-window re-registration
   preserves counters in place (never zeroed/resized — Codex P1) and
   does not touch the layout, so it introduces no new shared-mutable
   state on the hot path. Aggregate-bucket folding still mutates
   `Start`/`End`/`MemberRoutes` under `metaMu`, but aggregates stay
   single-bucket, so sub-bucketing adds no new `metaMu` interaction.
   Run `go test -race ./keyviz/...` plus the register/remove churn
   cases. Mixed-`K` fan-out has its own test (the §9 decision 2).
3. **Performance** — per-`Observe` cost adds a bounded byte-window read
   + one multiply/divide + clamp; no allocation, no lock, no extra Raft
   round-trip (HLC path untouched). Memory bounded to `K * 4 * 8` B per
   route, `K` capped. A benchmark (`Observe` with `K = 1` vs `K = 64`)
   gates the hot-path regression; flush cost rises with non-empty
   sub-buckets but is off the request path.
4. **Data consistency** — sub-rows carry the parent route's
   `(RaftGroupID, LeaderTerm)` unchanged, so the canonical fan-out
   dedupe and per-cell conflict detection (§5.1, §7) operate
   identically at finer granularity. Bucket assignment is deterministic
   and monotone (the order-preservation guarantee of §3.1), so the same
   key lands in the same sub-bucket every Step (stable rows across
   columns — the matrix contract). Row budget + Start-order sort
   unchanged.
5. **Test coverage** — new unit tests: `subBucketIndex`
   order-preservation + clamping + underflow guard (table-driven, incl.
   `Start==nil`, `End==nil`→single-bucket, degenerate window,
   `k == subEnd` edge); a `rapid` property test that `key1 <= key2 ⇒
   idx1 <= idx2` over random `[Start,End)` and keys; `Flush` emits one
   row per non-empty sub-bucket with bounds that **tile `[Start,End)`
   exactly** (no gap/overlap; bucket-0 `Start == route Start`,
   bucket-`K-1` `End == route End`); a **pivot collision** test that
   two sub-rows sharing a `RouteID` survive as distinct output rows in
   both `pivotKeyVizColumns` and `adapter.matrixToProto` (the Gemini
   HIGH regression); a **grace-window re-registration** test that a
   route re-registered with a different `[Start,End)` **continues to
   bucket into its original (stale) sub-ranges for the remainder of the
   grace window** — verifying the §4.2 contract (cosmetic
   misattribution, but no lost or double-counted counts) rather than
   asserting the layout is recomputed (which §4.2 deliberately does
   *not* do); `K = 1` round-trips identically to the pre-change
   snapshot (regression pin); caller audit covered by existing
   coordinator tests plus a new assertion that a hot sub-range shows up
   on a distinct row. Mixed-`K` fan-out test per the §9 decision 2.
