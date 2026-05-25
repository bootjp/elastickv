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
  `Start`/`End` narrowed to the sub-bucket bounds, so the existing
  pivot + row-budget + SPA path renders finer rows with no structural
  change.
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

2. **On the hot path** (`Observe`), with the key in hand:
   - Read the same `[subPrefixLen, +W)` window of `key` big-endian into
     `k` (bytes beyond the key's length read as `0x00`; a key shorter
     than `subPrefixLen` clamps to bucket 0).
   - `idx = K * (k - subStart) / subSpan`, computed as
     `uint64` math, then **clamped to `[0, K-1]`** (keys at or past
     `End`, or below `Start`, pin to the edge buckets rather than
     escaping the array).
   - `atomic.Add` into `slot.subBuckets[idx]`'s counter family.

   No allocation, no lock, one extra multiply/divide and a handful of
   byte reads versus today. The `128`-bit intermediate for
   `K * (k - subStart)` is avoided by requiring `K ≤ 2^32` (enforced at
   config parse) so the product fits a `uint64` whenever
   `k - subStart < 2^32`; for larger spans we shift both operands right
   by the same amount before multiplying (precomputed shift stored on
   the slot). Exact arithmetic is unnecessary — bucket assignment only
   needs to be monotone and stable.

### 3.2 Degenerate and unbounded bounds

- **`Start == nil`** (first route / open low end): treat as all-`0x00`,
  i.e. `subStart = 0`. Natural.
- **`End == nil`** (last route / open high end): there is no finite
  span to divide. Fallback: set `subSpan = 1 << (8*W)` *relative to
  `subStart`* — i.e. bucket by the **high-order bits of
  `key - subStart`** across the full window. This spreads keys over
  `K` buckets order-preservingly under the assumption that real keys
  stay within `W` bytes of variation past the prefix; keys far past
  that pin to the top bucket. Documented as approximate for the
  unbounded tail.
- **`subEnd <= subStart`** (the `W`-byte window doesn't capture any
  difference — e.g. `Start`/`End` differ only beyond `subPrefixLen+W`):
  the route is treated as a **single bucket** (`K_effective = 1`) for
  that slot. Correct and safe; it just means "this route's interesting
  variation is past our resolution window," which a wider `W` or a
  route split would resolve. Surfaced via a debug metric so we can tell
  whether `W` is too small in practice.

These edge rules are the part most worth scrutinising in review;
§9 Open Questions calls them out explicitly.

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
into bucket 0). Known callers: `observeMutation` (`mut.Key`),
`observeRead` ×2 (`key` is already the parameter). The nil-receiver and
typed-nil no-op contract is preserved.

### 4.2 `routeSlot`

Individual slots gain (all set once at `RegisterRoute`, immutable
after publish — so the hot path reads them without a lock, same as
`RouteID`):

```go
subPrefixLen int
subStart     uint64
subSpan      uint64
subShift     uint8            // overflow-avoidance shift (see §3.1)
subBuckets   []subCounter     // len == K (1 for aggregate / degenerate)
```

`subCounter` holds the same four `atomic.Uint64` the slot holds today.
For `K = 1` (flag default) and for aggregate / degenerate slots,
`subBuckets` has length 1 and `bucketIndex` is hard-wired to 0, so the
code path collapses to exactly today's behaviour with one extra
indirection.

**Memory**: `K * 4 * 8` bytes per individual route. `K = 16` →
512 B/route; at the default `MaxTrackedRoutes = 10_000` that is ~5 MB
worst case (every route fully populated). Bounded by construction.
§9 Q3 weighs eager (at register) vs lazy (CAS on first non-zero
Observe) allocation; this doc proposes **eager** for hot-path
simplicity and lock-freedom, with lazy as a follow-up if the 5 MB
ceiling proves to matter.

### 4.3 `Flush`

`appendDrainedRow` becomes a loop over the slot's `subBuckets`: for
each sub-bucket with any non-zero counter, emit one `MatrixRow` whose
`Start`/`End` are the **sub-bucket bounds** (derived by inverting the
interpolation: `Start_i = key-space point at fraction i/K`,
`End_i = point at (i+1)/K`, reconstructed from `subPrefixLen` +
`subStart` + window). The parent `RouteID`, `RaftGroupID`,
`LeaderTerm`, `Aggregate`, and member metadata are copied onto every
sub-row so fan-out dedupe (`(bucketID, group, term, column)`) and the
SPA's route attribution keep working. Idle sub-buckets are skipped, so
a route with one hot sub-range emits one row, not `K`.

A new `SubBucket uint32` (or a derived `BucketID` suffix) distinguishes
sub-rows of the same route within a column; see §5.

## 5. Wire format & SPA

The JSON `KeyVizRow` already carries `Start`/`End`; sub-rows simply
have narrower bounds. To keep `BucketID` unique per row within a
column (the fan-out merge and the SPA both key on it), the bucket ID
gains a sub-range discriminator:

- individual sub-row: `route:<id>#<subIdx>`
- whole route (`K = 1`): `route:<id>` (unchanged)
- virtual bucket: `virtual:<id>` (unchanged)

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
- Clamped to `[1, keyvizKeyBucketsCap]` (cap proposed at `4096`,
  mirroring the row-budget philosophy — an operator typo can't reserve
  unbounded per-route memory). Values above the cap clamp down; values
  `< 1` clamp to 1.

Threaded through `MemSamplerOptions` exactly like the existing
`MaxTrackedRoutes` / `HistoryColumns` knobs (`buildKeyVizSampler` in
`main.go`).

## 7. Backward compatibility & rollout

- Flag defaults to `1`: existing deployments behave identically and
  pay no extra memory (length-1 sub-bucket slice, index hard-wired to
  0). No wire change for `K = 1` rows.
- `Observe` signature change is internal (all callers in-tree); audited
  per §4.1.
- Mixed fan-out clusters: a peer running `K = 1` returns route-level
  rows, a peer running `K = 16` returns sub-rows; the aggregator merges
  by `(bucketID, group, term, column)` and they simply coexist as
  distinct rows. No peer needs to agree on `K`. (Worth a fan-out test —
  §9 Q2.)
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

## 9. Open questions (for review)

1. **Unbounded-tail bucketing (§3.2).** Is "high-order bits of
   `key - subStart` over a `W`-byte window" the right fallback for the
   last route's open `End`, or should the unbounded tail stay a single
   row until a split gives it a finite `End`? Leaning toward the
   single-row-until-split option for honesty if review prefers it.
2. **`K` per peer in fan-out.** Confirm the merge tolerates peers with
   different `K` (it should — rows are keyed by `BucketID` which now
   embeds `#subIdx`). Add an explicit mixed-`K` fan-out test.
3. **Eager vs lazy sub-bucket allocation (§4.2).** Eager keeps the hot
   path lock-free and simple at a bounded ~5 MB worst case; lazy
   (atomic.Pointer + CAS on first non-zero Observe) saves memory for
   idle routes but adds hot-path branches. Proposing eager; flagging
   for a perf opinion.
4. **Window width `W = 8`.** Enough for most key shapes; routes whose
   distinguishing bytes sit past `subPrefixLen + 8` degrade to a single
   bucket (§3.2). Is a configurable `W` worth it, or is the
   single-bucket degradation + a debug metric acceptable?

## 10. Self-review (per CLAUDE.md five lenses)

1. **Data loss** — none. Read-only sampling/visualization path; no
   Raft / FSM / Pebble / TTL interaction. Sub-bucketing only changes
   how already-counted traffic is *attributed* across rows; the
   existing retired-slot / grace-window machinery that prevents
   lost counts on route churn is untouched (sub-buckets live inside
   the same slot lifecycle).
2. **Concurrency / distributed** — hot path stays lockless: sub-bucket
   metadata is immutable post-`RegisterRoute` (same contract as
   `RouteID`/`Start`/`End`), counters are per-sub-bucket
   `atomic.Uint64`, `Flush` drains with `atomic.Swap` exactly as today.
   No new shared mutable state on `Observe`. Index computation reads
   only immutable fields. Aggregate-bucket folding (which *does* mutate
   metadata under `metaMu`) is excluded from sub-bucketing, so no new
   `metaMu` interaction. Run `go test -race ./keyviz/...`.
3. **Performance** — per-`Observe` cost adds a bounded byte-window read
   + one multiply/divide + clamp; no allocation, no lock, no extra Raft
   round-trip (HLC path untouched). Memory bounded to `K * 4 * 8` B per
   route, `K` capped. A benchmark (`Observe` with `K = 1` vs `K = 64`)
   gates the hot-path regression; flush cost rises with non-empty
   sub-buckets but is off the request path.
4. **Data consistency** — sub-rows carry the parent route's
   `(RaftGroupID, LeaderTerm)` unchanged, so the §9.1 canonical fan-out
   dedupe and per-cell conflict detection operate identically at finer
   granularity. Bucket assignment is deterministic and monotone, so the
   same key lands in the same sub-bucket every Step (stable rows across
   columns — the matrix contract). Row budget + Start-order sort
   unchanged.
5. **Test coverage** — new unit tests: `bucketIndex` order-preservation
   + clamping (table-driven, incl. `Start==nil`, `End==nil`,
   degenerate window); a `rapid` property test that `key1 <= key2 ⇒
   idx1 <= idx2` over random `[Start,End)` and keys; `Flush` emits one
   row per non-empty sub-bucket with correct narrowed bounds; `K = 1`
   round-trips identically to the pre-change snapshot (regression
   pin); caller audit covered by existing coordinator tests plus a new
   assertion that a hot sub-range shows up on a distinct row. Mixed-`K`
   fan-out test per §9 Q2.
