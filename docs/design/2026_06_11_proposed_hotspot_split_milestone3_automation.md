# Hotspot Shard Split — Milestone 3: Automation

Status: Proposed
Author: bootjp
Date: 2026-06-11

Parent: [2026_02_18_partial_hotspot_shard_split.md](2026_02_18_partial_hotspot_shard_split.md) §12 "Milestone 3: Automation" (1. Access aggregation, 2. Hotspot detector, 3. Auto-split scheduler with cooldown/hysteresis).
Sibling: [2026_06_11_proposed_hotspot_split_milestone2_migration.md](2026_06_11_proposed_hotspot_split_milestone2_migration.md) (M2 — migration plane). M3 composes with M2 but does **not** depend on it for first value (see §2, §6).

## 1. Background

### 1.1 What M1 shipped (control plane)

- `distribution/catalog.go` — durable `RouteDescriptor` records in the default Raft group's MVCC state. `RouteState` already reserves `RouteStateActive`, `RouteStateWriteFenced`, `RouteStateMigratingSource`, `RouteStateMigratingTarget` (`distribution/catalog.go:51-58`).
- `distribution/engine.go` — in-memory `RouteEngine` cache with the M2 versioned-snapshot history ring used by the Composed-1 gate (`distribution/engine.go:33-58`).
- `distribution/watcher.go` — applies versioned `CatalogSnapshot`s into the engine.
- `proto.Distribution.SplitRange` — manual split RPC. M1 implements **same-group split only** (no data movement); handler at `adapter/distribution_server.go:136`.

### 1.2 What M2 proposes (migration plane)

M2 extends `SplitRange` with an optional `target_group_id`. When non-zero it kicks off a resumable `SplitJob` (phases `PLANNED → BACKFILL → FENCE → DELTA_COPY → CUTOVER → CLEANUP → DONE`) driven by a migrator goroutine on the default-group leader, moving the right child of a split to another Raft group. The same-group path from M1 keeps working byte-for-byte when `target_group_id` is zero or equals the source group. M2 explicitly defers hotspot detection and auto-split scheduling to M3.

### 1.3 The RecordAccess wiring gap

The parent doc §1 states the central gap, and the code confirms it:

- `distribution/engine.go` has per-range load counters (`Route.Load`, `distribution/engine.go:28-29`) and a threshold-based `RecordAccess` (`distribution/engine.go:352-375`) that calls a midpoint-only `splitRange` (`distribution/engine.go:445-466`).
- **`RecordAccess` is never called from a real request path.** Its only callers are `distribution/engine_test.go` and `distribution/watcher_test.go` (verified by `grep -rn RecordAccess --include='*.go'`). No code in `kv/`, `adapter/`, or `main.go` invokes it. The whole engine-counter detection path — counters, threshold check, `splitRange` — is dead code.
- The `splitRange` it would call is midpoint-only (`midpoint(r.Start, r.End)` appends a `0x00` byte, `distribution/engine.go:541-547`) and mutates the **in-memory** engine routes directly, bypassing the catalog. That violates the repo convention that all catalog mutations go through `SplitRange` so the version bumps and watchers fan out (CLAUDE.md "Route catalog mutations must go through `SplitRange`"). It also contradicts the parent doc §3.1.3 requirement "Choose split keys from observed key distribution (not midpoint-only)."

Meanwhile, a **second**, fully-wired load-observation pipeline already exists: the keyviz sampler (`keyviz/sampler.go`). The coordinator calls `Sampler.Observe` per resolved `(RouteID, key)` on both the write path (`groupMutations → observeMutation`, `kv/sharded_coordinator.go:1844` and `:1795-1800`) and the linearizable/lease read path (`observeRead`, `kv/sharded_coordinator.go:1819-1824`). The sampler keeps per-route read/write op counts and byte counts, windowed at a flush interval (`keyviz/flusher.go:19` `RunFlusher`), with an optional per-route Top-K hot-key sketch (`docs/design/2026_05_28_proposed_keyviz_hot_key_topk.md`) and order-preserving sub-range buckets (`docs/design/2026_05_25_proposed_keyviz_subrange_sampling.md`). It is allocation-free on the hot path and already behind `--keyvizEnabled`.

M3's job is to close the gap **without building a third counter pipeline**: drive detection off the keyviz sampler (the already-wired, already-sampled signal), feed candidates into a detector with hysteresis/cooldown, and have a scheduler invoke the existing `SplitRange` RPC. The engine's vestigial `RecordAccess`/`splitRange` path is retired (see §3.4).

## 2. Goals and Non-Goals

### 2.1 Goals

1. **Auto-detect hot routes** from observed load and **auto-split** them, with same-group splits (the M1 capability) working **standalone** — M3 delivers first value before M2 lands.
2. A single, reused load-observation pipeline. No second/third parallel counter set on the hot path.
3. Detection state, scoring, hysteresis, and cooldown are **deterministic and unit-testable** in isolation from the data plane.
4. **Default OFF**, behind a flag (`--autoSplit`), with a runtime kill switch and bounded-cardinality metrics.
5. **Compose with M2**: when M2 lands, the scheduler picks a `target_group_id` (least-loaded group) and passes it to the same `SplitRange` RPC. M3 does not reimplement migration.
6. Guardrails: minimum route size, maximum route count, per-cycle split budget, and per-route cooldown to prevent split storms and detector flapping.

### 2.2 Non-Goals

1. **Migration mechanics** — BACKFILL/FENCE/DELTA_COPY/CUTOVER, `SplitJob`, data movement. That is M2. M3 only *selects* the split and *calls* `SplitRange`.
2. **Automatic Raft group creation / membership orchestration.** The scheduler only targets groups that already exist in `--raftGroups` (parent doc §2.2.1).
3. **Automatic merge** (the inverse of split). Parent doc §2.2.2.
4. **Cross-shard load rebalancing beyond split** (e.g. moving a whole route without splitting it). Out of scope; the only auto action M3 takes is "split a hot route."
5. **Adapter-direct read sampling.** Reads that bypass the coordinator (Redis/DynamoDB/S3 hitting `MVCCStore.GetAt` directly) are not observed by keyviz today (`kv/sharded_coordinator.go:1815-1818`); closing that is keyviz follow-up work, not M3. M3 detects on what the sampler sees; under-observation biases the detector conservative (fewer splits), never toward an unsafe split.

## 3. Detection Signal Design

### 3.1 Decision: reuse the keyviz sampler, retire the engine counters

**Chosen signal: the keyviz `MemSampler`.** Rationale:

| | keyviz sampler | `distribution/engine` counters |
|---|---|---|
| Wired into request path | Yes — `observeMutation` / `observeRead` (`kv/sharded_coordinator.go:1795-1824`) | No — `RecordAccess` has no production caller |
| Read **and** write split | Yes (`OpRead` / `OpWrite`, byte counts too) | Single `Load` counter, no read/write split |
| Windowed | Yes — flush-interval ring buffer (`keyviz/flusher.go`) | No — monotonic counter, never reset except on split |
| Split-key evidence | Yes — sub-range buckets + Top-K hot keys | No — forces midpoint-only |
| Hot-path cost | Allocation-free, lockless `atomic.Add` (`keyviz/sampler.go:631-673`) | `RWLock` + atomic; also tries to split inline under a write lock (`distribution/engine.go:366-374`) |
| Cardinality control | `MaxTrackedRoutes` coarsening into virtual buckets | None |

Building detection on the engine counters would mean re-implementing windowing, read/write separation, sub-range split-key evidence, and cardinality bounds that keyviz already has — i.e. a second parallel pipeline, which §2.2 forbids. Therefore:

- **M3 reads detection input from the keyviz sampler**, not from `Route.Load`.
- The engine's `RecordAccess` / `splitRange` / `Route.Load` path is **retired** (§3.4). No new call site is added for it.

This means **auto-split requires keyviz sampling to be active**, and not merely at its today-defaults. The bare keyviz defaults are `DefaultKeyBucketsPerRoute = 1` (route-granular, no sub-range buckets — `keyviz/sampler.go:91`) and `HotKeysEnabled = false` (`--keyvizHotKeysEnabled` default false — `main.go:205`). Under *those* defaults the detector has **no intra-route distribution** and §5.4.3 makes it decline every split (it has no split-key evidence). So "turn on `--autoSplit` and nothing else" with the bare keyviz defaults would be a permanent no-op — exactly the gap raised in review.

To close it, **`--autoSplit` implies the split-key evidence source it needs, not just route-granular counts.** When `--autoSplit` is true and the operator **did not explicitly set** `--keyvizKeyBucketsPerRoute`, auto-split constructs the sampler with `KeyBucketsPerRoute = autoSplitDefaultBuckets` (default 16, well under the `MaxKeyBucketsPerRoute = 256` cap at `keyviz/sampler.go:115`) so the sub-range-p50 split-key path (§5.4.1) has evidence by default.

This hinges on distinguishing "operator left `--keyvizKeyBucketsPerRoute` at its default of 1" from "operator explicitly passed `--keyvizKeyBucketsPerRoute 1`" — and review correctly noted that with a plain `flag.Int` (which is what the repo uses today, `main.go:197`) both cases end with the same value `1`, so the value alone cannot tell them apart. The mechanism is **`flag.Visit`**, the Go stdlib API that iterates only flags that were *actually set on the command line* (as opposed to `flag.VisitAll`, which visits all registered flags including unset ones). After `flag.Parse()`, M3-PR3 records explicit presence:

```go
keyBucketsExplicit := false
flag.Visit(func(f *flag.Flag) {
    if f.Name == "keyvizKeyBucketsPerRoute" {
        keyBucketsExplicit = true
    }
})
// later, when wiring the sampler:
buckets := *keyvizKeyBucketsPerRoute
if *autoSplit && !keyBucketsExplicit && buckets <= 1 {
    buckets = *autoSplitDefaultBuckets // implied evidence source
}
```

`flag.Visit` (and the equivalent `flag.CommandLine.Lookup("…")` presence check) is already the idiomatic Go answer here; no config-wrapper type is needed. The same `keyBucketsExplicit` computation is mirrored in `cmd/server/demo.go` (which has its own flag set, §7.1). With this, the implied-vs-explicit config is:

| Flag | Bare keyviz default | With `--autoSplit` (no other keyviz flags) | Effect on auto-split |
|---|---|---|---|
| sampler constructed | only if `--keyvizEnabled` | **forced on** (`if keyvizEnabled \|\| autoSplit`, §7.1) | required signal exists |
| `--keyvizStep` | 60s (`sampler.go:84`) | 60s (unchanged) | window/eval cadence |
| `--keyvizKeyBucketsPerRoute` | 1 (route-granular, `sampler.go:91`) | **raised to `autoSplitDefaultBuckets`=16** when operator did not explicitly set it (`flag.Visit` presence check, see above) | enables §5.4.1 sub-range-p50 split key — without this auto-split always declines |
| `--keyvizHotKeysEnabled` | false (`main.go:205`) | **stays false** (opt-in) | §5.4.2 single-key isolation is opt-in; absence only loses the single-key path, not all splits |

So the advertised "just `--autoSplit`" mode produces splits out of the box via sub-range p50. An operator who explicitly sets `--keyvizKeyBucketsPerRoute 1` while enabling `--autoSplit` is choosing route-granular-only and gets the §5.4.3 decline (counted as `autosplit_skipped_total{reason="no_split_key"}`) — that is now a deliberate operator choice, not a hidden default trap. Top-K (`--keyvizHotKeysEnabled`) remains opt-in and only adds the §5.4.2 single-key-isolation path; its absence costs only that path, not all splits.

### 3.2 Where it hooks in (already done; no hot-path change)

No new hot-path instrumentation is added by M3. The hooks already exist:

- **Write path:** `ShardedCoordinator.groupMutations` (`kv/sharded_coordinator.go:1826-1853`) resolves the engine `RouteID` for each mutation key and calls `observeMutation` → `Sampler.Observe(routeID, key, OpWrite, len(value))`.
- **Read path:** `ShardedCoordinator.observeRead` (`kv/sharded_coordinator.go:1819-1824`) on the linearizable/lease read path calls `Sampler.Observe(routeID, key, OpRead, 0)`.

The parent doc §6.1 named `ShardStore.GetAt` / `ScanAt` (read) and `ShardedCoordinator.groupMutations` (write) as the instrumentation points. The write point matches exactly; the read point is satisfied at the coordinator `observeRead` layer instead of inside `ShardStore.GetAt`, with the known gap that adapter-direct reads bypass it (§2.2.5). M3 accepts that gap rather than adding a second read hook, to keep the "one pipeline" invariant.

### 3.3 Hot-path cost budget

M3 adds **zero** hot-path cost: it consumes the sampler's already-flushed windows from a background goroutine (§4), not from `Observe`. The existing `Observe` budget is preserved (per `keyviz/sampler.go:631-673`): one `atomic.Pointer.Load`, one map lookup against an immutable snapshot, one sub-bucket index computation, and at most two `atomic.AddUint64`. No allocation, no mutex, contention bounded by the sharded per-route slot. The detector's read of the windowed matrix happens off-path via `MemSampler.Snapshot` (`keyviz/sampler.go:1463`), which takes `historyMu` — never touched by `Observe`.

If sub-range buckets (`--keyvizKeyBucketsPerRoute > 1`) or Top-K (`--keyvizHotKeysEnabled`) are on, those already-budgeted costs apply, unchanged by M3; auto-split does not require them.

### 3.4 Retiring the engine counters

In M3-PR1 the engine's dead detection path is removed or neutralized so two pipelines cannot drift:

- Delete `Engine.RecordAccess`, `Engine.splitRange`, `Engine.midpoint`, and the `Route.Load` field along with the `hotspotThreshold`/`NewEngineWithThreshold` surface, **or** (if other code reads `Route.Load` via `Stats()` for diagnostics) keep `Load` but delete `RecordAccess`/`splitRange` and the threshold constructor. The existing `engine_test.go` / `watcher_test.go` tests that exercise `RecordAccess` are updated or removed in the same PR.
- This is a behavior-preserving deletion: nothing in production calls the deleted code. The five-lens review records it as "dead-code removal, no live caller (grep evidence in §1.3)."

Open question OQ-1 (§9) asks reviewers whether to fully delete `Route.Load`/`Stats` or keep `Stats` for operator diagnostics.

## 4. Aggregation

### 4.1 Decision: leader-local consumption of the local sampler, no new collection RPC (M3); optional fan-out reuse later

The detector runs **only on the default-group leader** (where the scheduler must run anyway, since `SplitRange` mutates the catalog through the default group). The question is how the leader gets per-route load.

Options considered:

1. **New `ReportAccess` collection RPC** (parent doc §10.1 lists `ReportAccess` as not-yet-implemented). Followers periodically push their windows to the leader. Cost: a new wire RPC, follower-side scheduling, and merge logic.
2. **Piggyback on existing RPCs** (e.g. attach window deltas to heartbeat/lease traffic). Cost: couples an unrelated message to load telemetry; awkward versioning.
3. **Leader-local counting from the local sampler.** The detector reads *this node's* `MemSampler.Snapshot` for the routes this node leads. A node only writes/reads (and therefore only samples) traffic it serves; for a given route, the **leader** serves the writes and the lease/linearizable reads, so the leader's local sampler already holds the authoritative load for routes it leads. **Chosen.**

**Recommendation: option 3 (leader-local).** It needs no new wire surface, composes with the existing `--autoSplit` leader-local design (§7.6), and is sufficient because per-route write + leader-read load is observed where it lands — on the leader. The detector aggregates the sampler's recent windows (last N flush columns, default N=3 to match the parent doc's "3 consecutive windows" candidate rule, §5.2) per route the local node currently leads.

**Why not the keyviz cluster fan-out** (`docs/design/2026_04_27_proposed_keyviz_cluster_fanout.md`): that aggregates across nodes for the admin heatmap UI and dedupes write samples by `(groupID, leaderTerm)`. For split *decisions* we want the leader's own view of routes it leads — follower-observed counts for a route it does not lead are noise (forwarded reads are rare and bounded). Pulling cross-node data in would add latency and a dependency on the fan-out being configured. M3 stays leader-local. If a future milestone wants cluster-wide scoring it can read the same fan-out aggregate behind the same detector interface (§5) — recorded as OQ-5.

**Known limitation — multi-leader (multi-group) deployments.** Leader-local consumption is only authoritative for routes whose shard group is led by the **same node** that runs the detector — i.e. the default-group leader (where the scheduler must run, §6.1). In a multi-group cluster (`--raftGroups` with G1, G2, …), the default-group leader is **not necessarily** the leader of G1/G2/…: each node runs a single `ShardedCoordinator` and `MemSampler` and only `Observe`s traffic it *receives* (`kv/sharded_coordinator.go:1795-1824`). Concretely, in a 3-node cluster where node A leads the default group and node B leads G1, **node A's sampler has no data for routes in G1 served through node B**, so the detector on A cannot score or split them. This is **broader than "heavy follower-forwarded reads"**: it is a structural gap for any route whose group leader differs from the default-group leader, not just an edge case of read forwarding.

M3 treats this as an explicit **non-goal / known limitation, not a defect**: in M3, routes whose shard group's leader is a different node than the default-group leader are **invisible to the detector** (no load data → no candidacy → never auto-split). Under-observation only biases the detector conservative (it splits fewer routes, never an unsafe one — §2.2.5), so the limitation is safe, just incomplete. Closing it requires cross-node scoring (reading per-route load from the group leaders, e.g. via the keyviz cluster fan-out aggregate behind the same detector interface, §5). That is deferred to a future milestone and tracked as the reframed OQ-5 below. M3-PR2/PR3 reviewers and operators of multi-group clusters must understand this: **single-default-group deployments get full coverage; multi-group deployments get coverage only for routes the default-group leader also leads.**

### 4.2 What the detector consumes per route

For each route the local node leads, over the trailing window set:

- `read_ops`, `write_ops` (summed across the trailing N flush columns).
- `read_bytes`, `write_bytes` (for a future bytes-weighted score; not in the v1 score).
- sub-range bucket rows and/or Top-K hot keys for the route, when enabled (split-key selection, §5.4).

These come straight from `MemSampler.Snapshot(from, to)` `[]MatrixColumn` (`keyviz/sampler.go:1463`) filtered to routes whose `RaftGroupID` the local node currently leads (the `MatrixRow.RaftGroupID` field, `keyviz/sampler.go:449`, plus the engine's leader state per group).

**Deriving `from`/`to` (the time-range → trailing-N-columns recipe).** `Snapshot` takes a *time range*, not a column count, so the detector must convert "trailing `N` flush columns" into `(from, to)` carefully, **excluding the current still-accumulating column**. Each emitted column carries its flush instant in `MatrixColumn.At` (`keyviz/sampler.go:424`); `Flush()` (`keyviz/sampler.go:1178`) commits a column at each `Step` boundary. The recipe:

```
cols := Snapshot(now − (N+1)×Step, now)   // over-fetch by one column
to    := At of the most-recently *committed* column (cols[last].At)
from  := to − N×Step
window := columns in cols with from < At ≤ to   // the N committed columns
```

The over-fetch-then-trim avoids a subtle skew: a "trailing N columns" detector that naively called `Snapshot(now − N×Step, now)` would include the **current, partially-flushed** column (the bucket accumulating since the last boundary), summing it before it is full and biasing the per-route rate **low** toward the end of each flush window — exactly the partial-flush case to guard against. Anchoring `to` on the last *committed* `At` (not on `now`) drops that partial column.

The detector learns the last committed boundary from `MatrixColumn.At` of the freshest column in the snapshot; it does not need a separate handle into `RunFlusher` (`keyviz/flusher.go:19`). If `Snapshot` returns fewer than `N` committed columns (cold start, just after a flush-config change), the detector scores on what it has but does **not** advance the consecutive-over counter until at least `N` committed columns exist — see §5.2.

## 5. Detector

The detector is a pure function of (windowed per-route load, per-route detector state, config) → (split decisions). Keeping it pure makes §8's table-driven tests trivial and makes the leader-local reset (§7.6) a matter of dropping the state map.

### 5.0 Candidacy precondition: only `RouteStateActive` routes

Before any scoring, the detector filters candidates to routes in **`RouteStateActive`** only (`distribution/catalog.go:51-58`). Routes in a transitional state — `RouteStateWriteFenced`, `RouteStateMigratingSource`, `RouteStateMigratingTarget` — are **excluded from candidacy entirely**: they are not scored, not promoted, and never produce a split decision. This is the first gate of every evaluation cycle.

Rationale: a `WRITE_FENCED`/`MIGRATING_*` route is mid-cutover under an M2 `SplitJob` (parent doc §6, §7) or a manual fence. Issuing a `SplitRange` against such a route would race the in-flight migration and corrupt the cutover invariants. The state read comes from the same engine `CatalogSnapshot` the detector already loads each cycle (§4.2, `RouteDescriptor.State`), so the gate costs one field compare per route. This is strictly stronger than, and subsumes, the §6.3 "exclude routes with an in-flight `SplitJob`" rule — a fenced/migrating route is excluded even before the scheduler looks at job state. The exclusion is asserted in the §8.2 unit tests (a route in each non-active state must never be promoted) and the §8.2 rapid invariants.

### 5.1 Scoring

Per route, per evaluation cycle, the score is a **weighted ops/min rate** computed over the trailing `N` flush columns. The formula is pinned (resolves OQ-8 — it is no longer left to implementation):

```
N          = candidateWindows            // trailing flush columns summed (default 3, §5.2)
Step_s     = keyvizStep.Seconds()        // flush-column width in seconds (default 60)
write_rate = (Σ write_ops over N columns) / (N × Step_s) × 60   // ops/min
read_rate  = (Σ read_ops  over N columns) / (N × Step_s) × 60   // ops/min
score      = write_rate × Ww + read_rate × Wr                   // weighted ops/min
```

Defaults from the parent doc §6.2: `Ww = 4`, `Wr = 1`, `threshold = 50_000 ops/min`.

**Why this normalization.** Dividing by `N × Step_s` and rescaling by 60 makes `score` an **ops-per-minute** rate, so the `50_000 ops/min` threshold means the same thing regardless of `--keyvizStep` and regardless of how many columns are summed — the `--keyvizStep` ↔ sensitivity coupling that OQ-8 worried about is removed by construction. `ops/min` is also the unit operators reason in when setting `--autoSplitThreshold`, so the flag and the formula speak the same language. (We score over the same `N = candidateWindows` columns the hysteresis counter advances on, §5.2, so "score over N windows" and "N consecutive windows over threshold" refer to the same window set; if a future need arises to decouple them, add an `--autoSplitWindowColumns` flag defaulting to `candidateWindows` — not done in M3 to keep the surface small.)

This exact formula (with named constants `N`, `Step_s`, `Ww`, `Wr`) is the reference for the §8.2 table-driven scoring tests, so the test cases are unambiguous: given a column vector of `(read_ops, write_ops)` and a `Step`, the expected `score` is fully determined.

**Partial-column guard.** The trailing column set is taken from `MemSampler.Snapshot(from, to)` (a *time range*, not a column count — `keyviz/sampler.go:1463`), so the detector must avoid including the **current, still-accumulating** flush column (it would be summed before it is full and bias the rate low near the end of each window). See §4.2 for how `from`/`to` are derived from the last *committed* flush boundary.

### 5.2 Hysteresis (candidate promotion)

A route is promoted to a **split candidate** only after `score >= threshold` for `candidateWindows` **consecutive flush windows** over threshold (parent doc §6.2: "Candidate after 3 consecutive windows over threshold"; default `candidateWindows = 3`). A single window below threshold resets the consecutive counter to zero. This is the up-side hysteresis that prevents a one-off burst from triggering a split.

**The counter advances per new flush column, not per evaluation cycle.** `--autoSplitEvalInterval` defaults to `--keyvizStep` but can be set independently, and when `evalInterval < Step` multiple evaluation cycles complete before a new flush column is committed. If the detector advanced `consecutiveOver` on every cycle, the same N columns would be re-counted: with `evalInterval = 5s` and `Step = 60s`, a candidate could be "promoted" in 15 s (3 cycles) on a *single* window of data — defeating the "3 consecutive flush windows" intent. The detector therefore **only advances (or resets) `consecutiveOver` when a new committed flush column has appeared since the last evaluation**. Mechanism: it records the `MatrixColumn.At` of the freshest committed column at the end of each evaluation; a cycle whose freshest `At` is unchanged from the last is a **no-op for the counter** (it may still recompute and log the score, but does not touch hysteresis state, cooldown, or emit a decision). This makes promotion timing a function of flush *windows* elapsed, independent of how often the detector wakes. Conversely, if `evalInterval > Step` and several columns accumulate between cycles, the detector processes each new column once (so a burst spanning multiple skipped columns still advances/resets the counter per column, not per cycle) — see §8.2 tests.

Down-side hysteresis: the detector tracks a per-route `consecutiveOver` counter in its leader-local state map; falling below `threshold * hysteresisDownFactor` (default 0.8) clears it immediately, while scores in the `[0.8×threshold, threshold)` band hold the counter (neither increment nor reset) so a route hovering at the line does not oscillate between "almost candidate" and "reset." (OQ-2 asks whether the band should instead decay the counter.)

### 5.3 Cooldown (per route)

After a split is *scheduled* for a route (the `SplitRange` call is issued), both child routes enter a **cooldown** of `splitCooldown` (parent doc §6.2 default 10 minutes). During cooldown the detector will not promote either child to a candidate, regardless of score. Cooldown is keyed by route lineage (the child `RouteID`s produced by the split / observed via the next catalog snapshot), tracked in the leader-local state map with a wall-clock expiry that is **diagnostic-only** for logging but enforced via a monotonic deadline (`internal/monoclock`) so a wall-clock step cannot shorten or extend a cooldown (CLAUDE.md: no ordering-sensitive use of wall clock).

**Compound single-key isolation is cooldown-exempt for its own intermediate child.** The §5.4.2 single-key-isolation flow first splits `[A,B) → [A,H) + [H,B)`, then immediately splits the *intermediate* child `[H,B)` again to isolate `{H}`. If the first split's cooldown applied to `[H,B)`, the second `SplitRange` could never fire and the hot key would never be isolated — which is the contradiction review flagged. The detector therefore treats the compound decision as a **single logical split**: cooldown is applied to the **final** children only — `[A,H)`, `{H}` = `[H, H·0x00)`, and `[H·0x00, B)` — and the *transient* intermediate child `[H,B)` is exempt because it exists only between the two calls of the same atomic operation and is consumed by the second call. This exemption is scoped strictly to the compound operation's own intermediate child (matched by lineage during the cycle that issues it); it does **not** weaken cooldown for any independently-scheduled split. The §8.2 tests assert (a) the three final children are all in cooldown after a compound split, and (b) the intermediate child is never separately re-promoted by the detector.

### 5.4 Split-key selection (not midpoint-only)

Per parent doc §3.1.3 and §6.3, the split key comes from the **observed key distribution**, not a byte midpoint:

1. **Default — sub-range p50.** When `--keyvizKeyBucketsPerRoute > 1`, use the route's sub-range bucket rows to find the bucket whose **cumulative load first crosses 50%**, and use the **`hi` (upper boundary)** of that bucket — `SubBucketBoundsFor(routeID, bucket)` returns `(lo, hi []byte, ok bool)` (`keyviz/sampler.go:559`); the split key is the `hi` of the median bucket, i.e. the boundary *between* the p50 bucket and the next one, so all of the p50 bucket's load stays in the left child. This places the boundary where load is actually halved.
   - **Boundary-bucket fallback (review, codex P2).** `SubBucketBoundsFor` returns the **route's own `End`** as `hi` for the last sub-bucket (and `nil` for an unbounded tail; `keyviz/sampler.go:570-575` — the single-bucket case at `:571-573` likewise returns `start, end`). When the median (p50) bucket is the **last** bucket — an upper-tail hotspot where the final bucket alone carries `> 50%` of the load — its `hi` equals `route.End`, and §5.4's `split_key < route.End` rule plus `validateSplitKey`'s boundary rejection (`adapter/distribution_server.go:359-360,370-371`: `splitKey == parent.End` → `errDistributionSplitKeyAtBoundary`) would make that key invalid, so the detector would drop the decision and never split a genuinely hot tail. The detector therefore **falls back to the median bucket's `lo` (its lower boundary) when the chosen `hi` is `>= route.End` (or `route.End == nil`)**: an interior boundary that still places the dominant last bucket in the *right* child while satisfying `route.Start < split_key < route.End`. Symmetrically, if the p50 bucket is the **first** bucket and its `lo` equals `route.Start`, the detector uses that bucket's `hi` instead (it is necessarily interior because a route always has `> 1` sub-bucket on this path). If *both* candidate boundaries coincide with a route edge (degenerate single-sub-bucket route, only possible when `--keyvizKeyBucketsPerRoute == 1`, which this path is gated out of), the detector declines per §5.4.3. The §8.2 split-key tests add the "p50 in the last bucket → interior `lo` fallback" and "p50 in the first bucket → interior `hi` fallback" cases.
2. **Single-key skew — isolate the hot key (compound split).** When Top-K is enabled (`--keyvizHotKeysEnabled`) and one key's share of the route's writes is `>= topKeyShare` (parent doc §6.3 default 0.8), split so the hot key `H` lands in its own single-key child. Isolating `H` from a route `[A, B)` normally takes **two boundaries** — one at `H` and one at `H`'s successor `H·0x00` — so the middle child is exactly `[H, H·0x00)` = `{H}`. The detector emits this as **one atomic compound decision** carrying both split keys, not two independent decisions across cycles. The scheduler executes it as a back-to-back pair of `SplitRange` calls (first `[A,B) → [A,H) + [H,B)`, then `[H,B) → [H, H·0x00) + [H·0x00, B)`), counted and budgeted as described in §6.4 (compound = 1 budget unit, cooldown exempt for the intermediate child). This is the single-hot-key hotspot case the whole feature targets.

   - **Edge case — `H` already at the lower route boundary (`H == route.Start`) (review, codex P2).** `validateSplitKey` rejects `splitKey == parent.Start` (`adapter/distribution_server.go:358-360` → `errDistributionSplitKeyAtBoundary`), so the two-boundary form's *first* call `SplitRange([A,B), split_key=H)` is invalid whenever the hot key is already the route's start (the natural state after a prior isolation split at `H`, or for any route whose lower bound *is* the hot key). `{H}` is then already pinned to the left edge and needs only **one** boundary at `H·0x00`: a single `SplitRange([A,B), split_key=H·0x00)` yields `[A, H·0x00) = [H, H·0x00) = {H}` (since `A == H`) plus `[H·0x00, B)`. The detector special-cases `H == route.Start` to emit the **single-split** form (one `SplitRange`, one budget unit) instead of the two-boundary compound; if `H·0x00 >= route.End` (the route is already exactly `{H}`), the key is fully isolated and the detector declines (nothing to split). The §8.2 split-key tests add the `H == route.Start` single-split case and the already-isolated `[H, H·0x00)` decline case.

   If Top-K is **not** enabled, single-key isolation is unavailable; the detector falls through to the §5.4.1 sub-range-p50 path (which still relieves a *range* skew but cannot pin a single key), so the operator loses only this path, not all splits.
3. **Fallback — coarse boundary.** When neither sub-range buckets nor Top-K are enabled (route-granular sampling only), the detector has no intra-route distribution. Rather than fall back to a blind midpoint (which the parent doc forbids and which can produce an empty child), the detector **declines to split** and emits a metric/log (`autosplit_skipped_total{reason="no_split_key"}`) telling the operator to raise `--keyvizKeyBucketsPerRoute`. OQ-3 asks whether a midpoint-of-observed-min/max-key fallback is acceptable here instead of declining.
4. **Unbounded-tail routes (`End == nil`).** Derive the split key from observed keys (sub-range layout over `[start, MaxUint64]` already supports this, `keyviz/sampler.go:833-848`), never from a synthetic byte.

The selected split key must satisfy `route.Start < split_key < route.End` (or `< +inf`); the detector validates this against the live engine route before emitting a decision and drops the decision (with a metric) if the key no longer falls inside the route (the route may have shifted under a concurrent split).

### 5.5 Guardrails

- **Minimum route size** (`minRouteSpan`, OQ-4): a route whose observed key span (distance between min and max observed key, or sub-range extent) is below a floor is not split — splitting a tiny range yields children too small to relieve load and churns the catalog. Because the engine has no per-route key-count, this is approximated from the sub-range/Top-K evidence; when that evidence is absent the guard is conservatively "do not split" (matches §5.4.3).
- **Max routes** (`maxAutoRoutes`, default e.g. 1024): once the catalog holds `maxAutoRoutes` routes, the detector stops proposing new splits and emits `autosplit_skipped_total{reason="route_cap"}`. Prevents unbounded catalog growth (and bounds the watcher fan-out and the Composed-1 history-ring pressure noted at `distribution/engine.go:53-58`).
- **Per-cycle split budget** (`maxSplitsPerCycle`, default 1): at most this many splits are scheduled per evaluation cycle, so a cluster-wide load spike across many routes cannot trigger a split storm in one tick. Remaining candidates carry their `consecutiveOver` state to the next cycle.

## 6. Scheduler

### 6.1 All catalog mutations go through `SplitRange`

The scheduler takes the detector's decisions and, for each, calls the existing `proto.Distribution.SplitRange` RPC against the default-group leader (handler `adapter/distribution_server.go:136`). It **never** mutates the engine or catalog directly — this is the CLAUDE.md invariant and the reason the engine's in-memory `splitRange` is retired (§3.4). Going through `SplitRange` means the catalog version bumps, the watcher fans the new routes out to every node, and the new child `RouteID`s become observable for cooldown tracking (§5.3) via the next `CatalogSnapshot`.

A `SplitRangeRequest` carries `expected_catalog_version` (CAS) so a scheduler issuing a split against a catalog that shifted under it fails closed; the scheduler re-reads the engine version and re-evaluates next cycle rather than retrying blindly.

### 6.2 Same-group today; least-loaded target once M2 lands

- **M3 standalone (no M2):** `SplitRange` does a same-group split — both children stay in the source group. This already relieves a hot **range** by halving the key span each child serves and is the first-value deliverable. (It does not relieve a single-group CPU/IO hotspot, since both children stay on the same Raft group — that needs M2's cross-group move.)
- **Once M2 lands:** the scheduler selects `target_group_id` for the right child via a **least-loaded-group policy** and passes it on the same `SplitRange` RPC (M2's new field). "Least-loaded" is computed from the same keyviz per-group aggregate (sum of route scores per group) the detector already has, choosing the group with the lowest aggregate score among groups in `--raftGroups`, excluding the source group and any group already targeted by an in-flight `SplitJob`. The migration mechanics (the resumable job) are entirely M2's; the scheduler just chooses the target and fires the RPC, then tracks the returned `job_id` for observability.

The target-group selection slots **behind the same `SplitRange` interface** — the scheduler code path is identical; only the `target_group_id` field flips from zero (same-group) to a chosen group once M2 is present. M3 ships the policy hook disabled (always same-group) and a follow-on M3.x / M2-completion PR enables target selection. OQ-6 asks whether to gate target selection on an explicit `--autoSplitCrossGroup` sub-flag.

### 6.3 Concurrency with in-flight splits

The scheduler issues at most `maxSplitsPerCycle` (§5.5) `SplitRange` calls per cycle and waits for each to return (same-group splits are synchronous; cross-group returns a `job_id` immediately per M2 §5.1). A route with an in-flight cross-group `SplitJob` (not yet `DONE`) is excluded from candidacy until the job completes, so the detector cannot stack a second split on a route mid-migration.

### 6.4 Compound single-key isolation execution and budget

The §5.4.2 single-key-isolation decision is one logical operation that the scheduler executes as two back-to-back `SplitRange` calls within the same cycle:

1. `SplitRange([A,B), split_key=H, expected_catalog_version=v0)` → `[A,H) + [H,B)`, returns `v1` and the intermediate child's `RouteID`.
2. `SplitRange([H,B), split_key=H·0x00, expected_catalog_version=v1)` → `[H, H·0x00) + [H·0x00, B)`.

Budget and cooldown treatment:

- **Budget: the compound counts as 1 unit** against `maxSplitsPerCycle`, not 2. The per-cycle budget exists to bound *distinct hot routes* split per tick (anti-storm); isolating one hot key is a single anti-storm event, so it consumes one unit even though it issues two RPCs. This keeps the default `maxSplitsPerCycle = 1` workable for single-key isolation without forcing operators to raise it to 2.
- **CAS chaining.** The second call uses the `v1` returned by the first (not the cycle's starting `v0`), so the pair is serialized against the live catalog version. If the first call's CAS fails (a concurrent manual split landed), the scheduler abandons the compound, emits `autosplit_splits_failed_total{reason="cas_conflict"}`, and re-evaluates next cycle — it does **not** issue the second call against a stale version.
- **Partial-completion safety.** If the first call commits but the second fails (CAS conflict or RPC error), the catalog is left in a valid two-child state `[A,H) + [H,B)` — `[H,B)` is simply a narrower route that the detector re-evaluates next cycle (it is now in cooldown only if the §5.3 rule applied it; per §5.3 the intermediate `[H,B)` is exempt, so it is eligible next cycle and the detector can retry the isolation). No invariant is broken by stopping after the first call; the worst case is the hot key takes one extra cycle to isolate. This is logged `autosplit_compound_partial`.
- Cooldown applies to the three **final** children only, per §5.3.

## 7. Safety and Operations

### 7.1 Default OFF behind a flag

- `--autoSplit` (bool, default `false`). When false, none of the detector/scheduler goroutines start; behavior is identical to today. When true, the default-group leader runs the detector + scheduler loop, and the keyviz sampler is constructed and flushed if it is not already.
- **Sampler-wiring condition.** The sampler is built when `keyvizEnabled || autoSplit` (today it is built only on `keyvizEnabled`; see `main.go:2005` `newKeyvizSampler` and the analogous wiring in `cmd/server/demo.go`). M3-PR3's flag-wiring change touches both `main.go` and `cmd/server/demo.go`. When auto-split forces the sampler on and the operator **did not explicitly set** `--keyvizKeyBucketsPerRoute` (detected via `flag.Visit` after `flag.Parse()`, see §3.1 — plain `flag.Int` cannot distinguish an omitted flag from an explicit `1`), auto-split raises it to `autoSplitDefaultBuckets` (default 16) so split-key evidence exists by default (§3.1 table). An explicit `--keyvizKeyBucketsPerRoute 1` is honored as-is (operator opts into route-granular-only → §5.4.3 decline). `--autoSplitDefaultBuckets` is itself clamped to `[2, MaxKeyBucketsPerRoute]` (256, `keyviz/sampler.go:115`) and validated at startup — a value below 2 would defeat the purpose, and above the cap is rejected like the other keyviz bucket flags.
- Tuning flags (all with parent-doc-derived defaults): `--autoSplitWriteWeight` (4), `--autoSplitReadWeight` (1), `--autoSplitThreshold` (50000 ops/min), `--autoSplitCandidateWindows` (3), `--autoSplitCooldown` (10m), `--autoSplitMaxRoutes`, `--autoSplitMaxPerCycle` (1), `--autoSplitEvalInterval` (evaluation cycle period, default = `--keyvizStep`), `--autoSplitDefaultBuckets` (sub-range buckets auto-split implies when keyviz bucketing is left at the default, default 16).

### 7.2 Runtime kill switch

The detector/scheduler loop checks an `atomic.Bool` enable flag each cycle. An admin RPC / endpoint (reuse the existing admin surface that hosts keyviz/distribution operator calls) toggles it without a restart. Flipping it off cancels in-progress scheduling at the next cycle boundary; it does **not** abort an already-issued cross-group `SplitJob` (that is M2's `AbandonSplitJob`). The kill switch is the operator's "stop making new splits NOW" lever.

### 7.3 Metrics (bounded cardinality)

All metrics are **per-node** with bounded label sets — **no per-route or per-key labels** (route count is operator-unbounded; that would blow up cardinality). Labels are limited to fixed enums.

Counters (the two `{reason}` counters partition cleanly by **whether a `SplitRange` RPC was issued**: `..._failed` = the RPC was issued and rejected/errored; `..._skipped` = the detector declined *before* issuing any RPC):
- `autosplit_candidates_promoted_total`
- `autosplit_splits_scheduled_total`
- `autosplit_splits_failed_total{reason}` — a `SplitRange` RPC **was issued** and failed; `reason` ∈ {`cas_conflict`, `rpc_error`, `target_unavailable`}.
- `autosplit_skipped_total{reason}` — the detector **did not issue** a `SplitRange` RPC; `reason` ∈ {`no_split_key`, `route_cap`, `budget_exhausted`, `non_active_state`, `unsplittable_hot_key`} (fixed enum).
- `autosplit_compound_partial_total` — a compound single-key isolation (§6.4) committed its first split but not the second; the hot key will be isolated on a later cycle.

Gauges:
- `autosplit_enabled` (0/1)
- `autosplit_tracked_routes` (size of the leader-local state map)
- `autosplit_cooldown_active` (count of routes currently in cooldown)
- `autosplit_eval_duration_seconds` (last cycle wall time, diagnostic)

The per-route score itself is **not** a metric (unbounded route cardinality); it surfaces only in slog at the moment a candidate is promoted or a split is scheduled.

### 7.4 slog keys

Structured `slog` with stable keys per CLAUDE.md conventions: `route_id`, `group_id`, `score`, `read_ops`, `write_ops`, `threshold`, `consecutive_over`, `split_key` (hex-encoded, length-capped), `target_group_id`, `catalog_version`, `job_id` (M2), `cooldown_until` (diagnostic wall time), `decision` ∈ {`promote`, `schedule`, `skip`, `cooldown`, `cap`}. Each scheduled split logs exactly one line at the decision point and one at the RPC result.

### 7.5 Failure modes

- **Detector flapping** (route oscillates around threshold): mitigated by up-side `candidateWindows` consecutive-window hysteresis (§5.2) + down-side `hysteresisDownFactor` band (§5.2) + per-route `splitCooldown` (§5.3).
- **Split storms** (many routes hot at once): mitigated by `maxSplitsPerCycle` per-cycle budget (§5.5) and `maxAutoRoutes` cap.
- **Empty/degenerate child** (split key produces a child with no keys): prevented by distribution-based split-key selection (§5.4) — the key sits at the observed load median, never a synthetic byte; plus the `route.Start < split_key < route.End` validation. When no distribution evidence exists, the detector declines (§5.4.3) rather than risk an empty child.
- **Catalog churn under the scheduler** (route shifted between detection and `SplitRange`): the `expected_catalog_version` CAS fails closed; the scheduler re-evaluates next cycle (§6.1).
- **Repeated re-split of the same hotspot** that a single split cannot relieve (genuinely un-splittable single-key hotspot, e.g. one key taking all writes): single-key isolation (§5.4.2) splits the key into its own child once; cooldown + the `minRouteSpan` guard then prevent infinitely re-splitting a child that is already a single key (a one-key range cannot be split further — `split_key` validation has no valid interior key, the detector declines and counts `autosplit_skipped_total{reason="unsplittable_hot_key"}`). This is the limit of split-based mitigation; truly relieving a single-key hotspot is an application/data-model problem, not a sharding one.

### 7.6 Interaction with leader changes

The detector's hysteresis counters and cooldown deadlines live in a **leader-local in-memory state map** — they are **not** replicated through Raft. On a default-group leadership change:

- The deposed leader's detector goroutine stops (it observes the leader-loss callback. The concrete mechanism is the same per-group leadership-transition signal the lease layer consumes to invalidate the read lease — `kv/lease_state.go` (lease invalidation on leadership loss) driven from the etcd-raft `SoftState`/leader-change callback surfaced through `kv/raft_engine.go`. M3-PR3 subscribes the detector goroutine's lifecycle to that same signal rather than introducing a new one).
- The **new** leader starts its detector with an **empty** state map. Consecutive-over counters reset to zero, so a route must re-accumulate `candidateWindows` cycles before it can be promoted again. This is intentionally conservative: after an election, the new leader re-earns confidence before splitting, avoiding a split based on counters it never observed.
- **Cooldowns are reconstructed from a durable timestamp, not guessed from lineage.** See §7.7 — the new leader seeds cooldown from a new `RouteDescriptor.SplitAtHLC` field (a durable, leader-fenced commit ts) rather than from `parent_route_id` recency. In-flight cross-group `SplitJob`s are durable (M2) so the new leader also sees them and excludes those routes (§6.3) regardless of detector state.

Keeping detector state leader-local (not Raft-replicated) is a deliberate cost/safety trade: replicating per-cycle counters would add Raft traffic for what is a best-effort heuristic, and the worst case of a lost counter is a *delayed* split (safe), never an *erroneous* one. We state this explicitly so a reviewer does not mistake the non-replication for an oversight.

### 7.7 Bounding the leader-local state map and reconstructing cooldown

Two correctness properties of the leader-local state map, both raised in review:

**(a) The state map is bounded — no unbounded growth.** Route IDs are retired when a route is split (the parent `RouteID` disappears from the catalog and is replaced by child `RouteID`s) or merged (future work). If the detector kept a hysteresis/cooldown entry for every `RouteID` it ever saw, the map would grow without bound across the cluster's lifetime even though the live route set is bounded by `maxAutoRoutes` (§5.5). The detector therefore **reconciles the state map against the live catalog at the start of every evaluation cycle**:

- Build the set of live `RouteID`s from the current engine `CatalogSnapshot` (already loaded for scoring, §4.2).
- Evict any state-map entry whose `RouteID` is not in that set. This GC step is O(map size) per cycle, off the hot path, and runs at `--autoSplitEvalInterval` cadence.

This makes the map size strictly bounded by the live route count (`<= maxAutoRoutes`), and the `autosplit_tracked_routes` gauge (§7.3) reflects the post-GC size so an operator can see it never exceeds the cap. The reconciliation also resolves the **manual-split / disappeared-route** case raised in review: when a route is removed out-of-band (a manual `SplitRange`), its stale entry is dropped at the next cycle instead of the detector wasting work re-evaluating a route the engine no longer knows. The §8.2 tests assert the map shrinks after a split retires a parent `RouteID` and that an evicted route's counters do not survive.

**(b) Cooldown reconstruction needs a durable timestamp — add `RouteDescriptor.SplitAtHLC`.** Review correctly observed that the current `RouteDescriptor` (`distribution/catalog.go:71-78`: `RouteID, Start, End, GroupID, State, ParentRouteID`) carries **no creation/split timestamp**, so "seed cooldown for routes whose creation is recent per the catalog" had no basis in the schema — a new leader could not tell a 1-minute-old child from a 1-hour-old one. The original §7.6 wording was therefore unimplementable; this section replaces it.

M3 adds one durable field to `RouteDescriptor`:

- **`SplitAtHLC uint64`** — the **committed HLC timestamp of the `SplitRange` transaction that produced this route**. This must be a leader-fenced, Raft-committed timestamp, **not** an arbitrary `HLC.Next()` allocation (review correctly flagged this: `HLC.Next()` bypasses the physical-ceiling fence — `kv/hlc.go:110-128` — so a stale leader could mint a `SplitAtHLC` inside a window the ceiling fence is meant to forbid, the exact case the fence exists to prevent). The safe source is already at hand: the `SplitRange` handler commits the catalog mutation through `coordinator.Dispatch` (`adapter/distribution_server.go:229-235`), and the coordinator allocates that transaction's commit ts via the **fenced** path (`resolveTxnCommitTS`, `kv/sharded_coordinator.go:1091-1107` → `nextTxnTSAfter`, `:1365-1379` → `NextFenced()`). So `SplitAtHLC` is **defined to be that transaction's commit ts** — the same value the split mutation is durably applied at.
  - **Mechanism (so the stored value equals the actual commit ts).** Today `CoordinateResponse` returns only `CommitIndex`, not the commit ts (`kv/coordinator.go:165-167`), so the handler cannot read the ts back after dispatch. Instead the handler **pre-allocates** the fenced commit ts on the default-group leader, stamps it into both child `RouteDescriptor.SplitAtHLC` *before* encoding, and passes it as the explicit `OperationGroup.CommitTS` (with `StartTS = snapshot.ReadTS`). The coordinator honors a caller-supplied `CommitTS` (it `Observe`s it to keep the clock monotonic, `kv/sharded_coordinator.go:1098-1102`) rather than minting a fresh one, so the value encoded in the descriptors is provably the transaction's commit ts.
    - **Pre-allocation must guarantee `CommitTS > StartTS` (review, codex P2).** `resolveTxnCommitTS` rejects any caller-supplied `commitTS <= startTS` with `ErrTxnCommitTSRequired` (`kv/sharded_coordinator.go:1103-1105`) — it does **not** retry on the caller's behalf. A naive single `HLC.NextFenced()` can return a value `<= snapshot.ReadTS` after a restart or leader change: when the durable catalog's latest ts shares the same physical-ms component as this process's clock but carries a higher logical counter than the local HLC has observed, the fresh `NextFenced()` lands at-or-below `ReadTS` and the dispatch fails even though a valid fenced ts exists. The handler therefore **mirrors `nextTxnTSAfter(startTS)` exactly** (`kv/sharded_coordinator.go:1365-1388`): allocate `ts = HLC.NextFenced()`; if `ts <= snapshot.ReadTS`, `HLC.Observe(snapshot.ReadTS)` and re-allocate `ts = HLC.NextFenced()`; only then stamp `ts` into `SplitAtHLC` and pass it as `CommitTS`. After the Observe-and-retry the clock has been bumped strictly past `ReadTS`, so the second `NextFenced()` is `> ReadTS` and the dispatch's `CommitTS > StartTS` check passes. (Factoring `nextTxnTSAfter` into an exported coordinator helper the `SplitRange` handler can call is preferable to re-implementing the retry in the adapter, so the fence/retry semantics stay in one place.)
    - **Ceiling-expired path.** If either `NextFenced()` returns `ErrCeilingExpired` (the leader's physical ceiling has lapsed), the handler fails the `SplitRange` closed (`FailedPrecondition`) instead of writing an unfenced timestamp — a brief, safe rejection, not a corrupt record.
  - **Safety across leader changes.** Because the ts is fenced, a newly elected (or stale, deposed) leader can never write a `SplitAtHLC` inside the previous leader's lease window; combined with the physical-ceiling clamp on `Next()`, `SplitAtHLC` is monotonic across elections and never under-counts a cooldown. This is what makes the §7.6 cooldown reconstruction trustworthy after an election.
- **Encoding / back-compat.** `SplitAtHLC` is encoded/decoded alongside `ParentRouteID` in `EncodeRouteDescriptor`/`DecodeRouteDescriptor` (`distribution/catalog.go:168-217`). See §7.7c for the codec-version handling and the rolling-upgrade fence — the decoder treats a record without the field as `SplitAtHLC = 0` (= "unknown / pre-upgrade", never in cooldown), so new code reading old records is safe by construction.
- **In-memory copy/compare helpers must include the new field (review, Issue 2).** Adding `SplitAtHLC` to the `RouteDescriptor` struct compiles **without** any exhaustiveness check, so two struct-by-field helpers in `distribution/catalog.go` would silently drop the field unless M3-PR1 updates them. Both are easy to miss in code review and are therefore named in the M3-PR1 checklist (§8.1):
  - **`CloneRouteDescriptor` (`distribution/catalog.go:368-377`)** copies fields by name into a struct literal. Every route on the write path is cloned through `normalizeRoutes → prepareSave → Save`, so if the literal omits `SplitAtHLC` the field is **zeroed before encoding** — the durable timestamp is lost on every save even though the encoder writes it. M3-PR1 adds `SplitAtHLC: route.SplitAtHLC` to the literal (it is a scalar `uint64`; no `CloneBytes` needed).
  - **`routeDescriptorEqual` (`distribution/catalog.go:693-700`)** is used by `appendUpsertRouteMutations` to skip re-encoding routes that did not change. Without `SplitAtHLC` in the equality test, two descriptors that differ *only* in `SplitAtHLC` compare equal, so an update that changes a route's `SplitAtHLC` (with the same `RouteID`/`Start`/`End`/`GroupID`/`State`/`ParentRouteID`) is **skipped and never written**. M3-PR1 adds `left.SplitAtHLC == right.SplitAtHLC` to the comparison. The §8.1 codec/helper tests assert (i) a clone preserves `SplitAtHLC`, and (ii) two routes differing only in `SplitAtHLC` are *not* equal.

With `SplitAtHLC` present, the new leader reconstructs cooldown from the durable fenced timestamp (robust to bounded wall-clock skew, not guessed from lineage). **`SplitAtHLC` must be converted to physical milliseconds (`hlcPhysicalMs`) before any duration arithmetic, and the present time is read from the wall clock** (see §7.7d for the conversion and the `now`-source decision): for each live route, if `time.Now().UnixMilli() − hlcPhysicalMs(SplitAtHLC) < splitCooldownMs`, seed that route's cooldown deadline to expire `splitCooldown` after the split's physical instant (tracked on the leader's monotonic clock per §5.3, so a wall-clock step cannot shorten or extend the enforcement deadline once seeded). This uses the durable fenced timestamp rather than `parent_route_id` recency (which only tells you *whether* a route is a child, not *when*).

**(c) Codec version and rolling upgrades.** Adding `SplitAtHLC` changes the on-wire `RouteDescriptor` record. The current decoder is **strict in two places**, and both must be relaxed (review Issue 1):

1. The **version-byte check** (`distribution/catalog.go:200-202`: `if raw[0] != catalogRouteCodecVersion { return …, ErrCatalogInvalidRouteRecord }`) rejects any record whose leading version byte is not exactly `catalogRouteCodecVersion` (= `1` today, `distribution/catalog.go:24`).
2. The **trailing-bytes check** (`distribution/catalog.go:213-215`: `if r.Len() != 0 { return …, ErrCatalogInvalidRouteRecord }`) runs unconditionally after `decodeRouteDescriptorEnd` and rejects *any* bytes left in the reader. **A v2 record carries an 8-byte `SplitAtHLC` tail, so even after the version-byte check is relaxed this second check would still reject every v2 record** — defeating the accept-and-ignore scheme entirely. Both checks are in `DecodeRouteDescriptor` and both are in M3-PR1's change scope.

A naive bump to v2 therefore breaks the *old-code-reads-new-record* direction: in a rolling upgrade an M3 node would write v2 records and any not-yet-upgraded node decoding them would hit `ErrCatalogInvalidRouteRecord`, which propagates up through `Snapshot` and can surface as route-not-found on that node. (The other direction — new code reading old v1 records — is the easy one and is handled by the `SplitAtHLC = 0` default in (b).)

We resolve this by **relaxing the decoder to accept-and-ignore unknown trailing bytes for forward versions, and shipping that relaxation ahead of the v2 writer**. The relaxed `DecodeRouteDescriptor` has precise, version-keyed semantics:

- **Version-byte check → minimum, not equality.** Introduce `catalogRouteCodecVersionMin` (= `1`) and `catalogRouteCodecVersion` (bumped to `2`). Reject only `raw[0] < catalogRouteCodecVersionMin` (truly old/corrupt) with `ErrCatalogInvalidRouteRecord`; accept any `raw[0]` in `[catalogRouteCodecVersionMin, catalogRouteCodecVersion]` and treat any `raw[0] > catalogRouteCodecVersion` as a forward version (parse the fields this build knows, ignore the rest — see the trailing-bytes rule below). The version byte is retained in a local `version` variable for the field-reading step.
- **Field reading is version-keyed.** After `decodeRouteDescriptorHeader` and `decodeRouteDescriptorEnd` (unchanged), **read the 8-byte big-endian `SplitAtHLC` tail iff `version >= 2`**. For a v1 record (`version == 1`) there is no tail, so `SplitAtHLC` stays `0`. For a `version >= 2` record, read exactly 8 bytes for `SplitAtHLC`.
- **Trailing-bytes check → skip-unknown-tail, not reject.** Replace `if r.Len() != 0 { return …, err }` with **"discard any bytes remaining after the fields this version knows."** Concretely: after the version-keyed field reads, drain the reader (`io.Copy(io.Discard, r)` or `r.Seek(0, io.SeekEnd)`) instead of erroring. This makes the decoder tolerant of *both* directions:
  - **New code (v2 decoder) reading a future v3+ record** — reads `SplitAtHLC`, drains whatever v3 appended, returns a valid `RouteDescriptor`.
  - **Old code (the v1-era decoder shipped *before* the bump) reading a v2 record** — this is the rolling-upgrade case the fence is about. The relaxed v1-era decoder, having no `version >= 2` branch, reads through `End`, then drains the 8-byte `SplitAtHLC` tail instead of erroring. It loses only the cooldown hint (equivalent to `SplitAtHLC = 0`, which is safe per (b)).
- **Encoder.** `EncodeRouteDescriptor` writes `catalogRouteCodecVersion` (now `2`) and appends `SplitAtHLC` as a fixed 8-byte big-endian tail after the `End` block; `routeDescriptorEncodedSize` adds `catalogUint64Bytes` (`distribution/catalog.go:708`).
  - **The `SplitAtHLC` tail must be appended on *both* End-encoding paths (review, load-bearing).** The current encoder (`distribution/catalog.go:183-192`) has an **early `return out, nil`** on the `End == nil` path (`:185`) — it emits the `0` end-flag byte and returns *before* reaching the bottom of the function. Appending `SplitAtHLC` "after the `End` block" at the bottom would therefore **silently skip the tail on every `End == nil` route**, and the v2 decoder (which unconditionally reads the 8-byte tail for `version >= 2`) would then fail with `ErrCatalogInvalidRouteRecord` on that record. This is not a corner case: the **rightmost route always has `End == nil`**, and a split of it produces an `End == nil` right child, so the bug would fire on the live write path. The idiomatic fix is to **convert the early return into an `if`/`else` over the end-flag and append `SplitAtHLC` exactly once at the end**, so both paths fall through to it:

    ```go
    if route.End == nil {
        out = append(out, 0)
    } else {
        out = append(out, 1)
        out = appendU64(out, uint64(len(route.End)))
        out = append(out, route.End...)
    }
    out = appendU64(out, route.SplitAtHLC) // v2 tail — reached on both End paths
    return out, nil
    ```

    The v2 decoder needs no special-casing here: it reads the 8-byte tail *after* `decodeRouteDescriptorEnd` (`distribution/catalog.go:209`), which already consumes the end-flag and the optional `End` block for both `endFlag == 0` and `endFlag == 1`, so the tail lands at the same offset regardless of the `End == nil` / non-nil case.
- **Upgrade ordering fence (documented ops constraint).** Even with accept-and-ignore, the safe rollout is: deploy the relaxed-decoder build (the relaxation of *both* checks above) to **all** nodes first; only then does any node begin issuing `SplitRange` writes that emit v2 records. Because M3-PR1 ships the decoder relaxation and the v2 writer is only exercised once a split runs (M3-PR3 wires the scheduler; manual `SplitRange` is operator-initiated), the practical fence is "complete the M3-PR1 rollout cluster-wide before issuing any split." This is stated in the M3-PR1 PR description and the §8.4 acceptance criteria.
- **Rejected alternative — separate catalog key.** Storing `SplitAtHLC` under a sibling key (`{routePrefix}{routeID}/split_at_hlc`) avoids any codec change, but it splits one logical record across two keys (extra read, non-atomic update relative to the descriptor, and a second key to GC on route retirement). The single-record append with a relaxed decoder is simpler and keeps the descriptor atomic, so we take it.

**Codec round-trip test matrix (M3-PR1, §8.1) — both directions explicit.** The relaxation must be locked down by tests written *before* the encoder bump (so the relaxed decoder is proven against a v2 record built by hand, not only against its own encoder):

| # | Encoder | Decoder | Expectation |
|---|---|---|---|
| 1 | v2 (`SplitAtHLC = T ≠ 0`) | v2 (relaxed) | round-trips: decoded `SplitAtHLC == T`, all other fields equal |
| 1a | v2, **`End == nil`**, `SplitAtHLC = T ≠ 0` | v2 (relaxed) | round-trips: decoded `SplitAtHLC == T`, `End == nil`, no error — catches the encoder early-return bug that would skip the tail on `End == nil` routes (rightmost route / split right child) |
| 2 | v2 (`SplitAtHLC = 0`) | v2 (relaxed) | round-trips with `SplitAtHLC == 0` (zero is a legal value, not "absent") |
| 3 | v1 (hand-built, no tail) | v2 (relaxed) | decodes, `SplitAtHLC == 0`, version-keyed read skips the absent tail without error |
| 4 | v2 (hand-built `SplitAtHLC = T`) | **v1-era relaxed decoder** (no `version >= 2` branch, drains tail) | decodes without error; `SplitAtHLC == 0` (tail dropped); other fields equal — the *old-code-reads-new-record* rolling-upgrade case |
| 5 | future v3 (hand-built: v2 layout + extra appended bytes, version byte `3`) | v2 (relaxed) | decodes, `SplitAtHLC == T`, extra trailing bytes drained without error — forward-version tolerance |
| 6 | corrupt (version byte `0`, i.e. `< catalogRouteCodecVersionMin`) | v2 (relaxed) | rejected with `ErrCatalogInvalidRouteRecord` — relaxation does **not** weaken the floor |
| 7 | v2 with a **truncated** tail (`< 8` bytes after `End`) | v2 (relaxed) | rejected — a present-but-short `SplitAtHLC` is corruption, not a forward-version tail |

Cases 4 and 5 are the load-bearing ones for the rolling-upgrade fence; case 7 ensures "skip unknown tail" is not mistaken for "ignore truncation of a field this version requires."

**(d) HLC → physical-ms conversion (do the duration math in ms, never on raw HLC values).** A `RouteDescriptor.SplitAtHLC` is a packed HLC, **not** a millisecond count. Per `kv/hlc.go:31-49`, the 64-bit layout is `physical_ms << 16 | logical_counter` (16 low logical bits, `hlcLogicalBits = 16`, exported as the package const `kv.HLCLogicalBits` at `kv/hlc.go:41`). Subtracting two raw HLC values is therefore **not** a millisecond delta — it advances by `1 << 16 = 65536` per physical millisecond. If the cooldown comparison subtracted raw HLCs and compared against a millisecond duration, a 10-minute (600 000 ms) cooldown would appear to elapse after only `600000 / 65536 ≈ 9.16 ms` of physical time, letting a just-split route be re-split almost immediately after an election — the bug review flagged.

The detector therefore extracts the physical half **before** any arithmetic:

```
// physical Unix-ms component of a packed HLC value
hlcPhysicalMs(v uint64) int64 = int64(v >> kv.HLCLogicalBits)    // == v >> 16

splitCooldownMs := splitCooldown.Milliseconds()                  // time.Duration → ms
nowMs           := time.Now().UnixMilli()                        // wall clock — see "now source" below
elapsedMs       := nowMs - hlcPhysicalMs(SplitAtHLC)
inCooldown      := elapsedMs >= 0 && elapsedMs < splitCooldownMs
```

**Source of `now` (review Issue 3 — committed: wall clock).** `SplitAtHLC` must be converted with `hlcPhysicalMs` because it is a *packed* HLC, but the present time only needs to be a current physical-millisecond count — the comparison is a duration heuristic, **not** an ordering-sensitive read. We use `time.Now().UnixMilli()` directly for `now`. This is permitted by the CLAUDE.md rule, which forbids the wall clock only for "snapshot reads, MVCC visibility checks, OCC validation, lease/expiry decisions, or any other **ordering-sensitive** read" and explicitly allows it for "diagnostics/metrics." Cooldown reconstruction is neither: it decides only *whether enough wall time has elapsed since a split to consider the route eligible again*, a purely advisory throttle whose worst-case error (a slightly early or late re-split) is benign and bounded by the `splitCooldown` window. The candidate alternatives were rejected for this read:
- `HLC.Next()` — advances the clock and churns the logical counter for what is a read; unnecessary, and a write the detector has no reason to make.
- `HLC.NextFenced()` — can return `ErrCeilingExpired`, forcing error handling into a heuristic throttle that should never *fail* a cycle.
- `HLC.Current()` — would also be *correct* (it returns the last issued/observed value without advancing), but it makes cooldown reconstruction implicitly depend on the local HLC having been ticked recently on this node; on a freshly elected leader that has not yet issued a timestamp, `Current()` can read stale (0 or a low value), under-counting elapsed time and over-holding cooldown. The wall clock has no such dependency.

The fence that makes this safe lives on `SplitAtHLC` (a leader-fenced, Raft-committed ts, §7.7b), not on `now`: even with a modestly skewed wall clock the elapsed estimate stays within clock-skew of the true value, which CLAUDE.md already requires ("keep wall clocks reasonably synchronized"). The raw HLC is retained only for the durable `SplitAtHLC` field and for diagnostics; **all** conversions of `SplitAtHLC` go through `hlcPhysicalMs(...)`. (The actual enforcement deadline is then pinned on the leader's monotonic clock per §5.3 — the elapsed-ms delta only decides *whether* a route is still in cooldown at reconstruction time and *how much* monotonic time remains.)

**Scoping.** This is a catalog schema change, so it lands in **M3-PR1** (which already touches the engine/catalog cleanup, §8.1) — the decoder relaxation (first), the encoder v2 bump, the `SplitRange`-handler fenced-ts write, the HLC→ms conversion helper, and the round-trip + cross-version codec tests ship together, ahead of the detector that consumes them (M3-PR2/PR3). The §8.1 table and §8.4 acceptance criteria are updated accordingly.

## 8. Milestone / PR Breakdown, Test Strategy, Open Questions

### 8.1 PR breakdown (each independently shippable)

| PR | Scope | Tests | Independently shippable? |
|---|---|---|---|
| **M3-PR1** | Retire engine `RecordAccess`/`splitRange`/`Route.Load` threshold path (§3.4); confirm keyviz is the sole signal; **add the durable `RouteDescriptor.SplitAtHLC` field** — relax **both** strict decoder checks (version-byte → `>= catalogRouteCodecVersionMin`, trailing-bytes → drain unknown tail; ship first, §7.7c), then bump the encoder to v2 and append the 8-byte tail; update `CloneRouteDescriptor` and `routeDescriptorEqual` to include `SplitAtHLC` (§7.7b, Issue 2); `SplitRange`-handler writes a **fenced** commit ts that mirrors `nextTxnTSAfter` (Observe-then-retry against `snapshot.ReadTS` so `CommitTS > StartTS`, §7.7b) passed as `OperationGroup.CommitTS`; add the `hlcPhysicalMs` conversion helper (§7.7d); add `autosplit_*` metric scaffolding (no detector yet). | `distribution/engine_test.go` updated (dead-code removal); **codec round-trip matrix cases 1-7 (§7.7c)** — v2 round-trip preserves `SplitAtHLC` (incl. `=0`), v1 decodes to `SplitAtHLC=0`, **v2 decodes under the v1-era relaxed decoder dropping the tail (old-reads-new)**, future-v3 trailing bytes drained, version `< min` rejected, truncated tail rejected; the round-trip test includes at least one `End == nil` route (matrix case 1a), asserting `SplitAtHLC` round-trips correctly (guards the encoder early-return-on-`End == nil` path); helper tests — clone preserves `SplitAtHLC` and `routeDescriptorEqual` distinguishes routes differing only in `SplitAtHLC`; fenced-ts test — pre-allocation `<= ReadTS` triggers Observe-and-retry yielding `CommitTS > StartTS` (a single-`NextFenced` value at-or-below `ReadTS` must not be passed through); `hlcPhysicalMs` unit test incl. the `1<<16`-per-ms regression case; metric registration test. | Yes — cleanup + additive schema field with forward/back-compat decode + metric names; no behavior change. Carries the M3-PR1 rollout ordering note (relaxed decoder cluster-wide before any v2-emitting split, §7.7c). |
| **M3-PR2** | Aggregation reader (leader-local consumption of `MemSampler.Snapshot`, §4) + the pure detector: `RouteStateActive`-only candidacy precondition (§5.0), scoring, hysteresis, cooldown, compound single-key isolation (§5.4.2), split-key selection, guardrails (§5), **state-map GC reconciliation against the live catalog each cycle (§7.7a)**. No scheduler wiring — detector emits decisions to a sink that is a no-op/log in this PR. | Table-driven unit tests for candidacy precondition (non-active states never promote)/scoring/hysteresis/cooldown/compound-split cooldown exemption/split-key/guardrails; state-map shrinks after a parent `RouteID` retires (the detector is a pure function, §5). | Yes — detector runs and logs would-be decisions; nothing splits. Safe to ship "observe-only." |
| **M3-PR3** | Scheduler wiring to `SplitRange` (§6, incl. compound single-key isolation §6.4) + `--autoSplit` flag + sampler-wiring condition `keyvizEnabled \|\| autoSplit` and `autoSplitDefaultBuckets` implied bucketing in `main.go` and `cmd/server/demo.go` (§7.1) + runtime kill switch + slog + `SplitAtHLC`-seeded cooldown reconstruction on election (§7.7b). Same-group target only (cross-group target hook present but disabled). | Integration: detector→`SplitRange` end-to-end in the 3-node demo (`cmd/server/demo.go`); kill-switch + leader-change reset + seeded-cooldown-from-`SplitAtHLC` tests. | Yes — completes standalone auto-split. |
| **M3-PR4** (post-M2) | Enable least-loaded `target_group_id` selection on the existing scheduler hook once M2's cross-group `SplitRange` lands (§6.2); exclude in-flight `SplitJob` routes. | Integration: cross-group auto-split against M2 migrator; target-selection unit tests. | Depends on M2; the M3 scheduler interface is unchanged. |
| **M3-PR5** | Lifecycle: rename `*_proposed_*` → `*_partial_*` after M3-PR1, update parent partial doc's M3 row; → `*_implemented_*` after M3-PR3 (standalone) with M3-PR4 tracked as the cross-group follow-on. | — | Doc-only. |

Each PR carries the five-lens self-review and is gated by its tests + `make lint`.

### 8.2 Test strategy

- **Unit (table-driven, co-located `*_test.go`):** scoring math against the **pinned §5.1 formula** (given `(read_ops, write_ops)` column vectors and a `Step`, the expected weighted-ops/min `score` is fully determined, including the `Step`-invariance check across two different `--keyvizStep` values); consecutive-window promotion and reset, **including the `evalInterval < Step` case where extra cycles with no new flush column must not advance `consecutiveOver` (§5.2)**; down-side hysteresis band; cooldown enforcement and expiry (monotonic enforcement deadline) **including the `SplitAtHLC` → physical-ms conversion (§7.7d): a raw-HLC subtraction must not pass where the `hlcPhysicalMs(SplitAtHLC)` form fails (the `1<<16`-per-ms regression guard), with `now` taken from the wall clock per the §7.7d `now`-source decision**; split-key selection across the §5.4 cases — sub-range p50 using the bucket `hi` boundary, **plus the boundary-bucket fallbacks (codex P2): p50 in the last bucket → interior `lo`, p50 in the first bucket → interior `hi`**; single-key skew isolation (two-boundary compound), **plus the `H == route.Start` single-split special case and the already-isolated `[H, H·0x00)` decline (codex P2)**; no-evidence decline; unbounded tail; guardrails (min span, max routes, per-cycle budget). The detector being a pure function (§5) makes these deterministic with no data plane.
- **Property tests (`pgregory.net/rapid`):** feed randomized window sequences and assert invariants — never schedule during cooldown; never exceed `maxSplitsPerCycle`; never emit a `split_key` outside `(route.Start, route.End)`; a route below `0.8×threshold` for any cycle never promotes that cycle.
- **Integration:** detector → `SplitRange` end-to-end in the 3-node demo (`cmd/server/demo.go`) — drive a hot key, assert a same-group split appears in `ListRoutes` and the catalog version bumped; kill-switch flips off mid-load and no further splits occur; default-group leader change resets detector state and re-earns a candidate before re-splitting; seeded cooldown from catalog lineage prevents immediate re-split after election (§7.6).
- **Jepsen:** the hotspot workload (hot-key load + partition/kill nemeses, linearizability during split) is **deferred to M4** per the parent doc §12 (M4: "Jepsen hotspot workloads") and §13.3. M3 does not add a Jepsen suite; M3-PR3's integration test is the end-to-end gate.

### 8.3 Five-lens self-review (to be filled per PR)

| Lens | M3-specific risk to check |
|---|---|
| Data loss | Auto-split issues only `SplitRange` (no direct catalog/store write); same-group split moves no data; cross-group is M2's resumable job. No M3 path can lose a committed write. Confirm M3-PR1's dead-code removal touches no live write path. |
| Concurrency / distributed | Detector goroutine vs. default-group leadership flip (state reset, §7.6); `expected_catalog_version` CAS vs. concurrent manual `SplitRange`; kill-switch toggle race; scheduler vs. in-flight `SplitJob`. Run `go test -race ./kv/... ./distribution/...`. |
| Performance | Zero new hot-path cost (§3.3); detector cost is off-path per-cycle `Snapshot` read; bounded by `maxSplitsPerCycle` RPCs/cycle; metric cardinality fixed-enum only (§7.3). |
| Data consistency | All catalog mutations via `SplitRange` → version bump + watcher fan-out; split key validated inside the live route (incl. the boundary-bucket and `H == route.Start` fallbacks, §5.4); cooldown *enforcement deadline* pinned on the leader's monotonic clock; cooldown *reconstruction* uses the wall clock only as a non-ordering-sensitive elapsed estimate against the fenced `hlcPhysicalMs(SplitAtHLC)` (§7.7d) — permitted by the CLAUDE.md "diagnostics/heuristic" carve-out, not an ordering-sensitive read. Pre-allocated `SplitAtHLC` is fenced and `> StartTS` (Observe-and-retry, §7.7b). |
| Test coverage | Pure-detector table tests + rapid invariants + 3-node integration; leader-change reset covered; Jepsen explicitly deferred to M4 (documented, not skipped silently). |

### 8.4 Acceptance criteria (M3 standalone)

1. With `--autoSplit` on, a sustained hot key drives a same-group split that appears in `ListRoutes` with a bumped catalog version, with the split key at the observed load boundary (not a byte midpoint).
2. With `--autoSplit` off (default), behavior is byte-identical to today; no detector goroutines run.
3. A route below threshold, or above for fewer than `candidateWindows` cycles, never splits.
4. A just-split route does not re-split within `splitCooldown`.
5. The runtime kill switch stops new splits within one evaluation cycle without a restart.
6. A default-group leader change resets detector state and re-earns a candidate before re-splitting; cooldown seeded from `RouteDescriptor.SplitAtHLC` (a leader-fenced commit ts, §7.7b) prevents immediate post-election re-split, with the elapsed check computed as `time.Now().UnixMilli() − hlcPhysicalMs(SplitAtHLC)` (§7.7d) — `SplitAtHLC` is converted with `hlcPhysicalMs` (a raw-HLC subtraction must not let a 10-minute cooldown lapse in ~9 ms), and `now` is the wall clock because cooldown reconstruction is not ordering-sensitive (§7.7d `now`-source decision).
7. All catalog mutations go through `SplitRange` (no direct engine/catalog writes); the engine's `RecordAccess`/`splitRange` path is gone.
8. Routes in `RouteStateWriteFenced`/`RouteStateMigratingSource`/`RouteStateMigratingTarget` are never auto-split (§5.0); only `RouteStateActive` routes are candidates.
9. The leader-local state map is bounded: after a split retires a parent `RouteID`, its entry is evicted within one cycle and `autosplit_tracked_routes` never exceeds the live route count (§7.7a).
10. **Rolling-upgrade safe:** a pre-M3 (v1-era) node, once it has the relaxed decoder, reads a v2 `RouteDescriptor` (with `SplitAtHLC`) without error — **both** the version-byte and trailing-bytes checks are relaxed (§7.7c, Issue 1), so it drains the 8-byte tail and treats the route as never-in-cooldown. The codec matrix cases 4 (old-reads-new) and 5 (forward version) and the §7.7b helper updates (`CloneRouteDescriptor` / `routeDescriptorEqual` preserve `SplitAtHLC`) pass. The M3-PR1 rollout completes cluster-wide before any split emits a v2 record.
11. **Split-key boundary cases (codex P2):** an upper-tail hotspot whose p50 falls in the last sub-bucket splits at the interior `lo` fallback (never drops the decision on a `hi == route.End` boundary, §5.4.1); a single-key hotspot whose hot key is already the route start (`H == route.Start`) isolates `{H}` with the single-split form, not the invalid two-boundary form (§5.4.2).

## 9. Open Questions

Per the design-doc-first workflow this section **converges**: where review (or this revision) produced a recommendation, the OQ is marked **RESOLVED** with the decision inline; only questions that genuinely need an implementer/reviewer call before the relevant PR remain open.

1. **OQ-1 — `Route.Load` / `Stats` fate. (RESOLVED — keep `Stats`, drop `RecordAccess`/`splitRange`/threshold ctor.)** `Engine.Stats()` is a useful read-only debugging surface and costs nothing when unused, so M3-PR1 keeps `Route.Load`/`Stats` but deletes the dead `RecordAccess`/`splitRange`/`NewEngineWithThreshold` write path (§3.4). Full deletion of `Load`/`Stats` is not worth losing the diagnostic. (§3.4)
2. **OQ-2 — Down-side hysteresis shape. (RESOLVED — hold the counter in the band.)** Keep the current proposal: in the `[0.8×threshold, threshold)` band the `consecutiveOver` counter holds (neither increments nor resets), rather than decaying one-per-cycle. Decay creates an implicit max-hold time that is a function of `candidateWindows`, making the threshold↔cooldown interaction harder to reason about; hold is simple, deterministic, and easy to test. (§5.2)
3. **OQ-3 — No-evidence fallback. (OPEN — implementer call in M3-PR2.)** When neither sub-range buckets nor Top-K are enabled, decline to split (current proposal) or allow a midpoint-of-observed-min/max-key split? Declining is safer and is the proposal. This is now only reachable when the operator *explicitly* sets `--keyvizKeyBucketsPerRoute 1` alongside `--autoSplit` (§3.1, §7.1 — `--autoSplit` otherwise implies `autoSplitDefaultBuckets` sub-range buckets), so the "silent no-op by default" trap is already closed; the decline only fires on a deliberate route-granular-only configuration. Left open only because a future operator might want an opt-in `--autoSplitMidpointFallback` escape hatch; not needed for M3. (§5.4.3)
4. **OQ-4 — Minimum route size without per-route key counts. (OPEN — implementer call in M3-PR2.)** The engine has no per-route key count. Approximate `minRouteSpan` from sub-range/Top-K evidence (current proposal, conservative "do not split" when evidence is absent), or add a cheap per-route key-count estimate to the sampler? The proposal (approximate, fail-conservative) is sufficient for M3; the sampler-side estimate is a keyviz follow-up, not an M3 blocker. (§5.5)
5. **OQ-5 — Cross-node scoring for multi-leader deployments. (OPEN — deferred to a future milestone; reframed.)** *Reframed from "heavy follower-forwarded reads" — the real gap is broader (§4.1).* In a multi-group cluster, routes whose shard group leader is a different node than the default-group leader are invisible to the leader-local detector. M3 ships leader-local (full coverage for single-default-group; partial for multi-group, documented as a known limitation in §4.1). Closing it needs cross-node scoring (e.g. reading the keyviz cluster fan-out aggregate behind the same detector interface, §5). Deferred; not an M3 blocker because under-observation is conservative (fewer splits, never an unsafe one). (§4.1)
6. **OQ-6 — Cross-group activation gate. (RESOLVED — gate behind `--autoSplitCrossGroup`.)** Once M2 lands, do **not** auto-enable least-loaded `target_group_id`; gate it behind an explicit `--autoSplitCrossGroup` sub-flag. Cross-group splits trigger M2's `SplitJob` data movement, whose blast radius warrants a separate, explicit opt-in distinct from same-group range splitting. M3 ships the hook disabled; M3-PR4 adds the flag. (§6.2)
7. **OQ-7 — Durable cooldown across elections. (RESOLVED.)** Resolved by adding `RouteDescriptor.SplitAtHLC` — a leader-fenced commit ts (§7.7b) used by the new leader to reconstruct cooldown from a durable timestamp (the elapsed estimate compares `time.Now().UnixMilli()` against `hlcPhysicalMs(SplitAtHLC)`, §7.7d; `now` is the wall clock because reconstruction is a non-ordering-sensitive throttle, robust to bounded clock skew). The remaining sub-question — is a single `SplitAtHLC` (the split that *created* the route) sufficient, or should it refresh on later same-route events? — is **answered: single is enough**, because cooldown is only ever started by a split, and a split always creates fresh child routes each with their own `SplitAtHLC`. (§7.7b)
8. **OQ-8 — Threshold units across `--keyvizStep`. (RESOLVED — pin the ops/min formula.)** Resolved by pinning the normalization formula in §5.1: `score` is a weighted ops/min rate, `score = write_rate×Ww + read_rate×Wr` with `rate = (Σ ops over N columns)/(N×Step_s)×60` and `N = candidateWindows`. This makes the `50_000 ops/min` threshold invariant to `--keyvizStep` (the coupling OQ-8 worried about) and keeps the unit the one operators reason in. The formula is the reference for the §8.2 scoring tests (incl. a `Step`-invariance case). Expressing the threshold in ops-per-`Step` instead is rejected — it would re-introduce the `Step`↔sensitivity coupling. (§5.1)

## 10. Lifecycle

This document begins as `*_proposed_*`. Per CLAUDE.md / `docs/design/README.md`:

- Rename to `*_partial_*` after M3-PR1 lands; track per-PR landing under §8.1 and update the parent partial doc's M3 row.
- Rename to `*_implemented_*` after M3-PR3 ships (standalone auto-split complete), with M3-PR4 (cross-group target selection, depends on M2) tracked as the cross-group follow-on.

Use `git mv` so history follows the rename. The propose date (2026-06-11) and slug stay fixed.
