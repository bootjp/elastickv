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

This means **auto-split requires keyviz sampling to be active**. `--autoSplit` implies the sampler is constructed and flushing (§7.1). We do *not* require the operator to also pass `--keyvizEnabled`; enabling auto-split turns on the minimum sampling auto-split needs (route-granular counts; sub-range buckets and Top-K stay opt-in via the existing keyviz flags and only sharpen split-key selection — see §5.4).

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

### 4.2 What the detector consumes per route

For each route the local node leads, over the trailing window set:

- `read_ops`, `write_ops` (summed across the trailing N flush columns).
- `read_bytes`, `write_bytes` (for a future bytes-weighted score; not in the v1 score).
- sub-range bucket rows and/or Top-K hot keys for the route, when enabled (split-key selection, §5.4).

These come straight from `MemSampler.Snapshot(from, to)` `[]MatrixColumn` (`keyviz/sampler.go:1463`) filtered to routes whose `RaftGroupID` the local node currently leads (the `MatrixRow.RaftGroupID` field, `keyviz/sampler.go:449`, plus the engine's leader state per group).

## 5. Detector

The detector is a pure function of (windowed per-route load, per-route detector state, config) → (split decisions). Keeping it pure makes §8's table-driven tests trivial and makes the leader-local reset (§7.6) a matter of dropping the state map.

### 5.1 Scoring

Per route, per evaluation cycle:

```
score = write_ops * Ww + read_ops * Wr
```

Defaults from the parent doc §6.2: `Ww = 4`, `Wr = 1`, `threshold = 50_000 ops/min`. The score is computed over a 1-minute equivalent window (the trailing flush columns summed and normalized to the flush `Step`, default 60s, so the threshold's units stay "ops/min" regardless of `--keyvizStep`).

### 5.2 Hysteresis (candidate promotion)

A route is promoted to a **split candidate** only after `score >= threshold` for `candidateWindows` **consecutive** evaluation cycles (parent doc §6.2: "Candidate after 3 consecutive windows over threshold"; default `candidateWindows = 3`). A single cycle below threshold resets the consecutive counter to zero. This is the up-side hysteresis that prevents a one-off burst from triggering a split.

Down-side hysteresis: the detector tracks a per-route `consecutiveOver` counter in its leader-local state map; falling below `threshold * hysteresisDownFactor` (default 0.8) clears it immediately, while scores in the `[0.8×threshold, threshold)` band hold the counter (neither increment nor reset) so a route hovering at the line does not oscillate between "almost candidate" and "reset." (OQ-2 asks whether the band should instead decay the counter.)

### 5.3 Cooldown (per route)

After a split is *scheduled* for a route (the `SplitRange` call is issued), both child routes enter a **cooldown** of `splitCooldown` (parent doc §6.2 default 10 minutes). During cooldown the detector will not promote either child to a candidate, regardless of score. Cooldown is keyed by route lineage (the child `RouteID`s produced by the split / observed via the next catalog snapshot), tracked in the leader-local state map with a wall-clock expiry that is **diagnostic-only** for logging but enforced via a monotonic deadline (`internal/monoclock`) so a wall-clock step cannot shorten or extend a cooldown (CLAUDE.md: no ordering-sensitive use of wall clock).

### 5.4 Split-key selection (not midpoint-only)

Per parent doc §3.1.3 and §6.3, the split key comes from the **observed key distribution**, not a byte midpoint:

1. **Default — sub-range p50.** When `--keyvizKeyBucketsPerRoute > 1`, use the route's sub-range bucket rows to find the bucket boundary nearest the cumulative-load median, and use that boundary key (reconstructed via `MemSampler.SubBucketBoundsFor`, `keyviz/sampler.go:559`) as the split key. This places the boundary where load is actually halved.
2. **Single-key skew — isolate the hot key.** When Top-K is enabled (`--keyvizHotKeysEnabled`) and one key's share of the route's writes is `>= topKeyShare` (parent doc §6.3 default 0.8), split so the hot key is isolated into its own narrow child (split key = hot key, and a follow-on split at hot key's successor so the child contains only that key). This is the single-hot-key hotspot case the whole feature targets.
3. **Fallback — coarse boundary.** When neither sub-range buckets nor Top-K are enabled (route-granular sampling only), the detector has no intra-route distribution. Rather than fall back to a blind midpoint (which the parent doc forbids and which can produce an empty child), the detector **declines to split** and emits a metric/log (`autosplit_skipped_no_split_key`) telling the operator to raise `--keyvizKeyBucketsPerRoute`. OQ-3 asks whether a midpoint-of-observed-min/max-key fallback is acceptable here instead of declining.
4. **Unbounded-tail routes (`End == nil`).** Derive the split key from observed keys (sub-range layout over `[start, MaxUint64]` already supports this, `keyviz/sampler.go:833-848`), never from a synthetic byte.

The selected split key must satisfy `route.Start < split_key < route.End` (or `< +inf`); the detector validates this against the live engine route before emitting a decision and drops the decision (with a metric) if the key no longer falls inside the route (the route may have shifted under a concurrent split).

### 5.5 Guardrails

- **Minimum route size** (`minRouteSpan`, OQ-4): a route whose observed key span (distance between min and max observed key, or sub-range extent) is below a floor is not split — splitting a tiny range yields children too small to relieve load and churns the catalog. Because the engine has no per-route key-count, this is approximated from the sub-range/Top-K evidence; when that evidence is absent the guard is conservatively "do not split" (matches §5.4.3).
- **Max routes** (`maxAutoRoutes`, default e.g. 1024): once the catalog holds `maxAutoRoutes` routes, the detector stops proposing new splits and emits `autosplit_route_cap_reached`. Prevents unbounded catalog growth (and bounds the watcher fan-out and the Composed-1 history-ring pressure noted at `distribution/engine.go:53-58`).
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

## 7. Safety and Operations

### 7.1 Default OFF behind a flag

- `--autoSplit` (bool, default `false`). When false, none of the detector/scheduler goroutines start; behavior is identical to today. When true, the default-group leader runs the detector + scheduler loop, and the keyviz sampler is constructed and flushed if it is not already (`--autoSplit` implies the minimum sampling, §3.1).
- Tuning flags (all with parent-doc-derived defaults): `--autoSplitWriteWeight` (4), `--autoSplitReadWeight` (1), `--autoSplitThreshold` (50000 ops/min), `--autoSplitCandidateWindows` (3), `--autoSplitCooldown` (10m), `--autoSplitMaxRoutes`, `--autoSplitMaxPerCycle` (1), `--autoSplitEvalInterval` (evaluation cycle period, default = `--keyvizStep`).

### 7.2 Runtime kill switch

The detector/scheduler loop checks an `atomic.Bool` enable flag each cycle. An admin RPC / endpoint (reuse the existing admin surface that hosts keyviz/distribution operator calls) toggles it without a restart. Flipping it off cancels in-progress scheduling at the next cycle boundary; it does **not** abort an already-issued cross-group `SplitJob` (that is M2's `AbandonSplitJob`). The kill switch is the operator's "stop making new splits NOW" lever.

### 7.3 Metrics (bounded cardinality)

All metrics are **per-node** with bounded label sets — **no per-route or per-key labels** (route count is operator-unbounded; that would blow up cardinality). Labels are limited to fixed enums.

Counters:
- `autosplit_candidates_promoted_total`
- `autosplit_splits_scheduled_total`
- `autosplit_splits_failed_total{reason}` — `reason` ∈ {`cas_conflict`, `rpc_error`, `no_split_key`, `route_cap`, `budget_exhausted`, `target_unavailable`}.
- `autosplit_skipped_total{reason}` — same fixed reason enum.

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
- **Repeated re-split of the same hotspot** that a single split cannot relieve (genuinely un-splittable single-key hotspot, e.g. one key taking all writes): single-key isolation (§5.4.2) splits the key into its own child once; cooldown + the `minRouteSpan` guard then prevent infinitely re-splitting a child that is already a single key (a one-key range cannot be split further — `split_key` validation has no valid interior key, the detector declines and logs `autosplit_unsplittable_hot_key`). This is the limit of split-based mitigation; truly relieving a single-key hotspot is an application/data-model problem, not a sharding one.

### 7.6 Interaction with leader changes

The detector's hysteresis counters and cooldown deadlines live in a **leader-local in-memory state map** — they are **not** replicated through Raft. On a default-group leadership change:

- The deposed leader's detector goroutine stops (it observes the leader-loss callback, the same mechanism the lease invalidation uses).
- The **new** leader starts its detector with an **empty** state map. Consecutive-over counters reset to zero, so a route must re-accumulate `candidateWindows` cycles before it can be promoted again. This is intentionally conservative: after an election, the new leader re-earns confidence before splitting, avoiding a split based on counters it never observed.
- **Cooldowns are partially lost** across an election (the new leader does not know a route was recently split). To avoid a too-soon re-split, the new leader seeds cooldown from the catalog: any route whose `parent_route_id` lineage (parent doc §5, `RouteDescriptor.parent_route_id`) indicates a recent split, or whose creation is recent per the catalog, starts in cooldown. (OQ-7: is `parent_route_id` + a catalog-recorded split timestamp enough to reconstruct cooldown, or do we need a durable `last_split_at` per route?) In-flight cross-group `SplitJob`s are durable (M2) so the new leader sees them and excludes those routes (§6.3) regardless of detector state.

Keeping detector state leader-local (not Raft-replicated) is a deliberate cost/safety trade: replicating per-cycle counters would add Raft traffic for what is a best-effort heuristic, and the worst case of a lost counter is a *delayed* split (safe), never an *erroneous* one. We state this explicitly so a reviewer does not mistake the non-replication for an oversight.

## 8. Milestone / PR Breakdown, Test Strategy, Open Questions

### 8.1 PR breakdown (each independently shippable)

| PR | Scope | Tests | Independently shippable? |
|---|---|---|---|
| **M3-PR1** | Retire engine `RecordAccess`/`splitRange`/`Route.Load` threshold path (§3.4); confirm keyviz is the sole signal; add `autosplit_*` metric scaffolding (no detector yet). | `distribution/engine_test.go` updated (dead-code removal); metric registration test. | Yes — pure cleanup + metric names; no behavior change. |
| **M3-PR2** | Aggregation reader (leader-local consumption of `MemSampler.Snapshot`, §4) + the pure detector: scoring, hysteresis, cooldown, split-key selection, guardrails (§5). No scheduler wiring — detector emits decisions to a sink that is a no-op/log in this PR. | Table-driven unit tests for scoring/hysteresis/cooldown/split-key/guardrails (the detector is a pure function, §5). | Yes — detector runs and logs would-be decisions; nothing splits. Safe to ship "observe-only." |
| **M3-PR3** | Scheduler wiring to `SplitRange` (§6) + `--autoSplit` flag + runtime kill switch + slog (§7). Same-group target only (cross-group target hook present but disabled). | Integration: detector→`SplitRange` end-to-end in the 3-node demo (`cmd/server/demo.go`); kill-switch + leader-change reset tests. | Yes — completes standalone auto-split. |
| **M3-PR4** (post-M2) | Enable least-loaded `target_group_id` selection on the existing scheduler hook once M2's cross-group `SplitRange` lands (§6.2); exclude in-flight `SplitJob` routes. | Integration: cross-group auto-split against M2 migrator; target-selection unit tests. | Depends on M2; the M3 scheduler interface is unchanged. |
| **M3-PR5** | Lifecycle: rename `*_proposed_*` → `*_partial_*` after M3-PR1, update parent partial doc's M3 row; → `*_implemented_*` after M3-PR3 (standalone) with M3-PR4 tracked as the cross-group follow-on. | — | Doc-only. |

Each PR carries the five-lens self-review and is gated by its tests + `make lint`.

### 8.2 Test strategy

- **Unit (table-driven, co-located `*_test.go`):** scoring math; consecutive-window promotion and reset; down-side hysteresis band; cooldown enforcement and expiry (monotonic deadline, not wall clock); split-key selection across the four cases in §5.4 (sub-range p50, single-key skew isolation, no-evidence decline, unbounded tail); guardrails (min span, max routes, per-cycle budget). The detector being a pure function (§5) makes these deterministic with no data plane.
- **Property tests (`pgregory.net/rapid`):** feed randomized window sequences and assert invariants — never schedule during cooldown; never exceed `maxSplitsPerCycle`; never emit a `split_key` outside `(route.Start, route.End)`; a route below `0.8×threshold` for any cycle never promotes that cycle.
- **Integration:** detector → `SplitRange` end-to-end in the 3-node demo (`cmd/server/demo.go`) — drive a hot key, assert a same-group split appears in `ListRoutes` and the catalog version bumped; kill-switch flips off mid-load and no further splits occur; default-group leader change resets detector state and re-earns a candidate before re-splitting; seeded cooldown from catalog lineage prevents immediate re-split after election (§7.6).
- **Jepsen:** the hotspot workload (hot-key load + partition/kill nemeses, linearizability during split) is **deferred to M4** per the parent doc §12 (M4: "Jepsen hotspot workloads") and §13.3. M3 does not add a Jepsen suite; M3-PR3's integration test is the end-to-end gate.

### 8.3 Five-lens self-review (to be filled per PR)

| Lens | M3-specific risk to check |
|---|---|
| Data loss | Auto-split issues only `SplitRange` (no direct catalog/store write); same-group split moves no data; cross-group is M2's resumable job. No M3 path can lose a committed write. Confirm M3-PR1's dead-code removal touches no live write path. |
| Concurrency / distributed | Detector goroutine vs. default-group leadership flip (state reset, §7.6); `expected_catalog_version` CAS vs. concurrent manual `SplitRange`; kill-switch toggle race; scheduler vs. in-flight `SplitJob`. Run `go test -race ./kv/... ./distribution/...`. |
| Performance | Zero new hot-path cost (§3.3); detector cost is off-path per-cycle `Snapshot` read; bounded by `maxSplitsPerCycle` RPCs/cycle; metric cardinality fixed-enum only (§7.3). |
| Data consistency | All catalog mutations via `SplitRange` → version bump + watcher fan-out; split key validated inside the live route; cooldown via monotonic clock, not wall clock (CLAUDE.md). |
| Test coverage | Pure-detector table tests + rapid invariants + 3-node integration; leader-change reset covered; Jepsen explicitly deferred to M4 (documented, not skipped silently). |

### 8.4 Acceptance criteria (M3 standalone)

1. With `--autoSplit` on, a sustained hot key drives a same-group split that appears in `ListRoutes` with a bumped catalog version, with the split key at the observed load boundary (not a byte midpoint).
2. With `--autoSplit` off (default), behavior is byte-identical to today; no detector goroutines run.
3. A route below threshold, or above for fewer than `candidateWindows` cycles, never splits.
4. A just-split route does not re-split within `splitCooldown`.
5. The runtime kill switch stops new splits within one evaluation cycle without a restart.
6. A default-group leader change resets detector state and re-earns a candidate before re-splitting; seeded cooldown prevents immediate post-election re-split.
7. All catalog mutations go through `SplitRange` (no direct engine/catalog writes); the engine's `RecordAccess`/`splitRange` path is gone.

## 9. Open Questions

1. **OQ-1 — `Route.Load` / `Stats` fate.** Fully delete `Route.Load` and `Engine.Stats`, or keep `Stats` (sans `RecordAccess`) for operator diagnostics? (§3.4)
2. **OQ-2 — Down-side hysteresis shape.** Hold the `consecutiveOver` counter in the `[0.8×threshold, threshold)` band (current proposal), or decay it by one per cycle? (§5.2)
3. **OQ-3 — No-evidence fallback.** When neither sub-range buckets nor Top-K are enabled, decline to split (current proposal) or allow a midpoint-of-observed-min/max-key split? Declining is safer but means auto-split is a no-op unless the operator raises `--keyvizKeyBucketsPerRoute`. (§5.4.3)
4. **OQ-4 — Minimum route size without per-route key counts.** The engine has no per-route key count. Approximate `minRouteSpan` from sub-range/Top-K evidence (current proposal), or add a cheap per-route key-count estimate to the sampler? (§5.5)
5. **OQ-5 — Leader-local vs. cluster-wide scoring.** Leader-local is sufficient because the leader serves the load it would split on. Is there a workload (heavy follower-forwarded reads) where cluster fan-out scoring is needed? If so, slot it behind the detector interface later. (§4.1)
6. **OQ-6 — Cross-group activation gate.** Once M2 lands, enable least-loaded `target_group_id` automatically, or gate behind an explicit `--autoSplitCrossGroup` sub-flag so operators opt into data movement separately from same-group auto-split? (§6.2)
7. **OQ-7 — Durable cooldown across elections.** Is `parent_route_id` lineage + a catalog-recorded split timestamp enough for the new leader to reconstruct cooldown, or do we need a durable `last_split_at` per `RouteDescriptor`? (§7.6)
8. **OQ-8 — Threshold units across `--keyvizStep`.** The `50_000 ops/min` default assumes a normalization to ops/min. Should the threshold be expressed in ops-per-`Step` instead, to avoid a hidden coupling between `--keyvizStep` and detection sensitivity? (§5.1)

## 10. Lifecycle

This document begins as `*_proposed_*`. Per CLAUDE.md / `docs/design/README.md`:

- Rename to `*_partial_*` after M3-PR1 lands; track per-PR landing under §8.1 and update the parent partial doc's M3 row.
- Rename to `*_implemented_*` after M3-PR3 ships (standalone auto-split complete), with M3-PR4 (cross-group target selection, depends on M2) tracked as the cross-group follow-on.

Use `git mv` so history follows the rename. The propose date (2026-06-11) and slug stay fixed.
