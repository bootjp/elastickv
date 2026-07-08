# Scaling roadmap — multi-region, route scale-out, storage tier, coordinator

**Status:** Proposed (no implementation yet).
**Author:** bootjp
**Date:** 2026-06-12

> This roadmap captures the scaling work elastickv needs across four
> subsystems that today have known ceilings and have not yet been
> designed past those ceilings. The roadmap itself is not a single
> milestone — it sequences the work and points each subsystem at a
> sibling `*_proposed_*.md` design that will land later as its own PR.
> Per CLAUDE.md the design-doc-first workflow applies: each milestone
> below ships its own `*_proposed_*` doc before its implementation
> starts; this file is the shared north star and the sequencing
> constraint between them.

## 1. Motivation

elastickv is currently sized for **a single DC, low-thousands of
shards per cluster, single-digit-TB per shard, and leader-bound serve
paths**. Each of those four limits is reachable from the existing
production envelope within one growth cycle:

| Dimension | Today's comfortable ceiling | Where the next operator wants to go | Triggering signal |
| --- | --- | --- | --- |
| Routes (shards) per cluster | ~10 k (binary search + 100 ms watcher + history ring of 32 versions) | 100 k–1 M routes | hotspot-split-M3 automation (`docs/design/2026_06_11_partial_hotspot_split_milestone3_automation.md`) generates routes faster than the catalog watcher can fan out at scale |
| Region/DC fan-out | Single DC, LAN-tuned etcd/raft (10 ms tick, 100 ms heartbeat, 1 s election), single default-group HLC ceiling | 2–3 regions with active-active per-shard | Disaster-recovery SLO ("survive a region outage with < 30 s data-plane reconvergence") |
| Bytes per shard | ~1 TB before snapshot transfer / WAL replay becomes operational pain | 5–10 TB per shard, with shard counts in the 100 k range | DynamoDB-compatibility customers ingesting at multi-GiB/s per table |
| Per-node QPS | Leader-bound write, lease-read scales across shards but only on each shard's leader | 5–10x current per-node throughput; sustained reads from non-leader replicas | Redis-compatibility migration projects ingesting from upstream Redis where the upstream serves reads from replicas |

The four subsystems are coupled: route scale-out without multi-region
is a 1 M-route single-DC cluster (operationally fragile); multi-region
without route scale-out is a small cluster spread across regions
(wastes the cross-WAN budget); both without storage-tier work tip
over on the snapshot transfer path; and all three without
coordinator scale-out concentrate every read on each shard's leader.
The sequencing in §6 picks a path through them that buys SLO wins
incrementally without leaving half-shipped state.

## 2. Current state (per the survey of 2026-06-12)

### 2.1 Routing / shard scale-out

`distribution/engine.go` holds an in-memory `[]Route` sorted by
`Start`; `GetRoute` is a `sort.Search` binary lookup (O(log N)).
Snapshot apply is a full slice rebuild under a single write lock,
plus a deep clone into a FIFO history ring of
`DefaultRouteHistoryDepth = 32` versions used by the Composed-1
cross-version-read gate. `distribution/watcher.go` polls
`CatalogStore.versionAt` every `defaultCatalogWatcherInterval =
100 ms` per node — no push, no delta. On version bump every node
pulls the full catalog via `routesAt`, which scans MVCC rows in
pages of `catalogScanPageSize = 256`.

`kv/shard_router.go` consults an optional `PartitionResolver` (read
on the hot path with no synchronization — fixed-at-startup
contract), then `engine.GetRoute`. `groups map[uint64]*routerGroup`
lookup is under `sync.RWMutex`. Hot-range handling is
manual same-group `SplitRange` today; the earlier
`Engine.RecordAccess` + ephemeral midpoint split path was removed by
hotspot M3-PR1a because it was never wired into request paths.

Breakage points:

- **100 k routes**: full slice clone + 32-version history ring is
  ~3.2 M `Route` structs resident per node; catalog watcher pull is
  ~400 paged `ScanAt` calls every version bump.
- **1 M routes**: `ApplySnapshot` is held under `Engine.mu` write
  lock for the entire rebuild — every `GetRoute` on the hot path
  blocks for the duration. History ring 32 × 1 M Routes ≈ 32 M
  structs in RAM per node. `GetIntersectingRoutes` is O(N) linear
  scan even for narrow ranges.

### 2.2 Multi-region

**There is no cross-DC mechanism.** No region/DC/WAN/gossip
references anywhere in `kv/`, `distribution/`, or
`internal/raftengine/etcd/`. Everything is single-region etcd/raft.

HLC is `kv/hlc.go` + `kv/coordinator.go`: physical ceiling is
Raft-agreed via `ProposeHLCLease`, `hlcPhysicalWindowMs = 3 s`,
renewed every `hlcRenewalInterval = 1 s` from the **default-group
leader only** (`ShardedCoordinator.RunHLCLeaseRenewal`). `NextFenced`
fails closed with `ErrCeilingExpired` once `wall_now >= ceiling`.
Wall clock is `time.Now().UnixMilli()` — assumes NTP-synced hosts.

Raft engine (`internal/raftengine/etcd/engine.go`):
`defaultTickInterval = 10 ms`, `defaultHeartbeatTick = 10` (100 ms),
`defaultElectionTick = 100` (1 s), `leaseSafetyMargin = 300 ms`.
LAN-tuned; would spuriously election-time-out cross-WAN.

Multi-region blockers:

- HLC ceiling is **single-point** — default-group leader proposes
  every 1 s; cross-region default-group leadership means every
  shard's persistence-grade ts depends on a cross-WAN propose. 1 s
  WAN RTT + 1 s renewal interval ≈ no margin against the 3 s window.
- 100 ms heartbeat / 1 s election timeout cannot run cross-DC.
- No TrueTime/clockbound integration.
- Cross-shard txns are blocked (`ErrCrossShardTransactionNotSupported`),
  so any region partitioning has to be by shard placement.

### 2.3 Storage tier (Pebble)

`store/lsm_store.go`: per-shard `pebble.Open`, default block cache
`defaultPebbleCacheBytes = 256 MiB` **per store** (not shared).
WAL sync via `ELASTICKV_FSM_SYNC_MODE` (default `pebble.Sync` on
FSM apply; `nosync` opt-in).

`store/mvcc_store.go`: encoded as `UserKey ++ 0x00 ++ inverted_TS`,
`maxSnapshotVersionCount = 1 M`, `maxSnapshotValueSize = 256 MiB`.

`store/snapshot_pebble.go`: snapshot serialization iterates **every
live KV** via `snap.NewIter(nil)`; single-pass, length-prefixed.
`snapshotBatchByteLimit = 8 MiB` on the restore side. No chunking,
no parallelism, no SST-ingest.

`kv/compactor.go`: 5 min tick, 30 min retention window, advances
`MinRetainedTS`, calls `Store.Compact`. Skips when Candidate or FSM
behind.

Storage breakage at 1–10 TB/shard:

- Snapshot transfer is a single-stream full-iter scan; at 1 TB it
  is hours, pins SSTs (blocks compaction reclamation), and any raft
  snapshot transfer serializes through it.
- WAL replay between `defaultSnapshotEvery = 10 000` raft entries
  can be multi-GiB; with `pebble.Sync` durability path the replay
  is tens of minutes.
- Pebble L0CompactionThreshold / LBaseMaxBytes / compaction
  concurrency are defaults; write-heavy shards hit stall thresholds.
  256 MiB block cache per shard × N shards/node = N × 256 MiB
  resident memory (shared-cache TODO not landed).
- MVCC retention 30 min + per-key 1 M version cap means a key
  written ≥ 555/s for 30 min trips the cap; compactor runs every
  5 min so read tail latency spikes during accumulation.

### 2.4 Coordinator / API gateway

`kv/sharded_coordinator.go`: `Dispatch` is the choke point. Every
txn re-allocates `StartTS` via `Clock().NextFenced()`. `IsLeader`,
`VerifyLeader`, `LinearizableRead`, and `LeaseRead` (non-keyed) all
pin to `c.groups[c.defaultGroup]`.

HLC ceiling renewal is leader-only on default group → **every
persistence-grade ts allocation across all shards depends on
default-group default leader staying healthy**. A 3 s outage on
default-group default leader stops cluster-wide ts issuance after
the ceiling window expires.

`kv/lease_state.go`: per-shard lease, monotonic-raw nanos. Fast
path single atomic load; extend mutex-serialized once per Dispatch.
Reads scale horizontally across shards once each shard's lease is
warm — **but only on each shard's leader**; followers serve
nothing.

`kv/coordinator.go` `LeaderProxy`: forwards to remote leader,
`redirectForwardTimeout = 5 s`, `dispatchLeaderRetryBudget = 5 s`
(retry every 25 ms = 200 retries on election storms).

`kv/lock_resolver.go`: every 10 s every leader scans every group it
leads, batch of 100. O(num_groups × 100) per cycle, not partitioned
across nodes.

Coordinator breakage:

- Default-group leader is the **HLC ceiling single point of
  failure**. Multi-region only makes this worse.
- No follower-read path; per-shard read ceiling = each shard's
  leader CPU.
- Cross-shard txn is unsupported — adapters needing multi-key
  atomicity are single-shard-bound, forcing the operator to pack
  related keys into one shard (which limits route-scale-out's
  re-balancing freedom).
- `LeaderProxy`'s 200-retry budget on election storms is a
  thundering-herd amplifier.
- Lock resolver is unpartitioned — every node leading any group
  pays an O(groups it leads × 100) Pebble scan storm every 10 s.

## 3. Subsystem 1 — Routing scale-out (target: 1 M routes)

### 3.1 Target SLO

- Catalog watcher convergence at 1 M routes: < 1 s p95 from
  catalog-write to all-nodes-observe-version.
- `GetRoute` p99 ≤ 5 µs at 1 M routes.
- Memory ≤ 1 GiB per node for the route cache (history ring
  included).
- Catalog write proposal apply ≤ 100 ms p99 (small split / move).

### 3.2 Design — milestones

**M1 — Versioned diff fan-out (`*_proposed_*` doc TBD).**
Replace the watcher's "version bump → pull full catalog" with a
**versioned diff protocol** off the existing
`distribution/catalog.go` storage:

- Add an explicit catalog change log to `distribution/catalog.go`:
  each `CatalogStore.Save` writes the global version bump, changed
  route rows, and `catalog_change/<version>/<route_id>` records in
  the same MVCC commit. `RouteDelta = {op: ADD|MODIFY|REMOVE,
  RouteDescriptor}` is derived from this log, not by diffing full
  route snapshots. `GetCatalogChanges(from_version, to_version)`
  scans only the retained change-log range.
- Watcher pulls deltas into a copy-on-write route-version overlay,
  validates the batch against the previous catalog version, then
  publishes the new catalog version atomically. Readers never observe
  a partially-applied delta set; the Composed-1 cross-version-read
  gate and the M2 history overlay always see a point-in-time
  `RouteHistorySnapshot`.
- Falls back to full snapshot pull when `from_version` is older
  than the catalog's retention horizon (analogous to Raft snapshot
  fallback when log is truncated).
- Push transport via a streaming gRPC `WatchCatalog` RPC on
  `proto.Distribution` so the 100 ms poll becomes "best-effort
  near-real-time"; the 100 ms poll remains as a fallback to bound
  staleness if the stream drops.

Trade-off: streaming watch adds a long-lived RPC per node ↔
default-group leader; default-group leader CPU goes up by the
number of nodes. Multi-region (§4) wants a region-local catalog
mirror anyway, so M1 should also ship a per-node mirror that
serves `GetRoute`/`GetIntersectingRoutes` from the mirror.

**M2 — Indexed `RouteEngine` (`*_proposed_*` doc TBD).**
- Replace `[]Route` slice with a **B-tree keyed on `Start`** for
  insert/delete in O(log N) without slice shift; lookups still
  O(log N) but constant-factor-lower at large N.
- Add a **per-`group_id` secondary index** so
  `GetIntersectingRoutes` over a narrow range and "routes I lead"
  queries are O(log N + result) instead of O(N).
- Trim the history ring: 32 versions × 1 M routes is 32 M structs.
  Switch to **copy-on-write per-version overlay** so unchanged
  routes are shared across versions; resident overhead falls to
  ~(routes changed in last 32 versions) instead of full ring × N.

**M3 — Catalog write throughput (`*_proposed_*` doc TBD).**
Split / move proposals today serialize on the default-group Raft.
At 1 M routes the M3 hotspot-split automation can propose hundreds
of splits per minute; batched proposals are needed:

- `BatchSplitRange(splits []SplitRangeRequest)` apply: one Raft
  entry carries N splits with a single catalog version bump. Idempotent
  at the entry level (an apply that finds the catalog already at the
  resulting version is a no-op).
- Watcher streaming RPC (M1) lets every node observe one batched
  bump instead of N individual ones.

### 3.3 Dependencies

- M1 depends on streaming RPC infrastructure (already used by Raft
  message transport; reuse the same connection pool).
- M2 is independent of M1 but benefits from M1's reduced
  apply-lock duration to make the B-tree's incremental insert path
  worthwhile (full rebuild dominates today).
- M3 depends on M1's batched apply observation path.

## 4. Subsystem 2 — Multi-region (target: 2–3 region active-active)

### 4.1 Target SLO

- Region outage: data plane reconverges in < 30 s for the
  surviving regions.
- Cross-region write latency: bounded by cross-WAN RTT for shards
  whose Raft membership spans regions; shards pinned to a single
  region pay no cross-WAN cost.
- HLC ceiling renewal: ≥ 99.99% availability cluster-wide even
  with 1 region partitioned.

### 4.2 Design — milestones

**M1 — WAN-safe Raft tuning + region-aware membership
(`*_proposed_*` doc TBD).**
- Per-cluster config knob `--raftWANMode`: when on,
  `defaultHeartbeatTick = 100` (1 s heartbeat) and
  `defaultElectionTick = 1000` (10 s election), preserving the
  2-tick-to-quorum safety margin. Per-shard override so a
  LAN-resident shard does not pay the WAN tax.
- `RouteDescriptor` extended with `repeated string allowed_regions`
  so the route catalog records placement preferences; the existing
  hotspot-split mover honours it.
- `RaftEngine.Configuration` extended with per-voter region; the
  leader-election Raft transport prefers same-region voters for
  quorum reads (existing Raft semantics already permit this via
  `pre-vote`).

**M2 — Region-local HLC mirror, async cross-region reconcile
(`*_proposed_*` doc TBD).**

The hard part. Today's single HLC ceiling is Raft-agreed across the
whole cluster; cross-WAN that is a 1 s RTT cliff. Options surveyed:

- **Option A — Per-region HLC ceiling, monotone merge on
  CUTOVER.** Each region runs its own default-group HLC; reads /
  writes against a route owned by that region use the local
  ceiling. Cross-region route moves (the existing M2 hotspot-split
  migration path) carry the source-region ceiling on the
  `ExportRangeVersionsRequest` and the target region's
  `SetPhysicalCeiling` applies max(local, imported) — the same
  monotone-merge primitive M2 already specifies. This bounds the
  cross-region cost to the migration event, not every write.
- **Option B — Hybrid: TrueTime-style external clock-bound
  service.** Operationally heavy (needs reliable atomic-clock
  reference per DC); rejected as v1.
- **Option C — Stretched single ceiling with relaxed window.**
  Increase `hlcPhysicalWindowMs` to 30 s; renewal still cross-WAN
  but the failure mode is "slow ts allocation," not "no ts
  allocation." Useful as a fallback when option A is partially
  shipped.

V1 = option A. Carries the existing M2 hotspot-split monotone-merge
contract over the region boundary.

**M3 — Per-region catalog mirror + region-local
control-plane (`*_proposed_*` doc TBD).**
- Catalog is replicated to each region as a Raft Learner (read-only
  follower across the WAN); each region's `RouteEngine` consumes
  from the local mirror.
- `SplitRange` / `BatchSplitRange` proposals are accepted by any
  region's coordinator and forwarded to the default-group leader;
  the existing leader-redirect path (`kv/coordinator.go` `LeaderProxy`)
  generalises.
- Read-only RPCs (`ListRoutes`, `GetSplitJob`) terminate at the
  region-local mirror without cross-WAN.

**M4 — Cross-region disaster recovery (`*_proposed_*` doc TBD).**
- Surviving regions' coordinator detects a partitioned region via
  membership timeout, declares its routes "soft-failed", and
  optionally promotes a stand-by route owner in another region
  using the existing M2 migration plan as the data-mover.
- The promotion is operator-gated by default (a Distribution RPC
  flag) — auto-promotion is M5+.

### 4.3 Dependencies

- M1 standalone but doesn't enable cross-region writes — it just
  makes Raft survive cross-WAN partition.
- M2 depends on the M2 hotspot-split migration contract
  (`2026_06_11_proposed_hotspot_split_milestone2_migration.md`)
  being implemented so the monotone-merge primitive exists.
- M3 depends on M1's region-aware membership and M2's per-region
  ceiling.
- M4 depends on M2 and M3.

## 5. Subsystem 3 — Storage tier (target: 5–10 TB per shard)

### 5.1 Target SLO

- Raft snapshot transfer: ≤ 1 hour for a 5 TB shard from leader to
  a recovering follower.
- WAL replay on restart: ≤ 5 min for a healthy node.
- Read tail latency p99: < 50 ms on a hot key during MVCC version
  accumulation.
- Pebble compaction stall: never trigger under sustained 100 MiB/s
  write per shard.

### 5.2 Design — milestones

**M1 — SST ingest snapshot transfer (`*_proposed_*` doc TBD).**
- Replace `pebbleSnapshot.WriteTo`'s full-iter stream with
  **Pebble SST-level snapshot transfer**: leader takes a
  Pebble snapshot, copies the live SSTs (+ memtable flush) as
  file-level chunks, follower uses `db.Ingest` to mount them.
- Native Pebble has the primitives; the wrapper layer is the new
  work.
- Streamed in parallel across the leader's outgoing transport.

**M2 — Shared block cache + per-shard tuning (`*_proposed_*` doc
TBD).**
- Land the existing shared-cache TODO: one `pebble.Cache` per node
  shared across all shards' stores, sized as a per-node config
  fraction of available RAM (default 25%).
- Surface `L0CompactionThreshold`, `LBaseMaxBytes`,
  `MaxConcurrentCompactions` as per-shard config so a write-heavy
  shard can be tuned without touching the cluster default.
- Compaction backpressure: when L0 stalls, the FSM apply path
  flips into "lease-read-only" mode and rejects writes with
  `ErrShardWriteBackpressure` until L0 drains — propagated to the
  adapter as a 503 the operator's retry budget can absorb.

**M3 — MVCC retention sharded (`*_proposed_*` doc TBD).**
- Compactor `kv/compactor.go` ticks every 5 min globally — at
  large per-node shard counts this is a thundering-herd. Switch
  to per-shard ticking with a per-shard random jitter, capped at
  a node-wide concurrency budget.
- Per-key version budget: keep the 30 min retention window as the
  hard correctness contract. The 100 k target is a soft default for
  cold / moderate keys, while hot keys dynamically raise the per-key
  budget to at least `write_rate * retention_window * safety_factor`;
  the compactor prioritises those keys and emits pressure metrics
  instead of dropping versions still inside the retention window.
- Inline tombstone GC for keys with no live history (TTL-expired
  rows) so they do not pile up in L0 between compactor runs —
  reuses the existing TTL helper path.

**M4 — Disaster-recovery snapshot offload (`*_proposed_*` doc
TBD).**
- Periodic per-shard Pebble snapshot uploaded to an S3-compatible
  bucket (the S3 adapter already speaks the protocol).
- Restore is `s3 fetch → pebble.Ingest`; combined with M1's
  SST-ingest path this is the same code path as routine snapshot
  transfer.
- Retention is a per-shard policy; the operator picks a window.

### 5.3 Dependencies

- M1 is independent and the highest-leverage win — unblocks every
  TB-scale operation.
- M2 is independent.
- M3 depends on the compactor's existing
  `MinRetainedTS` plumbing.
- M4 reuses the S3 adapter and depends on M1.

## 6. Subsystem 4 — Coordinator / API gateway (target: 5–10x QPS)

### 6.1 Target SLO

- HLC ceiling renewal availability: ≥ 99.999% cluster-wide,
  surviving any single default-group member.
- Per-node read QPS: 5x current via follower-read path.
- Per-node write QPS: 3x current via cross-shard 2PC unblocking
  re-balanced shard layouts.
- Lock-resolver scan cost per node: bounded by
  `node.groups_led × 100` × `1/spread_factor` where
  `spread_factor` is the number of nodes sharing the resolution
  workload.

### 6.2 Design — milestones

**M1 — Default-group decoupling for HLC ceiling
(`*_proposed_*` doc TBD).**

The hardest single-point-of-failure today. Concrete steps:

- Move HLC ceiling renewal from "default-group leader only" to
  **"every group's leader, per-group ceiling"**. Each shard
  proposes its own ceiling on its own Raft, so a default-group
  partition does not stop other shards.
- Cross-shard ts ordering preserved via the existing monotone-merge
  primitive (§4.2 M2 carries it over the region boundary; M1
  carries it across shards).
- `ShardedCoordinator.RunHLCLeaseRenewal` becomes a central renewal
  scheduler: one timer-wheel / bounded-worker service walks the
  groups led by this node, batches due groups by deadline, and issues
  per-group ceiling proposals without spawning one goroutine per
  group.
- Capability bit `cap_per_group_hlc_v1` gates the wire change
  (same pattern as M2 hotspot-split capability bits).

**M2 — Follower-read path (`*_proposed_*` doc TBD).**
- Followers serve reads at a bounded staleness, with the staleness
  guaranteed by a follower-visible read-index / closed-timestamp
  mechanism rather than the current leader-local lease fast path.
  A follower-read request carries a `max_staleness_ms` from the
  adapter; the follower returns `(value, served_at_ts)`. Staleness
  comparisons operate on decoded HLC physical milliseconds, not on
  the packed `physical_ms << HLCLogicalBits | logical` value:
  reject when
  `physical_ms(served_at_ts) < physical_ms(read_ts) - max_staleness_ms`.
- Per-shard "follower ready" gate: follower must have applied at
  least up to
  `physical_ms(max_applied_ts) ≥ physical_ms(read_ts) - max_staleness_ms`.
- Adapter integration: DynamoDB's eventually-consistent read
  (`ConsistentRead = false`) and Redis replicaof reads route here.
- Multi-region (§4.2 M3) regions have an inherent staleness
  bound — follower-reads compose with region-local catalog mirror
  for a region-local read path.

**M3 — Cross-shard 2PC (`*_proposed_*` doc TBD).**
- The existing `ErrCrossShardTransactionNotSupported` lifts to
  "supported via 2-phase commit with the prepare-locking already
  used by single-shard txns."
- Composed-1's per-shard verify becomes per-shard-prepare; commit
  is a second round-trip; abort path reuses the existing lock
  resolver.
- 2PC opens cross-shard route re-balancing freedom — the M3
  hotspot-split mover no longer has to keep "related keys"
  co-located.

**M4 — Lock resolver partitioned across nodes (`*_proposed_*`
doc TBD).**
- Today every leader scans every group it leads, every 10 s.
- New: lock resolver is **a group-scoped Raft proposal** issued by
  the group's leader on a 10 s tick, but the scan work is
  delegated to a follower (any voter can do the Pebble scan; the
  result is a Raft-replicated batch).
- Net: the leader is no longer the single scan point. Followers scan
  Pebble snapshots, resolve transaction statuses asynchronously
  outside the Raft apply path, and propose only final commit / abort
  decisions as resolved batches. Raft apply never performs remote
  primary-shard lookups.

**M5 — Leader-proxy budget bounded (`*_proposed_*` doc TBD).**
- Replace 200-retry budget with circuit-breaker: after 3 failed
  redirects to the same leader, surface `ErrLeaderUnavailable`
  to the adapter; the adapter's retry/back-off absorbs.
- Election storm thundering-herd avoided.

### 6.3 Dependencies

- M1 depends on `cap_per_group_hlc_v1` capability bit infra
  (already used by hotspot-split M2 `cap_migration_v2`).
- M2 depends on a follower-safe read-index / closed-timestamp
  mechanism; the existing per-shard leader lease is not sufficient
  for follower reads.
- M3 depends on the existing Composed-1 verify path; it is the
  per-shard-prepare → multi-shard-commit generalisation.
- M4 depends on `cap_per_group_hlc_v1`'s per-group Raft tick and an
  async transaction-status resolver that runs outside Raft apply.
- M5 is standalone.

## 7. Cross-cutting concerns

### 7.1 HLC across all subsystems

The HLC ceiling Raft-agreed primitive is the load-bearing
correctness invariant for every subsystem. Each subsystem changes
the ceiling shape:

- §3 M1 (delta watcher) — no HLC interaction.
- §3 M2 (B-tree, history ring overlay) — no HLC interaction.
- §3 M3 (batched splits) — single catalog version bump per batch;
  HLC ceiling unchanged.
- §4 M2 (per-region ceiling) — adds per-region ceiling, monotone
  merge on cross-region migration.
- §5 M1–M4 — no HLC interaction.
- §6 M1 (per-group ceiling) — adds per-shard ceiling, monotone
  merge on cross-shard txn (§6 M3).

Composability invariant: **every monotone-merge happens via the
same `SetPhysicalCeiling` + `Observe` primitive**. The M2
hotspot-split contract (§6.2.1 of
`2026_06_11_proposed_hotspot_split_milestone2_migration.md`) is the
reference implementation; per-region and per-group merges reuse it.

### 7.2 Capability bits

Each milestone above ships with a `cap_<feature>_v<n>` advertised
in the existing distribution heartbeat (same surface as
`cap_migration_v2`). The rollout pattern is:

1. Every node carries the *reader* code before any writer turns
   on (`cap_<feature>_v<n>` bit advertised when reader is present).
2. The control-plane RPC that enables the feature checks "every
   active voter advertises the bit" before accepting.
3. The capability bit goes live only when the full end-to-end
   path can complete — same rule the M2 hotspot-split design
   (`§11.1`) uses.

### 7.3 Observability

Each subsystem ships with `slog` keys aligned to the existing stable
schema (`key`, `commit_ts`, `route_id`, `group_id`, `region`,
`shard_id`). New keys for new subsystems:

- §3: `route_count`, `catalog_version`, `watcher_lag_ms`.
- §4: `region`, `cross_region_rtt_ms`, `partitioned_regions`.
- §5: `pebble_l0_files`, `snapshot_transfer_bytes_total`,
  `compaction_stall_count`.
- §6: `hlc_ceiling_renewal_age_ms` (per group), `follower_read_count`,
  `cross_shard_txn_count`, `lock_resolver_partition_id`.

Metric cardinality budget: `region × shard_id` is fine (single
digits × thousands); `route_id` is not (millions) — `route_id`
appears only in `slog` lines, never in metric labels.

## 8. Sequencing

The four subsystems' milestones partial-order by dependency. The
recommended path:

```text
Phase 1 (data-plane scale, no new region):
  §6 M1 (per-group HLC ceiling)
    → §3 M1 (delta watcher)
      → §5 M1 (SST-ingest snapshot)
        → §5 M2 (shared cache + per-shard tuning)

Phase 2 (route-count scale-out, still single region):
  §3 M2 (B-tree + per-group index)
    → §3 M3 (batched splits)
      → §5 M3 (per-shard compactor)

Phase 3 (multi-region):
  §4 M1 (WAN raft tuning)
    → §4 M2 (per-region HLC ceiling, monotone merge)
      → §4 M3 (per-region catalog mirror)
        → §4 M4 (DR auto-failover)

Phase 4 (read & write scale):
  §6 M2 (follower reads)
    → §6 M3 (cross-shard 2PC)
      → §6 M4 (partitioned lock resolver)
        → §6 M5 (leader-proxy circuit breaker)

Phase 5 (DR):
  §5 M4 (S3 snapshot offload)
```

§6 M1 is first because it removes the default-group single point of
failure that every later milestone otherwise inherits. §3 M1 is
second because it unblocks the route-count growth that §4 (regions)
and §6 (cross-shard txn re-balancing) both want to lean on. The
multi-region phase deliberately follows the single-region scale-out
phase so each milestone exercises the monotone-merge primitive at
shard-boundary first (smaller blast radius), then region-boundary.

Each Phase is one PR-batch; within a Phase, milestones land in the
order shown.

## 9. Test strategy

Per CLAUDE.md, replication / MVCC / OCC / Redis changes need the
corresponding Jepsen workload. The mapping:

| Subsystem | Unit / property tests | Jepsen workload |
| --- | --- | --- |
| §3 (routing) | `distribution/engine_*_test.go` table-driven + `pgregory.net/rapid` for B-tree property tests | Route-shuffle workload (existing `2026_06_02_implemented_composed1_m5_jepsen_route_shuffle.md`) extended to 100 k routes |
| §4 (multi-region) | `internal/raftengine/etcd` WAN-tuning unit tests; `kv/hlc_*_test.go` / `kv/coordinator_*_test.go` property tests for per-region HLC ceilings, monotone-merge invariants, and region-local catalog mirror staleness | New `jepsen/multi-region/` partition-during-write and partition-stale-mirror workloads |
| §5 (storage) | `store/snapshot_pebble_test.go` SST-ingest fixture; `store/lsm_store_compaction_test.go` stall test | Existing Jepsen workloads with 10x data; new "snapshot-transfer-during-write" partition |
| §6 (coordinator) | `kv/sharded_coordinator_*_test.go` per-group HLC; `kv/follower_read_test.go` staleness bound; `kv/cross_shard_txn_*_test.go` 2PC | DynamoDB + Redis workloads extended with cross-shard txns; follower-read consistency workload |

The five-pass self-review (CLAUDE.md "Self-review of code changes")
applies to every milestone's PR. Data-loss, concurrency, performance,
data-consistency, and test-coverage passes recorded in the PR
description.

## 10. Open questions

1. **Per-group HLC ceiling cost.** §6 M1 turns one Raft propose
   per second into N (number of groups). At 1 k groups / node the
   leader's Raft pipeline gains 1 k extra apply / s. Measurement
   required: is the apply pipeline the bottleneck (yes → batch
   per-group ceilings into one apply on the leader, lose
   per-group isolation but recover throughput), or is the
   propose-side serialization the bottleneck (no → leave it).
2. **Per-region monotone merge under partitioned regions.** §4 M2
   monotone-merges ceilings on cross-region migration. While a
   region is partitioned its ceiling cannot be observed — does the
   surviving region freeze migrations across the partition, or
   promote anyway with a "merged later" pending state? Operator
   policy or design-level decision?
3. **Follower-read staleness contract.** §6 M2's
   `max_staleness_ms` semantics: bound by the adapter's read
   contract (DynamoDB `ConsistentRead=false` has no formal SLO),
   or by a server-side gate (refuse reads older than X ms)? V1
   probably server-side gate with operator-configurable
   per-shard ceiling.
4. **Cross-shard txn locking order.** §6 M3 must define a global
   lock acquisition order to prevent deadlock. Per-shard
   lexicographic key order works for single-key 2PC; cross-shard
   needs a (shard_id, key) tuple ordering. Confirm before
   implementation that the existing Composed-1 verify path
   tolerates the per-shard prepare/commit split.
5. **Catalog mirror consistency across regions (§4 M3).** Raft
   Learners across the WAN have arbitrary staleness; reads at the
   region-local mirror can lag the global catalog version. Do
   reads at the mirror require a `min_catalog_version` lower bound
   (rejecting if the mirror is too stale), or do they accept any
   version and rely on the existing Composed-1 retry to detect
   mismatch?
6. **Snapshot offload encryption (§5 M4).** S3 snapshots leave the
   cluster's trust boundary. Per-object KMS encryption is the
   minimum; the existing S3 adapter has the wire path but not the
   key-management policy. Defer to the M4 doc.
7. **Lock-resolver delegation (§6 M4).** Follower scans a Pebble
   snapshot — needs the follower to hold a Pebble snapshot open
   for the scan duration, which competes with compaction. Bound
   the scan to a short window or break into smaller resumable
   batches?
8. **Atomic catalog delta retention (§3 M1/M2).** The watcher can
   publish deltas atomically only while it can still reconstruct the
   requested `from_version → to_version` overlay. Define the delta
   retention horizon and the exact fallback trigger to a full
   snapshot so every node makes the same choice under lag.
9. **Hot-key MVCC retention budget (§5 M3).** The 30 min retention
   window is a hard contract, but dynamic per-key budgets can exceed
   the nominal 100 k target for hot keys. Define the memory budget,
   pressure metric, and operator action before implementing the
   adaptive cap.
10. **Follower-read evidence (§6 M2).** Pick the proof followers use
    to serve stale reads safely: Raft ReadIndex on demand, a
    leader-published closed timestamp, or a hybrid. The choice sets
    the latency floor and the Jepsen workload's failure model.

## 11. What this doc is not

- Not a per-subsystem design — each subsystem milestone above gets
  its own `*_proposed_*` doc when its work is queued up. This
  doc is the sequencing and SLO contract those sibling docs
  honour.
- Not a guarantee of order — Phase 2 and Phase 3 can swap if a
  customer's regional-DR SLO arrives sooner than the
  route-count-growth signal. The dependency arrows in §8 are the
  binding constraint; Phase order is the recommendation.
- Not a rewrite of any existing milestone. M2 hotspot-split (PR
  #945) and M3 hotspot-split-automation (PR #951) ship first;
  this roadmap's §3 M1–M3 layer over those landings.
