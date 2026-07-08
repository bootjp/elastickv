# Scaling Roadmap — what exists, what is proposed, what is missing

Status: Proposed
Author: bootjp
Date: 2026-06-23

This is a roadmap / implementation-plan document (the kind `docs/design/README.md`
explicitly admits under "Concrete implementation plans"). It surveys the
current scaling envelope of elastickv, maps every scaling dimension to the
design that already addresses it, and identifies the gaps where no design
exists yet. Every claim about current behaviour below is anchored to a file
and line on `main` at the propose date; every referenced design doc is one
that actually exists in `docs/design/` (or, for in-flight work, an open PR
branch named explicitly).

Scope: this doc proposes **no implementation**. It proposes a sequence and a
short list of new design docs to write. It does not duplicate the detail of
the docs it references; read those for mechanism.

---

## 1. Current scaling envelope

What bounds a single elastickv deployment today:

1. **Single-process multi-group only.** Multiple Raft groups run in one
   process (`--raftGroups id=addr,…`, `shard_config.go:61-99`); each group
   gets its own engine and its own gRPC listener at `rt.spec.address`
   (`main.go:1606-1620`, listener at `:1613`). But a *multi-group topology whose
   per-group voter sets span nodes is not deployable from the startup flags*:
   `resolveBootstrapServers` rejects `--raftBootstrapMembers` whenever
   `len(groups) != 1` (`main.go:746`, `ErrBootstrapMembersRequireSingleGroup`
   at `:736`), and `buildRuntimeForGroup` passes the same `bootstrapServers`
   (which is `nil` in any multi-group config) to every group with
   `LocalAddress: group.address` (`multiraft_runtime.go:246-254`), so each
   group in a multi-group process bootstraps as a **single-member** cluster.
   The multi-group story today is "one process hosts every group, each group
   has one voter" (the demo / Jepsen M5 model). True multi-node multi-group
   is not wired (see §2(b), §2(e)).

2. **Per-group memory.** Each Raft group owns a private Pebble store, and
   `NewPebbleStore` → `defaultPebbleOptionsWithCache` allocates a *fresh*
   block cache per store (`store/lsm_store.go:273-279`, `pebble.NewCache(pebbleCacheBytes)`
   at `:274`), default 256 MiB (`defaultPebbleCacheBytes` at `:66`). N groups
   on a node means N × 256 MiB of block cache alone, plus N memtables, N WALs,
   N compaction budgets. The TODO at `store/lsm_store.go:117-120` records the
   intended fix (a process-wide shared cache plumbed through `NewPebbleStore`)
   but it is a comment, not a design.

3. **Leader concentration.** Leadership of each group is elected
   independently by etcd/raft; nothing spreads leaderships across nodes. One
   node can end up leading every group while peers idle, carrying all
   leader-only work (write proposals, HLC ceiling renewal, lease reads, OCC
   timestamp issuance, route-catalog proposes). This is the explicit
   motivation for PR #953 (leader balance scheduler).

4. **Range granularity is fixed after a manual split.** M1 of the hotspot
   shard split shipped a durable, versioned route catalog and a same-group
   `SplitRange` RPC (`adapter/distribution_server.go`, `distribution/catalog.go`,
   `distribution/engine.go`, `distribution/watcher.go`). There is no data
   movement (cross-group migration is M2, in flight), no auto-detection (M3,
   in flight), and **no merge** (no design exists). The detection counters in
   `distribution/engine.go` (`RecordAccess`) are dead code — not called from
   any request path (confirmed in the M3 doc §1.3).

5. **Cross-group timestamps via per-group HLC, and not even per-group yet.**
   `ShardedCoordinator.RunHLCLeaseRenewal` proposes the physical-ceiling
   renewal **only to the default group** (`kv/sharded_coordinator.go:1960-1985`,
   `group, ok := c.groups[c.defaultGroup]` at `:1961`). A node that leads a
   non-default group but is not a member of the default group never advances
   its ceiling from that group — exactly the cross-group monotonicity gap the
   centralized-TSO doc §1.1 describes. The doc's "near-term fix" (M1: iterate
   *all* led groups in parallel) is **not yet implemented** on `main`. Today
   this gap is masked because every group is single-node (§1.1): all FSMs in
   the process share one `*HLC` (CLAUDE.md "Timestamp Oracle"), so the default
   group's ceiling covers the whole process. The moment groups span nodes, the
   gap becomes real.

6. **Large values traverse the Raft log.** Every byte of an S3 object travels
   through the Raft log as `s3keys.BlobKey` entries (the S3 raft-blob-offload
   doc §1). WAL and snapshot size scale with object bytes, and follower
   catch-up re-applies every byte on the single-threaded apply loop.

7. **FSM apply is serial per group.** `kvFSM.Apply` runs on the group's
   single apply goroutine — "Raft applies are serial" (`kv/fsm.go:123-124`,
   `Apply` at `:297`). Apply throughput per group is bounded by that one
   goroutine; the only way to add apply parallelism today is to add groups.

---

## 2. Dimension-by-dimension analysis

Each dimension below lists what bounds it and the design (existing, in-flight,
or missing) that addresses it.

### (a) Data volume per node

The amount of data one node can hold is bounded by Pebble capacity and by the
memory each group's private cache/memtable pins.

- **Range split — distribute a range across groups.** Same-group split
  shipped in M1 (`distribution/`). Cross-group migration (the part that
  actually relocates data and reduces per-node volume) is **PR #945**
  (`docs/design/2026_06_11_proposed_hotspot_split_milestone2_migration.md`,
  branch `docs/hotspot-split-m2-proposal`): a resumable `SplitJob` with
  `PLANNED → BACKFILL → FENCE → DELTA_COPY → CUTOVER → CLEANUP → DONE` phases
  driven by a migrator on the default-group leader. M2 is the required
  ownership-migration mechanism, but it reduces per-node bytes only when the
  target group has different replica placement; if every group still shares the
  same local voter set, migration changes ownership but cannot move data off an
  over-full node. Actual per-node volume relief therefore also depends on Gap 1
  plus the replica-placement / region-balance work in Gap 4. Auto-detection /
  scheduling is **PR #951**
  (`docs/design/2026_06_11_proposed_hotspot_split_milestone3_automation.md`,
  branch `design/hotspot-split-m3-automation`), which drives detection off the
  already-wired keyviz sampler rather than the dead `RecordAccess` counters.
- **Range MERGE — missing, no design exists.** The inverse of split. Both the
  parent hotspot doc (§2.2.2) and M2 (§2.2) list merge as an explicit
  non-goal, and no `*_merge_*.md` exists in `docs/design/` (verified). Why it
  is needed: without merge, a workload whose hot range cools, or one that
  over-splits during a transient spike, accumulates permanent route fragments.
  Each fragment costs a route entry, sampler cardinality, and — once a child
  lives in its own group — a full group's per-node memory overhead (§1.2). A
  long-running cluster's route count is monotonically non-decreasing, which is
  not sustainable. Hard parts to sketch: choosing merge candidates (adjacent
  routes, both cold, same or adjacent groups); fencing both sides atomically;
  reconciling the two MVCC histories and the per-key internal keyspace
  (`!txn|…`, list/hash/set/zset meta) without losing a committed write or an
  unresolved prepare; cutover that bumps the catalog version exactly like
  split; and the Composed-1 cross-group commit guard interaction (the same
  apply-time hazard M2 §7.3 closes for split). Merge is strictly harder than
  split because it must *unify* two independent commit-timestamp streams, not
  bisect one.
- **Blob offload — proposed.**
  `docs/design/2026_04_25_proposed_s3_raft_blob_offload.md` keeps large object
  payloads out of the Raft log (chunkref through Raft, chunkblob via a
  peer-to-peer side channel with semi-synchronous quorum replication). This
  bounds WAL/snapshot growth to O(manifest), which is the data-volume lever
  for the S3 surface specifically. Still proposed; the legacy `BlobKey`-on-Raft
  path is what runs today.
- **Shared Pebble cache / resource pools — TODO only, promoted to a design
  item here.** The `store/lsm_store.go:117-120` TODO is the single biggest
  per-node memory tax as group count grows: N independent 256 MiB caches with
  no shared eviction pool. This deserves a real design (see §3, Gap 2), not a
  comment. It blocks high group counts on a single node, which in turn blocks
  the "many small ranges" model that split (a) produces.

### (b) Write throughput

- **Multi-group** is the primary write-scaling lever: independent Raft
  pipelines and independent serial apply loops (§1.7). Already in-tree
  structurally.
- **Leader balance — PR #953**
  (`docs/design/2026_06_11_proposed_leader_balance_scheduler.md`, branch
  `design/leader-balance-scheduler`) spreads group leaderships across nodes so
  write-proposal load is not pinned to one node. Count-based v1 (TiKV
  balance-leader equivalent), embedded in the default-group leader, default
  OFF behind `--leaderBalance`.
- **Multi-node multi-group bootstrap — GAP.** Leader balance is *blocked on a
  topology that does not exist*: PR #953 §1.1a ("PR0") documents that no
  startup wiring produces a group whose voters span more than one node — the
  `len(groups) != 1` guard at `main.go:746` and the single-member bootstrap in
  `multiraft_runtime.go:246-254` mean every group in a multi-group process is
  single-voter, so there is nothing to transfer a leader *to*. Closing this is
  a prerequisite for write-throughput scaling beyond one node, and a companion
  proposal is in review as **PR #955**
  (`docs/design/2026_06_12_proposed_multinode_multigroup_bootstrap.md`, branch
  `design/multinode-multigroup-bootstrap`): extend `--raftGroups` / a per-group
  members flag to accept a multi-node voter set per group, lifting the
  `len(groups)==1` guard. Once PR #955 lands it is the authoritative spec for
  this gap; this roadmap treats it as the unblocking dependency for (b), (e),
  and the region-balance gap in §3.

### (c) Read throughput

- **Lease reads — done.** Leader-served linearizable reads without a Raft
  round-trip per read (`kv/lease_state.go`,
  `docs/design/2026_04_20_implemented_lease_read.md`; the lease-read observer
  is wired in at `main.go:426`). This scales leader read throughput but keeps
  all reads on the leader.
- **Learner reads → follower/learner reads.** The learner *primitive* is
  effectively implemented despite the doc filename still reading
  `2026_04_26_proposed_raft_learner.md`: `AddLearner` / `PromoteLearner` are
  on the engine `Admin` interface (`internal/raftengine/engine.go:233`, `:245`)
  and implemented in the etcd backend (`internal/raftengine/etcd/engine.go:1268`,
  `:1289`, `ConfChangeAddLearnerNode` handling at `:2550`), the raftadmin CLI
  has `add_learner` / `promote_learner` (`cmd/raftadmin/main.go:204-206`), and
  `--raftJoinAsLearner` exists (`main.go:104`). The learner doc itself scopes
  follower-served reads as an explicit non-goal (§2 "Non-goals", §8 OQ-5):
  a `LinearizableRead` against a learner is **not** forwarded by the engine —
  `handleRead` returns `ErrNotLeader` for any non-leader state
  (`internal/raftengine/etcd/engine.go:1583`), and the *caller* must forward to
  the leader (`docs/raft_learner_operations.md`, "Serve linearizable reads from
  the learner"). So the read-replica attach primitive exists, but the design
  that consumes it for off-leader reads **does not** — **follower-read design
  is missing**, and it must supply the forwarding/proxy or replica-read API the
  current primitive deliberately omits. Requirements to
  sketch (Gap 3): a leader-issued read-timestamp pipeline so a follower/learner
  serves a snapshot read at a ts the leader has vouched for (CLAUDE.md HLC
  rule: never use the local wall clock or a follower-issued ts for MVCC
  visibility / OCC / lease decisions); a staleness bound and a way for the
  reader to know it has applied past that ts; lease invalidation interaction;
  and adapter routing so reads can be steered to a replica. The low-level
  packages must stay generic: `internal/raftengine` / `store` should expose an
  optional, nil-safe delegate/interceptor interface for "may serve at this
  leader-vouched read timestamp?" and "has this replica applied past it?",
  with nil preserving today's leader-only behavior. `kv` / `distribution`
  implement the policy above that interface rather than being imported by the
  engine/store packages. The centralized TSO doc §9 Q4 and the learner doc
  §8 OQ-5 both flag this as the open follow-on.

### (d) Cross-group transactions at scale

- **Per-group HLC vs centralized TSO.** Today the ceiling is renewed only on
  the default group (§1.5); the TSO doc
  (`docs/design/2026_04_16_proposed_centralized_tso.md`) proposes first the
  near-term fix (renew on all led groups, in parallel — its M1) and then a
  dedicated TSO Raft group with a batch allocator.
  **Assessment of whether TSO is still needed once groups multiply:** the
  near-term per-group fix (TSO doc §6) is *necessary regardless* — it is the
  minimum correctness fix the moment a node leads a non-default group it is
  member of, and it should land before multi-node multi-group bootstrap (b)
  makes that topology reachable. The *full* dedicated-TSO component (TSO doc
  M6–M7) is a larger component, but the need for a shared ordering source is
  not a throughput/amortization question once cross-node cross-group
  transactions are possible: with the per-group fix in place, every node's
  timestamps are self-monotonic, but **global** monotonicity across coordinator
  nodes is still not guaranteed without a shared oracle (TSO doc §6
  "Guarantee"). Cross-group transactions (`kv/transaction.go`,
  `kv/txn_codec.go`) whose timestamps can be allocated by different ingress /
  coordinator nodes need a single ordering source for OCC commit-ts
  comparability, regardless of where the participating groups' current leaders
  live. `LeaderProxy.Commit` / `Internal.Forward` preserve non-zero timestamps,
  so leader co-location does not prove single-clock allocation. (The shared-snapshot
  invariant — every operation in one txn reading at the *same* `startTS` — is
  already upheld: `nextStartTS` allocates one `startTS` for the whole txn and
  propagates it via `reqs.StartTS` to every participating group; the gap is the
  cross-*coordinator* comparability of the per-txn `commitTS`, not the per-txn
  `startTS`. See OQ-1.) So: per-group renewal fix is in-scope-soon and
  load-bearing for one node leading multiple groups; before enabling any
  cross-group transaction mode in which more than one coordinator node can issue
  `startTS` / `commitTS`, the roadmap must either pull the dedicated TSO group
  forward or land a narrower single-oracle bridge that pins cross-group
  timestamp allocation to one designated leader. Until such txns are enabled,
  the per-group fix plus the shared-`*HLC` property remains adequate.

### (e) Cluster size / membership

- **Learner / conf-change — primitive present.** `AddVoter` / `AddLearner` /
  `PromoteLearner` / `RemoveServer` exist on the engine and the raftadmin CLI
  (see (c) / §1). Single-step V1 conf changes only; joint consensus is
  rejected (`validateConfState`).
- **Multi-node bootstrap — GAP** (same gap as (b)): the membership primitives
  exist for *live* expansion (`AddVoter` after bootstrap), but there is no
  startup wiring to declare a multi-node voter set per group, and no
  integration harness for multi-voter groups across processes (PR #953 §1.1a;
  Jepsen M5 runner records "True distributed multi-group is M6+ work",
  `scripts/run-jepsen-m5-local.sh`).
- **Auto group creation — explicit non-goal today, long-term.** Both the
  hotspot parent doc (§2.2.1) and M2/M3 (§2.2) state automatic Raft group
  creation / membership orchestration is out of scope. The scheduler only
  targets groups already in `--raftGroups`. Noting it as a long-term item:
  elastic scale-out (add a node → automatically create/rebalance groups onto
  it) needs an auto group lifecycle, which depends on multi-node bootstrap (b),
  region balance (§3 Gap 4), and merge (a) all being in place first.

### (f) Operational scaling

- **keyviz** — the per-route load sampler is wired and allocation-free on the
  write hot path (`observeMutation`, `kv/sharded_coordinator.go:1841-1846`),
  with proposed extensions for cluster fan-out
  (`2026_04_27_proposed_keyviz_cluster_fanout.md`),
  subrange sampling (`2026_05_25_implemented_keyviz_subrange_sampling.md`),
  hot-key top-K (`2026_05_28_implemented_keyviz_hot_key_topk.md`), and per-cell
  conflict (implemented). It is the detection signal M3 reuses. Current
  adapter-direct Redis/DynamoDB/S3 reads that hit `MVCCStore.GetAt` bypass this
  coordinator sampler, so read-heavy hotspots remain invisible until read-path
  sampling or an equivalent adapter read observation path is added.
- **admin** — admin dashboard / data browser / purge-queue are implemented
  (`2026_04_24_implemented_admin_dashboard.md`,
  `2026_05_22_implemented_admin_data_browser.md`,
  `2026_05_16_implemented_admin_purge_queue.md`).
- **metrics cardinality** — recurring constraint across proposals (S3
  admission `protocol`/`stage` labels, SQS per-queue counters bounded by a
  top-N sketch, keyviz `MaxTrackedRoutes` coarsening). Any new per-route /
  per-queue / per-peer metric must carry a cardinality bound; this is a
  cross-cutting operational-scaling rule, not a single design.
- **workload isolation** — `2026_04_24_implemented_workload_isolation.md` (heavy
  command worker pool, optional Raft-thread pinning, per-client admission,
  XREAD O(N)→O(new)), S3 PUT admission control
  (`2026_04_25_proposed_s3_admission_control.md`), SQS per-queue throttling
  (`2026_04_26_proposed_sqs_per_queue_throttling.md`). These bound *one
  workload's* impact so it cannot starve the shared runtime / Raft control
  plane; they scale a deployment by making it predictable under adversarial or
  unbalanced load rather than by raising raw capacity.

---

## 3. Gap list → recommended new designs (ranked by leverage)

Each gap is a design doc that should be written (`*_proposed_*.md`). Ranked by
how much further scaling it unlocks. "Depends-on" lists hard prerequisites.

### Gap 1 — Multi-node multi-group bootstrap (highest leverage)
**Problem.** Startup wiring can already bootstrap a *single* Raft group with
multiple voters via `--raftBootstrapMembers`, but it cannot express a
*multi-group* topology where each group has its own multi-node voter set
(`main.go:746` rejects `--raftBootstrapMembers` when `len(groups) != 1`, and
the multi-group path still falls back to single-member group bootstraps). That
multi-group limitation blocks write-throughput scaling beyond one group, leader
balance across groups, follower reads for every group, and every cluster-size
dimension that needs multiple replicated ranges. **Rough milestones:** (M1)
extend `--raftGroups` / add a per-group members flag to accept per-group
multi-node voter sets; lift the guard only for the explicitly modeled
multi-group form. (M2) integration harness that stands up multi-voter groups
across processes. (M3) Jepsen multi-group multi-node workload. **Depends-on:**
none for writing the design; rollout must land the HLC per-group ceiling
renewal fix first, as sequenced in §4, before enabling the topology that
exposes stale ceilings. *A companion proposal is in review as **PR #955**
(`docs/design/2026_06_12_proposed_multinode_multigroup_bootstrap.md`, branch
`design/multinode-multigroup-bootstrap`); once it lands it is the authoritative
spec for this gap and this roadmap defers to it.*

### Gap 2 — Shared Pebble cache / resource pools
**Problem.** Each group's store allocates a private 256 MiB block cache
(`store/lsm_store.go:273-279`); N groups = N × 256 MiB plus N memtables/WALs,
with no shared eviction. This caps how many groups (hence how many ranges) one
node can hold, throttling the "many small ranges" model that split produces.
**Rough milestones:** (M1) process-wide shared `pebble.Cache` plumbed through
`NewPebbleStore` (the existing TODO), with per-node sizing. (M2) shared
memtable / compaction concurrency budget across stores. (M3) per-group
fairness so one hot group cannot evict everyone else's working set.
**Depends-on:** none functionally; pairs naturally with Gap 1 (high group
counts only matter once multi-node groups exist).

### Gap 3 — Follower / learner reads
**Problem.** Reads are leader-only; the learner primitive exists but its
consumer (off-leader reads) has no design. Read throughput cannot scale past
one node's leader capacity. **Rough milestones:** (M1) leader-issued read-ts
pipeline + a replica apply-watermark so a follower/learner can serve a
snapshot read at a leader-vouched timestamp (CLAUDE.md HLC rule), exposed
through optional nil-safe low-level delegate interfaces so raftengine/store
do not import `kv` / `distribution`. (M2) lease invalidation interaction +
staleness bound + adapter read routing to replicas.
(M3) Jepsen stale-read bound workload. **Depends-on:** Gap 1 (multi-node
groups to have a remote replica); the learner primitive (already in-tree).

### Gap 4 — Region (range) balance scheduler
**Problem.** Leader balance (PR #953) spreads *leaderships*; nothing spreads
*data* (which node holds which range's replicas). After enough splits, ranges
pile up unevenly across nodes. This is TiKV's separate balance-region
scheduler; PR #953 §2.2(3) explicitly excludes replica/data movement. The M2
migration plane can move *range ownership* between Raft groups, but it does not
by itself move replicas off an over-full node when the source and target groups
share the same voter set. A real region-balance design therefore needs both a
target-group placement constraint and, when no suitable group already exists, a
Raft membership-change / replica-placement step before or during migration.
**Rough milestones:** (M1) data-balance policy that chooses a target group whose
replica set actually reduces node skew, then reuses the M2 migration plane for
the range-ownership move only when that placement predicate holds. (M2) Raft
membership-change primitive for creating or reshaping groups when no existing
group has the needed placement. (M3) compose with leader balance so the two
schedulers don't fight. **Depends-on:** M2 migration plane (PR #945) for
range-ownership movement, a replica-placement / membership-change design for
per-node data movement, and Gap 1 (multi-node bootstrap) for somewhere to place
replicas. Edge: region balance ⟂ depends on M2 + multi-node bootstrap + replica
placement.

### Gap 5 — Range merge
**Problem.** No way to recombine ranges; route count is monotonically
non-decreasing (§2(a)). Over-split or cooled workloads leak route fragments
and (once children are in their own groups) per-node memory. **Rough
milestones:** (M1) same-group merge of two adjacent cold routes (mirror M1
split: catalog CAS + version bump). (M2) cross-group merge (relocate one
child's data into the other's group, reusing the M2 migration plane in
reverse). (M3) auto-merge in the M3 detector (cold-route hysteresis).
**Depends-on:** M2 migration plane for cross-group merge; the Composed-1 guard
for cutover coherence.

### Gap 6 — Connection / transport scaling (streaming transport soak)
**Problem.** Raft inter-node messages previously used unary gRPC per message
(`docs/design/2026_04_18_implemented_raft_grpc_streaming_transport.md` §1),
which paid a full RTT per send. The implemented `SendStream` transport removes
that bottleneck, but multi-node multi-group (Gap 1) still needs transport soak
coverage under real cross-node traffic. **Rough milestones:** (M1) run transport
soak with multi-node multi-group traffic. (M2) decide whether the optional
biased-select multiplexing worker from the implemented transport doc is needed.
The blob-fetch RPC in the S3 offload doc (§3.6) can reuse the same
chunked-streaming abstraction. **Depends-on:** Gap 1 for realistic traffic;
value scales with Gap 1.

### Gap 7 — Auto group lifecycle (longest-term)
**Problem.** Groups are static (`--raftGroups`). Elastic scale-out (add node →
auto-create/rebalance groups) needs automatic group creation + membership
orchestration, an explicit non-goal everywhere today (§2(e)). **Rough
milestones:** out of near-term scope; sketch only. **Depends-on:** Gaps 1, 4,
5 all in place (you cannot auto-create groups before you can stand up
multi-node groups, move ranges between them, and merge fragments).

---

## 4. Sequencing (dependency-ordered rollout)

The ordering is driven by unblock-edges, not by perceived value in isolation.

1. **HLC per-group ceiling renewal fix** (TSO doc §6 / M1). Smallest correct
   change; closes the cross-group monotonicity gap (§1.5) *before* the
   topology that exposes it exists. Land first so each group remains safe when
   replicas move across nodes, but do not enable cross-group transactions whose
   timestamps can be allocated by more than one coordinator node until step 11
   (or its single-oracle bridge) lands.
2. **Multi-node multi-group bootstrap** (Gap 1 / **PR #955**,
   `2026_06_12_proposed_multinode_multigroup_bootstrap.md`). The root
   unblocker for (b), (c), (e), Gap 3, Gap 4. Nothing else multi-node-shaped
   can land until a group can have voters on more than one node.
3. **Leader balance scheduler** (PR #953). Its PR0 is exactly Gap 1; PR1
   (observe-only) can land against today's single-voter topology, but the
   transfer-issuing PR2–PR3 are blocked on step 2. So: PR #953 PR1 in
   parallel with step 2; PR2–PR3 after.
4. **Hotspot split M2 migration plane** (PR #945). The data-movement
   mechanism every later data-balance/merge step reuses. Independent of the
   multi-node work for its own correctness (it moves ranges between groups
   that already exist), so it can proceed in parallel with steps 2–3, but its
   *value* compounds once groups span nodes.
5. **Hotspot split M3 automation** (PR #951). Drives detection off keyviz;
   delivers same-group auto-split standalone (does not require M2), and picks
   a least-loaded target once M2 lands. After step 4 for the cross-group case.
6. **Shared Pebble cache** (Gap 2). Needed once split + multi-node lets a node
   hold many groups; land before pushing high group counts in production.
7. **Follower / learner reads** (Gap 3). After step 2 (remote replicas exist)
   and the learner primitive (already in-tree).
8. **Region balance scheduler** (Gap 4). After step 4 (migration plane), step 2
   (multi-node), and a replica-placement / membership-change design that can
   reshape groups when existing target groups share the same voter set.
   Complement to step 3's leader balance.
9. **Range merge** (Gap 5). After step 4 for cross-group merge.
10. **Streaming transport** (Gap 6). Any time after step 2 makes inter-node
    Raft traffic significant; pairs with the S3 blob-fetch RPC.
11. **Dedicated TSO group or single-oracle bridge** (TSO doc M6–M7 / OQ-1) —
    shown late only because the roadmap must first create node-spanning groups.
    This placement is not a throughput amortization tradeoff: before enabling
    any cross-group transaction mode whose `startTS` / `commitTS` can be
    allocated by more than one coordinator / ingress node, step 2 must either
    pull the dedicated TSO group forward or land a narrower bridge that
    allocates all cross-group `startTS` and `commitTS` values from one
    designated oracle. The
    per-group fix gives per-node monotonicity only (TSO doc §6 "Guarantee");
    commit-ts comparability across coordinators on different nodes is not
    covered by it (OQ-1), even when forwarding preserves timestamps to leaders
    that happen to be co-located. Treat step 11 as
    "deferred-pending-OQ-1-resolution", not "settled-last".
12. **Auto group lifecycle** (Gap 7) — long-term, after 2/4/8/9.

In-flight PRs map cleanly: **#955** is step 2 (Gap 1 bootstrap proposal),
**#953** is step 3 (and its PR0 = step 2's intent), **#945** is step 4,
**#951** is step 5.

### 4.1 Rolling-upgrade and live-cutover guardrails

This roadmap does not introduce a cluster-version Raft entry as part of the
first sequencing slice; that broader coordination protocol should be its own
design if needed. The near-term mitigation is layered:

1. **Capability-gated admin operations.** `raftadmin` / coordinator admin RPCs
   that enable multi-node bootstrap, leader transfers, follower reads,
   migration/import, write fences, learner admission/promotion, or a cross-group
   timestamp bridge must first observe that every current voter and every target
   learner/voter/server involved advertises the matching capability. Mixed
   binary clusters run in compatibility mode with these features disabled.
2. **Operator-driven in-place expansion.** For clusters that can tolerate a
   controlled maintenance window, upgrade all binaries first, verify capability
   convergence, then expand one group at a time by adding a new replica as a
   learner (`AddLearner`), waiting for its match/apply watermark to catch up,
   and promoting it with the learner-promotion path. Keep the old single-voter
   leader serving until the learner is caught up and promoted; reserve direct
   `AddVoter` for bootstrap/offline fully-caught-up peers, not live in-place
   expansion. Do not permit a flag-only restart to reinterpret an existing
   single-voter group as multi-voter.
3. **Blue/green or bridge/proxy cutover for zero-downtime moves.** Deployments
   that cannot accept the in-place operational window should use a fresh
   multi-node cluster plus a temporary bridge/proxy mode: dual-write or
   write-through to both sides, shadow-read / compare, then flip reads and retire
   the old cluster. This mirrors the existing Redis migration pattern and the
   TSO doc's feature-flagged shadow phase, without forcing every bootstrap PR to
   carry a full migration coordinator.
4. **Deferred complex protocol.** If the capability checks above are too weak for
   a later feature, the fix is not to overload this roadmap; write a separate
   cluster-version / rolling-upgrade design and make that feature depend on it.

---

## 5. Open Questions

1. **Per-group HLC fix vs full TSO ordering.** Is the per-group renewal fix
   (step 1) sufficient for cross-group OCC correctness in the multi-node
   topology, or does the first node-spanning cross-group transaction force the
   dedicated TSO group earlier than step 11?

   **Where the timestamps come from (grounded in code).** A cross-group txn's
   coordinator issues *both* of its timestamps from one `*HLC` — `c.clock` on
   the coordinator's node:
   - `startTS` via `nextStartTS` (`kv/sharded_coordinator.go:1429`):
     `Observe(maxLatestCommitTS(keys))` then `c.clock.NextFenced()`. The
     `maxLatestCommitTS` floor is a per-key read against the store — it pins
     `startTS` above the latest commit on *the keys this txn touches*, nothing
     more.
   - `commitTS` via `resolveTxnCommitTS` → `nextTxnTSAfter`
     (`kv/sharded_coordinator.go:1102`, `:1376`): `c.clock.NextFenced()`,
     re-allocated after `Observe(startTS)` if it did not strictly exceed
     `startTS`. A caller-supplied `commitTS` is fed through `c.clock.Observe`
     to keep the clock monotonic.

   Both calls go through the same `c.clock`. The apply-time OCC/ownership check
   then compares these timestamps against stored `CommitTS` values (the
   Composed-1 guard, `docs/design/2026_05_29_partial_composed1_cross_group_commit_guard.md`
   §4.2(a)/§4.4; FSM `latest > startTS` write-conflict check). OCC
   serializability depends on those `commitTS` values being **mutually
   comparable** across all participating groups, and read-only participant shards
   need the separate validation gate described below.

   **Why the per-group fix is necessary but not sufficient.** Today this is
   safe only because every group shares the *same* process-wide `*HLC`
   (single-node groups, §1.1/§1.5): one clock issues every timestamp, so all
   `commitTS` are trivially comparable. The per-group renewal fix (step 1)
   extends correctness to *one node leading several groups* — but its guarantee
   is explicitly scoped: "all timestamps issued by a single node are strictly
   monotonic … monotonicity across nodes that lead *different* groups is not
   fully guaranteed without a shared TSO" (TSO doc §6 "Guarantee", §1.1). The
   moment step 2 (multi-node bootstrap) makes it possible for two clients to
   coordinate cross-group txns on *different nodes*, two concurrent cross-group
   txns can draw `commitTS` from two different `*HLC` instances whose ceilings
   were advanced independently. This is true even if the participating groups'
   leaders are later co-located, because `ShardedCoordinator.Dispatch` and
   `resolveTxnCommitTS` stamp the requests before `LeaderProxy.Commit` /
   `Internal.Forward` forwards them. The `maxLatestCommitTS`/`Observe` floor
   does not close this: it orders writes only on the specific keys read, not the
   global commit order two unrelated cross-group txns need to be serializable
   against each other.

   A second guardrail is independent of the timestamp oracle. Cross-group txns
   with read-only participant shards currently rely on `validateReadOnlyShards`;
   that path has a documented TOCTOU window between the linearizable barrier and
   the `LatestCommitTS` check. A dedicated TSO group or single-oracle bridge
   makes commit timestamps comparable, but it does not by itself close that
   read-validation gap. The rollout gate must therefore either reject
   read-only-shard txn shapes until a dedicated read-validate FSM phase lands,
   or land that phase before enabling those shapes in a multi-node topology.

   **Conclusion / trigger.** OQ-1 therefore is *not* a "focused correctness
   review deferrable until just before step 2 lands" — the per-group fix cannot
   answer it in the affirmative for the cross-node case. The trigger to pull the
   dedicated TSO group forward from step 11 is concrete: **the first
   cross-group transaction mode whose timestamps may be issued by more than one
   coordinator node**, not merely the first case whose participant leaders sit
   on different nodes. Until then the per-group fix + shared-`*HLC` property
   holds. Open sub-question: whether an interim measure short of the full TSO
   group — e.g. pinning every cross-group txn's timestamp allocation to a single
   designated group's leader (the default group's `c.clock`), so all cross-group
   `startTS` and `commitTS` still come from one clock — can bridge the timestamp
   gap between step 2 and step 11 without the full batch-allocator TSO. That interim option
   must be paired with the read-only-shard validation gate above; it should be
   evaluated as part of the step-2 design (PR #955) rather than left to step 11.
2. **Shared cache (Gap 2) vs per-group isolation (workload isolation doc).** A
   single shared block cache trades isolation for density: one hot group can
   evict a latency-sensitive group's working set. How does Gap 2's per-group
   fairness reconcile with the workload-isolation proposal's CPU-side
   reservations? They are the memory- and CPU-axis siblings and should share a
   resource-accounting vocabulary.
3. **Merge (Gap 5) and unresolved prepares.** Merge must unify two MVCC
   histories *and* two `!txn|…` keyspaces. What is the fence/drain protocol
   that guarantees no in-flight prepare on either side is lost across the
   merge cutover? (M2's split drain is the starting point but split bisects one
   history; merge unifies two.)
4. **Region balance (Gap 4) signal.** Count-based (ranges per node) like
   leader balance v1, or size/load-weighted from keyviz from day one? Leader
   balance chose count-first; data balance may need size from the start
   because a 1 GiB range and a 1 KiB range are not interchangeable.
5. **Follower-read staleness contract (Gap 3).** What bound does the adapter
   surface advertise — bounded-staleness with an explicit lag, or
   read-your-writes only? This determines whether the leader-issued read-ts
   pipeline needs a per-client session token.
6. **Auto group lifecycle (Gap 7) trigger.** What signal creates a new group —
   node join, aggregate range count crossing a threshold, or operator action?
   Premature auto-creation interacts badly with merge (create/merge thrash).
7. **Live cutover / rolling-upgrade strategy for the single-node→multi-node
   transition (Gap 1).** Moving a deployment from "one process hosts every
   group, each single-voter" to genuine multi-node multi-group is a topology
   change, not just a flag flip: a group that bootstrapped single-member must
   add remote replicas as learners, wait for catch-up, and promote them through
   the learner-promotion path (the primitives exist, §2(e)); direct live
   `AddVoter` remains reserved for bootstrap/offline fully caught-up peers as
   in §4.1. The cluster may run mixed binary versions mid-upgrade. What is the
   supported path — operator-driven learner-add/promote expansion of an
   existing single-voter group, blue/green with a dual-write proxy
   (`proxy/`, the existing Redis-migration pattern), or a fresh cluster +
   data migration? §4.1 defines the interim guardrails — capability-gated admin
   operations, in-place expansion only after all binaries advertise support,
   and bridge/proxy cutover for zero-downtime deployments — but PR #955 must
   choose the supported default path and spell out rollback. (Note: the TSO
   doc §7 already specifies a phased dual-write/shadow-read/feature-flag
   cutover for the *timestamp* migration; the bootstrap cutover should mirror
   that structure.)
