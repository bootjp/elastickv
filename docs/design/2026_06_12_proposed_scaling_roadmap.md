# Scaling Roadmap — what exists, what is proposed, what is missing

Status: Proposed
Author: bootjp
Date: 2026-06-12

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
   (`main.go:1606-1620`, listener at `:1613`). But a *group whose voter set
   spans more than one node is not deployable from the startup flags*:
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
   renewal **only to the default group** (`kv/sharded_coordinator.go:1914-1953`,
   `group, ok := c.groups[c.defaultGroup]` at `:1915`). A node that leads a
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
  driven by a migrator on the default-group leader. Auto-detection /
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
  proposal is being written in parallel:
  `2026_06_12_proposed_multinode_multigroup_bootstrap.md` (extend
  `--raftGroups` / a per-group members flag to accept a multi-node voter set
  per group, lifting the `len(groups)==1` guard). This roadmap references it
  by name and treats it as the unblocking dependency for (b), (e), and the
  region-balance gap in §3.

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
  learners forward `LinearizableRead` to the leader today. So the read-replica
  attach primitive exists, but the design that consumes it for off-leader
  reads **does not** — **follower-read design is missing**. Requirements to
  sketch (Gap 3): a leader-issued read-timestamp pipeline so a follower/learner
  serves a snapshot read at a ts the leader has vouched for (CLAUDE.md HLC
  rule: never use the local wall clock or a follower-issued ts for MVCC
  visibility / OCC / lease decisions); a staleness bound and a way for the
  reader to know it has applied past that ts; lease invalidation interaction;
  and adapter routing so reads can be steered to a replica. The centralized
  TSO doc §9 Q4 and the learner doc §8 OQ-5 both flag this as the open
  follow-on.

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
  M6–M7) is a larger question: with the per-group fix in place, every node's
  timestamps are self-monotonic, but **global** monotonicity across nodes
  leading different groups is still not guaranteed without a shared oracle
  (TSO doc §6 "Guarantee"). Cross-group transactions (`kv/transaction.go`,
  `kv/txn_codec.go`) that span groups led by different nodes need a single
  ordering source for OCC commit-ts comparability. So: per-group renewal fix
  is in-scope-soon and load-bearing; the dedicated TSO group is only justified
  once (i) multi-node multi-group is real and (ii) cross-group transactions
  are common enough that the batch-allocator amortization pays for the extra
  Raft group. Until both hold, the per-group fix plus the shared-`*HLC`
  property is adequate. The recommendation in §4 sequences the per-group fix
  early and defers the dedicated TSO group behind real cross-group-txn load.

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
  hot path (`kv/sharded_coordinator.go:1795-1824`), with proposed extensions
  for cluster fan-out (`2026_04_27_proposed_keyviz_cluster_fanout.md`),
  subrange sampling (`2026_05_25_proposed_keyviz_subrange_sampling.md`),
  hot-key top-K (`2026_05_28_proposed_keyviz_hot_key_topk.md`), and per-cell
  conflict (implemented). It is the detection signal M3 reuses.
- **admin** — admin dashboard / data browser / purge-queue are implemented
  (`2026_04_24_implemented_admin_dashboard.md`,
  `2026_05_22_implemented_admin_data_browser.md`,
  `2026_05_16_implemented_admin_purge_queue.md`).
- **metrics cardinality** — recurring constraint across proposals (S3
  admission `protocol`/`stage` labels, SQS per-queue counters bounded by a
  top-N sketch, keyviz `MaxTrackedRoutes` coarsening). Any new per-route /
  per-queue / per-peer metric must carry a cardinality bound; this is a
  cross-cutting operational-scaling rule, not a single design.
- **workload isolation** — `2026_04_24_proposed_workload_isolation.md` (heavy
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
**Problem.** No startup wiring produces a Raft group whose voters span more
than one node (`main.go:746` `len(groups) != 1` guard;
`multiraft_runtime.go:246-254` single-member bootstrap). This blocks
write-throughput scaling beyond one node, leader balance (nothing to transfer
to), follower reads (no remote replica to read from), and every cluster-size
dimension. **Rough milestones:** (M1) extend `--raftGroups` / add a per-group
members flag to accept a multi-node voter set; lift the guard. (M2)
integration harness that stands up multi-voter groups across processes. (M3)
Jepsen multi-group multi-node workload. **Depends-on:** nothing (it is the
root unblocker). *A companion proposal,
`2026_06_12_proposed_multinode_multigroup_bootstrap.md`, is being written in
parallel and is the authoritative spec for this gap; this roadmap defers to
it.*

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
snapshot read at a leader-vouched timestamp (CLAUDE.md HLC rule). (M2) lease
invalidation interaction + staleness bound + adapter read routing to replicas.
(M3) Jepsen stale-read bound workload. **Depends-on:** Gap 1 (multi-node
groups to have a remote replica); the learner primitive (already in-tree).

### Gap 4 — Region (range) balance scheduler
**Problem.** Leader balance (PR #953) spreads *leaderships*; nothing spreads
*data* (which node holds which range's replicas). After enough splits, ranges
pile up unevenly across nodes. This is TiKV's separate balance-region
scheduler; PR #953 §2.2(3) explicitly excludes replica/data movement.
**Rough milestones:** (M1) data-balance policy (move a range's replica set from
an over-full node to an under-full one) reusing the M2 migration plane for the
actual move. (M2) compose with leader balance so the two schedulers don't
fight. **Depends-on:** M2 migration plane (PR #945) for the data-movement
mechanism, **and** Gap 1 (multi-node bootstrap) for somewhere to move replicas
to. Edge: region balance ⟂ depends on M2 + multi-node bootstrap.

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

### Gap 6 — Connection / transport scaling (streaming transport revival)
**Problem.** Raft inter-node messages use unary gRPC per message
(`docs/design/2026_04_18_proposed_raft_grpc_streaming_transport.md` §1): each
send pays a full RTT, capping per-peer throughput at ~RTT⁻¹ regardless of
bandwidth. This bites exactly when multi-node multi-group (Gap 1) puts real
inter-node Raft traffic on the wire. **Rough milestones:** revive the existing
streaming-transport proposal (client-streaming `SendStream` per peer, biased
heartbeat select, backward-compat fallback). The blob-fetch RPC in the S3
offload doc (§3.6) already wants to reuse the same chunked-streaming
abstraction, so landing both behind one transport layer is the leverage.
**Depends-on:** nothing hard; value scales with Gap 1.

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
   topology that exposes it exists. Land first so multi-node groups are safe
   on arrival.
2. **Multi-node multi-group bootstrap** (Gap 1 /
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
8. **Region balance scheduler** (Gap 4). After step 4 (migration plane) and
   step 2 (multi-node). Complement to step 3's leader balance.
9. **Range merge** (Gap 5). After step 4 for cross-group merge.
10. **Streaming transport** (Gap 6). Any time after step 2 makes inter-node
    Raft traffic significant; pairs with the S3 blob-fetch RPC.
11. **Dedicated TSO group** (TSO doc M6–M7) — only once cross-group
    transactions across node-spanning groups are common enough to justify it
    (§2(d)).
12. **Auto group lifecycle** (Gap 7) — long-term, after 2/4/8/9.

In-flight PRs map cleanly: **#953** is steps 3 (and its PR0 = step 2's intent),
**#945** is step 4, **#951** is step 5.

---

## 5. Open Questions

1. **Per-group HLC fix vs full TSO ordering.** Is the per-group renewal fix
   (step 1) sufficient for cross-group OCC correctness in the multi-node
   topology, or does the first node-spanning cross-group transaction force the
   dedicated TSO group earlier than step 11? The answer depends on how
   `kv/transaction.go` compares commit timestamps issued by different nodes;
   needs a focused correctness review before step 2 lands.
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
