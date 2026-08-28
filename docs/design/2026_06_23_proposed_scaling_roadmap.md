# Scaling roadmap ownership index

Status: Proposed
Document type: Roadmap ownership index
Author: bootjp
Date: 2026-06-23
Last audited: 2026-08-27 (UTC) against `origin/main` and GitHub pull requests

## 1. Scope

This document owns no runtime behavior. It is the canonical ownership and
dependency index for scaling work. A behavior change belongs to the focused
design document or pull request named below, not to this roadmap.

The audit follows these rules:

1. One focused design owns one mechanism. Its implementation may use a stacked
   pull request series when the change cannot be reviewed safely as one patch.
2. An implemented document and merged implementation are authoritative over a
   proposal or historical roadmap description.
3. An open pull request is in flight, not implemented on `main`.
4. A requirement with neither a focused design nor an implementation pull
   request is explicitly unowned. This roadmap only defines the boundary of the
   missing design; it does not silently become that design.

## 2. Relationship to the 2026-06-12 roadmap

This document supersedes
`2026_06_12_proposed_scaling_roadmap.md` as the roadmap index and sequencing
authority. It does not claim that all mechanisms proposed on 2026-06-12 have
been implemented or moved into focused designs.

The 2026-06-12 document remains historical input. The requirement-by-
requirement disposition in section 4 is authoritative for the subsystem
milestones it enumerates. In particular, "superseded" means ownership moved
here or to a focused owner; it does not mean "implemented".

Section 4 covers the subsystem milestone tables only. The cross-cutting
contracts in
[`2026_06_12_proposed_scaling_roadmap.md`](2026_06_12_proposed_scaling_roadmap.md)
§7 — the shared `SetPhysicalCeiling` / `Observe` invariant, the per-feature
capability gate, and the required observability keys and metric cardinality
budget — are **not** superseded and remain normative for focused designs written
from this index, whether or not the row repeats them.

They are not applied retroactively to already-shipped milestones. One shipped
mechanism deviates deliberately: SST ingest snapshot transfer (§4.3, implemented)
gates rollout with a cluster-wide environment variable set after operators deploy
a receiver-capable binary everywhere, rather than the §7.2 `cap_<feature>_v<n>`
bit and preflight check. There is no SST capability bit in the tree. The env gate
is fail-safe on an invalid value and reversible without touching persisted data,
so this is recorded as a deviation rather than outstanding work; requiring the
bit later is a separate decision.

## 3. Current canonical owners

Status is a snapshot from the audit date. Pull request state must be checked
again before merge or deployment.

| Scaling requirement | Canonical owner | Implementation evidence | Audited status |
|---|---|---|---|
| Multi-node, multi-group bootstrap | `2026_06_14_implemented_multinode_multigroup_bootstrap.md` | PR #1011 merged | Implemented on `main` |
| Learner membership primitive | `2026_04_26_implemented_raft_learner.md` | PR #676 (`51907e6b`) merged the `AddLearner` / `PromoteLearner` engine, admin, protobuf, CLI, and persistence surfaces; PR #1002 merged the later status promotion and cleanup | Implemented on `main`; follower reads are separate |
| Leader balance | `2026_06_11_implemented_leader_balance_scheduler.md` | PR #1012 merged | Implemented on `main`; data placement is separate |
| Hotspot split M1 catalog and same-group split | `2026_02_18_implemented_hotspot_split_milestone1_pr.md` and `2026_02_18_partial_hotspot_shard_split.md` | The durable catalog, engine snapshots, split admin API, watcher, and M1 integration coverage landed as an earlier series; PR #999 (`a28afa25`, "Clean up hotspot split catalog path") is cleanup and status promotion only, not that implementation. The focused owner is the evidence of record — an auditor should start there, not from #999 | Implemented M1; parent design remains partial |
| Hotspot split M2 migration | `2026_06_11_partial_hotspot_split_milestone2_migration.md` | PR #1096 merged the durable `SplitJob` catalog and codec substrate; PRs #1084, #1085, #1088, and #1090 open | The catalog substrate is on `main`; `StartSplitMigration`, the migrator FSM, fencing, cutover, promotion, and cleanup remain in flight, so this is not a complete migration plane |
| Hotspot split M3 automation | `2026_06_11_implemented_hotspot_split_milestone3_automation.md` | PRs #1097 and #1152 merged the detector core and the committed-window reader (`afec0597`, `distribution/autosplit/sampler_reader.go` plus the observe-only detector bridge); PR #1104 (`07c48af2`, "Complete standalone hotspot split automation") merged M3-PR2b Top-K evidence alignment, the leadership watermark, and M3-PR3 scheduler wiring, and renamed the focused owner to `*_implemented_*` | Standalone M3 is implemented on `main`. The focused owner's §8.1 leaves one slice open: M3-PR4 least-loaded `target_group_id` selection, which waits on the M2 migration plane |
| Per-group HLC renewal and default-group allocator bridge | `2026_04_16_partial_centralized_tso.md` | PR #998 merged | Implemented bridge; dedicated TSO remains in flight |
| Dedicated TSO group and durable routing | `2026_04_16_partial_centralized_tso.md` | PRs #1064, #1103, #1108, and #1150 merged the reservation, configuration scaffolding, and ceiling state machine (`0e85c822` added `kv/tso_fsm.go` with snapshot/restore of the physical ceiling); PR #1095 open | Group reservation/scaffolding and the ceiling FSM are on `main`; production group-0 issuance, TSO-leader redirect/routing, and operational exposure remain in flight |
| Shared Pebble block cache | PR #1082 | PR #1082 merged | Implemented on `main`; cache sharing only, not all resource-pool work |
| Raft gRPC streaming transport | `2026_04_18_implemented_raft_grpc_streaming_transport.md` | PR #1006 merged; PR #1048 merged the kill switch | Implemented on `main`; multi-group soak evidence landed with the design's §8 (`cmd/elastickv-raft-stream-soak`, `scripts/run-jepsen-raft-streaming-multigroup-soak.sh`, `docs/evidence/raft_streaming_multigroup_soak.json`) |
| S3 Raft blob offload | `2026_04_25_partial_s3_raft_blob_offload.md` | PRs #1057 and #1063 merged the rollout scaffolding and blob fetch RPC; `77ea547d` merged the local offload decision, PUT/GET path, and peer replication/fetch; #1126 (`5eaaa05d`) merged follower repair and asynchronous backfill | Transport path is on `main`; the focused design names reference counting, GC readiness, and legacy migration as the remaining blockers |
| Live logical backup | `2026_04_29_proposed_logical_backup.md` | PRs #1065 and #1059 merged scan primitives; PR #1056 and PR #1128 are open for the live pin, admin, and producer stack | In flight; distinct from physical SST snapshot offload |

## 4. 2026-06-12 requirement audit

### 4.1 Routing scale-out

| 2026-06-12 milestone | Disposition | Remaining ownership |
|---|---|---|
| M1 versioned catalog delta and streaming watch | Implemented on `main` | `2026_07_18_implemented_route_catalog_delta_watch.md` (PR #1117, `6c7a66e5`) owns the durable delta log, retention fallback, atomic mirror publication, capability negotiation, and stream reconnect semantics |
| M2 indexed route engine and copy-on-write history | Unimplemented and unowned | Write `*_proposed_route_catalog_index.md`; own the primary index, group secondary index, immutable snapshot sharing, memory bound, and migration from the slice representation |
| M3 batched catalog mutation | Unimplemented and unowned | Write `*_proposed_route_catalog_batching.md`; own batch conflict semantics, one-version publication, idempotency, limits, and interaction with the delta watch |

Hotspot split M2 and M3 do not own these catalog-scale mechanisms. They consume
the existing catalog and must not absorb the three designs above.

### 4.2 Multi-region

| 2026-06-12 milestone | Disposition | Remaining ownership |
|---|---|---|
| M1 WAN Raft tuning and region-aware membership | Unimplemented and unowned | Write `*_proposed_wan_raft_membership.md`; own topology, timing bounds, region identity, quorum failure modes, and rollout. No prerequisite inside this subsystem: it makes Raft survive a cross-WAN partition and does not by itself enable cross-region writes |
| M2 region-local HLC | Unimplemented and unowned | Write `*_proposed_regional_timestamp_oracle.md`; reconcile regional issuance with the dedicated TSO invariant before choosing local ceilings or a global oracle. Depends on the hotspot split M2 migration contract (`2026_06_11_partial_hotspot_split_milestone2_migration.md`) being implemented, because the monotone-merge primitive has to exist first; §3 records that plane as still in flight |
| M3 regional catalog mirror | Unimplemented and unowned | Write `*_proposed_regional_catalog_mirror.md`; define freshness and failover contracts. Depends on the region-aware membership from M1, the per-region ceiling from M2, and the catalog delta/watch design (§4.1 M1, implemented) |
| M4 cross-region disaster recovery | Unimplemented and unowned | Write `*_proposed_cross_region_failover.md`; own authority, fencing, data completeness, operator approval, failback, and split-brain prevention. Depends on M2 and M3 |

Multi-node bootstrap is a prerequisite, not an implementation of multi-region
placement or failover.

### 4.3 Storage tier

| 2026-06-12 milestone | Disposition | Remaining ownership |
|---|---|---|
| M1 SST ingest snapshot transfer | Implemented on `main` | `2026_07_19_implemented_pebble_sst_ingest_snapshot_transfer.md` (PR #1130, `910a97e0`) owns checkpoint consistency, file manifest, integrity, transport, ingest, cleanup, and fallback |
| M2 shared block cache | Implemented on `main` | PR #1082 owned only process-wide cache sizing, lifetime, metrics, and tests |
| M2 per-shard Pebble tuning and write admission | Partially addressed operationally, but the proposed per-shard contract is unowned | Write `*_proposed_pebble_resource_governor.md`; own tuning scope, node-wide fairness, stall thresholds, admission errors, and adapter retry semantics |
| M3 sharded retention scheduling | Existing compaction is implemented; jitter, node budget, and hot-key dynamic retention are unimplemented and unowned | Write `*_proposed_sharded_mvcc_retention.md`; preserve the hard retention contract and active timestamp pins |
| M4 physical disaster-recovery snapshot offload | Partially implemented on `main` | `2026_07_19_partial_physical_snapshot_object_offload.md` (PR #1131, `764db2d8`) owns the export/restore substrate and records M0/M1 implemented; that owner still owns the pending leader-only scheduler, retention/GC readiness, and operational validation. Keep it distinct from logical backup and S3 user-payload blob offload |

### 4.4 Coordinator and API gateway

| 2026-06-12 milestone | Disposition | Remaining ownership |
|---|---|---|
| M1 timestamp issuance decoupling | Partially implemented and in flight | `2026_04_16_partial_centralized_tso.md`, PR #998, and the #1064/#1095/#1103/#1108 stack own this work |
| M2 follower and learner reads | Unimplemented and unowned | Write `*_proposed_follower_reads.md`; own leader-vouched read timestamps, apply watermarks, staleness/session contract, invalidation, routing, and Jepsen evidence. Multi-node groups and the learner primitive are already available; this row is not gated on dedicated TSO unless the focused design adds cross-shard or session-global timestamp semantics |
| M3 cross-shard 2PC completion | Partially implemented; no focused end-to-end owner | Write `*_proposed_cross_shard_transaction_completion.md`; start from the existing `ShardedCoordinator` 2PC and Composed-1 guard, then own unsupported router paths, read-only validation, recovery, and adapter coverage. Gate completion on the dedicated timestamp invariant and globally comparable issuance path |
| M4 resolver work delegation | Unimplemented and unowned | Write `*_proposed_lock_resolver_delegation.md`; own snapshot assignment, leader-vouched decisions, duplicate work, failover, admission, and Raft apply boundaries. It also needs a focused per-group HLC tick capability and preflight contract, such as `cap_per_group_hlc_v1`, before resolver work can move away from process-local ticking |
| M5 leader-proxy circuit breaker | Implemented on `main` | `2026_07_19_implemented_leader_proxy_circuit_breaker.md` (PR #1132, `56e36e94`) owns the data-plane breaker in `kv/leader_proxy_breaker.go` plus retry budget, leader-identity reset, half-open behavior, and adapter error mapping |

- **Per-group HLC vs centralized TSO.** The centralized TSO design
  (`docs/design/2026_04_16_implemented_centralized_tso.md`) has shipped M1-M8:
  all-led-group compatibility renewal, the dedicated group-0 FSM, leader-routed
  durable windows, one-way cutover and Phase D, runtime mode reload, and
  operational gates. The shared ordering source is available for cross-node
  cross-group transactions. Deployments that remain in `legacy` still have
  only per-node monotonicity; before enabling multiple coordinator nodes for
  cross-group issuance, they must complete the documented `shadow -> cutover`
  sequence. `LeaderProxy.Commit` / `Internal.Forward` preserve non-zero
  timestamps, and one `startTS` remains shared by every transaction participant.
  Phase D additionally validates a caller-supplied cross-shard `StartTS` at the
  group-0 leader before commit allocation.
The admin package's existing `ErrLeaderUnavailable` mapping was never evidence
for the data-plane breaker; that gap was closed separately by PR #1132, which
added `kv/leader_proxy_breaker.go` and its adapter error mapping.

## 5. Additional gaps introduced by the 2026-06-23 roadmap

| Gap | Disposition | Required owner |
|---|---|---|
| Replica placement | Unimplemented and unowned | `*_proposed_replica_placement.md`; own creation and reshaping of Raft replica sets — which members a group has and where. Listed as its own row because both the region/range balance scheduler and auto group lifecycle name it as a prerequisite and nothing else owns it. Hotspot migration cannot substitute: it moves ownership to an already suitably placed group |
| Region/range balance scheduler | Unimplemented and unowned | `*_proposed_region_balance_scheduler.md`; depend on the replica placement row above, multi-node bootstrap, and the hotspot split M2 migration plane |
| Range merge | Unimplemented and unowned | `*_proposed_range_merge.md`; split same-group and cross-group merge into reviewable milestones. Same-group merge can own local transaction drain/fencing first; cross-group merge additionally waits for the hotspot split M2 migration plane and the Composed-1 cross-group commit guard/drain protocol |
| Streaming transport multi-group soak | Closed | `2026_04_18_implemented_raft_grpc_streaming_transport.md` §8 records the repeatable soak (`cmd/elastickv-raft-stream-soak` plus a fail-closed verifier over `docs/evidence/raft_streaming_multigroup_soak.json`); no protocol semantics changed |
| Auto group lifecycle | Orchestration unimplemented and unowned; the join/replacement substrate is implemented | `2026_07_18_implemented_raft_fresh_learner_join.md` (PR #1118, `6d8ee633`) and `2026_07_18_implemented_fenced_raft_member_replacement.md` (PR #1122, `5d4c5cad`) already own single-group fresh learner joining and resumable fenced same-ID voter replacement, and both explicitly leave automatic decisions and multi-group joining open. Write `*_proposed_auto_group_lifecycle.md` for the automatic creation/orchestration and multi-group extensions only — do not restate the shipped join/replacement mechanics — and depend on replica placement, the hotspot split M2 migration plane, and the range-merge milestones it chooses to automate |

## 6. Dependency order

Every entry below states its own prerequisites, taken from the rows in sections
3 to 5. A numbered step is **not** a barrier for the steps after it: two items
are ordered only where one names the other. Anything whose prerequisites are
already satisfied may start immediately and in parallel with the rest.

The list is written this way because sequencing items by position repeatedly
invented dependencies the rows do not state.

**Ready now — no unmet prerequisites, may proceed in parallel:**

1. Hotspot split M2 migration plane (§3): the open #1084/#1085/#1088/#1090
   stack owns `StartSplitMigration`, the migrator FSM, fencing, cutover,
   promotion, and cleanup. It is a root prerequisite for later cross-group range
   movement, not implemented by this roadmap.
2. Dedicated TSO group-0 issuance, routing, and exposure (§3, §4.4 M1): proceed
   as its own open stack. It gates cross-shard transaction completion but not
   follower reads.
3. Live logical backup pin, admin, and producer stack (§3): scan primitives are
   on `main`; PR #1056 and PR #1128 own the remaining live stack.
4. S3 Raft blob offload follow-ups (§3): reference counting, GC readiness, and
   legacy migration remain in the focused S3 owner.
5. Indexed route engine (§4.1 M2) and batched catalog mutation (§4.1 M3), which
   are **parallel, not sequential**: the predecessor's §3.3 records M2 as
   independent and M3 as depending only on M1's batched apply observation path,
   and PR #1117 merged M1. Catalog delta/watch is therefore no longer in this
   list either.
6. The remaining physical snapshot offload milestones (§4.3 M4: the leader-only
   scheduler, retention/GC readiness, and operational validation) in
   [`2026_07_19_partial_physical_snapshot_object_offload.md`](2026_07_19_partial_physical_snapshot_object_offload.md).
   Their only prerequisite was SST ingest snapshot transfer, which PR #1130
   merged and §4.3 records as implemented.
7. Per-shard Pebble tuning and write admission (§4.3 M2) and sharded retention
   scheduling (§4.3 M3). Independent of
   step 6: the predecessor's §5.3 records M4 as depending on M1 alone, and the
   focused offload owner names no dependency on either design.
8. Same-group range merge (§5), after the focused range-merge design defines
   local drain and fencing. Cross-group merge is separate and gated below.
9. Replica placement (§5). Named as a prerequisite by region balance and by
   auto group lifecycle, and owned by nothing else, so it gates those two.
10. Follower and learner reads (§4.4 M2). Multi-node groups and the learner
    primitive are on `main`; this is not waiting for dedicated TSO unless the
    focused design adds cross-shard or session-global timestamp semantics.
11. WAN Raft tuning and region-aware membership (§4.2 M1). Multi-node bootstrap
    is on `main`, and this row has no unmet prerequisite inside the regional
    subsystem.

**Gated — each names what it waits for:**

12. Hotspot split M3 cross-group targeting (§3), after the hotspot split M2
    migration plane in step 1. Standalone M3 is implemented — the detector core,
    committed-window reader, Top-K evidence alignment, leadership watermark, and
    scheduler wiring are all on `main`, so none of those is work to schedule.
    What remains is M3-PR4 least-loaded `target_group_id` selection, and the
    focused owner's §8.1 records it as post-M2 because it is a scheduler action
    that moves data.
13. Cross-group range merge (§5), after the hotspot split M2 migration plane
    and the Composed-1 cross-group commit guard/drain protocol required by the
    range-merge owner.
14. Region/range balance scheduler (§5), after replica placement, multi-node
    bootstrap, and the hotspot split M2 migration plane.
15. Auto group lifecycle (§5), after replica placement, the hotspot split M2
    migration plane, and the range-merge milestones it chooses to automate. It
    is **not** gated on the regional stack below: its row names only those
    prerequisites.
16. Cross-shard transaction completion (§4.4 M3), after the dedicated timestamp
    invariant/group-0 issuance and the read-only validation, recovery, and
    router owner.
17. Lock-resolver delegation (§4.4 M4), after a focused design/implementation
    provides per-group HLC tick capability/preflight (`cap_per_group_hlc_v1` or
    equivalent) and safe status-resolver routing. The current process-local
    ticker is not enough.
18. Region-local HLC (§4.2 M2), after the hotspot split M2 migration plane and
    the dedicated/global timestamp decision.
19. Regional catalog mirror (§4.2 M3), after regional M1, regional M2, and
    catalog delta/watch.
20. Cross-region disaster recovery (§4.2 M4), after regional M2 and M3.

## 7. Completion rule

This roadmap can be promoted from `proposed` only when every row is either:

- implemented on `main` with its focused owner promoted to `implemented`, or
- implemented on `main` where the row's canonical owner is a merged pull
  request rather than a design document, evidenced by that merge commit, or
- deliberately rejected with a recorded rationale in its focused owner.

The second clause exists because a pull request has no lifecycle status to
promote. The shared Pebble block cache row names PR #1082 as its only canonical
owner and treats that work as complete, so under the first clause alone this
roadmap could never become eligible for promotion no matter what else shipped.

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
2. **Multi-node multi-group bootstrap** (Gap 1, implemented in
   `2026_06_14_implemented_multinode_multigroup_bootstrap.md`). The root
   topology unblocker for (b), (c), (e), Gap 3, Gap 4 is now in-tree; downstream
   work can build on groups whose voters span more than one node.
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
11. **Dedicated TSO group** (TSO doc M6–M8 / OQ-1 resolved) — implemented as
    the shared ordering source for cross-node, cross-group transactions. The
    remaining sequencing constraint is operational: deployments still running
    in `legacy` retain only the per-node HLC guarantee from step 1, while
    multi-node cross-group issuance requires completing the documented
    `shadow -> cutover -> phase-d` gate so both `startTS` and `commitTS` come
    from group 0 and caller-supplied timestamps are validated against its
    durable allocation floor.
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

1. **Centralized TSO ordering status.** OQ-1 is resolved for the timestamp
   oracle itself: the dedicated TSO group is now the shared source for
   cross-node, cross-group `startTS` / `commitTS` once a deployment completes
   the `shadow -> cutover -> phase-d` sequence. Step 1 remains necessary as the
   legacy compatibility floor, but it is no longer the answer for multi-node
   cross-group issuance. In `legacy`, timestamps are still drawn from each
   coordinator node's local HLC and the old per-node limitation applies. After
   cutover, coordinator-owned timestamps route through group 0, follower
   timestamp requests redirect to the TSO leader, Phase D validates
   caller-supplied cross-shard timestamps against the durable allocation floor,
   and pre-D applied read watermarks use bounded vouchers at dispatch.

   A separate guardrail remains outside OQ-1: cross-group txns with read-only
   participant shards still rely on `validateReadOnlyShards`, whose
   linearizable-barrier-to-`LatestCommitTS` check is not made stronger merely by
   moving timestamp issuance to group 0. Enabling additional multi-node
   read-only-shard shapes must still either reject those shapes or land the
   dedicated read-validation phase described above.
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
An *open* pull request, a code primitive, or a superseded roadmap paragraph is
still not sufficient evidence of completion.
