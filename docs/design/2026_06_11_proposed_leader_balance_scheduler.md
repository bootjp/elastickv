# Leader Balance Scheduler — spreading Raft-group leaderships across nodes

Status: Proposed
Author: bootjp
Date: 2026-06-11

Sibling: [2026_06_11_proposed_hotspot_split_milestone3_automation.md](2026_06_11_proposed_hotspot_split_milestone3_automation.md) — shares the "scheduler lives on the default-group leader, default OFF behind a flag, runtime kill switch, bounded-cardinality metrics, leader-local state reset on election" conventions. This doc reuses those conventions but does **not** depend on M3 landing.

## 1. Background

### 1.1 The problem

elastickv runs multiple Raft groups in one process (`--raftGroups id=addr,id=addr,…`, `shard_config.go:61-99`; default group is the lowest ID, `shard_config.go:386-397`). Every node is a member of **every** group: `buildShardGroups` iterates the parsed `groups` once per process and constructs a `raftGroupRuntime` per group (`main.go:786-889`), each with its own engine and its own gRPC listener at `rt.spec.address` (`main.go:1606-1620`). Leadership of each group is elected independently by etcd/raft, so there is **no mechanism today that spreads leaderships across nodes**. After a rolling restart, a partition heal, or simply unlucky election timing, one node can end up leading every group while its peers lead none. That node then carries all the leader-only work — write proposals, HLC ceiling renewal, lease reads, OCC timestamp issuance, route-catalog proposes — while the rest of the cluster sits idle. The user's explicit goal: "TiKVのようにリーダー以外のノードに別のRaftグループのリーダーをおいて負荷を均等化したい" — put other groups' leaders on the non-leader nodes so the load is even.

### 1.2 TiKV / PD comparison

In TiKV the Placement Driver (PD) runs a **balance-leader scheduler**: it periodically inspects how many region leaders each store holds and issues `TransferLeader` operators to move leaderships off the most-loaded store onto the least-loaded one, with operator limits and store-score hysteresis to avoid thrashing. The v1 objective is count-based (leader count per store), with later load/size weighting layered on. elastickv has no PD — there is no external control plane process. We therefore **embed the equivalent of PD's balance-leader scheduler inside the default-group leader** (the same seat that already hosts HLC ceiling renewal and the M3 auto-split scheduler), and drive transfers through the same `TransferLeadership` machinery PD uses on TiKV.

### 1.3 What is already in-tree (the primitives we build on)

- **Engine transfer API.** The `Admin` interface exposes `TransferLeadership(ctx)` and `TransferLeadershipToServer(ctx, id, address)` with a documented non-blocking / goroutine-offload contract (`internal/raftengine/engine.go:247-248`, and `RegisterLeaderAcquiredCallback` at `:249-271`).
- **etcd backend implementation.** `TransferLeadership` / `TransferLeadershipToServer` submit an admin request onto the single-threaded event loop (`internal/raftengine/etcd/engine.go:1317-1331`); `handleTransferLeadership` (`:1743-1771`) resolves the target, **rejects the call when the local node is not leader** (`errLeadershipTransferNotLeader`, `:1754-1757`), calls `rawNode.TransferLeader`, and surfaces an immediate `errLeadershipTransferRejected` if raft drops the request (target is a learner / has no progress / equals self, `:1765-1768`). `waitForLeadershipTransfer` / `checkLeadershipTransfer` (`:1377-1439`) poll `Status.LeadTransferee` and fail closed if raft aborts the transfer (e.g. election timeout elapses before the target catches up). Default target selection picks the first non-self voter (`defaultTransferTargetLocked`, `:3852-3860`); named target selection rejects self (`namedTransferTargetLocked`, `:3862-3879`).
- **An automated-transfer precedent already in-tree.** `main_sqs_leadership_refusal.go` registers a per-group leader-acquired observer that *refuses* leadership of any group hosting a partitioned FIFO queue when the binary lacks the `htfifo` capability, by calling `TransferLeadership` on acquire. It demonstrates the exact patterns this design needs: idempotent refusal (a second call is a no-op once a transfer is in flight, `:104-109`), goroutine-offload of the blocking admin call from the non-blocking callback (`:88-95`), and the TOCTOU-safe install (read `State()`, fire if already leader, register, re-check, `:97-120`). It is wired in `main.go:439-442` via `installSQSLeadershipRefusalAcrossGroups` (`main_sqs_leadership_refusal.go:178-217`), which iterates every `raftGroupRuntime` and installs the hook per group hosting a partitioned queue.
- **Per-group remote transfer over gRPC.** Each group's engine is exposed on its own listener via `internalraftadmin.RegisterOperationalServicesWithInterceptor(ctx, gs, rt.engine, …)` (`main.go:1610`; the server is engine-generic, one engine per `RaftAdmin` service, `internal/raftadmin/server.go:13-47`, `internal/raftadmin/health.go:33-52`). `cmd/raftadmin/main.go` is the operator client (`leadership_transfer`, `leadership_transfer_to_server`, `:210-217`, `:359-378`). This is the existing in-tree mechanism for invoking a transfer on a group whose leader is a *different* node.
- **Per-group leader observation precedent.** `startKeyVizLeaderTermPublisher` / `publishLeaderTerms` (`main.go:2088-2143`) already iterate `runtimes` on a ticker and read each group engine's `Status()` (term) through the `snapshotEngine()` accessor (`multiraft_runtime.go:41-48`). The same iteration over `rt.engine.State()` / `rt.engine.Leader()` (`internal/raftengine/engine.go:131-138`) gives the balancer its observation input for free.
- **Leader-local reset hook.** Coordinator subsystems already reset on leadership change via `LeaseProvider.RegisterLeaderLossCallback` (`kv/coordinator.go:131`, `kv/sharded_coordinator.go:584`), the same mechanism the lease invalidation uses.

## 2. Goals and Non-Goals

### 2.1 Goals

1. Automatically spread Raft-group leaderships across nodes so no single node leads a disproportionate share of groups (TiKV count-based balance-leader, v1).
2. **Default OFF** behind `--leaderBalance`, with a runtime kill switch and bounded-cardinality metrics.
3. Deterministic, unit-testable policy: given a leader-count map, the decision is a pure function.
4. Never fight existing leader-pinning policy (SQS leadership refusal); never transfer to an unhealthy / lagging follower; never act mid-conf-change.
5. Reuse the in-tree transfer mechanism (`TransferLeadership(ToServer)` and the per-group `RaftAdmin` gRPC service) — no new wire surface for v1.

### 2.2 Non-Goals

1. **Load- or size-weighted balancing.** v1 balances *leader count* only. Weighting by keyviz per-group load (the same signal M3 consumes, `keyviz/sampler.go`) is explicit future work (§7, future PR), not v1.
2. **Raft group creation / membership orchestration.** The balancer only moves leadership among the voters a group already has; it never adds/removes members (that is `raftadmin add_voter` / `remove_server`).
3. **Replica (peer) rebalancing / data movement.** Moving where a group's *replicas* live is out of scope; only *leadership* moves. (Region/replica balance is TiKV's separate balance-region scheduler; not in scope here.)
4. **Cross-process coordination beyond Raft.** The balancer runs in-process on the default-group leader; there is no external PD.

## 3. Design questions answered

### 3.1 Who balances (decision: the default-group leader)

The scheduler runs **only on the current default-group leader**, for three reasons that all already hold for other control-plane work:

- It is the natural control-plane seat: HLC ceiling renewal already runs there (`ShardedCoordinator.RunHLCLeaseRenewal`, `kv/sharded_coordinator.go:1914-1948`; the proposer that issues the ceiling is leader-gated), and route-catalog proposes go through the default group (`distribution/`, CLAUDE.md "Control plane").
- The M3 auto-split scheduler is placed there too (sibling doc §4.1, §7.6) — co-locating the leader balancer keeps "one scheduler seat per cluster" rather than electing yet another coordinator.
- There is exactly one default-group leader at a time, so the scheduler is singleton without extra leader election.

**State reset on election.** All balancer state (per-group cooldown deadlines, the global cooldown deadline, the last observed leader map) is **leader-local, in-memory, not Raft-replicated**. On a default-group leadership change the deposed leader's scheduler goroutine stops (via `RegisterLeaderLossCallback`, `kv/coordinator.go:131`) and the new leader starts with empty state. The worst case of a lost cooldown is a *too-soon* extra transfer (mildly wasteful, self-correcting next cycle), never an unsafe action — so non-replication is the right cost/safety trade (same rationale as M3 §7.6). We state this explicitly so it is not mistaken for an oversight.

### 3.2 Observation (decision: local per-group `State()` / `Leader()`, no polling RPC)

Because every node runs every group (§1.1), the default-group leader is itself a member (leader or follower) of every other group and can read **the local engine's view of who leads each group** with zero network cost: iterate `runtimes`, and for each `rt.snapshotEngine()` read `State()` and `Leader()` (`internal/raftengine/engine.go:131-138`). This is exactly what `publishLeaderTerms` already does for term (`main.go:2126-2143`); the balancer adds a sibling reader for leader identity. From the per-group `Leader().ID` the scheduler builds the **leader-count map** `nodeID → number of groups this node currently leads`.

- **Unknown / no-leader groups are skipped.** A group whose local engine reports `Leader().ID == ""` (election in flight, or this node has no fresh contact) is *excluded* from the count map for this cycle — the scheduler never acts on a group it cannot see a stable leader for. This is the cheapest correct source; it relies only on the local Raft state the node already maintains, and a group mid-election is exactly the group we must not perturb.
- **Why not poll peers** (a `GetClusterOverview`-style fan-out, `adapter/admin_grpc.go:138-149`): unnecessary, because local Raft state already names each group's leader on every member, and a fan-out would add latency, a configured-endpoint dependency, and a fresh source of staleness. We stay local. (A future load-weighted policy that needs per-group load from *non-leader* nodes could reuse the keyviz cluster fan-out behind the same policy interface — noted in §7, mirrors M3 OQ-5.)

### 3.3 Policy (decision: count-based, one transfer per cycle, hysteresis + cooldowns)

The policy is a **pure function** `(leaderCountMap, eligibility, config, now) → at most one TransferDecision{groupID, fromNode, toNode}`. Pure-function shape makes the table-driven tests trivial (§8) and the leader-local reset a matter of dropping state.

- **Objective:** minimize the spread of leader counts across nodes. The ideal per-node count is `⌈groups / nodes⌉` (ceil) at most.
- **Imbalance trigger (hysteresis):** act only when `max(count) - min(count) >= imbalanceThreshold` (default **2**). A spread of 1 is the unavoidable remainder when `groups` is not divisible by `nodes` and must never trigger a transfer — otherwise the scheduler ping-pongs forever against the arithmetic. A threshold of 2 is the smallest value that is provably stable at the optimum.
- **Source / group / target choice (deterministic):**
  - **Source node** = the node with the **most** leaders (tie-break: lexicographically smallest node ID, so the decision is reproducible in tests).
  - **Group to move** = among the groups led by the source node that are *eligible* (§3.5), pick deterministically (tie-break by group ID ascending).
  - **Target node** = among nodes that are **voters of that group**, **healthy** (§3.5), and **not policy-excluded for that group** (§3.5), the one with the **fewest** leaders (tie-break: smallest node ID). The transfer is skipped if moving the leader to the target would not strictly reduce the spread (i.e. `targetCount + 1 > sourceCount - 1`), so a transfer never makes things worse or merely shuffles a tie.
- **One transfer per cycle.** At most one `TransferLeadershipToServer` is issued per evaluation cycle. A cluster-wide imbalance is corrected over several cycles, never in a burst (mirrors TiKV's operator limit and M3's `maxSplitsPerCycle`).
- **Per-group cooldown** (default **30 s**, ≥ a few election timeouts): after a transfer is issued for a group, that group is ineligible until the cooldown expires, so a single group is not bounced repeatedly while the new leader settles.
- **Global cooldown** (default **10 s**) after *any* transfer: gives the cluster time to re-stabilize and the next observation to reflect the completed move before another is scheduled.
- **Anti-ping-pong with natural elections:** cooldowns + the spread-strictly-decreases guard + the `>=2` threshold together ensure the scheduler does not chase a natural re-election (which momentarily changes counts) into an oscillation. Cooldown deadlines use a **monotonic clock** (`internal/monoclock`) so a wall-clock step cannot shorten/extend them (CLAUDE.md: no ordering-sensitive wall-clock use).

### 3.4 Mechanism (decision: the balancer host issues the transfer; follower-initiated transfer is NOT possible, so requests to groups the host does not lead are FORWARDED over the per-group `RaftAdmin` gRPC service)

The transfer must be initiated **on the current leader of the target group**: `handleTransferLeadership` rejects with `errLeadershipTransferNotLeader` whenever the local engine is not in `StateLeader` (`internal/raftengine/etcd/engine.go:1754-1757`). So a follower **cannot** initiate a transfer — this is a hard constraint, not a preference.

The balancer host (the default-group leader) is a member of every group but is, in general, a **follower** of the group it wants to rebalance (indeed, the whole point is to move leadership *off* the over-loaded node, which may or may not be the balancer host). Two sub-cases:

1. **The over-loaded source node is the balancer host itself** (it leads the target group). Then the balancer calls `engine.TransferLeadershipToServer(ctx, target.ID, target.Address)` directly on the **local** runtime's engine for that group — the local engine is the leader, the call is accepted, and the goroutine-offload + idempotency patterns from `main_sqs_leadership_refusal.go:88-95` apply verbatim.
2. **The over-loaded source node is a *different* node.** The balancer host is a follower of that group and cannot initiate the transfer locally. It **forwards** the request to the source node's leader of that group, using the in-tree per-group `RaftAdmin` gRPC service: dial the source node's group listener (`rt.spec.address` for that group is in the group's `Configuration`, available via `engine.Configuration(ctx)`, `internal/raftengine/engine.go:213-215`) and call `RaftAdmin.TransferLeadership` with `TargetId`/`TargetAddress` set to the chosen target (the same RPC `cmd/raftadmin` uses, `internal/raftadmin/server.go:155-172`, `cmd/raftadmin/main.go:359-378`). The receiving node's engine is the leader of that group, so `handleTransferLeadership` accepts it.

Reusing the existing `RaftAdmin` service avoids inventing a new wire surface. The forward path is a thin gRPC client (mirror the dial/credentials handling in `cmd/raftadmin/main.go:72-105`; reuse the admin connection cache that `startAdminFromFlags` already threads, `main.go:1238`). **Open question OQ-4** asks whether to instead add a small purpose-built internal RPC rather than reuse the operator-facing `RaftAdmin` service.

> Note on `TransferLeadershipToServer` vs. `TransferLeadership`: the design always uses the **targeted** form (`ToServer`) so the destination is the balancer's chosen least-loaded node, not raft's default "first non-self voter" (`defaultTransferTargetLocked`, `internal/raftengine/etcd/engine.go:3852-3860`), which would not respect the balance objective.

### 3.5 Safety / exclusions

A group is **eligible** for a transfer this cycle only if **all** of the following hold; otherwise it is skipped (and counted in a skip metric):

- **No conf-change in flight.** Skip a group whose membership is changing. (Surface via the engine status / configuration read; never transfer mid-membership-change — a transfer racing a conf-change can land on a member about to be removed.) **OQ-5** asks for the cleanest in-tree signal for "conf-change pending" per group.
- **Target follower is caught up / healthy.** etcd/raft only *completes* a transfer once the transferee's log has caught up to the leader (`waitForLeadershipTransfer` will otherwise observe the transfer abort, `internal/raftengine/etcd/engine.go:1384-1405`, `:1431-1437`). Transferring to a lagging follower stalls writes on that group until the transferee catches up or raft aborts. The scheduler therefore **must not pick a lagging follower as target**: it filters targets to followers whose `Progress.Match` (or the available `Status` liveness/last-contact proxy) is close enough to the leader's commit index, and prefers a follower with recent contact. The targeted transfer also fails fast at submit time if raft drops it (`errLeadershipTransferRejected`, `:1765-1768`), so a bad target surfaces as a logged failure rather than a silent stall.
- **Not policy-pinned.** A group **must never** be balanced onto a node whose policy *refuses* leadership of it:
  - **SQS leadership refusal.** A node lacking the `htfifo` capability refuses leadership of any group hosting a partitioned FIFO queue (`main_sqs_leadership_refusal.go:69-121`, wired from `--sqsFifoPartitionMap` via `partitionedGroupSet`, `main_sqs_leadership_refusal.go:136-156`, `shard_config.go:174-196`). If the balancer transferred such a group onto a refusing node, that node would immediately transfer it away again — a guaranteed ping-pong. The balancer must therefore **know which (group, node) pairs are refusing** and exclude them as transfer targets, and exclude refusing groups from being *moved onto* such nodes. **How it knows:** the same configuration that drives the refusal hook — the partitioned-group set (`partitionedGroupSet`) plus each node's advertised `htfifo` capability — is the source of truth. v1 takes the conservative correct stance: for any group in the partitioned-FIFO set, the balancer **excludes that group from balancing entirely** unless it can positively confirm every candidate target advertises `htfifo`. Since capability is per-binary and not currently published per-node in a balancer-readable form, v1 simply **excludes partitioned-FIFO groups from leader balancing** (they are already governed by the refusal hook). **OQ-2** asks whether to instead publish per-node `htfifo` capability (e.g. via the existing capability fanout, `main.go` `encryptionCapabilityFanout` / `CapabilityReport`) so those groups *can* be balanced among the htfifo-capable subset.
  - **Future pinning flag.** Reserve a `--leaderBalancePinGroups` (and/or per-group pin) exclusion list so an operator can pin a group's leadership (e.g. to a node with special hardware) and keep the balancer off it. (Design the config now; ship the flag with the scheduler PR.)
- **Default group is balanceable, with a called-out blip.** The default group is *not* excluded — leaving it pinned to one node defeats half the point (that node also carries HLC/catalog work). But moving the default group's leadership has two transient effects the operator must understand:
  - **HLC ceiling renewal** restarts on the new leader. The ceiling is proposed every `hlcRenewalInterval` (1 s) with a `hlcPhysicalWindowMs` (3 s) window (`kv/coordinator.go:37-46`, `:644-669`); because the window (3 s) exceeds the renewal interval (1 s) and a newly elected leader clamps `Next()` to `max(wall, ceiling)`, a clean transfer does **not** let the new leader issue a timestamp inside the old leader's window — the safety invariant holds across a transfer exactly as it does across a natural election.
  - **Lease reads:** a transfer invalidates the lease on the old leader (`RegisterLeaderLossCallback → lease.invalidate`, `kv/coordinator.go:131`, `kv/sharded_coordinator.go:584`), so the new leader's *first* read takes the slow `LinearizableRead` path (one ReadIndex round-trip) before its lease warms again — a single read-latency blip, not a correctness issue. This is identical to the blip on any natural election. The cooldowns keep these blips rare. **OQ-3** asks whether the default group should be balanced first, last, or pinned by default.
- **In-flight hotspot split (M2/M3) interaction — decision: rely on cooldowns, do not add an explicit interlock in v1.** A leadership transfer during an in-flight `SplitJob` (M2) does not corrupt the job — the migrator runs on the default-group leader and is itself reset/resumed on leadership change (M3 §7.6 / M2 resumability). Adding a hard "pause balancing during any active SplitJob" interlock would couple this scheduler to M2/M3's job state, which the doc-first plan keeps decoupled (M3 is not a dependency). v1 therefore relies on the per-group and global cooldowns to keep transfer churn low enough not to disturb a split; if production shows interference, a follow-on can add the interlock behind the shared scheduler interface. **OQ-6** records this for reviewers.

### 3.6 Ops

- **Default OFF behind `--leaderBalance`** (bool, default `false`). When false, no scheduler goroutine starts; behavior is byte-identical to today. When true, the default-group leader runs the observe→decide→transfer loop on `--leaderBalanceInterval` (default e.g. 30 s).
- **Tuning flags:** `--leaderBalanceInterval`, `--leaderBalanceImbalanceThreshold` (default 2), `--leaderBalancePerGroupCooldown` (30 s), `--leaderBalanceGlobalCooldown` (10 s), `--leaderBalancePinGroups` (comma-separated group IDs to exclude).
- **Runtime kill switch:** the loop checks an `atomic.Bool` each cycle; an admin RPC / endpoint (reuse the existing admin surface that hosts keyviz/distribution operator calls, `adapter/admin_grpc.go`) toggles it without a restart. Flipping it off stops new transfers at the next cycle boundary; an already-issued transfer completes (it is a single raft operation). Mirrors M3 §7.2.
- **Metrics (bounded cardinality, per-node, fixed-enum labels only — no per-group/per-node-pair labels):**
  - Counters: `leaderbalance_transfers_attempted_total`, `leaderbalance_transfers_succeeded_total`, `leaderbalance_transfers_failed_total{reason}` where `reason ∈ {not_leader, rejected, aborted, rpc_error, timeout}`; `leaderbalance_skipped_total{reason}` where `reason ∈ {below_threshold, cooldown, conf_change, no_healthy_target, pinned, sqs_refused, in_cooldown_global}`.
  - Gauges: `leaderbalance_enabled` (0/1); `leaderbalance_leaders_per_node{node}` — **bounded** because node count is small and operator-fixed (unlike route count); `leaderbalance_eval_duration_seconds` (last cycle wall time, diagnostic). (If even per-node cardinality is a concern in very large clusters, expose `leaderbalance_leader_count_spread` — the single max-min number — instead; **OQ-7**.)
- **Structured slog** with stable keys per CLAUDE.md: `group_id`, `from`, `to`, `from_count`, `to_count`, `spread_before`, `spread_after`, `decision ∈ {transfer, skip, cooldown}`, `reason`. Exactly one line at the decision point and one at the transfer result.
- **Manual escape hatch already exists.** `cmd/raftadmin <addr> leadership_transfer_to_server <id> <address>` lets an operator move leadership by hand at any time (`cmd/raftadmin/main.go:212-217`, `:359-378`); the balancer does not remove that path, and an operator can disable the balancer (kill switch) and drive transfers manually.

## 4. Milestones / PR breakdown

| PR | Scope | Tests | Independently shippable? |
|---|---|---|---|
| **PR1** | Leader-count **observation + metrics only**: a leader-identity reader sibling to `publishLeaderTerms` (`main.go:2126-2143`) that builds the per-node leader-count map on the default-group leader, and exports the `leaderbalance_leaders_per_node` gauge + `leaderbalance_enabled=0`. **No transfers.** Pure observability. | Unit: leader-map construction from a fake set of per-group `State()`/`Leader()`; gauge registration. | Yes — observe-only, zero behavior change. |
| **PR2** | The pure **policy function** (§3.3) + the **scheduler loop** + **transfer execution** behind `--leaderBalance` (default OFF) + runtime kill switch + slog + the transfer-result metrics. Local-leader transfer path (case 1, §3.4) and forwarded path (case 2). Eligibility limited to "no-leader skip" + per-group/global cooldowns + conf-change skip + healthy-target filter. | Unit (table-driven): policy decisions over crafted leader maps (imbalance threshold, source/target choice, tie-breaks, strict-spread-decrease guard, cooldown gating). Integration: 3-node demo (`cmd/server/demo.go`) — force all leaders onto one node, enable `--leaderBalance`, assert convergence to ≤ `⌈groups/nodes⌉` per node. Kill-switch + leader-change-reset tests. | Yes — completes count-based balancing. |
| **PR3** | **SQS-refusal / pinning awareness** (§3.5: exclude partitioned-FIFO groups and pinned groups) + the **split-job interlock decision** (rely on cooldowns; add explicit interlock only if a reviewer requires it) + `--leaderBalancePinGroups`. | Unit: eligibility excludes refused/pinned groups; property test (`rapid`) — never target a refusing node, never exceed one transfer/cycle, never transfer below threshold. | Yes — hardens the policy; safe to ship after PR2. |
| **Future** | **Load-weighted policy**: consume keyviz per-group load (the M3 signal) behind the same policy interface so the objective becomes "balance leader *load*", not just count. Optional cross-node load fan-out (OQ-1). | Unit: weighted scoring; integration with skewed load. | Depends on keyviz signal availability; policy interface unchanged. |
| **PR-doc** | Lifecycle: `*_proposed_*` → `*_partial_*` after PR1; → `*_implemented_*` after PR3 (count-based complete), with the load-weighted PR tracked as a follow-on. `git mv`, propose date fixed. | — | Doc-only. |

Each PR carries the five-lens self-review and is gated by its tests + `make lint`.

## 5. Test strategy

- **Unit (table-driven, co-located `*_test.go`):** the policy function is pure (§3.3), so tests feed a `nodeID → leaderCount` map (plus eligibility set, cooldown state, config, a fixed `now`) and assert the single `TransferDecision` (or "no decision"): imbalance threshold boundary (spread 1 → none, spread 2 → transfer), source = max / target = min, tie-breaks by node/group ID, strict-spread-decrease guard, per-group and global cooldown gating, pinned/refused exclusion.
- **Property tests (`pgregory.net/rapid`):** randomized leader maps + cooldown states; assert invariants — at most one transfer per cycle; a transfer always strictly decreases (or keeps) the spread, never increases it; never targets a node not in the group's voter set; never targets a pinned/refusing node; never acts below threshold.
- **Integration (3-node demo, `cmd/server/demo.go`):** configure N groups across 3 nodes; force all leaderships onto one node (drive transfers there, or restart the others); enable `--leaderBalance`; assert the cluster converges to no node leading more than `⌈N/3⌉` groups within a bounded number of cycles, and that it then **stays** converged (no ping-pong) for several further cycles. Separately: flip the kill switch mid-convergence and assert no further transfers; kill the default-group leader and assert the new leader resumes balancing with reset state.
- **Jepsen note:** existing Jepsen suites (Redis / DynamoDB workloads under `jepsen/`, `scripts/run-jepsen-local.sh`) already churn leadership via nemeses; the balancer adds *controlled* churn on top. The acceptance bar is **no new anomalies with `--leaderBalance` on** — run the existing suites once with the balancer enabled and confirm linearizability/consistency results are unchanged versus the baseline run. The balancer does not introduce a new workload; it is validated as "safe under the existing safety net."

## 6. Five-lens self-review (to be filled per PR)

| Lens | Leader-balance-specific risk to check |
|---|---|
| **Data loss** | A leadership transfer is a standard raft operation that loses no committed entry; etcd/raft completes it only after the transferee has caught up (`waitForLeadershipTransfer`, `etcd/engine.go:1377-1405`). The scheduler issues no writes and touches no FSM/Pebble state. Confirm PR1 is observe-only (no transfers). Confirm a transfer that races a conf-change is *skipped*, not forced (§3.5). |
| **Concurrency / distributed** | Scheduler goroutine vs. default-group leadership flip (leader-local state reset, §3.1); transfer racing a natural election (cooldowns + strict-spread guard, §3.3); follower-initiated transfer is impossible and must be forwarded (§3.4) — verify the forward path targets the *current* leader of the group and fails closed (`errLeadershipTransferNotLeader`) if the leader moved under it; kill-switch toggle race (atomic). Run `go test -race ./kv/... ./internal/raftengine/...` and the matching Jepsen suite (CLAUDE.md lens 2). |
| **Performance** | Observation is local `Status()`/`Leader()` reads on a 30 s ticker — no Raft round-trips, no per-`Next()` HLC consensus, no Pebble reads (§3.2). One transfer per cycle bounds churn. Metric cardinality is fixed-enum / per-node only (§3.6). No hot-path code path is touched. |
| **Data consistency** | A transfer perturbs the default group's HLC renewal and lease reads transiently but preserves the HLC physical-ceiling invariant (a clean transfer is no different from a natural election; the 3 s window > 1 s renewal guarantees no in-window reissue, §3.5). Lease-read freshness is preserved — the new leader falls back to `LinearizableRead` until its lease re-warms. No route-catalog mutation occurs (the balancer never writes the catalog). |
| **Test coverage** | Pure-policy table tests + `rapid` invariants + 3-node convergence/anti-ping-pong integration + kill-switch + leader-change reset; Jepsen "no new anomalies with balancer on" run recorded. SQS-refusal/pinning exclusion has dedicated tests in PR3. |

## 7. Future work (out of scope for v1)

- **Load-weighted balancing.** Replace the count objective with a leader-*load* objective driven by keyviz per-group aggregates (the same signal M3 consumes, `keyviz/sampler.go`), behind the unchanged policy interface. Optionally pull cross-node per-group load via the keyviz cluster fan-out (mirrors M3 OQ-5).
- **Per-node capability publication** so partitioned-FIFO groups can be balanced among htfifo-capable nodes instead of excluded entirely (OQ-2).
- **Shared scheduler runtime** with the M3 auto-split scheduler (one leader-local control loop hosting both) once both ship — reduces duplicated leader-local state-reset and kill-switch plumbing.

## 8. Open Questions

1. **OQ-1 — Count vs. load for v1.** v1 balances leader *count*. Is count sufficient as the first deliverable (groups are roughly equal load), or should v1 already weight by keyviz per-group load? (Recommendation: ship count first; it is deterministic and needs no load signal.) (§3.3, §7)
2. **OQ-2 — SQS-refused groups: exclude vs. balance-among-capable.** v1 excludes partitioned-FIFO groups from balancing entirely (the refusal hook already governs them). Should we instead publish per-node `htfifo` capability and balance those groups among the capable subset? (§3.5)
3. **OQ-3 — Default group: balance first, last, or pinned by default?** Balancing the default group is correct (it carries HLC/catalog load) but incurs an HLC-renewal restart + one lease-read blip on transfer. Should the default group be balanced normally, balanced *last* (only after others are even), or pinned-by-default with an opt-in? (§3.5)
4. **OQ-4 — Forward path: reuse `RaftAdmin` gRPC vs. a purpose-built internal RPC.** The forward path (transfer on a group the balancer host does not lead) reuses the operator-facing `RaftAdmin.TransferLeadership` service. Acceptable, or add a small internal RPC so the operator surface and the automated surface are separable (e.g. for auth/audit)? (§3.4)
5. **OQ-5 — Cleanest "conf-change pending" signal per group.** What is the least-leaky in-tree signal that a group has a membership change in flight, so the scheduler can skip it (§3.5)? Is `Status`/`Configuration` enough, or do we need an explicit engine flag?
6. **OQ-6 — Split-job interlock vs. cooldown-only.** v1 relies on cooldowns rather than an explicit "pause during active SplitJob" interlock, to keep this scheduler decoupled from M2/M3. Is cooldown-only acceptable, or should PR3 add the interlock behind the shared interface? (§3.5)
7. **OQ-7 — Per-node gauge cardinality.** `leaderbalance_leaders_per_node{node}` is bounded by node count. In very large clusters, prefer the single `leaderbalance_leader_count_spread` scalar instead? (§3.6)
8. **OQ-8 — Default tuning values.** Are `imbalanceThreshold = 2`, per-group cooldown 30 s, global cooldown 10 s, eval interval 30 s the right starting defaults, or should they be derived from the election timeout? (§3.3, §3.6)

## 9. Lifecycle

This document begins as `*_proposed_*`. Per CLAUDE.md / `docs/design/README.md`:

- Rename to `*_partial_*` after PR1 lands (observation + metrics).
- Rename to `*_implemented_*` after PR3 ships (count-based balancing with SQS-refusal/pinning awareness complete), with the load-weighted policy tracked as a follow-on.

Use `git mv` so history follows the rename. The propose date (2026-06-11) and slug stay fixed.
