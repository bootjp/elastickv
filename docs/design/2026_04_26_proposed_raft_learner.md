# Raft Learner support

> **Status: Proposed**
> Author: bootjp
> Date: 2026-04-26
>
> Builds on the etcd/raft migration (`2026_04_05_implemented_etcd_raft_migration.md`).
> Phase 3 of that doc deliberately deferred learner / non-voter membership;
> this proposal closes that gap. It is independent of — but compatible with —
> the future follower-served-read effort, which would consume learners as a
> read replica primitive.

---

## 1. Problem

Today every member of an Elastickv Raft group is a voter. The only membership
mutations exposed by the engine are `AddVoter` and `RemoveServer`
(`internal/raftengine/engine.go:174`). That has three concrete consequences:

1. **Membership change is unsafe under load.** Adding a fresh node directly as a
   voter shrinks the effective fault tolerance of the group during catch-up:
   the new voter counts toward the denominator immediately but cannot vote
   intelligently until it has caught up on the log. A leader that loses
   contact with one healthy follower while the new voter is still receiving
   the snapshot can stall writes — the classic "fourth-voter pause" problem.
   Operators today work around this by pre-seeding data dirs out of band,
   which defeats the abstraction.

2. **Replica catch-up has no first-class story.** The etcd backend already
   knows how to send a snapshot and stream log entries to a peer
   (`snapshot_spool.go`, the snapshot dispatch lane in `engine.go`), but the
   only way to attach a peer to that pipeline is to make it a voter.

3. **The roadmap requires it.** Hotspot shard split
   (`2026_02_18_partial_hotspot_shard_split.md`), workload isolation
   (`2026_04_24_proposed_workload_isolation.md`), and any future
   follower-served read mode all need a way to attach a *non-voting* replica
   that pulls log entries without joining the quorum. Hand-rolling that
   per-feature is the wrong shape.

The etcd/raft library already supports learners natively
(`raftpb.ConfChangeAddLearnerNode`, `ConfState.Learners`). The codebase has
even partly anticipated this — `applyConfigPeerChangeToMap` keeps learner
metadata when replaying logs, and `nextPeersAfterConfigChangeKeepsLearnerMetadata`
is a regression test for that — but the live engine path explicitly drops
learner conf changes and the persistence layer rejects any `ConfState` with
non-empty `Learners` as "joint consensus". This doc proposes turning that
dormant support on.

## 2. Goals & non-goals

**Goals**

- Add `AddLearner` and `PromoteLearner` to the `raftengine.Admin` interface,
  the etcd backend, the `RaftAdmin` gRPC service, and the `cmd/raftadmin`
  CLI.
- Report learner suffrage end-to-end. `Configuration.Servers[i].Suffrage`
  must be `"learner"` for learners and `"voter"` for voters; today it is
  hard-coded to `"voter"` in two places (`engine.go:2656`, `engine.go:3096`).
- Persist learner membership across restarts. The
  `etcd-raft-peers.bin` peer-metadata file must round-trip suffrage so that
  a node restarting from disk does not silently re-promote itself or
  forget that it is a learner.
- Allow a node to **start as a learner** when joining an existing cluster,
  without ever being a voter first. Cold-bootstrap a cluster with
  learner-only members must remain rejected.
- Make sure learners do **not** affect liveness. The lease-read fast path
  (`LeaseProvider`) must not require a learner ack. The `quorumAckTracker`
  denominator must exclude learners.
- Accept a `min_applied_index` precondition on promotion so an operator who
  promotes a learner that has not actually caught up gets a clean
  precondition error from the engine, not a silent quorum stall.

**Non-goals**

- Follower-served reads (`LinearizableRead` answered locally on a follower
  or learner). That is a separate proposal that consumes this primitive;
  it requires its own treatment of staleness bounds, lease invalidation,
  and adapter routing changes. Until that lands, learners forward
  `LinearizableRead` to the leader's `ReadIndex` like any other follower
  would.
- Joint consensus / atomic multi-member reconfig. `validateConfState`
  continues to reject `VotersOutgoing`, `LearnersNext`, and `AutoLeave`.
  Learner add/promote uses the simple V1 single-step `ConfChange` path that
  the engine already takes for `AddVoter` / `RemoveServer`.
- Witness / weighted voter / arbiter roles.
- Cross-engine portability. The HashiCorp backend was deleted in `a35245a`;
  the etcd backend is the only target.
- Auto-promotion. The engine never decides on its own to turn a learner into
  a voter. Promotion is always an explicit operator (or higher-layer
  orchestration) action through `PromoteLearner`.
- Adapter / KV semantics changes. `ShardedCoordinator`, `LeaderProxy`,
  `LeaseProvider`, and `LinearizableRead` already gate on
  `state == StateLeader` for the fast path; a learner naturally falls
  through to the slow path. No KV-layer change is required.

## 3. Background

### 3.1 etcd/raft semantics

In etcd/raft, a learner is a node that:

1. Receives `MsgApp` and `MsgSnap` like any follower.
2. Persists log entries and applies them to its local state machine.
3. Does **not** vote. `MsgVote` / `MsgPreVote` from a learner is dropped at
   the leader's progress tracker, and a learner cannot become candidate.
4. Is **not** counted toward majority. `Progress.IsLearner == true` excludes
   it from the quorum denominator on writes and on `ReadIndex`.
5. Is **not** a valid leadership-transfer target. The library drops
   `MsgTransferLeader` aimed at a learner. The engine already documents
   this in `handleTransferLeadership` (`engine.go:1384`); it gives us the
   "learner cannot be promoted by accident via TransferLeader" property
   for free.

The two membership operations relevant to this doc are:

| etcd/raft conf change       | Effect                                                                                                 |
|-----------------------------|--------------------------------------------------------------------------------------------------------|
| `ConfChangeAddLearnerNode`  | Add a peer as a learner. No quorum impact.                                                             |
| `ConfChangeAddNode` for an existing learner | **Promote** the learner to voter. The library expects the node to be caught up before this lands. |

Neither operation enters joint consensus.

### 3.2 Today's behaviour in the elastickv etcd backend

The engine *partially* handles learners and *fully* refuses to expose them:

| File                                              | Behaviour                                                                                                                                                                                |
|---------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `internal/raftengine/engine.go`                    | `Admin` interface has no `AddLearner` or `PromoteLearner`. `Server.Suffrage` field exists but no caller produces `"learner"`.                                                            |
| `internal/raftengine/etcd/engine.go:1817-1830`     | Live `applyConfigPeerChange` ignores `ConfChangeAddLearnerNode`. Comment: "Phase 3 only exposes voter membership changes."                                                               |
| `internal/raftengine/etcd/engine.go:1869-1884`     | Map-form `applyConfigPeerChangeToMap` does keep learner peer metadata in the in-memory snapshot — necessary for log replay round-trip — but the live engine's `peers` map does not. |
| `internal/raftengine/etcd/engine.go:2644-2667`     | `configurationFromConfState` only walks `ConfState.Voters`; it never emits a server with `Suffrage: "learner"`.                                                                          |
| `internal/raftengine/etcd/engine.go:3080-3106`     | `upsertPeer` hard-codes `Suffrage: "voter"`.                                                                                                                                              |
| `internal/raftengine/etcd/wal_store.go:419-421`    | `validateConfState` rejects any `ConfState` with non-empty `Learners` / `LearnersNext` / `VotersOutgoing` as "joint consensus state is not supported".                                  |
| `internal/raftengine/etcd/peers.go:216-222`        | `confStateForPeers` only emits `Voters`. There is no per-peer suffrage on the `Peer` struct.                                                                                              |
| `internal/raftengine/etcd/peer_metadata.go`        | Persisted peers file (`etcd-raft-peers.bin`, version 1) stores `(NodeID, ID, Address)`. No suffrage byte.                                                                                  |
| `proto/service.proto`                              | `RaftAdmin` exposes `AddVoter`, `RemoveServer`, `TransferLeadership`. No `AddLearner` or `PromoteLearner`. `RaftAdminMember.suffrage` exists in the response message.                    |
| `cmd/raftadmin/main.go`                            | Subcommands: `add_voter`, `remove_server`, `leadership_transfer`, `leadership_transfer_to_server`. No learner subcommands.                                                                |

The dormant pieces (`applyConfigPeerChangeToMap`, the `Suffrage` field on
`Server`, the `suffrage` field on `RaftAdminMember`, the regression test for
preserved learner metadata) all suggest the original etcd-migration design
expected this exact follow-up.

## 4. Design

### 4.1 Engine API

Add two methods to `raftengine.Admin`. Both are leader-only. Both use the
same `prevIndex` precondition pattern as the existing membership RPCs.

```go
type Admin interface {
    LeaderView
    StatusReader
    ConfigReader

    AddVoter(ctx context.Context, id, address string, prevIndex uint64) (uint64, error)
    AddLearner(ctx context.Context, id, address string, prevIndex uint64) (uint64, error)
    PromoteLearner(ctx context.Context, id string, prevIndex uint64, minAppliedIndex uint64) (uint64, error)
    RemoveServer(ctx context.Context, id string, prevIndex uint64) (uint64, error)

    TransferLeadership(ctx context.Context) error
    TransferLeadershipToServer(ctx context.Context, id, address string) error
}
```

Notes on the new methods:

- `AddLearner` is the symmetric counterpart of `AddVoter`. It proposes a
  V1 `ConfChangeAddLearnerNode` with the same encoded peer-context the
  voter path already uses (`encodeConfChangeContext`). Address resolution
  uses the same `resolveAdminPeer` helper.
- `PromoteLearner` proposes a V1 `ConfChangeAddNode` for an existing
  learner. The engine asserts on the leader, on the single-threaded admin
  loop, that:
  1. The peer exists in `ConfState.Learners` (not in `Voters`). Otherwise
     return a `FailedPrecondition` so callers do not silently no-op.
  2. The leader's `Progress[nodeID].Match >= minAppliedIndex`. The leader
     is the authority on follower progress; an operator who reads
     `Status.AppliedIndex` from the learner itself sees a slightly stale
     view, so the engine re-checks against the leader's tracker before
     proposing. `minAppliedIndex == 0` skips the precondition (matches the
     existing "0 means don't care" convention used by `prevIndex`).
- `RemoveServer` is unchanged. etcd/raft handles `ConfChangeRemoveNode` for
  a learner correctly, and the existing path already calls `peerForID` /
  `removePeer`.

### 4.2 etcd backend internals

Three concrete edits, each tightly scoped:

1. `applyConfigPeerChange` (engine.go:1817) stops dropping
   `ConfChangeAddLearnerNode`. The handler routes to `applyAddedPeer`
   (same as `ConfChangeAddNode`), and the *suffrage* of the peer is no
   longer carried in the engine's `peers` map but inferred at read time
   from `ConfState`. The `peers` map remains a (nodeID → addr) cache
   keyed by node identity. Promotion does not change the peer entry —
   only `ConfState.Voters` / `ConfState.Learners` changes.
2. `configurationFromConfState` walks both `conf.Voters` and
   `conf.Learners` and emits the right `Suffrage` string for each. This
   becomes the single source of truth for suffrage; `upsertPeer` stops
   stamping a hard-coded `Suffrage: "voter"`.
3. `validateConfState` (wal_store.go:406) accepts learner-only entries:
   `len(conf.Learners) > 0` is no longer rejected. Joint-consensus markers
   (`VotersOutgoing`, `LearnersNext`, `AutoLeave`) stay rejected — learner
   add/promote uses the simple V1 path which never sets them.

`nextPeersAfterConfigChange` already routes `ConfChangeAddLearnerNode` via
`applyAddedPeerToMap`, so the snapshot/restart code path is correct
once the live `applyConfigPeerChange` matches it.

### 4.3 Persisted peers file

The existing `etcd-raft-peers.bin` is version 1 and encodes
`(NodeID, ID, Address)` per peer. Suffrage is not stored. The authoritative
suffrage view lives in the raft snapshot's `ConfState`, which is already
persisted via `persistConfigSnapshot`, so in principle we could leave the
peers file unchanged. However:

- The peers file is consulted on `Open` to resolve addresses for peers that
  appear in `ConfState` but not in the operator-supplied peer list.
- A learner that restarts before its first snapshot lands would see a
  `ConfState` from the bootstrap snapshot that lists it as voter (in the
  cold-bootstrap case) or as learner (in the join-as-learner case). In the
  join case there is no in-memory hint about its own role until the
  snapshot is replayed.
- Future tooling — `raftadmin configuration` printing local node state,
  monitoring labels — will want a single read of "what am I" that does
  not require synthesizing it from `ConfState` plus the local node ID.

Therefore: bump the peers file format to **version 2**. Layout:

```text
magic   : "EKVP"
version : u32 = 2
index   : u64               // unchanged
count   : u32               // unchanged
peers   : count × {
    nodeID   : u64
    suffrage : u8           // 0 = voter, 1 = learner. NEW.
    id       : len-prefixed string
    address  : len-prefixed string
}
```

Compatibility:

- `readPersistedPeersFile` keeps the v1 reader. A v1 file is treated as
  all-voter, which is exactly the current world. Upgrades therefore do not
  require a migration step.
- Writers always emit v2. A downgrade to a build that only knows v1 reads
  the v2 file as a header-version mismatch and rejects it; that is
  acceptable because we already require explicit offline migration tooling
  for engine downgrades (Phase 5 of the etcd-migration doc).

The `ConfState` carried inside the local raft snapshot remains the
*authoritative* source of suffrage. The peers file is a cache plus an
operator-readable summary; on conflict, the snapshot's `ConfState` wins,
and `persistConfigState` overwrites the peers file accordingly.

### 4.4 RPC and CLI surface

Proto additions (`proto/service.proto`):

```protobuf
service RaftAdmin {
  // ... existing RPCs ...
  rpc AddLearner(RaftAdminAddLearnerRequest) returns (RaftAdminConfigurationChangeResponse) {}
  rpc PromoteLearner(RaftAdminPromoteLearnerRequest) returns (RaftAdminConfigurationChangeResponse) {}
}

message RaftAdminAddLearnerRequest {
  string id = 1;
  string address = 2;
  uint64 previous_index = 3;
}

message RaftAdminPromoteLearnerRequest {
  string id = 1;
  uint64 previous_index = 2;
  uint64 min_applied_index = 3; // 0 = skip precondition
}
```

`internal/raftadmin/server.go` wires them through `s.admin`. The two
existing wiring helpers (`adminError`, the `Unimplemented` guard for
`s.admin == nil`) are reused unchanged.

`cmd/raftadmin` subcommands:

```text
raftadmin <addr> add_learner <id> <address> [previous_index]
raftadmin <addr> promote_learner <id> [previous_index] [min_applied_index]
```

The existing `configuration` subcommand already prints the per-server
`suffrage` field (`cmd/raftadmin/main.go:235`), so once the engine emits
`"learner"` for learners the operator-visible output is correct without
further changes.

### 4.5 Bootstrap and join-as-learner

Cold bootstrap (`--raftBootstrap` plus optionally `--raftBootstrapMembers`):

- The bootstrap member list is voters-only. A learner cannot start an
  election, and the cold-bootstrap flow expects the local node to win an
  uncontested election to commit the first config snapshot. We reject
  attempts to bootstrap with a learner-only member list with
  `errNoVotersConfigured` — symmetric to `errNoPeersConfigured` for the
  empty case.

Joining an existing cluster as a learner:

- New CLI flag (per group): `--raftJoinAsLearner` (default `false`). When
  set, the local node refuses to self-bootstrap and refuses to be added
  with `AddVoter` from outside; it expects an operator to call
  `AddLearner` against the existing leader before it appears in
  `ConfState`.
- Operationally:
  1. Operator brings up the joiner with `--raftJoinAsLearner` and the
     peer list it knows about (existing voters; the joiner's own
     `--raftId` is the local node).
  2. The joiner starts but stays in follower-no-config until the leader
     fans out the new `ConfState` via `MsgApp` / `MsgSnap`.
  3. Operator calls `raftadmin <leader> add_learner <id> <address>`.
  4. Once the joiner is caught up (operator polls `Status.AppliedIndex`
     against the leader's `CommitIndex`), operator calls
     `promote_learner` with `min_applied_index` set to a recent leader
     commit index. The engine re-verifies against
     `Progress[nodeID].Match` before proposing.
- This flow does **not** require a new `--raftBootstrapMembers` syntax.
  The joiner does not appear in any cold-bootstrap member list; it only
  enters the cluster's `ConfState` via the explicit `add_learner` RPC.

### 4.6 Quorum, lease reads, and the ack tracker

The lease-read fast path
(`LeaseProvider` / `LastQuorumAck` / `quorumAckTracker`) must not require
acknowledgement from learners. Specifically:

- `followerQuorumForClusterSize` currently takes the total *cluster size*
  (`len(e.peers)`) to compute a follower-quorum denominator. After this
  change, that count includes learners, which would inflate the
  denominator and stall lease reads whenever a learner is slow.
- The engine must compute the lease-read denominator from
  `len(ConfState.Voters)` (excluding self if leader), not `len(e.peers)`.
  `recordQuorumAck` already gates on `isLeader`; we additionally drop
  responses from peers whose `nodeID` is in `ConfState.Learners` so a
  learner heartbeat ack cannot count toward voter majority.

This is the **highest-risk** area of this proposal and gets its own
behaviour test (see §5.2). The wrong fix here would silently regress
lease-read latency the moment any cluster has a single learner attached.

### 4.7 What does *not* change

To keep the milestones reviewable, the following surfaces stay exactly
where they are:

- KV / adapter code (`kv/`, `adapter/`). `LeaderProxy`, `LeaseProvider`,
  `LinearizableRead`, and `ShardedCoordinator` already gate fast paths on
  `state == StateLeader`. A learner falls through to slow paths
  automatically.
- Route catalog (`distribution/`). Routing decisions are independent of
  raft suffrage; the route catalog points at a Raft group, and the group
  internally decides leadership.
- Snapshot send/receive (`snapshot_spool.go`, the snapshot dispatch
  lane). Snapshot sending to a slow joiner already works for voters; a
  learner consumes the same code path.
- WAL purge (`wal_purge.go`). Already bounded by per-peer `Match` indexes
  in the leader's progress tracker; the tracker includes learners by
  construction in etcd/raft.
- HLC issuance. Learners do not issue persistence timestamps (they cannot
  — they are never leader). No change to `kv/hlc.go` is required.

## 5. Self-review of the design (five passes)

Per CLAUDE.md §"Self-review of code changes", spelled out at design time
because each of these passes informs the milestone breakdown.

### 5.1 Data loss

- A learner cannot lose committed data because it never *commits*: it
  only replays the leader's committed entries. Its FSM lags or matches
  the leader, never diverges.
- Promotion does **not** decrement the voter count at any point. It is a
  V1 `ConfChangeAddNode` that increments `Voters` by one in the resulting
  `ConfState`. Quorum size after promotion is `floor((N+1)/2)+1`, which
  is at least the pre-promotion quorum, so there is no transient window
  where a single voter failure can stall the cluster on the promote
  step.
- WAL purge: the leader's `Progress` map already includes learners
  (they receive `MsgApp`), so the existing per-peer `Match` floor that
  bounds purge cannot truncate entries the learner still needs.
  Verification: a unit test that forces a snapshot + purge while a
  learner is mid-catch-up.
- Persisted peers file forward-compat: writers always emit v2; readers
  accept v1 (treat as all-voter) and v2. A v1→v2 upgrade is therefore a
  read-once-write-once transition; we do not silently "migrate" until
  the next conf change forces a `persistConfigState` write.

### 5.2 Concurrency / distributed failures

- Add and promote run on the engine's single-threaded admin loop, the
  same loop as `AddVoter` / `RemoveServer`. The same `prevIndex`
  precondition prevents a stale operator from racing two reconfigurations.
- Promote-during-leader-change: the new leader's `Progress` map starts
  fresh. The `min_applied_index` precondition will reject promotion until
  the new leader has observed enough catch-up traffic from the learner;
  this is the desired behaviour, not a bug.
- Quorum-ack tracker race: `recordQuorumAck` is gated by `isLeader`; the
  step-down path (`leaderLossCbs`) already invalidates the tracker. The
  new gate ("drop responses from learner peers") reads `ConfState`
  through the same lock as the existing `peers` map, so it does not
  introduce a new race.
- Removing a learner mid-promote: etcd/raft processes conf changes
  serially — the second proposal stays in the log behind the first and
  is applied or rejected deterministically. `pendingConfigs` already
  tracks one outstanding change at a time per request ID.
- Partition + heal: a learner that is partitioned from the leader simply
  falls behind. On heal, the leader resumes `MsgApp` / `MsgSnap`; no
  divergence is possible because the learner cannot have been the
  authority on any committed entry.

### 5.3 Performance

- Adding a learner adds outbound replication traffic (one extra peer)
  but no extra Raft round-trips on the write path: the learner does not
  vote, and after §4.6 it does not gate the lease-read denominator
  either. Hot-path cost on the write path: O(1) extra `MsgApp` per
  Ready iteration, dispatched on the existing per-peer queue.
- Snapshot-on-join: identical to the existing voter-snapshot path. Same
  4-lane dispatcher behaviour, same throttling.
- The `quorumAckTracker` denominator change is O(voters), recomputed
  only on conf change, not on each ack — same cadence as today.
- Persisted peers file v2 is one extra byte per peer; the file is
  bounded at `maxPersistedPeers = 1024`, so the cost is negligible.
- No new metric-cardinality blow-up. The `suffrage` label appears on
  per-peer monitoring labels (`monitoring/raft.go:355`) which is already
  bounded by cluster size.

### 5.4 Data consistency

- A learner's local FSM may lag. It MUST NOT serve linearizable reads
  via the lease fast path. This proposal does not change adapter
  routing; the existing `state == StateLeader` gate already enforces
  this. We add an explicit unit test on `LeaseProvider`:
  `LastQuorumAck()` returns the zero `Instant` on a learner regardless
  of cluster ack traffic.
- `LinearizableRead` on a learner: the current implementation forwards
  to the leader's `ReadIndex` and waits for local apply. Behaviour on a
  learner is identical to behaviour on a slow follower — local apply
  may take longer, but staleness bound is the same. We add a behaviour
  test that exercises this.
- HLC: learners never issue persistence timestamps. The
  `Coordinator.HLC().Next()` call site is leader-gated already; a
  learner that somehow got asked for a timestamp would be rejected by
  `VerifyLeader` upstream.
- OCC: irrelevant. Learners do not commit transactions.
- MVCC visibility: a learner's local snapshot reads at HLC ts T are
  consistent with the leader-issued `T` only after the learner has
  applied up to the index that committed `T`. Today's `LinearizableRead`
  fence enforces that already; we just inherit it.

### 5.5 Test coverage

- Unit tests for `AddLearner`, `PromoteLearner`, including:
  - happy path (add → catch up → promote → vote)
  - promote without prior add → `FailedPrecondition`
  - promote with `min_applied_index` ahead of `Progress.Match` →
    `FailedPrecondition`
  - leader change between add and promote
  - remove a learner directly via `RemoveServer`
- Conformance test (`internal/raftengine/etcd/conformance_test.go`):
  `Configuration().Servers[i].Suffrage` reports `"learner"` for
  learners and `"voter"` for voters, including after restart.
- Persisted peers v1→v2 round-trip test.
- `quorumAckTracker` regression test: lease reads must not block on
  learner ack.
- `WAL purge` test with a learner mid-catch-up (data-loss pass §5.1).
- Behaviour test: `LinearizableRead` on a learner forwards to leader's
  `ReadIndex` and returns once local apply catches up.
- Property test for the conf-change context codec is unchanged
  (`encodeConfChangeContext` already round-trips peer metadata for the
  V1 conf-change types we use).

A multi-node Jepsen-style test that exercises learner attach + promote
during partition is deferred to Milestone 3 because it requires a
parameterised cluster harness in the existing Jepsen workloads.

## 6. Milestones

### Milestone 1 — Engine-level learner support (no operator surface yet)

- `Admin` interface: add `AddLearner`, `PromoteLearner`.
- etcd backend: live `applyConfigPeerChange` accepts
  `ConfChangeAddLearnerNode`; `configurationFromConfState` reports
  `"learner"`; `upsertPeer` stops hard-coding `"voter"`.
- `validateConfState` accepts learner-only entries (joint-consensus
  markers stay rejected).
- `quorumAckTracker` denominator + admission audit (§4.6).
- Persisted peers file v2.
- Unit + conformance tests (§5.5 first three bullets).
- No proto / CLI / flag changes yet. Promote / add are reachable via
  the engine API and engine tests only.

Exit criteria: a unit test that adds a learner to a 3-node cluster,
sees suffrage reported correctly across restarts, and promotes it.

### Milestone 2 — Operator surface

- Proto: `AddLearner`, `PromoteLearner` RPCs and request messages.
- `internal/raftadmin/server.go` wiring.
- `cmd/raftadmin` subcommands.
- `--raftJoinAsLearner` flag in `main.go`.
- Operator runbook update in `docs/etcd_raft_migration_operations.md`
  (or a sibling runbook) covering attach-as-learner / promote / verify
  via `raftadmin configuration`.
- Behaviour test: `LinearizableRead` on a learner forwards correctly.

Exit criteria: a manual operator workflow that brings up a
single-process 3-node demo cluster, attaches a learner via
`raftadmin add_learner`, and promotes it.

### Milestone 3 — Hardening

- Jepsen workload that exercises learner attach during partition and
  promote after heal.
- Monitoring: `suffrage` label on per-peer Prometheus labels (already
  exists in `monitoring/raft.go:355`; verify it survives the engine
  changes).
- Promote precondition: surface `Progress[nodeID].Match` in
  `Status.PerPeer` so an operator can choose `min_applied_index`
  without guessing.
- Decision gate for follower-served reads: write a separate proposal,
  do not extend this one.

After Milestone 3, rename
`docs/design/2026_04_26_proposed_raft_learner.md` →
`docs/design/2026_04_26_implemented_raft_learner.md` (or
`*_partial_*` if Milestone 3 slips behind Milestone 2 shipping).

## 7. Risks

1. **`quorumAckTracker` denominator regression.** If the
   voter/learner gate in §4.6 is wrong, lease reads silently stall when
   a learner is slow. Mitigation: explicit unit test, then an
   integration test that injects learner-side `MsgHeartbeatResp` delay
   and asserts lease-read latency is unaffected.
2. **Persisted peers v1→v2 mis-handling on downgrade.** A v2 file read
   by a v1 reader fails closed (header version mismatch), which is the
   safe outcome but breaks rollback. Mitigation: document this in the
   Phase-5 rollout playbook of the etcd-migration doc; require explicit
   offline tooling for downgrade, same as the existing engine-marker
   policy in `multiraft_runtime.go`.
3. **Operator promotes a not-yet-caught-up learner.** Mitigation:
   `min_applied_index` precondition checked against
   `Progress[nodeID].Match` on the leader. Default `0` skips the check
   for compatibility with scripts that don't set it; the runbook
   strongly recommends a non-zero value.
4. **Learner cannot become leader by accident.** etcd/raft already
   enforces this; we inherit the property. The rejection path in
   `handleTransferLeadership` (`engine.go:1387`) already documents it.
5. **Cold bootstrap with a learner-only member list.** Reject explicitly
   with a typed error rather than relying on the underlying library to
   refuse. Otherwise an operator who specifies a learner-only bootstrap
   list could see the cluster wedge waiting for a leader that can never
   be elected.

## 8. Open questions

1. Should `RemoveServer` distinguish between removing a voter and
   removing a learner at the API layer, or is the existing single
   method enough? Current proposal: single method, since etcd/raft uses
   `ConfChangeRemoveNode` for both. (Operators reading
   `raftadmin configuration` already see the suffrage of the target.)
2. Should `min_applied_index` be expressed as an absolute index or as a
   tolerance behind the leader's commit index ("within N entries")? The
   absolute form is simpler to validate; the tolerance form is easier
   for an operator to choose. Current proposal: absolute, with a
   helper in the runbook that computes "current leader commit minus N".
3. Do we want a `--raftBootstrapMembers` syntax extension to mark
   bootstrap members as learners? Current proposal: **no**. Cold
   bootstrap is voter-only; learners enter exclusively via
   `AddLearner` against an existing leader. This keeps the bootstrap
   path narrow and avoids a footgun where someone bootstraps a
   learner-majority cluster.
4. Future work: should `LinearizableRead` on a learner be served
   locally with `ReadIndex` + apply-wait, instead of forwarded? That
   is the follower-served-reads question and explicitly out of scope
   here.

## 9. Decision criteria

Proceed with Milestone 1 if at least one of the following is true:

- An operator workflow needs to attach a catch-up replica without
  shrinking voter fault tolerance during catch-up. (Always true for
  any production-shaped use of elastickv.)
- A planned subsystem — hotspot shard split, workload isolation,
  follower-served reads — needs a non-voting replica primitive.

Otherwise the cost of carrying the persisted-peers v2 format change
without a consumer is non-trivial and we should defer.
