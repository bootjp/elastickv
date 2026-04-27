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
(the `Admin` interface in `internal/raftengine/engine.go`). That has three concrete consequences:

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
  hard-coded to `"voter"` in two places (`configurationFromConfState` and
  `upsertPeer` in `internal/raftengine/etcd/engine.go`, where the helper
  emits `Suffrage: "voter"` for every server).
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

The engine *partially* handles learners and *fully* refuses to expose them.
Line numbers below reflect `main` at the propose date and will drift; the
function names and the small excerpts are the stable references.

| Location                                               | Behaviour                                                                                                                                                                              |
|--------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `raftengine.Admin` (`internal/raftengine/engine.go`)   | No `AddLearner` or `PromoteLearner`. `Server.Suffrage` exists but no caller produces `"learner"`.                                                                                       |
| `Engine.applyConfigPeerChange` (etcd backend)          | Live path ignores `ConfChangeAddLearnerNode`. Comment: "Phase 3 only exposes voter membership changes."                                                                                  |
| `applyConfigPeerChangeToMap` (etcd backend)            | Map-form path *does* keep learner peer metadata for log-replay round-trip; live engine map does not.                                                                                     |
| `configurationFromConfState` (etcd backend)            | Only walks `ConfState.Voters`; never emits a server with `Suffrage: "learner"`.                                                                                                          |
| `Engine.upsertPeer` (etcd backend)                     | Hard-codes `raftengine.Server{..., Suffrage: "voter"}` for `e.config.Servers`.                                                                                                            |
| `validateConfState` (`wal_store.go`)                   | Rejects any `ConfState` with non-empty `Learners` / `LearnersNext` / `VotersOutgoing` as "joint consensus state is not supported".                                                       |
| `confStateForPeers` (`peers.go`)                       | Emits every `Peer` as a voter; no per-peer suffrage on the `Peer` struct.                                                                                                                 |
| `peer_metadata.go`                                     | Persisted peers file (`etcd-raft-peers.bin`, version 1) stores `(NodeID, ID, Address)`. No suffrage byte.                                                                                  |
| `proto/service.proto`                                  | `RaftAdmin` exposes `AddVoter`, `RemoveServer`, `TransferLeadership`. No `AddLearner` or `PromoteLearner`. `RaftAdminMember.suffrage` exists.                                            |
| `cmd/raftadmin/main.go`                                | Subcommands: `add_voter`, `remove_server`, `leadership_transfer`, `leadership_transfer_to_server`.                                                                                       |

The dormant pieces (`applyConfigPeerChangeToMap`, the `Suffrage` field on
`Server`, the `suffrage` field on `RaftAdminMember`, the regression test
`TestNextPeersAfterConfigChangeKeepsLearnerMetadata`) all suggest the original
etcd-migration design expected this exact follow-up.

### 3.3 `len(e.peers)` is overloaded as "voter count"

A grep of the etcd backend reveals four hot-path call sites where
`len(e.peers)` is currently used as a proxy for the voter denominator. Each
breaks the moment a learner is added to `e.peers`:

| Call site                            | What `len(e.peers)` means today                          | What it should mean post-learner |
|--------------------------------------|----------------------------------------------------------|-----------------------------------|
| `recordQuorumAck`                    | Total cluster size (incl. self) for `followerQuorumForClusterSize`. | Number of **voters** (incl. self). |
| `refreshStatus` single-node fast path | `clusterSize <= 1` → leader-of-1 → instant lease ack.   | Voter-count `<= 1` → leader-of-1-voter → instant lease ack. |
| `removePeer` post-removal cluster size | Used to recompute `followerQuorum` after a removal.    | Post-removal voter count.         |
| `nextPeersAfterConfigChange*` sizing | Address-cache fan-out only — already correct, included for completeness. | Unchanged. |

§4.6 enumerates the fix and §5.2 audits each site for races.

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
  learner. Both preconditions below run on the leader, on the
  single-threaded admin loop (`handleAdmin`), **before** calling
  `rawNode.ProposeConfChange`. The check intentionally cannot be lazy:
  if the propose is queued first and the precondition then fails, the
  conf-change entry has already entered the log and a `RemoveServer`
  rollback would be required. So:
  1. The peer exists in `ConfState.Learners` (not in `Voters`). Otherwise
     return a `FailedPrecondition` so callers do not silently no-op.
  2. The leader's `Progress[nodeID].Match >= minAppliedIndex`. The leader
     is the authority on follower progress; an operator who reads
     `Status.AppliedIndex` from the learner itself sees a slightly stale
     view, so the engine re-checks against the leader's tracker before
     proposing. The `Progress` map is read directly from `rawNode.Status()`
     inside the admin handler — same goroutine that owns the rawNode —
     so no lock or clone is required.
  3. `minAppliedIndex == 0` is **discouraged**: it skips the
     precondition. We accept the convention to stay consistent with the
     existing `prevIndex` ("0 means don't check the config index") on
     the same RPC family, but the runbook calls it out as a footgun and
     §8 lists "should we use a separate `skip_min_applied_check` boolean
     instead" as an open question.
- `RemoveServer` is unchanged. etcd/raft handles `ConfChangeRemoveNode` for
  a learner correctly, and the existing path already calls `peerForID` /
  `removePeer`.

### 4.2 etcd backend internals

Four concrete edits, each tightly scoped. Edits 1–3 are the "turn on
learner support" core; edit 4 is the ownership cleanup the reviewer
flagged as a structural prerequisite.

1. `applyConfigPeerChange` stops dropping `ConfChangeAddLearnerNode`.
   The handler routes to `applyAddedPeer` (same as `ConfChangeAddNode`),
   and the *suffrage* of the peer is no longer carried in the engine's
   `peers` map but inferred at read time from `ConfState`. The `peers`
   map remains a (nodeID → addr) address cache keyed by node identity.
   Promotion does not change the peer entry — only `ConfState.Voters` /
   `ConfState.Learners` changes.
2. `configurationFromConfState` walks both `conf.Voters` and
   `conf.Learners` and emits the right `Suffrage` string for each. This
   becomes the single source of truth for suffrage that gets surfaced
   to callers (`Configuration()`, monitoring labels).
3. `validateConfState` (`wal_store.go`) accepts learner-only entries:
   `len(conf.Learners) > 0` is no longer rejected. Joint-consensus
   markers (`VotersOutgoing`, `LearnersNext`, `AutoLeave`) stay rejected
   — learner add/promote uses the simple V1 path which never sets them.

   The voter-count comparison inside `validateConfState`
   (`len(conf.Voters) != len(expected.Voters)`) currently builds
   `expected` via `confStateForPeers(peers)`, which emits **every**
   `Peer` as a voter. Once a learner has been persisted to the v2 peers
   file, the peers slice passed in by `validateOpenPeers` is mixed
   voter + learner. Without a fix, `expected.Voters` would be longer
   than `conf.Voters` and the node would refuse to restart with
   `errClusterMismatch`. Two valid resolutions, both kept inside
   `wal_store.go`:

   - **(a) Filter at the comparison.** `validateConfState` builds its
     own `expected` voter set from `peers` filtered by
     `Suffrage != "learner"`. The voter check stays element-wise
     (matching the existing pattern, which depends on the
     well-established voter ordering invariant: `persistConfigState`
     writes voters in `ConfState.Voters` order and `normalizePeers`
     preserves it). The **learner check is set-based**. As an error
     condition (return `errClusterMismatch` when true):

     ```go
     len(conf.Learners) != filteredLearnerCount ||
         !allLearnersPresent(conf.Learners, learnerSet)
     ```

     Equivalently, the valid condition:

     ```go
     len(conf.Learners) == filteredLearnerCount &&
         allLearnersPresent(conf.Learners, learnerSet)
     ```

     A set comparison avoids pinning a new ordering contract on the
     v2 writer for learners — there is no prior convention since
     learners have never been persisted before, and the set form is
     strictly more robust against future writer reordering. Use `||`
     (not `&&`) so that **same-count member divergence** (e.g.
     `conf.Learners = [A, B]` vs peers `[A, C]`) and **conf-learner
     missing from peers** are both rejected; an `&&` would silently
     accept them.
   - **(b) Add a sibling helper.** Introduce
     `confStateWithLearners(peers []Peer) raftpb.ConfState` used only
     by `validateConfState`; leave `confStateForPeers` voters-only as
     §4.3 documents.

   Resolution (a) is preferred: it keeps the cold-bootstrap helper
   minimal and isolates the validation-time peer-filtering logic next
   to the comparison that needs it.
4. **`upsertPeer` stops owning `e.config.Servers`.** Today it does two
   jobs: refresh the address cache `e.peers`, and stamp a
   `raftengine.Server{Suffrage: "voter"}` into `e.config.Servers`. Once
   learners exist, the second job is wrong — `upsertPeer` has no
   `ConfState` at call time, so it cannot decide the correct suffrage.
   The fix: `upsertPeer` keeps `e.peers` only. `e.config.Servers`,
   `e.voterCount`, and `e.isLearnerNode` are all recomputed from the
   post-change `ConfState` inside `applyConfigChange` /
   `applyConfigChangeV2`. Symmetric change in `removePeer`: it stops
   splicing `e.config.Servers` directly.

   To make the post-change `ConfState` reachable from those functions,
   **extend their signatures** to accept `confState raftpb.ConfState`:

   ```go
   func (e *Engine) applyConfigChange(changeType raftpb.ConfChangeType,
                                       nodeID uint64, context []byte,
                                       index uint64,
                                       confState raftpb.ConfState)

   func (e *Engine) applyConfigChangeV2(cc raftpb.ConfChangeV2,
                                         index uint64,
                                         confState raftpb.ConfState)
   ```

   This mirrors `nextPeersAfterConfigChange(_, _, _, conf raftpb.ConfState)`
   which already takes `ConfState` for the same reason. The call sites
   in the apply loop already capture the return value of
   `rawNode.ApplyConfChange(...)`, so wiring the parameter is a
   one-line change at each call site (no new mutex, no new state).

   The two suffrage-aware updates inside the body have **different
   ordering relative to the per-peer cleanup helpers**:

   1. **`e.voterCount` and `e.isLearnerNode` are updated *before*
      `upsertPeer` / `removePeer`.** This is what §4.6 and §5.2
      require: `removePeer` reads `e.voterCount` to compute the
      post-removal ack-tracker threshold, so it must see the
      post-change count, not the pre-change count.
   2. **`e.config.Servers` is rebuilt *after* `upsertPeer` /
      `removePeer`** via `configurationFromConfState(e.peers, confState)`.
      This rebuild reads the address cache `e.peers`, so it has to
      observe the post-cleanup map.

   The full sequence inside `applyConfigChange` /
   `applyConfigChangeV2` is therefore:

   ```text
   1. recompute e.voterCount, e.isLearnerNode from confState
   2. upsertPeer / removePeer  (mutates e.peers, may read e.voterCount)
   3. rebuild e.config.Servers via configurationFromConfState(e.peers, confState)
   4. resolveConfigChange / configIndex bookkeeping (existing)
   ```

   All four steps run on the single-threaded apply loop, so no extra
   synchronization is needed.

`nextPeersAfterConfigChange` already routes `ConfChangeAddLearnerNode`
via `applyAddedPeerToMap`, so the snapshot/restart code path is correct
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

#### Authoritative source of suffrage during recovery

The `ConfState` carried inside the local raft snapshot, plus any
`ConfChange` entries replayed from the WAL on top of that snapshot, is
the **authoritative** source of suffrage. The peers file is an address
cache plus an operator-readable summary, with one important asymmetry:
`validateOpenPeers` already short-circuits when
`persisted.Index > snapshot.Metadata.Index` because the persisted file
can be more current than the local snapshot. That asymmetry is fine for
addresses but not for suffrage, because between the snapshot and the
persisted-peers index there may be `ConfChangeAddLearnerNode` and
`ConfChangeAddNode`-as-promote entries that change a peer's role.

Recovery is therefore explicitly two-phase:

1. **Open phase** — load addresses from the v2 peers file (or the v1
   file, treated as all-voter). The engine boots with that as the
   address cache. The suffrage byte from the file is used only for the
   bootstrap-time `confStateForPeers` call (see next bullet).
2. **WAL-replay phase** — after `rawNode.Ready()` drains the snapshot
   and replays log entries up to the persisted commit index, the engine
   re-derives the peer-set view from the resulting `ConfState`:
   `e.config.Servers` is rebuilt by `configurationFromConfState`
   (edit 4 of §4.2). The peers-file suffrage byte is **not** read
   again after this point. The next `persistConfigState` call writes
   the post-replay state back to the v2 file.

Concretely: after open, the rule is "snapshot+log wins, the peers file
catches up". The reviewer's concern that an in-flight learner-add could
look inconsistent during the gap between snapshot read and WAL replay
is addressed by never *acting* on the peers-file suffrage during that
gap — `validateConfState` runs against the snapshot's `ConfState`
(which always represents a consistent committed state), and the
in-memory `e.config.Servers` is the post-replay view, not the
post-open-phase view.

#### `confStateForPeers` and the `Peer.Suffrage` field

`confStateForPeers` is called only on cold bootstrap, before any
`ConfState` exists, to mint the very first conf state from the operator's
`--raftBootstrapMembers` list. Cold bootstrap is voters-only (§4.5), so
the simplest correct implementation is:

```go
func confStateForPeers(peers []Peer) raftpb.ConfState {
    voters := make([]uint64, 0, len(peers))
    for _, peer := range peers {
        voters = append(voters, peer.NodeID)
    }
    return raftpb.ConfState{Voters: voters}
}
```

— i.e., unchanged. Cold bootstrap with a learner is rejected upstream in
`prepareOpenState` with a typed error (§4.5 / risk #5).

For the **persisted-peers v2 read** path, we still add a
`Peer.Suffrage` field to the in-memory struct so v2 payloads round-trip
cleanly through tests and through the operator-visible
`raftadmin configuration` output during the open phase. But that
field is *not* consulted by `confStateForPeers`. The two writers stay
distinct:

- `confStateForPeers(peers []Peer) raftpb.ConfState` — cold bootstrap,
  voters only, ignores `Peer.Suffrage`.
- `configurationFromConfState(peers map[uint64]Peer, conf raftpb.ConfState) raftengine.Configuration` —
  hot path, reads suffrage from `conf.Voters` / `conf.Learners`,
  ignores `Peer.Suffrage`.

`Peer.Suffrage` is therefore a v2-file artifact only. This keeps the
`ConfState` as the single source of suffrage truth and avoids the
"peers file says learner, ConfState says voter" divergence the reviewer
flagged.

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

- New CLI flag (per group): `--raftJoinAsLearner` (default `false`).
- **Enforcement is local and post-apply, not pre-propose.** The flag
  does *not* attempt to block the leader from issuing a
  `ConfChangeAddNode` for this node. Doing so would either require a
  network-level vetoing protocol (no such mechanism exists in the
  codebase) or rejecting an `Apply` mid-stream (catastrophic — diverges
  the log). Instead, after each conf-change `Apply`, the joining node
  inspects the resulting `ConfState`:
  1. If `--raftJoinAsLearner=true` and the local node ID appears in
     `ConfState.Voters` (and not in `ConfState.Learners`), the engine
     emits an ERROR-level structured log
     (`elastickv_raft_join_role_violation_total`) **and the node
     continues running** as a voter. We do not shut down: by the time
     the conf change has applied, the local node already counts toward
     quorum, and an unilateral shutdown would shrink fault tolerance.
     The operator is responsible for either (a) calling
     `RemoveServer` against the leader followed by re-adding via
     `AddLearner`, or (b) acknowledging the deviation and clearing the
     flag.
  2. If `--raftJoinAsLearner=true` and the local node ID is in
     `ConfState.Learners`, normal operation continues.
  3. The flag is therefore primarily an **operator alarm**, not a
     consensus-level enforcement. The runbook makes this explicit.
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
acknowledgement from learners. This is the **highest-risk** area of the
proposal — the wrong fix silently regresses lease-read latency the
moment any cluster has a single learner attached.

#### Voter-count cache

Introduce a single event-loop-owned field `e.voterCount int` (and a
companion `e.isLearnerNode map[uint64]bool` keyed by node ID).

**Initialization at Open time.** `refreshStatus()` runs once during
`Open` before the apply loop processes any entry, so the cache must
have a correct value before that call. Wire it up adjacent to the
existing `e.config` initialization that already reads
`prepared.disk.LocalSnap.Metadata.ConfState`:

```go
// existing
config: configurationFromConfState(peerMap, prepared.disk.LocalSnap.Metadata.ConfState),
// added
voterCount:    len(prepared.disk.LocalSnap.Metadata.ConfState.Voters),
isLearnerNode: learnerSetFromConfState(prepared.disk.LocalSnap.Metadata.ConfState),
```

Without this, a 3-voter cluster boots with `voterCount == 0`, which
would falsely activate the single-node fast path in `refreshStatus`
on the very first tick.

**Steady-state writes.** After Open, both fields are **only written
from the apply loop** in `applyConfigChange` /
`applyConfigChangeV2`, **after** `rawNode.ApplyConfChange(...)`
returns the new `ConfState`. They are derived directly from
`conf.Voters` / `conf.Learners` and **rebuilt from scratch** on each
conf change — not incrementally patched. A patched form
(`e.isLearnerNode[nodeID] = true`) would leak stale `true` entries
after a learner is promoted; a full rebuild from `conf.Learners` is
the only safe form. This piggybacks on the existing single-writer
invariant for the apply loop: no extra mutex needed.

Readers on the lease hot path read these fields under the same
`e.mu` they already take for `e.peers`, so the critical section
grows by one int load and one map lookup.

#### Call-site fixes (all four `len(e.peers)` overloads)

1. **`recordQuorumAck`** (event-loop goroutine, single writer):
   - Replace `clusterSize := len(e.peers)` with
     `clusterSize := e.voterCount`.
   - Reject the ack early when `e.isLearnerNode[msg.From]` is true.
     This is in addition to the existing `e.peers[msg.From]` membership
     check; a learner peer is in `e.peers` but must not contribute to
     the voter ack tracker.
   - The early `clusterSize <= 1` short-circuit stays — it now
     correctly means "no other voter to ack from" rather than "no
     other peer at all".

2. **`refreshStatus` single-node fast path**:
   - Replace `clusterSize := len(e.peers)` with the cached
     `e.voterCount`. The leader-of-1 fast path
     (`singleNodeLeaderAckMonoNs`) now triggers when the leader is the
     **only voter** regardless of how many learners are attached. This
     is the single critical fix for the 1-voter+1-learner stall the
     reviewer flagged.

3. **`removePeer` post-removal cluster size**:
   - Today it computes `postRemovalClusterSize := len(e.peers)` after
     deleting the peer and feeds that into
     `followerQuorumForClusterSize`. After the fix, the apply loop
     updates `e.voterCount` from the post-change `ConfState` *before*
     `removePeer` is called for any address-cache cleanup, so
     `removePeer` simply reads `e.voterCount`. (Removing a learner
     does not change `e.voterCount`; removing a voter decrements it
     by one.)

4. **`nextPeersAfterConfigChange*`** — already correct (it walks
   addresses, not voters). Listed only to confirm we audited it.

#### Why an event-loop-owned cache instead of "read `ConfState` on each call"

The lease-read fast path is intentionally lock-free up to a single
`e.mu` acquisition. Re-deriving voter count from `rawNode.Status()` on
each ack would either (a) require acquiring the rawNode-internal mutex
on a hot path, or (b) clone the `Progress` map per call. Caching the
scalar voter count and the per-peer learner bitset keeps both hot paths
(`recordQuorumAck`, `refreshStatus`) at O(1) cost and ties the cache
update to the natural sequencing point — the apply loop, where the new
`ConfState` is unambiguously known.

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
  new gate ("drop responses from learner peers") reads
  `e.isLearnerNode` (§4.6 voter-count cache), which is written only on
  the single-threaded apply loop and read under the same `e.mu` already
  held for the `e.peers` membership check. No new race.
- `e.voterCount` ordering: the apply loop updates `e.voterCount` and
  `e.isLearnerNode` *before* invoking the per-peer cleanup helpers
  (`upsertPeer`, `removePeer`), so any subsequent `recordQuorumAck` on
  the same loop iteration sees the post-change view. `refreshStatus`
  runs after `drainReady` and likewise sees the post-change view.
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
  learner ack. Specific cases: (a) 1-voter + 1-learner cluster lease
  reads stay on the single-node fast path; (b) 3-voter + 1-learner
  cluster lease reads use a 3-voter denominator, not 4.
- `WAL purge` test with a learner mid-catch-up (data-loss pass §5.1).
- Snapshot-during-learner-catch-up test: trigger a new FSM snapshot
  while a learner is still receiving the previous snapshot. Verifies
  `snapshot_spool.go`'s dedup/cancel logic does the right thing for a
  slow learner — the catch-up replica is exactly the slow consumer the
  spool was designed to handle, and it is the most likely place for a
  regression to land.
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
- etcd backend:
  - Live `applyConfigPeerChange` accepts `ConfChangeAddLearnerNode`.
  - `configurationFromConfState` reports `"learner"`.
  - `upsertPeer` no longer writes to `e.config.Servers`; the apply
    loop rebuilds `e.config.Servers` from the post-change `ConfState`
    (§4.2 edit 4).
  - `validateConfState` accepts learner-only entries (joint-consensus
    markers stay rejected).
- **All four `len(e.peers)` overloads from §4.6 fixed in one PR**
  (`recordQuorumAck`, `refreshStatus`, `removePeer`, plus the audit
  pass on `nextPeersAfterConfigChange*`). Voter-count cache
  (`e.voterCount`, `e.isLearnerNode`) wired through the apply loop.
- Persisted peers file v2 with the `Peer.Suffrage` field; v1 reader
  preserved for upgrades. `confStateForPeers` stays voters-only;
  `Peer.Suffrage` is a v2-file artifact only (§4.3).
- Two-phase recovery documented in code comments matching §4.3:
  open-phase reads addresses from peers file; WAL-replay phase
  rebuilds `e.config.Servers` and `e.voterCount` from `ConfState`.
- Unit + conformance tests (§5.5 first four bullets, including both
  lease-read regression cases — 1-voter+1-learner and 3-voter+1-learner).
- No proto / CLI / flag changes yet. Promote / add are reachable via
  the engine API and engine tests only.

Exit criteria:
1. Unit test that adds a learner to a 3-node cluster, sees suffrage
   reported correctly across restarts, and promotes it.
2. Lease-read regression test passes: a 1-voter+1-learner cluster
   serves lease reads at the same latency as a 1-voter cluster
   (the single-node fast path is retained).
3. Lease-read regression test passes: a 3-voter+1-learner cluster
   does not stall lease reads when the learner is partitioned.

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
   `handleTransferLeadership` already documents it (rejects transfer to
   a learner via `errLeadershipTransferRejected`).
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
3. ~~**`min_applied_index = 0` footgun.**~~ **Resolved in M2.** The
   `RaftAdminPromoteLearnerRequest` and the `Admin.PromoteLearner`
   engine API both carry an explicit `skip_min_applied_check`
   boolean. The engine rejects `min_applied_index == 0` with
   `errPromoteLearnerMinAppliedZero` unless the skip flag is also
   set, so a script that omits the catch-up bound gets a clean
   `FailedPrecondition` instead of silently disabling the safety
   check. The `prevIndex == 0` symmetry is preserved (it remains a
   no-op skip — which is benign since `prevIndex` is just a
   sequencing guard, not a safety check).
4. Do we want a `--raftBootstrapMembers` syntax extension to mark
   bootstrap members as learners? Current proposal: **no**. Cold
   bootstrap is voter-only; learners enter exclusively via
   `AddLearner` against an existing leader. This keeps the bootstrap
   path narrow and avoids a footgun where someone bootstraps a
   learner-majority cluster.
5. Future work: should `LinearizableRead` on a learner be served
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
