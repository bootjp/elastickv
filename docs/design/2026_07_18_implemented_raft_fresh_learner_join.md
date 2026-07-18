# Fresh Raft learner join for wiped and replacement nodes

Status: Implemented
Author: bootjp
Date: 2026-07-18

## 1. Incident and root cause

During recovery of the production Raft cluster, nodes `n2` and `n4` were
started with fresh, wiped data directories. Those nodes no longer had any of
the durable local state that identifies an existing Raft member:

- the Raft WAL and snapshots, including the committed `ConfState`;
- `etcd-raft-peers.bin`, which supplies the transport peer inventory on
  restart; and
- the local applied/configuration indexes used by cold-start recovery.

This is intentionally different from a normal restart. A normal restart can
recover membership from disk and must ignore stale bootstrap intent after the
first committed membership change. A wiped node has no authoritative local
membership and cannot infer whether it is a voter, learner, removed member, or
founder of a new cluster.

The existing startup flags did not represent the required state:

1. `--raftJoinAsLearner` expressed operator intent and enabled role-mismatch
   alarms, but supplied no transport peers. With an empty peer list and no
   persisted peers, `normalizePeers` correctly hit the self-bootstrap guard and
   returned `errNoPeersConfigured`. Relaxing that guard would reintroduce the
   split-brain failure where a wiped node silently starts a one-node cluster.
2. `--raftBootstrapMembers` supplied peer addresses, but its presence implies
   bootstrap. Passing it to a replacement creates an initial voter
   configuration instead of joining the surviving cluster. It is therefore not
   a discovery flag and must not be reused for replacement.
3. Copying `etcd-raft-peers.bin` or another node's data directory would confuse
   transport discovery with committed membership and could copy identity or
   state that does not belong to the replacement.

The missing state was a third startup mode: a fresh node needs addresses for
Raft transport, while its initial local `ConfState` must remain empty until the
existing leader commits `AddLearner`.

## 2. Goals

1. Start one wiped or new node against a live single-group cluster without
   self-bootstrapping it.
2. Keep transport discovery separate from both bootstrap membership and
   durable Raft membership.
3. Attach the node as a learner, catch it up, promote it, and then restart it
   using only membership persisted by Raft.
4. Provide a safe same-ID replacement procedure that never runs the old and
   replacement instances concurrently.
5. Make the production rolling-update path express the temporary join mode for
   exactly one node through `RAFT_JOIN_NODE`.
6. Preserve the self-bootstrap guard for every startup that does not explicitly
   request bootstrap or supply join discovery peers.
7. Keep Redis, DynamoDB, S3, and SQS listeners closed until the joiner has
   committed membership, knows a leader, and applied through its observed
   commit index.
8. Persist only peers present in the authoritative `ConfState`, even when the
   join discovery inventory contains a stale extra address.

## 3. Non-goals

1. Recovering a cluster that has lost voter quorum. A join requires a live
   leader that can commit configuration changes.
2. Automatically deciding which member to remove, attaching a learner, or
   promoting it. Membership changes remain explicit Raft admin operations.
3. Recovering or automatically fencing a stale removed node. Durable removal
   tombstones and an authoritative rejoin protocol remain follow-up work.
4. Joining more than one Raft group in one process. The implemented flag is
   single-group only; a future design needs a per-group join inventory.
5. Changing learner read behavior, voter quorum calculation, snapshot format,
   or the existing bootstrap protocols.

## 4. Invariants

1. **Join discovery never bootstraps.** `--raftJoinMembers` may initialize a
   transport peer map, but it must produce `Bootstrap=false`, an empty
   `BootstrapSeed`, and an initially empty `ConfState` on a fresh data dir.
2. **Bootstrap discovery and join discovery are disjoint.** A process cannot
   combine `--raftJoinMembers` with `--raftBootstrap`,
   `--raftBootstrapMembers`, or `--raftGroupPeers`.
3. **The local node is addressable but not yet a member.** The join list must
   contain the local `--raftId` at the local group's listener address. Presence
   in the transport map does not grant voter or learner suffrage.
4. **Only consensus assigns suffrage.** The node becomes a learner only after a
   committed `ConfChangeAddLearnerNode`; it becomes a voter only after a
   committed promotion. `--raftJoinAsLearner` remains an operator alarm, not a
   local consensus veto.
5. **A replacement ID has one live owner.** The old process or VM is fenced
   before its ID is removed and remains fenced while the fresh instance joins.
6. **Wiping is target-local and sequential.** Operators wipe only the selected
   replacement node after removal has committed. No second node is wiped while
   the replacement is incomplete.
7. **Join flags are temporary.** Once the learner membership and promotion are
   durable, the node is redeployed without `--raftJoinMembers` and
   `--raftJoinAsLearner`.
8. **Normal restart remains disk-authoritative.** WAL, snapshot, and persisted
   peer metadata take precedence after the first successful join; bootstrap or
   join input is not a substitute for committed state.
9. **Discovery does not become durable membership.** Snapshot and ConfChange
   persistence filter the address cache to IDs present in `ConfState`; stale
   discovery-only peers cannot cause the next restart to fail membership
   validation.
10. **Public protocols wait for membership readiness.** Raft transport and
    RaftAdmin start first, but user-facing listeners remain closed until the
    local ID is a learner or voter, a leader is known, no ConfChange is pending,
    and `AppliedIndex >= CommitIndex`.

## 5. CLI and configuration validation

The server adds:

```text
--raftJoinMembers "n1=192.168.0.210:50051,n2=192.168.0.211:50051,n3=192.168.0.212:50051"
--raftJoinAsLearner
```

`resolveRuntimeInputs` delegates all peer-mode selection to
`resolveRaftPeerConfig`. When `--raftJoinMembers` is empty, startup follows the
existing `resolveBootstrapConfig` path unchanged. When it is non-empty, startup
fails before opening an engine unless all of these conditions hold:

- `--raftJoinAsLearner` is set;
- exactly one Raft group is configured;
- no explicit or member-list bootstrap flag is set;
- the list parses with the existing `raftID=host:port` member grammar;
- the list contains at least the local node and one remote member;
- the local `--raftId` appears exactly once; and
- its address matches the local Raft group address.

The resolved `raftBootstrapConfig` stores these peers in `joinServers`, not in
`legacyServers` or `groupServers`. Consequently:

- `serversForGroup` and `adminSeed` expose the join peers for transport and
  discovery;
- `anyBootstrapServers` remains false;
- `bootstrapsGroup` remains false; and
- `bootstrapSeedForGroup` returns nil.

This separation prevents the previous `len(servers) > 0` shortcut from turning
join discovery into bootstrap intent.

## 6. Runtime flow

### 6.1 Fresh process startup

For a fresh data directory, the factory receives all join peers in `Peers`, no
bootstrap seed, and `Bootstrap=false`.

1. `Factory.Create` creates a `GRPCTransport` because the join list contains
   more than one peer.
2. `normalizePeers` validates and normalizes the local identity against that
   list. The self-bootstrap guard is satisfied by explicit discovery peers; it
   is not disabled globally.
3. `openDiskState` finds no WAL or legacy state and calls
   `bootstrapNewCluster`.
4. Because there are multiple peers and `Bootstrap=false`,
   `bootstrapNewCluster` selects `joinWalState`, not `bootstrapWalState`.
5. `joinWalState` creates an empty WAL state. The local snapshot has no voters
   or learners, so the initial `ConfState` is empty and the node cannot campaign
   or count toward quorum.
6. The gRPC Raft listener is available, and the peer map can route traffic to
   and from the existing members. The node waits for the live leader to commit
   its membership change.

Only the Raft transport/RaftAdmin listener is available at this stage. Public
protocol listeners are held behind `waitForRaftJoinReady`, so clients cannot
send Redis/SQS/etc. work to an empty-ConfState process and receive a storm of
leader-not-found failures.

Transport peers are an address cache only. They are deliberately a superset of
the empty initial `ConfState`; `validateOpenPeers` permits that state while the
snapshot index or voter set is empty.

### 6.2 Attach, catch up, and promote

The operator sends `add_learner <id> <address>` to the current leader. The
leader commits `ConfChangeAddLearnerNode`, existing members add the peer to
their transport maps, and the joiner observes itself in
`ConfState.Learners`. The learner does not change the voter quorum denominator.

The leader then replicates log entries or a snapshot. Before promotion, the
operator compares leader progress with a chosen non-zero minimum applied index.
`PromoteLearner` rejects a target that is not currently a learner or has not
caught up to that bound. After the promotion conf change commits, the node is
listed in `ConfState.Voters` and participates in quorum.

If a node started with `--raftJoinAsLearner` is instead added directly as a
voter, the post-apply role check emits the existing ERROR-level alarm and
increments `JoinRoleViolationCount`. The process keeps running because stopping
an already committed voter would reduce availability; remediation is an
explicit remove and correct learner rejoin.

## 7. Safe replacement sequence

For replacement of an unhealthy voter while reusing its Raft ID:

1. Verify the surviving cluster has a leader and enough healthy voters to
   commit a membership change.
2. Fence the old instance at the process, VM, and network level. Confirm its
   Raft listener cannot return before proceeding.
3. Run `remove_server <id>` against the leader and wait until
   `configuration` no longer contains the ID.
4. Stop the target container and wipe or archive only the target's Raft data
   directory. Do not wipe another member.
5. Start only the target in join mode. With the rolling-update script:

   ```sh
   ROLLING_ORDER=n2 RAFT_JOIN_NODE=n2 ./scripts/rolling-update.sh
   ```

   The script derives `RAFT_JOIN_MEMBERS` from `NODES`, injects
   `--raftJoinAsLearner --raftJoinMembers ...` only into the selected node, and
   includes the mode in the deployment configuration fingerprint.
6. Run `add_learner`, wait for the configuration to show learner suffrage, and
   wait until replication catches up.
7. Run `promote_learner` with a non-zero catch-up bound and verify the ID is a
   voter.
8. Clear `RAFT_JOIN_NODE` and redeploy that same node once more. This removes
   the temporary join flags and exercises a normal restart from durable
   membership.
9. Verify a stable leader, full expected voter configuration, advancing applied
   indexes, and a committed application write before replacing another node.

`scripts/rolling-update.sh` rejects `RAFT_JOIN_NODE` when it names an unknown
ID, when `ROLLING_ORDER` contains any other node, or when
`RAFT_BOOTSTRAP_MEMBERS` is also set. These checks make the one-target nature of
the recovery explicit.

## 8. Persistence and restart

The temporary join list is sufficient only until consensus state arrives.
Durability then follows the existing configuration-change path:

1. Applying `AddLearner` updates the peer address cache and derives suffrage
   from the post-change `ConfState`.
2. `persistConfigState` writes the current peer inventory to
   `etcd-raft-peers.bin` at the committed configuration index and persists a
   Raft snapshot carrying the same `ConfState`.
3. Catch-up entries and snapshots persist the replicated log/FSM state.
4. Promotion repeats the configuration persistence with the node in
   `ConfState.Voters`.
5. On restart, `Factory.Create` loads persisted peers before constructing the
   transport, while `openDiskState` loads the WAL/snapshot. The recovered
   `ConfState`, not the old join list, determines voter and learner roles.

Both ConfChange and received-snapshot paths derive the persisted peer list by
iterating the authoritative voters and learners in `ConfState`. Addresses are
reused from the discovery cache when present; an ID missing from the cache gets
a non-dialable placeholder rather than being omitted. Discovery-only IDs not in
`ConfState` are dropped. This keeps peer metadata and snapshot membership
cardinality aligned on the next open.

Peer metadata may be published just before the matching local Raft snapshot;
restart recovery tolerates this ordering and replays the committed conf change
from WAL when needed. The final redeploy without join flags is therefore an
acceptance criterion: it proves that peer inventory, suffrage, and replicated
state are durable and that the node no longer depends on operator-supplied join
discovery.

## 9. Failure handling and rollback

| Failure | Required action |
|---|---|
| Joiner fails before `AddLearner` commits | Stop it, keep the old instance fenced, wipe only the incomplete joiner's target data dir if necessary, and retry join mode. Existing quorum is unchanged. |
| `AddLearner` committed but catch-up stalls | Keep the node a learner while diagnosing transport, disk, and snapshot transfer. Retry without promotion, or commit `remove_server` and restart the join sequence. |
| Promotion is rejected as not caught up | Wait for leader-side progress to reach the chosen bound and retry. Do not bypass the check in normal production recovery. |
| Joiner was added as a voter | Treat the role alarm as a failed procedure. Preserve quorum, then remove the node and repeat the learner workflow after the removal commits. |
| Joiner crashes after `AddLearner` or promotion | Restart first from its existing data dir. Once membership has persisted, do not wipe it merely to repeat the join command. |
| Remaining cluster loses quorum | Stop replacement work. `--raftJoinMembers` cannot recover quorum or create a replacement cluster. Use a separately reviewed quorum-recovery procedure. |
| Deployment must roll back to an older binary | Remove the learner before downgrade if it has not been promoted. If it is already a voter, first verify that the target version can read the current WAL, snapshot, and peers metadata; do not erase durable state as a rollback mechanism. |

At every failure point, the safe rollback is a committed membership operation
against the surviving leader. Bootstrap flags are never a rollback path for an
existing cluster.

## 10. Security and operations

- `--raftJoinMembers` contains cluster topology. Protect deployment
  configuration and logs accordingly; do not accept untrusted values that can
  redirect Raft traffic.
- Raft transport must remain on the trusted cluster network with the same
  firewall and authentication controls as normal replication. Join mode does
  not add an authorization bypass to `AddLearner` or `PromoteLearner`.
- Fencing is mandatory when reusing a Raft ID. The implementation cannot prove
  that a removed VM will never reboot with stale data.
- Confirm host identity and target data path before wiping. The script does not
  authorize multi-node destructive cleanup.
- Monitor `configuration`, leader stability, commit/applied indexes, snapshot
  transfer, transport errors, and `JoinRoleViolationCount` throughout the
  operation.
- Keep bootstrap variables unset during replacement. In particular,
  `RAFT_BOOTSTRAP_MEMBERS` describes a new cluster and is rejected with
  `RAFT_JOIN_NODE`.
- Remove join mode after promotion. Leaving it configured causes misleading
  intent alarms and obscures whether normal disk recovery works.

## 11. Tests and acceptance evidence

### 11.1 Configuration tests

`TestResolveRaftPeerConfigJoinMembers` covers the server-level contract:

- join peers are available to engine/admin discovery without enabling
  bootstrap;
- bootstrap seed remains nil;
- learner intent is required;
- explicit bootstrap, legacy bootstrap members, and group bootstrap peers are
  rejected;
- multi-group join is rejected;
- missing local ID and local-address mismatch are rejected; and
- a local-only list is rejected.

`TestRaftJoinRuntimesReady` covers membership presence, known leader, apply
catch-up, pending ConfChange rejection, and configuration-read errors for the
public-service gate.

### 11.2 Engine end-to-end test

`TestFreshLearnerJoinPromotesAndRestartsFromPersistedMembership` exercises the
engine lifecycle with real transports:

1. bootstrap one existing voter;
2. prove the second node has no copied peer metadata;
3. open the fresh node with all transport peers and `Bootstrap=false`;
4. assert its configuration is initially empty;
5. add it as learner and replicate an application proposal;
6. promote only after the leader reports sufficient progress;
7. verify a stale discovery-only peer is absent from persisted metadata;
8. stop both voters; and
9. reopen both without join peers or learner intent, elect a leader from the
   recovered two-member configuration, and commit another application write.

This test protects the central invariant that an explicit peer list can support
transport discovery without creating an initial voter `ConfState`, and that the
post-promotion membership survives a full-cluster restart rather than depending
on one still-running member's in-memory view.

### 11.3 Operational validation

The rolling-update script is syntax-checked and dry-run with `RAFT_JOIN_NODE` to
confirm the selected-node-only plan and derived join list. Production
acceptance additionally requires the post-promotion normal redeploy, stable
leadership, expected voter count, advancing indexes, and a successful write.

## 12. Follow-ups

1. Add durable removed-node tombstones or an authoritative cluster-side rejoin
   handshake so a stale removed node can fail closed even if an operator starts
   its old data directory.
2. Design a per-group equivalent of `--raftJoinMembers` for multi-group
   processes, including independent catch-up and promotion state per group.
3. Add a Jepsen replacement workload covering remove, wipe, learner catch-up,
   promotion, restart, and network partitions.
4. Add scripted status gates around catch-up and post-promotion stability so the
   rolling update cannot advance based only on the process port becoming ready.

Stale removed-node recovery and multi-group join are deliberately not claimed
by this implementation.
