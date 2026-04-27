# Raft Learner operations

Runbook for attaching a Raft learner replica and promoting it to voter.
Companion to the design doc `docs/design/2026_04_26_proposed_raft_learner.md`.

The learner primitive lets you attach a fresh node, let it catch up
on the log, and then promote it to voter — without ever shrinking the
cluster's effective fault tolerance during catch-up. The voter quorum
denominator stays at the existing voter count for the entire catch-up
window because the learner does not vote and is not part of the
ack-tracker majority.

## When to use

- Adding a new replica to an existing cluster.
- Replacing a removed voter with a fresh data dir.
- Bringing up a follower-served-read replica (future work; the learner
  is the storage primitive, the read path is a separate effort).

## Prerequisites

- The new node has a clean Raft data dir (`--raftDataDir`). If the dir
  contains an old engine marker that disagrees with the cluster's
  engine, refuse to reuse it — see `docs/etcd_raft_migration_operations.md`.
- The `raftadmin` CLI binary is available on a host that can reach the
  leader's gRPC port. Either build it (`go build ./cmd/raftadmin`) or
  use the binary shipped with the deployment.
- You know the cluster's leader (e.g. via `raftadmin <leader-addr> leader`).

## Workflow

### 1. Bring up the joining node

Start the node with `--raftJoinAsLearner` set. The flag is an
**operator alarm**, not a consensus-level enforcement:

```sh
go run . \
  --raftId          n4 \
  --address         127.0.0.1:50054 \
  --redisAddress    127.0.0.1:63794 \
  --raftDataDir     /var/lib/elastickv/n4 \
  --raftJoinAsLearner
```

The joiner will start, register with the gRPC transport, and wait for
the leader to fan out the cluster's `ConfState` — it has no
peer membership of its own yet.

`--raftJoinAsLearner` does *not* prevent the leader from issuing an
`AddVoter` for this node. If that happens (operator typo, runaway
script), the joiner's apply loop detects the role mismatch
post-apply, emits an `ERROR`-level structured log
(`etcd raft join-as-learner alarm: local node was added as voter`)
and bumps `JoinRoleViolationCount`. The node keeps running — by the
time the conf change has applied, it already counts toward the voter
quorum, and shutting down would shrink fault tolerance. The operator
remediation is to remove and re-add the node correctly.

### 2. Attach as learner against the leader

From the operator host:

```sh
raftadmin <leader-addr> add_learner <id> <address> [previous_index]
```

Concrete example:

```sh
raftadmin 127.0.0.1:50051 add_learner n4 127.0.0.1:50054
```

The leader proposes a `ConfChangeAddLearnerNode`. The learner enters
`ConfState.Learners`, the existing voters' quorum size is **unchanged**,
and the leader starts replicating log entries (and snapshots, if the
joiner is far behind) over the existing gRPC raft transport.

The optional `previous_index` is the leader's expected current
configuration index. Pass it when scripting back-to-back conf changes
to fail fast on stale plans; pass `0` (or omit) to skip the check.

### 3. Verify the learner is attached

```sh
raftadmin <leader-addr> configuration
```

The output now lists the learner with `suffrage: "learner"`:

```text
servers {
  id: "n1"
  address: "127.0.0.1:50051"
  suffrage: "voter"
}
servers {
  id: "n2"
  address: "127.0.0.1:50052"
  suffrage: "voter"
}
servers {
  id: "n3"
  address: "127.0.0.1:50053"
  suffrage: "voter"
}
servers {
  id: "n4"
  address: "127.0.0.1:50054"
  suffrage: "learner"
}
```

The learner does not appear in `Status.NumPeers` calculations as a
voter; it does appear in `Configuration().Servers`.

### 4. Wait for the learner to catch up

Poll the leader's `Status` and the learner's own `Status` until the
learner's `applied_index` is close to the leader's `commit_index`:

```sh
LEADER_COMMIT=$(raftadmin <leader-addr> state | awk '/CommitIndex/{print $2}')
LEARNER_APPLIED=$(raftadmin <learner-addr> state | awk '/AppliedIndex/{print $2}')
```

(Adjust to your `raftadmin state` output format.)

A reasonable rule of thumb is "applied within N entries of leader
commit" — choose N based on how aggressively new writes are landing.
For a quiet cluster, applied == commit is achievable in seconds; for
a busy cluster, allow more slack.

### 5. Promote the learner to voter

```sh
raftadmin <leader-addr> promote_learner <id> [previous_index] [min_applied_index]
```

The recommended invocation **always** passes `min_applied_index`:

```sh
LEADER_COMMIT=$(...)              # latest leader commit_index
raftadmin 127.0.0.1:50051 promote_learner n4 0 ${LEADER_COMMIT}
```

The leader runs both preconditions on the single-threaded admin loop
**before** proposing:

1. The target peer must currently be a learner. Promoting a node that
   is already a voter (or that doesn't exist) returns
   `FailedPrecondition: etcd raft promote-learner target is not a learner`.
2. The leader's `Progress[nodeID].Match` must be `>= min_applied_index`.
   If the learner has not caught up, the leader returns
   `FailedPrecondition: ... target has not caught up to min_applied_index`.

`min_applied_index = 0` skips the catch-up check. **Do not use 0 in
production.** It is accepted for symmetry with `previous_index` on
the same RPC family but it removes the primary safety check of
promote (see design §8 open question 3).

After a successful promote, `raftadmin configuration` shows the peer
with `suffrage: "voter"` and the cluster's voter count has grown by
one.

### 6. Sanity checks after promotion

- `raftadmin configuration` lists the new voter.
- `raftadmin state` on the new voter reports `state: FOLLOWER` (or
  `LEADER` if it later wins an election).
- A `Propose` against the leader still commits — write traffic is
  uninterrupted by the membership change.

## Removing a learner

If a learner needs to be detached (e.g., the operator decided not to
promote, or the node is being decommissioned), use the existing
`remove_server` command. There is no learner-specific RemoveLearner
RPC; etcd/raft uses `ConfChangeRemoveNode` for both voters and
learners.

```sh
raftadmin <leader-addr> remove_server <id>
```

The voter quorum is unchanged because the removed peer was never a
voter.

## Common errors

| Error | Cause | Remediation |
|-------|-------|-------------|
| `add learner: rpc error: ... not leader` | The address is not currently leader. | Re-issue against `raftadmin leader`. |
| `promote learner: rpc error: ... target is not a learner` | The peer is already a voter, or the ID is wrong. | Verify with `raftadmin configuration`. |
| `promote learner: rpc error: ... target has not caught up to min_applied_index` | Learner is still replicating. | Wait and retry; lower `min_applied_index` only if you understand the safety implication. |
| `promote learner: rpc error: ... target has no leader-side progress entry` | The learner is not in the leader's `Progress` map (e.g., raft has not yet processed the AddLearner). | Confirm the prior `add_learner` was committed via `configuration` before issuing `promote_learner`. |
| `add learner: rpc error: ... has too many pending config changes` | A previous conf change is still in flight. | Wait for it to commit; retry. |

## Observability

- `JoinRoleViolationCount` (process-wide counter): bumped each time
  the post-apply alarm fires for a node booted with
  `--raftJoinAsLearner` that finds itself in `ConfState.Voters`.
  Surfaced via `internal/raftengine/etcd.JoinRoleViolationCount()`.
- `raftadmin configuration` is the operator-readable view of suffrage
  for every member of the cluster.

## What this workflow does **not** do

- **Cold-bootstrap a learner-only cluster.** Bootstrap requires at
  least one voter to win an uncontested first election. A learner-only
  bootstrap configuration is rejected at startup.
- **Auto-promotion.** The engine never promotes a learner on its own.
  Promotion is always an explicit operator action via
  `promote_learner`.
- **Serve linearizable reads from the learner.** Today
  `LinearizableRead` on a learner returns `ErrNotLeader` and the
  caller must forward to the leader. Follower-served reads are an
  explicit non-goal of this milestone and will be addressed in a
  separate proposal.
