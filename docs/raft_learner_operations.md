# Raft Learner operations

Runbook for attaching a Raft learner replica and promoting it to voter.
Companion to the design doc `docs/design/2026_04_26_implemented_raft_learner.md`.

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
  --raftJoinAsLearner \
  --raftJoinMembers "n1=127.0.0.1:50051,n2=127.0.0.1:50052,n3=127.0.0.1:50053,n4=127.0.0.1:50054"
```

`--raftJoinMembers` is transport discovery only. Unlike
`--raftBootstrapMembers`, it does not create an initial `ConfState` and does
not make the node campaign. The list must describe the current single-group
membership plus the local joiner, include at least one existing remote member,
and match the joiner's `--raftId` and `--address`. It is rejected unless
`--raftJoinAsLearner` is set, and it cannot be combined with any bootstrap
flag.

The joiner will start, register with the gRPC transport, and wait for
the leader to fan out the cluster's `ConfState` — it has no
peer membership of its own yet.

While it waits, the Raft/RaftAdmin port is available so the operator can run
`add_learner` and inspect status. Redis, DynamoDB, S3, and SQS listeners remain
closed until the local ID appears in committed membership, a leader is known,
the local apply index has caught up to its observed commit index, and no
configuration change is pending.

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
raftadmin <leader-addr> promote_learner <id> [previous_index] [min_applied_index] [skip_min_applied_check]
```

The recommended invocation **always** passes a non-zero
`min_applied_index`:

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

If `min_applied_index = 0` and `skip_min_applied_check` is not set,
the engine rejects the request with
`FailedPrecondition: ... requires min_applied_index>0 unless skip_min_applied_check is set`.
This is intentional: it forces an operator who copy-pastes a script
that forgets the catch-up bound to make an explicit decision, rather
than silently disabling the primary safety check of the promote
operation.

The `skip_min_applied_check=true` escape hatch exists for bootstrap-
time scaffolding where the operator has confirmed catch-up
out-of-band (e.g., by inspecting `raftadmin <learner-addr> state`).
**Do not use it in normal production runs** — pass a real
`min_applied_index` instead.

After a successful promote, `raftadmin configuration` shows the peer
with `suffrage: "voter"` and the cluster's voter count has grown by
one.

Restart the promoted node without `--raftJoinAsLearner` and
`--raftJoinMembers`. Its committed `ConfState` and peer inventory are now
durable in the Raft snapshot/WAL and `etcd-raft-peers.bin`; normal restart must
use that state rather than the temporary join intent.

### 6. Sanity checks after promotion

- `raftadmin configuration` lists the new voter.
- `raftadmin state` on the new voter reports `state: FOLLOWER` (or
  `LEADER` if it later wins an election).
- A `Propose` against the leader still commits — write traffic is
  uninterrupted by the membership change.

## Replacing a voter with a fresh data dir

Do not erase a voter and run a normal deploy. A wiped node no longer has the
Raft WAL, snapshot, or `etcd-raft-peers.bin` that identify the committed
cluster. The normal startup guard intentionally refuses to self-bootstrap in
that state.

Use this order for a replacement that keeps the same Raft ID:

1. Fence the old process or VM so the old and replacement instances cannot run
   concurrently with the same Raft ID.
2. Verify the remaining voters have a leader and can commit a configuration
   change. Run `remove_server <id>` against that leader and wait until the ID
   disappears from `configuration`.
3. Stop the target container and erase or archive only that node's Raft data
   directory. Never wipe more than one replacement target at a time.
4. Start the target with `--raftJoinAsLearner` and `--raftJoinMembers`, then
   follow the add, catch-up, and promote steps above.
5. Restart the promoted node without the two join flags and verify a write can
   commit.

For `scripts/rolling-update.sh`, step 4 can be expressed without copying peer
metadata by selecting exactly one rollout node:

```sh
ROLLING_ORDER=n4 RAFT_JOIN_NODE=n4 ./scripts/rolling-update.sh
```

The script derives `--raftJoinMembers` from `NODES` and rejects a join rollout
that contains another node. After promotion, clear `RAFT_JOIN_NODE` and run the
same single-node rollout once more to remove the temporary join flags.

For recurring production replacements, use the fenced, resumable wrapper
instead of issuing those steps independently:

```sh
TARGET_NODE=n4 \
REPLACEMENT_CONFIRM=n4 \
ROLLING_UPDATE_ENV_FILE=/path/to/deploy.env \
REPLACEMENT_FENCE_COMMAND='fence-n4-and-block-raft' \
REPLACEMENT_VERIFY_COMMAND='verify-a-real-write-and-read' \
./scripts/raft-member-replace.sh --execute
```

Run the same command with `--dry-run` first. The wrapper verifies surviving
quorum before fencing, records the complete member signature and
`configuration_index`, records a non-zero learner catch-up floor, restarts
without join flags, and requires the application verification command to
succeed. Any unrelated membership change aborts the operation. It supports one
Raft group with a surviving quorum; it is not a forced no-quorum recovery tool.
Supply `TARGET_NODE` and all `REPLACEMENT_*` values on the invocation, not in
`deploy.env`; the deployment env is inventory only. A retained completed state
file is audit evidence, so archive it or choose a new state path for a later
incident.
See
`docs/design/2026_07_18_implemented_fenced_raft_member_replacement.md` for the
state machine, fencing contract, and failure handling.

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
| `flag --raftJoinMembers requires --raftJoinAsLearner` | Join discovery was configured without learner intent. | Pass both flags for a fresh join, or neither for a normal restart. |
| `flag --raftJoinMembers cannot be combined with raft bootstrap flags` | Join and new-cluster creation were requested together. | Remove every bootstrap flag; a replacement joins the surviving cluster. |
| `conf state node id=...: etcd raft peer address is required` | The join list omitted a voter or learner present in the authoritative `ConfState`. | Stop the joiner and restart it with the complete current member inventory plus its own ID/address. Do not insert an empty or guessed address. |

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
- **Recover a cluster that has lost quorum.** Join members are only transport
  discovery for attaching to a live leader; they are not a force-new-cluster
  or quorum-recovery mechanism.
- **Join multiple Raft groups.** The first implementation is single-group. A
  future per-group join inventory must preserve the same bootstrap separation.
