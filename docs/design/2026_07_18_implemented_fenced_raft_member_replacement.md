# Fenced Raft voter replacement operation

## 1. Status

Implemented by `scripts/raft-member-replace.sh` as the operational companion to
the fresh learner join mode. The command automates one same-ID voter
replacement in one Raft group while a surviving quorum remains available.

This is not an automatic repair loop. A candidate state, failed health check,
or missing process never authorizes data deletion or membership mutation.

## 2. Motivation

Removing a damaged data directory and running the normal deployment command is
not a valid Raft repair. Membership survives on the other voters in committed
Raft configuration, snapshots, WAL, and peer metadata. The wiped process has
none of that authority and may campaign with an incompatible local view.

The safe operation already had to be performed manually:

1. fence the old ID owner;
2. commit `RemoveServer`;
3. archive the target data;
4. start a fresh process in discovery-only join mode;
5. commit `AddLearner`;
6. wait for catch-up;
7. commit `PromoteLearner`; and
8. restart without temporary join flags.

Repeated manual execution makes ordering, stale configuration indexes, and
partial failure the dominant risks. The implementation makes those boundaries
explicit and resumable.

## 3. Scope and non-goals

The v1 command supports:

- one current voter selected by exact Raft ID;
- one Raft group exposed at the configured RaftAdmin endpoint;
- same-ID replacement at the same advertised address;
- external fencing or an explicitly selected container fence;
- archive-by-default data reset;
- the existing `rolling-update.sh` fresh learner mode; and
- durable local resume state on the control host.

The command deliberately does not support:

- no-quorum recovery or forced configuration rewrite;
- automatic execution based on CANDIDATE state;
- simultaneous replacement of multiple members;
- changing a member ID or advertised address;
- multi-group process replacement; or
- proving that a stopped VM can never be powered on again.

No-quorum recovery needs a separate disaster-recovery design with an explicit
choice of authoritative log and accepted data-loss boundary.

## 4. Safety invariants

### 4.1 Explicit authorization

`--execute` is required and `REPLACEMENT_CONFIRM` must exactly equal
`TARGET_NODE`. `--dry-run` performs no build, RPC, SSH, fence, or deployment.
For `--execute`, `TARGET_NODE`, `REPLACEMENT_CONFIRM`,
`REPLACEMENT_VERIFY_COMMAND`, and the external fence command must be supplied
on the invocation. Replacement controls are never read from the deployment env;
that file is inventory only, so a stale recovery value cannot authorize a new
operation.

The target must be present in `NODES`, be a current voter, and have exactly the
advertised `NODES` address. A learner, absent ID, or address mismatch aborts.

### 4.2 Surviving quorum before fencing

The command reads the leader's authoritative configuration and computes
`floor(voters/2)+1`. At least that many non-target voters must answer RaftAdmin
status before fencing starts. This is intentionally stricter than counting the
target: removal must still commit after the target is stopped.

The leader must be fully applied, expose a non-zero configuration index, and
have no pending configuration change. If the target is leader, leadership is
transferred only to a non-target follower whose applied index has reached the
leader's observed commit index.

### 4.3 One live owner of a Raft ID

The old process is fenced before `RemoveServer`. External mode requires an
operator-supplied command that fences the VM/process from the cluster network.
Container mode is opt-in and is valid only when host lifecycle policy prevents
the stopped container from independently returning.

After the fence command, the target RaftAdmin endpoint must remain unreachable
for the configured consecutive verification interval. It is checked again
before removal and before the fresh deployment. Membership is never removed
while the old endpoint still answers.

External mode also stops and verifies the target container over the management
SSH path after the Raft fence is observed. If that management action is not
available, the operation stops before `RemoveServer` rather than shrinking the
group with a still-running process. Docker inspection must itself succeed;
missing containers, daemon failures, and permission errors are not interpreted
as proof that the process is stopped.

### 4.4 Serialized membership changes

RaftAdmin status now reports:

- `configuration_index`, the current durable membership index; and
- `pending_conf_change`, whether an unapplied change is in flight.

Preflight records both the complete member signature and its non-zero
configuration index. Every `remove_server`, `add_learner`, and
`promote_learner` call passes that recorded index as `previous_index`; it never
adopts a newer index merely because another topology change committed. After
each successful change, the command verifies the exact expected post-change
signature before persisting the returned index for the next stage. The engine
rejects every new add, promote, or remove proposal while an earlier
configuration entry remains unapplied, so etcd/raft cannot silently rewrite a
racing proposal to a no-op. A resume after a lost RPC response is accepted only
when the complete member signature matches the one transition that was in
flight. It also accepts an equal saved/live configuration index when that
signature already proves the metadata write completed before the stage write.

### 4.5 Catch-up before promotion

Immediately before `AddLearner`, the command records the leader's commit index
as a durable `min_catch_up_index`. It writes that value to the resume file
before proposing the change so a control-host crash after commit cannot lose
the promotion bound.

The learner must report `FOLLOWER`, a known leader, and
`applied_index >= min_catch_up_index`. Promotion passes the same non-zero value
to `promote_learner` and explicitly leaves `skip_min_applied_check=false`.

### 4.6 Durable membership restart and application verification

After promotion, the target is redeployed without `RAFT_JOIN_NODE`. This proves
it can start from its WAL, snapshot, and persisted peer metadata rather than
temporary discovery intent.

Completion requires all original voter IDs to be present as voters, every
voter to answer as LEADER or FOLLOWER with no local apply lag, and one leader to
remain stable for the configured interval. The operator must also provide
`REPLACEMENT_VERIFY_COMMAND`; a successful application-level write/read check
is mandatory.

## 5. Resume state machine

The local state file is mode `0600` and is updated by write-then-rename. An
atomic directory lock prevents two control processes from operating on the
same state file.

| Stage | Durable fact |
| --- | --- |
| 0 | Target identity, address, complete member signature and configuration index, original voter set, operation ID, canonical data directory, and archive path recorded |
| 1 | Preflight and any leadership transfer completed |
| 2 | Old ID owner fenced and endpoint observed down |
| 3 | Target absent from committed membership |
| 4 | Only the target data directory archived or deleted |
| 5 | Target deployed with fresh learner join intent |
| 6 | Target committed as learner; catch-up floor persisted |
| 7 | Target committed as voter |
| 8 | Target redeployed without join intent |
| 9 | Stable voter set and application write/read verification completed |

Operations around a stage boundary are idempotent. The chosen data mode and
canonical data directory are stored with the operation and cannot change on
resume. For example, a resumed stage 2 accepts that removal already committed,
stage 4 safely reruns the target-only learner deployment after a lost rollout
response, stage 5 accepts an already committed learner only when its catch-up
floor exists, and stage 6 accepts an already promoted voter. A conflicting
suffrage, voter set, or data directory aborts instead of guessing what
happened.

The state file is retained by default as operational evidence. A retained
stage-9 file is never treated as authorization for a later incident: a new
replacement must archive that evidence or select a new
`REPLACEMENT_STATE_FILE`. Set `REPLACEMENT_KEEP_STATE=false` only when another
durable audit system records the completed operation.

## 6. Data handling

Archive mode is the default. The deterministic archive path includes the
target ID and operation ID, making a retry distinguishable from a new
replacement. If both the live data path and archive path exist, the command
fails rather than overwriting either.

Trailing slashes are removed from `DATA_DIR` before the sibling archive path is
derived, and the filesystem root is rejected. The archive can therefore never
be constructed as a child of the directory being moved.

Delete mode must be explicitly selected. Both modes execute only after the ID
is absent from committed membership and verify the target container is not
running. The command never iterates over multiple data hosts.

## 7. Operator interface

Validate a plan without network or filesystem changes:

```sh
TARGET_NODE=n2 \
ROLLING_UPDATE_ENV_FILE=~/dump/elastickv-deploy/deploy.env \
./scripts/raft-member-replace.sh --dry-run
```

Execute with an external hypervisor or network fence:

```sh
TARGET_NODE=n2 \
REPLACEMENT_CONFIRM=n2 \
ROLLING_UPDATE_ENV_FILE=~/dump/elastickv-deploy/deploy.env \
REPLACEMENT_FENCE_COMMAND='fence-n2-and-block-raft' \
REPLACEMENT_VERIFY_COMMAND='redis-cli -h 192.168.0.64 SET replacement-check ok && redis-cli -h 192.168.0.64 GET replacement-check | grep -Fx ok' \
./scripts/raft-member-replace.sh --execute
```

Use container mode only when the host cannot revive the old container during
the operation:

```sh
REPLACEMENT_FENCE_MODE=container
```

The replacement command sources the deployment env for inventory and then
invokes `rolling-update.sh` twice with only `TARGET_NODE` selected. Bootstrap
membership is removed from both invocations. The first deploy includes
`RAFT_JOIN_NODE`; the post-promotion deploy clears it. Both invocations force
`DRY_RUN=false`, so a stale deployment-env dry-run setting cannot advance the
replacement state without starting the target.

## 8. Failure handling

| Failure | Required response |
| --- | --- |
| No leader or insufficient surviving quorum | Stop. Restore an existing voter or use a separately reviewed disaster-recovery procedure. |
| Target endpoint remains reachable after fence | Strengthen the fence. Do not remove membership. |
| Recorded member signature or `previous_index` mismatch | Inspect the concurrent topology change and reconcile the state file with the authoritative configuration. Do not retry blindly. |
| Existing state is already at stage 9 | Archive the completed evidence or choose a new state-file path before authorizing a new replacement. |
| Data and deterministic archive both exist | Inspect the target host. Do not delete either path automatically. |
| Learner catch-up timeout | Leave it a learner, diagnose transport/disk/snapshot transfer, and resume with the same state file. |
| Promotion committed but normal restart fails | Keep the surviving voters running. Repair the target from its existing post-join data; do not wipe it again. |
| Final voter set differs from the recorded set | Stop and audit membership. The command does not normalize unexpected changes. |
| Application verification fails | Replacement remains incomplete even if Raft health checks pass. Diagnose the public protocol path and resume verification. |

## 9. Tests and acceptance

`raft_member_replace_script_test.go` runs the complete shell workflow against a
stateful fake RaftAdmin, SSH transport, and rolling deploy command. It verifies
the exact voter-to-absent-to-learner-to-voter sequence, non-zero catch-up floor,
both deploy modes, forced execute rollout, application verification, durable
stage 9, canonical data-directory persistence, external-fence process stop,
and rejection of a completed state as a new operation.

A negative test makes one surviving voter unreachable in a three-voter group
and proves the command aborts before the fence action. Another test loses the
`AddLearner` RPC response after commit and proves the persisted catch-up floor
allows a safe resume without a duplicate add. A separate lost-response test
proves a completed learner rollout is safely repeated from stage 4 rather than
leaving the group shrunken. Lock ownership is tested so a losing control
process cannot remove the winner's directory lock. RaftAdmin server and CLI
tests cover propagation and rendering of the configuration index and pending
change fields. Existing engine membership tests cover stale previous indexes,
learner catch-up enforcement, same-ID replacement, and persisted membership
restart.

Additional negative tests prove that deployment-env replacement controls
cannot authorize an operation, a configuration-index change after preflight
aborts before removal, invocation-level timing controls override stale env
values, dot-segment data paths canonicalize to a sibling archive, and a changed
data directory is rejected on resume. Engine coverage also proves a membership
proposal is rejected while the pending configuration fence is active; the
shell resume test covers the equal-index metadata-before-stage crash window.

Production acceptance still requires recording the state file, fence evidence,
final `raftadmin status`/`configuration`, and the output of the configured
application verification command.
