# Target membership changes during online split

The runtime base is `ec15b79b897fcaad3dff2baf3f439b70f06af348` from PR #1544.
The experiment exercises a RawKV split from group 1 to group 2 using real
etcd/Raft, separate local process data directories, the default synchronous
FSM storage setting and authenticated TLS on loopback. Three initial voters
serve both groups. A fourth process joins the catalog group as a learner
before the split starts and joins the target group during the fault.

```sh
PREFLIGHT_LOG_DIR=/path/to/new/log-directory \
  bash scripts/jepsen-split-preflight.sh --online-member-change
cd jepsen && lein test elastickv.split-preflight-workload-test
```

## Actual cutover boundary and read-back

The build overlay pauses only the cutover controller after the final online
readiness probes and before `commitSplitJobCutover`. The same attempt must
commit, identified by its job, catalog version, observed target configuration,
seal and nonce. The Jepsen nemesis uses the real Raft admin API with the
preceding configuration index as each change's CAS. It observes `n4` first
as a learner, waits for its local apply to cover the target leader's sampled
commit index, promotes it and verifies the voter configuration on `n4`.
It also verifies the catalog still names group 1 before releasing publication.

The overlay withholds catalog cache updates from `n3`, the old ingress and
initial target leader. Its RPC and Raft services continue running. The checker
requires an ACK for overwriting 4202 with 5101 and for deleting a key seeded
with 4303 during online capture. During the stale-cache interval it probes
both keys through both endpoints. It then transfers target leadership to `n4`
and requires successful reads of 5101 and absence from that endpoint while
`n3` still holds the old route. After releasing the cache update, the checker
requires DONE, equality of the imported and sealed sequences, a successful
post-cutover write and four exact reads on each ingress. This covers the
retained key, overwritten key, deleted key and new key.

The fixed Go 1.27.4 build passed three runs on Darwin/arm64 with fresh stores.
All three attempts published catalog version 4 while `n3` held version 3 with
source owner 1. The before/after attempt markers matched byte-for-byte.

| Run | Target configuration at confirmation | Add index | Sampled commit / learner applied | Promote / voter applied | Stale probes | Reads led by `n4` while `n3` was stale | Final audit |
|---|---:|---:|---:|---:|---|---|---|
| 21:56 JST | 1 | 74 | 74 / 74 | 75 / 75 | 4 `Unavailable` | 5101, absent | 8/8 |
| 21:57 JST | 1 | 74 | 75 / 75 | 76 / 76 | 4 `Unavailable` | 5101, absent | 8/8 |
| 22:04 JST | 1 | 80 | 81 / 81 | 82 / 82 | 4 `Unavailable` | 5101, absent | 8/8 |

All four stale probes per run returned the route guard’s `Unavailable`.
The last run enforced that status and message in the oracle and used a relative
log directory to verify marker collection. Every run ACKed
all six mutations, including all three seeds, the in-flight overwrite and
delete, and the post-cutover write. The final audit checks their resulting
state, where the overwritten and deleted seeds have been superseded.

## Regression and fix

The strengthened runtime check reproduced `Unknown` when `n4` forwarded
RawGet to the stale target leader. The request cited a route version above
the receiving node's cache. An error-valued argument in CockroachDB
`errors.Wrapf` attached the local wait timeout or cancellation as a secondary
cause. The mixed-error guard correctly declined to translate that error chain.
The route wait now attaches diagnostic text to its single typed result.
The RawGet receiver translates the typed lag to `Unavailable`, with the
existing bounded retry for unpinned requests. Pinned requests return without
retrying. A Bufconn regression built from the real shard-store error fails
on the original wrapper and passes with the fix. Genuinely mixed errors still
avoid both classification and retries.

The existing voter-pause and catalog-leader failover scenarios also passed in
fresh clusters. The full Jepsen harness ran 444 tests with 6,217 assertions
and no failures or errors. The RawGet and shard-store read-fence regression
tests passed with Go’s race detector.

## Model checks and correspondence

`make lean-check` checks `lean/SplitMembership.lean` as well as the existing
Split proofs. `successful_joined_read_preserves_ack` composes the exact
Raft log-prefix replay with the serving gate. Given log matching, a sampled
commit index at or above the durable Ready prefix, and application through
that index, every successful read in the model equals a lookup in the sealed
image. The lookup can yield a value or absence. The model's stale source and
stale forwarding target produce `Unavailable`. The proof uses `propext`.

`make tla-check` exhausts the finite add/apply/promote/publish model in
`tla/split_membership/`. The safe config generated 127 states, 41 distinct,
with an empty work queue. The two negative configurations detect loss of an
acknowledged overwrite when serving can skip Ready apply, and resurrection
of a delete when the old ingress can serve through its stale route.

These checks cover the tested learner catching up before promotion and
subsequent latest reads behind the validated ownership and Ready gates.
Raft log matching and the existing sealed-prefix replay are the input premises.
The configuration index at publication may be newer than the one seen by the
last probes. The existing strict configuration-freshness counterexample
continues to show this boundary. Forced promotion, removals, concurrent
arbitrary reconfiguration and crash recovery in this exact window are outside
this run.
