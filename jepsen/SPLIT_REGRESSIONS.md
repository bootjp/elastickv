# Bounded Split Regressions

## Process pause during cross-group preflight

Run `bash scripts/jepsen-split-preflight.sh` from the repository root with
Go, Java 21, Leiningen, OpenSSL and jq. The script starts three processes
with independent Raft data directories, two groups and authenticated TLS
on loopback. It uses ports 25101–25103, 25201–25203 and 25301–25303.
Set `LEIN` to select the Leiningen executable and `PREFLIGHT_LOG_DIR` to
retain node logs. Jepsen writes its history under
`jepsen/store/elastickv-split-preflight-pause/`.

The client writes two RawKV registers on opposite sides of the split.
The nemesis waits for a durable checkpoint containing both completed and
pending brackets, stops a catalog follower with SIGSTOP, checks its process
state and samples the same job on the same catalog leader and term.
The checker requires both samples inside preflight, matching scan timestamps
and preserved completed brackets. After reads during the pause, SIGCONT
restores the voter. The job must reach DONE with every bracket complete,
the expected route owners, and both original values readable through all
three nodes. The test fails if it misses the preflight window.

## Leader failover after captured RawKV mutations

Run the same three-process harness with `--online-leader-failover`:

```sh
PREFLIGHT_LOG_DIR=/path/to/logs bash scripts/jepsen-split-preflight.sh --online-leader-failover
```

The workload requires an unfinished preflight checkpoint with online capture
active. It acknowledges a moving-range overwrite and delete, stops the
catalog/source leader, and requires a higher-term replacement to observe the
same unfinished generation while the old leader remains stopped. It resumes
the voter and waits for DONE and the exact sealed import position. After a
new write on the moved range, it requires the retained value, the overwrite,
absence of the deleted key, and the new value on all three ingresses.

The checker treats a missed fault window, missing ACK, resurrected delete,
or missing or incorrect ingress read as a failed run. History is recorded
under `jepsen/store/elastickv-split-online-failover/`. The preflight workflow
runs each scenario using fresh stores. See the [safety evidence](../docs/evidence/online_split_safety_20261122/README.md)
for the accompanying Lean and TLC checks and the remaining fault cases.

## Target learner promotion between final confirmation and catalog publication

```sh
PREFLIGHT_LOG_DIR=/path/to/logs bash scripts/jepsen-split-preflight.sh --online-member-change
```

This variant also needs Python 3 and starts a fourth process on ports 25104,
25204 and 25304. A Go build overlay pauses the cutover controller after the
last readiness probes and delays catalog cache updates on ingress `n3`.
The fault uses the real Raft admin API to add `n4` as a target learner, wait
for its committed prefix to apply, and promote it. The same paused cutover
attempt must publish the new owner only after that promotion applies.

The checker requires successful overwrite and delete ACKs during online
capture. Read probes through `n3` and `n4` may return the correct value or
`Unavailable` from the route guard while `n3` is stale and the target leader. After target
leadership transfers to `n4`, it must read the correct overwrite and absence
of the deleted key while `n3` is still stale. After healing and DONE, both
ingresses must return all four expected values, including a post-cutover
write. History lives under `jepsen/store/elastickv-split-target-membership/`.
The log directory also preserves the before/after attempt stamps and cache
observations. See the [membership evidence](../docs/evidence/online_split_membership_20261122/README.md).

Jepsen Scheduled Stress Test runs the `none` case as the regular
`test (split-membership)` job. It keeps the publication hold, learner
promotion, and stale-route checks. To run that job on its own:

```sh
gh workflow run jepsen-test-scheduled.yml --ref BRANCH \
  -f 'workloads-json=["split-membership"]'
```

The same schedule runs the three additional membership cases under `faults`.
To run those cases and the three Redis faults on a branch:

```sh
gh workflow run jepsen-test-scheduled.yml --ref BRANCH \
  -f faults-only=true -f 'fault-adapters-json=["redis"]'
```

The fault group runs three independent membership jobs on fresh stores:

| Case | Required evidence |
| --- | --- |
| `kill` | `SIGKILL` of the target leader after promotion and before publication, a higher-term replacement while the former leader is down, and the former leader replaying the promoted configuration after restart. The `cleanup-n3` marker records the restarted process PID after it has exited. |
| `partition` | Dropped Raft traffic to the target learner during catch-up, a namespace-admission or catch-up rejection while the learner is behind the required index, and promotion after the link heals. |
| `pause` | The new voter is stopped when the catalog publishes the same cutover attempt, then resumes with the same PID. |

All four jobs require all six mutation ACKs, correct reads from the new
leader while the old route is held, and all eight final reads after healing.
The regular job publishes `jepsen-store-split-membership` for history and
`jepsen-split-membership-logs` for node logs and cutover and ingress markers.
Each fault job publishes both in `jepsen-split-membership-<case>-<run-attempt>`.
Use `SPLIT_MEMBERSHIP_FAULT=kill`, `partition`, or `pause` with the local
script to select an additional fault.

## Repeated splits

`elastickv.split-workload --repeat-splits` opts into exactly three splits:

1. Same-owner split at `m2-split`.
2. Split its right child at `m2-t` into another Raft group.
3. Split the moved right child at `m2-u` back into the original group.

Each step re-reads the route catalog and uses its current version and child ID.
The next split cannot start until the preceding split completes. A rejected
helper call, FAILED/ABANDONED job, timeout, or missing lifecycle fails the run;
it does not count as successful migration or authorize another split.

The oracle checks exact partition bounds, unchanged retained/unrelated routes,
owners, increasing catalog versions, distinct jobs, and the next source being
the previous moving child. SourceRouteID remains original lineage. Observed
working children must keep their ID through cutover (e.g. working route 40,
not the original parent 20). Polling can miss FENCE; absence of a sampled fence
does not prove that a durable fence was correct or never occurred. Job-phase
and route queries are separate RPCs, not an atomic evidence snapshot. The helper
does not expose durable SourceRouteID/WorkingSourceRouteID fields; the lineage
check uses the requested source and observed geometry, not decoded job internals.

Four small integer registers cover all final ranges. Final reads from every
node run after the main phase and fault recovery. The coverage checker requires
an acknowledged write to every register and successful final reads after the
last verification; the existing independent linearizability checker checks values.
The default single-split and explicit abandon workloads are unchanged.

## Running

Use an isolated cluster with an ACTIVE route containing all four register keys,
two Raft groups, and the existing split/list-routes binaries. A programmatic test
uses 8 workers by default; CLI invocations must specify `--concurrency 8` (the
shared CLI default is 5). The worker count must be even and at least the node
count and 8. For an already provisioned cluster, for example:

```sh
lein run -m elastickv.split-workload --local --nodes n1,n2,n3 \
  --raft-groups 1=50051,2=50052 --grpc-host-port n1:50051 \
  --repeat-splits --concurrency 8 --rate 5 --time-limit 90 \
  --split-bin /path/to/elastickv-split --list-routes-bin /path/to/elastickv-list-routes
```

There are at most 100 writes/key by default. Verification polls every 250 ms,
times out after 120 s/job, and retains at most 64 distinct observations; exceeding
that cap fails rather than discarding evidence. A short run that misses a split
fails. `--repeat-splits` and `--abandon-at-fence` are mutually exclusive.

## Size-Guard Limits And Runtime Gate

The controller/oracle unit cases cover a ResourceExhausted helper rejection,
preserved diagnostics, sticky failure, and preflight ABANDONED not being DONE.
They do **not** construct an oversized value or exercise the replicated guard.
Redis register writes are integers; the helpers expose only job ID/phase, not
guard activation, durable fence indexes, preflight cause, or pending PREPAREs.
Adding payload padding alone cannot establish a pre-fence admission test.

The Go adapter's `TestMigrationQuorumPartitionAcceptance` separately exercises
three real data voters, a new learner, configuration/leader changes and native
snapshot restart. It asserts typed moving-range rejection, retained-left large
writes and exact restored bytes. `TestMigrationQuorumPreparedDrainAndHistoricalPreflight`
holds large PREPAREs before guard activation, resolves COMMIT and ABORT through
real Raft, and detects a retained oversized historical version beneath a small
latest value before either a source or catalog write fence. Root metadata in
these data-quorum tests is explicitly stubbed; they do not establish TLS/auth
or full cross-group root/data fault-injection acceptance. Deterministic controller
regressions separately cover repeated cross-group moves and retained negative owners.

Namespace activation requires authentication, opcode support, installed
read/proposal/consumer hooks, a supported applied configuration and
all-current-apply-member negotiation. Flags alone cannot bypass namespace
activation or authorize imported data. See [online split activation](../docs/design/2026_11_21_partial_online_split_activation.md)
for the persistent RawKV default and its fallback behavior.

Run focused coverage and the full harness suite from `jepsen/`:

```sh
lein test elastickv.split-workload-test elastickv.split-regression-test
lein test
```

The full suite requires the pinned `jepsen/redis` submodule, unlike the focused
split tests. In a fresh worktree, initialize it or add a clean checkout of that
same pinned revision to Lein's source paths; do not substitute stub namespaces.
