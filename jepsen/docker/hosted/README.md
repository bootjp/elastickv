# Hosted fault tests

The `Jepsen Hosted Faults` workflow runs on Ubuntu GitHub-hosted runners. Each
matrix job creates three SSH-managed database containers and a separate Jepsen
control container on a dedicated Docker bridge. No ports are published. Only DB
containers receive `NET_ADMIN`; none receive `SYS_TIME` or privileged mode.
All three voters start with the same explicit `--raftBootstrapMembers` list;
this suite tests a fixed single-group cluster rather than membership changes.

The jobs exercise `partition`, `kill` (SIGKILL), and `pause` (SIGSTOP) separately.
Each round targets every node while the other two retain a quorum. Each injection
is followed by healing/restart/resume. The last phase explicitly heals again,
waits 10 seconds, then reads every key from every node or drains the queue.
Redis may forward reads
to the leader, so these are checks through each node's client endpoint, not
direct inspections of each follower's local storage.
The workload tracks emitted key IDs, including keys introduced when Elle
rotates its active key window, and reads them all in a final transaction per
node. Graphviz and gnuplot are included for checker diagnostics.

The existing adapter checkers analyze the complete history, including recovery:

| Adapter | Managed port | Consistency and recovery |
| --- | --- | --- |
| Redis | 6379 | Strict-serializable list append; read all emitted keys through every node |
| DynamoDB | 8000 | Strict-serializable list append; one-key final reads avoid the 100-key transaction limit |
| S3 | 9000 | Linearizable object registers; final GET for every object through every node |
| SQS Standard | 9324 | Acknowledged sends must be received and deleted; 40s receive/delete-only drain |
| SQS HT-FIFO | 9324 | Delivery identities, no lost acknowledged sends, duplicates, or ordering violations; 40s drain |

An additional checker requires successful
writes, completed injection and healing operations for every target, and a
successful recovery read for every node/key pair. Missing operations, failed
reads, and unknown/invalid Jepsen results fail the job. Required recovery reads
must succeed; queue delivery correctness is checked across the bounded drain.

## Run locally on Linux

Requirements: Docker Engine with Compose v2+, Git, Bash, and `ssh-keygen`.
Java, Leiningen, Go, and node dependencies are installed in the image. Initialize
the Redis submodule before running:

```bash
git submodule update --init --recursive
bash jepsen/docker/hosted/run.sh partition
bash jepsen/docker/hosted/run.sh kill
bash jepsen/docker/hosted/run.sh pause
# Optional second argument selects the adapter (default: redis).
bash jepsen/docker/hosted/run.sh partition dynamodb
bash jepsen/docker/hosted/run.sh kill s3
bash jepsen/docker/hosted/run.sh pause sqs-standard
bash jepsen/docker/hosted/run.sh partition sqs-htfifo
```

The default main phase is 90s, with a 5s warmup and 5s fault/recovery intervals.
Set `JEPSEN_TIME_LIMIT` to extend the main phase; it must exceed 35s. Fault
operations take time in addition to their intervals: the evidence checker
rejects a run that ends before all three cycles complete. Builds, cluster
startup, recovery, and analysis add to the total job duration.

## Scheduled stress suite

`Jepsen Scheduled Stress Test` calls this reusable workflow every six hours.
It runs all five adapters against all three faults (15 jobs), using a 300s
main phase by default. Fault rounds repeat throughout the main phase and the
checker requires at least two completed injection/healing cycles per node.
The final healing phase also recovers an in-progress last round before reads
or queue draining begin.

Use manual dispatch with `faults-only=true` to run just these 15 jobs. A nonempty
`workloads-json` selects the existing local workloads and omits fault jobs,
unless `faults-only` is true. The normal PR workflow continues to run only
Redis for 90s with one round. Scheduled and PR jobs have separate concurrency
groups and do not cancel each other.

Stress `time-limit`, `rate`, `key-count`, `max-writes-per-key`, and
`max-txn-length` inputs are forwarded. Fault concurrency is six to cover the
three nodes; it is independent of the local S3 workload's concurrency setting.
S3 reuses `s3_budget.sh` to cap per-object histories at 32 operations while
preserving the aggregate requested budget. Effective settings are saved in
`parameters.txt`. SDK retries are disabled for DynamoDB/SQS so uncertain
outcomes remain visible rather than replaying workload operations.
Only idempotent table/bucket/queue setup is retried with bounded backoff while
the fresh cluster elects its leader and opens its API listeners.

For a local stress run:

```bash
JEPSEN_TIME_LIMIT=300 JEPSEN_REPEAT_FAULTS=true JEPSEN_MIN_FAULT_CYCLES=2 \
  bash jepsen/docker/hosted/run.sh partition dynamodb
```

With 5s intervals, two cycles require a main phase longer than 65s plus time
for fault operations. The reusable workflow has a 25-minute step budget for
image build, setup, workload, recovery, analysis, and cleanup.

Results are written to `artifacts/jepsen-<adapter>-<fault>/`: the complete Jepsen `store/`
(packed as `store.tar.gz` to preserve timestamp paths containing colons),
(history, checker results and collected node logs), control/container logs,
commit, parameters, resolved Compose configuration, and exit code. GitHub
uploads these even after failure and retains them for 14 days. Per-run SSH
keys are temporary and are never included in artifacts. The exit trap collects
diagnostics and destroys the Compose project, including on SIGINT/SIGTERM;
a hard runner termination relies on the hosted VM's disposal.

The workflow runs on pull requests, main/master pushes, nightly schedules, and
manual dispatch. It uses read-only repository permissions and no repository
secrets. Container clock skew and OS/power failures require a separate VM suite.
