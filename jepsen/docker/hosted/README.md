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
| S3 | 9000 | Knossos linearizable object registers plus Elle read/write registers; final GET for every object through every node |
| gRPC RawKV / TransactionalKV | 50051 | Elle read/write registers plus Knossos; each Get/Put is one operation |
| Filesystem overwrite / rename | FUSE via SSH | Elle read/write registers plus Knossos and required final reads |
| SQS Standard | 9324 | Acknowledged sends must be received and deleted; 40s receive/delete-only drain |
| SQS HT-FIFO | 9324 | Delivery identities, no lost acknowledged sends, duplicates, or ordering violations; 40s drain |

An additional checker requires successful
writes, completed injection and healing operations for every target, and a
successful recovery read for every node/key pair. Missing operations, failed
reads, and unknown/invalid Jepsen results fail the job. Required recovery reads
must succeed; queue delivery correctness is checked across the bounded drain.

## Elle analysis and replay

Elle 0.2.8 is an explicit dependency. Redis and DynamoDB already use
`elle.list-append` through Jepsen's cycle-append checker with
`[:strict-serializable]`. S3 additionally uses `elle.rw-register` with that model,
alongside the existing per-object Knossos checker. Each S3 PUT/GET becomes one
single-operation transaction; separate HTTP operations are never combined into
an atomic multi-object transaction. Invocation/completion ordering and failed or
indeterminate outcomes are preserved. The generator writes unique values per
object, as required by Elle. Elle cannot infer every register anomaly, so its
verdict supplements Knossos. Neither checker is disabled.
Register replay rejects repeated or nil write values for a key rather than
analyzing a history that violates those inference assumptions.

SQS Standard and HT-FIFO retain their queue-specific checkers: receive/delete
histories are not list-append or read/write register transactions. They are not
reported as Elle passes. The normal and fault/stress matrices remain in place;
S3 runs both checkers in each existing job, including multipart workloads.

The complete `store.tar.gz` includes `history.edn`, `results.edn`, and any Elle
cycle explanations and Graphviz diagrams below checker `elle/` directories.
S3, gRPC and Filesystem save normalized `elle-history.edn` and `elle-results.edn` there.
Filesystem/SQLite replay uses `filesystem` to map its pre-created -1 initial
value to Elle's nil initial version. gRPC replay uses `grpc`; neither mode
combines separate API calls into an invented transaction.
Anomaly diagnostics are generated when anomalies are found; successful runs
need not contain a cycle graph. To recheck an extracted history from `jepsen/`:

```bash
lein run -m elastickv.elle-checker list-append /path/to/history.edn /tmp/elle-append
lein run -m elastickv.elle-checker s3 /path/to/history.edn /tmp/elle-s3
# For histories already containing [:r key value] / [:w key value] transactions:
lein run -m elastickv.elle-checker rw-register /path/to/history.edn /tmp/elle-register
```

Replay saves the normalized `elle-history.edn`, `elle-results.edn`, and any
anomaly diagnostics in the output directory. It exits successfully only for
`:valid? true`; invalid or unknown verdicts and malformed input fail. Use a
fresh output directory for each analysis. Graphviz is already installed in the
Hosted image and is required locally to render anomaly diagrams.

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
It runs nine adapter/workload entries against all three faults (27 jobs), using a 300s
main phase by default. Fault rounds repeat throughout the main phase and the
checker requires at least two completed injection/healing cycles per node.
The final healing phase also recovers an in-progress last round before reads
or queue draining begin.

Use manual dispatch with `faults-only=true` to run just these 27 jobs. A nonempty
`workloads-json` selects the existing local workloads and omits fault jobs,
unless `faults-only` is true. The normal PR workflow continues to run only
Redis for 90s with one round. Scheduled and PR jobs have separate concurrency
groups and do not cancel each other.

Stress `time-limit`, `rate`, `key-count`, `max-writes-per-key`, and
`max-txn-length` inputs are forwarded. Fault concurrency is six to cover the
three nodes; it is independent of the local S3 workload's concurrency setting.
S3 reuses `s3_budget.sh` to cap per-object histories at 32 operations while
preserving the aggregate requested budget. Effective settings are saved in
`parameters.txt`. SDK retries are disabled for DynamoDB/SQS only in this fault
harness so uncertain outcomes remain visible rather than replaying workload
operations. Fault-only DynamoDB/SQS HTTP requests have a 10-second total
deadline so an isolated connection cannot block the final healing phase. Both
the AWS protocol timeout key and the pinned transport implementation's qualified
key are set. Existing local workloads retain their default SDK retry policy.
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


## Additional data adapters

Public gRPC is exercised through a persistent Go JSON-line bridge built from
`cmd/jepsen-grpc-client`, using the generated RawKV and TransactionalKV clients.
Each worker has its own connection and helper process. RPCs have deadlines;
write errors are indeterminate and are never automatically replayed. Missing
registers remain nil; unexpected read payloads remain observations so the
checkers can reject them. The TransactionalKV name does not imply multi-request
transactions: PreWrite/Commit/Rollback are unimplemented, so this suite checks
its supported Get/Put calls individually.

Filesystem fault jobs use actual node-local FUSE mounts and SSH file operations,
including overwrite and rename modes. Only those jobs apply `compose.fuse.yml`,
which exposes `/dev/fuse`, adds SYS_ADMIN and disables the container AppArmor
profile for mount operations. Other adapter containers retain their existing
capabilities. A missing FUSE device fails rather than testing a plain directory.
Normal CI also checks SQLite through FUSE; it inherits the Filesystem Elle
checker. SQLite uses one worker per database because locks are local to a mount.

Normal CI preserves encryption off/on and all existing workloads, adds both
gRPC APIs, and always uploads `elle-adapter-histories-encryption-*`. The scheduled
normal matrix adds `grpc-raw` and `grpc-transactional`; Filesystem overwrite,
rename and SQLite entries retain their existing strict/legacy controls and now
also run Elle. Its fault matrix adds both gRPC APIs and both Filesystem modes.
SQS retains its dedicated delivery/acknowledgement checkers. Distribution,
RaftAdmin, backup, monitoring and internal replication/control RPCs do not expose
application register transactions and are not presented as Elle-tested data
adapters. Protocol coverage does not imply coverage of every command or API.

```bash
# Local cluster, from jepsen/ after building the helper at the repository root:
# go build -o /tmp/jepsen-grpc-client ./cmd/jepsen-grpc-client
lein run -m elastickv.grpc-workload --local --host 127.0.0.1 \
  --grpc-ports 50051,50052,50053 --grpc-api raw --concurrency 6
lein run -m elastickv.elle-checker filesystem /path/to/history.edn /tmp/elle-filesystem
```
