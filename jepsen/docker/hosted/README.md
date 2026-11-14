# Hosted fault tests

The `Jepsen Hosted Faults` workflow runs on Ubuntu GitHub-hosted runners. Each
matrix job creates three SSH-managed database containers and a separate Jepsen
control container on a dedicated Docker bridge. No ports are published. Only DB
containers receive `NET_ADMIN`; none receive `SYS_TIME` or privileged mode.
All three voters start with the same explicit `--raftBootstrapMembers` list;
this suite tests a fixed single-group cluster rather than membership changes.

The jobs exercise `partition`, `kill` (SIGKILL), and `pause` (SIGSTOP) separately.
Each node is targeted once while the other two retain a quorum. Each injection
is followed by healing/restart/resume. The last phase explicitly heals again,
waits 10 seconds, then reads every key from every node. Redis may forward reads
to the leader, so these are checks through each node's client endpoint, not
direct inspections of each follower's local storage.
The workload tracks emitted key IDs, including keys introduced when Elle
rotates its active key window, and reads them all in a final transaction per
node. Graphviz and gnuplot are included for checker diagnostics.

The existing strict-serializability list-append checker analyzes the complete
history, including recovery reads. An additional checker requires successful
writes, completed injection and healing operations for every target, and a
successful recovery read for every node/key pair. Missing operations, failed
reads, and unknown/invalid Jepsen results fail the job. Each worker has a 10s
client timeout; a failed final read is not silently ignored or retried away.

## Run locally on Linux

Requirements: Docker Engine with Compose v2+, Git, Bash, and `ssh-keygen`.
Java, Leiningen, Go, and node dependencies are installed in the image. Initialize
the Redis submodule before running:

```bash
git submodule update --init --recursive
bash jepsen/docker/hosted/run.sh partition
bash jepsen/docker/hosted/run.sh kill
bash jepsen/docker/hosted/run.sh pause
```

The default main phase is 90s, with a 5s warmup and 5s fault/recovery intervals.
Set `JEPSEN_TIME_LIMIT` to extend the main phase; it must exceed 35s. Fault
operations take time in addition to their intervals: the evidence checker
rejects a run that ends before all three cycles complete. Builds, cluster
startup, recovery, and analysis add to the total job duration.

Results are written to `artifacts/jepsen-<fault>/`: the complete Jepsen `store/`
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
