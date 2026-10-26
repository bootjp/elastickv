# Strict FUSE durability and rollout evidence

Status: validation in progress. Strict mode remains opt-in.

## Scope of the current evidence

The mounted tests use three Linux processes, three FUSE mounts, and one
three-voter Raft group. SQLite uses rollback journals on one mount. The tests
do not establish shared-database locking across mounts or SQLite WAL safety.

## Transaction decisions

An operation record is the transaction primary. Its generation and fenced
attempt IDs survive restart. A committed primary can outlive the coordinator's
response. Therefore an error returned by the primary commit call is not, by
itself, an abort decision.

The coordinator now leaves prepared secondary intents intact when the primary
response is missing. Lock resolution reads the primary decision before
committing a secondary. A definitive, typed apply verdict or pre-append
leadership rejection can still abort the
prepared intents. Nil commit responses are unknown, including on the
single-group path.
The etcd Raft engine now returns `ErrLeadershipLost` for proposals still
pending when the leader steps down. Those proposals may have entered the log,
so `ErrNotLeader` is reserved for the pre-append check.
The Redis error adapter does not convert a wrapped unknown outcome into
`NOTLEADER`, which clients may interpret as permission to retry a write.

`TestLostPrimaryReplyResolvesPreparedSecondaryFromDurableDecision` exercises
the lost-response path with two live Raft groups and their lock resolver.
`TestStrictLostPrimaryCommitReplyDoesNotAbortPreparedParticipants` and
`TestStrictDefinitivePrimaryConflictAbortsPreparedParticipants`,
`TestPrimaryRouteVerdictAbortsPreparedParticipants`, and
`TestPrimaryCommitDecisionClassification` cover the two error classes.
`TestJournalGenerationFenceSurvivesProcessKill` verifies that a
restarted store still rejects an old attempt after its durable fence.
`TestJournalCommittedAttemptDecisionSurvivesProcessKill` verifies that a
committed attempt, stored result, and effect survive a process kill; retrying
the same attempt returns that result without publishing another effect.
`TestMultiGroupPrimaryDecisionSurvivesProcessKill` runs two independent
single-voter Raft groups with Pebble stores, kills their shared process after
the primary COMMIT but before the secondary COMMIT, then reopens both groups.
The test invokes the restarted resolver's decision step directly; it reads
the primary decision and commits the prepared secondary. The test does not
exercise the periodic lock scan. It covers a precise cross-group crash
boundary but does not
model a three-voter group or a crash during WAL persistence.

## Crash and mounted evidence

The `filesystem-node-crash` job writes and fsyncs a file, fsyncs its parent
directory, kills one Raft node, checks the surviving mounts, restarts the
killed node with strict session recovery, and checks the restarted mount.
This job passed in [run 40319573770](https://github.com/bootjp/elastickv/actions/runs/40319573770).
The candidate extends the same job to acknowledged rename, chmod, unlink,
mkdir/rmdir, and truncate followed by file and directory fsyncs. The expanded
job passed in [run 40328639750](https://github.com/bootjp/elastickv/actions/runs/40328639750)
at `48755a5b`.

The test establishes durability after successful fsync through one node
process crash. It does not inject a crash at each boundary between Raft WAL
persistence, FSM application, receipt publication, FUSE reply, and fsync. It
also does not crash a multi-group deployment. Those cases remain separate
acceptance requirements.

All 23 jobs in [run 40320716183](https://github.com/bootjp/elastickv/actions/runs/40320716183)
passed at commit `a33dad1821631f8f43a6578aa3125712161108d5`, including the
mounted crash test, SQLite and filesystem workloads, and both benchmark jobs.
All 23 jobs also passed in
[run 40322309957](https://github.com/bootjp/elastickv/actions/runs/40322309957)
at commit `daebcb77fb4715f51c6a006870ebaa9fc5ec29a7`.

## Performance evidence

The first paired measurement used independent GitHub runners in
[run 40320716183](https://github.com/bootjp/elastickv/actions/runs/40320716183).
Each operation wrote 4 KiB and called `fsync`. The hot-inode case used four
writers with disjoint offsets and no client retry.

| Case | Legacy ops/s | Strict ops/s | Legacy p99 | Strict p99 |
| --- | ---: | ---: | ---: | ---: |
| One writer, 40 operations | 105.9 | 40.8 | 12.7 ms | 30.7 ms |
| Four writers, 160 operations | 86.4 | 30.4 | 82.7 ms | 475.1 ms |

The three-trial paired measurement in
[run 40322309957](https://github.com/bootjp/elastickv/actions/runs/40322309957)
used the same workload and commit on separate fresh runners. Across the three
trials, the median serial throughput was 59.2 ops/s in legacy mode and 36.1
ops/s in strict mode; median serial p99 was 22.7 ms and 40.0 ms. The median
hot-inode throughput was 53.4 and 32.1 ops/s; median hot-inode p99 was 130.5
ms and 250.2 ms. These comparisons are descriptive because the runners are
separate and scheduling noise is not controlled.

These are observations, not performance limits. The workflow retains the JSON
artifacts for all trials. A rollout
limit must use repeated matched measurements and explicit maintainer approval,
as required by the parent design. The current hot-inode result warrants review
before choosing that limit.
The benchmark now records a per-worker throughput fairness ratio for the
hot-inode case. A separate two-group layout places operation records and file
effects on different Raft groups. Its first run
[40325784929](https://github.com/bootjp/elastickv/actions/runs/40325784929)
failed with a `txn locked` FUSE error. A subsequent run
[40327671052](https://github.com/bootjp/elastickv/actions/runs/40327671052)
returned EIO while the mount was up but the catalog watcher still reported a
connection refusal on the local Raft listener. The benchmark setup now waits
for both listeners and a successful mounted `stat`. Neither failed run yields
a valid two-group performance measurement.
Run [40328639750](https://github.com/bootjp/elastickv/actions/runs/40328639750)
reached both listeners and still failed with a typed `txn locked` error during
the first write. A deterministic two-group Raft test reproduced a second local
journal commit reaching PREPARE while the first held the mount session lock.
The same test passed after serializing this service's journal commits. This
does not serialize separate mounts and does not yet establish a passing
mounted two-group workload or an acceptable performance cost.
Run [40330054840](https://github.com/bootjp/elastickv/actions/runs/40330054840)
then failed while reading a session row locked by an in-flight journal
transaction. Journal reads and definitive prewrite lock rejections now retry
the same timestamp and attempt with a bounded backoff; a typed unknown
transaction outcome is never retried. The amended two-group benchmark passed
in [run 40331001888](https://github.com/bootjp/elastickv/actions/runs/40331001888)
at `fd9a2294`. Across three trials, median serial throughput was 67.1
write/fsync operations per second with a 28.7 ms median of trial p99 values.
Four writers on one inode achieved 59.0 operations per second, 126.5 ms p99,
and a 0.954 median minimum-to-maximum worker throughput ratio. This was one
process with two single-voter Raft groups and one FUSE mount; it is not a
multi-mount or three-voter performance result. The expanded acknowledged
mutation node-crash job also passed at this SHA.
The same run's separate single-group runners measured median legacy serial
throughput of 113.0 operations per second (p50/p95/p99 8.2/12.0/14.3 ms)
and strict serial throughput of 28.3 (34.1/42.1/45.8 ms). For the hot inode,
legacy reached 79.0 operations per second (43.4/83.4/98.4 ms) and strict
reached 25.6 (136.5/242.5/309.4 ms). These are separate runners, so the
ratios are descriptive rather than a controlled regression threshold. The
two-group result uses a different one-voter topology and cannot be compared
directly to the three-voter single-group result. A two-group legacy job is
being added to supply a same-topology baseline.
The first paired two-group run
[40332154333](https://github.com/bootjp/elastickv/actions/runs/40332154333)
did not produce valid throughput results. The legacy hot-writer job reported
a primary commit timestamp mismatch on an open-handle fence. The strict job
reported operation-record errors during acknowledgement and subsequent writes
in the hot-writer trial. A possible duplicate-retirement path was identified:
the ACK path now checks the durable session retirement fence before consuming
the record's retained-byte reservation again. Its effect on the mounted
failure remains unverified. Both jobs must pass before the two-group
comparison can be used for rollout.
The long run
[40337556125](https://github.com/bootjp/elastickv/actions/runs/40337556125)
again failed the Redis workload and both two-group benchmarks at `f2cbf5cb`.
The strict benchmark exposed a gap between secondary PREPARE and primary
PREPARE: a foreground read treated the absent primary lock as an abort and
removed the prepared secondary. The resolver now keeps that secondary pending
until its TTL expires or a durable primary decision is visible. A two-group
Raft regression reproduces concurrent journal admissions; a second regression
checks the gap directly. The legacy benchmark exposed a primary identity
collision between distinct open references. Open and release now use the
per-reference key as their transaction primary. The Redis history classified
a transaction that lost its leader after dispatch as a definite failure,
although a later read observed its writes. The Jepsen client now records
that leadership outcome as indeterminate. The corrected candidate still
requires a complete CI run before the two-group measurements can be used.
The same long run's SQLite job also became inconclusive: operation journal
ACK accounting failed, subsequent operations hit the journal quota, and the
final database reads could not open their files. It did not report a
linearizability anomaly. The new resolver behavior may address the missing
session update, but this must be established by a passing long run.
In the first rerun at `ed5efef1`, the legacy two-group benchmark still
returned a typed `txn locked` error from a concurrent open. Open now waits
and rebuilds its transaction after that transient lock, within its existing
30-second retry budget; unknown outcomes still stop without replay.
The same rerun passed the Redis, SQLite, and strict two-group jobs. Its
remaining long-running rename job was superseded by the next full run.
Both two-group benchmark jobs passed in
[run 40345042339](https://github.com/bootjp/elastickv/actions/runs/40345042339)
at `711a7ba5`. Across three trials, the median legacy serial throughput was
112.1 ops/s with a 12.6 ms median trial p99; the strict values were 80.5
ops/s and 39.5 ms. For four writers on one inode, the median legacy
throughput was 83.9 ops/s with an 88.6 ms p99 and 0.914 worker fairness;
the strict values were 54.9 ops/s, 276.3 ms, and 0.880. These jobs use
separate runners, so the comparison is descriptive. The full stress run
failed: Redis, Redis ZSet, and DynamoDB were cut off by a 720-second shell
timeout despite their requested 1800-second workload limit. The SQLite job
became inconclusive after operation journal ACK accounting errors and quota
exhaustion. The workflow now allows 3600 seconds for these protocol jobs,
including checker time. The numbers do not establish a rollout threshold.
The push test in
[run 40346594962](https://github.com/bootjp/elastickv/actions/runs/40346594962)
also failed a mounted strict-mode quota test when a FUSE request was canceled
after admission and before publishing its attempt decision. Every journaled
mutation now uses a separate bounded decision context after BeginAttempt;
request cancellation can no longer abandon that phase immediately. The
formerly failing mounted test passed ten consecutive runs in a local Linux
container with `/dev/fuse`, and the complete FUSE adapter package passed
there. The corrected push test
[40363982074](https://github.com/bootjp/elastickv/actions/runs/40363982074)
passed at `7323b1f9`; that SHA also includes a test-only fix for an unrelated
HLC renewal test race.

The 30-minute stress run
[40362040528](https://github.com/bootjp/elastickv/actions/runs/40362040528)
at `3e3f4ab4` passed 24 of 25 jobs, including the formerly timed-out Redis,
Redis ZSet, and DynamoDB jobs. Its SQLite job remained inconclusive. The
first server-side failure was an operation-record error at sequence 1275;
ACK retirement then reported that a record's reservation exceeded the
session's retained usage. Several final SQLite reads could not open their
database files, so the checker returned `:unknown`. No linearizability
violation was reported, but this run does not close the SQLite gate or
establish a rollout threshold.

The next scheduled strict run on main,
[40375070553](https://github.com/bootjp/elastickv/actions/runs/40375070553)
at `a299b49a`, passed SQLite but returned `:unknown` for filesystem
overwrite and rename. Their final reads failed with FUSE I/O errors. The
overwrite server also logged an exhausted HLC ceiling; the rename server
logged operation-record errors during ACK retirement. Neither checker
reported `:valid? false`. This main run predates the post-admission request
cancellation fix in the pull request, so its failures do not establish the
outcome of the corrected candidate.

For a mode comparison, the targeted legacy run
[40378658363](https://github.com/bootjp/elastickv/actions/runs/40378658363)
at `2f542c4f` ran the same 30-minute overwrite, rename, and SQLite workloads
with strict mode disabled. All three checkers returned `:valid? true`.
The production Go code is unchanged between this commit and strict run
`40362040528`; the intervening changes are a test, this document, and
workflow inputs. Both runs used 512 keys with 16 writes per key for each
filesystem workload, and 128 keys with 16 writes per key for SQLite. The
strict SQLite run was inconclusive while its legacy counterpart passed.
These are separate runners and one trial per mode, so they do not quantify a
failure rate or prove a successful-operation consistency regression. They
provide evidence of an operational regression that blocks default-on strict
mode. Repeat the comparison after correcting the ACK accounting failure.
Require complete final reads in every strict checker.

The strict mount now publishes `elastickv_fs_operation_journal_sessions`,
`elastickv_fs_operation_journal_pending`, and
`elastickv_fs_operation_journal_retained_bytes` from a fenced snapshot. These
gauges make pending work and retained storage visible during a canary. A
failed scan is logged; the last gauge value must not be treated as fresh
evidence after a scan failure.

## Remaining rollout gates

1. Exercise lost replies and process or node crashes at every publication and
   barrier boundary, including a multi-group secondary crash and three-voter
   primary and secondary recovery.
2. Review the full race suite, Linux mount tests, Jepsen histories and
   checker output for the same candidate SHA. The scheduled workflow archives
   Jepsen histories for successful as well as failed workload jobs.
3. Repeat the legacy and strict benchmarks. Record single- and multi-group
   latency, throughput, conflicts, fairness, and retained journal bytes.
   Approve numerical canary limits from those measurements.
4. Run an opt-in canary with a verified backup and observable pending sessions.
   Stop the canary on data mismatch, unverifiable fsync receipts, or unresolved
   work that does not drain after recovery. Resolve or cancel every pending
   operation and retire its session before a supported downgrade.

Default-on strict mode is blocked until these gates close. A successful
single-group smoke run cannot substitute for the multi-group crash campaign.
