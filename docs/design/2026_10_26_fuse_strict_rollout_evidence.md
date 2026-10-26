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

## Crash and mounted evidence

The `filesystem-node-crash` job writes and fsyncs a file, fsyncs its parent
directory, kills one Raft node, checks the surviving mounts, restarts the
killed node with strict session recovery, and checks the restarted mount.
This job passed in [run 40319573770](https://github.com/bootjp/elastickv/actions/runs/40319573770).

The test establishes durability after successful fsync through one node
process crash. It does not inject a crash at each boundary between Raft WAL
persistence, FSM application, receipt publication, FUSE reply, and fsync. It
also does not crash a multi-group deployment. Those cases remain separate
acceptance requirements.

All 23 jobs in [run 40320716183](https://github.com/bootjp/elastickv/actions/runs/40320716183)
passed at commit `a33dad1821631f8f43a6578aa3125712161108d5`, including the
mounted crash test, SQLite and filesystem workloads, and both benchmark jobs.

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

The strict mount now publishes `elastickv_fs_operation_journal_sessions`,
`elastickv_fs_operation_journal_pending`, and
`elastickv_fs_operation_journal_retained_bytes` from a fenced snapshot. These
gauges make pending work and retained storage visible during a canary. A
failed scan is logged; the last gauge value must not be treated as fresh
evidence after a scan failure.

## Remaining rollout gates

1. Exercise lost replies and process or node crashes at every publication and
   barrier boundary, including multi-group primary and secondary recovery.
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
