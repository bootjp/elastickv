# Default online RawKV Split validation

The implementation selects the online journal for new cross-group jobs on
persistent plaintext Pebble stores. Same-group splits remain catalog operations.
This directory records local verification separately from Linux cluster load.

## Observed result

| Metric | Baseline | Default online |
| --- | ---: | ---: |
| Cross-group Split to Done | 190.917 s | 82.371 s |
| Preflight sampled upper bound | 2.881 s | 1.421 s |
| Preflight versions inspected | 4,362 | 2,090 |
| Acknowledged writes | 47,398 | 49,761 |
| Write errors (Unavailable) | 3,284 | 3 |
| Write errors (Unknown) | 0 | 0 |
| Read errors (Unknown, route cutover pending) | 175 | 8 |
| Capacity/scheduler drops | 69,729 | 71,361 |
| All-node ACK audit | 142,194 / 142,194 | 149,283 / 149,283 |
| Additional post-Done writes/readbacks | 3,000 / 3,000 | 3,000 / 3,000 |

Both correctness fixtures exited 0. The load drivers exited 1 (`load degraded`)
because drops/transient errors remain; the fixture explicitly retains those
counts while checking every ACK. The candidate used online protocol v1 and
finished with `OnlineSequence == OnlineSealSequence == 1204`.

The Split finished 56.9% sooner. ACKed writes increased about 5.0% across this
workload, while successful reads decreased about 1.1% and offered operations
dropped more often. In particular, the 300 ops/s stage's successful-request
p99 worsened during online copying/cutover. This is not an error-free or uniform
latency improvement. Each 90-second stage observes different migration phases
in the two variants; the preflight version counts also differ because the
candidate reaches it sooner. One A/B pair is descriptive, not a significance
estimate or a controlled fixed-version microbenchmark.

Values below are baseline / candidate; throughput counts successful logical
requests and latency excludes failed requests. Readback/audit traffic is extra.

| Offered ops/s | Read success/s | Write success/s | Read p99 ms | Write p99 ms |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 50.0 / 50.0 | 50.0 / 50.0 | 38.7 / 8.4 | 72.0 / 16.7 |
| 300 | 149.0 / 145.6 | 131.3 / 145.7 | 13.8 / 77.1 | 24.0 / 179.0 |
| 500 | 180.8 / 173.5 | 163.9 / 174.1 | 84.8 / 81.2 | 189.2 / 166.8 |
| 1000 | 179.9 / 184.3 | 181.1 / 182.4 | 83.1 / 81.4 | 175.6 / 168.3 |

[Raw baseline stages](baseline.load.jsonl), [raw candidate stages](candidate.load.jsonl),
[baseline events](baseline.events.log), [candidate events](candidate.events.log),
[full structured summaries](candidate.json), and [binary/source identity](manifest.json)
retain errors, drops, timing and audit denominators. The stage containing the
observed transient errors includes both a same-group split and the cross-group
job; individual errors are not timestamped by phase, so these counts do not
establish which specific transition produced each error.

## Comparison protocol

Baseline: `5f6ef4e20` (the preceding diagnostic capture implementation, inactive
for normal Split jobs). Candidate: `8c2282837ecee54d51c664c67576a8f90e5f90e4` (see `manifest.json`).
Both use Go 1.27.3, `GOOS=linux GOARCH=amd64 CGO_ENABLED=0`, eight VM vCPUs,
8 GiB RAM, three real etcd-Raft nodes/two groups in one process, a separate
RawKV load process, TLS and Forward authentication, synchronous persistence,
GOMAXPROCS=8, fresh stores and profiles disabled. Offered rates are
100/300/500/1000 logical operations/s, 90 seconds each, 50% reads,
32 concurrent requests, 1 KiB values, two-second RPC deadlines, immediate
write readback and no load-driver mutation retries. Split starts in stage 1.
Every acknowledged write is audited through all three ingress nodes after
catalog convergence. Five post-Done ingress paths additionally write/read back.

One earlier baseline with 60-second stages ended load before Split finished;
its run is exploratory and excluded from the formal comparison. Candidates at `a15067ea4` and `ff29542ea` were stopped and excluded after
independent review found liveness defects. The accepted `8c2282837` includes
fixed catch-up goals, a lag budget and exact job validation before fencing. A passing
ACK audit is a correctness observation, not proof that every offered operation
succeeded. Capacity/scheduler drops and all error categories remain in results.
One VM does not demonstrate physical scale-out or a fault-injection campaign.

## Review passes

1. Data loss: source mutation, effects, sequence and applied index share a
   synced indexed batch. Unsupported data writes persist monotone invalidation
   before ordinary execution. Target versions/receipt/sequence commit together;
   Ready closes base and journal import. Quota exhaustion preserves user data.
2. Distributed failures: immutable source seals survive native membership
   changes; fresh current apply-member probes and indexed target reattestation
   replace old evidence. Capture-start nonce is durable and rediscoverable after
   response loss. Learner admission checks online capability. Root CAS and
   existing ownership/read-drain rules remain authoritative.
3. Performance: capture and transfer have explicit frame/byte/time budgets.
   Metadata is checked once per committed batch; the per-request sealed check
   runs only after the source fence is armed. Handoff retry runs only on the
   error path and bounds count, bytes and waiting time. No unknown write retries.
4. Consistency: transfer replays actual MVCC postimages, including low and
   repeated timestamps. Sequence gaps and changed duplicate receipts fail.
   Ambiguous legacy collection keys invalidate both possible moving owners.
   Final cutover checks the exact immutable seal on current source/target members.
5. Coverage: regressions exercise source/target configuration changes, a
   remaining sealed-tail frame, snapshot reattestation, unsupported writes,
   ambiguous keys, malformed persistent metadata, target freshness/locks,
   handoff cancellation/deadlines, multiple initial frames with a writer after
   every frame ACK, catch-up across ticks, stale goals after runner stalls,
   route/version OCC during goal refresh, and lost initial ACK.

## Limits

Only RawKV point entries are captured online. Intersecting transactions and
collection operations abandon migration while preserving ordinary writes.
The target must be a fresh dedicated group. Encrypted stores use the existing
path. Final handoff still uses creation/read fences; bounded request waiting
does not promise unlimited availability under overload or partitions.

## Reproduction

On a Linux checkout with Go 1.27.3 and dependencies installed, run:

```sh
GOMAXPROCS=8 ELASTICKV_LOAD_RATES=100,300,500,1000 \
  ELASTICKV_LOAD_STAGE_DURATION=90s ELASTICKV_LOAD_SPLIT_STAGE=1 \
  ELASTICKV_LOAD_SPLIT=cross ELASTICKV_LOAD_REQUIRE_ONLINE=1 \
  ./scripts/run-split-load-ramp.sh /tmp/online-split-results
```

For baseline, check out the baseline commit and omit `ELASTICKV_LOAD_REQUIRE_ONLINE`.
The VM used here has no Go SDK: both server test binaries and the shared driver
were cross-built with CGO disabled and copied to it. The two sides used the same
load-driver binary; its SHA and both server SHAs are recorded in `manifest.json`.
The runner creates fresh temporary stores and loopback listeners for each run.

## Verification and resource cleanup

The final clean `8c2282837` worktree passed [all Go packages with `-short`](go-short.log)
and [the related adapter race suite](race.log). Full lint reported zero issues;
protobuf breaking checks passed. The Jepsen **harness unit suite** passed 441
tests/6,179 assertions; this is separate from the real Linux load/audit and is
not a live fault campaign. [Verification details](verification.txt) retain
commands, scope and the intermediate readiness-test failure.

The dedicated VM150 was started from stopped, used only for these experiments,
and [normally shut down after collection](vm-shutdown.txt). The disk and static
binaries remain for reproduction; no experiment process was left running.
