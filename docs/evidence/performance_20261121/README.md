# Linux load ramp and Raft commit-status experiment

The reusable entry points are [the RawKV load command](../../../cmd/loadtest/README.md),
`./scripts/run-split-load-ramp.sh`, and `./scripts/analyze-load-ramp.py`.
The fixture checks manual same-group/cross-group Splits while offered load is
active. It does not provision resources or establish automatic node scaling.

## Scope and source

Base: `a2455239c177` (merged bounded preflight work). Three etcd-Raft nodes,
two groups, one Ubuntu 24.04 VM with 8 vCPU / 8 GiB. TLS and Forward authentication
are enabled, storage sync is unchanged, GOMAXPROCS=8. The load process is separate;
server profiles combine all three nodes. Every trial uses fresh data directories,
256 immutable seed keys, unique append-only writes, 1 KiB values, 50% planned
reads, 32 maximum active logical operations, a 2 s deadline per RPC, and immediate
readback of successful writes. Audit reads and readback RPCs are additional traffic.
All ACKs are audited through all three ingresses after Done and route convergence.

The candidate replaces `rawNode.Status().GetCommit()` with
`rawNode.BasicStatus().GetCommit()` in the pre-admission peer Step path. The pinned
Raft v3.7.0 implementation obtains both commit fields from `raftLog.committed`;
the full Status additionally clones configuration and leader progress. The event
loop ownership and the snapshot-protection decision still use the same scalar
before Step. No quorum, durability, admission or timestamp rule changes.

## Profiled exploratory ramp

`pilot-profile-summary.json` retains stage counts, all-attempt and successful
latencies, errors/drops, throughput denominators and the complete final audit.
Rates: 100, 500, 1000, 2000 logical operations/s for 90 s each. The Split starts
at stage 1. This diagnostic has mutex/block sampling and ten-second CPU captures;
it must not be used as an unprofiled A/B baseline. Its dataset grows between stages.

- Cross-group Done: 195.498 s; preflight upper bound: 8.880 s.
- Preflight: 46 completed families, 5,870 accepted versions.
- ACK ledger: 55,480 unique successful writes; 166,440/166,440 audit reads passed.
- Five post-Done paths: 600/600 write/readbacks each, zero errors.
- Availability is degraded: load drops and RPC errors occur during the ramp.
  A fixture PASS certifies its correctness gates, not zero errors or enough capacity.

The allocation profile is cumulative from process start, not a preflight-only
allocation rate. `preflight-alloc-top.txt` shows 696.57 MiB in `Inflights.Clone`;
the `RawNode.Status` subtree under `handleStepWithAdmission` accounts for
1,019.19 MiB. Profile hashes and the original diagnostic binary hashes are retained.
The raw profiles are in the task artifact directory
`/private/tmp/elastickv-load-ramp-20261121-artifacts/pilot-profile/profiles/`.

## Isolated allocation benchmark

`BenchmarkRejectedPeerStep` exercises the actual pre-admission handler with a
definitive rejection, avoiding log growth. It does not measure normal accepted
replication throughput. Three repetitions on the shared macOS M1 Max workstation:

| Role | Baseline B/op | Candidate B/op | Baseline allocs/op | Candidate allocs/op |
| --- | ---: | ---: | ---: | ---: |
| follower | 544 | 264 | 8 | 3 |
| leader | 1424 | 264 | 13 | 3 |

Raw logs are retained. Shared workstation timing is diagnostic; the deterministic
allocation reduction is the relevant result. Use the Linux comparisons for
end-to-end latency, error/drop and Split-duration claims.

## Fixed-load comparison protocol

The four immutable v2 trials run in A1 (baseline), B1 (candidate), B2 (candidate),
A2 (baseline) order. Each offers 500 logical operations/s for 300 s, with a
same-group Split at stage 0 followed five seconds later by a cross-group Split.
Profiles and mutex/block sampling are disabled. All other settings match the
scope above. Per-stage elapsed time includes drain; operation rates use that
elapsed time. The fixture requires Done and route convergence before the end of
the request-offering window, then audits all ACKs and runs five post-Done paths.
The p99 values are RPC latency over all attempts; successful-only latency and
scheduled-to-completion latency are retained in each JSON summary.

`comparison-source.json` identifies the measured source and verifies the new Go
files against commit `1eaf9a309`. The baseline uses that same harness with only
the scalar status read restored. Later commit `eb804ce63` tightens the harness's
OCC retry matching and deadline; it does not change production code, and these
formal trials continue to use the original immutable v2 binaries for both sides.

An earlier A1 ended after 5.91 s on a same-group Split catalog-version conflict,
before a complete load stage or audit. It is excluded from the comparison, not
counted as a passing data-integrity trial, and retained in
`comparison-rejected-occ/` in the task artifact directory. Both sides were
rebuilt with identical OCC handling before restarting the four-trial sequence.

This is two trials per version, not a statistical significance claim. Stage-wide
errors span Split phases and post-Done traffic; the summaries do not identify the
phase of every failed RPC. Unknown mutations are not retried or counted as ACKs.
The ACK audit does not establish correctness of unacknowledged outcomes or a
fault-injection/linearizability campaign. The five post-Done paths are a separate
60 s correctness check, not additional samples in the measured stage.

## Validation scope

- Load generator / CLI: race tests passed, including dropped-arrival accounting,
  per-ingress ACK audit, duplicate/missing ACK rejection and extra audit attempts.
- Monitoring / wiring tests passed; pprof is disabled by default and explicit
  non-loopback binding still requires its bearer token.
- Raft Step/snapshot/admission related tests passed. The complete etcd engine
  package also passed under `-race` (687.673 s).
- Repository Jepsen harness: 441 tests, 6,179 assertions, zero failures/errors.
  This is a harness regression suite, separate from the real Linux load trials.
- The upstream pinned `jepsen/redis` submodule's own `lein test` fails to load
  `jepsen.redis`; its test namespace is an upstream placeholder referencing a
  missing namespace. It is not reported as passing. The repository harness uses
  the submodule client source and passed as noted above.
- The broad Go suite initially failed two unchanged filesystem tests:
  `TestJournalReceiptsSurviveProcessKill` (child exited normally instead of the
  expected SIGKILL) and `TestReleaseAbandonedSkipsUnmountPastItsDeadline` (helper
  marker missing). Each passed three isolated repetitions; the complete
  `internal/filesystem` and `internal/filesystem/fuseadapter` packages then passed
  with `-p 1 -parallel 2 -count=1` (95.094 s / 9.319 s). The original broad-suite
  result is retained; these reruns do not turn its original exit into a pass.

## Unprofiled fixed-load results

| Trial | Read success/s | Write success/s | Read RPC p99 ms | Write RPC p99 ms | Capacity drops | Scheduler drops | Unknown writes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 (baseline) | 195.00 | 181.95 | 80.94 | 178.59 | 32270 | 172 | 4246 |
| B1 (candidate) | 199.20 | 185.49 | 77.04 | 173.29 | 29930 | 103 | 4334 |
| B2 (candidate) | 198.51 | 184.88 | 80.07 | 173.66 | 30340 | 107 | 4301 |
| A2 (baseline) | 194.64 | 180.53 | 81.92 | 181.73 | 32869 | 119 | 4296 |

| Trial | Preflight upper s | Versions checked | Cross-group Done s | ACK writes | Audit reads passed | Post-Done write/readbacks |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 4.641 | 4771 | 191.615 | 54611 | 163833 | 3000/3000 |
| B1 | 4.243 | 4800 | 193.875 | 55672 | 167016 | 3000/3000 |
| B2 | 3.951 | 4747 | 190.047 | 55494 | 166482 | 3000/3000 |
| A2 | 2.961 | 4517 | 186.651 | 54188 | 162564 | 3000/3000 |

Both paired comparisons show roughly 2% higher successful read/write throughput,
2.3–4.8% lower read RPC p99 and 3.0–4.4% lower write RPC p99. Capacity drops fall
7.3–7.7%. These are descriptive results from two pairs, not statistical evidence
of a general throughput improvement. The isolated allocation reduction is clear.

All four preflight upper bounds are below five seconds in this fixed 500 RPS
case. The candidate does not consistently shorten preflight: one pair improves
8.6%, the other worsens 33.4%, and the number of inspected versions differs.
Cross-group Done is 1.2–1.8% slower in the candidate pairs. The patch is therefore
not claimed to improve end-to-end Split time. The profiled exploratory ramp also
exceeded five seconds and is a different workload/measurement condition.

Result-unknown write errors persist (4,334 / 4,301 in the candidate stages,
versus 4,246 / 4,296 in the baseline). The capacity and latency improvements do
not establish that this availability issue is fixed. No such request is replayed.
Across the four formal trials, all 219,965 acknowledged unique writes passed
659,895 ingress audit reads, and all 12,000 separate post-Done write/readbacks
passed. No value mismatch was observed.

The broad `go test -p 2 -parallel 4 -timeout=30m ./...` completed with exit 1
because of the two filesystem failures listed above. All other packages passed,
including `adapter` (1,174.570 s), `internal/server` (594.224 s) and
`internal/raftengine/etcd` (592.219 s). See `go-test-all.log` and the separate
successful filesystem rerun logs; this is not a clean first-pass suite result.

## Current harness smoke and cleanup

After the four v2 measurements, the current `eb804ce63` harness ran a Linux
same-group Split at 50 offered operations/s for five seconds (four workers).
It passed all 124 ACKs / 372 ingress observations, the definitive-OCC predicate
cases, and the default-disabled pprof flag test. This smoke is separate from the
formal cross-group performance measurements; see `current-linux-smoke-summary.json`.

The full comparison, rejected first attempt, raw profiles, binaries and source
snapshots were collected under
`/private/tmp/elastickv-load-ramp-20261121-artifacts/`. Binary SHA-256 hashes were
checked against the Linux records. VM150 on `pve16intel` was shut down after
collection and Proxmox reported `status: stopped`; guest data were preserved.
