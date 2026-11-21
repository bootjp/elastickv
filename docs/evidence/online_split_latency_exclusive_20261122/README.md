# Exclusive VM 150 Split comparison, 2026-11-22

Final measured runtime: `c664c80bb954a462b145846b602f6e13849b0cbe`. Baseline: `8c2282837ecee54d51c664c67576a8f90e5f90e4`. The baseline completed before the candidate. Both executed server binaries were built with Go 1.27.3; native regression checks use the merged main toolchain, Go 1.27.4.

## Isolation and measurement

The other VM user explicitly handed VM 150 over after its workload, ACK audit and profiles finished. Process inspection confirmed no other load. That exclusive reservation covered both runs and the intervening candidate build/guard correction. Previous potentially concurrent runs are excluded from latency/throughput conclusions; their raw evidence and ACK observations remain in `../online_split_latency_20261122/` with a superseded notice.

The VM has 8 vCPUs/8 GiB. Three real etcd-Raft nodes and two groups share one server process and disk; the client is a separate process. TLS, Forward authentication and synchronous persistence are enabled. Profiles are disabled in both comparison runs. Both use the identical trace-driver executable, 100/300/500/1000 offered ops/s for 90 seconds each, concurrency cap 32, 50% reads, 256 seed keys, unique 1 KiB writes and 2-second RPC deadlines. ACKed writes also receive immediate verification reads. This stage experiment is separate from the older closed-loop 4-reader/4-writer benchmark.

Each run uses fresh stores. Primary RPC completions between cross-group Split request start and observed Done define the full-Split window. Success ops/s divides successful primary RPCs by window duration. p99 uses successful primary RPCs only, excluding immediate verification and final audit. The common window is the first 58 whole seconds after each request within the 300 ops/s stage. Done is polled every 100 ms. Every trace is recounted against original stage counts, error maps and p99; planned/admitted/drop invariants and distinct IDs are checked.

## Results

| Metric | Baseline 8c2282837 | Fixed c664c80bb |
| --- | ---: | ---: |
| Entire Split: seconds | 64.239 | 58.166 |
| Entire Split: read p99 (ms) | 12.535 | 11.109 |
| Entire Split: read successful ops/s | 149.878 | 149.967 |
| Entire Split: read errors | Unknown: 6 | 0 |
| Entire Split: write p99 (ms) | 22.646 | 22.221 |
| Entire Split: write successful ops/s | 149.925 | 149.950 |
| Entire Split: write errors | Unavailable: 3 | Unavailable: 1 |
| Equal-rate window: seconds | 58.000 | 58.000 |
| Equal-rate window: read p99 (ms) | 12.600 | 11.109 |
| Equal-rate window: read successful ops/s | 149.862 | 149.948 |
| Equal-rate window: read errors | Unknown: 6 | 0 |
| Equal-rate window: write p99 (ms) | 23.041 | 22.319 |
| Equal-rate window: write successful ops/s | 149.931 | 149.948 |
| Equal-rate window: write errors | Unavailable: 3 | Unavailable: 1 |
| Preflight sampled upper bound (s) | 1.642 | 1.510 |
| Preflight versions inspected | 2,080 | 2,073 |
| Full-load successful reads | 83,180 | 83,297 |
| Full-load successful writes | 83,170 | 83,303 |
| Full-load read errors | Unknown: 6 | 0 |
| Full-load write errors | Unavailable: 3 | Unavailable: 1 |
| Capacity drops before RPC | 795 | 621 |
| Scheduler drops before RPC | 3,846 | 3,778 |

This is one exclusive A/B comparison. Normal storage/scheduling variability remains; it does not establish statistical significance or a general throughput/pause guarantee. Versions differ by the reviewed fixes and the merged main changes; this is not an isolated one-line causal benchmark. Both fixture exits are 0. Client degraded-load exits remain separate and are recorded in the JSON.

## ACK audit after cutover

Baseline: 83,170 unique ACKs and **249,510/249,510** exact-value observations through all three ingress nodes. Candidate: 83,303 unique ACKs and **249,909/249,909** observations. Successful-write counts, distinct ledger IDs, ACK count, expected observations, audit attempts and audit successes all agree. Both also pass 600 additional writes/readbacks on each of five post-Done paths: 3,000/3,000 per version. Candidate sequence/seal sequence: 1130/1130.

## Cause and correction

- Earlier high-latency comparisons were confounded by another workload on the same VM. Exclusive access and both-run remeasurement replace those performance claims.
- A separate profile exposed ordinary data WAL-sync waits holding the metadata mutex. The final implementation retains main PR #1538's guarded fast path: only indexed batches whose every staged timestamp is newer than the published watermark can overlap metadata reads. Low/equal/mixed timestamps, control and readiness transitions keep their publication lock. The intermediate atomic retention mirror bypassed historical-read protection; it was removed. LSM code now matches merged main. Real-WAL RED/GREEN regressions cover six low/equal timestamp historical-read cases, mixed batches, watermark publication and ordinary higher-timestamp read progress.
- Transient cutover reads crossing gRPC lost their process-local sentinel and became Unknown. RawGet now carries Unavailable across the hop and retries safe reads with fresh scopes under a bounded budget. Pinned requests and mixed errors do not retry. A real gRPC-hop regression verifies this behavior.
- A proof-only mutation handoff could consume the entire client deadline. It now reserves up to 100 ms for the response. Actual post-submission timeouts/lost replies remain ambiguous and are not automatically replayed or relabeled as noncommit. The old 2,001.649 ms write Unknown cannot retrospectively establish its server-side commit outcome; no claim assigns all historical timeouts to one cause.

The six review corrections preserve halt-on-local-read-error behavior, retryable prepared-target locks, complete-member capability selection, audit cancellation, mixed-range capture and whole-entry request bounds. A committed entry over 256 requests preserves every write and invalidates migration instead of halting replicas.

## Evidence and reproduction

`comparison.json`, timestamped `baseline-events.jsonl`/`candidate-events.jsonl`, `manifest.json` and `validation.json` identify source/binary hashes, raw files, checks and exits. Full raw CSV/logs and profiles are preserved under the artifact root in the manifest. The pre-review trace driver is intentionally identical in both runs; its cancellation fix is separately regression-tested, and both measured audits completed without cancellation.

The initial exclusive controller ran only baseline. Its candidate gate was held before candidate execution for the final historical-read guard correction, and the waiting controller was stopped after baseline passed. The final candidate used a separate frozen controller with identical load settings. One empty launch failed before server execution because an executable bit was missing; that empty attempt is preserved and excluded. The candidate then ran from the same content hash with mode 0755. Neither a running script nor an executing server binary was rewritten.

To recompute, run `python3 compare.py` in the raw artifact root. The supplied A/B `measure.py` reproduces the same workload after supplying the fixed server binaries, identical driver and expected hashes in a fresh directory. The trace driver is reproducible from 8c2282837 using `rpc_trace_overlay/create_overlay.py`; pin `GOTOOLCHAIN=go1.27.3` when building comparison binaries. Prior source revisions, profiled/concurrent attempts and the never-executed held candidate are excluded from the final comparison.

## Remaining tail latency

The candidate had a transient slowdown 75–85 seconds after Split start, approximately 17–27 seconds after Done. The full 300 ops/s stage therefore worsened: read p99 12.056 → 24.765 ms, write p99 21.886 → 159.050 ms. These events are outside the predefined Split windows; they are retained in the full traces. Their cause was not established, and no storage/CPU attribution or uniform whole-stage latency improvement is claimed. The full workload nevertheless had zero candidate DeadlineExceeded/Unknown, and every ACK passed audit.

## Validation

The final native full-short Go suite passed after a bootstrap test observation race was corrected; the initial failed log is retained. Store/KV full race suites passed (343.040/187.981 s), the real-WAL guard regressions passed with race detection, and full commit-hook lint reports zero issues. All GitHub checks for measured runtime c664c80bb passed, including the available Redis fault jobs; this report does not claim a new live Split-specific Jepsen campaign. Root Jepsen harness: 442 tests / 6,184 assertions, zero failures/errors. The unmodified upstream Redis submodule unit test still fails on its missing namespace, as recorded in validation.json.

All raw files matched their VM SHA-256 values. All workload processes ended and VM 150 was shut down after collection. Publication adds only evidence and a bootstrap test observation fix to the measured production code.
