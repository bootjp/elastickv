# Online Split latency and review corrections, 2026-11-22

The measured revision is `663c2e2b755b1456eb83238e54afad6d4d921fed`. The baseline is the previous online release `8c2282837ecee54d51c664c67576a8f90e5f90e4`, rather than the older legacy implementation. The earlier legacy/online comparison remains in `../online_split_20261121/`.

## Diagnosis and change

A separate profiled run of the intermediate revision `6263ac0a6` recorded 87.66 cumulative seconds of RWMutex delay (96.77% of 90.59 seconds). Ordinary FSM data application held the store metadata mutex while waiting for WAL sync. This delayed watermark and admission readers. These cumulative goroutine wait times are not elapsed wall-clock time. `mutex-top.txt` preserves the profile summary; the diagnostic is excluded from performance comparisons.

Data-only commits now retain the writer serialization lock across staging, durable commit and watermark publication, while releasing the reader-facing metadata lock during WAL sync. Metadata-only commits and readiness transitions keep the exclusive publication boundary. The retention floor has an atomic read mirror. Tests pause a real Pebble WAL sync and check that old committed values and the old watermark remain readable, with the new watermark published after sync.

A captured residual read Unknown was a nested gRPC error containing `route cutover pending`. The process-local sentinel was lost across forwarding. RawGet now projects this transient condition to Unavailable and retries safe reads for a bounded interval with a fresh read scope; pinned reads and mixed errors do not retry. A real gRPC-hop regression fails before the change and passes afterward.

The previous write handoff budget could consume the same two seconds as the client deadline, leaving no time to return a proven rejection. It now reserves up to 100 ms of response time. A deadline after submission remains ambiguous and is neither relabeled as noncommit nor automatically replayed. The historical post-Done write Unknown took 2,001.649 ms with a 2-second deadline; its individual server-side commit outcome cannot be reconstructed from that old run. The evidence does not establish that every historical timeout had the same cause.

Review regressions cover local metadata read failure halting apply, lost target-prepare responses with transient locks, capability selection from both complete apply-member sets, cancelable ACK audits, mixed-range batch capture and the whole-entry 256-request bound. Oversized committed entries preserve all writes and invalidate the migration instead of halting replicas.

## Completed-run comparison and failed fresh baseline retry

The completed control was measured earlier in this task; the fresh fixed candidate was followed by another baseline attempt. That fresh retry failed to finish Split, so it is reported separately below, not substituted for a successful control or omitted. This is not a successful fresh B/A pair. Each attempt used a fresh store on VM 150 (8 vCPUs, 8 GiB). Three real etcd-Raft nodes and two replication groups run in one server process, with a separate load process, TLS/Forward authentication and synchronous persistence. Profiles were disabled. Both runs used the same load-driver binary and verified binary hashes before launch.

The offered rates were 100/300/500/1000 ops/s, 90 seconds each, concurrency cap 32, 50% reads, initial 256 keys, unique 1 KiB writes, and 2-second RPC deadlines. Successful writes also have immediate readback verification. These are the existing stage-load conditions; they are separate from the historical closed-loop 4-reader/4-writer benchmark.

Counts below select primary RPC completions between the cross-group request start and observed Done; successful ops/s divides by that window's elapsed time. p99 includes successful primary RPCs only and excludes verification/audit. The catalog is polled every 100 ms. The equal-rate window is the first 62 whole seconds after each Split request, entirely within the 300 ops/s stage. Different full Split durations otherwise change phase exposure. The trace recount must match every original stage's count, error map and p99 exactly.

| Metric | Baseline 8c2282837 | Fixed 663c2e2b7 |
| --- | ---: | ---: |
| Entire Split: seconds | 62.781 | 66.673 |
| Entire Split: read p99 (ms) | 15.608 | 12.867 |
| Entire Split: read successful ops/s | 148.214 | 149.956 |
| Entire Split: read errors | Unknown 4 | 0 |
| Entire Split: write p99 (ms) | 45.248 | 26.669 |
| Entire Split: write successful ops/s | 148.277 | 149.866 |
| Entire Split: write errors | Unavailable 4 | Unavailable 3 |
| Equal-rate window: seconds | 62.000 | 62.000 |
| Equal-rate window: read p99 (ms) | 15.840 | 13.025 |
| Equal-rate window: read successful ops/s | 148.210 | 149.952 |
| Equal-rate window: read errors | Unknown 4 | 0 |
| Equal-rate window: write p99 (ms) | 47.117 | 27.045 |
| Equal-rate window: write successful ops/s | 148.258 | 149.887 |
| Equal-rate window: write errors | Unavailable 4 | Unavailable 3 |
| Preflight sampled upper bound (s) | 1.460 | 1.460 |
| Preflight versions inspected | 2,032 | 2,048 |
| Full-load successful reads | 49,648 | 51,646 |
| Full-load successful writes | 49,765 | 51,856 |
| Full-load read errors | Unknown 4 | 0 |
| Full-load write errors | Unavailable 4 | Unavailable 3 |
| Capacity drops before RPC | 64,980 | 57,168 |
| Scheduler drops before RPC | 6,599 | 10,327 |

One completed run per version, with intervening diagnostic runs, does not establish statistical significance or a uniform latency improvement under all loads. Capacity and scheduler drops remain distinct from RPC errors. Both load drivers exit 1 for degraded load; fixture exit 0 requires the complete integrity audit and post-Done checks.

## Failed fresh baseline retry

The second 8c2282837 run ended with fixture exit 1 after 401.93 seconds. Split stayed in phase 3 (FENCE), with catalog split job conflicts logged, and never reached Done. Its full ACK audit and post-Done checks therefore did not run. It is not a passing comparison and does not establish data loss. The existing strict comparison validator rejects this run. A separate validator checks its complete four-stage raw trace against the reported metrics without inventing an audit or Done.

In the same first 62-second window of 300 ops/s traffic, the failed retry had read p99 68.607 ms / 145.210 successful ops/s and write p99 189.567 ms / 144.919 successful ops/s, with zero primary RPC errors. Its 46,166 reported write successes lack a final cutover audit. These observations show baseline variability and a liveness failure; they are kept in `baseline-retry-failed.json` and `baseline-retry-failed-events.jsonl`. The candidate's entire-Split time was 66.673 seconds versus 62.781 seconds in the completed control, so an overall Split speedup is not claimed.

## ACK integrity after cutover

Baseline: 49,765 unique ACKs, with 149,295/149,295 exact-value observations through all three ingress nodes. Candidate: 51,856 unique ACKs, with 155,568/155,568 observations. The full-load successful-write count, distinct ACK ledger count, expected observations, audit attempts and audit successes must all agree. Both also pass 600 additional write/readbacks on each of five post-Done paths (3,000/3,000 per run). Candidate sequence/seal sequence are 1655/1655.

## Evidence and exclusions

`comparison.json`, the timestamped `*-events.jsonl`, `manifest.json` and `validation.json` record the results and identities. Full raw logs/traces and diagnostic profiles are retained in the local artifact directory named in the manifest; their SHA-256 values are recorded there. `measure.py` reproduces the B/A execution after supplying the named fixed server binaries and identical driver. `compare.py` validates raw traces before computing comparisons.

The identical trace driver was built from 8c2282837 using the two instrumentation-only overlay files in `rpc_trace_overlay/`. The driver preallocates per-request records and writes the CSV after timed load; it adds no client mutation retry. Both servers use that exact binary. Its pre-review audit cancellation implementation is unchanged across the comparison; the current client cancellation fix is separately tested, and both measured audits must finish completely.

The earlier 6263ac0a6 pair is preserved separately: its common 62-second read p99 was 82.040 ms versus 15.840 ms for baseline, with write p99 188.746 versus 47.117 ms. That regression motivated the WAL-sync fix and is not pooled with this completed-run comparison. The earlier 487f16bb8 intermediate candidate that stalled on a capture nonce/ownership-field mixup was terminated, preserved and excluded. Its durable resume nonce is now a separate protobuf field from active ownership. Profiled diagnostic runs are also excluded. No live Jepsen fault-injection campaign is claimed by this report.

## Validation and reproduction

- `GOMAXPROCS=4 go test -p 2 -short ./...`: exit 0 (cached results included).
- Full store race suite: PASS 234.180 s; full KV race suite with socket access: PASS 184.235 s.
- Focused store/adapter race suite: PASS 4.053 s / 42.159 s. Regression RED/GREEN outcomes and commands are in `validation.json`.
- Full commit-hook lint: 0 issues. All runtime revision GitHub checks passed.
- Root Jepsen harness: 441 tests / 6,179 assertions, 0 failures/errors. This is a harness unit suite.
- The unmodified upstream Redis submodule suite exits 1 because its test imports the missing `jepsen.redis` namespace. Its template also contains `(is (= 0 1))`; it is not represented as passing.
- All measurement processes ended, all collected files matched their VM hashes, and VM 150 was shut down.

For the preserved artifacts, run `python3 compare.py` in the artifact root from the manifest, followed by `python3 summarize_failed.py`. The first command requires the completed control and candidate to pass their audit/trace invariants; the second adds the explicitly failed baseline attempt without treating it as complete.

To rebuild the trace driver, check out 8c2282837, then generate an overlay with `python3 rpc_trace_overlay/create_overlay.py /absolute/path/to/checkout > /tmp/rpc-overlay.json`. From that checkout, build `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -overlay=/tmp/rpc-overlay.json -o loadtest-window ./cmd/loadtest`. Both experiment arms must use the same resulting executable; regenerated build hashes can differ with toolchain and path changes. Build each server test binary from its recorded source revision. Place those binaries, the driver, expected hash JSON and `measure.py` in a fresh directory. Retain every attempt, including failures. Successful completed-run comparisons require matching `baseline/` and `candidate/` directories; this report's baseline is the preserved earlier completed control, not the failed retry.
