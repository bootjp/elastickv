# RawKV load and Split scaling diagnostics

`cmd/loadtest` drives the public gRPC RawKV API from a separate process. It
increases offered load in explicit stages; it does not provision nodes or change
an existing cluster's Split policy.

```sh
go run ./cmd/loadtest \
  --addresses=127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053 \
  --ca-file=/path/to/ca.pem \
  --rates=100,500,1000,2000 --stage-duration=90s --concurrency=32 \
  --read-percent=50 --value-bytes=1024 --seed-keys=256 \
  --max-requests=1000000 > load.jsonl
```

TLS verification is enabled by default. `--plaintext` explicitly selects a
plaintext test cluster; a bearer token cannot be sent with that option.
The default random run prefix owns a fresh namespace. Reusing an existing prefix
is rejected before seeding. Keys are distributed over `a`, `p`, and `z` so a
controlled Split at `m` and cross-group Split at `t` exercise outside, retained,
and migrated ranges. Ingress and key domain are assigned independently from
request IDs, before request outcomes are known.

Each stage has an absolute arrival schedule and bounded active concurrency.
A missed slot is counted as a scheduler drop; a slot without an available worker
is counted as a capacity drop. Missed arrivals are not replayed as a burst.
For a complete stage, `offered = admitted + scheduler drops + capacity drops`.
The next stage starts after in-flight requests drain. Stage elapsed time includes
that drain; successful RPC operations/s use that elapsed time as denominator.
JSON includes both all-attempt and successful-attempt latency/sample counts.
Read/write scheduled-to-completion latency is separate from RPC latency.
Dropped requests do not have RPC latency samples; drops must be considered
alongside p99, rather than treating their absence as low latency.

Reads verify immutable seeded values. Every measured write has a unique key and
value. An ACK means RPC success and `Success=true`; ambiguous mutations are never
retried or counted as ACKs. By default each ACK is immediately read back; those
additional verification RPCs have separate statistics. After the measured stages,
every ACK is audited through every supplied ingress. Stage write successes, ledger
cardinality, unique IDs, expected observations, attempted observations and successful observations are
cross-checked. An empty write audit is not a passing write test. Read-only runs
report an empty write audit and validate their measured immutable reads instead.
Tracking and latency memory are bounded by the explicit planned-request limit.

An optional `--audit-ready-file=/fresh/run/done` delays the audit until an external
controller has confirmed Split Done and routing convergence and creates that file.
A pre-existing marker is rejected, and cancellation or timeout fails the wait.
Without this option the standalone command audits after its own stages drain;
that alone does not prove an independently running Split has completed.
The command exits nonzero for RPC errors, mismatches, drops or incomplete audits,
while preserving separate availability and acknowledged-write audit results.

## Reproducible Linux fixture

```sh
ELASTICKV_LOAD_RATES=100,500,1000,2000 \
ELASTICKV_LOAD_STAGE_DURATION=90s \
ELASTICKV_LOAD_SPLIT=cross \
  scripts/run-split-load-ramp.sh /absolute/path/to/results
```

The opt-in `TestLinuxSplitLoadRamp` starts three real etcd-Raft nodes and two
groups with TLS and Forward authentication, on one Linux process/disk. The load
command is a separate process. This measures load scaling on that placement;
it does not establish multi-host throughput scaling or failure isolation.
`ELASTICKV_LOAD_SPLIT` selects `none`, `same`, or `cross`.
The requested `ELASTICKV_LOAD_SPLIT_STAGE` starts the Split at that stage's event
(default: stage 1 for a ramp, stage 0 for a single fixed rate). A cross-group case
first performs the same-group Split, then moves the right child's upper range.
The fixture confirms Done and all-node route convergence before releasing the
ACK audit. It rejects request-offering windows ending before Split completion (drain is excluded), and also
checks the existing five-path post-Done write/readback workload.
Saturation intentionally produces drops. The fixture accepts the CLI's explicit
`load degraded` exit only after its complete ACK audit passes; a fixture PASS
therefore proves these correctness checks, not zero errors or adequate capacity.
Use the stage errors, successful operation rates and drop counts to judge capacity.

Use a ramp to locate saturation. For comparisons, use a single fixed rate and
fresh data directories for each run, keeping initial data, offered schedule,
leader placement, value size, concurrency, sync mode and timeouts identical.
Compare `none`/`cross` controls and baseline/candidate in alternating order.
Ramp stages grow the stored dataset, so their differences alone do not identify
a rate-dependent bottleneck. Metrics are stage-wide, not per-Split-phase p99.

## Opt-in profiles

The normal server and demo have no pprof listener by default. Explicitly pass
`--pprofAddress=127.0.0.1:6060` to enable `/debug/pprof/`; an external bind still
requires the existing bearer-token configuration. CPU and heap examples:

```sh
curl -f 'http://127.0.0.1:6060/debug/pprof/profile?seconds=20' -o cpu.pprof
curl -f 'http://127.0.0.1:6060/debug/pprof/heap' -o heap.pprof
```

For the Linux fixture, `ELASTICKV_LOAD_PROFILE_DIR=/absolute/path/to/profiles`
explicitly enables a loopback-only debug listener plus mutex/block sampling and
captures CPU/heap/mutex/block at preflight and peak load. Profile window start and
duration are logged; a ten-second CPU window may include subsequent phases.
Busy captures are logged as skipped. `ELASTICKV_LOAD_PROFILE_ONLY=preflight` or
`peak` separates diagnostic trials when their windows would overlap. These are
process-wide profiles of the three server nodes, excluding the load generator.
Run formal comparisons without either profile environment variable; sampling and
capture overhead belong only to diagnostic runs.

Summarize the retained log without discarding error/drop counts:

```sh
python3 scripts/analyze-load-ramp.py /absolute/path/to/results/run.log > summary.json
```

The wrapper retains host/runtime information, binary hashes, the original test
exit code, tracked changes and untracked source for reproducing an uncommitted
run. Store results outside the repository to avoid archiving previous results.
