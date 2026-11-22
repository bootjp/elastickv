# VM150 Sync throughput correction, 2026-11-22

Fixed measured source: `d0ee76e0a1268d9af39e64560ba3e78814e2fb06`.
Baseline: evidence head `18a509add`, unchanged production `5226ac757`.

The local group LeaderProxy and Internal.Forward previously used separate
TransactionManager flushers for the same engine. Small independent batches
fragmented Sync persistence. The production factory now borrows the group-owned
commit queue. The receiver timestamp epoch, final migration admission, dynamic
encryption wrapping, original owned context and terminal ambiguous outcomes
are preserved. No gather delay, batch-limit change, NoSync, mount change or
timestamp-publication guard relaxation is included.

The retained parallel-append optimization overlaps follower MsgApp with the
same local WAL Save only for eligible legacy-leader Ready batches. Save still
precedes owner reply handling, application, read/proposal ACK and Advance.

## Protocol and denominators

Exclusive VM150: 8 vCPU/8 GiB, GOMAXPROCS8, three real etcd-Raft nodes/two
groups in one server process and disk, separate identical client, Go1.27.3,
TLS and Forward authentication, Sync, fresh stores and profiles off. ABBA is
A1/B1/B2/A2 with identical bytes within each variant. Rates 100/300/500/1000
offered ops/s each last 90s; concurrency 32; read 50%; 256 seed keys; unique1-KiB
writes; 2s RPC deadlines; immediate write verification; no mutation retries.

The preregistered primary window is **45s** from cross-group
Split request start at 300 offered ops/s: min(60, floor(shortest Done within
that stage)), applied equally to all trials. Count primary RPC completions;
verification/audit latency is excluded. Full stages, full Split, the final10s
of stage300 and post-Done8s when available are retained in the JSON. Full-run
successful ops/s uses the sum of actual stage elapsed times, including drain.
Capacity and scheduler drops are separate from RPC errors.

## Primary comparison

| Trial | Read p99 ms | Write p99 ms | Read success ops/s | Write success ops/s | RPC errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| A1 | 11.630 | 22.460 | 149.911 | 149.911 | 0 |
| B1 | 10.278 | 19.567 | 149.978 | 149.956 | 0 |
| B2 | 9.425 | 17.341 | 150.000 | 149.956 | 0 |
| A2 | 13.539 | 25.571 | 150.022 | 150.022 | 0 |

Paired percentage changes (B versus A; negative p99 is better):

| Pair | Read p99 | Write p99 | Read success ops/s | Write success ops/s |
| --- | ---: | ---: | ---: | ---: |
| A1→B1 | -11.6% | -12.9% | +0.0% | +0.0% |
| A2→B2 | -30.4% | -32.2% | -0.0% | -0.0% |

## Full rate stages

| Trial | Offered ops/s | Read p99 ms | Write p99 ms | Read success ops/s | Write success ops/s | Capacity drops | Scheduler drops |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| A1 | 100 | 7.333 | 15.649 | 49.997 | 49.997 | 0 | 0 |
| A1 | 300 | 12.747 | 31.535 | 148.720 | 148.776 | 220 | 3 |
| A1 | 500 | 79.233 | 168.765 | 178.493 | 178.715 | 12762 | 60 |
| A1 | 1000 | 84.910 | 172.596 | 179.944 | 180.909 | 50739 | 6752 |
| B1 | 100 | 6.846 | 14.555 | 49.998 | 49.998 | 0 | 0 |
| B1 | 300 | 11.610 | 28.721 | 148.854 | 148.854 | 200 | 4 |
| B1 | 500 | 59.470 | 103.413 | 238.128 | 237.884 | 2059 | 71 |
| B1 | 1000 | 68.806 | 112.777 | 253.987 | 254.808 | 37358 | 6806 |
| B2 | 100 | 14.091 | 30.589 | 49.998 | 49.998 | 0 | 0 |
| B2 | 300 | 8.855 | 15.983 | 149.977 | 149.966 | 0 | 3 |
| B2 | 500 | 63.028 | 111.305 | 231.228 | 232.172 | 3179 | 72 |
| B2 | 1000 | 67.842 | 113.305 | 256.956 | 258.343 | 37124 | 6455 |
| A2 | 100 | 8.075 | 17.296 | 49.997 | 49.997 | 0 | 0 |
| A2 | 300 | 14.243 | 30.425 | 148.798 | 148.787 | 214 | 1 |
| A2 | 500 | 79.708 | 168.753 | 183.414 | 183.825 | 11867 | 40 |
| A2 | 1000 | 85.794 | 173.840 | 176.422 | 177.155 | 51548 | 6590 |

Whole-workload aggregation by actual duration:

| Variant | Read success ops/s | Write success ops/s | Capacity drops | Scheduler drops |
| --- | ---: | ---: | ---: | ---: |
| baseline | 139.494 | 139.791 | 127350 | 13446 |
| fixed | 172.422 | 172.784 | 79920 | 13411 |

## Errors and complete ACK audit

| Trial | Read errors | Write errors | ACK writes | Exact observations | Done s |
| --- | --- | --- | ---: | ---: | ---: |
| A1 | {} | {} | 50288 | 150864 | 61.982 |
| B1 | {} | {} | 62277 | 186831 | 49.532 |
| B2 | {} | {} | 62188 | 186564 | 45.589 |
| A2 | {} | {} | 50421 | 151263 | 76.776 |

All **124,465 fixed ACK writes** were read through all three ingress nodes
after Done and route convergence: **373,395/373,395** exact-value observations.
The comparison asserts ACK count equals the sum of successful writes and
expected=attempts=success=ACK×3, plus complete/verified flags. Each trial
also passes 600/600 write/readbacks on each of five post-Done paths; fixed
trials total 6,000/6,000. All four server fixtures exit 0. CSV replay checks
distinct operation IDs, per-stage counts/errors/p99 and offered=admitted+drops.

## Diagnosis and exclusions

The instrumented full-ramp diagnostic found about 1.02 requests/raw batch
at 300 offered ops/s. At 1000 the mean raw proposal wait was41.09ms, queue
wait21.38ms and entry-bearing WAL Ready persistence13.29ms. These are
interior complete-second aggregate bins over all engines, not per-RPC
attribution. Goroutine stacks show separate bare-engine and dynamic-wrapped
TransactionManager flushers for the same two leaders. Kernel traces include
actual fdatasync waits. CPU used roughly 2.3/8 cores, RSS roughly 550MiB,
and guest disk reads were zero; no ongoing handoff retry loop explains the
post-cutover high-rate degradation. Physical NVMe health and thin-pool
capacity checks found no thermal/media/capacity fault. The publication guard
seen in the mutex profile remains necessary and unchanged.

The standalone parallel-append ABBA (`d403e0c89`) is an excluded intermediate:
whole-workload successful read/write ops/s150.414/150.274→137.807/138.027,
inconsistent common-window p99, zero RPC errors, 99,467 candidate ACKs and
298,401 exact observations. It was not accepted as a performance improvement
and is not pooled with the final comparison. Instrumented diagnostics and
the single shared-queue pilot are also excluded from formal metrics. A
4ms backlog-gather prototype was never run on VM150 and is not in the PR.

## Validation

Full native short Go coverage passes. The initial non-short full run exited1
because its default10m budget expired in the two existing9,999-iteration adapter
sequences; all other packages passed. Repeating adapter unchanged with the
repository CI30m budget passes in724.101s. The original failed log is preserved.
Focused KV/server and related KV/adapter/server races, full lint and all25
fixed-source CI checks pass. Root Jepsen passes442 tests/6,184 assertions.
Real three-node Raw and transactional30s histories both pass Elle: Raw
1,335 reads/1,352 writes; transactional1,363 reads/759 writes. These histories
have no nemesis. CI Redis kill/pause/partition jobs are separate fault evidence.
The unmodified upstream Redis unit invocation still fails on its missing
namespace; it is not reported as a pass. See validation.json for exact scope.

The final host-isolation interval has zero firmware-service journal events.
The original masks/configuration and timer state were restored without errors;
restarting the overdue timer normally activates its service after isolation.
No workload/profiler remains, and the host lock is free.132 selected original
VM files, including expanded CSV hashes, match the collected bytes.

## Limits

At100 offered ops/s, B2 read/write p99 is14.091/30.589ms versus A2
8.075/17.296ms. This low-rate regression is retained; the correction is not
claimed to improve every trial at every rate. Both500/1000 stages and both
primary Split pairs improve p99.

Two trials per arm do not establish statistical significance. The VM is
exclusive; the physical NVMe remains shared. A fixed offered schedule does
not imply identical admitted key counts or captured migration work. Drops
remain explicit. No failure-free availability, hard pause bound or general
Split-duration improvement follows from these runs. Live Jepsen histories
and CI fault jobs are distinct from this no-nemesis performance comparison.

## Reproduction and preserved evidence

See protocol.json, binary-identities.json, comparison.json, per-trial summaries,
validation.json, self-review.md and archive-manifest.json. Source design records
are under docs/design/2026_11_22_implemented_{parallel_raft_append,shared_raw_commit_queue}.md.
The raw archive retains markers, run logs, compressed primary RPC CSVs,
post-run byte hashes, host-isolation receipts, diagnostics and overlays.

To replay, extract the raw archive, copy its formal-shared directory to a
scratch directory, then run `python3 compare.py /absolute/scratch/directory`.
The script validates all four runs and rewrites comparison.json and per-trial
summaries in that scratch directory. Do not substitute driver/binary bytes,
enable profiling, or weaken Sync when repeating the measurement.

After durable-copy verification and fresh replay, VM150 was normally shut down,
its stopped state verified, and the reservation released. See vm-shutdown.json.
