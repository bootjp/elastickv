# Improving steady-state read and write performance in Go

Status: In progress
Author: bootjp
Date: 2026-11-19

## Goals and baseline

Reduce server CPU time per successful request and the p99 latency clients experience on successful reads and writes. Keep Raft group membership and key ownership fixed. The first two candidates are allocations on point reads and the cost of persisting concurrent writes to the same group. Both must preserve gRPC response semantics, read consistency, and the default synchronous durability guarantees.

The initial phase of a short Linux diagnostic run measured RawKV with 1 KiB values and eight workers. All three nodes ran in one process on one VM, using Go 1.27.3 at commit `5ccfbc74a`. [Measurement log and configuration](../evidence/2026_11_19_steady_state_linux_baseline.md)

| Workload | Successful ops/s | p99 for successful requests | Successes / errors |
| --- | ---: | ---: | ---: |
| Read-only | 28,968 | 0.599 ms | 57,939 / 0 |
| Write-only | 308.9 | 37.012 ms | 625 / 0 |

On the same Linux VM, applying a single 64 B update to Pebble took a median of 2.755 ms/op with synchronous writes and 3.538 µs/op with sync disabled. These store-level medians and the end-to-end RPC p99 measure different stages and percentiles. They cannot establish how much of the RPC latency came from storage. Read request lifetime management had also changed by the time of the design review at `origin/main` commit `afe0a9f9d`. Establish a fresh steady-state baseline against the commit used for each implementation comparison. [Store measurements](../evidence/2026_11_19_steady_state_linux_baseline.md) and [read request implementation](https://github.com/bootjp/elastickv/blob/afe0a9f9dc1542b4a039a6a0392776a41370e1ef/kv/read_request.go)

## Point-read allocations

On a successful hit, the baseline `RawGet` converts the key and value to strings before calling `InfoContext`, even at the default Warn level. Pebble also copies the value during decoding. To isolate the cost of preparing log arguments, compare the baseline with a build that constructs those arguments inside `logger.Enabled(ctx, slog.LevelInfo)`. At each value size (64 B, 1 KiB, and 16 KiB), use the same sequence of successful hits, snapshot timestamp, and Warn setting. Measure the difference in bytes allocated and CPU time per operation. Collect allocation profiles in separate runs to identify allocations in the handler and in Pebble. Regression tests check the log fields at Info level, the distinction between an empty value and a missing key, and the number of KeyViz observations. [gRPC handler](../../adapter/grpc.go)

Measure latest reads, historical reads at a specified timestamp, forward scans, and reverse scans separately. For requests sent directly to the leader and those forwarded by a follower, record whether a lease was established, when reads fell back to ReadIndex, and how long each snapshot stayed pinned. For reads spanning multiple groups, also count read watermark updates and measure time spent waiting for a quorum. Before adopting a read optimization, verify repeat reads at the same snapshot with an intervening delayed write, reads during compaction, and pin release after cancellation. [Read watermark tests](../../kv/read_watermark_advance_test.go)

## Write batching and durability

Trace each write from admission through Raft proposal, write-ahead log (WAL) persistence, quorum commit, state machine (FSM) application, Pebble batch completion, and the client response. Correlating these events shows where queueing overlaps with I/O. Raft can persist multiple entries in one `Ready`. For each group, measure entries per save, syncs per successful write, batch size in bytes, and the age of the oldest queued request. Use those measurements to locate the sync cost in the Raft WAL or Pebble, then change one stage at a time. [Raft persistence](../../internal/raftengine/etcd/wal_store.go) and [Pebble apply path](../../store/lsm_store.go)

The first write candidate is the batch that `commitRaw` forms within a group before proposing an entry. Vary the request and byte limits and the wait for the first request. The current queue waits 500 µs before its first flush and normally takes at most 64 requests. Count calls by proposal path. If the target workload uses `commitRawOwned`, which retains a separate proposal context for each request, tuning the `commitRaw` queue cannot benefit that workload. [Raw write batching and proposal paths](../../kv/transaction.go)

### Apply boundaries and failure recovery

The FSM validates the raw requests in a Raft entry in order. On stores that support entry-level batching, it persists the successful updates and the applied index in the same batch. Exercise three requests to the same key in order. The first is a successful write, the second conflicts with it under optimistic concurrency control (OCC), and the third succeeds. Compare the ordered results, the value visible at each timestamp, and the post-commit apply notifications against sequential execution.

If a store read or batch commit fails, discard the updates and notifications for the entire entry and stop applying before advancing past its index. Pre-proposal batching must preserve the apply boundary between committed entries. Inject a failure in the middle of three consecutive entries. Verify the first entry's response and durable value, zero success responses and apply notifications for the failed and trailing entries, the durable applied index at restart, and the values after replay. [FSM tests](../../kv/fsm_batch_entry_test.go) and [completed-prefix recovery test](../../internal/raftengine/etcd/engine_storage_pipeline_failure_test.go)

### WAL persistence and peer responses

A `HardState` update such as a vote must trigger persistence even when there are no user writes to apply. On the synchronous path, save the snapshot first if present, then save the entries and `HardState`. Any peer response, apply operation, or `Advance` that depends on a sync must wait for it. On paths where WAL `Save` performs the sync internally, wait for `Save` to finish. On paths that issue an explicit `Sync`, wait for that call to finish.

Submit a vote-only `Ready` and verify the term and vote after reopening the WAL. Block and fail the save and sync operations separately, checking which dependent responses and state transitions can proceed. Verify that acknowledged updates survive process termination on either side of a sync, a leader change, and recovery from a snapshot. [WAL tests](../../internal/raftengine/etcd/wal_store_test.go), [asynchronous storage failure tests](../../internal/raftengine/etcd/engine_storage_pipeline_failure_test.go), and [store sync benchmark](../../store/lsm_store_sync_mode_benchmark_test.go)

For filesystem operations spanning multiple groups, take measurements from the operation journal. For a request with `RequireAllParticipants=true`, measure through the commit acknowledgment from the last participating group. If a secondary's response is lost after the primary commits, record the outcome as unknown. After recovery, use the operation ID to reconcile the recorded outcome with the effects in storage. [Two-phase commit outcomes](../../kv/sharded_coordinator.go) and [multi-group journal tests](../../internal/filesystem/operation_journal_multigroup_test.go)

## Experiment sequence and acceptance criteria

1. **Establish the baseline.** Run three Linux servers with the load generator on a separate host. Fix TLS, synchronous durability, the dataset, and leader placement. Test read-only, write-only, a 95:5 read/write mix, and scans. Cover uniform and hot-key access with concurrency limits of 1, 8, 32, and 128.
2. **Test point reads.** Change only how the handler prepares log arguments. Check whether the allocation savings reduce server CPU time per successful request or RPC p99.
3. **Test writes.** Use sync counts and queueing times to select one batching stage. Compare limits at that stage and check low-load p99, successful ops/s at high load, and operation histories after faults.

### Load generation and reporting

For each workload, set the scheduled arrival rates to 50% and 80% of the baseline's successful ops/s at saturation. Round down to whole ops/s with a minimum of 1. Before measuring the candidate, record the actual rates, concurrency limits, key-sequence seed, deadlines, dataset, primary metric, and target configuration.

Warm up for 30 seconds, then schedule requests for 120 seconds. At the end of the window, collect responses to outstanding requests through their deadlines. Queue a scheduled request if the concurrency limit prevents dispatch. Count it as unsent if its deadline expires in the queue. Use all scheduled requests as the denominator when reporting unsent requests, successes, definitive failures, and unknown outcomes. Track deadline expiry separately.

Measure successful-request latency from scheduled send time to response, and record dispatch delay separately. For mixed workloads, report success rates and p50, p95, and p99 separately for reads and writes. Measure successful throughput at saturation in a separate run. For each measurement window, sum CPU time and allocated bytes across all servers and divide each by the number of successes. If a window has no successes, report the duration of the stall and the request counts.

Collect 10 matched baseline/candidate pairs per configuration, alternating which build runs first. If a run has no successes or fewer than 1,000 successes for an operation whose p99 is being evaluated, defer the decision and rerun both builds with the same longer measurement window.

### Adoption and rollout

For point reads, the primary metric is server CPU time per successful request under uniform read-only traffic with 1 KiB values, a concurrency limit of 8, and an arrival rate of 80% of baseline saturation throughput. For the write candidate, use RPC p99 under the corresponding write-only workload.

For each metric, calculate the candidate-to-baseline ratio within each pair. Report the median of the 10 ratios and the interval from the second-smallest to the ninth-smallest ratio. Assuming independent pairs, this gives an approximately 97.9% distribution-free confidence interval for the median. A candidate must meet all of the following criteria.

- The upper bound for the primary metric is at most 0.90.
- The upper bound for each operation's p99 is at most 1.05 in every configuration, including the target workload.
- The lower bound for successful throughput at saturation is at least 0.95.
- In every fixed-arrival-rate steady-state run, both builds achieve 100% success with zero unsent requests and zero unknown outcomes.

Defer adoption if a history or durability check fails or an interval crosses its threshold. During baseline measurement, check these provisional criteria against the operational SLOs and record any adjustment before measuring the candidate. [Independent design review](../evidence/2026_11_19_go_steady_state_performance_review.md)

Roll out each candidate independently and in stages. Monitor history-check violations, increases in unknown outcomes, and latency regressions at low load. Before restoring the previous write batching limits, finish applying accepted batches and notifying callers of their results.

## Initial implementation and local measurements

On 2026-11-19, we implemented the Info-level guard on the successful `RawGet` path. We compared commit `5ccfbc74a` with the same commit plus the guard, using Go 1.27.3 on darwin/arm64 with `GOMAXPROCS=1`. Both builds ran the same local handler benchmark at the default Warn level. For each value size, the benchmark read the same 64 Pebble keys in the same order and verified all 100,000 hits per run. Each configuration had 10 paired runs with alternating execution order. [Benchmark](../../adapter/grpc_raw_get_test.go) and [measurements for all runs](../evidence/2026_11_19_steady_state_raw_get.tsv)

| Value size | Allocated B/read, median (baseline → candidate) | Allocations/read (baseline → candidate) | Process CPU time/read (median paired candidate/baseline ratio) |
| --- | ---: | ---: | ---: |
| 64 B | 552 → 376 | 9 → 5 | 0.796 |
| 1 KiB | 2,472 → 1,336 | 9 → 5 | 0.824 |
| 16 KiB | 33,193 → 16,696 | 9 → 5 | 0.616 |

Allocation figures cover the timed Go benchmark loop. Each value size ran in its own process. The CPU metric divides total user and system CPU time by the read count, so it includes startup, data loading, and shutdown. In a separate 16 KiB allocation profile under the same conditions, pprof attributed 1.53 GiB of allocations directly to `RawGet` in the baseline and 10 MiB in the candidate (flat allocation space). In the candidate, `store.decodeValue` accounted for approximately 97% of allocated bytes.

The three-server acceptance decision still requires measurements of RPC p99 over TLS, server CPU time, and successful throughput at saturation.
