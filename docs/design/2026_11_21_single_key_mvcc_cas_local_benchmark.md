# Local single-key MVCC CAS benchmark

This fixture runs the transactional Get and CAS gRPC handlers against a real single-voter etcd Raft group and synchronous Pebble FSM persistence.
The client and server share a process and use gRPC over `bufconn` without TLS.
All update arms use the sharded coordinator and leader-issued HLC timestamps, with no dedicated TSO group.
The fixture activates and verifies the replicated commit timestamp fence and migration version identity marker before sending writes.
Its admission checks validate the live sole-voter configuration and both applied markers on each CAS submission and versioned read.

Run from the repository root after building the CAS and opt-in Get implementation.

```sh
go test ./adapter \
  -run '^TestMVCCCASContention(Handles)?$' -count=1 -v

go test ./adapter \
  -run '^$' -bench '^BenchmarkMVCCCASContention/(known|stale100|hot8)$' \
  -benchtime=10x -count=1 -benchmem

go test ./adapter \
  -run '^$' -bench '^BenchmarkMVCCCASContention$' \
  -benchtime=1000x -count=1 -benchmem
```

One benchmark iteration is one round. The fixture uses fixed 1 KiB values with distinct replacements for each contender.
The test-only 1PC frontend uses the same fenced versioned-read helper as the public Get.
It checks the observed version and submits the replacement with that snapshot's original timestamp and target in the OCC read and write sets.

| Arm | Timed work with the original version already known |
| --- | --- |
| CAS | Public CompareAndSwap |
| Fresh | One RPC that reads, checks the version and submits 1PC |
| TwoCall | Acquire a fresh read handle, then submit 1PC if the version matched |
| Retained | Submit 1PC against the original read handle |

Handles are bound to the client, key and observed version.
They release the snapshot pin on consumption, explicit cancellation, or expiry after five seconds.
The handle test checks client binding, cancellation, reuse and expiry.
For a successful update in `known`, expect one data proposal per arm and one extra client RPC in TwoCall.
`known-inclusive` charges the original acquisition in every arm, including the retained handle.

`stale50` and `stale100` advance exactly four or eight of eight original versions by rewriting the same bytes.
The measured attempts use separate keys and run sequentially to isolate the injected stale predicate from shard-wide timestamp reordering.
The hot cells release 2, 8 or 32 contenders together against one version, with no client retries.
The separate parked correctness trials wait for all eight attempts at the Raft proposal boundary, then submit in ascending or descending commit timestamp order.
Together those trials distinguish the baseline's OCC conflicts from its commit-order rejections, which share a wire error.
The harness matches returned success timestamps and values against exported Pebble versions and requires one winner in each hot correctness round.

The reported per-method RPC counts come from the unary client interceptor.
Proposal counts come from calls into the real Raft engine, including completed proposals whose FSM verdict rejected the update.
The engine wrapper counts typed FSM verdicts before gRPC projects them to public statuses.
Applied fences and leader checks count calls to the engine methods. The local election settings disable Raft lease reads, and a single voter can satisfy those checks locally.
Control reads count fenced pre-reads by the 1PC frontend.
Handle pin time includes already-retained pins carried into the timed update.
Setup, reset, history verification and background HLC renewal are excluded from request counters.
The natural hot benchmark reports baseline write conflicts together and reports CAS exact mismatch separately from its certified timestamp conflict.
Per-success ratios and success latency percentiles are omitted when there are no successes.
`ns/op` is per round, `ns/attempt` divides round wall time by the number of attempts, and p50/p95/p99 use individual latencies measured from release to reply.

This is a local smoke and correctness fixture. The planned three-host adoption experiment also requires calibrated open-loop arrivals, a separate load generator, TLS, multi-voter replication, paired runs, and the broader instrumentation in the [CAS design](2026_11_20_proposed_single_key_mvcc_cas.md#contention-benchmark).

## Ordinary-path comparison

Run the same fixtures against the candidate and the base revision. For an older checkout, copy `adapter/ordinary_get_comparison_test.go` and `kv/ordinary_write_comparison_test.go` into the corresponding packages. Existing benchmark results used identical fixtures with Go overlays. Alternate the two revisions and retain the output from each run.

```sh
GOMAXPROCS=2 go test -p 1 ./adapter ./kv -run '^$' \
  -bench '^BenchmarkOrdinary' -benchtime=150ms -count=1 -benchmem
```

The write fixture measures the leader-local path through the counting proposer with 1 or 64 ordinary mutations. The read fixture compares the plain Pebble point read and direct adapter calls on a hot 1 KiB value. [Boundaries and results](../evidence/2026_11_20_single_key_mvcc_cas_review.md#ordinary-path-measurements)
