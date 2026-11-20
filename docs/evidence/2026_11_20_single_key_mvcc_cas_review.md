# Independent review of single-key MVCC CAS

**Verdict (2026-11-20). Revise before using the benchmark for an adoption decision.** An independent read-only review traced the proposal against ElasticKV at `6943eff8ab66143ad023629dd238906eddd5a989`. The reviewed document has SHA-256 `b1dbd055013b4858201ebbd2a810d9c1b0ca4b33bdb1885daa9846279a35fe35`. References to proposal lines use that revision. The counts below are predictions from source inspection. [Reviewed design](../design/2026_11_20_proposed_single_key_mvcc_cas.md)

## Correctness findings

1. **High · Lines 44, 58 and 79. Specify which writers make the 1PC control equivalent.** Consider a read of A@5 at snapshot 20 followed by a delayed RawPut of B@15. OCC checks for a write newer than 20 and can pass. Exact CAS for version 5 must reject. The raw path has a regression test for a write landing below an observed watermark before fence activation. Use fenced 1PC writers for the equivalence experiment. Test active and inactive fences, same-timestamp replay versus replacement, and mixed raw/transactional writers separately. [Fence activation tests](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/commit_ts_fence_test.go#L47) · [OCC predicate](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/store/mvcc_store.go#L852)
2. **High · Lines 37, 42–43 and 67–73. Give token mismatch its own verdict.** A writer allocated timestamp 30 can arrive after an unrelated write at 31. The group timestamp fence can reject that attempt even if the target still has the expected version. Carry the precise predicate result over forwarding. Record token mismatches, timestamp-order rejections, locks, routing errors and unknown outcomes separately. Only a proven non-effect is eligible for retry, retaining the original token and deadline. [Commit timestamp fence](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/store/lsm_store.go#L2591) · [Forwarded failure classification](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/adapter/internal.go#L303)
3. **High · Lines 42–43 and 79. Specify the atomic boundary for migration side effects.** The current 1PC path records migration progress before fenced store apply. Inserting the CAS predicate only in store apply could advance the migration tracker on a mismatch. Stage the tracker and user change behind the same verdict. With tracking active, reject a stale token and check the tracker, stored version, notifications and replay. [1PC apply ordering](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/fsm.go#L2260) · [Tracker write](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/fsm_migration_readiness.go#L161)

## RPCs, Raft proposals and contention

- **Confirmed · Lines 48–56.** On the specified successful, leader-local, single-group, unbatched path, 1PC submits one data command and apply-only CAS is designed to submit one. Reducing two external calls to one applies to the benchmark frontend that performs a fresh read before submitting. The single-handler control already uses one external call. A fenced pre-read can reject stale input before proposing, while apply-only CAS consumes a proposal. [1PC dispatch](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/sharded_coordinator.go#L2156) · [Actual proposer call](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/transaction.go#L167)
- **Medium · Lines 48–66. Include the retained-snapshot control.** A caller retaining a validated transaction snapshot can submit an OCC write directly. Measure fresh-read and retained-snapshot callers independently. Charge handle acquisition, pinning, expiry and release. The current public Get releases its read scope on return, so this comparison requires a benchmark frontend. With an allocate-on-zero start timestamp the sharded coordinator can also probe the target's latest version. Count the public Get, internal reads and forwards, timestamp allocations, read fences and storage probes separately. Isolate lock resolution and add a follower-connected case before claiming remote-read savings. [Get lifetime](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/adapter/grpc.go#L836) · [Sharded start timestamp](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/sharded_coordinator.go#L2643) · [Remote version probe](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/shard_store.go#L4954)
- **Medium · Lines 67–73 and 79. Instrument the race and distinguish batching layers.** Transactional requests reach the proposer individually at this revision. The existing shared-entry batcher handles raw requests. Record submissions, proposals, committed entries and persistence flushes independently. Releasing clients together does not force all prechecks to finish before the first apply. Count survivors at each stage, or use a barrier after precheck in the controlled race. Exclude round setup and resets from all counters. [Transactional dispatch](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/transaction.go#L225) · [FSM batch eligibility](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/fsm.go#L499)
- **Adoption criterion · Lines 66–73. Define a reproducible primary workload.** Fix the worker count, token acquisition and refresh policy, saturation calibration, offered arrival rate, deadline and latency population before candidate runs. Fast mismatches can lower pooled p99 even as fewer useful writes complete. Gate successful-update p99 and successful throughput in the primary workload, count every scheduled request, and bound deadline misses and unknown outcomes. Keep the stale-token sweep and separate per-attempt and per-success denominators, including the zero-success case.

The following isolation assessment addresses the follow-up question alongside the independent review.

## Scope of the isolation guarantee

TiKV's optional commit timestamp on transactional reads is compatible with ElasticKV's snapshot isolation and optimistic validation. The timestamp identifies the observed version of one key. A transaction's original snapshot timestamp and the complete set of keys on which its decision depended have different roles. Either byte equality or version equality can be an additional predicate inside that transaction.

Use a two-key write-skew history as a correctness control. Start with A and B both on. Two clients read both at the same snapshot. One turns A off if B is on, and the other turns B off if A is on. Independent CAS operations on A and B can both succeed. With both point keys in each transaction's read set and validation atomic with commit, the second committer must encounter a conflict. Run a same-shard case first. A read dependency on another shard selects the existing multi-shard path and must use its own RPC/proposal budget. A range predicate also needs coverage for matching keys inserted after the read. Listing only the keys returned by a scan cannot detect that phantom. [Read-set format](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/proto/internal.proto#L65) · [Read-only shard routing](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/kv/sharded_coordinator.go#L1855) · [Point-key conflict check](https://github.com/bootjp/elastickv/blob/6943eff8ab66143ad023629dd238906eddd5a989/store/mvcc_store.go#L852)

## Independent re-review, 2026-11-21

The reviewer checked the revised proposal (SHA-256 `0d22e2f6eb56be25c75c4eef754d2dd1ed3cbb0e9609b3c6095e9a24a4cb8f56`) against source revision `9d2ed17d0` and approved starting implementation. The two-call baseline saves one external RPC. The single-handler and retained-snapshot controls already use one RPC and one data-group proposal. The fenced, non-expiring 1PC fixture and the separate mixed-writer test address the delayed-write counterexample.

The review also checked the explicit predicate verdict, the transaction boundary around migration tracking and replacement, and the successful-update latency and throughput criteria. Two clarifications were incorporated. A mismatching entry must explicitly persist its applied index, because an untouched Pebble batch skips commit. Injected staleness is a workload input; the measured outcome comes from the returned verdict. A timestamp rejection on a stale-token request belongs in the timestamp bucket. The clarified proposal has SHA-256 `f6a5ba0b3286c536f0cef5856fd92543e01b41c2a34d68437c230500f2b36899`.

This verdict covers the design. Rollout depends on implementation verification and the predeclared performance criteria.

## Local contention counters, 2026-11-21

The fixture ran ten iterations per cell on Darwin/arm64 (Apple M1 Max) with one etcd/Raft voter, synchronous Pebble and three Go scheduler threads. Setup, token acquisition for the known-token cell, and activation are outside the measured window. The fixture intercepts actual client RPCs and data proposals. Its local admission hook omits the production peer capability fan-out. The hot-key cells used three correctness rounds plus ten separate measured rounds of eight synchronized attempts. The host was shared, so these measurements establish operation counts only. The separate Linux performance decision still requires three hosts.

| Input | Arm | RPCs per attempt | Data proposals per attempt | Successful attempts |
| --- | --- | ---: | ---: | ---: |
| Known token | Apply-only CAS | 1 | 1 | 100.0% |
| Known token | Single-handler 1PC | 1 | 1 | 100.0% |
| Known token | Two-call 1PC | 2 | 1 | 100.0% |
| Known token | Retained-snapshot 1PC | 1 | 1 | 100.0% |
| 100% stale | Apply-only CAS | 1 | 1 | 0.0% |
| 100% stale | Single-handler 1PC | 1 | 0 | 0.0% |
| 100% stale | Two-call 1PC | 1 | 0 | 0.0% |
| 100% stale | Retained-snapshot 1PC | 1 | 1 | 0.0% |
| Eight contenders | Apply-only CAS | 1 | 1 | 12.5% |
| Eight contenders | Single-handler 1PC | 1 | 0.2375 | 12.5% |
| Eight contenders | Two-call 1PC | 1.975 | 0.975 | 12.5% |
| Eight contenders | Retained-snapshot 1PC | 1 | 1 | 12.5% |

Every contended round produced one winner. The fixture checked successful versions and values against exported MVCC history. When all supplied tokens were stale, apply-only CAS made one proposal for each definitive mismatch. Both fresh-read 1PC variants rejected those tokens before proposing. The retained-snapshot control had already completed its precheck and submitted one proposal per attempt. The benchmark guide includes the exact invocation and the acquisition-inclusive variant. [Local benchmark guide](../design/2026_11_21_single_key_mvcc_cas_local_benchmark.md)

## Implementation review and corrections

The first implementation review identified a follower fixture that rejected requests before forwarding, loss of an indexed lock verdict, gaps in recovery and encrypted-read coverage, and repeated work on ordinary reads and writes. The follow-up review verified the dedicated forwarding path, including one unsupported-method response from a legacy leader and zero calls to generic forwarding. It also verified that indexed lock rejections return `Aborted/CAS_LOCKED`, lost replies remain indeterminate, and exact version mismatches have a distinct response. A protected three-voter TLS test exercised follower ingress.

The recovery test submits an encoded CAS, injects a failure after staging, reopens Pebble, verifies the original value and applied index, then retries the entry and replays it. The follower read test reads an encrypted value immediately before expiry and sees it missing at expiry. A separate two-key test allows both standalone CAS operations to succeed and requires a transaction declaring both point reads to reject the second writer.

A further independent review verified the ordinary-path changes and identified a malformed in-process follower batch that could dereference a nil request. Forwarding now rejects any nil request before assigning a forwarding ID. A regression test exercises nil requests at both ends of a CAS batch and a singleton nil request.

## Ordinary-path measurements

Three interleaved 150 ms samples compared the candidate with `9d2ed17d08c12e0ff6e977f7c07708cc48cac3ca` on the shared Apple M1 Max host with Go 1.27.3 and `GOMAXPROCS=2`. The table reports medians and the full observed range. Reads use a hot 1 KiB value. Adapter calls exclude network and serialization. The 1PC fixture measures local verification, transaction encoding and a counted proposer call. Replication, FSM apply, and storage I/O are outside the write measurement.

| Operation | Base ns/op (range) | Candidate ns/op (range) | Allocations/op, both |
| --- | ---: | ---: | ---: |
| Ordinary 1PC, 1 mutation | 940 (754–1,058) | 1,012 (804–1,242) | 10 |
| Ordinary 1PC, 64 mutations | 6,749 (6,701–7,233) | 7,575 (5,139–11,223) | 10 |
| Pebble GetAt | 1,766 (1,331–1,988) | 1,455 (1,192–2,743) | 4 |
| Memory Get | 1,922 (1,455–3,053) | 1,594 (1,559–3,072) | 19 |
| Memory RawGet | 2,250 (1,505–4,380) | 2,009 (1,326–5,994) | 17 |
| Pebble Get | 3,889 (2,999–4,561) | 4,929 (2,731–6,910) | 21 |
| Pebble RawGet | 3,088 (2,770–4,443) | 3,794 (2,763–7,188) | 19 |

Every write verified leadership once and submitted one ordinary proposal. Allocated bytes per write stayed at 496 for one mutation and 3,920 for 64. The median allocation size per adapter read increased by 15–16 bytes. The samples overlap for every operation, including the slower candidate medians for ordinary 1PC and Pebble-backed adapter reads. The three-host experiment must quantify this cost alongside the benefit to version-aware callers. [Benchmark invocation](../design/2026_11_21_single_key_mvcc_cas_local_benchmark.md#ordinary-path-comparison)

## Verification

The full Go package run exposed two fixture assumptions about the legacy coordinator, a historical raw batch that the new proposal gate correctly refuses to construct, and a bootstrap test that could sample different published and durable apply indices. The corrected public tests exercise lost replies on the production leader path and verify that the legacy coordinator rejects CAS before proposal. The replay test encodes the historical entry explicitly. The three-voter bootstrap fixture now waits for a consistent index before checking for an absent policy. Two runs of the corrected public endpoint and Redis tests passed, as did two runs of the protected TLS cluster and absence-guard tests.

The final adapter sweep passed with the two long-running sequencing tests excluded. Both sequencing tests had completed in the preceding full adapter run. All failures from the repository-wide Go run passed in the subsequent scoped runs. The complete suite has not been repeated in a single invocation on the final tree.

The focused race-detector run passed in `store`, `kv`, and `adapter`. It covered the nil follower batch, legacy leader forwarding, write skew with and without read-set validation, indexed lock rejection, rollback and replay of a framed proposal, encrypted TTL reads through a follower, and contending gRPC writers. Repository-wide lint returned zero issues. The ordinary-path fixtures also compiled and completed on the candidate.

At the pinned Jepsen revision `54ab5c10ace229fc780e43e188ef1b64c3a32ee4`, the main Leiningen suite passed 410 tests and 1,932 assertions. The separate Redis Leiningen project failed while loading its test namespace. Its test requires `jepsen.redis` and the source tree provides `jepsen.redis.core`. The distributed three-host performance campaign remains outstanding.
