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

## Final pre-PR review on main, 2026-11-21

**Publication blocked.** Two independent read-only reviews examined candidate `70b237c62c1c7ec8d574e54209a8c371c7b1742b` against `9edcad5d00a3a5e7f48aa466708a096977317714`. Both found correctness defects. Two focused tests on Pebble reproduce the following violations at the Raft FSM boundary. [Regression tests](../../kv/version_cas_review_regression_test.go)

| Scenario | Observed result | Invariant required to publish |
| --- | --- | --- |
| Both replicas start with the same key at version 10 and apply the same CAS frame at index 2. One catalog watcher has observed a same-owner split and the other is one version behind. | The first replica acknowledged the replacement at version 20. The second returned a missing-catalog-version verdict, persisted index 2, and retained version 10 even after catalog catch-up and replay of index 2. | All replicas applying the same data-group log must reach the same value and version. A missing process-local catalog snapshot cannot authorize a permanent no-op. |
| A CAS consumes version 10 and creates version 20. A bounded migration version cleanup then deletes one row in that range. | Cleanup deleted version 20 and exposed version 10 again. A second CAS using the consumed token succeeded at version 30. The test invokes the encoded cleanup frame directly. The review separately traced namespace and configuration checks at the peer-only RPC. | Cleanup must preserve the identity of consumed tokens or durably exclude the range from CAS before deleting its visible head. |

The spec review also identified two client-visible verdict discrepancies by source inspection. A head written after the CAS start but before its commit can trigger the OCC timestamp-conflict branch before the exact-version comparison. An indexed route write-fence rejection can reach public gRPC as an indeterminate outcome. Both need contract tests, including follower ingress, before publication.

The standards review confirmed the integrated coordinator preserves the sealed-check timestamp handoff, ordinary 1PC mutation order and read dependencies introduced by main. Both DCO trailers match the designated author. The focused gRPC suite and the 12 local contention benchmark cells passed. A successful known-token update took 1 client RPC and 1 proposal with CAS and 2 RPCs and 1 proposal with two-call 1PC. At 100% stale, CAS used 1 proposal per attempt and both fresh-read controls used 0. With eight contenders CAS averaged one winner per round. Repository-wide lint found zero issues. The Jepsen harness on this base passed 442 tests and 6,184 assertions. The standalone Redis suite still failed to load `jepsen.redis`. The repository-wide race run was stopped after reproducing the replication defect. The three-host performance campaign is pending.

A protocol revision must identify the replicated routing evidence that orders a CAS relative to ownership and write-fence changes. It must also cover cleanup that can remove the latest version while an older version remains. The two tests below are expected to fail on the reviewed commit with the review tests applied.

```sh
go test ./kv -run '^TestCAS(ReplicasAgreeAcrossSameOwnerCatalogSplit|ConsumedVersionStaysConsumedAfterBoundedCleanup)$' -count=1 -v
```

## Remediation of the pre-PR findings

| Prior finding | Implementation and regression evidence |
| --- | --- |
| Watcher lag could change the outcome of applying the same CAS frame. | The v2 frame identifies its group and carries the admitted catalog version. The FSM reads the authoritative catalog in its own indexed store scope. A same-owner split can advance the catalog while preserving eligibility. An actual ownership loss or write fence rejects the write. The paired Pebble regressions assert the durable value, version and index, including replay after the lagging watcher catches up. [Routing](../../kv/version_cas_route.go) · [Catalog tests](../../kv/version_cas_catalog_test.go) |
| A one-row cleanup could expose a consumed token again. | Once migration version protection is active, cleanup touching eligible keys requires durable migration evidence covering the requested interval. The verdict is independent of which rows survive on each replica. The Pebble test consumes a token, requests a one-row cleanup, reopens the store, replays and attempts to reuse the token. It checks both the public predicate's result at apply and the durable head. [Cleanup](../../kv/fsm_migration_cleanup.go) · [Regression](../../kv/version_cas_review_regression_test.go) |
| An intervening write could mask an exact-version mismatch. | Given a safe commit timestamp and an unlocked key, the FSM compares the head before checking the caller's start timestamp against a matching head. The public test commits an ordinary write between CAS timestamp allocations, then checks the mismatch and reads the surviving value and version. [RPC tests](../../adapter/grpc_compare_and_swap_test.go) |
| A certified route fence could surface as an unknown outcome. | Public ingress preserves the indexed refusal locally and across the dedicated forwarding RPC. A lost reply remains indeterminate. [Outcome mapping](../../adapter/grpc_compare_and_swap.go) · [RPC tests](../../adapter/grpc_compare_and_swap_test.go) |

### Why the existing Jepsen suite missed these histories

The register workloads recorded values and ordinary reads and writes. They did not capture the expected or returned MVCC version, or attempt reuse of a token after an acknowledged replacement. A forwarded read can also reach the leader while a follower has a different durable image at that index.

The new public workload records invocation-time tokens and completion-time versions. Its history tests include an overlapping read that may return the old value, a subsequent read that must see the new version or a later one, and two successful CAS operations consuming the same token. A timeout cannot certify either a mismatch or a replacement. The workload requires positive evidence of a chain of successful replacements, a read after an acknowledgement, and rejection of a consumed token. [Checker and generator](../../jepsen/src/elastickv/version_cas.clj) · [History tests](../../jepsen/test/elastickv/version_cas_test.clj) · [Run instructions](../operations/mvcc_version_cas.md)

Lean proves equal-index agreement and single consumption for an arbitrary finite committed log in the abstract model. TLC explored 15,419 distinct states for two replicas and six bounded workloads. With only the old watcher behavior enabled, TLC violated `EqualAppliedIndexAgrees`. With only the old cleanup behavior enabled, it violated `SingleConsumption`. The model assumes atomic persistence of data and index and abstracts the interval proof to a coverage predicate. [Assumptions and implementation mapping](../../tla/cas/README.md)

### Follow-up review and publication status

The three-voter TLS smoke used six Jepsen clients against the public gRPC endpoints and a fresh key. In six seconds it observed 11 acknowledged replacements, including 10 chained updates, 37 reads invoked after an acknowledged update, and 31 confirmed rejections of consumed tokens. The helper conservatively marked two RPC errors indeterminate. The checker recorded no violations. This run used ordinary client traffic on one host.

Independent review of tree `e4e6b95faf02926e07c368f5bdbb13ef9e3953b8` identified a further replica-agreement failure on a permanently covered migration range. Two Pebble stores start with the same versions 10 and 20. One compacts its older row. After both apply the same one-row cleanup at index 12, the uncompacted store exposes version 10 and the compacted store reports the key missing. CAS is already excluded on the range, but its ordinary latest value differs at the same applied index. The [paired-store regression](../../kv/version_cas_review_regression_test.go) reproduces this failure. The positive Lean agreement theorem and the original bounded TLC check assume a common durable image and omit independent physical compaction. A separate Lean example and TLC retention scenario exercise the covered-range counterexample under different retention histories. The positive checks apply only within their original assumptions.

The same review identified the public classification of a pre-proposal routing refusal and an obsolete expected verdict in the parked contention test. The follow-up changes distinguish pre-proposal refusal from an indexed fence on both ingress paths, and expect an exact mismatch for a higher-commit-timestamp loser that presents a consumed token. The existing 1PC conflict assertion remains part of the same trial. The draft PR tracks the open covered-range cleanup decision. Merge remains blocked on replica agreement.

The focused public leader/follower tests passed for the pre-proposal and indexed route outcomes, including the case in which the front end still uses the old route. The parked and reverse-release trials passed for both CAS and the fresh 1PC control. On the updated generator the main Jepsen unit suite passed 445 tests and 6,198 assertions. The subsequent checker additions passed in the focused run of 8 tests and 40 assertions. The pinned standalone Redis subproject still fails while loading its pre-existing `jepsen.redis` namespace. The repository-wide Go sweeps started before the follow-up fixes and did not establish a passing final run.
