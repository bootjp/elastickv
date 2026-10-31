# Test suite cleanup: redundant tests, live-path retargets, and shared test support

Status: Partial
Author: bootjp
Date: 2026-10-16

This document is written so that each work package (WP) below can be handed to a separate agent or contributor as a self-contained task. Every WP lists the exact tests and functions to change, why, how to prove the change, and when it is done. Line numbers refer to `main` at `95889d1f` and point at the `func` line; if they drift, locate the test by name.

## Context

An audit of the Go test suite on 2026-10-16 (472 `*_test.go` files, about 198k lines, 5,924 `Test` functions) found five problems. The audit used `deadcode`, `dupl`, an AST scan for identical function bodies, per-package review, coverage runs, and mutation checks. A mutation check breaks production code through `go test -overlay` and confirms whether any test fails.

1. **Redundant tests.** About 47 tests duplicate another test, are strictly subsumed by one, or assert nothing that can fail. They cost runtime and review attention without adding coverage.
2. **Tests that guard a dead copy instead of the live path.** About 22 tests call a production function that production no longer uses. Usually this is a wrapper or a twin left behind when callers moved to a new variant, so the live path has no test. Mutation checks confirmed real gaps:
   - Making `dynamicWrappedProposer.Propose` propose cleartext when the Raft payload wrap fails passes every `kv` and root test. That is an encryption bypass no test detects.
   - Changing the compaction retention boundary in `shouldDeleteCompactionVersion` so the anchor version at `minTS` is deleted passes every `store` and `kv` test. That is data loss no test detects.
   - The 2026-05-08 incident regression test (`#745`) goes through a wrapper that sets its own deadline. So the live guard in `verifyLeaderEngineCtx`, which bounds a deadline-less context, is never exercised.
   - The live Redis HDEL path (`persistHashReadTimestampTxn`) has 0% coverage. Its regression test calls the dead twin `persistHashTxn`.
3. **Test-only code in production builds and an over-broad lint exclusion.**
   - `adapter/test_util.go` was not a `_test.go` file, so the server and `cmd/server` binaries linked `testing` and testify.
   - The `.golangci.yaml` exclusion `(.*)test(.*).go` also matched the production files `kv/latest_commit_ts.go` and `proto/raw_latest_commit_ts_batch.go`. That disabled `dupl`, `goconst` and `wrapcheck` there.
4. **Duplicated test support.**
   - About 67 fake coordinator types (1,855 lines); 12 implement all nine `kv.Coordinator` methods from scratch.
   - About 36 fake Raft engine types (884 lines); 13 implement the full `raftengine.Engine`.
   - Six in-memory "apply an `OperationGroup`" coordinators that disagree on `DEL_PREFIX`, commit-timestamp source and OCC.
   - About 160 hand-written "receive or fail" `select` blocks.
   - 27 hand-built `encryption.Cipher` fixtures.
5. **FUSE end-to-end tests never run in CI.** `go-test.yml` neither installs `fuse3` nor sets `ELASTICKV_REQUIRE_FUSE`, so the kernel-mount tests always skip.

Problems 3 and 5 are fixed by the PR that adds this document (WP-0). The rest are split into the work packages below.

## Goals

- Delete or merge the redundant tests (WP-1 to WP-7).
- Retarget every test that guards a dead copy onto the function production actually calls, and prove each retarget with a mutation check (WP-1 to WP-6).
- Introduce shared test-support packages and move the duplicated fakes, waits and fixtures onto them (WP-8 to WP-12).

## Non-goals

- **No production behaviour changes.** The audit also found product bugs; each needs its own PR with a failing test first:
  - SQS FIFO same-millisecond reordering.
  - Redis TTL-inline migration doing no work behind `startupGatedCoordinator`.
  - Admin write authorization differing between SQS and S3/DynamoDB.
  - S3 SigV4 `ListObjectsV2` returning 500 for an expired continuation token.
  - See [Follow-ups](#follow-ups-out-of-scope).
- Removing production dead code that is not orphaned by a retarget in this plan. For example `MigrateFSMStore`, `CheckLocalEpochRollback` and `adapter/redis_transcord.go` stay as follow-ups.
- CI runtime tuning beyond what falls out of the deletions (follow-up list below).

## Ground rules for every work package

1. **Production code.** Allowed edits are:
   - deleting a function that a retarget in the same WP leaves without callers;
   - fixing comments that name a deleted function.
   Anything else is out of scope; stop and report instead.
2. **Retarget means "call the live function".** For every retarget or new guard:
   - Break the live code with a mutation (see [Mutation-check recipe](#mutation-check-recipe)) and show the test fails.
   - Show it passes on the real code.
   - Record each mutation and its result in the PR description.
   - If a retargeted test **fails on the real code**, you have found a real bug. Do not weaken the assertion: stop and report it.
3. **Deleting a test requires a survivor.** Each deletion below names the test that already asserts the same thing. Re-read both before deleting.
   - When the deleted test carries an incident or review reference (Codex, Gemini, CodeRabbit, PR number), move that comment onto the surviving test or table row.
4. **Lint.** golangci-lint's `unused` checks test files too. After deleting or retargeting, delete any helper, fake or production function left unreferenced. Run `golangci-lint run --config=.golangci.yaml ./...` with v2.9.0 (the CI pin; `make lint` adds `--fix`) and keep it at 0 issues.
5. **Tests.** Run `go test -race` for every touched package and for packages that import it. Root `.` imports almost everything, so run it when touching `kv`, `store` or `adapter`.
6. **One PR per WP.**
   - First line of the PR body: `Design: docs/design/2026_10_16_proposed_test_suite_cleanup.md`.
   - Include behaviour change (none), risk, test evidence, the mutation table, and the five self-review lenses from `CLAUDE.md`.
   - Commit prefixes follow the touched area (`kv:`, `store:`, `adapter:`, …).
7. **Stay inside the WP's directories.** WPs are partitioned by directory so they can run in parallel without merge conflicts. Comment-only edits elsewhere are allowed when a deleted function is named in a comment.

## Work packages

| WP | Area (directories) | Content | Depends on |
|---|---|---|---|
| WP-0 | `adapter`, `.golangci.yaml`, `kv/latest_commit_ts.go`, `.github/workflows/go-test.yml` | Harness out of production, lint exclusion, FUSE in CI. **Done in the PR that adds this document.** | — |
| WP-1 | `kv` | 3 deletions/merges, 5 retargets, 1 strengthened test | — |
| WP-2 | `store` | 3 deletions, 4 retargets/new guards, shared conformance runner | — |
| WP-3 | `internal/raftengine/etcd`, `internal/encryption` (+`fsmwire`) | 3 deletions, 2 retargets | — |
| WP-4 | `adapter/redis*` | 7 deletions, 6 retargets | — |
| WP-5 | `adapter` (non-Redis) | 10 deletions, 3 retargets | — |
| WP-6 | root `.`, `cmd/**`, `proxy` | 12 deletions, 6 retargets | — |
| WP-7 | `internal/admin`, `distribution`, `keyviz` | 4 deletions | — |
| WP-8 | new: `internal/testutil`, `raftenginetest` additions, `internal/encryption/encryptiontest`, `store/storetest`, `kv/kvtest` | Create shared test-support packages and the production-import guard | — |
| WP-9 | `adapter` | Migrate onto WP-8 helpers | WP-4, WP-5, WP-8 merged |
| WP-10 | `kv` | Migrate onto WP-8 helpers | WP-1, WP-8 merged |
| WP-11 | `internal/**`, `distribution`, `keyviz`, `monitoring` | Migrate onto WP-8 helpers | WP-3, WP-7, WP-8 merged |
| WP-12 | root `.`, `cmd/**`, `proxy`, `store` | Migrate onto WP-8 helpers | WP-2, WP-6, WP-8 merged |

WP-1 to WP-8 can run in parallel. WP-9 to WP-12 start once their dependencies merge, and can then run in parallel with each other.

## Progress

| WP | State |
|---|---|
| WP-0 | Landed with this document (#1277). |
| WP-1 | Landed (#1345). |
| WP-2 | Landed (#1292). |
| WP-3 | Landed (follow-up PR after #1345). |
| WP-4 | Landed (same PR as WP-3); see deviation 3. |
| WP-5 | Landed (same PR as WP-3); see deviation 1. |
| WP-6 | Landed (same PR as WP-3); see deviation 2. |
| WP-7 | Landed (same PR as WP-3). |
| WP-8 | Landed (#1288). |
| WP-9 to WP-12 | Landed in the same PR as WP-3, with the exceptions listed under "WP-9 to WP-12 results". |

WP-9 to WP-12 results (all four directory sets were migrated together, one commit per pattern):

| Pattern | Before | After | Left as is, and why |
|---|---|---|---|
| Receive-or-fail `select` blocks | 276 candidate blocks | 154 replaced by `testutil.Recv` / `NoRecv` / `Closed`; 122 remain | A bare receive becomes `Closed` only when the file closes that channel and `Recv` only when it sends on it. Blocks were kept when that was ambiguous, the case body uses `break`/`goto`, the failure is non-fatal (`t.Error`), or hoisting the case variable could shadow another name. |
| Legacy list delta key copies | 5 hand-rolled encoders | 0; all use `store.LegacyListMetaDeltaKey` (adapter seq 0, kv seq 1 preserved) | The `internal/backup` mirrors stay independent on purpose. |
| Encryption fixtures | `store.CounterNonceFactory` exported from production `store`; 10 random-DEK keystore+cipher blocks; 2 byte-identical sidecar writers plus 4 temp-dir writers | `store/encryption_test_helpers.go` deleted (callers use `encryption.NewDeterministicNonceFactory`); 37 `encryptiontest` call sites | Fixed-byte DEKs, sidecar rewrites of an existing path, and `wiringEncryptionObserver` ≈ `recordingEncryptionObserver` (different packages; sharing needs a new package). |
| Loopback gRPC servers | about 27 | 14 kv servers on `grpctest.Serve` | Servers whose tests assert the `Serve` error, need the listener address before construction, or stop the server mid-test. |
| Fake Raft engines | 13 full fakes, about 10 `LeaderView`, about 12 `Proposer` | `stubLeaderEngine`/`noopEngine`, `stubFollowerEngine`, `membershipConfigEngine` → `raftenginetest.StubEngine`; three `LeaderView` fakes → `StaticLeaderView`; etcd `testStateMachine` aliases `raftenginetest.TestStateMachine` | Rule 4: fakes whose behaviour is the subject (toggling leader, blocking reads, lease, call recording, panicking `VerifyLeader`, error-injecting proposers). |
| Fake coordinators | 12 full fakes, 6 in-memory apply coordinators | HELLO/INFO fakes folded into `stubAdapterCoordinator`; `testCoordinatorWrapper`, `phaseDGateCoordinator`, `fakeGroupRoutableCoordinator` embed instead of forwarding; `localAdapterCoordinator`, `occAdapterCoordinator`, `retryOnceCoordinator`, `fstest.MemoryDispatcher` and the filesystem `testCoordinator` dispatch through `kvtest.LocalCoordinator` (new `Clock` option shares the wrapper's HLC) | Rule 4 again for fakes with hooks, counters or scripted failures (`distributionCoordinatorStub`, `stubLeaderCoordinator`, `stubStartupCoordinator`, the TSO fakes). `phaseDGateCoordinator` embeds an interface of exactly its old method set, not `*ShardedCoordinator`, so it gains no optional capabilities. |

Behaviour notes for the `LocalCoordinator` move: dispatches are serialized, elements are copied, `ctx` is checked before applying, the response carries `CommitTS`/`CommitIndex`, unknown ops return `ErrUnknownOp`, and an invalid commit-timestamp patch surfaces kv's validation error instead of `filesystem.ErrInvalid`.

Deviations from the plan, found while implementing:

1. **WP-5 retarget 1 (DynamoDB base-table hash-key mismatch) cannot be written.** `queryLeasePrefix` is only reached from `queryLeaseKey` with an empty `IndexName`, so `keySchemaForQuery` always returns `PrimaryKey` and the `keySchema.HashKey != schema.PrimaryKey.HashKey` branch is unreachable. A base-table key condition that does not pin the hash key fails `buildQueryCondition` and is classified as a skip. The duplicate test was deleted. The dead branch was removed in a follow-up change, with `TestQueryLeasePrefix_BaseTableClassification` pinning that a base-table Query resolves to a single-group prefix or a skip. The twin branches in `queryScanPrefix` were unreachable for the same reason: every caller derives the key schema from the same `IndexName`, and a GSI Query returns the index prefix first. They were removed with the now-unused `keySchema` parameter; `TestQueryScanPrefix_SelectsHashPrefixPerEncoding` pins the base-table and GSI prefix for both key encodings.
2. **WP-6 retarget 3 (pprof/metrics).** The live `PreparePprofServer`/`PrepareMetricsServer` moved to `internal/serverwiring`, whose own tests already pin the address-before-token validation. The root wrappers and their tests were deleted rather than retargeted.
3. **WP-4 retarget 6 (EXEC dispatch timeout).** `redisDispatchTimeout` is a 30 s constant, so a blocking-store test would take 30 s or need a production change. The rewritten test records the deadline on the stream-deletion scan instead. The mutation named in the plan (`context.Background()` for the EXEC dispatch) survives because `txnContext.dispatchContext` bounds the commit context a second time. Removing that second bound (`dispatchContext` returning an unbounded context) fails the test.

---

### WP-0: Keep test code out of production; run FUSE tests in CI (done)

- `git mv adapter/test_util.go adapter/test_util_test.go`.
  - All its identifiers are used only by `adapter` tests.
  - `go list -deps .` and `go list -deps ./cmd/server` no longer contain `testing` or `github.com/stretchr/testify/*`.
- `.golangci.yaml`: the test-only exclusion for `dupl`/`funlen`/`goconst`/`wrapcheck` is now `path: (_test\.go$|(^|/)[a-z0-9]+test/)`. That covers test files and test-support directories (`raftenginetest/`, `fstest/`), and no longer covers `kv/latest_commit_ts.go` or `proto/raw_latest_commit_ts_batch.go`.
  - The newly linted `kv/latest_commit_ts.go` had one `wrapcheck` finding (unwrapped `errgroup.Wait`), now wrapped with `errors.WithStack`.
- `.github/workflows/go-test.yml` installs `fuse3` and runs `go test -race ./...` with `ELASTICKV_REQUIRE_FUSE=1`. These two tests now run, and fail rather than skip when FUSE is unusable:
  - `internal/filesystem/fuseadapter/mount_e2e_test.go`
  - `TestMountFilesystemServiceServesKernelMount` in `main_filesystem_test.go`
  - The Jepsen workflows already mount FUSE on the same runner image with `fuse3`.

### WP-1: `kv`

**Delete or merge.**

| Test | Survivor | Why it is redundant |
|---|---|---|
| `kv/sharded_coordinator_txn_test.go:462` `TestShardedCoordinatorDispatchNonTxn_RejectsRouteWriteTimestampFloor` | `kv/sharded_coordinator_del_prefix_test.go:145` `TestShardedCoordinatorRejectsPointWriteAtMigrationTimestampFloor` | Same catalog (one route, group 1, `MinWriteTSExclusive=MaxUint64`) and the same non-txn `Put` through `Dispatch`. The two assert different sentinels of the same `errors.Join` (`routeWriteTimestampFloorError`). **Merge:** make the survivor assert both `store.ErrWriteConflict` and `ErrRouteWriteTimestampTooLow`, then delete this one. |
| `kv/shard_store_test.go:3649` `TestMergeAndTrimReverseScanResults_ExactLimit` | `kv/shard_store_test.go:3609` `TestMergeAndTrimReverseScanResults` | Both take the sort-then-trim branch of `mergeAndTrimScanResultsWithOptions` (4 inputs, limit < 4). Despite its name, it never has `len == limit`. |
| `kv/shard_store_test.go:3694` `TestScanLockBoundsForKVs_PreservesOriginalStart` | `kv/shard_store_test.go:3668` `TestScanLockBoundsForKVs_ReverseOrder` | Identical once literals are normalised; same forward branch. |

**Retarget.**

1. **`kv/raft_engine_test.go:39` `TestVerifyLeaderEngine_BoundsBlockingReadIndex`** (incident 2026-05-08, `#745`).
   - Problem: it calls `verifyLeaderEngine` (`kv/raft_engine.go:86`), which has had no production caller since `d11258a6` and creates its own 5 s deadline. So the live guard in `verifyLeaderEngineCtx` is never exercised: when the caller's context has no deadline, add `verifyLeaderTimeout` (`raft_engine.go:78-82`).
   - Change:
     - Call `verifyLeaderEngineCtx(context.Background(), blockingLeaderView{})` with the same assertions.
     - Run the call in a goroutine and fail if it has not returned within about 3 × `verifyLeaderTimeout`, so a regression fails instead of hanging the package.
     - Delete `verifyLeaderEngine`.
     - Fix the comments that name it: `kv/raft_engine.go:12,73`, `kv/leader_proxy.go:60`, `kv/transaction.go:102`, `main_admin.go:202`.
   - Mutation that must now fail: delete the `if _, ok := ctx.Deadline(); !ok { … }` block.
2. **Static `wrappedProposer`** (`kv/raft_payload_wrapper.go:54-112`: type, `newWrappedProposer`, `Propose`, `ProposeAdmin`).
   - Problem: added in `50e9bf98`, it was never wired. Production uses `dynamicWrappedProposer`, wired through `NewLeaderProxyForShardGroup` since `a342fd29`.
   - Change:
     - Port `TestWrappedProposer_PropagatesWrapperError` (`kv/raft_payload_wrapper_test.go:169`) and `TestWrappedProposer_RoundTripWithRealCipher` (`:190`) to `newDynamicWrappedProposer`; the dynamic type has no equivalent today.
     - Delete the other static tests, which already have dynamic equivalents:
       - `:101` `TestWrappedProposer_ProposeAdminAppliesWrap` → `TestDynamicWrappedProposer_ProposeAdminAppliesCurrentWrap`
       - `:132` `TestNewWrappedProposer_NilWrapperReturnsInnerVerbatim` → `TestDynamicWrappedProposer_NilPointerReturnsInnerVerbatim`
       - `:142` `TestWrappedProposer_InvokesWrapperOncePerCall` → `TestDynamicWrappedProposer_LoadsCurrentWrapEveryCall`
     - Delete the static type.
     - Fix the comments at `raft_payload_wrapper.go:59,185,190,200`.
   - Mutation that must now fail: in `dynamicWrappedProposer.Propose`, replace `return nil, err` after `applyRaftPayloadWrap` with `wrapped = data` (cleartext fallback). Today every `kv` and root test passes under this mutation.
3. **`kv/sharded_coordinator_composed1_retry_test.go:299` `TestShardedCoordinator_DoesNotRetryReadWriteTxnOnComposed1` and `:357` `TestShardedCoordinator_DoesNotAutoPinObservedRouteVersionForReadWriteTxn`.**
   - Problem: both pin the ReadKeys guards (`66ad5b0e`: Gemini CRITICAL, Codex P1) but also set `StartTS: 10`. The later caller-supplied-StartTS guard (`144ec0ca`, around `sharded_coordinator.go:1154` and `:1236`) short-circuits first, so they duplicate `:483` and `:546`.
   - Change: drop `StartTS` so the coordinator allocates it and only the ReadKeys condition can stop the retry or auto-pin.
   - Mutation that must now fail: remove the `len(reqs.ReadKeys) != 0` conditions from those two guards. Today the whole `kv` package passes.
4. **`kv/sharded_coordinator_del_prefix_test.go:175` `TestShardedCoordinatorRejectsRawDelPrefixMutationAtMigrationTimestampFloor`.**
   - Problem: it calls `rejectWriteTimestampFloorMutations`/`rejectWriteTimestampFloorMutation` (`kv/sharded_coordinator.go:3197-3220`), which have no production caller. The raw path uses `ensureMutationsWriteAllowedWithBypass` → `ensureMutationWriteAllowed`.
   - Change:
     - Call the live entry for both a `"z"` prefix and a nil prefix, e.g. `ensureMutationsWriteAllowedWithBypass(muts, ts, nil)`, or `stampRawRequestTimestamps` if that is the narrower live seam.
     - Delete the two dead methods.
   - Mutation that must now fail: make the `DEL_PREFIX` branch of `ensureMutationWriteAllowed` return nil.
5. **`kv/coordinator_retry_test.go:290` `TestIsTransientLeaderError_PinsRealSentinels`.**
   - Problem: it claims to lock the sentinel `.Error()` texts to `leaderErrorPhrases` (see the comment at `kv/coordinator.go:828`). But it passes the raw sentinels, which `errors.Is` classifies before the phrase check runs.
   - Change: pass `errors.New(tc.err.Error())`, which models an error flattened by gRPC.
   - Mutation that must now fail: change the text of `raftengine.ErrNotLeader` or `ErrLeadershipLost` in `internal/raftengine/engine.go`.

**Strengthen.** `kv/lock_resolver_test.go:271` `TestLockResolver_LeaderOnlyExecution` has no assertion ("either way, no panic"), but it is the only caller of `resolveAllGroups`. Assert that the expired pending lock is aborted on the group this node leads.

**Done when:**
- `go test -race ./kv/... .` passes.
- lint reports 0 issues.
- The mutation table covers retargets 1–5.

### WP-2: `store`

**Delete.**

| Test | Survivor | Why it is redundant |
|---|---|---|
| `store/lsm_store_txn_test.go:501` `TestPebbleStore_Compact_TxnInternalKeyOldVersionRemoved` | `store/compact_txn_test.go:113` `TestPebbleStore_Compact_TxnLockLifecycle` | Same fixture (lock key `PutAt@10`, `DeleteAt@20`, `Compact(25)`), same assertion; the survivor also checks the version count. Re-introducing the 2026-03-26 txn-key skip fails the survivor but **not** this test. |
| `store/lsm_store_txn_test.go:470` `TestPebbleStore_Compact_TxnInternalKeySingleVersionPreserved` | `store/compact_txn_test.go:175` `TestPebbleStore_Compact_TxnActiveLockPreserved` | Same single-version lock fixture; its extra read is above `minTS`, so compaction cannot affect it. |
| `store/lsm_store_txn_test.go:363` `TestPebbleStore_Compact_RemovesOldVersions` | `store/lsm_store_test.go:508` `TestPebbleStore_Compact` | Same versions and reads; the survivor also checks the physical count and `MinRetainedTS`. This test passes even when compaction deletes nothing. |

**Retarget or add guards.**

1. **Compaction retention boundary** (`shouldDeleteCompactionVersion`, `store/lsm_store.go:2907`).
   - Problem: no fixture puts versions at `minTS` or `minTS+1`.
   - Change: add a table test with versions at `minTS-1`, `minTS` and `minTS+1`, then `Compact(minTS)`. Assert that:
     - `GetAt(minTS)` returns the `minTS` version;
     - the `minTS-1` version is physically gone;
     - the `minTS+1` version is kept.
   - Mutations that must now fail:
     - change `version > minTS` to `version > minTS+1`, which deletes the true anchor (today `store` and `kv` pass);
     - change it to `version >= minTS`, which over-retains.
2. **Unknown snapshot magic.**
   - Problem: `store/lsm_store_test.go:1034` `TestPebbleStore_Restore_PebbleMagicMismatch` calls `restorePebbleNative` (`lsm_store.go:3377`), which has been dead since `b8b9a163`. `Restore` dispatches on the magic first, so the live default branch (`lsm_store.go:3311`, "unrecognized snapshot format") is untested.
   - Change:
     - Call `Restore(bytes.NewReader(<8 bytes of unknown magic>))` and assert that error.
     - Delete `restorePebbleNative` (~41 lines).
   - Mutation that must now fail: make the default branch return nil.
3. **Checksum on the streaming MVCC restore path in Pebble** (`lsm_store.go`, around `:3556`).
   - Problem: only the in-memory store has a corrupted-checksum test (`store/mvcc_store_snapshot_test.go:62` `TestMVCCStore_RestoreRejectsInvalidChecksum`).
   - Change: run that test against both stores through the runner in item 4.
   - Mutation that must now fail: remove the checksum comparison in the Pebble restore path.
4. **One conformance runner.** Add `forEachMVCCStore(t, func(t *testing.T, newStore func(*testing.T) MVCCStore))` in a new `store/mvcc_conformance_test.go`. It generalises the three ad-hoc runners:
   - `runMigrationStoreSuite` (`migration_versions_test.go:16`)
   - `runCommittedVersionAtSuite` (`committed_version_at_test.go:15`)
   - `testReverseScanAt` (`reverse_scan_test.go:57`)

   Move the following into it so they also run against Pebble, the store production uses. `NewMVCCStore` has had no production caller since `5ceaefb5`, but it stays as the default test double.
   - Both rapid property tests in `mvcc_store_prop_test.go`, with one store per `Check` and unique keys.
   - The concurrency tests in `mvcc_store_concurrency_test.go`.
   - TTL tests: `PutWithTTLAt` and `ExpireAt` (`mvcc_store_test.go`, `mvcc_store_at_test.go`).
   - `TestMVCCStore_Compact_KeepAll` (`mvcc_store_compact_test.go`).
   - `TxnCommitRecordPreserved` and `AdvancesMinRetainedTSEmptyStore` (`compact_txn_test.go`).
   - `ScanAt_NilEnd` (`scan_bounds_test.go`).
   - The checksum test from item 3.

   Then fold each in-memory/Pebble mirror pair into one runner test and delete the duplicated half (about 424 lines on the in-memory side):
   - `mvcc_store_at_test.go` `GetAtSnapshots` / `DeleteAtTombstone` / `ScanAtSortedAndSnapshot` ↔ `lsm_store_test.go` `Basic` / `Scan`
   - `mvcc_store_retention_test.go` ↔ `lsm_store_test.go:374` and `:774`
   - `mvcc_store_compact_test.go` `Compact` / `Compact_Delete` ↔ `lsm_store_test.go:508` / `lsm_store_txn_test.go:418`
   - the in-memory and Pebble halves of `compact_txn_test.go`, `scan_bounds_test.go` and `write_conflict_store_test.go`
   - the four `ApplyMutations_ReadConflict*` tests in `mvcc_store_concurrency_test.go` ↔ `lsm_store_txn_test.go:576/632/655/675`
   - `mvcc_store_snapshot_test.go` round-trip / max-stored-key ↔ `lsm_store_test.go:734` / `:1216`
   - the `TestMVCCStore_*_ValueTooLarge` tests ↔ their Pebble twins in `lsm_store_test.go`

   If a test that so far ran only on the in-memory store fails on Pebble, that is a real divergence: stop and report it. Update `CLAUDE.md` if the cited `mvcc_store_prop_test.go` / `mvcc_store_retention_test.go` files are renamed.
5. **`store/lsm_store_encryption_prop_test.go:35` `TestEncryption_Property_PutGet`.**
   - Problem: it opens a new Pebble store on every rapid iteration. All of them stay open until the test ends, because `Cleanup` is registered on the outer `t`. That also defeats its documented purpose, catching nonce reuse across writes to one fixture.
   - Change: build the store once per `Check` and reuse it across iterations.

**Done when:**
- `go test -race ./store/... ./kv/... .` passes.
- lint reports 0 issues.
- The mutation table covers items 1–3.
- The PR reports the `store` package runtime before and after.

### WP-3: `internal/raftengine/etcd`, `internal/encryption`

**Delete.**

| Test | Survivor | Why it is redundant |
|---|---|---|
| `internal/raftengine/etcd/engine_test.go:1710` `TestMaxAppliedIndexStartsFromSnapshotIndex` | `engine_test.go:433` `TestOpenInitializesAppliedIndexFromPersistedSnapshot` | Since `0e19266e`, `maxAppliedIndex` is `snapshot.GetMetadata().GetIndex()`. The test re-states the function and its storage setup is dead. |
| `internal/raftengine/etcd/engine_applied_index_test.go:343` `TestRecordingFSM_SatisfiesAppliedIndexWriter` | compile-time assertion | A run-time check that a test fake satisfies an interface. Replace it with `var _ raftengine.AppliedIndexWriter = (*recordingAppliedIndexFSM)(nil)`. |
| `internal/encryption/envelope_test.go:193` `TestHeaderAADBytes_Layout` | `envelope_test.go:201` `TestAppendHeaderAADBytes_AppendsAndAllocFree` | Same six bytes for the same inputs. |

**Retarget.**

1. **`persistConfigState`.**
   - Problem: `engine_test.go:1207` `TestPersistConfigSnapshotSkipsSerializationWhenNewerSnapshotExists`, `:1222` `TestPersistConfigSnapshotDoesNotCompactLogs` and `:1245` `TestPersistConfigSnapshotWaitsForDurablePublication` call `persistConfigSnapshot` (`engine.go:3371`). It has had no caller since `99bf9cf2` inlined it into `persistConfigState` (`engine.go:3392`).
   - Why it matters: these three assert properties the live tests (`:1319`, `:1343`) do not: skipping when a newer snapshot exists, no log compaction, and SaveSnap→Release ordering.
   - Change:
     - Call `persistConfigState(index, confState, peers)` with a data dir.
     - Delete `persistConfigSnapshot`.
   - Mutation that must now fail: remove the up-to-date fast path in `persistConfigState`. Today the whole etcd package passes.
2. **Bootstrap batch-count cap** (`internal/encryption/fsmwire/wire.go:422`, Codex P1 round 3 on PR748).
   - Problem: `wire_test.go:158` `TestBootstrap_RejectsOverCapBatchCount` and `:188` `TestBootstrap_RejectsHugeBatchCount` provide no trailing registration bytes. So the remaining-bytes check rejects both inputs even with the cap deleted; today every `TestBootstrap_*` passes without it.
   - Change: in the over-cap test, append `16385 × 14` bytes so that only the cap can reject, and assert the error says the count exceeds the cap.
   - Mutation that must now fail: delete the `count > maxBootstrapBatchCount` check.

**Done when:**
- `go test -race ./internal/raftengine/... ./internal/encryption/...` passes.
- lint reports 0 issues.
- The mutation table covers both retargets.

### WP-4: Redis adapter (`adapter/redis*`)

**Delete.**

| Test | Survivor | Why it is redundant |
|---|---|---|
| `adapter/redis_lua_collection_fastpath_test.go:971` `TestLua_ZRANGEBYSCORE_FreshZSetEmptyRange` | `:763` `TestLua_ZRANGEBYSCORE_EmptyRange` | Same Eval, same `[]any{}`; its doc admits it cannot build the delta-only zset it is named after. |
| `:1125` `TestLua_ZRANGEBYSCORE_MissingKeyReturnsEmptyViaFastPath` | `:782` `TestLua_ZRANGEBYSCORE_MissingKey` | Same Eval on a missing key; "via fast path" is not observable on the wire. |
| `:1148` `TestLua_ZRANGEBYSCORE_StringKeyReturnsWrongType` | `:799` `TestLua_ZRANGEBYSCORE_WRONGTYPE` | Same SET plus Eval, same WRONGTYPE check. Keep the List variant that follows it. |
| `adapter/redis_apply_observer_test.go:79` `TestRedisApplyObserverPendingLocalZSetApplyStillForcesFullRecheck` | `:68` `TestRedisApplyObserverZSetSignalRequiresFullRecheck` | Token-identical; `6fc69faa` removed the "pending local apply" state it is named after. |
| `adapter/redis_lua_pool_test.go:150` `TestLua_PoolSerialAcquireReusesState` | `:191` `TestLua_PoolRecordsReuseVsAllocation` | Same pool and loop; the survivor also checks miss accounting. |
| `adapter/redis_command_test.go:382` `TestCommand_RoutedViaServer` and `:413` `TestCommand_RouteMatchesTable` | `TestCommandCount_MatchesTableSize` | The first asserts a map it built itself. The second is true by construction since `ccde8689`: both tables come from one `redisCommandSpecs` loop, and drift already panics in `buildRouteMap`. |
| `adapter/redis_server_setup_test.go` (whole file) | — | It checks the store type that the test harness itself injects (`test_util_test.go` wraps the store in `kv.NewLeaderRoutedStore`). Production passes `shardStore` (`main.go:3701`). |

**Retarget.**

1. **HDEL.**
   - Problem: `adapter/redis_collection_ttl_test.go:1602` `TestRedisHDelLegacyRewritePreservesLegacyTTL` (regression from `66d868cd`) calls the dead `persistHashTxn` (`redis_hash_cmds.go:532`). The live path, `hdelTxn` → `persistHashReadTimestampTxn` (`:518`, `:567`), has 0% coverage across the adapter suite.
   - Change:
     - Drive the scenario through `server.hdelTxn(...)`, or `persistHashReadTimestampTxn` with a timestamp from `kv.BeginReadTimestampThrough`.
     - Delete `persistHashTxn`.
   - Mutation that must now fail: drop the legacy-TTL preservation step in `persistHashReadTimestampTxn`.
2. **EXPIRE dispatch.**
   - Problem: six tests call uint64 twins left behind by merge `227aff51`:
     - `dispatchExpireForType` (`redis_expire_cmds.go:296`)
     - `dispatchCollectionExpire` (`redis_collection_ttl.go:135`)
     - `dispatchHLLExpire` (`redis_expire_cmds.go:424`)

     `dispatchStringExpire` (`:389`) is dead and untested.
   - The six tests:
     - `redis_collection_ttl_test.go:213` `TestRedisCollectionExpireHandlesLegacyBlobs`
     - `:365` `…AllowsDeltaHeavyCollections`
     - `:493` `…TriggersCompactionForDeltaOnlyTruncatedCollections`
     - `:572` `…WritesInlineTTLForZeroLengthSimpleMeta`
     - `:646` `…DoesNotReviveDeltaEmptiedSimpleMeta`
     - `redis_hll_ttl_test.go:175` `TestRedisDispatchHLLExpireWritesInlineAnchorAndScanIndex`
   - Change:
     - Get a read timestamp with `kv.BeginReadTimestampThrough(ctx, coord, ts, "test")` (or `prepareExpireReadTimestamp`) and call the live `*ReadTimestamp` variants, e.g. `dispatchExpireForReadTimestamp`.
     - Move the OCC comment from `dispatchStringExpire` onto its live twin.
     - Delete the four uint64 variants.
   - Mutation that must now fail: for each test, break the live branch it pins, e.g. skip the inline-TTL anchor write for zero-length simple meta.
3. **Read fence (`adapter/redis_txn.go`).**
   - The dead key-only chain:
     - `redisTxnReadFenceKeys` (`:80`)
     - `redisTxnReadFenceKeysForRanges` (`:130`)
     - `readFenceTargetKeys` (`:152`; keep as a test helper only if still needed)
     - `redisQueuedCommandReadFenceKeys` (`:278`)
     - `redisQueuedCommandReadFenceKeysForServer` (`:282`)
     - `redisCommandReadFenceKeysForServer` (`:301`)
     - `queuedCommandReadFenceGroupKeys` (`:382`)
     - `redisReadFencedTimestamp` (`:595`)
     - `redisReadFencedReadTimestamp` (`:608`)
   - The `r == nil` branch of `commandReadFenceTargets` (`:422-427`) produces output that is always discarded.
   - Tests (in `redis_txn_test.go`):
     - `:1197` `TestRedisExecReadFenceUsesRedisStorageKeys`: the only test of the live `!sqs` bare-key exclusion (`redisLegacyBareReadFenceAllowed`), but it runs through the dead copy. Call `commandReadFenceTargets` / `redisTxnReadFencePointKeys`.
     - `:601` `TestRedisExecReadFenceUsesRangeRoutesAndExactHashFields` and `:640` `TestRedisExecReadFenceUsesOnlyCommandRelevantRanges`: call `queuedCommandReadFenceGroupTargets` directly (one line each).
     - `:801` `TestRedisReadFencedTimestampLeasesBeforeAndAfterSelectingTimestamp`: call `redisReadFencedTimestampForTargets`.
   - Then:
     - delete the dead chain and the discarded `r == nil` branch;
     - fix the comments at `:386-389` and `:409-410`.
   - Mutation that must now fail: make `redisLegacyBareReadFenceAllowed` always return true.
4. **Unsupported command name with invalid UTF-8.**
   - Problem: `adapter/redis_metrics_test.go:519` `TestRedisMetricsUnsupportedCommandRawInvalidUTF8Bytes` calls the monitoring observer directly, duplicating `:429`. The adapter call site its commit fixed (`db4fa305`, `redis.go:835-845`) has 0% coverage.
   - Change: rewrite it as a raw-RESP test on a one-node cluster (pattern: `TestRedisRESP2NullArrayReplies` in `redis_resp_compat_test.go`). Send a command named `\xff\xff\xff` and assert both the unsupported-command reply and the sanitised metric label.
   - Mutation that must now fail: pass the raw name to the observer at the call site.
5. **Dual-encoding WRONGTYPE guard.**
   - Problem: `adapter/redis_collection_fastpath_test.go:277` `TestRedis_HGET_FastPathGuardDualEncodingSmoke` never reaches `hasHigherPriorityStringEncoding`, because SET tombstones the hash row first. The guard branches run nowhere in the suite: `redis_hash_cmds.go:296-298` and `:639-641`, `redis_set_cmds.go:674-676`.
   - Change: replace it with a unit test that seeds both a string encoding and a hash field / set member directly in the store, then asserts WRONGTYPE from `hget`, `hexists` and `sismember`.
   - Mutation that must now fail: make `hasHigherPriorityStringEncoding` return false.
6. **EXEC dispatch timeout.**
   - Problem: `adapter/redis_compat_commands_stream_test.go:932` `TestRedis_StreamMultiExecDelHonoursDispatchTimeout` cannot observe a timeout; its doc calls it "smoke-level". It is a subset of `:733` `TestRedis_StreamMultiExecDelRemovesWideColumnLayout`.
   - Change: rewrite it with a store wrapper whose `ScanAt` blocks until its context is done (pattern: `redis_delta_compactor_test.go:1206`), and assert EXEC returns within the dispatch budget. If that is not feasible without production changes, delete it.
   - Mutation that must now fail: use `context.Background()` for the EXEC dispatch.

**Done when:**
- `go test -race ./adapter/... .` passes.
- lint reports 0 issues.
- The mutation table covers retargets 1–6.
- The Redis Jepsen suite is not required: there is no runtime change, only removal of unreferenced code. Say so in the PR.

### WP-5: non-Redis adapters (`adapter/{dynamodb,s3,sqs,admin_backup,distribution_server,…}`)

**Delete.**

| Test | Survivor | Why it is redundant |
|---|---|---|
| `adapter/admin_backup_test.go:1213` `TestBeginBackupMapsCapacityReservationToResourceExhausted` | `admin_backup_renew_race_test.go:206` `TestBeginBackupSkipsUnreserveOnDefinitiveCapacityRejection` | Same setup and status; the survivor also checks that no Unreserve is proposed. |
| `adapter/admin_backup_renew_race_test.go:77` `TestRenewBackupStillReleasesWhenItOwnsTheSession` | `admin_backup_test.go:1182` `TestRenewBackupReleasesResourcesAfterPinFanoutFailure` | Same scenario. The failure counts differ (8 vs 3), but the test server caps `RenewAttempts` at 3, so both exhaust retries. |
| `adapter/s3_test.go:1304` `TestS3Server_RangeReadInvalidRange` | `:1383` `TestS3Server_InvalidRangeContentRangeHeader` | Both request a range past the end of the object and expect 416; the survivor also checks `Content-Range`. |
| `adapter/s3_test.go:2099` `TestS3Server_PrivateBucket_AnonymousGetRejected` | `:2130` `TestS3Server_PutBucketAcl_ChangeToPublicAndBack` | Same steps as the survivor's first half. |
| `adapter/sqs_test.go:130` `TestSQSServer_AllTargetsHaveHandlers` | `:102` `TestSQSServer_UnknownTargetReturnsInvalidAction` | Despite its name it only posts an unknown target and expects 400, a subset of the survivor. |
| `adapter/sqs_test.go:88` `TestSQSServer_HealthRejectsNonGetHead` | `sqs_health_test.go:98` `TestServeSQSHealthz_RejectsNonGETHEAD` | The survivor checks the exact `Allow` header in both modes. |
| `adapter/dynamodb_test.go:221` `TestDynamoDB_Healthz` | `dynamodb_metrics_test.go:81` `TestDynamoDBServerHealthzSkipsDynamoMetrics` | Starts a Raft node only to check `200 "ok\n"` from a handler with no coordinator dependency. |
| `adapter/dynamodb_test.go:301` `TestDynamoDB_TransactWriteItems` | `:364` `TestDynamoDB_TransactGetItems` | Identical `TransactWriteItems` call; the survivor also reads the items back. |
| `adapter/sqs_health_test.go:120` `TestSQSAdvertisedCapabilities_TracksFlag` | — | `htfifoCapabilityAdvertised` is a `const true`; the else branch cannot run. |
| `adapter/distribution_server_test.go:410` `TestWithCatalogReloadRetryPolicy_OverridesDefaults` | — | Sets an option and reads the same fields back. |

**Retarget.**

1. **DynamoDB base-table hash-key mismatch.**
   - Problem: `adapter/dynamodb_lease_validation_test.go:158` `TestDynamoDB_LeaseCheckQuery_WholeTableQueryStillFencesAllGroups` is byte-identical, apart from messages, to `dynamodb_lease_fence_test.go:115` `TestDynamoDB_QueryLeaseFallbackFencesAllGroups`. It runs the GSI path, not the base-table hash-key-mismatch branch its name and doc describe (`dynamodb_lease.go:316`). That branch is untested.
   - Change: query the base table with a key condition that does not pin the table's hash key, so `queryLeasePrefix` takes the mismatch branch, and assert that every group is fenced.
   - Mutation that must now fail: make that branch return the single-group prefix.
2. **SigV4 signature parsing.**
   - Problem: `adapter/s3_test.go:1823` `TestExtractS3Signature` tests `extractS3Signature` (`s3_auth.go:428`), dead since `#606`. The live parser `extractSigV4Signature` (`sigv4.go:128`) has no direct test.
   - Change:
     - Move the table into a new `adapter/sigv4_test.go` against `extractSigV4Signature`.
     - Delete `extractS3Signature`.
   - Mutation that must now fail: break the `Signature=` delimiter handling in `extractSigV4Signature`.
3. **SQS throttle reset.**
   - Problem: seven tests call `bucketStore.invalidateQueue` (`sqs_throttle.go:449`), which has had no production caller since `07365fcf`. Production calls `beginQueueReset` (`:198`) before commit (via `beginThrottleReset`, `sqs.go:381`) and `invalidateQueueBuckets` (`:457`) after commit.
   - The tests, in `adapter/sqs_throttle_test.go`: lines 169, 234, 713, 757, 846, 864, 942.
   - Change:
     - Replace each call with the production sequence.
     - The test around line 160 duplicates the live-path test that follows it: delete it.
     - Delete `invalidateQueue` and `bumpQueueEpoch` (`:187`; it equals `beginQueueReset(q, nil)`).
     - Move the lock-ordering comment onto `invalidateQueueBuckets`.
     - Update `docs/design/2026_04_26_implemented_sqs_per_queue_throttling.md:115,117`.
   - Mutation that must now fail: skip the epoch bump in `beginQueueReset`.

**Done when:**
- `go test -race ./adapter/... .` passes.
- lint reports 0 issues.
- The mutation table covers the three retargets.

### WP-6: root package, `cmd/**`, `proxy`

**Delete.**

| Test | Survivor | Why it is redundant |
|---|---|---|
| `main_encryption_registration_test.go:847` `TestBuildProcessStartRegistrationGate_Phase0BootDoesNotMarkRegistered` | `:311` `TestBuildProcessStartRegistrationGate_NilGateBranches`, row `phase0_envelope_inactive` | Same input (`testRegDEKID` is 7) and the same checks. It became a duplicate when `28c4d42c` removed its extra step. Move its §2.3 comment onto the row. |
| `main_admin_forward_test.go:40` `TestBuildLeaderForwarder_HappyPathReturnsForwarder` | `:197` `TestBuildAdminLeaderForwarder_HappyPathReturnsForwarder` | Same inputs and checks; the wrapper only delegates. |
| `main_encryption_write_wiring_test.go:53` `TestEncryptionWriteWiring_PebbleOptions_CleartextWhenNoCipher` and `:65` `…_WiredWhenCipherSet` | `:184` (disabled → cleartext), `:207` (active DEK → cipher wired) | Same branches of `pebbleOptions`. |
| `main_encryption_metrics_wiring_test.go:117` `TestMetricsRegistryEncryptionObserverSatisfiesStoreInterface` | compile time (`main.go:515`), `monitoring/encryption_test.go:164` | The interface is already enforced at compile time; the non-nil check is duplicated. |
| `main_encryption_fanout_test.go:142` `TestBuildCapabilityFanoutFn_NonNil` | — | `buildCapabilityFanoutFn` returns a function literal, so `fn != nil` cannot fail. |
| `main_encryption_admin_test.go:255`, `:271`, `:283` (`TestEncryptionAdmin_DedicatedTSOGroup*`, `…DataGroupPreservesMutatorGate`) | `shard_config_test.go:404`, `main_encryption_admin_test.go:123`, `adapter/encryption_admin_test.go` follower-rejection tests | They test only `encryptionAdminWiringForGroup`, which has had no production caller since merge `227aff51`. The invariant is held by `defaultGroupID` skipping group 0. Delete them, their now-unused helpers, and the function. |
| `cmd/elastickv-admin/main_test.go:93` `TestLoadTokenReadsAndTrims` and `:109` `TestLoadTokenRejectsEmptyFile` | `internal/tokenfile_test.go:10` / `:26` | Same fixtures and assertions; `loadToken` only calls `LoadBearerTokenFile`. |
| `cmd/elastickv-admin/main_test.go:815` `TestFanoutClientForRaceDeduplicates` | `:311` `TestFanoutClientForDeduplicatesConcurrentDials` | The survivor is stronger (start barrier). |
| `proxy/pubsub_test.go:155`, `:172`, `:185` (`TestPubSub_SubscribeDuplicate_IdempotentCount`, `…UnsubscribeNonSubscribed_NoEffect`, `…UnsubscribeSpecific_RemovesFromSet`) | — | They exercise only Go map insert and delete, not proxy code. `subCount` is covered elsewhere in the file. |
| Helper `cmd/elastickv-snapshot-prepare-restore/main_fifo_unix_test.go:70` `writeEncodeInfoWithRecordedSHA` | `main_test.go:304` `writeEncodeInfoWithSHA(t, in, id, true, true, sha)` | Identical helper. |

**Retarget.**

1. **KEK unwrap timing decorator.**
   - Problem: `main_encryption_metrics_wiring_test.go:153` `TestTimedKEKUnwrapperCoversStartupPreflightUnwraps` (from `b978c3dc`) never calls a `main.go` function, so it would pass against the old ordering.
   - Change: run it non-parallel. Set `t.Setenv(kek.EnvVar, …)`, `*encryptionEnabled = true` and a temporary sidecar path (restore the globals with `t.Cleanup`), call `loadKEKAndRunStartupGuards(counter)` (`main.go:1853`), and assert the counter is greater than 0.
   - Mutation that must now fail: decorate the wrapper after the startup guards run.
2. **Orphaned dial.**
   - Problem: `cmd/elastickv-admin/main_test.go:369` `TestFanoutClientForOrphanedDialClosed` never reaches the orphan path; its comment says so.
   - Change: after `clientFor(addr)`, call `f.installOrAttach(addr, conn2)` (`cmd/elastickv-admin/main.go:500`). Assert that `conn2` is closed and the cached client is returned.
   - Mutation that must now fail: do not close the orphan in `installOrAttach`.
3. **Dead start/read wrappers.** Each test reaches live code only through a dead wrapper; these are the only unit tests of that logic.

   | Tests | Dead wrapper | Live function | Origin |
   |---|---|---|---|
   | `main_pprof_test.go` (2 tests), `main_metrics_test.go` (1 test) | `startPprofServer` (`main.go:3784`) / `startMetricsServer` (`:3820`) | `preparePprofServer` / `prepareMetricsServer` | dead since `be5da97d` |
   | `main_s3_test.go:20/27` | `startS3Server` / `prepareS3Server` (`main_s3.go:23-77`) | `newS3Server` | — |
   | `main_encryption_registration_test.go:181` `TestReadWriterRegistryLastSeen` | `readWriterRegistryLastSeen` | `registryLastSeen` | `f75b8773` |

   - Change: point each test at the live function, delete the wrappers, and fix the stale comment at `main_sqs.go:64`.
   - Mutation that must now fail: remove the address/token validation in `prepare*`.

**Done when:**
- `go test -race . ./cmd/... ./proxy/...` passes.
- lint reports 0 issues.
- The mutation table covers retargets 1–3.

### WP-7: `internal/admin`, `distribution`, `keyviz`

| Test | Survivor | Why it is redundant |
|---|---|---|
| `internal/admin/server_test.go:507` `TestServer_WriteRejectsMissingCSRF` | `:408` `TestServer_DynamoCreateTable_MissingCSRFRejected` | It still **passes** with `CSRFDoubleSubmit` removed from the protect chain (`server.go:315`); the survivor fails as it should. It is a placeholder that accepts any of 401/403/405. |
| `internal/admin/keyviz_fanout_test.go:853` `TestKeyVizFanoutRunPeerOverlargeBody` | `:767` `TestKeyVizFanoutRunPeerExceedsBodyLimit` | Despite its name, its body is under the cap and it only checks that an in-cap body succeeds. It is also flaky under load (2.05 s against the 2 s per-peer timeout). |
| `distribution/watcher_test.go:223` `TestCatalogWatcherLoggerOption` | — | Sets an option and reads it back. |
| `keyviz/labels_test.go:17` `TestAllLabelConstantsInAllLabels` | — | Compares `AllLabels` with a hand-copied list, so it only fails when the test itself goes stale. |

**Done when:** `go test -race ./internal/admin/... ./distribution/... ./keyviz/...` passes and lint reports 0 issues.

### WP-8: Shared test-support packages

Create the packages and their own unit tests. Do not migrate call sites yet, apart from one example per package to prove the API. Every package here is imported only from `_test.go` files.

**Layering.**

Go `_test.go` helpers cannot be imported across packages, and most test files here are internal: `kv` 76/0 and `adapter` 122/0 internal/external. So a `<pkg>test` package that imports `<pkg>` cannot be used by that package's own tests.

| Package | May import | Usable from | Contents |
|---|---|---|---|
| `internal/testutil` | stdlib, `testify` | every package | `Recv[T](tb, ch, d, format, args...) T`, `NoRecv[T](tb, ch, d, format, args...)`, `Closed(tb, ch, d, …)`, `Listen(tb) net.Listener` (127.0.0.1:0, closed on cleanup) |
| `internal/testutil/grpctest` | `grpc`, root `internal` | every package | `Serve(tb, register func(*grpc.Server), opts...) (addr string)`, defaulting to `internal.GRPCServerOptions()` so tests use production message limits; `Dial(tb, addr, opts...) *grpc.ClientConn`. Both register `t.Cleanup`. |
| `internal/raftengine/raftenginetest` (extend) | `raftengine`, `monoclock` | etcd-internal, kv-internal, adapter, root, raftadmin | Export `NewSingleNode(tb, factory, sm)` and `WaitForLeader(tb, d, views...) int`. Add `StubEngine` (configurable `State`, `Leader`, `ProposeFunc`, `ReadIndex`/`ReadErr`, `StatusValue`, `Config`), `StaticLeaderView`, `RecordingProposer`, `ApplyingProposer(sm)`. |
| `internal/encryption/encryptiontest` | `encryption` | store-internal, etcd, kv, root, adapter, cmd | `NewCipher(tb, keyID) *encryption.Cipher`, `WriteSidecar(tb, *encryption.Sidecar) (path string)`, `NewNonceFactory(tb)`, `WriteKEKFile(tb)` |
| `store/storetest` | `store` | kv, adapter, distribution, etcd, root, cmd (not store-internal) | `NewPebble(tb, opts...) store.MVCCStore` with `t.TempDir` and cleanup |
| `kv/kvtest` | `kv`, `store` | adapter, filesystem, root (not kv-internal) | `NopCoordinator`: always leader, no-op `Dispatch`, lazily created `*kv.HLC`. `LocalCoordinator`: applies `OperationGroup`s to a `store.MVCCStore`, with explicit options for each behaviour the current fakes disagree on (`DEL_PREFIX` handling, commit-timestamp source, OCC read-key validation, `CommitTSValueOffset` patching). It replaces `fstest.MemoryDispatcher`, so filesystem-internal tests can drop their copy. |
| `kv/fakes_test.go` (in-package) | — | kv-internal | `nopCoordinator`, plus engines built on `raftenginetest` |

Also export `store.LegacyListMetaDeltaKey(userKey, ts, seq)` next to `store.ListMetaDeltaKey` (`store/list_helpers.go:75`). It replaces five test copies of `legacyListMetaDeltaKey` / `deltaLookingListMetaUserKey`, which today disagree on `seq` (adapter uses 0, kv uses 1).

**Rules.**

1. **Exact interface.** Base fakes implement exactly the target interface (`kv.Coordinator`, `raftengine.Engine`, `raftengine.LeaderView`, `raftengine.Proposer`) and **none** of the optional capabilities:
   - `raftengine`: `LeaseProvider`, `Admin`, `HealthReader`, `StartupBarrier`;
   - `kv`: `LeaseReadableCoordinator`, `GroupRoutableCoordinator`, `AllGroups*`.

   Production type-asserts these (`kv/coordinator.go:340-389`, `kv/keyviz_label.go:152-171`). Embedding a richer base would silently change what tests that check the fallback cover, e.g. `nonLeaseEngine` in `kv/lease_read_test.go`. Offer opt-in wrappers such as `StubLeaseEngine` instead.
2. **Production must not import test code.** Add a `depguard` rule (or a CI step around `go list -deps . ./cmd/...`) so no production package imports any of these:
   - `testing`
   - `github.com/stretchr/testify/...`
   - `internal/testutil/...`
   - any `*test` package
3. **Lint exclusion.** Extend the WP-0 path exclusion with `(^|/)testutil/`, because `testutil` does not end in `test`.
4. **Keep a fake when its behaviour is the point.** A fake whose behaviour is the subject of the test stays local; do not force it onto a shared base.

**Done when:**
- Each new package has unit tests.
- The `depguard` guard fails on a deliberate violation; show this in the PR.
- lint reports 0 issues.
- One example migration per package compiles and passes.

### WP-9 to WP-12: Migrate call sites onto the shared packages

Each WP owns a directory set (table above) and applies all of the migrations below within it. That way, two parallel PRs never touch the same file.

1. **Receive-or-fail `select` blocks → `testutil.Recv` / `NoRecv` / `Closed`.**
   - About 160 blocks at `95889d1f`:

     | Directory | Blocks |
     |---|---|
     | `internal/raftengine/etcd` | 53 |
     | `adapter` | 49 |
     | root | 20 |
     | `kv` | 10 |
     | `proxy` | 9 |
     | `store` | 8 |
     | `internal/filesystem/fuseadapter` | 7 |
     | `keyviz` | 4 |
     | `internal/memwatch` | 2 |
     | `internal/encryption/kek` | 1 |

   - Also replace these named variants:
     - `adapter/redis_apply_observer_test.go:12`
     - `kv/lease_warmup_test.go:602`
     - `kv/tso_floor_test.go:248`
     - `internal/raftengine/etcd/engine_test.go:1837`
   - Find them with a scan for `select { … }` blocks of at most three cases that contain `time.After(` and a `t.Fatal*`/`t.Error*`/`require.Fail*`.
   - Keep each block's timeout and failure message.
2. **Fake Raft engines → `raftenginetest`.**
   - The 13 full-`Engine` fakes:
     - `kv/coordinator_retry_test.go:23` `stubLeaderEngine` (≈ `kv/sharded_coordinator_txn_test.go:1378` `noopEngine`)
     - `kv/leader_proxy_test.go:80`, `:196`
     - `kv/lease_read_test.go:21`, `:154`
     - `kv/shard_store_test.go:1245`
     - `kv/tso_raft_test.go:711`
     - `internal/raftadmin/server_test.go:21`, `:411`
     - `main_tso_routing_test.go:360`
     - `main_sqs_leadership_refusal_test.go:51`
     - `main_encryption_confchange_test.go:271`
   - About 10 `LeaderView`-only fakes, e.g.:
     - `adapter/internal_admin_proposal_test.go:16`
     - `kv/leader_admin_proposer_test.go:16`
     - `kv/raft_engine_test.go:17`
   - About 12 `Proposer`-only fakes, e.g.:
     - `adapter/encryption_admin_test.go:1474`
     - `adapter/internal_migration_test.go:60`
     - `kv/raft_payload_wrapper_test.go:23`
     - `main_encryption_rotate_on_startup_test.go:772`
   - Replace `internal/raftengine/etcd/engine_test.go:28-101` (`testStateMachine`, `testSnapshot`) with `type testStateMachine = raftenginetest.TestStateMachine`; `mustEncodeSnapshotData` needs an `Apply` loop instead of a struct literal.
   - Replace the kv/adapter single-node engine helpers with `raftenginetest.NewSingleNode`, which removes the 43 `defer stop()` lines:
     - `kv/sharded_integration_test.go:23/33`
     - `kv/shard_router_test.go:17` `newTestRaft`
     - `adapter/distribution_milestone1_e2e_test.go:237`
     - `adapter/s3_test.go:927`
3. **Fake coordinators → `kvtest` (outside kv) or `kv/fakes_test.go` (inside kv).**
   - The 12 full-`Coordinator` fakes:
     - `adapter/redis_keys_pattern_test.go:14` `stubAdapterCoordinator` (base for 18 files)
     - `adapter/redis_hello_test.go:98` and `adapter/redis_info_test.go:14` (near-identical to the base)
     - `adapter/redis_retry_test.go:20`
     - `adapter/dynamodb_test.go:2035` `testCoordinatorWrapper` (pure forwarding: embed `kv.Coordinator`)
     - `adapter/distribution_server_test.go:1487`
     - `kv/leader_routed_store_test.go:16`
     - `kv/lease_group_keys_test.go:16` (every method panics: use `struct{ Coordinator }`)
     - `kv/sharded_coordinator_txn_test.go:936` `phaseDGate` (pure forwarding: embed `*ShardedCoordinator`) and `:941`
     - `kv/tso_test.go:774`
     - `main_encryption_rotate_on_startup_test.go:163`
   - The six in-memory apply coordinators move onto `kvtest.LocalCoordinator`, choosing options that reproduce each one's current behaviour exactly; any intentional change must be called out in the PR:
     - `internal/filesystem/fstest.MemoryDispatcher`
     - filesystem `testCoordinator` (`internal/filesystem/service_test.go:24`)
     - `adapter/dynamodb_migration_test.go:15` `localAdapterCoordinator` (about 190 references in 20 files)
     - `adapter/redis_list_pop_benchmark_test.go:77` `occAdapterCoordinator` (a shared fake that lives in a benchmark file)
     - `adapter/distribution_server_test.go:1646` `coordinatorStubMutations`
     - `adapter/redis_retry_test.go:35`
   - Remove stub `LeaseRead` methods that only call `LinearizableRead`; `kv.LeaseReadThrough` already falls back to it.
4. **Encryption fixtures → `encryptiontest`.**
   - 27 `NewCipher` sites in 15 files across 6 packages.
   - 44 `WriteSidecar` calls behind 9 helpers. `adapter/encryption_admin_test.go:1537` `writeSidecarFixture` and `cmd/elastickv-admin/encryption_test.go:785` `writeStatusFixture` are byte-identical.
   - `wiringEncryptionObserver` (`main_encryption_metrics_wiring_test.go:25`) ≈ `recordingEncryptionObserver` (`store/lsm_store_encryption_metrics_test.go:31`).
   - Delete `store/encryption_test_helpers.go`. It exports a test-only `CounterNonceFactory` from production `store`; its byte layout matches `encryption.NewDeterministicNonceFactory`, which the 17 calls in 7 store test files can use instead.
5. **gRPC loopback servers → `grpctest.Serve`.**
   - About 27 sites:
     - `kv`: `leader_routed_store_test.go:192`, `leader_proxy_test.go` ×3, `grpc_conn_cache_test.go` ×2, `leader_admin_proposer_test.go` ×2, `sharded_coordinator_leader_test.go:100`
     - root: `main_bootstrap_e2e_test.go:770`, `main_admin_test.go:272`
     - `adapter`: `distribution_server_test.go:383`, `distribution_server_delta_test.go:414`, `s3_blob_m1_test.go:544/601/628`
     - etcd, raftadmin and `cmd/elastickv-admin`
   - The root e2e harness also switches to production server options.
6. **Duplicated list-key helpers → `store.LegacyListMetaDeltaKey`** (from WP-8):
   - `adapter/redis_list_dedup_test.go:320/334`
   - `distribution/migrator_export_plan_test.go:568/583`
   - `kv/shard_key_test.go:338`
   - `store/list_helpers_test.go:230`
   - Leave the `internal/backup` mirrors alone. They are independent on purpose, as a cross-check (`ff38ad84`, `badd9400`).

**Done when:**
- `go test -race` passes for the owned directories and root `.`.
- lint reports 0 issues.
- The PR states the before/after counts for each migrated pattern.
- No test lost an assertion; diff-review each replaced block.

## Mutation-check recipe

```sh
# 1. Copy the production file and apply the mutation to the copy only.
mkdir -p /tmp/mut && cp kv/raft_payload_wrapper.go /tmp/mut/raft_payload_wrapper.go
$EDITOR /tmp/mut/raft_payload_wrapper.go        # e.g. replace `return nil, err` with `wrapped = data`
# 2. Point the build at the copy without touching the tree.
cat > /tmp/mut/overlay.json <<EOF
{"Replace": {"$(pwd)/kv/raft_payload_wrapper.go": "/tmp/mut/raft_payload_wrapper.go"}}
EOF
# 3. The retargeted test must FAIL under the mutation and PASS without it.
go test -overlay /tmp/mut/overlay.json -count=1 -run 'TestDynamicWrappedProposer_PropagatesWrapperError' ./kv/
go test -count=1 -run 'TestDynamicWrappedProposer_PropagatesWrapperError' ./kv/
```

Record each mutation in the PR as a row: file and line, the change, the test that failed, and the test's result on the unmodified tree.

## Decisions

- **One PR per work package, partitioned by directory.** WP-1 to WP-8 can run in parallel; WP-9 to WP-12 wait for the packages they edit.
- **Delete orphans in the same PR.** A function left unreferenced by a retarget is deleted in the same PR. golangci-lint's `unused` would otherwise fail CI for unexported functions, and exported ones would remain dead.
- **Prove every retarget with a mutation.** A retarget is only accepted with a mutation that fails the retargeted test. "The test still passes" is not evidence that it guards the live path.
- **Layering and the exact-interface rule** are as described in WP-8.
- **Narrow lint exclusion.** The test-only lint exclusion is anchored to `_test.go` files and `*test/` test-support directories (WP-0); WP-8 adds `testutil/`.
- **FUSE tests are mandatory in CI.** CI installs `fuse3` and sets `ELASTICKV_REQUIRE_FUSE=1`, so missing FUSE coverage fails instead of skipping (WP-0).

## Follow-ups (out of scope)

- **Product bugs found by the audit.** Each needs its own PR with a failing test first.
  - Fixed: SQS FIFO messages in one group sent within the same millisecond could be received out of order, because the visibility key orders by `(visibleAtMillis, random messageID)`. See `2026_10_29_implemented_sqs_fifo_same_millisecond_order.md`.
  - The Redis TTL-inline migration builds zero scan plans in production. `startupGatedCoordinator` does not forward `LocalLeaderGroupIDs`, and `keyVizLabeledCoordinator` returns nil when the inner value lacks it. Regression since `a36dac7f`.
  - Admin write authorization: SQS trusts the live role alone; S3 and DynamoDB require both the JWT role and the live role. Tests pin both. A policy decision is needed.
  - S3 SigV4 `ListObjectsV2` returns 500 for an expired continuation token; only the admin path translates `ErrReadTSCompacted`.
  - `store.PrefixScanEnd` returns a looser bound than the other six prefix-end helpers (not yet verified).
  - Merge `3dfbabd8` dropped 48 feature-branch tests and left `WithGRPCReadGate`, `proxy/noop_backend.go` and `ProxyConfig.PProfAddr` orphaned. Confirm the intent.
- **Production dead code that is only tested.** Delete it, porting live-relevant cases first:
  - `internal/raftengine/etcd/migrate.go` (`MigrateFSMStore`; the root guard tests use it as a fixture)
  - pre-WAL `persistence.go` writers
  - `encryption.CheckLocalEpochRollback` (its tests contradict the live guard)
  - `internal/backup` helpers (`ResolveCommitTS`, `peekMsgDataKey`, `IsBlobAtomicWrite*`, `HasInlineTTL`, `IsBinarySegment`, `effectivePartitionCount`, `WriteChecksumsWithVirtualFile`)
  - `adapter/redis_transcord.go` and its property test (`CLAUDE.md` cites it)
  - `selectXReadEntries`, `unmarshalStreamValue` and their benchmarks
  - SQS `throttleAttributesPresent`, `sqsMsgByAgePrefixesForQueue`
  - `fskeys.ChunkRouteHomeFromKey`
  - `distribution.IsCatalogDeltaKey`
  - autosplit `ObserveSnapshot` / `ReadCommittedWindows`
  - `monitoring` `observeLeaderChange`
  - unused options listed in the audit
- **CI runtime.**
  - `adapter/grpc_test.go` `Test_consistency_satisfy_write_after_read_sequence` and `Test_grpc_transaction`: 9,999 sequential RPCs each, about 5.5 min under `-race`. Iteration count is a maintainer decision (restored on purpose in `24b9f80b`).
  - `internal/encryption/keystore_test.go` `TestKeystore_Concurrent`: no assertion, 179 s under `-race`; bounding the key space makes it about 3 s.
  - `TestSnapshotSpool_DefaultCapAcceptsRealisticFSM` writes 1.5 GiB.
  - `TestS3Objects_Put_Rejects413OnOversizedBody` streams 100 MiB.
  - The 8 serial `raft_member_replace_script` tests.
  - Per-file cluster sharing for the 167 Redis tests that each boot a Raft cluster.
- **CI never runs `proxy/integration_test.go`**, because there is no Redis service.
- **`TestFinalizeAsFSMFile_RenameFailureCleansUpSpool` fails when run as root,** because `chmod 0500` does not stop root. Skip it under `os.Geteuid() == 0` or use an injection seam.
