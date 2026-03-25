# Review TODO

Critical and high-severity issues found during a comprehensive code review.
Items are ordered by priority within each section.

---

## 1. Data Loss

### ~~1.1 [Critical] `saveLastCommitTS` uses `pebble.NoSync` — timestamp rollback after crash~~ DONE

- **Status:** Fixed. `saveLastCommitTS` changed to `pebble.Sync`; `ApplyMutations` writes `lastCommitTS` atomically in the same `WriteBatch`.

### 1.2 [Low — downgraded] Batch Apply in FSM allows partial application without rollback

- **File:** `kv/fsm.go:44-69`
- **Problem:** When a `RaftCommand` contains multiple requests, each is applied individually. If request N fails, requests 1..N-1 are already persisted with no rollback.
- **Analysis:** Downgraded from Critical. Batch items are independent raw writes from different clients. `applyRawBatch` returns per-item errors via `fsmApplyResponse`, so each caller receives their correct result. Partial success is by design for the raw batching optimization.
- **Remaining concern:** If a future code path batches requests that must be atomic, this would need revisiting.

### ~~1.3 [High] `pebbleStore.Compact()` is unimplemented — unbounded version accumulation~~ DONE

- **Status:** Fixed. Implemented MVCC GC for pebbleStore and `RetentionController` interface (`MinRetainedTS`/`SetMinRetainedTS`).

### ~~1.4 [High] Secondary commit is best-effort — lock residue on failure~~ DONE

- **Status:** Fixed. Added `LockResolver` background worker (`kv/lock_resolver.go`) that runs every 10s on each leader, scans `!txn|lock|` keys for expired locks, checks primary transaction status, and resolves (commit/abort) orphaned locks.

### ~~1.5 [High] `abortPreparedTxn` silently ignores errors~~ DONE

- **Status:** Fixed. Errors are now logged with full context (gid, primary_key, start_ts, abort_ts).

### ~~1.6 [High] MVCC compaction does not distinguish transaction internal keys~~ DONE

- **Status:** Fixed. Both mvccStore and pebbleStore compaction now skip keys with `!txn|` prefix.

---

## 2. Concurrency / Distributed Failures

### ~~2.1 [Critical] TOCTOU in `pebbleStore.ApplyMutations`~~ DONE

- **Status:** Fixed. `ApplyMutations` now holds `mtx.Lock()` from conflict check through batch commit.

### ~~2.2 [High] Leader proxy forward loop risk~~ DONE

- **Status:** Fixed. Added `forwardWithRetry` with `maxForwardRetries=3`. Each retry re-fetches leader address via `LeaderWithID()`. Returns immediately on `ErrLeaderNotFound`.

### ~~2.3 [High] Secondary commit failure leaves locks indefinitely~~ DONE

- **Status:** Fixed. (Same as 1.4) Background `LockResolver` worker resolves expired orphaned locks.

### ~~2.4 [Medium] `redirect` in Coordinate has no timeout~~ DONE

- **Status:** Fixed. Added 5s `context.WithTimeout` to redirect gRPC forward call.

### ~~2.5 [Medium] Proxy gRPC calls (`proxyRawGet` etc.) have no timeout~~ DONE

- **Status:** Fixed. Added 5s `context.WithTimeout` to `proxyRawGet`, `proxyRawScanAt`, and `proxyLatestCommitTS`.

### 2.6 [Low — downgraded] `GRPCConnCache` allows ConnFor after Close

- **File:** `kv/grpc_conn_cache.go`
- **Analysis:** Downgraded. `Close` properly closes all existing connections. Subsequent `ConnFor` lazily re-inits and creates fresh connections — this is by design and tested.

### ~~2.7 [Medium] `Forward` handler skips `VerifyLeader`~~ DONE

- **Status:** Fixed. Added `raft.VerifyLeader()` quorum check to `Forward` handler.

---

## 3. Performance

### ~~3.1 [Critical] `mvccStore.ScanAt` scans the entire treemap~~ DONE

- **Status:** Fixed. Replaced `tree.Each()` with `Iterator()` loop that breaks on limit and seeks via `Ceiling(start)`.

### ~~3.2 [Critical] PebbleStore uses unbounded iterators in `GetAt` / `LatestCommitTS`~~ DONE

- **Status:** Fixed. Both methods now use bounded `IterOptions` scoped to the target key.

### ~~3.3 [High] FSM double-deserialization for single requests~~ DONE

- **Status:** Fixed. Added prefix byte (`0x00` single, `0x01` batch) to `marshalRaftCommand` and `decodeRaftRequests` with legacy fallback.

### ~~3.4 [High] Excessive `pebble.Sync` on every write~~ DONE

- **Status:** Fixed. `PutAt`, `DeleteAt`, `ExpireAt` changed to `pebble.NoSync`. `ApplyMutations` retains `pebble.Sync`.

### 3.5 [High] `VerifyLeader` called on every read — network round-trip (deferred)

- **File:** `kv/shard_store.go:53-58`
- **Problem:** Each read triggers a quorum-based leader verification with network round-trip.
- **Trade-off:** A lease-based cache improves latency but widens the stale-read TOCTOU window (see 4.3). The current approach is the safe default — linearizable reads at the cost of one quorum RTT per read. Implementing leader leases is a major architectural decision requiring careful analysis of acceptable staleness bounds.

### ~~3.6 [High] `mvccStore.Compact` holds exclusive lock during full tree scan~~ DONE

- **Status:** Fixed. Split into 2 phases: scan under RLock, then apply updates in batched Lock/Unlock cycles (batch size 500).

### ~~3.7 [High] `isTxnInternalKey` allocates `[]byte` on every call (5x)~~ DONE

- **Status:** Fixed. Added package-level `var` for all prefix byte slices and common prefix fast-path check.

### ~~3.8 [Medium] txn codec `bytes.Buffer` allocation per encode~~ DONE

- **Status:** Fixed. `EncodeTxnMeta`, `encodeTxnLock`, `encodeTxnIntent` now use direct `make([]byte, size)` + `binary.BigEndian.PutUint64`.

### ~~3.9 [Medium] `decodeKey` copies key bytes on every iteration step~~ DONE

- **Status:** Fixed. Added `decodeKeyUnsafe` returning a zero-copy slice reference. Used in all temporary comparison sites (`GetAt`, `ExistsAt`, `skipToNextUserKey`, `LatestCommitTS`, `Compact`, reverse scan seek). `decodeKey` (copying) retained for `nextScannableUserKey`/`prevScannableUserKey` whose results are stored in `KVPair`.

---

## 4. Data Consistency

### ~~4.1 [High] pebbleStore key encoding ambiguity with meta keys~~ DONE

- **Status:** Fixed. Meta key changed to `\x00_meta_last_commit_ts` prefix. Added `isMetaKey()` helper for both new and legacy keys. `findMaxCommitTS()` migrates legacy key on startup. All scan/compact functions use `isMetaKey()` to skip meta keys.

### ~~4.2 [High] Write Skew not prevented in one-phase transactions~~ DONE

- **Status:** Fixed. Documented as Snapshot Isolation in `handleOnePhaseTxnRequest` and `MVCCStore.ApplyMutations` interface. Write skew is a known limitation; SSI requires read-set tracking at a higher layer.

### ~~4.3 [High] VerifyLeader-to-read TOCTOU allows stale reads~~ DONE (documented)

- **Status:** Documented as known limitation. The TOCTOU window between quorum-verified `VerifyLeader` and the read is inherently small. Full fix requires Raft ReadIndex protocol which is a significant architectural change. Added doc comment to `leaderOKForKey` explaining the trade-off.

### ~~4.4 [High] DynamoDB `ConditionCheck` does not prevent write skew~~ DONE

- **Status:** Already addressed. `buildConditionCheckLockRequest` writes a dummy Put (re-writing the current value) or Del for the checked key. This includes the key in the transaction's write set, so write-write conflict detection covers it.

### ~~4.5 [Medium] Cross-shard `ScanAt` does not guarantee a consistent snapshot~~ DONE

- **Status:** Documented. Added doc comment to `ShardStore.ScanAt` explaining the limitation and recommending transactions or a snapshot fence for callers requiring cross-shard consistency.

---

## 5. Test Coverage

Overall coverage: **60.5%** — **1,043 functions at 0%**.

### 5.1 [Critical] FSM Abort path entirely untested

- **File:** `kv/fsm.go` — `handleAbortRequest`, `buildAbortCleanupStoreMutations`, `appendRollbackRecord` all 0%.
- **Needed:** Prepare->Abort flow, abort rejection for committed txns, lock cleanup verification.

### 5.2 [Critical] PebbleStore transaction functions entirely untested

- **File:** `store/lsm_store.go` — `ApplyMutations`, `checkConflicts`, `LatestCommitTS`, `Compact` all 0%.
- **Needed:** Write conflict detection, atomic batch application, TTL put/expire, compaction visibility.

### 5.3 [Critical] `Coordinate.Dispatch` untested

- **File:** `kv/coordinator.go` — `Dispatch`, `dispatchRaw`, `redirect` all 0%.
- **Needed:** Leader dispatch, follower redirect, leader-absent error handling.

### 5.4 [High] ShardedCoordinator Abort rollback flow untested

- **Needed:** Test that when Shard2 Prepare fails, Shard1 (already prepared) receives a correct Abort.

### 5.5 [High] Jepsen tests are single-shard, single-workload only

- **Current:** Append workload on one Raft group, 30s duration.
- **Needed:** Multi-shard transactions, CAS workload, longer duration (5-10 min).

### 5.6 [Medium] No concurrent access tests for ShardStore / ShardedCoordinator

- Only `mvcc_store_concurrency_test.go` with 1 test exists.

### 5.7 [Medium] No error-path tests (I/O failure, corrupt data, gRPC connection failure)
