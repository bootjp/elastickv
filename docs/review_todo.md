# Review TODO

Critical and high-severity issues found during a comprehensive code review.
Items are ordered by priority within each section.

---

## 1. Data Loss

### ~~1.1 [Critical] `saveLastCommitTS` uses `pebble.NoSync` — timestamp rollback after crash~~ DONE

- **Status:** Fixed. `saveLastCommitTS` changed to `pebble.Sync`; `ApplyMutations` writes `lastCommitTS` atomically in the same `WriteBatch`.

### 1.2 [Critical] Batch Apply in FSM allows partial application without rollback

- **File:** `kv/fsm.go:44-69`
- **Problem:** When a `RaftCommand` contains multiple requests, each is applied individually. If request N fails, requests 1..N-1 are already persisted with no rollback. The client receives an error but has no visibility into which writes succeeded.
- **Fix:** Apply all requests in a single atomic store batch. On any error, discard the entire batch.

### ~~1.3 [High] `pebbleStore.Compact()` is unimplemented — unbounded version accumulation~~ DONE

- **Status:** Fixed. Implemented MVCC GC for pebbleStore and `RetentionController` interface (`MinRetainedTS`/`SetMinRetainedTS`).

### 1.4 [High] Secondary commit is best-effort — lock residue on failure

- **File:** `kv/sharded_coordinator.go:181-217`
- **Problem:** After primary commit succeeds, secondary commits retry 3 times then give up with a log warning. Remaining locks/intents block subsequent access to those keys. Cross-shard lock resolution may also fail under network partition.
- **Fix:** Introduce a background lock resolution worker that periodically scans for expired/orphaned locks and resolves them.

### ~~1.5 [High] `abortPreparedTxn` silently ignores errors~~ DONE

- **Status:** Fixed. Errors are now logged with full context (gid, primary_key, start_ts, abort_ts).

### 1.6 [High] MVCC compaction does not distinguish transaction internal keys

- **File:** `store/mvcc_store.go:888-956`
- **Problem:** Compaction treats `!txn|cmt|` and `!txn|rb|` keys the same as user keys. If commit/rollback records are pruned, lock resolution becomes impossible.
- **Fix:** Skip keys with transaction internal prefixes during compaction, or apply a separate GC policy for them.

---

## 2. Concurrency / Distributed Failures

### ~~2.1 [Critical] TOCTOU in `pebbleStore.ApplyMutations`~~ DONE

- **Status:** Fixed. `ApplyMutations` now holds `mtx.Lock()` from conflict check through batch commit.

### 2.2 [High] Leader proxy forward loop risk

- **File:** `kv/leader_proxy.go:32-41`
- **Problem:** Leadership can change between `verifyRaftLeader` and `raft.Apply`. `forward` uses `LeaderWithID()` which may return a stale address, causing a redirect loop.
- **Fix:** Re-fetch leader address on forward error and add a retry limit.

### 2.3 [High] Secondary commit failure leaves locks indefinitely

- **File:** `kv/sharded_coordinator.go:181-217`
- **Problem:** (Same as 1.4) No background lock resolution worker exists.
- **Fix:** (Same as 1.4)

### ~~2.4 [Medium] `redirect` in Coordinate has no timeout~~ DONE

- **Status:** Fixed. Added 5s `context.WithTimeout` to redirect gRPC forward call.

### ~~2.5 [Medium] Proxy gRPC calls (`proxyRawGet` etc.) have no timeout~~ DONE

- **Status:** Fixed. Added 5s `context.WithTimeout` to `proxyRawGet`, `proxyRawScanAt`, and `proxyLatestCommitTS`.

### 2.6 [Medium] `GRPCConnCache` leaks connections after Close

- **File:** `kv/leader_proxy.go:25-30`
- **Problem:** After `Close` sets `conns = nil`, subsequent `ConnFor` calls lazy-init a new map and create leaked connections.
- **Fix:** Add a `closed` flag checked by `ConnFor`.

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

### 3.5 [High] `VerifyLeader` called on every read — network round-trip

- **File:** `kv/shard_store.go:53-58`
- **Problem:** Each read triggers a quorum-based leader verification with network round-trip.
- **Fix:** Introduce a lease-based caching mechanism; verify only when the lease expires.

### ~~3.6 [High] `mvccStore.Compact` holds exclusive lock during full tree scan~~ DONE

- **Status:** Fixed. Split into 2 phases: scan under RLock, then apply updates in batched Lock/Unlock cycles (batch size 500).

### ~~3.7 [High] `isTxnInternalKey` allocates `[]byte` on every call (5x)~~ DONE

- **Status:** Fixed. Added package-level `var` for all prefix byte slices and common prefix fast-path check.

### 3.8 [Medium] txn codec `bytes.Buffer` allocation per encode

- **File:** `kv/txn_codec.go:31-42, 82-97, 147-157`
- **Fix:** Use `make([]byte, size)` + `binary.BigEndian.PutUint64` directly, or `sync.Pool`.

### 3.9 [Medium] `decodeKey` copies key bytes on every iteration step

- **File:** `store/lsm_store.go:110-119`
- **Fix:** Add `decodeKeyUnsafe` that returns a slice reference for temporary comparisons; copy only for final results.

---

## 4. Data Consistency

### 4.1 [High] pebbleStore key encoding ambiguity with meta keys

- **File:** `store/lsm_store.go:102-119`
- **Problem:** Meta key `_meta_last_commit_ts` shares the user key namespace. A user key that happens to match the meta key bytes would be incorrectly skipped by `nextScannableUserKey`.
- **Fix:** Use a dedicated prefix for meta keys (e.g., `\x00_meta_`) that is outside the valid user key range.

### 4.2 [High] Write Skew not prevented in one-phase transactions

- **File:** `kv/fsm.go:268-294`
- **Problem:** Only write-write conflicts are detected. Read-write conflicts (write skew) are not tracked because there is no read-set validation.
- **Fix:** Document that the isolation level is Snapshot Isolation (not Serializable). If SSI is desired, add read-set tracking.

### 4.3 [High] VerifyLeader-to-read TOCTOU allows stale reads

- **File:** `kv/leader_routed_store.go:36-44`
- **Problem:** Leadership can be lost between `VerifyLeader` completion and `GetAt` execution, allowing a stale read from the old leader.
- **Fix:** Use Raft ReadIndex protocol: confirm the applied index has reached the ReadIndex before returning data.

### 4.4 [High] DynamoDB `ConditionCheck` does not prevent write skew

- **File:** `adapter/dynamodb.go:3952-3993`
- **Problem:** `TransactWriteItems` with `ConditionCheck` only evaluates conditions at read time but does not write a sentinel for the checked key. Another transaction can modify the checked key between evaluation and commit.
- **Fix:** Insert a dummy read-lock mutation for `ConditionCheck` keys so write-write conflict detection covers them.

### 4.5 [Medium] Cross-shard `ScanAt` does not guarantee a consistent snapshot

- **File:** `kv/shard_store.go:88-106`
- **Problem:** Each shard may have a different Raft apply position. Scanning multiple shards at the same timestamp can return an inconsistent view.
- **Fix:** Document the limitation, or implement a cross-shard snapshot fence.

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
