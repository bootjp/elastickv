# List Metadata Delta Design

## Objective

To resolve write conflicts caused by Read-Modify-Write (RMW) on list metadata (`!lst|meta|<key>`) during operations like `RPUSH`, `LPUSH`, `LPOP`, and `RPOP`, and to maintain conflict-free throughput even under high-concurrency append/pop workloads.

## Problem

### Current Structure

```
Key:   !lst|meta|<userKey>
Value: [Head(8)][Tail(8)][Len(8)]   ← Fixed 24 bytes
```

`ListMeta` stores `Head`, `Tail`, and `Len`. Every `RPUSH` or `LPUSH` follows this flow:

1. Read `!lst|meta|<key>` at `readTS`.
2. Calculate new `Head`/`Len` and generate new metadata + item keys.
3. Commit as a single transaction via `dispatchElems()`.

In this flow, **all writers Put to the same `!lst|meta|<key>`**. Due to write-write conflict detection in `ApplyMutations()` (`latestVer.TS > startTS`), concurrent `RPUSH` operations have a high probability of returning a `WriteConflictError`.

### Impact

- Large number of retries in high-concurrency `RPUSH` workloads.
- Every retry requires re-fetching `readTS`, wasting network RTT and Raft round-trips.
- Particularly noticeable in producer-consumer patterns where multiple producers push to the same list.

## Design

Using a Delta pattern, writers avoid touching the base metadata and instead write to individual Delta keys, completely avoiding write conflicts.

### 1. Key Layout

```
Base Metadata (Existing):
  !lst|meta|<userKey>                     → [Head(8)][Tail(8)][Len(8)]

Delta Key (New):
  !lst|meta|d|<userKey><commitTS(8)><seqInTxn(4)>  → DeltaEntry binary
```

- `commitTS` is an 8-byte big-endian timestamp determined at Raft apply time.
- `seqInTxn` is a 4-byte big-endian sequence number within the same transaction (needed if `LPUSH` is called multiple times for the same key in one `MULTI/EXEC`).
- Since all Delta keys for a `userKey` share the prefix `!lst|meta|d|<userKey>`, they are physically contiguous in the LSM tree, allowing for fast Prefix Scans.

```go
const ListMetaDeltaPrefix = "!lst|meta|d|"

func ListMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
    buf := make([]byte, 0, len(ListMetaDeltaPrefix)+len(userKey)+8+4)
    buf = append(buf, ListMetaDeltaPrefix...)
    buf = append(buf, userKey...)
    var ts [8]byte
    binary.BigEndian.PutUint64(ts[:], commitTS)
    buf = append(buf, ts[:]...)
    var seq [4]byte
    binary.BigEndian.PutUint32(seq[:], seqInTxn)
    buf = append(buf, seq[:]...)
    return buf
}
```

### 2. Delta Entry Format

```go
type ListMetaDelta struct {
    HeadDelta int64 // Change in Head (LPUSH: negative, LPOP: positive)
    LenDelta  int64 // Change in Len  (PUSH: positive, POP: negative)
}
```

Fixed 16-byte binary (2 x int64 big-endian).

- `RPUSH` n items: `HeadDelta=0, LenDelta=+n`
- `LPUSH` n items: `HeadDelta=-n, LenDelta=+n`
- `RPOP`: `HeadDelta=0, LenDelta=-1`
- `LPOP`: `HeadDelta=+1, LenDelta=-1`

`Tail` is always calculated as `Head + Len` and is not included in the Delta.

### 3. Write Path (Conflict-Free)

#### For RPUSH

```
Old Flow:
  1. Read !lst|meta|<key>   ← Registered in readSet → Source of conflict
  2. Put  !lst|meta|<key>   ← All writers write to the same key

New Flow:
  1. Read !lst|meta|<key>   ← Necessary (for seq calculation), but NOT registered in readSet
  2. Scan !lst|meta|d|<key> ← Read unapplied deltas to recalculate head/len
  3. Put  !lst|itm|<key><seq> ...   ← Item write (unique key)
  4. Put  !lst|meta|d|<key><commitTS><seqInTxn>  ← Delta write (unique key)
  ※ !lst|meta|<key> is never written to → No write conflict
```

**Important**: Delta keys are globally unique due to `commitTS + seqInTxn`, so concurrent writers do not collide, and write-write conflicts are avoided.

#### Item Key Sequence Calculation

In the Delta pattern, the base metadata's `Head`/`Len` alone is insufficient to determine the correct `Tail`. It is necessary to aggregate unapplied Deltas to calculate the effective `Head`/`Len`:

```go
func (r *RedisServer) resolveListMeta(ctx context.Context, userKey []byte, readTS uint64) (ListMeta, bool, error) {
    // 1. Read base metadata
    baseMeta, exists, err := r.loadListMetaAt(ctx, userKey, readTS)

    // 2. Fetch Deltas via prefix scan
    prefix := ListMetaDeltaScanPrefix(userKey)
    deltas, err := r.store.ScanAt(ctx, prefix, prefixEnd(prefix), maxDeltaScanLimit, readTS)

    // 3. Aggregate
    for _, d := range deltas {
        delta := UnmarshalListMetaDelta(d.Value)
        baseMeta.Head += delta.HeadDelta
        baseMeta.Len  += delta.LenDelta
    }
    baseMeta.Tail = baseMeta.Head + baseMeta.Len

    return baseMeta, exists || len(deltas) > 0, nil
}
```

### 4. Read Path (Read-Time Aggregation)

During reads (`LRANGE`, `LLEN`, `LINDEX`, etc.), `resolveListMeta()` is called to aggregate the base metadata and all unapplied Deltas.

```
LLEN key:
  1. resolveListMeta(key, readTS) → Effective ListMeta
  2. return meta.Len

LRANGE key start stop:
  1. resolveListMeta(key, readTS) → Effective ListMeta
  2. fetchListRange(key, meta, start, stop, readTS)
```

When the number of Deltas is small (< 100), the cost of a Prefix Scan is negligible. Since Delta keys are physically contiguous in the LSM tree, I/O can be performed in a single sequential read.

### 5. Background Compaction

To prevent read latency degradation, a background worker periodically collapses Deltas into the base metadata.

#### Compaction Flow

1. Read `!lst|meta|<key>` (baseMeta).
2. Scan `!lst|meta|d|<key>*` (deltas).
3. Aggregate: `mergedMeta = baseMeta + Σ(deltas)`.
4. In a single transaction:
   - Put `!lst|meta|<key>` (mergedMeta).
   - Delete all applied Delta keys.

#### Compaction Trigger

Add a `ListDeltaCompactor` phase to the existing `FSMCompactor`.

```go
type ListDeltaCompactor struct {
    store          store.ScanStore
    coordinator    *kv.Coordinate
    logger         *slog.Logger
    maxDeltaCount  int           // Compaction threshold (default: 64)
    scanInterval   time.Duration // Scan interval (default: 30 seconds)
}
```

- Scan the entire `!lst|meta|d|` prefix every `scanInterval`.
- If the number of Deltas for a `userKey` exceeds `maxDeltaCount`, mark it for compaction.
- Compaction is performed as a transaction (`IsTxn: true`), protecting the base metadata read via the `readSet` (using OCC to prevent concurrent compaction conflicts).

#### Compaction Safety

- The compaction transaction includes `!lst|meta|<key>` in its `readSet`. If two compactions run simultaneously, one will fail with a write conflict and retry with the latest base metadata, ensuring idempotency.
- Before deleting Deltas, the worker ensures their `commitTS` is older than `ActiveTimestampTracker.Oldest()` to avoid breaking in-flight reads.
- Deltas within the MVCC retention window are not deleted to guarantee consistency for historical reads.

### 6. POP Operations — Claim Mechanism

`POP` operations (`LPOP` / `RPOP`) involve both metadata updates and item deletions. If multiple clients attempt to `POP` simultaneously, they will compete for the same item. We introduce **Claim keys for CAS-based mutual exclusion** to resolve this.

#### 6.1. Claim Key Layout

```
Claim Key:
  !lst|claim|<userKey><seq(8-byte sortable)>  → claimValue binary
```

A Claim key shares the same `seq` suffix as the item key (`!lst|itm|`). The existence of a Claim key for an item means it has been popped (reserved).

```go
const ListClaimPrefix = "!lst|claim|"

func ListClaimKey(userKey []byte, seq int64) []byte {
    var raw [8]byte
    encodeSortableInt64(raw[:], seq)
    buf := make([]byte, 0, len(ListClaimPrefix)+len(userKey)+8)
    buf = append(buf, ListClaimPrefix...)
    buf = append(buf, userKey...)
    buf = append(buf, raw[:]...)
    return buf
}
```

#### 6.2. POP Claim Flow (LPOP example)

```
For LPOP:
  1. resolveListMeta(key, readTS) → Effective meta (Determine Head, Len)
  2. candidateSeq = meta.Head
  3. Loop:
     a. Check for Claim key at !lst|claim|<key><candidateSeq>
     b. If exists: candidateSeq++ and retry (Already claimed by another POP)
     c. If not exists:
        - Get item value from !lst|itm|<key><candidateSeq>
        - Put !lst|claim|<key><candidateSeq> → {claimerTS} (Write Claim)
        - Put !lst|meta|d|<key><commitTS><seq> → {HeadDelta: +1, LenDelta: -1}
        - Commit via dispatchElems()
  4. If commit successful: return item value
     If commit fails (WriteConflictError on claim key): retry from step 3
```

#### 6.3. Claim and OCC Interaction

Writing to a Claim key is protected by standard OCC:
- If two `POP` operations attempt to `Put` to the same Claim key sequence simultaneously, the later one will receive a `WriteConflictError` in `ApplyMutations()`.
- The failing side will skip the claimed sequence and try the next one upon retry.
- Since base metadata (`!lst|meta|<key>`) is not touched, there is no conflict with `PUSH` operations.

#### 6.4. Claim Key GC

A Claim key acts as a "logical deletion" marker. They are removed during Background Compaction:

```
1. Determine the base meta Head for the target userKey.
2. Claim keys with a sequence less than Head are no longer needed (Head has already passed them).
3. Within the compaction transaction:
   - Advance the base meta Head by the number of claimed items.
   - Delete corresponding Claim and Item keys.
   - Collapse corresponding Deltas.
```

Accumulated Claim keys do not affect read performance (they use the `!lst|claim|` prefix and are outside the metadata scan scope). GC is handled by the Background Compactor.

#### 6.5. RPOPLPUSH / LMOVE

`RPOPLPUSH src dst` is decomposed as:
1. Execute the `RPOP` claim flow on `src` → get value.
2. Execute the `LPUSH` delta flow on `dst` → insert value.
3. Commit both operations in a single transaction.

If `src` and `dst` are the same key, a single transaction generates both a Claim and a Delta, maintaining internal consistency.

### 7. Integration with MULTI/EXEC Transactions

Existing transaction processing using `listTxnState` within `txnContext` will be adapted for the Delta pattern:

```go
type listTxnState struct {
    meta       store.ListMeta      // Result of resolveListMeta() (Aggregated base + Deltas)
    metaExists bool
    appends    [][]byte
    deleted    bool
    purge      bool
    purgeMeta  store.ListMeta
    // New: Deltas generated within this transaction
    deltas     []store.ListMetaDelta 
}
```

- In `buildListElems()`, replace metadata `Put` with Delta `Put`.
- In `validateReadSet()`, exclude `!lst|meta|<key>` from the `readSet`, and instead only validate item key conflicts.
- Increment `seqInTxn` if pushing to the same list multiple times within one transaction.

### 8. New Key Helper Functions

```go
func IsListMetaDeltaKey(key []byte) bool {
    return bytes.HasPrefix(key, []byte(ListMetaDeltaPrefix))
}

func IsListClaimKey(key []byte) bool {
    return bytes.HasPrefix(key, []byte(ListClaimPrefix))
}

func ExtractListUserKeyFromDelta(key []byte) []byte {
    trimmed := bytes.TrimPrefix(key, []byte(ListMetaDeltaPrefix))
    if len(trimmed) < 12 { // 8(commitTS) + 4(seqInTxn)
        return nil
    }
    return trimmed[:len(trimmed)-12]
}

func ExtractListUserKeyFromClaim(key []byte) []byte {
    trimmed := bytes.TrimPrefix(key, []byte(ListClaimPrefix))
    if len(trimmed) < 8 { // 8(seq)
        return nil
    }
    return trimmed[:len(trimmed)-8]
}
```

### 9. Transition Plan

#### Phase 1: Add Delta Infrastructure

- Add `ListMetaDelta` struct and encode/decode functions to `store/list_helpers.go`.
- Add helpers like `ListMetaDeltaKey()`, `IsListMetaDeltaKey()`, etc.
- Add Claim helpers like `ListClaimKey()`, `IsListClaimKey()`, etc.
- Implement `resolveListMeta()` (aggregate base + Deltas).
- Verify marshal/unmarshal and aggregation logic via unit tests.

#### Phase 2: Switch Write Path

- Change `buildRPushOps()` / `buildLPushOps()` to write Deltas.
- Exclude `!lst|meta|<key>` from the `readSet` in `listRPush()` / `listLPush()`.
- Update `POP` commands to use the Claim mechanism + Delta pattern.
  - Adapt `luaScriptContext.popList()` / `popLazyListLeft()` / `popLazyListRight()` for the Claim flow.
  - Update `cmdRPopLPush` to a composite transaction of Claim (src) + Delta (dst).
- Update `txnContext.buildListElems()` for Delta support.

#### Phase 3: Switch Read Path

- Replace calls to `loadListMetaAt()` with `resolveListMeta()`.
- Update all read commands: `LRANGE`, `LLEN`, `LINDEX`, `LPOS`, etc.
- Skip claimed items: check for Claim keys in `fetchListRange()` and exclude claimed sequences from results.

#### Phase 4: Background Compaction

- Implement `ListDeltaCompactor`.
  - Fold Deltas (aggregate into base metadata + delete Deltas).
  - GC Claim keys (delete Claims + Items with sequence < base Head).
  - Detect empty lists and perform full deletion (base + all Deltas + all Claims + all Items).
- Integrate into the `FSMCompactor` run loop.
- Make compaction thresholds and intervals configurable.

#### Phase 5: Backward Compatibility and Benchmarks

- Ensure all existing Redis compatibility tests (`redis_test.go`, `redis_txn_test.go`) pass.
- Add concurrent `POP` tests (verify correctness of the Claim mechanism).
- Measure write conflict rates (compare before/after Delta introduction).
- Benchmark `LLEN` / `LRANGE` latency across different Delta accumulation levels.

### 10. Trade-offs

| Aspect | Current (Read-Modify-Write) | Delta + Claim Pattern |
|------|--------------------------|------------|
| PUSH write conflict | Increases with O(concurrent writers) | No metadata conflict |
| POP write conflict | Increases with O(concurrent poppers) | Only same-sequence conflicts (Claim-based) |
| Write Latency | 1 RTT (with retries) | 1 RTT (no retries, POP retries only on Claim collision) |
| Read Latency | O(1) | O(Number of Deltas) *Controlled by compaction* |
| Storage Usage | Metadata 24 bytes | Metadata 24 bytes + Delta 16 bytes × N + Claim × M |
| Implementation Complexity | Low | Medium (Add compaction worker + Claim GC) |
| Compaction Failure | N/A | Read latency increases, but no data inconsistency |

### 11. Design Decisions

The following points have been finalized.

#### 11.1. Limits on Delta Accumulation

**Decision: No synchronous compaction on the write side.**

Performing synchronous compaction during a write could cause write conflicts on the base metadata for the compaction transaction itself, introducing retries to what should be a conflict-free `PUSH` path. Read latency degradation due to Delta accumulation will be managed by tuning `scanInterval` and `maxDeltaCount` for Background Compaction.

If Delta accumulation becomes exceptionally high, a warning log will be emitted on the read side to allow operators to adjust compaction parameters.

#### 11.2. POP Conflict Avoidance

**Decision: Introduce a Claim mechanism (CAS-based).** (See Section 6)

Mutual exclusion for `POP` target items will be managed using Claim keys (`!lst|claim|<key><seq>`). Concurrent `POP` operations for the same sequence will result in one failing via OCC write-write conflict, with the failing side retrying by claiming the next sequence.

#### 11.3. Empty List Detection

**Decision: Defer to the next Background Compaction.**

Immediate deletion of base metadata or Deltas will not occur even if `Len=0` after aggregating Deltas.
Reasoning:
- Immediate deletion would require writing to the base metadata, risking inconsistency with concurrent `PUSH` Delta writes.
- When Background Compaction detects `Len=0`, it will atomically delete the base metadata, all Deltas, and any remaining Claim keys.
- During the brief window between compactions where an empty list persists, `resolveListMeta()` will return `Len=0`, ensuring `LLEN` / `LRANGE` correctly report an empty list.
