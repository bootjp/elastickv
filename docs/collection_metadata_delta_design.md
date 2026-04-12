# Collection Metadata Delta Design

## Objective

To resolve write conflicts caused by Read-Modify-Write (RMW) on collection metadata during concurrent operations on the same key, and to maintain conflict-free throughput even under high-concurrency workloads. This design covers all Redis collection types:

- **List**: Delta + Claim pattern for `RPUSH`/`LPUSH`/`LPOP`/`RPOP`
- **Hash**: Wide-column decomposition + Delta pattern for `HSET`/`HDEL`
- **Set**: Wide-column decomposition + Delta pattern for `SADD`/`SREM`
- **ZSet**: Delta pattern on top of existing wide-column format (PR #483) for `ZADD`/`ZREM`

## Problem

### Current Structure

All collection types suffer from the same fundamental issue: **multiple writers Put to the same metadata key**, causing OCC write conflicts.

#### List

```
Key:   !lst|meta|<userKey>
Value: [Head(8)][Tail(8)][Len(8)]   ← Fixed 24 bytes
```

`ListMeta` stores `Head`, `Tail`, and `Len`. Every `RPUSH` or `LPUSH` follows this flow:

1. Read `!lst|meta|<key>` at `readTS`.
2. Calculate new `Head`/`Len` and generate new metadata + item keys.
3. Commit as a single transaction via `dispatchElems()`.

In this flow, **all writers Put to the same `!lst|meta|<key>`**. Due to write-write conflict detection in `ApplyMutations()` (`latestVer.TS > startTS`), concurrent `RPUSH` operations have a high probability of returning a `WriteConflictError`.

#### Hash and Set (Monolithic Blob)

```
Hash: !redis|hash|<userKey> → JSON/Protobuf blob of entire field-value map
Set:  !redis|set|<userKey>  → Protobuf blob of sorted member array
```

Every mutation (`HSET`, `HDEL`, `SADD`, `SREM`) follows the same RMW pattern: read the entire blob, modify in memory, serialize and write back. All concurrent writers conflict on this single key.

#### ZSet (Wide-Column, PR #483)

```
Meta:   !zs|meta|<userKeyLen(4)><userKey>                          → [Len(8)]
Member: !zs|mem|<userKeyLen(4)><userKey><member>                   → [Score(8)]
Score:  !zs|scr|<userKeyLen(4)><userKey><sortableScore(8)><member> → (empty)
```

PR #483 decomposes ZSet into per-member keys, eliminating data-level conflicts. However, the metadata key (`!zs|meta|`) is still written by every `ZADD`/`ZREM` that changes cardinality, causing write conflicts on the Len field.

### Impact

- Large number of retries in high-concurrency workloads.
- Every retry requires re-fetching `readTS`, wasting network RTT and Raft round-trips.
- Particularly noticeable in producer-consumer patterns where multiple producers push to the same list.
- For Hash/Set, the monolithic blob additionally wastes bandwidth serializing/deserializing the entire collection on every mutation.

## Design

Using a Delta pattern, writers avoid touching the base metadata and instead write to individual Delta keys, completely avoiding write conflicts. For Hash and Set, a prerequisite wide-column decomposition is also introduced to break the monolithic blob into per-field/member keys.

---

## Part I: List (Delta + Claim)

### 1. Key Layout

```
Base Metadata (Existing):
  !lst|meta|<userKeyLen(4)><userKey>                          → [Head(8)][Tail(8)][Len(8)]

Delta Key (New):
  !lst|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)>  → DeltaEntry binary
```

- `userKeyLen` is a 4-byte big-endian length prefix that unambiguously separates `userKey` from the fixed-length suffix, even when `userKey` contains arbitrary bytes (including null bytes). This is the same approach used by Hash, Set, and ZSet key layouts, ensuring consistency across all collection types and full binary safety.
- `commitTS` is an 8-byte big-endian timestamp pinned by the coordinator before the Delta key is generated (via `kv.OperationGroup.CommitTS` during dispatch), then carried through Raft and used unchanged at apply time.
- `seqInTxn` is a 4-byte big-endian sequence number within the same transaction (needed if `LPUSH` is called multiple times for the same key in one `MULTI/EXEC`).
- Since all Delta keys for a `userKey` share the prefix `!lst|meta|d|<userKeyLen><userKey>`, they are physically contiguous in the LSM tree, allowing for fast Prefix Scans.

Because the Delta key embeds `commitTS`, the write path must know the final timestamp before emitting the key bytes. This design therefore assumes `CommitTS` is explicitly allocated once during dispatch and reused during Raft apply; it does not rely on the FSM rewriting Delta keys at apply time.

**HLC Leader Initialization Invariant**: When a new leader is elected, it must initialize its HLC to a value strictly greater than the maximum `commitTS` observed in its Raft log (the applied index's timestamp). This is already guaranteed by the existing `HLC.Update(observedTS)` call on each FSM `Apply`, which advances the local clock past any previously committed timestamp. Without this invariant, a new leader with a lagging wall clock could issue duplicate `commitTS` values, causing Delta key collisions in the LSM tree.

```go
const ListMetaDeltaPrefix = "!lst|meta|d|"

func ListMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
    buf := make([]byte, 0, len(ListMetaDeltaPrefix)+4+len(userKey)+8+4)
    buf = append(buf, ListMetaDeltaPrefix...)
    var kl [4]byte
    binary.BigEndian.PutUint32(kl[:], uint32(len(userKey)))
    buf = append(buf, kl[:]...)
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
  3. For each target sequence, check for stale Claim keys:
     - Scan !lst|claim|<keyLen><key>[seq …] for sequences being written
     - If a stale Claim key exists (left over from a prior POP on a recycled sequence),
       emit a Del for that Claim key in the same transaction
     ※ Without this step, a subsequent POP would see the stale claim and incorrectly
       skip the item, leading to orphaned items and list corruption.
  4. Put  !lst|itm|<key><seq> ...   ← Item write (unique key)
  5. Put  !lst|meta|d|<key><commitTS><seqInTxn>  ← Delta write (unique key)
  ※ !lst|meta|<key> is never written to → No write conflict
```

**Important**: Delta keys are globally unique due to `commitTS + seqInTxn`, so concurrent writers do not collide, and write-write conflicts are avoided.

**Stale Claim cleanup**: Sequences may be recycled after a POP followed by compaction that resets Head/Tail. If a Claim key from a prior POP still exists for the same sequence number, the PUSH must delete it to prevent future POPs from treating the newly pushed item as already popped.

#### Item Key Sequence Calculation

In the Delta pattern, the base metadata's `Head`/`Len` alone is insufficient to determine the correct `Tail`. It is necessary to aggregate unapplied Deltas to calculate the effective `Head`/`Len`:

```go
// Note: simplified pseudocode illustrating aggregation logic; error handling shown for clarity.
func (r *RedisServer) resolveListMeta(ctx context.Context, userKey []byte, readTS uint64) (ListMeta, bool, error) {
    // 1. Read base metadata
    baseMeta, exists, err := r.loadListMetaAt(ctx, userKey, readTS)
    if err != nil {
        return ListMeta{}, false, err
    }

    // 2. Fetch Deltas via prefix scan
    prefix := ListMetaDeltaScanPrefix(userKey)
    deltas, err := r.store.ScanAt(ctx, prefix, prefixScanEnd(prefix), maxDeltaScanLimit, readTS)
    if err != nil {
        return ListMeta{}, false, err
    }
    if len(deltas) == maxDeltaScanLimit {
        return ListMeta{}, false, ErrDeltaScanTruncated
    }

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

**`maxDeltaScanLimit` overflow**: If the number of unapplied Deltas exceeds `maxDeltaScanLimit`, `resolveListMeta` cannot aggregate them all in a single scan pass, which would produce an incorrect `ListMeta`. To preserve correctness, `resolveListMeta` must return an error when the scan result is truncated (i.e., when `len(deltas) == maxDeltaScanLimit`). The caller should then either surface the error or trigger an immediate synchronous compaction before retrying. This behaviour is the enforcement backstop for the hard-limit policy described in Section 11.1.

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

- Scan the entire `!lst|meta|d|` prefix every `scanInterval`, using a **cursor-based incremental scan** to avoid a single blocking pass over all Deltas. On each tick the compactor advances its cursor by at most `maxKeysPerTick` entries, wrapping around when it reaches the end. This keeps per-tick I/O bounded regardless of total Delta volume.
- Per-list Delta counters (maintained in memory or as a lightweight side-structure) can be used to prioritise lists that have accumulated many Deltas, so the compactor focuses effort where it matters rather than uniformly sampling every list every interval.
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
  !lst|claim|<userKeyLen(4)><userKey><seq(8-byte sortable)>  → claimValue binary
```

A Claim key shares the same `seq` suffix as the item key (`!lst|itm|`). The existence of a Claim key for an item means it has been popped (reserved). Like Delta keys, Claim keys use a `userKeyLen(4)` length prefix for binary safety and consistency with other collection types.

```go
const ListClaimPrefix = "!lst|claim|"

func ListClaimKey(userKey []byte, seq int64) []byte {
    var raw [8]byte
    encodeSortableInt64(raw[:], seq)
    buf := make([]byte, 0, len(ListClaimPrefix)+4+len(userKey)+8)
    buf = append(buf, ListClaimPrefix...)
    var kl [4]byte
    binary.BigEndian.PutUint32(kl[:], uint32(len(userKey)))
    buf = append(buf, kl[:]...)
    buf = append(buf, userKey...)
    buf = append(buf, raw[:]...)
    return buf
}
```

#### 6.2. POP Claim Flow

##### LPOP

```
For LPOP:
  1. resolveListMeta(key, readTS) → Effective meta (Determine Head, Len)
  2. candidateSeq = meta.Head
  3. Bulk-scan existing Claim keys in range [candidateSeq, candidateSeq+scanWindow):
     - scanWindow is a configurable constant (default: 128) that determines how many
       candidate sequences are checked in one batch.
     - prefix scan !lst|claim|<keyLen><key>[candidateSeq … candidateSeq+scanWindow)
     - collect the set of already-claimed sequences into a local skip-set
  4. Pick the first sequence in [candidateSeq, candidateSeq+scanWindow) not in skip-set
  5. If a candidate is found:
        - Get item value from !lst|itm|<key><candidateSeq>
        - Put !lst|claim|<keyLen><key><candidateSeq> → {claimerTS} (Write Claim)
        - Put !lst|meta|d|<keyLen><key><commitTS><seqInTxn(4)> → {HeadDelta: +1, LenDelta: -1}
        - Commit via dispatchElems()
     If no candidate found in window: advance window and repeat from step 3
  6. If commit successful: return item value
     If commit fails (WriteConflictError on claim key): refresh skip-set and retry from step 3
```

##### RPOP

Unlike LPOP which searches forward from `Head`, RPOP searches **backward** from `Tail - 1`:

```
For RPOP:
  1. resolveListMeta(key, readTS) → Effective meta (Determine Head, Tail, Len)
  2. candidateSeq = meta.Tail - 1
  3. Bulk-scan existing Claim keys in range (candidateSeq-scanWindow, candidateSeq]:
     - Reverse range scan of !lst|claim|<keyLen><key> within the window
     - collect the set of already-claimed sequences into a local skip-set
  4. Pick the last (highest) sequence in (candidateSeq-scanWindow, candidateSeq]
     not in skip-set
  5. If a candidate is found:
        - Get item value from !lst|itm|<key><candidateSeq>
        - Put !lst|claim|<keyLen><key><candidateSeq> → {claimerTS} (Write Claim)
        - Put !lst|meta|d|<keyLen><key><commitTS><seqInTxn(4)> → {HeadDelta: 0, LenDelta: -1}
        - Commit via dispatchElems()
     If no candidate found in window: retreat window and repeat from step 3
  6. If commit successful: return item value
     If commit fails (WriteConflictError on claim key): refresh skip-set and retry from step 3
```

**Key differences from LPOP**:
- RPOP scans backward from `Tail - 1` instead of forward from `Head`.
- RPOP emits `HeadDelta: 0` (Head does not change), whereas LPOP emits `HeadDelta: +1`.
- Both emit `LenDelta: -1`.
- The compactor advances `Tail` for RPOP claims (see Section 6.4) and `Head` for LPOP claims.

This replaces the previous O(N) point-lookup loop with a single range scan per window, reducing latency when many uncompacted Claim keys have accumulated.

#### 6.3. Claim and OCC Interaction

Writing to a Claim key is protected by standard OCC:
- If two `POP` operations attempt to `Put` to the same Claim key sequence simultaneously, the later one will receive a `WriteConflictError` in `ApplyMutations()`.
- The failing side will skip the claimed sequence and try the next one upon retry.
- Since base metadata (`!lst|meta|<key>`) is not touched, there is no conflict with `PUSH` operations.

#### 6.4. Claim Key GC

A Claim key acts as a "logical deletion" marker. They are removed during Background Compaction.

##### Head-side GC (LPOP claims)

```
1. Determine the base meta Head for the target userKey.
2. Scan Claim keys starting from the current Head sequence, forward.
3. Advance Head only through *contiguous* claimed sequences starting from the current Head.
   - If sequences Head, Head+1, Head+2 are all claimed but Head+3 is NOT claimed,
     advance Head to Head+3 and stop. Do NOT skip over the gap.
   - Advancing past an unclaimed sequence (a gap) would logically delete items that have
     not been popped, causing data loss.
4. Within the compaction transaction (bounded to at most `maxKeysPerCompactionTx` deletions
   to avoid Raft proposal timeouts or LSM performance issues; suggested default: 256,
   chosen to keep proposal sizes well under the typical 1 MiB Raft entry limit):
   - Advance the base meta Head by the number of contiguously claimed items.
   - Delete corresponding Claim and Item keys for the contiguous range only.
   - Collapse corresponding Deltas.
5. If more contiguous keys remain after the bound is reached, schedule another compaction
   pass for this userKey on the next compactor tick.
```

##### Tail-side GC (RPOP claims)

RPOP claims create Claim keys at the tail end of the list. Without tail-side GC, RPOP-heavy workloads would leak Claim keys and Item keys indefinitely.

```
1. Determine the effective Tail (= base Head + base Len, after delta aggregation).
2. Scan Claim keys starting from Tail - 1, backward.
3. Retreat Tail only through *contiguous* claimed sequences ending at the current Tail - 1.
   - If sequences Tail-1, Tail-2, Tail-3 are all claimed but Tail-4 is NOT claimed,
     retreat Tail to Tail-3 and stop. Do NOT skip over the gap.
4. Within the compaction transaction:
   - Reduce the base Len by the number of contiguously claimed tail items.
   - Delete corresponding Claim and Item keys for the contiguous range only.
   - Collapse corresponding Deltas.
5. If more contiguous keys remain after the bound is reached, schedule another compaction
   pass for this userKey on the next compactor tick.
```

**Note**: Head-side and Tail-side GC may run in the same compaction pass for a given key. The combined advancement must be reflected in a single transaction to avoid inconsistency.

##### Read-time strategy for Claim keys

- Claim keys are outside the `!lst|meta|` namespace, so they do not affect the metadata-only read path (`resolveListMeta()`).
- However, `fetchListRange()` must skip logically deleted items. To do that, it performs a **bulk range scan of Claim keys** for the candidate sequence interval being materialized, then filters claimed sequences in memory while assembling the result.
- This means Claim keys introduce bounded read amplification for list reads: **one additional range scan per fetched window**, not one extra point lookup per item.
- Background Compaction keeps this bounded by deleting Claim keys whose sequence is below the effective Head or at/above the effective Tail, and by collapsing old Deltas.

In summary: accumulated Claim keys do not affect metadata-only scans, but they do add a single range scan to `fetchListRange()` until compaction removes obsolete claims.

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

**Consistency note for metadata reads in MULTI/EXEC**: Commands that read collection cardinality within a transaction (e.g., `LLEN`, `HLEN`) observe the snapshot at `startTS` plus locally accumulated deltas within the transaction. Because the base metadata key is excluded from the `readSet`, concurrent writers may commit deltas between the transaction's `startTS` and its commit. This provides snapshot isolation (not strict serializability) for cardinality reads, which matches Redis's existing `MULTI/EXEC` semantics where commands see the state at execution time, not a globally serialized point. If strict serializability is required for a specific use case, the caller can opt in by explicitly registering the base metadata key in the `readSet`, accepting the higher conflict rate.

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
    if len(trimmed) < 4+8+4 { // 4(userKeyLen) + 8(commitTS) + 4(seqInTxn)
        return nil
    }
    ukLen := binary.BigEndian.Uint32(trimmed[:4])
    if uint32(len(trimmed)) < 4+ukLen+8+4 {
        return nil
    }
    return trimmed[4 : 4+ukLen]
}

func ExtractListUserKeyFromClaim(key []byte) []byte {
    trimmed := bytes.TrimPrefix(key, []byte(ListClaimPrefix))
    if len(trimmed) < 4+8 { // 4(userKeyLen) + 8(seq)
        return nil
    }
    ukLen := binary.BigEndian.Uint32(trimmed[:4])
    if uint32(len(trimmed)) < 4+ukLen+8 {
        return nil
    }
    return trimmed[4 : 4+ukLen]
}
```

---

## Part II: Hash (Wide-Column + Delta)

Hash currently stores the entire field-value map as a single blob (`!redis|hash|<userKey>`). This section introduces wide-column decomposition (per-field keys) and the Delta pattern for metadata.

### 9. Hash Key Layout

```
Base Metadata (New):
  !hs|meta|<userKeyLen(4)><userKey>                                → [Len(8)]

Field Key (New):
  !hs|fld|<userKeyLen(4)><userKey><fieldName>                      → field value bytes

Delta Key (New):
  !hs|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)> → [LenDelta(8)]
```

- `userKeyLen` is a 4-byte big-endian length prefix to prevent ambiguity when one `userKey` is a prefix of another (e.g., `"foo"` vs `"foobar"`). This follows the same convention as ZSet wide-column keys.
- `Len` is the number of fields in the hash (equivalent to `HLEN`).
- Each field has its own key, so concurrent `HSET` operations on **different fields** do not conflict on data keys.
- All collection types use the same `userKeyLen(4)` length-prefix approach to prevent prefix collisions and ensure binary safety.

```go
const (
    HashMetaPrefix      = "!hs|meta|"
    HashFieldPrefix     = "!hs|fld|"
    HashMetaDeltaPrefix = "!hs|meta|d|"
)

func HashMetaKey(userKey []byte) []byte {
    buf := make([]byte, 0, len(HashMetaPrefix)+4+len(userKey))
    buf = append(buf, HashMetaPrefix...)
    var kl [4]byte
    binary.BigEndian.PutUint32(kl[:], uint32(len(userKey)))
    buf = append(buf, kl[:]...)
    buf = append(buf, userKey...)
    return buf
}

func HashFieldKey(userKey, fieldName []byte) []byte {
    buf := make([]byte, 0, len(HashFieldPrefix)+4+len(userKey)+len(fieldName))
    buf = append(buf, HashFieldPrefix...)
    var kl [4]byte
    binary.BigEndian.PutUint32(kl[:], uint32(len(userKey)))
    buf = append(buf, kl[:]...)
    buf = append(buf, userKey...)
    buf = append(buf, fieldName...)
    return buf
}
```

### 10. Hash Delta Entry Format

```go
type HashMetaDelta struct {
    LenDelta int64 // Change in field count (HSET new field: +1, HDEL: -1)
}
```

Fixed 8-byte binary (int64 big-endian). Unlike List, Hash metadata only tracks `Len` (no Head/Tail).

### 11. Hash Write Path

#### HSET

```
1. Point-read !hs|fld|<key><field> to check if field already exists
   → This read IS registered in the readSet (for OCC on the field key)
2. Put !hs|fld|<key><field> → value
3. If field is new (did not exist in step 1):
     Put !hs|meta|d|<keyLen><key><commitTS><seqInTxn> → LenDelta: +1
   If field is an update (existed in step 1):
     No delta write needed (LenDelta would be 0)
※ !hs|meta|<key> is never read or written → No metadata conflict
```

**Concurrent HSET on different fields**: Both succeed with no conflict. Each writes to a different field key and appends independent delta entries.

**Concurrent HSET on the same field**: OCC detects the conflict on the field key. One succeeds, the other retries. On retry, the field exists, so no delta is written (just an update).

#### HDEL

```
1. Point-read !hs|fld|<key><field> to check existence
2. If field exists:
     Del !hs|fld|<key><field>
     Put !hs|meta|d|<keyLen><key><commitTS><seqInTxn> → LenDelta: -1
   If field does not exist:
     No-op
```

**Concurrent HDEL on the same field**: OCC conflict on the field key. One succeeds with delta(-1), the other retries and finds the field gone → no-op.

### 12. Hash Read Path

```
HLEN key:
  1. resolveHashMeta(key, readTS) → Effective Len
  2. return Len

HGET key field:
  1. Point-read !hs|fld|<key><field>  ← Direct, no delta involvement
  2. return value (or nil if not found)

HGETALL key:
  1. Prefix-scan !hs|fld|<key>*  ← Scan all field keys
  2. return all field-value pairs

HEXISTS key field:
  1. Point-read !hs|fld|<key><field>
  2. return 1 if found, 0 if not
```

Most read operations (`HGET`, `HGETALL`, `HEXISTS`) directly access field keys without delta involvement. Only `HLEN` requires delta aggregation via `resolveHashMeta()`.

```go
func (r *RedisServer) resolveHashMeta(ctx context.Context, userKey []byte, readTS uint64) (int64, bool, error) {
    baseMeta, exists, err := r.loadHashMetaAt(ctx, userKey, readTS)
    if err != nil {
        return 0, false, err
    }

    prefix := HashMetaDeltaScanPrefix(userKey)
    deltas, err := r.store.ScanAt(ctx, prefix, prefixScanEnd(prefix), maxDeltaScanLimit, readTS)
    if err != nil {
        return 0, false, err
    }
    if len(deltas) == maxDeltaScanLimit {
        return 0, false, ErrDeltaScanTruncated
    }

    length := baseMeta.Len
    for _, d := range deltas {
        length += UnmarshalHashMetaDelta(d.Value).LenDelta
    }
    return length, exists || len(deltas) > 0, nil
}
```

### 13. Hash Background Compaction

Hash delta compaction follows the same pattern as List (Section 5), but simpler:

1. Read `!hs|meta|<key>` (baseMeta).
2. Scan `!hs|meta|d|<key>*` (deltas).
3. Aggregate: `mergedLen = baseMeta.Len + Σ(deltas.LenDelta)`.
4. In a single transaction:
   - Put `!hs|meta|<key>` (mergedLen).
   - Delete all applied Delta keys.
5. If `mergedLen == 0`: update base metadata to `Len = 0` (do NOT delete), delete all deltas and all field keys (see Section 28).

No Claim mechanism is needed because `HDEL` targets named fields, not positional elements. OCC on the field key itself provides mutual exclusion.

### 14. Hash Migration from Legacy Format

```
1. On read: check !hs|meta|<key> first. If found, use wide-column path.
   If not found, fall back to legacy !redis|hash|<key> blob.
2. On write to legacy data: atomically migrate in a single transaction:
   - Scan legacy blob, create field keys for each field-value pair
   - Write !hs|meta|<key> with Len
   - Apply the triggering mutation (HSET/HDEL) to the new wide-column keys
   - Delete legacy !redis|hash|<key>
3. Subsequent reads/writes use wide-column path exclusively.
```

**Concurrent write behavior during migration**: Because the migration and the triggering write are committed as a single atomic transaction, concurrent writers targeting the same key will encounter an OCC write conflict on the legacy blob key (`!redis|hash|<key>`) and retry. On retry, the winner's migration will already be visible, so the retrier takes the wide-column path directly. Concurrent writes to **different** Hash keys are unaffected since they touch separate legacy blobs. Reads during migration use the fallback logic (step 1) and always see a consistent view: either the pre-migration blob or the post-migration wide-column data, never a partial state.

---

## Part III: Set (Wide-Column + Delta)

Set currently stores all members as a single protobuf blob (`!redis|set|<userKey>`). This section introduces wide-column decomposition (per-member keys) and the Delta pattern for metadata.

### 15. Set Key Layout

```
Base Metadata (New):
  !st|meta|<userKeyLen(4)><userKey>                                → [Len(8)]

Member Key (New):
  !st|mem|<userKeyLen(4)><userKey><member>                         → (empty value)

Delta Key (New):
  !st|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)> → [LenDelta(8)]
```

- Member keys store an empty value; the member name is embedded in the key itself.
- Each member has its own key, so concurrent `SADD` operations on **different members** do not conflict.

```go
const (
    SetMetaPrefix      = "!st|meta|"
    SetMemberPrefix    = "!st|mem|"
    SetMetaDeltaPrefix = "!st|meta|d|"
)

func SetMetaKey(userKey []byte) []byte {
    buf := make([]byte, 0, len(SetMetaPrefix)+4+len(userKey))
    buf = append(buf, SetMetaPrefix...)
    var kl [4]byte
    binary.BigEndian.PutUint32(kl[:], uint32(len(userKey)))
    buf = append(buf, kl[:]...)
    buf = append(buf, userKey...)
    return buf
}

func SetMemberKey(userKey, member []byte) []byte {
    buf := make([]byte, 0, len(SetMemberPrefix)+4+len(userKey)+len(member))
    buf = append(buf, SetMemberPrefix...)
    var kl [4]byte
    binary.BigEndian.PutUint32(kl[:], uint32(len(userKey)))
    buf = append(buf, kl[:]...)
    buf = append(buf, userKey...)
    buf = append(buf, member...)
    return buf
}
```

### 16. Set Delta Entry Format

```go
type SetMetaDelta struct {
    LenDelta int64 // Change in member count (SADD new: +1, SREM: -1)
}
```

Fixed 8-byte binary (int64 big-endian). Identical structure to Hash delta.

### 17. Set Write Path

#### SADD

```
1. Point-read !st|mem|<key><member> to check if member already exists
2. If member is new:
     Put !st|mem|<key><member> → (empty)
     Put !st|meta|d|<keyLen><key><commitTS><seqInTxn> → LenDelta: +1
   If member already exists:
     No-op (SADD is idempotent for existing members)
※ For SADD with multiple members, aggregate LenDelta within the transaction
  (e.g., 3 new members → single delta with LenDelta: +3)
```

#### SREM

```
1. Point-read !st|mem|<key><member> to check existence
2. If member exists:
     Del !st|mem|<key><member>
     Put !st|meta|d|<keyLen><key><commitTS><seqInTxn> → LenDelta: -1
   If member does not exist:
     No-op
※ For SREM with multiple members, aggregate LenDelta similarly
```

### 18. Set Read Path

```
SCARD key:
  1. resolveSetMeta(key, readTS) → Effective Len
  2. return Len

SISMEMBER key member:
  1. Point-read !st|mem|<key><member>  ← Direct, no delta involvement
  2. return 1 if found, 0 if not

SMEMBERS key:
  1. Prefix-scan !st|mem|<key>*  ← Scan all member keys
  2. return all members

SRANDMEMBER key [count]:
  1. Prefix-scan !st|mem|<key>* or sample via random offset
  2. return selected members
```

Only `SCARD` requires delta aggregation. Other read operations work directly on member keys.

### 19. Set Background Compaction

Identical pattern to Hash compaction (Section 13):

1. Read `!st|meta|<key>` → base Len.
2. Scan `!st|meta|d|<key>*` → deltas.
3. Aggregate: `mergedLen = baseMeta.Len + Σ(deltas.LenDelta)`.
4. Single transaction: Put merged meta + delete applied deltas.
5. If `mergedLen == 0`: update base metadata to `Len = 0` (do NOT delete), delete all deltas and all member keys (see Section 28).

No Claim mechanism is needed.

### 20. Set Migration from Legacy Format

```
1. On read: check !st|meta|<key> first. If found, use wide-column path.
   If not found, fall back to legacy !redis|set|<key> protobuf blob.
2. On write to legacy data: atomically migrate in a single transaction:
   - Deserialize legacy blob, create member keys for each member
   - Write !st|meta|<key> with Len
   - Apply the triggering mutation (SADD/SREM) to the new member keys
   - Delete legacy !redis|set|<key>
3. Subsequent reads/writes use wide-column path exclusively.
```

Concurrent write behavior follows the same pattern as Hash migration (Section 14): the migration transaction holds an OCC conflict on the legacy blob key, so concurrent writers retry and take the wide-column path.

---

## Part IV: ZSet (Delta on Wide-Column)

ZSet already uses wide-column format (PR #483) with per-member keys. This section adds the Delta pattern for the metadata key to eliminate cardinality-update conflicts.

### 21. ZSet Key Layout (Existing + Delta)

```
Base Metadata (Existing, PR #483):
  !zs|meta|<userKeyLen(4)><userKey>                                → [Len(8)]

Member Key (Existing):
  !zs|mem|<userKeyLen(4)><userKey><member>                         → [Score(8)] IEEE 754

Score Index Key (Existing):
  !zs|scr|<userKeyLen(4)><userKey><sortableScore(8)><member>       → (empty)

Delta Key (New):
  !zs|meta|d|<userKeyLen(4)><userKey><commitTS(8)><seqInTxn(4)> → [LenDelta(8)]
```

The only addition is the Delta key namespace `!zs|meta|d|`. Member and score index keys remain unchanged.

```go
const ZSetMetaDeltaPrefix = "!zs|meta|d|"

func ZSetMetaDeltaKey(userKey []byte, commitTS uint64, seqInTxn uint32) []byte {
    buf := make([]byte, 0, len(ZSetMetaDeltaPrefix)+4+len(userKey)+8+4)
    buf = append(buf, ZSetMetaDeltaPrefix...)
    var kl [4]byte
    binary.BigEndian.PutUint32(kl[:], uint32(len(userKey)))
    buf = append(buf, kl[:]...)
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

### 22. ZSet Delta Entry Format

```go
type ZSetMetaDelta struct {
    LenDelta int64 // Change in member count (ZADD new: +1, ZREM: -1)
}
```

Fixed 8-byte binary. Score updates that do not change cardinality produce no delta.

### 23. ZSet Write Path

#### ZADD

```
1. Point-read !zs|mem|<key><member> to check if member already exists
2. If member is new:
     Put !zs|mem|<key><member> → score (IEEE 754)
     Put !zs|scr|<key><sortableScore><member> → (empty)
     Put !zs|meta|d|<keyLen><key><commitTS><seqInTxn> → LenDelta: +1
   If member exists (score update only):
     Del old !zs|scr|<key><oldSortableScore><member>
     Put !zs|scr|<key><newSortableScore><member> → (empty)
     Put !zs|mem|<key><member> → newScore
     No delta write (cardinality unchanged)
※ !zs|meta|<key> is never read or written during ZADD → No metadata conflict
※ For ZADD with multiple members, aggregate LenDelta within the transaction
```

#### ZREM

```
1. Point-read !zs|mem|<key><member> to get current score
2. If member exists:
     Del !zs|mem|<key><member>
     Del !zs|scr|<key><sortableScore><member>
     Put !zs|meta|d|<keyLen><key><commitTS><seqInTxn> → LenDelta: -1
   If member does not exist:
     No-op
```

**Concurrent ZADD of different members**: Both succeed with no conflict. Each writes to different member/score keys and appends independent deltas.

**Concurrent ZADD of the same member**: OCC conflict on the member key. One succeeds, the other retries and sees the member exists (score update, no delta).

### 24. ZSet Read Path

```
ZCARD key:
  1. resolveZSetMeta(key, readTS) → Effective Len
  2. return Len

ZSCORE key member:
  1. Point-read !zs|mem|<key><member>  ← Direct, no delta involvement
  2. return score (or nil)

ZRANGEBYSCORE key min max:
  1. Range-scan !zs|scr|<key>[sortable(min)..sortable(max))
  2. return members with scores (already in score order from index)

ZRANK key member:
  1. Point-read !zs|mem|<key><member> → get score
  2. Count-scan !zs|scr|<key>[..sortable(score)] → rank
```

Only `ZCARD` requires delta aggregation. Score-based queries use the score index directly.

### 25. ZSet Background Compaction

Same pattern as Hash/Set compaction:

1. Read `!zs|meta|<key>` → base Len.
2. Scan `!zs|meta|d|<key>*` → deltas.
3. Aggregate: `mergedLen = baseMeta.Len + Σ(deltas.LenDelta)`.
4. Single transaction: Put merged meta + delete applied deltas.
5. If `mergedLen == 0`: update base metadata to `Len = 0` (do NOT delete), delete all deltas, all member keys, and all score index keys (see Section 28).

No Claim mechanism is needed. `ZREM` targets specific named members, and OCC on the member key provides mutual exclusion.

---

## Part V: Shared Infrastructure

### 26. Unified Compactor

The `DeltaCompactor` is generalized to handle all collection types. Each type registers a compaction handler:

```go
type DeltaCompactor struct {
    store          store.ScanStore
    coordinator    *kv.Coordinate
    logger         *slog.Logger
    maxDeltaCount  int           // Soft threshold (default: 64)
    scanInterval   time.Duration // Scan interval (default: 30 seconds)
}

type collectionCompactionHandler interface {
    DeltaPrefix() string                          // e.g., "!lst|meta|d|", "!hs|meta|d|"
    ExtractUserKey(deltaKey []byte) []byte         // Extract userKey from delta key
    Compact(ctx context.Context, userKey []byte) error  // Type-specific compaction logic
}
```

- List compaction includes Claim key GC (Head-side and Tail-side).
- Hash/Set/ZSet compaction only folds LenDelta into base metadata and optionally deletes empty collections.
- All types share the cursor-based incremental scan, per-key delta counters, and bounded transaction sizes (`maxKeysPerCompactionTx`).

### 27. Shared Delta Limits

The delta accumulation limits from List Section 11.1 apply uniformly to all collection types:

- **`maxDeltaCount`** (default: 64) — soft threshold for scheduling compaction.
- **`maxDeltaScanLimit`** (default: 256) — hard limit; `resolve*Meta()` returns an error when truncated, triggering synchronous compaction.

### 28. Empty Collection Detection

For all collection types, empty collection cleanup is deferred to Background Compaction (same reasoning as Design Decision D3):

- Immediate deletion would require writing to the base metadata, risking inconsistency with concurrent Delta writes.
- When the compactor detects `Len == 0`, it performs the following in a single transaction:
  1. **Update** the base metadata to `Len = 0` (and for List: `Head`/`Tail` reflecting the final state). The base metadata key is **not deleted** — it is retained as a tombstone to prevent concurrent writers from misinterpreting a missing metadata key as a fresh collection and restarting sequence numbering from zero.
  2. Delete all applied Delta keys.
  3. Delete all data keys (items for List, fields for Hash, members for Set, member+score keys for ZSet).
  4. For List: delete all Claim keys.
- A subsequent write to an empty collection (e.g., `RPUSH` on a list with `Len == 0`) will see the zeroed base metadata and correctly resume sequence numbering from the existing `Head`/`Tail`.
- During the brief window between compactions, `resolve*Meta()` returns `Len == 0`, ensuring cardinality queries correctly report an empty collection.
- **Full metadata deletion** may only occur as part of a `DEL` command on the key itself, which is handled outside the delta compaction path and uses the standard transactional `DEL` flow.

---

## Transition Plan

### List

#### Phase L1: Add Delta Infrastructure

- Add `ListMetaDelta` struct and encode/decode functions to `store/list_helpers.go`.
- Add helpers like `ListMetaDeltaKey()`, `IsListMetaDeltaKey()`, etc.
- Add Claim helpers like `ListClaimKey()`, `IsListClaimKey()`, etc.
- Implement `resolveListMeta()` (aggregate base + Deltas).
- Verify marshal/unmarshal and aggregation logic via unit tests.

#### Phase L2: Switch Write Path

- Change `buildRPushOps()` / `buildLPushOps()` to write Deltas.
- Exclude `!lst|meta|<key>` from the `readSet` in `listRPush()` / `listLPush()`.
- Add stale Claim key cleanup in PUSH operations (Section 3).
- Update `POP` commands to use the Claim mechanism + Delta pattern.
  - Adapt `luaScriptContext.popList()` / `popLazyListLeft()` / `popLazyListRight()` for the Claim flow.
  - Implement RPOP claim flow (reverse scan from Tail-1, Section 6.2).
  - Update `cmdRPopLPush` to a composite transaction of Claim (src) + Delta (dst).
- Update `txnContext.buildListElems()` for Delta support.

#### Phase L3: Switch Read Path

- Replace calls to `loadListMetaAt()` with `resolveListMeta()`.
- Update all read commands: `LRANGE`, `LLEN`, `LINDEX`, `LPOS`, etc.
- Skip claimed items: check for Claim keys in `fetchListRange()` and exclude claimed sequences from results.

#### Phase L4: Background Compaction

- Implement `ListDeltaCompactor`.
  - Fold Deltas (aggregate into base metadata + delete Deltas).
  - Head-side GC: advance Head through contiguous claimed sequences only (Section 6.4).
  - Tail-side GC: retreat Tail through contiguous claimed sequences only (Section 6.4).
  - Detect empty lists and perform full deletion (base + all Deltas + all Claims + all Items).
- Integrate into the unified `DeltaCompactor` (Section 26).
- Make compaction thresholds and intervals configurable.

#### Phase L5: Backward Compatibility and Benchmarks

- Ensure all existing Redis compatibility tests (`redis_test.go`, `redis_txn_test.go`) pass.
- Add concurrent `POP` tests (verify correctness of the Claim mechanism, both LPOP and RPOP).
- Measure write conflict rates (compare before/after Delta introduction).
- Benchmark `LLEN` / `LRANGE` latency across different Delta accumulation levels.

### Hash

#### Phase H1: Wide-Column Decomposition

- Add `HashMeta`, `HashMetaDelta` structs and marshal/unmarshal to `store/hash_helpers.go`.
- Add key helpers: `HashMetaKey()`, `HashFieldKey()`, `HashFieldScanPrefix()`, etc.
- Add type detection: `IsHashMetaKey()`, `IsHashFieldKey()`, `IsHashInternalKey()`, `ExtractHashUserKey()`.
- Implement migration-aware loader `loadHashMembersMap()` (check wide-column meta first, fall back to legacy `!redis|hash|` blob).
- Add `buildHashWriteElems()` (full write for migration) and `buildHashDiffElems()` (incremental update).

#### Phase H2: Switch Write Path

- Change `applyHashFieldPairs()` / `hdelTxn()` to use per-field key writes.
- On first write to legacy data, atomically migrate to wide-column format.
- Exclude `!hs|meta|<key>` from the `readSet`; use Delta writes for cardinality changes.
- Implement `resolveHashMeta()` for `HLEN`.
- Update `luaScriptContext` hash state management.

#### Phase H3: Switch Read Path

- `HGET`: point-read on field key (no delta involvement).
- `HGETALL`: prefix-scan field keys.
- `HLEN`: `resolveHashMeta()` with delta aggregation.
- `HEXISTS`: point-read on field key.

#### Phase H4: Background Compaction + Tests

- Add Hash compaction handler to the unified `DeltaCompactor`.
- Ensure all existing Hash tests pass.
- Add concurrent `HSET` tests for different fields.
- Benchmark `HLEN` latency across delta levels.

### Set

#### Phase S1: Wide-Column Decomposition

- Add `SetMeta`, `SetMetaDelta` structs and marshal/unmarshal to `store/set_helpers.go`.
- Add key helpers: `SetMetaKey()`, `SetMemberKey()`, `SetMemberScanPrefix()`, etc.
- Add type detection: `IsSetMetaKey()`, `IsSetMemberKey()`, `IsSetInternalKey()`, `ExtractSetUserKey()`.
- Implement migration-aware loader (check wide-column meta first, fall back to legacy `!redis|set|` blob).
- Add `buildSetWriteElems()` and `buildSetDiffElems()`.

#### Phase S2: Switch Write Path

- Change `mutateExactSet()` to use per-member key writes + Delta.
- On first write to legacy data, atomically migrate to wide-column format.
- Exclude `!st|meta|<key>` from the `readSet`.
- Implement `resolveSetMeta()` for `SCARD`.
- Update `luaScriptContext` set state management.

#### Phase S3: Switch Read Path

- `SISMEMBER`: point-read on member key.
- `SMEMBERS`: prefix-scan member keys.
- `SCARD`: `resolveSetMeta()` with delta aggregation.

#### Phase S4: Background Compaction + Tests

- Add Set compaction handler to the unified `DeltaCompactor`.
- Ensure all existing Set tests pass.
- Add concurrent `SADD` tests for different members.

### ZSet

#### Phase Z1: Add Delta Infrastructure

- ZSet already uses wide-column format (PR #483). No decomposition needed.
- Add `ZSetMetaDelta` struct and marshal/unmarshal to `store/zset_helpers.go`.
- Add `ZSetMetaDeltaKey()`, `IsZSetMetaDeltaKey()`, `ExtractZSetUserKeyFromDelta()`.
- Implement `resolveZSetMeta()` (aggregate base `!zs|meta|` + Deltas).

#### Phase Z2: Switch Write Path

- Change `persistZSetMembersTxn()` to exclude `!zs|meta|<key>` from the `readSet`.
- Replace direct metadata `Put` in `buildZSetWriteElems()` / `buildZSetDiffElems()` with Delta `Put`.
- Update `luaScriptContext` ZSet state management.

#### Phase Z3: Switch Read Path

- `ZCARD`: `resolveZSetMeta()` with delta aggregation.
- Other read operations (`ZSCORE`, `ZRANGEBYSCORE`, `ZRANK`) are unchanged (they use member/score keys directly).

#### Phase Z4: Background Compaction + Tests

- Add ZSet compaction handler to the unified `DeltaCompactor`.
- Ensure all existing ZSet tests pass (including migration tests from PR #483).
- Add concurrent `ZADD` tests for different members.

### Cross-Type

#### Phase X1: Unified Compactor

- Implement `DeltaCompactor` with `collectionCompactionHandler` interface (Section 26).
- Register handlers for List, Hash, Set, ZSet.
- Integrate cursor-based incremental scan, per-key delta counters, and bounded transactions.
- Integrate into `FSMCompactor` run loop.

#### Phase X2: Rolling Upgrade and Zero-Downtime Cutover

The Delta layout introduces new key namespaces (`!*|meta|d|`, `!lst|claim|`, `!hs|fld|`, `!st|mem|`) alongside existing namespaces. Old nodes that do not understand these keys will ignore them during reads, leading to stale cardinality values. To avoid service interruption, the following strategies are available:

**Option A — Feature flag (recommended for most deployments)**

- Introduce a cluster-wide feature flag (e.g. stored in Raft config or a well-known KV key) that gates Delta writes and wide-column writes per type.
- During rolling upgrade, all nodes upgrade to code that *understands* the new keys but the flag remains disabled.
- Once all nodes are upgraded and confirmed healthy, the flag is flipped to enable new writes.
- A brief dual-write window (writing both the old format *and* the new format) can be used if a fallback path must be preserved.

**Option B — Blue/Green deployment**

- Stand up a parallel cluster (green) with the new code.
- Use a proxy (or DNS cutover) to drain traffic from the old cluster (blue) to the new one.
- After traffic is fully on green, decommission blue.

**Option C — Dual-write proxy**

- Deploy a thin proxy layer that emits both legacy and new writes.
- Once all consumers use the new read path, remove legacy writes.

**Recommended approach**: Option A (feature flag) is the least operationally complex path. Option B is preferred when instant rollback capability is required.

---

## Trade-offs

### List

| Aspect | Current (RMW) | Delta + Claim |
|--------|---------------|---------------|
| PUSH write conflict | O(concurrent writers) | No metadata conflict |
| POP write conflict | O(concurrent poppers) | Only same-sequence conflicts (Claim) |
| Write Latency | 1 RTT (with retries) | 1 RTT (no retries, POP retries on Claim collision) |
| Read Latency (LLEN) | O(1) | O(Number of Deltas) *Controlled by compaction* |
| Read Latency (LRANGE) | O(range) | O(range) + 1 claim scan per window |
| Storage | 24 bytes metadata | 24B meta + 16B × N deltas + claim × M |
| Complexity | Low | Medium (compaction + Claim GC) |

### Hash / Set (Wide-Column + Delta)

| Aspect | Current (Monolithic Blob) | Wide-Column + Delta |
|--------|--------------------------|---------------------|
| Write conflict (different fields/members) | Always conflicts (same blob key) | No conflict |
| Write conflict (same field/member) | Always conflicts | OCC on field/member key only |
| Write Latency | 1 RTT (serialize entire blob, with retries) | 1 RTT (write only changed keys, no metadata retry) |
| Read Latency (HGET/SISMEMBER) | O(N) deserialize entire blob | O(1) point read |
| Read Latency (HLEN/SCARD) | O(1) from deserialized blob | O(Number of Deltas) *Controlled by compaction* |
| Read Latency (HGETALL/SMEMBERS) | O(N) deserialize | O(N) prefix scan |
| Storage | Single blob (compact) | Per-field/member keys + meta + deltas (more keys) |
| Bandwidth | Entire blob on every mutation | Only changed fields/members |
| Complexity | Low | Medium (wide-column + compaction) |

### ZSet (Delta on Wide-Column)

| Aspect | Current Wide-Column (PR #483) | Wide-Column + Delta |
|--------|-------------------------------|---------------------|
| ZADD write conflict (different members) | Conflicts on `!zs|meta|` | No metadata conflict |
| ZADD write conflict (same member) | OCC on member key + meta key | OCC on member key only |
| Write Latency | 1 RTT (with meta retries) | 1 RTT (no metadata retry) |
| Read Latency (ZCARD) | O(1) | O(Number of Deltas) *Controlled by compaction* |
| Read Latency (ZSCORE/ZRANGE) | Unchanged | Unchanged (no delta involvement) |
| Storage | meta + member + score keys | + delta 8B × N |
| Complexity | Medium | Medium (+ compaction for deltas) |

## Design Decisions

The following points have been finalized. Unless noted as List-specific, these decisions apply to **all collection types**.

#### D1. Limits on Delta Accumulation (All Types)

**Decision: Hard limit on unapplied Deltas with fallback to immediate compaction.**

Performing synchronous compaction on every write would cause write conflicts on the base metadata for the compaction transaction itself, introducing retries to what should be a conflict-free write path. Delta accumulation is therefore managed primarily by tuning `scanInterval` and `maxDeltaCount` for Background Compaction.

However, relying solely on warning logs is insufficient for production safety. The system uses three distinct limit parameters:

- **`maxDeltaCount`** (default: 64) — the soft threshold at which the Background Compactor schedules the key for compaction and emits a warning log.
- **`maxDeltaScanLimit`** (default: `maxDeltaCount × 4 = 256`) — the maximum number of Delta entries fetched by a single `ScanAt` call in `resolve*Meta()`. This is also the **hard limit**: when `len(deltas) == maxDeltaScanLimit`, the scan was truncated and the result would be incorrect. In that case `resolve*Meta()` returns an error instead of silently wrong metadata.
- **`maxDeltaHardLimit`** is an alias for `maxDeltaScanLimit`; they are the same value. The naming distinction in this document merely emphasises the two roles the value plays (scan ceiling vs. correctness guard).

When the hard limit is hit, the caller triggers a synchronous compaction for that key before retrying the operation. This prevents reads from ever returning silently incorrect results.

This two-tier approach avoids the performance cost of synchronous compaction on hot write paths while guaranteeing correctness under extreme accumulation.

#### D2. POP Conflict Avoidance (List Only)

**Decision: Introduce a Claim mechanism (CAS-based).** (See Section 6)

Mutual exclusion for `POP` target items will be managed using Claim keys (`!lst|claim|<key><seq>`). Concurrent `POP` operations for the same sequence will result in one failing via OCC write-write conflict, with the failing side retrying by claiming the next sequence.

This mechanism is **not needed for Hash, Set, or ZSet** because their removal operations (`HDEL`, `SREM`, `ZREM`) target named fields/members, not positional elements. OCC on the field/member key itself provides sufficient mutual exclusion.

#### D3. Empty Collection Detection (All Types)

**Decision: Defer cleanup to the next Background Compaction. Update base metadata to `Len = 0` but do NOT delete it.**

Immediate cleanup will not occur even if `Len == 0` after aggregating Deltas. When Background Compaction detects `Len == 0`:
- The base metadata key is **updated** to reflect `Len = 0` (not deleted). This is critical because writers do not register the base metadata in their `readSet`. If the key were deleted, a concurrent writer would see no metadata and incorrectly assume a fresh collection, restarting sequence numbering from zero and causing sequence collisions or data corruption.
- All applied Deltas, data keys (items, fields, members, score keys), and Claim keys (for List) are deleted.
- During the brief window between compactions where an empty collection persists, `resolve*Meta()` will return `Len == 0`, ensuring cardinality queries correctly report an empty collection.
- Full metadata deletion (including the base metadata key) is only performed by the `DEL` command, which follows the standard transactional flow with proper `readSet` registration.

**Lazy reaping of stale tombstones**: To prevent indefinite accumulation of `Len = 0` base metadata keys for transient collections (e.g., short-lived queues created by `RPUSH` and fully drained by `LPOP`), Background Compaction includes a lazy reaping pass. During each compaction cycle, if a base metadata key has `Len = 0`, no associated Deltas, no associated data keys, and the key's last-modified timestamp is older than the MVCC retention window (`ActiveTimestampTracker.Oldest()`), the compactor may safely delete the base metadata key. This is safe because:
- No in-flight readers can reference the key (it is older than the retention window).
- No concurrent writers can be active (no Deltas exist, and any new write would create fresh metadata).
- The `DEL` command path remains the primary mechanism for immediate cleanup; lazy reaping is a secondary safety net.

#### D4. Hash/Set Wide-Column Decomposition as Prerequisite

**Decision: Hash and Set must be decomposed into per-field/member keys before applying the Delta pattern.**

The Delta pattern for metadata only eliminates cardinality-update conflicts. If the data itself remains a monolithic blob, all mutations still conflict on that single key, making the metadata delta improvement moot. Wide-column decomposition ensures that:

- Mutations on different fields/members never conflict on data keys.
- The only remaining point of conflict (the metadata key) is then addressed by the Delta pattern.
- Read operations like `HGET` and `SISMEMBER` become O(1) point reads instead of O(N) blob deserialization.
