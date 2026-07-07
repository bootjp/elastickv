# Design: TTL Embedded in Data Values

Status: Partial — Redis strings, HLL, and the list/hash/set/zset/stream
metadata anchors now carry inline TTL, and `EXPIRE` updates those anchors in
the same Raft transaction as the secondary scan-index row. Full legacy
`!redis|ttl|` read-path removal and background migration remain open. The TTL
buffer has been removed.

## 1. Background and motivation

### Current architecture

TTL is stored as a separate MVCC key `!redis|ttl|<userKey>` in the Raft-backed
store.  Every key-existence check follows this path:

```
keyTypeAt(key, readTS)
  → rawKeyTypeAt()       # GetAt(!redis|str|key, readTS)   ← data read
  → hasExpiredTTLAt()    # GetAt(!redis|ttl|key, readTS)   ← extra TTL read
```

This design has three structural problems:

### Problem 1 — Read amplification

Every `GET`, `EXISTS`, `TYPE`, `TTL`, `KEYS`, and scan operation issues **two**
store reads per key: one for the data value and one for the TTL key.  At high
read rates this doubles MVCC store I/O.

### Problem 2 — Broken MVCC snapshot semantics

`ttlAt(key, readTS)` reads `!redis|ttl|key` at `readTS`, but TTL writes happen
in a **separate Raft entry** (and since the TTL buffer was introduced, with a
delay of up to `ttlFlushInterval`).  Consequently:

```
Snapshot readTS=T:
  data version at T  →  value written at commitTS ≤ T
  TTL  version at T  →  expiry written at a different commitTS, potentially T-N
```

A snapshot read at time T can observe data at version T but TTL from an earlier
or later version — breaking the atomicity guarantee that MVCC provides.

### Problem 3 — TTL write conflicts (symptom addressed by the buffer)

Because TTL is a separate writable key, concurrent `EXPIRE` calls on the same
key produce write conflicts in `IsTxn=true` Raft transactions.  The TTL buffer
(`docs/design/2026_04_17_implemented_ttl_memory_buffer.md`) is a workaround for this symptom; it does
not fix the root cause.

### Root cause

**TTL is not a first-class attribute of a data version.**  It is treated as an
independent, separately-versioned key rather than as metadata that belongs to a
specific data revision.

---

## 2. Goals / non-goals

### Goals

- Embed TTL as part of the data value so that a data write and its expiry are
  committed in a single Raft entry.
- Eliminate the extra `!redis|ttl|<key>` lookup on every read.
- Restore correct MVCC snapshot semantics: a snapshot at `readTS=T` observes
  the TTL that was in effect when the data version at T was written.
- Retain `!redis|ttl|<key>` as a **secondary scan index** for background
  expiration (not as an authoritative TTL source).

### Non-goals

- Changing the Raft log format or the MVCC store wire protocol.
- Implementing active expiration (lazy expiration via read-path TTL check is
  sufficient for correctness; background scanning is an optimisation).
- Migrating other metadata (e.g. type tags) into the value.

---

## 3. Design overview

### 3.1 Principle

Each data type has one **anchor key** whose value contains all mutable scalar
metadata for that logical key.  TTL is added to this anchor value.

| Type | Anchor key | Current anchor value |
|---|---|---|
| string | `!redis\|str\|<key>` | raw bytes |
| list | `!lm\|<key>` (ListMeta) | `{Head, Len, Tail}` |
| hash | `!hm\|<key>` (HashMeta) | `{Len}` |
| set | `!sm\|<key>` (SetMeta) | `{Len}` |
| zset | `!zsm\|<key>` (ZSetMeta) | `{Len}` |
| HLL | `!redis\|hll\|<key>` | serialised HLL |
| stream | `!redis\|stream\|<key>` | serialised stream |

After the change, every anchor value carries an optional `expireAt` field.

### 3.2 String value encoding

Current encoding: raw bytes.

New encoding (versioned header):

```
┌──────────────────────────────────────────────────┐
│ 1 byte  flags                                    │
│   bit 0 = has_ttl                                │
│ 8 bytes expireAtMillis  (present iff has_ttl=1)  │
│   big-endian uint64, milliseconds since epoch    │
│ N bytes user value                               │
└──────────────────────────────────────────────────┘
```

A leading `flags` byte equal to `0x00` (no TTL) followed by raw bytes is fully
backward-compatible with no-TTL reads once a migration is applied; the old raw
bytes that start with `0x00` are rare in practice (binary data) but must be
handled during migration (see §7).

Alternatively, a two-byte magic + version prefix (`0xFF 0x01`) distinguishes
the new encoding from the overwhelming majority of user payloads, at the cost
of two extra bytes per value:

```
┌─────────────────────────────────────────────────────┐
│ 2 bytes magic+version  0xFF 0x01                    │
│ 1 byte  flags                                       │
│ 8 bytes expireAtMillis (present iff flags & 0x01)   │
│ N bytes user value                                  │
└─────────────────────────────────────────────────────┘
```

A raw value not starting with `0xFF 0x01` is treated as a legacy (no-TTL)
encoding during the migration window.

**Ambiguity with legacy payloads.** Redis strings are arbitrary bytes and *can*
legitimately start with `0xFF 0x01`; the magic prefix is a heuristic, not a
cryptographic discriminator. The implementation accepts this residual risk as
follows:

- Before any write goes out in the new format, the value is always wrapped with
  the magic+version header, so any post-upgrade payload that starts with
  `0xFF 0x01` is guaranteed to be a new-format envelope produced by this code
  path.
- During the rolling-upgrade window, pre-existing bytes that happen to start
  with `0xFF 0x01` will be decoded as new-format. If the `flags` byte or TTL
  bytes happen to form an invalid header, `decodeRedisStr` returns an error
  and the caller treats the key as malformed (e.g. `isLeaderKeyExpired` treats
  the key as expired rather than silently alive).
- For deployments that need a stronger guarantee, the migration compactor
  should be run to rewrite every legacy value with the new envelope before
  Phase 2 (legacy-read removal) begins.

### 3.3 Collection metadata encoding

`ListMeta`, `HashMeta`, `SetMeta`, `ZSetMeta`, and `StreamMeta` now carry an
`ExpireAt uint64` field in milliseconds since epoch. The secondary
`!redis|ttl|<key>` row is still written as the scan index and as a migration
fallback for old-format metadata; it is no longer the only source of truth for
these collection anchors.

HLL payloads use a small magic+version envelope around the existing serialized
set-compatible payload. The envelope carries the same optional `expireAt`
metadata as strings while legacy bare HLL payloads still decode during the
migration window and fall back to `!redis|ttl|<key>`.

`ListMeta`, `HashMeta`, `SetMeta`, `ZSetMeta` are fixed-width binary structs.
Each gains an `ExpireAt uint64` field (0 = no TTL):

```go
// Example: ListMeta before
type ListMeta struct {
    Head int64
    Len  int64
    Tail int64   // computed field, not stored
}

// ListMeta after
type ListMeta struct {
    Head     int64
    Len      int64
    ExpireAt uint64  // ms since epoch; 0 = no TTL
}
```

The on-disk format gains 8 bytes; the existing 8-byte simple metadata and
24-byte list/stream metadata formats are distinguished from the new 16-byte and
32-byte formats by length during decode (backward-compatible read).

Stream metadata follows the same 32-byte metadata shape. HLL uses its own
versioned envelope because its anchor is a serialized probabilistic payload
rather than fixed-width metadata.

### 3.4 Read path (target after migration)

```
keyTypeAt(key, readTS)
  → getAnchorValueAt(key, readTS)   # single store read
    → decode flags → if has_ttl, check expireAt vs time.Now()
  → returns (type, isExpired) in one call
```

Today the read path checks inline string, HLL, and collection metadata first,
then falls back to `!redis|ttl|<key>` for legacy payloads and metadata. The
target after the migration phases is no secondary `!redis|ttl|<key>` lookup on
the hot read path.

### 3.5 Write path

#### `SET key value EX 30`

```
Raft entry (IsTxn=true):
  Put  !redis|str|key  →  encoded(value, expireAt=now+30s)
  Put  !redis|ttl|key  →  encoded(expireAt)   ← scan index only
```

Both are written in the same transaction.  The TTL buffer and its background
goroutine are no longer needed for correctness and have been removed.

#### `EXPIRE key 30` (after-the-fact TTL on an existing key)

For **string** keys: read-modify-write the anchor value to update `expireAt`.
For **collection** keys: read-modify-write the metadata key to update
`ExpireAt`. When the collection has uncompacted length deltas, `EXPIRE` first
folds the deltas into the base metadata key in the same transaction so the new
metadata value is authoritative and does not double-count future reads.

This is a `IsTxn=true` transaction (OCC read-modify-write on the anchor key),
which is the existing pattern for all other in-place updates.

The write conflict concern that motivated the TTL buffer does not apply here
because the anchor key is the same key already covered by OCC retries.

#### `PERSIST key`

Same as EXPIRE but sets `expireAt = 0` / clears the `has_ttl` flag.

### 3.6 `!redis|ttl|<key>` — secondary scan index

The TTL key is retained solely to support background expiration scanning:

```
Background scanner (future work):
  ScanAt(!redis|ttl|, !redis|ttl|\xFF, now)
    → for each entry where expireAt < now: schedule lazy deletion
```

For strings, new-format HLL payloads, and new-format collection metadata, reads
consult the anchor first. During the migration window, `!redis|ttl|<key>` is
still consulted when the anchor is old-format.

---

## 4. MVCC snapshot consistency

With TTL embedded in the anchor value:

```
Snapshot readTS=T:
  GetAt(!redis|str|key, T)  →  {value, expireAt}  ← both from the same version
```

The data version and its TTL are always from the same commit.  The anomaly
where a snapshot can observe a data version with a TTL written at a different
timestamp is eliminated.

---

## 5. Backward compatibility and migration

### Phase 0 — Dual-read fallback (deployed alongside old nodes)

The read path tries to decode the new format first; if the magic/version prefix
is absent it falls back to the legacy raw-byte format and reads
`!redis|ttl|<key>` from the store as before.

This allows a rolling upgrade with no downtime.

### Phase 1 — Collection metadata and HLL inline TTL

The live write path now emits the expanded metadata formats for list, hash, set,
zset, and stream. `EXPIRE` on those types updates the anchor metadata and the
secondary scan index together. Delta compaction preserves existing `ExpireAt`
when it folds length deltas into base metadata. The logical backup decoder
accepts both old and new metadata lengths and exports inline metadata TTL as
`expire_at_ms`.

HLL writes now emit an HLL-specific envelope around the existing payload.
`EXPIRE` and Redis transaction TTL updates rewrite the HLL anchor and the
secondary scan index in the same Raft transaction. Legacy HLL payloads remain
readable and are upgraded on the next HLL write.

### Phase 2 — Background migration

A one-time background job rewrites each string key to the new encoding and each
collection metadata key to the new format.  During migration the dual-read
fallback remains active.

### Phase 3 — Remove legacy TTL read path

Once all nodes have migrated (cluster-level flag or store version check), the
`!redis|ttl|<key>` lookup in `ttlAt()` is removed.

### Phase 4 — Remove TTL buffer

The `TTLBuffer`, `runTTLFlusher`, and `flushTTLBuffer` have been removed. TTL
writes are back in the same `IsTxn=true` transaction as data writes, so no
buffering is needed. The write-conflict concern is resolved because OCC
conflicts on the anchor key are expected and retried, and concurrent `EXPIRE`
operations on distinct keys have no interaction.

---

## 6. Impact on existing write-conflict fix

The TTL buffer (see `docs/design/2026_04_17_implemented_ttl_memory_buffer.md`) was introduced to
remove TTL from `IsTxn=true` transactions and thereby eliminate write conflicts
on `!redis|ttl|<key>` for concurrent Lua scripts (`INCR` + `EXPIRE`).

With TTL embedded in the anchor value:

- `INCR key` modifies `!redis|str|key` (OCC on the anchor).
- `EXPIRE key N` modifies `!redis|str|key` (OCC on the same anchor).
- These two operations do conflict on the anchor key, but that is correct: the
  last writer wins and retries converge.  The key difference is that the anchor
  key is already under OCC for `INCR`, so the retry is free.

The Lua pattern `INCR key; EXPIRE key N` packs both into the same Lua commit,
which writes the anchor key once (value + TTL atomically).  No conflict arises.

---

## 7. Trade-offs and risks

| Aspect | Impact |
|---|---|
| Read amplification | Eliminated: 1 store read per key instead of 2 |
| MVCC consistency | Restored: TTL and data version are always co-located |
| Write path for `EXPIRE` | Now requires read-modify-write on anchor; adds one store read per `EXPIRE` call. Acceptable because `EXPIRE` is not on the critical hot path for Misskey rate-limiting (which uses `SET EX`). |
| Migration complexity | Non-trivial; requires careful rolling-upgrade handling and a dual-read window |
| TTL buffer removal | Simplifies the adapter significantly; reduces code surface and potential failure modes |
| Wide-column `EXPIRE` | `EXPIRE` on a list/hash/set/zset must read-modify-write the meta key. This is consistent with how `RENAME` and type-change operations already work. |
| String values starting with `0xFF` | Must be handled correctly during migration to avoid misinterpreting legacy raw bytes as the new format. Using `0xFF 0x01` as the magic ensures byte `0xFF` alone (common in UTF-8 and binary data) does not trigger false positives. |

---

## 8. Affected components

```
store/
  list_helpers.go       ListMeta: add ExpireAt field; update marshal/unmarshal
  hash_helpers.go       HashMeta: add ExpireAt field
  set_helpers.go        SetMeta: add ExpireAt field
  zset_helpers.go       ZSetMeta: add ExpireAt field
  stream_helpers.go     StreamMeta: add ExpireAt field

adapter/
  redis_compat_types.go  encodeRedisStr / decodeRedisStr (new with TTL header)
                         encodeRedisHLL / decodeRedisHLL
                         ttlAt(): read from decoded anchor before fallback
                         hasExpired(): inline TTL check from anchor
  redis_collection_ttl.go collection metadata TTL helpers
  redis_expire_cmds.go   setExpire(): anchor read-modify-write
  redis_delta_compactor.go  preserve ExpireAt during metadata compaction
  redis.go               remove TTLBuffer field, runTTLFlusher, flushTTLBuffer
                         remove WithTTLFlushInterval option
  redis_ttl_buffer.go    delete (Phase 4)
  redis_lua_context.go   commit(): TTL back in data Raft entry; remove
                         flushTTLForKeyToBuffer / flushTTLToBuffer
```

---

## 9. Implementation phases summary

| Phase | Description | Risk |
|---|---|---|
| 0 | Add dual-read fallback; new writes use new encoding | Low — reads always fall back |
| 1 | Add inline TTL to list/hash/set/zset/stream metadata and HLL payloads | Medium — delta compaction must preserve TTL |
| 2 | Background migration of existing keys | Medium — requires careful compaction |
| 3 | Remove legacy `!redis\|ttl\|` read path | Low — only after full migration |
| 4 | Remove TTL buffer | Low — shipped |
