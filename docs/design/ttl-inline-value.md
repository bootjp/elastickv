# Design: TTL Embedded in Data Values

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
(`docs/design/ttl-memory-buffer.md`) is a workaround for this symptom; it does
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

Alternatively, a two-byte magic + version prefix (`0xFF 0x01`) avoids the
collision problem at the cost of two extra bytes per value:

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

### 3.3 Collection metadata encoding

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

The on-disk format gains 8 bytes; the existing 24-byte format is distinguished
from the new 32-byte format by length during decode (backward-compatible read).

For HLL and stream types the TTL is appended to the serialised value using the
same flags+expireAt header described in §3.2.

### 3.4 Read path (after migration)

```
keyTypeAt(key, readTS)
  → getAnchorValueAt(key, readTS)   # single store read
    → decode flags → if has_ttl, check expireAt vs time.Now()
  → returns (type, isExpired) in one call
```

No secondary `!redis|ttl|<key>` lookup on the hot read path.

### 3.5 Write path

#### `SET key value EX 30`

```
Raft entry (IsTxn=true):
  Put  !redis|str|key  →  encoded(value, expireAt=now+30s)
  Put  !redis|ttl|key  →  encoded(expireAt)   ← scan index only
```

Both are written in the same transaction.  The TTL buffer and its background
goroutine are no longer needed for correctness; they can be removed.

#### `EXPIRE key 30` (after-the-fact TTL on an existing key)

For **string** keys: read-modify-write the anchor value to update `expireAt`.
For **collection** keys: read-modify-write the metadata key to update `ExpireAt`.

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

On reads the TTL key is **never consulted**.  It is written in the same Raft
entry as the data to keep it consistent, but it is not the authoritative source.

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

### Phase 1 — Background migration

A one-time background job rewrites each string key to the new encoding and each
collection metadata key to the new format.  During migration the dual-read
fallback remains active.

### Phase 2 — Remove legacy TTL read path

Once all nodes have migrated (cluster-level flag or store version check), the
`!redis|ttl|<key>` lookup in `ttlAt()` is removed.

### Phase 3 — Remove TTL buffer

The `TTLBuffer`, `runTTLFlusher`, and `flushTTLBuffer` can be removed.  TTL
writes are back in the same `IsTxn=true` transaction as data writes, so no
buffering is needed.  The write-conflict concern is resolved because OCC
conflicts on the anchor key are expected and retried, and concurrent `EXPIRE`
operations on distinct keys have no interaction.

---

## 6. Impact on existing write-conflict fix

The TTL buffer (see `docs/design/ttl-memory-buffer.md`) was introduced to
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

adapter/
  redis_compat_types.go  encodeRedisStr / decodeRedisStr (new with TTL header)
                         ttlAt(): read from decoded anchor, not !redis|ttl| key
                         hasExpiredTTLAt(): inline TTL check from anchor
  redis_compat_helpers.go  keyTypeAt(): single anchor read
  redis_compat_commands.go  setExpire(): anchor read-modify-write
  redis.go               remove TTLBuffer field, runTTLFlusher, flushTTLBuffer
                         remove WithTTLFlushInterval option
  redis_ttl_buffer.go    delete (Phase 3)
  redis_lua_context.go   commit(): TTL back in data Raft entry; remove
                         flushTTLForKeyToBuffer / flushTTLToBuffer
```

---

## 9. Implementation phases summary

| Phase | Description | Risk |
|---|---|---|
| 0 | Add dual-read fallback; new writes use new encoding | Low — reads always fall back |
| 1 | Background migration of existing keys | Medium — requires careful compaction |
| 2 | Remove legacy `!redis|ttl|` read path | Low — only after full migration |
| 3 | Remove TTL buffer | Low — no longer needed post-migration |
