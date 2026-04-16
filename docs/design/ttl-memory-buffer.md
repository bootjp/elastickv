# Design: TTL In-Memory Buffer with Batch Raft Flush

## 1. Background and motivation

### Problem

When redis-proxy proxies Misskey requests, EVALSHA secondary writes time out en masse
with `context deadline exceeded`.

Root-cause analysis:

1. Misskey rate-limit Lua scripts follow the pattern `INCR key` + `EXPIRE key window`.
2. When elastickv4 executes the EVALSHA secondary write, `runLuaScript` →
   `retryRedisWrite` attempts to commit a Raft transaction.
3. `luaScriptContext.commit()` packs **data changes (INCR) and TTL changes (EXPIRE)
   into the same `IsTxn=true` transaction**.
4. Concurrent `EXPIRE` calls on the same rate-limit key produce **write conflicts on
   `!redis|ttl|<key>`**.
5. Data write conflicts are eliminated for list/hash/set/zset by the Delta
   compaction scheme, but TTL has no equivalent.
6. `retryRedisWrite` allows up to 50 retries with up to 50 ms backoff each, plus
   Raft round-trip overhead, totalling ≈ 5 s — exceeding `SecondaryTimeout`.

### Why TTL is hard to Delta-ize

Delta keys use a unique timestamp-embedded key per write to enable conflict-free
merges.  TTL is a single scalar (`uint64` milliseconds); there is no natural merge
semantic to apply.

### Proposed approach

**Buffer all TTL writes in memory and flush them to Raft in batches on a background
goroutine.**

Benefits:
- TTL elements are removed from `IsTxn=true` Raft transactions → TTL write
  conflicts eliminated.
- Multiple TTL updates for distinct keys are coalesced into one Raft entry →
  lower Raft write amplification.

---

## 2. Goals / non-goals

### Goals

- Eliminate TTL write conflicts in EVALSHA to remove secondary-write timeouts.
- Apply the same buffering to `EXPIRE`/`PEXPIRE`, `SET EX`/`SETEX`/`GETEX`, and
  `MULTI`/`EXEC` transactions uniformly.
- Reads (`ttlAt`) consult the buffer first so the accurate TTL is returned even
  before a flush.
- Flush failures re-queue entries for the next flush interval; no silent TTL loss.

### Non-goals

- Buffering data values (string/list/hash/set/zset): out of scope.
- Strict atomicity of data + TTL under process crash: the window where data is
  committed but TTL has not yet been flushed is accepted (see §8).
- Full MVCC snapshot isolation for TTL inside `MULTI`/`EXEC`: approximate model
  is preserved.

---

## 3. Design overview

```
Write (EXPIRE / SET EX / Lua EVALSHA / MULTI-EXEC)
     │
     ▼
┌──────────────────────────────┐
│  TTLBuffer  (per RedisServer) │
│  map[userKey] → entry         │
│  { expireAt *time.Time,       │
│    seq      uint64 }          │
└──────────────────────────────┘
     │  background goroutine (100 ms default)
     ▼
┌──────────────────────────────┐
│  coordinator.Dispatch         │
│  IsTxn = false                │  ← no conflict detection, last-writer-wins
│  batch: N TTL elements        │
└──────────────────────────────┘
     │
     ▼
  Raft / MVCCStore

Read (ttlAt / hasExpiredTTLAt)
  1. check TTLBuffer        ← buffer-first (always most recent)
  2. fallback: MVCCStore.GetAt(redisTTLKey, readTS)
```

### Components

| Component | Role | File |
|---|---|---|
| `TTLBuffer` | Thread-safe TTL write buffer | `adapter/redis_ttl_buffer.go` (new) |
| `RedisServer.ttlBuffer` | Server-level embedding | `adapter/redis.go` |
| TTL flush goroutine | Periodic background flush | `adapter/redis.go` `Run()` |
| `ttlAt()` | Buffer-first TTL read | `adapter/redis_compat_types.go` |
| Lua write path | Remove TTL from data commit, write to buffer after | `adapter/redis_lua_context.go` |
| Direct TTL write paths | `EXPIRE`, `saveString`, `txnContext` | multiple files |

---

## 4. TTLBuffer detailed design

```go
// ttlBufferEntry holds a pending TTL update for one user key.
// expireAt == nil represents a TTL deletion (PERSIST).
type ttlBufferEntry struct {
    expireAt *time.Time
    seq      uint64  // monotonic; later writes win on same key
}

// TTLBuffer is a thread-safe in-memory TTL write buffer.
type TTLBuffer struct {
    mu      sync.RWMutex
    entries map[string]ttlBufferEntry
    counter atomic.Uint64
}
```

### Key operations

```go
// Set writes a TTL entry for key. expireAt==nil means delete TTL (PERSIST).
func (b *TTLBuffer) Set(key []byte, expireAt *time.Time)

// Get returns the buffered TTL for key.
// found=false → no entry; caller should fall back to the store.
// nil expireAt with found=true → TTL was explicitly removed.
func (b *TTLBuffer) Get(key []byte) (expireAt *time.Time, found bool)

// Drain atomically snapshots the buffer and resets it for new writes.
func (b *TTLBuffer) Drain() map[string]ttlBufferEntry

// MergeBack re-inserts entries from a failed flush, skipping keys
// that already have a newer write in the live buffer.
func (b *TTLBuffer) MergeBack(entries map[string]ttlBufferEntry)
```

### `seq` field

A monotonically increasing counter ensures the latest call to `Set` wins when
`MergeBack` races with concurrent writes.

---

## 5. Read-path changes

### `ttlAt()` — `adapter/redis_compat_types.go`

```go
func (r *RedisServer) ttlAt(ctx context.Context, userKey []byte, readTS uint64) (*time.Time, error) {
    // Buffer holds the most recent write regardless of Raft flush state.
    if expireAt, found := r.ttlBuffer.Get(userKey); found {
        return expireAt, nil
    }
    // Fall back to Raft store at readTS.
    raw, err := r.store.GetAt(ctx, redisTTLKey(userKey), readTS)
    // ... unchanged
}
```

All callers of `ttlAt` (`hasExpiredTTLAt`, `keyTypeAt`, `TTL`/`PTTL` commands,
`luaScriptContext.finalTTL`) automatically gain buffer-first semantics.

### `isLeaderKeyExpired` / `tryLeaderNonStringExists` — `adapter/redis.go`

These read TTL directly via `tryLeaderGetAt(redisTTLKey, 0)`.  Both are updated to
check the buffer before the Raft store.

### MVCC TTL read tracking removal — `adapter/redis.go`

`txnContext` currently registers `redisTTLKey` reads via `trackReadKey` so that
external TTL writes trigger write-conflict detection.  Since TTL is now written via
`IsTxn=false` flushes (which do not participate in conflict detection), this tracking
is no longer useful.  All `trackReadKey(redisTTLKey(...))` call sites are removed.

### MULTI/EXEC — TTL snapshot isolation is not guaranteed

With TTL removed from OCC read sets, a concurrent `EXPIRE` executed between a
client's `MULTI` and `EXEC` will **not** surface as `ErrWriteConflict` for data
operations.  Concretely:

```
Client A:  MULTI
Client A:  SET   key value  ← queued
Client B:  EXPIRE key 5     ← writes TTL buffer, IsTxn=false
Client A:  EXEC             ← commits data, then flushes TTL from A's own state
```

After `EXEC`, Client A's buffered TTL (if any) will overwrite Client B's `EXPIRE`
via last-writer-wins, because both take the `IsTxn=false` path.

**Impact**: workloads that rely on TTL changes being detected as OCC conflicts
inside `MULTI/EXEC` will no longer see that behaviour.  For rate-limiting keys
(the primary use-case of this change) this is harmless — the TTL difference is
at most one flush interval (100 ms) and the final committed value is always the
most recent write as determined by `seq`.

---

## 6. Write-path changes

### Change sites

| Location | File | Change |
|---|---|---|
| `luaScriptContext.commit()` | `redis_lua_context.go` | Remove TTL elems from Raft group; call `flushTTLToBuffer()` after successful dispatch |
| `commitElemsForKey()` | `redis_lua_context.go` | Drop `ttlCommitElems` call |
| `setExpire()` positive TTL branch | `redis_compat_commands.go` | Replace `dispatchElems` with `ttlBuffer.Set` |
| `incr()` TTL-clear | `redis_compat_commands.go` | Remove `Del redisTTLKey` from Raft dispatch; add `ttlBuffer.Set(key, nil)` |
| `saveString()` | `redis_compat_helpers.go` | Remove TTL elems from dispatch; add `ttlBuffer.Set` after |
| `replaceWithStringTxn()` | `redis.go` | Remove TTL elems from dispatch; add `ttlBuffer.Set` after |
| `txnContext.commit()` | `redis.go` | Drop `buildTTLElems`; call `flushTTLToBuffer()` after Raft dispatch |

### Lua script commit — key change

```go
// Before: TTL inside IsTxn=true (causes write conflicts)
func (c *luaScriptContext) commit() error {
    elems := buildDataAndTTLElems(...)
    coordinator.Dispatch(&kv.OperationGroup{IsTxn: true, Elems: elems})
}

// After: data and TTL separated; TTL written to buffer post-commit
func (c *luaScriptContext) commit() error {
    dataElems := buildDataElems(...)   // no TTL
    if len(dataElems) > 0 {
        coordinator.Dispatch(&kv.OperationGroup{IsTxn: true, Elems: dataElems})
        // returns on error without writing TTL
    }
    c.flushTTLToBuffer()   // only reached on success (or pure-TTL scripts)
    return nil
}
```

This guarantees TTL reaches the buffer only after the data commit succeeds.  If the
data commit is retried by `retryRedisWrite`, TTL is written to buffer only on the
successful attempt.

---

## 7. Background flush

### Goroutine lifecycle

`Run()` starts a flush goroutine and ensures a final flush on shutdown:

```go
func (r *RedisServer) Run() error {
    ctx, cancel := context.WithCancel(context.Background())
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        r.runTTLFlusher(ctx)
    }()
    err := redcon.Serve(r.listen, ...)
    cancel()
    wg.Wait()   // drain buffer before returning
    return errors.WithStack(err)
}
```

### Flush logic

```go
func (r *RedisServer) flushTTLBuffer(ctx context.Context) {
    entries := r.ttlBuffer.Drain()
    if len(entries) == 0 { return }

    elems := buildTTLFlushElems(entries)   // Put or Del per entry
    _, err := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
        IsTxn: false,   // last-writer-wins, no conflict detection
        Elems: elems,
    })
    if err != nil {
        r.ttlBuffer.MergeBack(entries)  // retry on next tick
        // log warning
    }
}
```

### Flush interval trade-offs

| Interval | Batch size | Raft load | Max TTL loss on crash |
|---|---|---|---|
| 10 ms | small | higher | minimal |
| **100 ms** (default) | medium | low | < 100 ms |
| 1 s | large | minimal | < 1 s |

Default: 100 ms, configurable via `RedisServerOption`.

---

## 8. Consistency model and trade-offs

### Buffer as the authoritative TTL source

Entries in the buffer represent writes newer than the latest Raft flush.  Reading
the buffer before the Raft store ensures callers always see the most recent TTL
without waiting for a flush.

### Crash: data committed, TTL not yet flushed

If the process crashes after a successful Raft data commit but before the TTL flush:
- Data key exists in Raft.
- TTL entry exists only in the (now-lost) buffer.
- Effect: the key has no expiry until the next write sets it again.

**Accepted trade-off**: for rate-limiting keys, a temporarily non-expiring key is
harmless.  In the worst case, the rate-limit window resets on the next write.

### MVCC snapshot isolation for TTL

`ttlAt(key, readTS)` ignores `readTS` once a buffer entry exists.  This breaks
strict TTL snapshot isolation but aligns with the existing approximate TTL model.
The impact is limited to a very short window between a concurrent EXPIRE and commit.

### Follower reads

Each `RedisServer` instance maintains its own buffer.  Followers that handle read
requests will have an empty buffer and fall back to the Raft store.  TTL precision
on follower reads is already approximate; this does not change that property.

### Stale TTL keys after DEL

When a key is deleted (`DEL`, type-change replace), `deleteLogicalKeyElems` emits a
Raft `Del` for `redisTTLKey` if it exists in the store.  Any concurrent buffer entry
for the deleted key will be flushed later, creating a short-lived "zombie" TTL key in
Raft.  This is harmless because `keyTypeAt` only checks TTL after confirming the data
key exists; a zombie TTL for a non-existent data key is never observed.

---

## 9. Failure handling

| Scenario | Behaviour |
|---|---|
| Flush fails (Raft unavailable) | `MergeBack` restores drained entries; retried on next tick |
| Process crash | Buffer lost; keys exist without TTL until next write |
| Concurrent Set + Drain | Protected by `sync.RWMutex` |
| Buffer overflow (> 1 M entries) | Oldest entries dropped with a warning log |

---

## 10. Implementation steps

1. **`adapter/redis_ttl_buffer.go`** (new) — `TTLBuffer`, `buildTTLFlushElems`
2. **`adapter/redis.go`** — add field, init, `Run()` goroutine, flush helpers,
   `replaceWithStringTxn`, `isLeaderKeyExpired`, `tryLeaderNonStringExists`,
   remove TTL `trackReadKey` calls, replace `buildTTLElems` with
   `hasDirtyTTLStates` + `flushTTLToBuffer` in `txnContext`
3. **`adapter/redis_compat_types.go`** — `ttlAt()` buffer-first
4. **`adapter/redis_compat_helpers.go`** — `saveString()` TTL to buffer
5. **`adapter/redis_compat_commands.go`** — `setExpire()` and `incr()` TTL to buffer
6. **`adapter/redis_lua_context.go`** — `commit()` separation, `commitElemsForKey`
   drops TTL, add `flushTTLToBuffer()`

---

## 11. Affected files summary

```
adapter/redis_ttl_buffer.go        new
adapter/redis.go                   RedisServer struct, Run(), replaceWithStringTxn,
                                   isLeaderKeyExpired, tryLeaderNonStringExists,
                                   txnContext.commit, trackReadKey cleanup
adapter/redis_compat_types.go      ttlAt()
adapter/redis_compat_helpers.go    saveString()
adapter/redis_compat_commands.go   setExpire(), incr()
adapter/redis_lua_context.go       commit(), commitElemsForKey()
```

No changes to Raft commit paths for string/list/hash/set/zset data.
TTL keys are simply excluded from `IsTxn=true` transactions.
