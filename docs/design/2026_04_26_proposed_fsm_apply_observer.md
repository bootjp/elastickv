# FSM ApplyObserver for cross-node event-driven Redis BLOCK paths

Status: Proposed
Author: bootjp
Date: 2026-04-26

## Summary

Add an `ApplyObserver` hook to `kvFSM.Apply` so every successful raw-request
mutation fires a callback with the touched key (and op type). The Redis
adapter registers a stream-key observer that signals the `streamWaiterRegistry`
introduced in `perf/redis-event-driven-block`, eliminating the 100ms fallback
poll's role in correctness for **every** XREAD BLOCK / BZPOPMIN / blocking-list
waiter on **every** node — leader or follower, command-driven or Lua-driven.

This is Phase 2 of the XREAD CPU work landed in
`perf/redis-event-driven-block` (Phase 1). Phase 1's `xaddTxn`-side signal
covers the dominant production case (Redis-protocol XADD on the leader →
local XREAD BLOCK on the same leader) but leaves three classes of waiters
to the 100ms fallback timer:

1. **Follower-side XREAD BLOCK** — XADD proxies to the leader; the leader's
   `xaddTxn` signals only the leader's local registry. The follower's FSM
   later applies the replicated entry but has no signal-side hook.
2. **Lua-script XADD** — `redis_lua_context.cmdXAdd` buffers stream
   mutations into `luaStreamState` and flushes via `streamCommitElems` →
   coordinator dispatch. The flush path bypasses `xaddTxn`, so the signal
   is never fired even when the Lua-driving connection is on the leader.
3. **Future direct-mutation paths** — any code that writes a stream entry
   key without going through `xaddTxn` (e.g., admin tools, future
   replication-layer migrations) silently breaks the signal.

The 100ms fallback bounds latency for these cases at 100ms, which is
acceptable but leaves CPU on the table on followers and turns Lua XADDs
into a 100ms-latency floor for any waiter that depends on them.

A FSM-level observer fixes all three with one hook: every Put/Del that
applies on any node, regardless of which adapter or path produced it,
fires the callback.

## Background

### Phase 1 recap

`perf/redis-event-driven-block` added `streamWaiterRegistry` (a
multi-key signal channel keyed by stream user-key) plus:

- `xreadBusyPoll` registers a waiter for every key in the request before
  the first `xreadOnce` (so a pre-check signal cannot be lost), then
  `select`s on `{signal | fallback timer | handlerCtx | deadline}` instead
  of `time.Sleep(redisBusyPollBackoff)`.
- `xaddTxn` calls `streamWaiters.Signal(key)` after a successful
  `dispatchElems` returns. dispatchElems blocks until FSM apply on the
  leader, so the signal is never delivered before the entry is visible
  at the readTS the woken waiter will pick.

The fallback timer (`redisStreamWaitFallback = 100ms`) is the safety net
for paths that bypass `xaddTxn`'s signal call. Phase 1 ships with the
fallback at 100ms because the gap is the same (10x reduction from the
prior 10ms `time.Sleep`) regardless of whether Phase 2 lands.

### What Phase 1 does **not** cover

| Path                                            | Phase 1 wake source        | Latency |
|-------------------------------------------------|----------------------------|---------|
| Redis XADD on leader → XREAD on same leader      | `streamWaiters.Signal`     | µs      |
| Redis XADD on leader → XREAD on follower         | fallback timer             | ≤100ms  |
| Lua `redis.call("XADD")` on leader → XREAD anywhere | fallback timer          | ≤100ms  |
| BZPOPMIN, BLPOP / BRPOP, BLMOVE (Phase C)        | fallback timer (today: busy poll) | ≤fallback |

The follower row is the most important: production traffic on the
2026-04-26 cluster shows the leader at 1530% CPU while the BLOCK signal
already covers the leader's own waiters. The fallback-driven wakes on
followers are a smaller but non-zero share of follower CPU, and the
100ms latency floor for follower-side BLOCK waiters is a regression
from the prior 10ms busy-poll latency floor.

The Lua row is the second-most important. BullMQ-style workloads
(`adapter/redis_bullmq_compat_test.go`) drive XADD almost exclusively
through Lua scripts; under Phase 1 every BullMQ-issued XADD silently
takes the 100ms fallback path even when the consumer is on the leader.

## Design

### 1. ApplyObserver interface in `kv`

Add a small interface in the `kv` package and an option on `kvFSM`:

```go
// kv/fsm_observer.go (new)
package kv

import (
    pb "github.com/bootjp/elastickv/proto"
)

// ApplyObserver receives a notification after every successful raw
// mutation applied by kvFSM.Apply. The observer must be non-blocking:
// implementations are called inline on the Raft apply goroutine, and
// any work that takes longer than ~µs delays the next entry's apply
// (which is on the critical path for write throughput).
//
// Observers receive the op type and key — the value is intentionally
// withheld so observers cannot accidentally retain references to
// large payloads. If a future observer needs the value, the
// signature can extend without breaking existing observers (a
// non-public interface).
type ApplyObserver interface {
    OnApply(op pb.OpType, key []byte)
}

// kvFSM additions:
type kvFSM struct {
    ...
    observers []ApplyObserver  // never written after FSM construction
}

// NewKvFSMWithHLC accepts a variadic options slice for backward compat:
type FSMOption func(*kvFSM)

func WithApplyObserver(o ApplyObserver) FSMOption { ... }
```

### 2. Hook point in `handleRawRequest`

```go
// kv/fsm.go — inside handleRawRequest, after the per-mutation switch
// successfully applied to the store:
for _, mut := range r.Mutations {
    ...
    if err := f.applyMutation(ctx, mut, commitTS); err != nil {
        return err
    }
    for _, o := range f.observers {
        o.OnApply(mut.Op, mut.Key)
    }
}
```

For txn paths (`handleTxnRequest`), fire the observer at the same point
the corresponding lock / write commits — the COMMIT-phase Put is what
makes the entry visible, so the txn-path hook fires there.

DEL_PREFIX is rare (only used for full-key collection deletes) and
fires once per scanned tombstone; ApplyObserver subscribers should
treat it as up to N notifications and signal accordingly.

### 3. RedisServer-side observer

```go
// adapter/redis_stream_apply_observer.go (new)
type streamApplyObserver struct {
    waiters *streamWaiterRegistry
}

func (s *streamApplyObserver) OnApply(op pb.OpType, key []byte) {
    if op != pb.OpType_OP_PUT {
        return  // we only care about new entries; deletes never wake an XREAD
    }
    if !store.IsStreamEntryKey(key) {
        return
    }
    userKey := store.ExtractStreamUserKeyFromEntry(key)
    if userKey == nil {
        return
    }
    s.waiters.Signal(userKey)
}
```

`store.IsStreamEntryKey` and `store.ExtractStreamUserKeyFromEntry` already
exist; adding the observer is a 30-line file plus a wiring line in
`main.go` where the FSM is constructed.

### 4. Wiring

```go
// main.go — at FSM construction:
streamObs := &streamApplyObserver{waiters: redisServer.StreamWaiters()}
fsm := kv.NewKvFSMWithHLC(store, hlc, kv.WithApplyObserver(streamObs))
```

A getter on `RedisServer` exposes the registry without leaking the
unexported type:

```go
// adapter/redis.go
func (r *RedisServer) StreamWaiters() kv.ApplyObserverTarget { ... }
// or, simpler — make streamApplyObserver a method on *RedisServer.
```

Phase 2 will pick the cleaner of the two during implementation.

## Migration

This is purely additive:

- `kv.NewKvFSMWithHLC` keeps its existing signature; the observer is set
  via a new variadic option.
- `kvFSM.Apply` behavior is unchanged when no observer is registered
  (the observer slice is empty).
- `streamWaiterRegistry` and `xreadBusyPoll` from Phase 1 are unchanged
  — Phase 2 simply gives the registry a second source of `Signal()`
  calls.

The 100ms fallback timer stays in place as a safety net (any future
write path that does not register with the observer is still bounded
by it). Once Phase C (BZPOPMIN / BLPOP) lands, the same observer can
fan out to a generic key-waiter registry; XREAD only being one client.

## Trade-offs

### Why not a per-package callback (e.g., a function field on kvFSM)?

A slice-of-`ApplyObserver` is the same overhead (one indirect call per
mutation) but allows multiple subscribers — the Phase C list/zset
work plans to add another observer, and a function field would force
either coupling or wrapper plumbing.

### Why op + key, not the full mutation?

Mutations carry the value; passing the value to observers tempts
implementations to retain a reference to it past the apply boundary,
which can keep large `Put` payloads pinned in memory. Op + key is
strictly enough for any wake-up-style observer; payload-aware
observers can extend the interface later without breaking existing
ones.

### Why not signal directly from `pebbleStore.Apply`?

The store layer is shared with non-Redis use cases (DynamoDB, S3, SQS).
Stream-specific callbacks belong above the store. The FSM is the
narrowest place that sees the full ordered list of applied mutations
and is already the per-node fan-in point for Raft entries.

### Cost of the inline observer call

Each observer call is one virtual dispatch + one `IsStreamEntryKey`
prefix check (8-byte compare) + one nil-check. For non-stream keys
the prefix check fails and the observer returns immediately — well
under 100ns per applied mutation. The Redis adapter's prior 10ms
busy-poll cost dwarfs this even on streams that never see a BLOCK
waiter.

## Phase plan

- **Phase 2.1** — Land `kv.ApplyObserver` interface + `WithApplyObserver`
  option + the inline call in `handleRawRequest`. No subscribers yet.
  Tests: a mock observer asserts it sees every Put/Del op and key;
  benchmark confirms <1% throughput regression on the existing FSM
  apply benchmark.
- **Phase 2.2** — Add `streamApplyObserver`, wire it in `main.go`,
  drop the leader-side `xaddTxn` signal call (now redundant: the
  observer fires for all paths). Tests: existing
  `TestRedis_StreamXReadBlockWakesOnXAdd` still passes; new
  `TestRedis_StreamXReadBlockWakesOnFollowerApply` confirms a
  follower-side waiter wakes within ~µs of FSM apply (rather than
  ≤100ms via fallback); a Lua-script XADD test confirms the same.
- **Phase 2.3** — Generalize the registry into a `keyWaiterRegistry`
  shared by XREAD, BZPOPMIN (Phase C), BLPOP / BRPOP / BLMOVE. Each
  command's adapter handler picks the right key-prefix filter on
  registration; the observer remains a single subscriber.

## Risks

1. **Apply-loop overhead** — Adding an inline call per mutation must not
   regress write throughput. Phase 2.1 ships a benchmark gate; if the
   observer's cost is non-trivial we move to a buffered channel + a
   goroutine fan-out (one per observer), accepting up to one channel
   send of latency.
2. **Observer-side panic crashes the apply loop** — The observer call
   is wrapped in a `defer recover()` that logs and drops; an observer
   panic must never poison the FSM.
3. **Snapshot restore reapplies entries** — `kvFSM.Restore` does not go
   through `Apply`, so observers see no events during a restore. Document
   this; the only impact is that a BLOCK waiter pre-existing the snapshot
   would have to wait for the next post-restore mutation. In practice
   restores happen during startup before clients connect.
4. **Memory leak via observer references** — If an observer holds a
   reference to the FSM (e.g. via the registry holding the RedisServer),
   construction order matters. Phase 2.2 wires the observer last, after
   both the FSM and the RedisServer are fully constructed.

## Self-review (CLAUDE.md 5 lenses)

1. **Data loss** — None. Observers are read-only; the FSM apply path is
   unchanged in its mutation semantics.
2. **Concurrency** — Observers run inline on the apply goroutine. Their
   internal locking (e.g. `streamWaiterRegistry.mu`) is brief; the
   non-blocking-send signal pattern guarantees apply never stalls on a
   slow waiter.
3. **Performance** — Sub-100ns per mutation when no observer is wired;
   sub-µs when the stream observer fires (one prefix check + one
   `IsStreamEntryKey` + one map lookup + one non-blocking send).
4. **Data consistency** — Observers fire **after** apply succeeds, so a
   waiter woken by the observer can re-read at the next readTS and is
   guaranteed to see the entry. There is no race where the signal
   precedes visibility.
5. **Test coverage** — Phase 2.1: observer interface tests + apply-loop
   benchmark. Phase 2.2: end-to-end XREAD-on-follower test (a leader
   XADD, a XREAD BLOCK on a follower waking within ~µs); Lua-script
   XADD test (a XREAD on the leader waking from a Lua-driven XADD on
   the same leader within ~µs).
