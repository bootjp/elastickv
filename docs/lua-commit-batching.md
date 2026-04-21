# Lua Commit Batching Architecture

## Summary

**One Redis EVAL / EVALSHA = one Raft proposal** for all writes performed by
`redis.call(...)` inside the script.

This document captures the current behaviour of the Redis-Lua adapter's commit
path, verified in April 2026 while investigating whether BullMQ-style scripts
(e.g. `moveJobFromPrioritizedToActive`, which performs 5-7 `redis.call`s
including `ZREM` + `LPUSH` + `HSET` + `HINCRBY`) were inflating Raft commit
traffic proportionally to the number of in-script mutations. They were not:
the batching is already in place.

The investigation was triggered by a production observation that Raft Commit
p99 increased from ~5s to ~10s after a deploy. The batching layer described
here is *not* the regression; look elsewhere (e.g. proposal size, raft tick
tuning, snapshot churn, storage latency).

## Call flow

For `EVAL`/`EVALSHA` on the adapter:

1. `(*RedisServer).eval` / `.evalsha` in `adapter/redis_lua.go` dispatch to
   `runLuaScript`.
2. `runLuaScript` constructs a single `luaScriptContext` per attempt and wraps
   the whole thing in `retryRedisWrite` (optimistic concurrency retry on
   `TxnLockedError` / "read timestamp has been compacted").
3. A `gopher-lua` `LState` runs the script. Inside the script, every
   `redis.call(...)` / `redis.pcall(...)` is routed through
   `luaRedisCommand` -> `luaScriptContext.exec` -> the command handler table
   `luaCommandHandlers` (`cmdZAdd`, `cmdHSet`, `cmdLPush`, `cmdHIncrBy`, ...).
4. **Every write command handler mutates only the in-memory state maps on
   `luaScriptContext` (`strings`, `lists`, `hashes`, `sets`, `zsets`,
   `streams`, `ttls`).** No handler calls `coordinator.Dispatch`, proposes to
   Raft, or otherwise crosses the storage/replication boundary for writes.
   Reads snapshot-read at the script's pinned `startTS`; within the script,
   subsequent reads observe prior in-script writes via the state maps (RYW is
   preserved without going through Raft).
5. After `state.PCall` returns, `runLuaScript` calls `scriptCtx.commit()`
   **exactly once**.
6. `commit()`
   (`adapter/redis_lua_context.go`, around line 3045) walks the sorted set of
   touched keys, builds `luaKeyPlan` elements for each (via
   `commitPlanForKey` -> `zsetCommitPlan` / `hashCommitElems` /
   `listCommitPlan` / `setCommitElems` / `stringCommitElems` / ...), appends
   the non-string `!redis|ttl|` index entries, and issues **one**
   `c.server.coordinator.Dispatch(dispatchCtx, &kv.OperationGroup[kv.OP]{ IsTxn: true, StartTS, CommitTS, Elems })`
   carrying the union of every key's elements across the whole script.

A `grep` over `adapter/redis_lua_context.go` confirms exactly one `Dispatch`
call site (in `commit()`), and exactly one `commitTS` is allocated per
script via `c.server.coordinator.Clock().Next()`.

## Why this already gives "1 Eval = 1 proposal"

- **No per-call proposals.** Nothing in the `cmd*` handlers writes through to
  storage or Raft. They only update `luaScriptContext`.
- **Atomicity / MULTI-EXEC semantics.** The single `Dispatch` with
  `IsTxn: true` submits the whole batch as one MVCC transaction at
  `CommitTS`. Partial failure rolls back the entire Eval.
- **MVCC timestamps.** `startTS` is captured by `newLuaScriptContext` (the
  read-pin is released on `Close`). `commitTS` is allocated in `commit()`
  just before `Dispatch`, so every element in the batch shares one commit
  timestamp.
- **Read-your-writes within the script.** Handlers read from the in-memory
  state maps before falling through to snapshot reads at `startTS`, so later
  `redis.call("ZRANGE", ...)` in the same script sees the `ZADD` from
  earlier in the same script without any Raft round-trip.
- **Retries.** `retryRedisWrite` rebuilds a fresh `luaScriptContext` and
  replays the whole script on retryable errors (write conflict, compacted
  read-ts). Each *successful* attempt corresponds to one proposal; failed
  attempts do not leave partial state committed.

## Non-Lua compatibility commands

`execLuaCompat` (used by `RENAME`, `LLEN`, `LREM`, `LPOS`, `LSET`,
`RPOPLPUSH`, `ZCOUNT`, `ZRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`,
`ZSCORE`, `ZPOPMIN`, `ZREMRANGEBYSCORE`, and the legacy SCARD/ZCARD blob
fallback) uses the same mechanism: build a fresh `luaScriptContext`, call
`scriptCtx.exec` once, then `scriptCtx.commit` once. This is always a single
proposal for a single command, by construction.

`LPOP` / `RPOP` take a different path (`execListPop` -> `listPopClaim`) for
O(1) conflict-free pops; that path is outside the Lua commit machinery and is
not relevant here.

## Relevant source locations

- `adapter/redis_lua.go`
  - `runLuaScript` (Eval/EvalSha entry, retry loop, observer reporting)
  - `execLuaCompat` (single-command path re-using the Lua commit machinery)
  - `luaRedisCommand` (routes `redis.call` to `luaScriptContext.exec`)
- `adapter/redis_lua_context.go`
  - `luaScriptContext` struct and the per-type state (`luaStringState`,
    `luaListState`, `luaHashState`, `luaSetState`, `luaZSetState`,
    `luaStreamState`, `luaTTLState`)
  - `newLuaScriptContext` / `Close` (lease-read + startTS pin + release)
  - `exec` (dispatch to `luaCommandHandlers`)
  - `commit` (the single `coordinator.Dispatch` site, ~L3084)
  - `commitPlanForKey`, `valueCommitPlan`, and the per-type
    `*CommitPlan` / `*CommitElems` helpers that assemble the shared `Elems`
    slice
- `adapter/redis_retry.go`
  - `retryRedisWrite` (optimistic-concurrency retry wrapping one full
    script attempt)

## Conclusion

No code change is required to batch in-script writes into a single Raft
proposal; this is the existing design. If a future workload (or a future
refactor) introduces a `coordinator.Dispatch` inside a `cmd*` handler, this
invariant will regress and should be caught in review. Protecting it with a
test-only assertion (e.g. count `Dispatch` calls during a representative
BullMQ script and assert `== 1`) would be a reasonable follow-up.
