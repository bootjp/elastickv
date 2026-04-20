# Lease Read Design

Status: Proposed
Author: bootjp
Date: 2026-04-20

---

## 1. Background

### 1.1 Current read paths

elastickv has three read paths with different consistency mechanisms:

| Path | Read fence | Quorum cost per read |
|---|---|---|
| DynamoDB getItem / query / scan | `snapshotTS()` only | 0 |
| Redis SET / GET / non-Lua commands | `snapshotTS()` only | 0 |
| Redis Lua EVAL / EVALSHA | `coordinator.LinearizableRead(ctx)` once at script start (PR #546) | 1 ReadIndex per script |

`snapshotTS()` returns `store.LastCommitTS()` from the local FSM. It does not
verify leadership at read time.

### 1.2 Observed problem

After deploying the redis.call() optimization (PR #547 + #548), per-script
latency did not improve. Investigation showed:

- `redis.call()` time accounts for ~100% of Lua VM time
- Average time per `redis.call()` invocation is 800 ms - 2.2 s
- `Raft Commit Time` is ~500 us (not the bottleneck)
- Single-key `SET x` and `GET x` from redis-cli take ~0.96 s and ~0.92 s

Two distinct issues are hidden in the metric:

1. The per-script `LinearizableRead` in `newLuaScriptContext` triggers a full
   etcd/raft `ReadOnlySafe` ReadIndex (heartbeat broadcast + quorum
   `MsgHeartbeatResp` wait) on every Lua script invocation. There is no term
   cache or fast path in `submitRead` / `handleRead`
   (`internal/raftengine/etcd/engine.go:524, :841`).
2. Independently, the recent change to `defaultHeartbeatTick = 10` and
   `defaultElectionTick = 100` (PR #529) widened the worst-case Raft tick gap
   from 10 ms to 100 ms, slowing operations that wait on Raft progress.

This document addresses (1). Issue (2) is out of scope.

### 1.3 Asymmetry to remove

The current state is asymmetric:

- DynamoDB and Redis non-Lua paths trust local state with no quorum check.
  This already accepts a partition window of up to `electionTimeout` during
  which a stale leader can serve stale reads (until `CheckQuorum` steps it
  down).
- Lua paths pay a full quorum round-trip on every script.

A unified lease-read API can give all three paths the same trade-off:
serve from local state when leadership is recently confirmed, fall back to
ReadIndex when it is not.

---

## 2. Goals and Non-Goals

### 2.1 Goals

- Eliminate the per-script ReadIndex from the Lua path under steady load.
- Provide a single API used by all read paths, so the consistency trade-off
  is documented in one place.
- Improve safety of DynamoDB / Redis non-Lua reads by attaching them to a
  bounded lease, instead of unconditional trust of local state.
- Keep the change confined to elastickv layers; no fork of etcd/raft.

### 2.2 Non-Goals

- Strict linearizability under arbitrary network partitions. The design
  retains a partition window of at most `electionTimeout - margin`, the same
  trade-off TiKV's lease read accepts.
- Changes to `ReadOnlyOption` in etcd/raft. The fast path lives in elastickv;
  the slow path still uses `ReadOnlySafe`.
- Multi-shard read transactions. Lease check is per-shard.

---

## 3. Design

### 3.1 Lease state

Each `Coordinate` (single-shard) and each shard inside `ShardedCoordinator`
holds:

```go
type leaseState struct {
    gen    atomic.Uint64                // bumped by invalidate()
    expiry atomic.Pointer[time.Time]    // nil = expired / invalidated
}
```

- `expiry == nil` or `time.Now() >= *expiry`: lease is expired. The next
  `LeaseRead` falls back to `LinearizableRead` and refreshes the lease on
  success.
- `time.Now() < *expiry`: lease is valid. `LeaseRead` returns immediately
  without contacting the Raft layer.
- `invalidate()` increments `gen` before clearing `expiry`. `extend()`
  captures `gen` at entry and, after its CAS lands, undoes its own
  write (via CAS on the pointer it stored) iff `gen` has moved. This
  prevents a Dispatch that succeeded just before a leader-loss
  invalidate from resurrecting the lease milliseconds after it was
  cleared. A fresh `extend()` that captured the post-invalidate
  generation is left intact because it stored a different pointer.

The lock-free form lets readers do one atomic load + one wall-clock compare
on the fast path.

### 3.2 Lease duration

The lease duration must be strictly less than `electionTimeout`. The bound
comes from etcd/raft: with `CheckQuorum: true`, a leader that loses contact
with majority steps down within at most `electionTimeout`. Until then, it can
still serve reads from local state. As long as our lease expires before the
leader could realistically be replaced and accept new writes elsewhere, local
reads are safe.

The engine exposes:

```go
func (e *Engine) LeaseDuration() time.Duration
```

Implementation: `electionTimeout - leaseSafetyMargin`, where
`electionTimeout = defaultTickInterval * defaultElectionTick`. With current
config: `10ms * 100 - 300ms = 700 ms`.

`leaseSafetyMargin` (proposed: 300 ms) absorbs:

- Goroutine scheduling delay between heartbeat ack and lease refresh.
- Wall-clock skew between leader and the partition's new leader candidate.
- GC pauses on the leader.

The margin is conservative; reducing it shortens the post-write quiet window
during which lease reads still hit local state, at the cost of a smaller
safety buffer.

### 3.3 Refresh triggers

The lease is refreshed (set to `time.Now() + LeaseDuration()`) on:

1. Any successful `engine.LinearizableRead(ctx)` returning without error.
   The ReadIndex protocol confirmed quorum at that moment.
2. Any successful `engine.Propose(ctx, data)` whose result indicates commit.
   A committed entry implies majority append + ack, which is a stronger
   confirmation than ReadIndex.

Heartbeat ack tracking is intentionally not used. It would require deep
hooks into etcd/raft's internals and gives only a small marginal benefit
over (1) and (2).

### 3.4 Invalidation triggers

The lease is invalidated (set to nil) on:

1. State transition out of leader. `refreshStatus` fires registered
   `RegisterLeaderLossCallback` hooks on the `Leader -> non-Leader` edge,
   and `fail()` / `shutdown()` fire the same hooks when tearing down a
   node that was still leader, so the error-shutdown path does not leave
   lease holders serving stale state.
2. Any error returned by `engine.Propose` or `engine.LinearizableRead`
   from inside `LeaseRead` / `groupLeaseRead`. Implemented via
   `c.lease.invalidate()` on the slow-path error branch.
3. A no-op Raft commit (`resp.CommitIndex == 0`): the underlying
   `TransactionManager.{Commit,Abort}` can short-circuit on empty input
   or no-op abort without going through Raft. `leaseRefreshingTxn` and
   `Coordinate.Dispatch` only refresh the lease when `CommitIndex > 0`
   to avoid extending based on an operation that never reached quorum.

Note: the previous draft of this doc listed "term-change detection" as
a separate defensive trigger. That is not implemented; (1) covers the
only case term changes matter (an old leader being demoted), and
adding an explicit term check would be redundant.

### 3.5 API

```go
// internal/raftengine/engine.go — optional capability
type LeaseProvider interface {
    LeaseDuration() time.Duration
    AppliedIndex() uint64
    RegisterLeaderLossCallback(fn func())
}

// kv/coordinator.go
type Coordinator interface {
    // ...existing...
    LeaseRead(ctx context.Context) (uint64, error)
    LeaseReadForKey(ctx context.Context, key []byte) (uint64, error)
}
```

Returned index is the engine's applied index at the moment of return. Callers
that use `store.LastCommitTS()` can ignore the index; callers that need an
explicit fence can use it.

Pseudocode (matches `Coordinate.LeaseRead`; the sharded variant is the same
with per-shard `g.lease` and `g.Engine`):

```go
func (c *Coordinate) LeaseRead(ctx context.Context) (uint64, error) {
    lp, ok := c.engine.(raftengine.LeaseProvider)
    if !ok {
        return c.LinearizableRead(ctx) // hashicorp engine, test stubs
    }
    // Capture time.Now() exactly once. Reused for both the fast-path
    // validity check and (on the slow path) the lease-extension base:
    // a second sample would let the fast path accept a read whose
    // instant slightly exceeds the intended expiry boundary, while
    // also shortening the slow-path window by the same delta.
    now := time.Now()
    if c.lease.valid(now) {
        return lp.AppliedIndex(), nil
    }
    idx, err := c.LinearizableRead(ctx)
    if err != nil {
        c.lease.invalidate()
        return 0, err
    }
    // `now` was sampled strictly before LinearizableRead ran, so the
    // resulting lease window is strictly conservative (any real
    // quorum confirmation LinearizableRead witnessed happens at or
    // after `now`). extend uses a monotonic CAS to reject regressions.
    c.lease.extend(now.Add(lp.LeaseDuration()))
    return idx, nil
}
```

### 3.6 Application sites

| File | Current | After |
|---|---|---|
| `adapter/redis_lua_context.go:215` | `LinearizableRead` once per script | `LeaseRead` once per script |
| `adapter/redis.go` `get`, `keyTypeAt` callers | no fence | `LeaseRead` once per command |
| `adapter/redis.go` `set` and other write commands | no fence | implicit via `Propose` refresh |
| `adapter/dynamodb.go` `getItem`, `query`, `scan` | no fence | `LeaseRead` once per request |
| `adapter/dynamodb.go` write paths | no fence | implicit via `Propose` refresh |

For write paths, calling `LeaseRead` separately is not required: `Propose`
already confirms quorum at commit time and refreshes the lease.

For read paths, `LeaseRead` is added at the entry of the handler. The
existing `snapshotTS()` call is unchanged.

---

## 4. Safety

### 4.1 Correctness invariant

A lease read returning index `i` to a caller is safe iff, at the moment of
return, no other node holds a strictly higher commit index in a later term
that is visible to clients. Equivalently: this leader is still the unique
leader, and no replacement leader has accepted writes that this node has
not seen.

### 4.2 Why the lease bound is sufficient

etcd/raft with `CheckQuorum: true` enforces:

- A leader that fails to receive `MsgHeartbeatResp` from majority within
  `electionTimeout` steps down (becomes follower).
- A new leader cannot be elected until the previous leader's followers time
  out their election ticks, which is at least `electionTimeout`.

Combined: between losing quorum and a successor leader accepting writes,
at least `electionTimeout` of wall-clock time elapses on the followers'
clocks.

If the lease is refreshed at time `t0` (heartbeat ack received at `t0` is the
implicit refresh signal, modulo the margin discussion in 4.3), and the lease
duration is `electionTimeout - margin`, then the lease expires at
`t0 + electionTimeout - margin`. Any client read before that time runs on a
leader that, modulo clock skew bounded by `margin`, has not yet been
replaced.

### 4.3 Refresh-vs-ack gap

The design refreshes the lease on `LinearizableRead` and `Propose`
completion, not on individual heartbeat acks. This widens the gap between
the actual quorum confirmation event and the lease extension event by at
most one round-trip plus goroutine scheduling.

This gap is included in `leaseSafetyMargin`. Specifically, if the round-trip
plus scheduling delay is bounded by `D`, then `margin >= D + clock_skew_bound`
preserves the invariant.

### 4.4 Comparison to current state

| Path | Current safety window | After lease | Notes |
|---|---|---|---|
| DynamoDB / Redis non-Lua read | up to `electionTimeout` (until CheckQuorum step-down) | up to `LeaseDuration()` | strictly improved |
| Lua read | 0 (full ReadIndex per script) | up to `LeaseDuration()` | strictly weaker, matches the others |

The Lua change accepts the same trade-off the other paths already accept.

### 4.5 Read-then-write inside Lua

Lua scripts often read state, compute, then write. The write goes through
`Propose`, which requires quorum. A stale leader's `Propose` cannot commit
because it cannot reach majority (the quorum is on the other side of the
partition).

So a stale lease read inside a Lua script cannot directly cause a stale
write to commit. However:

- The script may decide to write based on a stale-but-not-divergent value
  (e.g. lost-update on a counter). This is the same hazard DynamoDB
  conditional writes face today.
- The client may receive a success response from the stale leader for the
  read portion before the write fails. With the current code, the entire
  script is wrapped in a single Raft proposal, so the script either commits
  atomically or returns an error. The lease change does not alter this.

### 4.6 Failure modes considered

- Leader losing quorum but lease still valid: stale read possible. Window
  is bounded by `LeaseDuration() < electionTimeout`. Any write attempt
  within the window fails because `Propose` cannot reach quorum. Same
  trade-off DynamoDB / Redis non-Lua already accept today.
- Leader losing quorum and lease expired: next `LeaseRead` calls
  `LinearizableRead`, which fails (no quorum), error propagated to caller.
  Lease invalidated.
- Leader transferring leadership: `refreshStatus` detects state transition
  out of leader and invalidates the lease.
- Clock skew exceeding `leaseSafetyMargin`: lease may extend beyond
  `electionTimeout`, allowing a stale read after a successor leader has
  accepted writes. Mitigation: keep `leaseSafetyMargin` larger than the
  documented clock-skew SLO of the deployment. Default 300 ms is consistent
  with the HLC physical window of 3 s used elsewhere.

---

## 5. Implementation Plan

### Phase 1: engine surface — DONE
1. Added `LeaseProvider` as a separate optional interface in
   `internal/raftengine/engine.go` (not on `LeaderView`) so non-etcd
   engines and test stubs can omit lease methods. Etcd engine implements
   `LeaseDuration() time.Duration` and `AppliedIndex() uint64`.
2. `RegisterLeaderLossCallback(fn func())` was added to `LeaseProvider`
   in the follow-up review pass; the etcd engine fires registered
   callbacks from `refreshStatus` whenever the local node leaves the
   leader role.

### Phase 2: coordinator lease — DONE
1. `leaseState` (lock-free `atomic.Pointer[time.Time]` with monotonic
   CAS extend) added to `Coordinate`; `ShardGroup` gets a per-shard
   `leaseState`.
2. `Coordinate.LeaseRead` / `Coordinate.LeaseReadForKey` and
   `ShardedCoordinator.LeaseRead` / `ShardedCoordinator.LeaseReadForKey`
   implemented. Time is sampled BEFORE the underlying
   `LinearizableRead` so the lease window starts at quorum
   confirmation.
3. `Coordinate.Dispatch` refreshes the lease on successful commit using
   the pre-dispatch timestamp. `ShardedCoordinator` wraps each
   `g.Txn` in `leaseRefreshingTxn` so all dispatch paths (raw via
   `router.Commit`, `dispatchSingleShardTxn`, `dispatchTxn` 2PC, and
   `dispatchDelPrefixBroadcast`) refresh the per-shard lease on
   `Commit` / `Abort` success.
4. `NewCoordinatorWithEngine` and `NewShardedCoordinator` register
   `lease.invalidate` via the `LeaseProvider.RegisterLeaderLossCallback`
   hook, so the engine's `refreshStatus` invalidates the lease the
   instant it observes a non-leader transition.

### Phase 3: callers — PARTIAL
1. DONE: `adapter/redis_lua_context.go:newLuaScriptContext` uses
   `LeaseRead` instead of `LinearizableRead`.
2. DONE for the highest-traffic single-key handlers; rest tracked as #557:
   - DONE: `adapter/redis.go` `get` (with bounded
     `redisDispatchTimeout` context).
   - DONE: `adapter/dynamodb.go` `getItem`.
   - DEFERRED (#557): `adapter/redis.go` `keys`, `exists`-family,
     ZSet/Hash/List/Set readers; `adapter/dynamodb.go` `query`, `scan`,
     `transactGetItems`, `batchGetItem`. Rely on the lease being kept
     warm by Lua scripts and successful Dispatch calls; safety identical
     to pre-PR (no quorum check).
3. No change to write paths beyond the implicit refresh via the
   `Coordinate.Dispatch` / `leaseRefreshingTxn` hooks.

### Phase 4: tests — PARTIAL
1. DONE: `kv/lease_state_test.go` covers `leaseState` extend, expire,
   invalidate, monotonic CAS, invalidate-vs-extend race.
2. DONE: `kv/lease_read_test.go` covers `Coordinate.LeaseRead` fast /
   slow / error / fallback paths and the leader-loss callback wiring.
   `kv/sharded_lease_test.go` covers `ShardedCoordinator` per-shard
   isolation and per-shard leader-loss wiring.
3. DONE: `TestCoordinate_LeaseRead_AmortizesLinearizableRead` proves
   100 LeaseRead calls inside one lease window trigger exactly 1
   underlying LinearizableRead. Stronger end-to-end Lua amortization
   under the adapter is implicit — `newLuaScriptContext` is the single
   call site and is exercised by every Lua test.
4. DEFERRED: Jepsen partition workload asserting no stale-read
   linearizability violation outside the lease window. Substantial
   scope; tracked separately. Existing Jepsen `redis-workload` already
   exercises the lease path under partition + kill faults, just without
   a lease-specific assertion.

### Phase 5: rollout
- Land Phases 1-3 behind no flag. The semantics are strictly equivalent or
  stronger than today for non-Lua paths and weaker (but documented) for Lua.
- Monitor `LinearizableRead` call rate and Lua per-script latency before
  and after deploy.

---

## 6. Alternatives Considered

### 6.1 Switch etcd/raft to ReadOnlyLeaseBased
One-line change: `ReadOnlyOption: etcdraft.ReadOnlyLeaseBased`. The leader
serves ReadIndex from local state without heartbeat broadcast, relying on
`CheckQuorum` for safety.

Rejected because:
- Lease semantics are implicit and tied to etcd/raft internals.
- The lease boundary is not surfaced to the elastickv layer, so we cannot
  track it for metrics or use it for non-engine reads.
- Future divergence: if elastickv ever needs to apply lease semantics to
  read paths that do not call into the engine, this option does not help.

The proposed design is essentially `ReadOnlyLeaseBased` reimplemented one
level up, with explicit timeout tracking.

### 6.2 Term cache only, no time bound
Cache the current term; skip ReadIndex if term has not changed.

Rejected because:
- Term changes are not the only safety trigger. A leader that loses quorum
  but has not yet stepped down keeps the same term while serving stale
  reads. CheckQuorum eventually catches it, but a term-only check has no
  bound on the stale-read window.
- The proposed lease design subsumes the term check: leader transition
  invalidates the lease, and time bounds the window even before a
  transition is detected.

### 6.3 Per-call heartbeat ack tracking inside engine
Hook into `handleHeartbeatResp` to refresh lease on every quorum ack.

Rejected for the initial implementation because:
- Requires deeper integration with etcd/raft message handling.
- The marginal latency benefit over `Propose`-driven refresh is small under
  any non-trivial write load.
- Can be added later without changing the API.

---

## 7. Open Questions

1. Should `LeaseDuration` be configurable per deployment, or kept as a
   derived constant? Proposal: derived constant, exposed as a method.
   Operators tune `defaultElectionTick` instead.
2. Should `LeaseRead` return the engine applied index or the store
   `LastCommitTS()`? Proposal: applied index (matches `LinearizableRead`),
   callers convert as needed.
3. Should a metric be added for lease hit/miss ratio? Proposal: yes,
   `elastickv_lease_read_total{outcome="hit|miss|error"}`.
