# Centralized Timestamp Oracle (TSO) Design

- Status: Partial — M1 all-led-group HLC renewal, the M2 reserved-group
  bootstrap bridge, the M3-M5 TSO allocator/batch cutover, and the minimal
  TSO-only FSM are implemented; group-0 timestamp issuance, follower
  redirect/admin exposure, and shadow validation remain open
- Author: bootjp
- Date: 2026-04-16
- Updated: 2026-07-23

---

## 1. Background and Motivation

### 1.0 Implementation Status

Implemented:

1. `ShardedCoordinator.RunHLCLeaseRenewal` renews every led shard group.
2. `HLC.NextBatchFenced` atomically reserves consecutive timestamp windows while enforcing the physical-ceiling fence.
3. `LocalTSOAllocator` implements `TSOAllocator` on top of a leader coordinator and its shared `HLC`.
4. `BatchAllocator` caches immutable timestamp windows and uses lock-free atomic slot claims on the hot path.
5. `Coordinate` and `ShardedCoordinator` can route every coordinator-owned
   persistence timestamp through a `TimestampAllocator`, covering raw writes,
   transaction `startTS`/`commitTS`, sharded raw writes, and `DEL_PREFIX`
   broadcasts.
6. Adapter-side persistence timestamp helpers (`NextTimestampThrough` /
   `NextTimestampAfterThrough`) route DynamoDB item-write retries, S3
   transaction timestamps, Redis delta compaction commits, and internal
   forwarded request stamping through the same optional coordinator allocator
   while preserving the legacy `Clock().NextFenced()` fallback for older test
   coordinators.
7. `main.go` exposes the default-off `--tsoEnabled` / `--tsoBatchSize` bridge,
   wiring a `LocalTSOAllocator` through `BatchAllocator` so production
   coordinators can cut over to the default-group-backed TSO path without
   changing the legacy HLC default.
8. `groupID = 0` is reserved for the dedicated TSO group in runtime config:
   shard ranges cannot route user data to it, the default data group skips it,
   and SQS FIFO partition maps cannot route queue partitions to it. Existing
   `--raftGroups` / `--raftGroupPeers` bootstrap the group as a compatibility
   bridge, and the all-led-group HLC renewal loop keeps its ceiling warm
   alongside data groups. Adding only group 0 to an existing single-data-group
   deployment keeps the data group on its legacy `base/raftID` directory while
   isolating group 0 under `base/raftID/group-0`. The current `--tsoEnabled`
   bridge still issues timestamps from the locally led data shard through
   `LocalTSOAllocator`; pinning timestamp issuance to group 0 remains deferred
   until the TSO-leader redirect path exists.
9. `TSOStateMachine` implements a TSO-only Raft FSM that accepts HLC lease
   entries, snapshots exactly the physical ceiling as eight big-endian bytes,
   restores with malformed-input rejection, and classifies HLC lease entries
   as volatile-only for safe cold-start replay. This removes the requirement
   for group 0 to carry the KV FSM's data store once startup wiring switches
   to the dedicated FSM.

Remaining:

1. Wire group 0 startup to `TSOStateMachine` instead of the compatibility KV
   FSM and pin production timestamp issuance to the group-0 leader.
2. Add follower redirect/admin exposure for the dedicated TSO leader.
3. Add Phase B shadow-read validation before making dedicated TSO the only
   production timestamp path.

### 1.1 Original Limitation

Before M1, `ShardedCoordinator.RunHLCLeaseRenewal` proposed HLC lease renewals
only to the `defaultGroup` Raft group. In a sharded deployment where shard
groups are distributed across different nodes, this created a correctness gap:

```
Node A: leader of Group-1 (defaultGroup) + member of Group-2
Node B: leader of Group-2, NOT a member of Group-1

Node A: leads defaultGroup → ceiling updated correctly ✅
Node B: not in defaultGroup → ceiling never updated ❌
         ceiling stays at 0 → timestamps derived from raw wall clock
         → may collide with the previous leader's committed window
```

M1 fixes that per-node renewal gap by proposing to every group this node
currently leads. **Global timestamp monotonicity is still not guaranteed when
different coordinators on different nodes allocate timestamps for cross-group
work**; that is the dedicated TSO / single-oracle work left open by M2-M7.

### 1.2 Near-Term Workaround

Propose a lease renewal for every group the node currently leads (see
Section 6). This eliminates the per-node gap but leaves a deeper architectural
limitation: timestamp issuance is still scattered across shard leaders, making
it difficult to enforce a single global ordering without coordination.

### 1.3 Long-Term Goal

Introduce a dedicated **Timestamp Oracle (TSO)** component, modelled after
TiDB's PD TSO module, that serves as the single authoritative source of
globally monotonic timestamps for all shard coordinators.

---

## 2. Reference: TiDB PD TSO Architecture

```
Client ──────────────────────────────► PD Leader
              GetTS()                       │
                                    ┌───────▼───────┐
                                    │   TSO module   │
                                    │  physical:     │
                                    │   Raft-agreed  │
                                    │   ceiling      │
                                    │  logical:      │
                                    │   in-memory    │
                                    └───────┬───────┘
                                            │
                              Raft log: [ceiling entry]
                                            │
                                   ┌────────▼────────┐
                                   │  PD Followers    │
                                   └─────────────────┘
```

Key properties:
- A **dedicated Raft group** (the PD cluster) manages the TSO.
- The physical part (`now + 3 s`) is committed to the Raft log and renewed
  every second by the leader.
- The logical part is a pure in-memory counter; no Raft round-trip is needed
  to increment it.
- **Batch allocation**: one `GetTS()` RPC can return a window of consecutive
  timestamps, amortising the Raft latency across many transactions.
- Only the PD leader issues timestamps; followers redirect.

---

## 3. Proposed Design: `TSOAllocator`

### 3.1 Component Overview

```
┌────────────────────────────────────────────────────────┐
│                     elastickv node                      │
│                                                         │
│  ┌──────────────┐    ┌───────────────────────────────┐ │
│  │ ShardedCoord │───►│         TSOAllocator           │ │
│  │              │    │                               │ │
│  │  startTS =   │    │  ┌────────┐   ┌────────────┐ │ │
│  │  tso.Next()  │    │  │  HLC   │   │ Raft Engine│ │ │
│  │              │    │  │ceiling │   │  (TSO grp) │ │ │
│  └──────────────┘    │  └───▲────┘   └─────┬──────┘ │ │
│                      │      └───────────────┘        │ │
│  ┌──────────────┐    │   FSM applies ceiling to HLC   │ │
│  │  Coordinate  │───►│                               │ │
│  └──────────────┘    └───────────────────────────────┘ │
└────────────────────────────────────────────────────────┘
```

### 3.2 Interface

```go
// TSOAllocator issues globally monotonic timestamps backed by a dedicated
// Raft group. It is the single source of truth for all shard coordinators.
type TSOAllocator interface {
    // Next returns the next globally unique, monotonically increasing timestamp.
    // Blocks until this node is (or becomes) the TSO leader, or ctx expires.
    Next(ctx context.Context) (uint64, error)

    // NextBatch returns the base of a window of n consecutive timestamps:
    //   [base, base+1, ..., base+n-1]
    // More efficient than calling Next n times for bulk operations.
    NextBatch(ctx context.Context, n int) (base uint64, err error)

    // IsLeader reports whether this node is the current TSO leader.
    IsLeader() bool

    // RunLeaseRenewal runs the background ceiling-renewal loop.
    // Blocks until ctx is cancelled; call in a goroutine.
    RunLeaseRenewal(ctx context.Context)
}
```

### 3.3 Dedicated TSO Raft Group

```
elastickv cluster (3 nodes)

Node-1 ──┐
Node-2 ──┼──► TSO Raft Group  (groupID = 0, reserved)
Node-3 ──┘         │
                   Raft log entries:
                   [0x02][ceiling_ms: T+3000]  ← initial
                   [0x02][ceiling_ms: T+3000]  ← renewed every 1 s
```

- `groupID = 0` is reserved for the TSO group; all user-data shards use
  `groupID >= 1`.
- The TSO FSM applies only HLC lease entries. It carries no key-value storage,
  so compaction and snapshot overhead are negligible.
- Membership mirrors the full cluster so any node can become TSO leader.

### 3.4 TSO FSM

```go
// TSOStateMachine implements raftengine.StateMachine. It is a minimal FSM
// that only tracks the HLC physical ceiling; its entire persisted state is a
// single int64 ceiling value.
//
// Interface mapping (raftengine.StateMachine vs raw raft.FSM):
//   Apply([]byte)             ← engine strips log envelope, passes raw payload
//   Snapshot() (Snapshot, _) ← returns raftengine.Snapshot (WriteTo/Close)
//   Restore(io.Reader)        ← caller owns and closes the reader
type TSOStateMachine struct {
    hlc *HLC
}

// Apply processes an HLC lease entry. The engine strips the raft.Log
// envelope and passes only the raw command bytes, matching the
// raftengine.StateMachine.Apply([]byte) signature.
func (f *TSOStateMachine) Apply(data []byte) any {
    if len(data) == 0 || data[0] != raftEncodeHLCLease {
        return nil
    }
    return f.applyHLCLease(data[1:])
}

// Snapshot serialises the ceiling as 8 big-endian bytes.
// Returns raftengine.Snapshot (WriteTo/Close) rather than raft.FSMSnapshot.
func (f *TSOStateMachine) Snapshot() (raftengine.Snapshot, error) {
    var ceiling int64
    if f.hlc != nil {
        ceiling = f.hlc.PhysicalCeiling()
    }
    return &tsoSnapshot{ceiling: ceiling}, nil
}

// Restore deserialises the ceiling and updates the shared HLC.
// The caller owns the reader and is responsible for closing it.
func (f *TSOStateMachine) Restore(r io.Reader) error {
    var buf [8]byte
    if _, err := io.ReadFull(r, buf[:]); err != nil {
        return err
    }
    ceiling := int64(binary.BigEndian.Uint64(buf[:]))
    if f.hlc != nil && ceiling > 0 {
        f.hlc.SetPhysicalCeiling(ceiling)
    }
    return nil
}

// tsoSnapshot implements raftengine.Snapshot. It serialises the HLC physical
// ceiling as 8 big-endian bytes — the entire persisted state of the TSO group.
// WriteTo/Close matches the raftengine.Snapshot interface (not Persist/Release
// from raft.FSMSnapshot).
type tsoSnapshot struct {
    ceiling int64
}

func (s *tsoSnapshot) WriteTo(w io.Writer) (int64, error) {
    var buf [8]byte
    binary.BigEndian.PutUint64(buf[:], uint64(s.ceiling))
    n, err := w.Write(buf[:])
    return int64(n), err
}

func (s *tsoSnapshot) Close() error { return nil }
```

### 3.5 Batch Allocator

```
┌──────────────────────────────────────────────────────┐
│  BatchAllocator                                        │
│                                                        │
│  current window: atomic.Pointer[windowSnapshot]        │
│  hot path:       atomic Add on offset inside immutable struct │
│                                                        │
│  when offset >= size:                                  │
│    → call TSOAllocator.NextBatch() for a new window    │
│    → single Raft round-trip amortised over batchSize   │
└──────────────────────────────────────────────────────┘
```

The hot path is **lock-free** via `atomic.Pointer[windowSnapshot]`.
Each `windowSnapshot` is an **immutable** struct published atomically on refill:
`base` is never written after the pointer is stored, so reading `w.base` after
claiming a slot via `w.offset.Add(1)` is always safe — there is no window where
an old offset could be combined with a new base.

The hot path uses `w.offset.Add(1)` rather than a CAS loop. Under high
contention, `Add` is more efficient: every goroutine obtains a unique offset
in a single atomic operation with no retry, whereas a CAS loop retries until
it wins. Goroutines that receive an out-of-bounds offset (≥ size) fall through
to the slow path; they hold a pointer to the old struct, which is safe because
the struct is never mutated after publish.

This eliminates the split-atomic race that would arise from storing `batchBase`
and `offset` as two separate atomics: a goroutine that loads the new `batchBase`
before the old `offset` is overwritten would silently return an out-of-range
timestamp.

The slow path (window exhausted) uses a lightweight mutex **only** for refill
coordination. The network call to `NextBatch` is always made outside the lock,
so I/O never blocks concurrent callers. Goroutines that still hold a pointer to
the old `windowSnapshot` continue to use it safely — the struct is never
mutated after it is published.

```go
// windowSnapshot is an immutable description of one timestamp batch window.
// base is set once when the struct is created and never modified; it is safe
// to read concurrently with atomic Add operations on offset.
type windowSnapshot struct {
    base uint64          // first timestamp in this window (immutable after publish)
    size int             // number of slots in this window (immutable after publish)
    offset atomic.Uint64 // slot counter; callers do Add(1) to claim a slot
}

// BatchAllocator pre-fetches a window of timestamps from the TSO leader and
// serves them locally without a Raft round-trip until the window is exhausted.
// This reduces per-transaction TSO latency from ~1 ms (one Raft RTT) to ~1 µs.
//
// Concurrency model:
//   Hot path  – lock-free Add(1) on w.offset inside an immutable windowSnapshot;
//               no mutex acquired on the common case; no CAS retry loop.
//   Slow path – mutex guards refill handoff only; I/O is always outside it.
//               Goroutines that loaded the old window pointer before the swap
//               continue using it safely (the struct is never mutated).
type BatchAllocator struct {
    tso       TSOAllocator
    batchSize int

    // win holds the current batch window. A new *windowSnapshot is stored
    // atomically on every refill; the old pointer remains valid for any
    // goroutine that loaded it before the swap.
    win atomic.Pointer[windowSnapshot]

    mu         sync.Mutex   // guards refill coordination only, not the hot path
    refillDone chan struct{} // non-nil while a refill is in progress; closed on completion
}

func NewBatchAllocator(tso TSOAllocator, batchSize int) *BatchAllocator {
    if tso == nil {
        panic("tso must not be nil")
    }
    if batchSize <= 0 {
        panic("batchSize must be positive")
    }
    return &BatchAllocator{tso: tso, batchSize: batchSize}
    // win is nil; the first Next() call triggers an immediate refill.
}

func (b *BatchAllocator) Next(ctx context.Context) (uint64, error) {
    for {
        if err := ctx.Err(); err != nil {
            return 0, err
        }

        // Fast path: claim a slot in the current window via atomic Add (lock-free).
        // We load the pointer once; Add and base read operate on the *same* struct,
        // so base cannot change after the Add — it is immutable for the window's
        // lifetime. Add(1) is preferred over CAS because it never retries: every
        // caller receives a unique offset in one atomic operation.
        if w := b.win.Load(); w != nil {
            off := w.offset.Add(1) - 1 // claim slot; off is the 0-based index
            if off < uint64(w.size) {
                return w.base + off, nil // base is immutable; safe to read here
            }
            // off >= size: this window is exhausted; fall through to slow path.
        }

        // Slow path: window is nil or exhausted; trigger or await a refill.
        b.mu.Lock()
        // Re-check under lock: another goroutine may have installed a new window.
        if w := b.win.Load(); w != nil && w.offset.Load() < uint64(w.size) {
            b.mu.Unlock()
            continue
        }
        if b.refillDone != nil {
            // A refill is already in flight; wait without holding the lock.
            ch := b.refillDone
            b.mu.Unlock()
            select {
            case <-ch:
                continue
            case <-ctx.Done():
                return 0, ctx.Err()
            }
        }

        // This goroutine is responsible for the refill.
        ch := make(chan struct{})
        b.refillDone = ch
        b.mu.Unlock()

        // Network I/O: no lock held.
        //
        // The deferred closure guarantees that refillDone is cleared and ch is
        // closed regardless of how this goroutine exits — including a panic in
        // NextBatch. Without the defer, a panic would leave refillDone non-nil
        // and ch unclosed, permanently deadlocking every goroutine waiting on it.
        //
        // The window is published inside the defer, under the mutex, before ch
        // is closed, so waiters that wake on <-ch are guaranteed to see the new
        // window on their next fast-path attempt.
        // success is set to true only after NextBatch returns without error.
        // Using an explicit flag (rather than checking retErr == nil in the defer)
        // guards against the panic case: if NextBatch panics, retErr is still nil
        // (zero value) but success remains false, so no zero-base window is published.
        var (
            newBase uint64
            success bool
        )
        err := func() (retErr error) {
            defer func() {
                b.mu.Lock()
                if success {
                    // Publish a fresh immutable window. Any goroutine that loaded
                    // the previous pointer before this Store continues to use it
                    // safely — the old struct is never mutated after publish.
                    b.win.Store(&windowSnapshot{base: newBase, size: b.batchSize})
                }
                b.refillDone = nil // clear under lock so waiters retry cleanly
                b.mu.Unlock()
                close(ch) // unblock all waiters after window is visible
            }()
            newBase, retErr = b.tso.NextBatch(ctx, b.batchSize)
            success = (retErr == nil)
            return
        }()

        if err != nil {
            return 0, err
        }
    }
}
```

---

## 4. Data Flow

### 4.1 Normal Write (single shard)

```
Client
  │  Put(k, v)
  ▼
ShardedCoordinator.Dispatch()
  │
  ├─ startTS = tso.Next()        ← served from local batch window (no RTT)
  │
  ├─ Propose to Shard-N Raft     ← write to the shard that owns the key
  │
  └─ return CommitIndex
```

### 4.2 TSO Ceiling Renewal (every 1 s)

```
TSO Leader
  │
  ├─ ceilingMs = now + 3000 ms
  │
  ├─ Propose([0x02][ceilingMs]) to TSO Raft group
  │
  └─ TSO FSM.Apply() on all TSO members
       → HLC.SetPhysicalCeiling(ceilingMs)
         ↳ shared HLC ceiling updated on every node ✅
```

### 4.3 Leader Failover

```
TSO leader crashes
  │
  ▼
New TSO leader elected via Raft
  │
  ├─ FSM.Restore() or Raft log replay
  │    → physicalCeiling restored to the last committed value
  │
  └─ HLC.Next() uses max(now, physicalCeiling)
       → new leader issues timestamps strictly above the old leader's window ✅
```

---

## 5. Comparison: Current vs. Proposed

| Aspect | Current implementation | Proposed TSO |
|--------|----------------------|--------------|
| Ceiling management | Each shard leader proposes to its own group | Dedicated TSO Raft group manages ceiling centrally |
| Cross-shard monotonicity | Every led group renews its local shared HLC ceiling, but no single global oracle yet | TSO group updates all nodes → **guaranteed** |
| Timestamp issuance | `ShardedCoordinator` (per shard leader) | `TSOAllocator` (TSO leader only) |
| Latency (with batch) | 0 Raft RTT (ceiling floor only) | ~0 (served from local batch window) |
| Latency (without batch) | 0 Raft RTT | 1 Raft RTT per `NextBatch()` call |
| Raft groups | shard count only | shard count + 1 (TSO group) |
| Single point of failure | None | TSO leader (fault-tolerant via Raft) |

---

## 6. Near-Term Fix (addresses Gemini review finding)

Before the full TSO is introduced, `RunHLCLeaseRenewal` iterates over
**all** shard groups rather than only `defaultGroup`. Because all FSMs on a
node share the same `*HLC` instance, a ceiling committed to any group advances
the node-wide clock floor and protects timestamps issued by that node.

Proposals to different Raft groups are independent and must be issued
**in parallel**. A sequential loop would serialize blocking `Propose` calls:
if one group's Raft quorum is slow (e.g. during a leader election), later
groups would not receive their ceiling update until the slow call returns,
potentially allowing `hlcRenewalInterval` to expire before all groups are
updated.

```go
// RunHLCLeaseRenewal proposes a ceiling renewal to every shard group this
// node currently leads. Proposals are fire-and-forget goroutines: the main
// loop never waits for them, so a slow or unresponsive Raft group cannot
// delay the next renewal cycle or block graceful shutdown.
//
// Implementation notes:
//   - c.groups is populated at construction and never mutated afterwards,
//     so iteration without a lock is safe. If dynamic shard membership is
//     added in the future, this loop must be protected by a sync.RWMutex.
//   - Each goroutine uses a per-proposal timeout (hlcRenewalInterval) to
//     bound its lifetime. In steady state the number of in-flight goroutines
//     is at most len(c.groups), which is typically small (O(10)).
func (c *ShardedCoordinator) RunHLCLeaseRenewal(ctx context.Context) {
    timer := time.NewTimer(hlcRenewalInterval)
    defer timer.Stop()
    for {
        select {
        case <-timer.C:
            ceilingMs := time.Now().UnixMilli() + hlcPhysicalWindowMs
            payload := marshalHLCLeaseRenew(ceilingMs)
            for gid, group := range c.groups {
                if group.Engine == nil || group.Engine.State() != raftengine.StateLeader {
                    continue
                }
                go func(gid uint64, group *ShardGroup) {
                    // Bound each proposal to one renewal interval so that
                    // goroutines from slow or partitioned groups do not
                    // accumulate indefinitely and exhaust resources.
                    pctx, cancel := context.WithTimeout(ctx, hlcRenewalInterval)
                    defer cancel()
                    if _, err := group.Proposer().Propose(pctx, payload); err != nil &&
                        !errors.Is(err, context.Canceled) &&
                        !errors.Is(err, context.DeadlineExceeded) {
                        // Suppress context-cancellation noise: these are
                        // expected on shutdown or proposal timeout and do not
                        // indicate a problem with the Raft group.
                        c.logger().WarnContext(ctx, "hlc lease renewal failed",
                            slog.Uint64("group_id", gid),
                            slog.Int64("ceiling_ms", ceilingMs),
                            slog.Any("err", err),
                        )
                    }
                }(gid, group)
            }
            timer.Reset(hlcRenewalInterval)
        case <-ctx.Done():
            return
        }
    }
}
```

**Guarantee:** A node that leads Group-B writes the ceiling to Group-B's Raft
log. Once applied, all Group-B members (which share the same `*HLC`) have their
ceiling updated. Monotonicity across nodes that lead *different* groups is not
fully guaranteed without a shared TSO, but **all timestamps issued by a single
node are strictly monotonic**.

---

## 7. Zero-Downtime Migration Strategy

Migrating from the current per-shard ceiling model to a centralized TSO must
not interrupt writes or violate timestamp monotonicity. The following phased
approach enables a live cutover.

### 7.1 Phase A — Dual-Write Bridge (no cutover risk)

```
┌──────────────────────────────────────────────────────────┐
│  ShardedCoordinator                                        │
│                                                            │
│  startTS = legacyHLC.Next()  (existing path, unchanged)   │
│                                                            │
│  RunHLCLeaseRenewal():                                     │
│    ├─ propose to all shard groups (M1 fix, Section 6)      │
│    └─ also propose to TSO group (new, write-only)          │
│       ↳ TSO FSM advances its ceiling in parallel           │
└──────────────────────────────────────────────────────────┘
```

- The TSO group receives ceiling proposals but **no reads are served from it**.
- This allows TSO FSM state to warm up and be validated in production before
  the cutover.
- Rollback: stop proposing to the TSO group; no state change on data path.

### 7.2 Phase B — Shadow Read Validation

- Both `legacyHLC.Next()` and `tso.Next()` are called per transaction.
- Results are compared in a shadow log; divergences are alerted but the legacy
  value is used.
- This phase validates that the TSO ceiling is always ≥ the legacy ceiling.

### 7.3 Phase C — TSO Cutover (feature flag)

- A runtime feature flag (`tso.enabled`) switches `startTS` to `tso.Next()`.
- The flag can be toggled per-node via config reload (no process restart).
- Because the TSO ceiling was kept ≥ the legacy ceiling throughout Phase A/B,
  there is no timestamp regression at the moment of cutover.
- Rollback: flip the flag back; the legacy HLC has been monotonically advancing
  in parallel so it remains safe to resume.

### 7.4 Phase D — Legacy Cleanup

- Remove per-shard ceiling proposals.
- Remove `legacyHLC` from `ShardedCoordinator`.
- Remove the shadow comparison code.

### 7.5 Monotonicity Invariant Across Phases

At every phase boundary, the following invariant must hold:

```
tso_ceiling ≥ max(ceiling committed by any shard group leader)
```

This is enforced by Phase A's dual-write: every ceiling update that reaches
a shard group also reaches the TSO group, so the TSO ceiling is always at
least as large as the maximum shard ceiling.

---

## 8. Milestones

| Phase | Scope | Priority |
|-------|-------|----------|
| M1 — shipped | Extend `RunHLCLeaseRenewal` to all shard groups with parallel proposals (Section 6) | High |
| M2 — shipped for reserved group 0 | Phase A dual-write bridge: when group 0 is configured, all-led HLC renewal also proposes ceiling updates to it while shard range validation prevents user data routes to group 0 (Section 7.1) | High |
| M3 — shipped | Define `TSOAllocator` interface; implement backed by `defaultGroup` | Medium |
| M4 — shipped | `BatchAllocator` with atomic counter for low-latency timestamp serving | Medium |
| M5 — shipped for default-group bridge | Coordinator feature-flag cutover via `--tsoEnabled`; shadow validation against a dedicated group remains deferred to M6 | Medium |
| M6 — partial | Dedicated TSO Raft group (`groupID = 0`) is reserved/bootstrap-capable and warmed by the HLC renewal bridge; minimal `TSOStateMachine` is implemented; TSO-leader-only timestamp issuance remains open | Low |
| M7 | Phase D legacy cleanup + cross-shard SSI read-timestamp validation via TSO | Low |

---

## 9. Open Questions

1. **TSO RTT when TSO leader ≠ write leader:** What batch size minimises tail
   latency? Needs benchmarking against realistic write fan-out.

2. **TSO group membership:** Should all cluster nodes join the TSO group, or
   should a dedicated subset (e.g. 3 out of N) be used to reduce Raft traffic?

3. **Clock floor semantics:** `max(now, ceiling)` vs. `ceiling + 1` — the
   stricter form (`ceiling + 1`) guarantees no overlap even if wall clocks
   drift, at the cost of one extra millisecond per renewal window.

4. **Non-leader TSO requests:** Should follower nodes redirect to the TSO
   leader via gRPC, or support follower reads with a known-safe timestamp
   bound?

5. **Backward compatibility:** Existing snapshots and Raft logs store ceiling
   values inline in shard FSMs. Migration to a dedicated TSO FSM requires a
   coordinated snapshot + compaction pass.
