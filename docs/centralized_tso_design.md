# Centralized Timestamp Oracle (TSO) Design

**Status:** Proposed  
**Author:** bootjp  
**Date:** 2026-04-16

---

## 1. Background and Motivation

### 1.1 Current Limitation

`ShardedCoordinator.RunHLCLeaseRenewal` currently proposes HLC lease renewals
only to the `defaultGroup` Raft group. In a sharded deployment where shard
groups are distributed across different nodes, this creates a correctness gap:

```
Node A: leader of Group-1 (defaultGroup) + member of Group-2
Node B: leader of Group-2, NOT a member of Group-1

Node A: leads defaultGroup → ceiling updated correctly ✅
Node B: not in defaultGroup → ceiling never updated ❌
         ceiling stays at 0 → timestamps derived from raw wall clock
         → may collide with the previous leader's committed window
```

As a result, **global timestamp monotonicity is not guaranteed when shard
groups span different nodes**.

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
// TSOStateMachine is a minimal FSM that only tracks the HLC physical ceiling.
// Its entire persisted state is a single int64 ceiling value.
type TSOStateMachine struct {
    hlc *HLC
}

func (f *TSOStateMachine) Apply(log *raft.Log) interface{} {
    if len(log.Data) == 0 || log.Data[0] != raftEncodeHLCLease {
        return nil
    }
    return f.applyHLCLease(log.Data[1:])
}

// Snapshot serialises the ceiling as 8 big-endian bytes.
func (f *TSOStateMachine) Snapshot() (raft.FSMSnapshot, error) {
    ceiling := f.hlc.PhysicalCeiling()
    return &tsoSnapshot{ceiling: ceiling}, nil
}

// Restore deserialises the ceiling and updates the shared HLC.
func (f *TSOStateMachine) Restore(r io.ReadCloser) error {
    defer r.Close()
    var buf [8]byte
    if _, err := io.ReadFull(r, buf[:]); err != nil {
        return err
    }
    ceiling := int64(binary.BigEndian.Uint64(buf[:]))
    f.hlc.SetPhysicalCeiling(ceiling)
    return nil
}
```

### 3.5 Batch Allocator

```
┌──────────────────────────────────────────────────────┐
│  BatchAllocator                                        │
│                                                        │
│  current window: [base ... base + batchSize)           │
│  next pointer:   atomic counter within window          │
│                                                        │
│  when next >= base + batchSize:                        │
│    → call TSOAllocator.NextBatch() for a new window    │
│    → single Raft round-trip amortised over batchSize   │
└──────────────────────────────────────────────────────┘
```

```go
// BatchAllocator pre-fetches a window of timestamps from the TSO leader and
// serves them locally without a Raft round-trip until the window is exhausted.
// This reduces per-transaction TSO latency from ~1 ms (one Raft RTT) to ~1 µs.
type BatchAllocator struct {
    tso       TSOAllocator
    batchSize int

    mu    sync.Mutex
    base  uint64
    limit uint64 // exclusive upper bound of the current window
}

func (b *BatchAllocator) Next(ctx context.Context) (uint64, error) {
    b.mu.Lock()
    defer b.mu.Unlock()
    if b.base >= b.limit {
        newBase, err := b.tso.NextBatch(ctx, b.batchSize)
        if err != nil {
            return 0, err
        }
        b.base = newBase
        b.limit = newBase + uint64(b.batchSize)
    }
    ts := b.base
    b.base++
    return ts, nil
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
| Cross-shard monotonicity | Only `defaultGroup` updated → **not guaranteed** | TSO group updates all nodes → **guaranteed** |
| Timestamp issuance | `ShardedCoordinator` (per shard leader) | `TSOAllocator` (TSO leader only) |
| Latency (with batch) | 0 Raft RTT (ceiling floor only) | ~0 (served from local batch window) |
| Latency (without batch) | 0 Raft RTT | 1 Raft RTT per `NextBatch()` call |
| Raft groups | shard count only | shard count + 1 (TSO group) |
| Single point of failure | None | TSO leader (fault-tolerant via Raft) |

---

## 6. Near-Term Fix (addresses Gemini review finding)

Before the full TSO is introduced, fix `RunHLCLeaseRenewal` to iterate over
**all** shard groups rather than only `defaultGroup`. Because all FSMs on a
node share the same `*HLC` instance, a ceiling committed to any group advances
the node-wide clock floor and protects timestamps issued by that node.

```go
// RunHLCLeaseRenewal proposes a ceiling renewal to every shard group this
// node currently leads. Because all FSMs on the node share a single HLC
// instance, any committed ceiling advances the node-wide clock floor,
// preventing timestamp collisions after a leader election.
func (c *ShardedCoordinator) RunHLCLeaseRenewal(ctx context.Context) {
    timer := time.NewTimer(hlcRenewalInterval)
    defer timer.Stop()
    for {
        select {
        case <-timer.C:
            ceilingMs := time.Now().UnixMilli() + hlcPhysicalWindowMs
            for gid, group := range c.groups {
                if group.Engine == nil {
                    continue
                }
                if group.Engine.State() != raftengine.StateLeader {
                    continue
                }
                if _, err := group.Engine.Propose(ctx, marshalHLCLeaseRenew(ceilingMs)); err != nil {
                    c.logger().WarnContext(ctx, "hlc lease renewal failed",
                        slog.Uint64("group_id", gid),
                        slog.Int64("ceiling_ms", ceilingMs),
                        slog.Any("err", err),
                    )
                }
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

## 7. Milestones

| Phase | Scope | Priority |
|-------|-------|----------|
| M1 — immediate | Extend `RunHLCLeaseRenewal` to all shard groups (Section 6) | High |
| M2 | Define `TSOAllocator` interface; implement backed by `defaultGroup` | Medium |
| M3 | `BatchAllocator` for low-latency timestamp serving | Medium |
| M4 | Dedicated TSO Raft group (`groupID = 0`) with `TSOStateMachine` | Low |
| M5 | Cross-shard SSI read-timestamp validation via TSO | Low |

---

## 8. Open Questions

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
