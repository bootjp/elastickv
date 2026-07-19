# Centralized Timestamp Oracle (TSO) Design

Status: Implemented
Author: bootjp
Date: 2026-04-16
Updated: 2026-07-19

M1-M8 implement the central subsystem: the dedicated group-0 FSM,
leader-routed durable windows, strict term bootstrap, serialized shadow
migration, one-way cutover and Phase-D retirement, validated cross-shard
timestamps, one-way runtime mode reload, production metrics and alerts, and
write-fanout benchmark evidence. The topology and clock-policy extensions in
Section 9 are intentional non-goals and do not leave central scope incomplete.

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
   isolating group 0 under `base/raftID/group-0`. When group 0 is present,
   `--tsoEnabled` now routes issuance to its current leader; without group 0,
   the flag preserves the earlier local/default-group compatibility bridge.
9. `TSOStateMachine` implements the dedicated-group FSM contract: it accepts
   HLC lease entries and explicit allocation-floor entries, halt-fails invalid
   entries, snapshots/restores TSO-owned ceiling/floor state, and classifies
   full ceiling/floor mirror entries as volatile-only so post-snapshot HLC
   mirror raises are not lost on duplicate replay skip paths.
10. Runtime bootstrap instantiates `TSOStateMachine` for `groupID = 0` without
    opening an MVCC store. Group 0 retains the normal wrap-aware proposer path
    for Raft-envelope cutovers, while route validation keeps all user data out
    of the control group. Restore accepts snapshots written by the earlier
    compatibility `kvFSM`, imports their HLC ceiling, derives a safe allocation
    floor, and drains the legacy MVCC payload for full CRC verification. The
    group-0 `EncryptionAdmin` surface is capability-only: mutators are left
    unwired and return `FailedPrecondition`. During upgrade replay, valid
    encryption control entries committed by the earlier compatibility FSM are
    decoded and deterministically rejected without halting the TSO apply loop;
   malformed control entries still halt fail-closed.
11. `RaftTSOAllocator` verifies group-0 leadership and commits every returned
    window's inclusive end before exposing it. On the first request of every
    leader term it obtains a strict, leader-fenced maximum `LastCommitTS` from
    every data group; failure to reach any authoritative group leader blocks
    issuance rather than falling back to a stale replica watermark. Remote
    watermark requests carry the explicit data-group ID, and the receiving
    node revalidates local leadership with a linearizable ReadIndex before
    returning that group's store watermark, so dialing a recently-demoted
    leader cannot satisfy the fence with stale local state. The response echoes
    the group ID and carries an explicit leader-fenced marker; a legacy server
    that ignores the request field is rejected fail-closed. The allocator also
    revalidates the same group-0 term after the remote floor/cutover work and
    after committing the allocation floor. A term change may leak a committed
    window, but that window is never returned and its floor prevents reuse.
12. `LeaderRoutedTSOAllocator` serves the local group-0 leader directly and
    redirects followers through `Distribution.GetTimestamp`. The response
    carries an explicit durable-TSO marker and echoed count, so a new client
    rejects a rolling-upgrade response from a legacy server that ignored the
    batch/minimum request fields.
13. `--tsoShadowEnabled` serializes each legacy candidate through group 0
    before returning it. The response includes the allocation floor that
    preceded the reservation. A candidate at or below that floor is discarded,
    the local HLC observes the newer reservation, and allocation retries. TSO
    unavailability fails the write closed because an unmirrored legacy value
    cannot establish cutover readiness.
14. The group-0 FSM persists a one-way production cutover marker. The first
    `--tsoEnabled` refill commits that marker before its window. Nodes still in
    shadow mode observe the marker in their next reservation response and
    return the TSO value instead of a legacy candidate, preserving safety
    during a rolling flag transition. Routed responses are also observed into
    the local legacy HLC so the fallback clock never moves backward. On
    restart, the restored marker takes precedence over startup flags: a node
    started with neither mode flag, or with only `--tsoShadowEnabled`, installs
    production group-0 routing instead of re-entering legacy or shadow
    issuance. The marker uses a versioned TSO envelope with the same legacy
    fail-closed prefix as allocation-floor entries and a distinct magic, so an
    old or misrouted data FSM halts while encryption bytes cannot activate it.
15. `--tsoPhaseDEnabled` commits a second one-way group-0 marker after cutover.
    The marker captures the maximum pre-Phase-D timestamp floor. Its state and
    floor are persisted in the V4 TSO snapshot. Before the marker applies, the
    FSM continues writing V3 snapshots so older followers can install them
    during the rolling upgrade. Malformed ordering or a changed replay floor
    halts the TSO FSM fail-closed. The marker has its own versioned TSO envelope
    with the same legacy fail-closed prefix, so old or misrouted data FSMs halt
    and no reserved encryption byte can activate Phase D.
16. Once Phase D is durable, data groups no longer receive HLC ceiling renewal,
    coordinator legacy HLC issuance is rejected, and shadow mode bypasses
    legacy candidate generation/comparison. Group 0 remains renewed while it
    is needed for the dedicated allocator's physical-ceiling fence.
17. Cross-shard transactions with a caller-supplied `StartTS` are accepted only
    when the group-0 leader verifies `phase_d_floor < StartTS <=
    allocation_floor`. The upper bound is a committed TSO reservation floor and
    the lower bound excludes every legacy or pre-Phase-D value. Follower-local
    state is never authoritative for this check.
18. Adapter read-modify-write paths call `BeginReadTimestampThrough` before the
    first read and pass an applied store/catalog watermark, never a freshly
    allocated clock value. If Phase D is required but not yet locally active,
    the read boundary first forces the marker and its post-marker allocation
    window to commit, then discards that allocation. The adapter still uses the
    exact applied watermark for every read and `StartTS`. When that watermark
    predates the Phase-D floor, the read boundary registers a bounded,
    process-local capability that identifies the audited applied snapshot. The
    adapter binds that capability to the logical operation and reserves exactly
    one one-use coordinator voucher immediately before each dispatch that
    shares the snapshot. Unvouched external `StartTS` values remain subject to
    group-0 validation and values at/below the floor fail closed. A zero,
    latest-sentinel, unavailable activation, or otherwise unprovable watermark
    also fails closed. Retries reload and revalidate the applied watermark;
    retries that intentionally reuse an exact OCC write set reserve a fresh use
    without widening the capability to another timestamp. Coordinator
    decorators forward both the allocator provider and voucher operation so
    startup and keyviz wrappers cannot silently fall back to an unvalidated
    value.
19. `BatchAllocator` validates cached candidates once Phase D is required. A
    candidate at/below the Phase-D floor invalidates the entire local window and
    forces a refill above the marker before any timestamp is returned.
20. `--tsoModeFile` polls an atomically replaced `legacy`, `shadow`, `cutover`,
    or `phase-d` mode. Live transitions must be adjacent and one-way. Invalid
    files, skipped phases, and process-local rollbacks leave the active allocator
    unchanged. Applied group-0 markers override a stale local mode immediately
    on allocator resolution, before the next poll can run.
21. The production registry exports reserve/validation latency by local or
    remote path and outcome, shadow comparison/divergence, process mode, every
    reload outcome, and durable marker state. Checked Prometheus rules define
    warning and critical latency/error thresholds, persistent reload failure,
    mode divergence, and the 15-minute zero-overlap cutover gate.
22. `BenchmarkTSOWriteFanout` models 16 concurrent coordinators, 1/3/8-way
    writes, a 1 ms remote TSO commit, and batch sizes 1/64/256. Three-run
    evidence supports the existing default batch size 256 and is recorded in
    `docs/centralized_tso_operations.md`.

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

M1 fixed that per-node renewal gap by proposing to every group this node
currently leads. It did not by itself guarantee global timestamp monotonicity
when different coordinators allocated timestamps for cross-group work; M2-M7
add the dedicated TSO and retire that distributed issuance path.

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
- The TSO FSM applies HLC lease entries and explicit allocation-floor entries.
  Allocation floors use a versioned TSO envelope whose first byte remains in
  the old data-FSM fail-closed range; the full magic prevents an encryption
  opcode from being interpreted as TSO state. It carries no key-value storage,
  so compaction and snapshot overhead are negligible.
- Membership mirrors the full cluster so any node can become TSO leader.

### 3.4 TSO FSM

```go
// TSOStateMachine implements raftengine.StateMachine. It is a minimal FSM
// that tracks TSO-owned HLC physical ceiling and allocation floor state. The
// shared HLC is a volatile mirror only; snapshots are sourced from the FSM's
// own fields so data-group lease renewals cannot advance group-0 state outside
// group-0 consensus.
//
// Interface mapping (raftengine.StateMachine vs raw raft.FSM):
//   Apply([]byte)             ← engine strips log envelope, passes raw payload
//   Snapshot() (Snapshot, _) ← returns raftengine.Snapshot (WriteTo/Close)
//   Restore(io.Reader)        ← caller owns and closes the reader
type TSOStateMachine struct {
    hlc             *HLC
    ceilingMs       atomic.Int64
    allocationFloor atomic.Uint64
}

// Apply processes TSO ceiling/floor entries. The engine strips the raft.Log
// envelope and passes only the raw command bytes, matching the
// raftengine.StateMachine.Apply([]byte) signature.
func (f *TSOStateMachine) Apply(data []byte) any {
    if len(data) == 0 {
        return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry,
            "empty entry"))
    }
    switch {
    case data[0] == raftEncodeHLCLease:
        if len(data) != hlcLeaseEntryLen {
            return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
                "expected %d bytes, got %d", hlcLeaseEntryLen, len(data)))
        }
        ceiling := int64(binary.BigEndian.Uint64(data[1:]))
        if ceiling <= 0 {
            return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
                "non-positive HLC lease ceiling %d", ceiling))
        }
        f.applyLeaseCeiling(ceiling)
    case bytes.HasPrefix(data, []byte(tsoAllocationFloorEnvelope)):
        if len(data) != len(tsoAllocationFloorEnvelope)+8 {
            return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
                "invalid allocation-floor envelope length %d", len(data)))
        }
        floor := binary.BigEndian.Uint64(data[len(tsoAllocationFloorEnvelope):])
        if floor == 0 {
            return haltErr(errors.Wrap(ErrTSOStateMachineInvalidEntry,
                "zero TSO allocation floor"))
        }
        f.applyAllocationFloor(floor)
    default:
        return haltErr(errors.Wrapf(ErrTSOStateMachineInvalidEntry,
            "unexpected tag 0x%02x", data[0]))
    }
    return nil
}

// Snapshot serialises ceiling and allocationFloor as two 8-byte big-endian
// fields. The source is TSO-applied state, not the shared HLC mirror.
// Returns raftengine.Snapshot (WriteTo/Close) rather than raft.FSMSnapshot.
func (f *TSOStateMachine) Snapshot() (raftengine.Snapshot, error) {
    return &tsoSnapshot{
        ceiling:         f.ceilingMs.Load(),
        allocationFloor: f.allocationFloor.Load(),
    }, nil
}

// Restore deserialises ceiling/floor state and mirrors it to the shared HLC.
// Current snapshots are 16 bytes; legacy 8-byte ceiling snapshots derive the
// allocation floor from the ceiling used by the previous FSM format.
// The caller owns the reader and is responsible for closing it.
func (f *TSOStateMachine) Restore(r io.Reader) error {
    payload, err := io.ReadAll(io.LimitReader(r, 17))
    if err != nil {
        return err
    }
    var ceiling int64
    var floor uint64
    switch len(payload) {
    case 8:
        ceiling = int64(binary.BigEndian.Uint64(payload[:8]))
        floor = tsoLeaseAllocationFloor(ceiling)
    case 16:
        ceiling = int64(binary.BigEndian.Uint64(payload[:8]))
        floor = binary.BigEndian.Uint64(payload[8:])
    default:
        return errors.Newf("expected 8 or 16 snapshot bytes, got %d",
            len(payload))
    }
    f.restoreSnapshotState(ceiling, floor)
    return nil
}

// tsoSnapshot implements raftengine.Snapshot. It serialises TSO-owned ceiling
// and floor state as 16 bytes — the entire persisted state of the TSO group.
// WriteTo/Close matches the raftengine.Snapshot interface (not Persist/Release
// from raft.FSMSnapshot).
type tsoSnapshot struct {
    ceiling         int64
    allocationFloor uint64
}

func (s *tsoSnapshot) WriteTo(w io.Writer) (int64, error) {
    var buf [16]byte
    binary.BigEndian.PutUint64(buf[:], uint64(s.ceiling))
    binary.BigEndian.PutUint64(buf[8:], s.allocationFloor)
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
  ├─ strict read of every data-group leader LastCommitTS
  │    → failure blocks issuance
  │
  └─ HLC.Next() uses max(now, physicalCeiling, global commit floor)
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

### 7.1 Phase A — Group-0 Warm-up (no read cutover)

```
┌──────────────────────────────────────────────────────────┐
│  ShardedCoordinator                                        │
│                                                            │
│  startTS = legacyHLC.Next()  (existing path, unchanged)   │
│                                                            │
│  RunHLCLeaseRenewal():                                     │
│    └─ each local Raft leader proposes to the groups it leads│
│       ↳ the group-0 leader warms the TSO FSM ceiling       │
└──────────────────────────────────────────────────────────┘
```

- The TSO group receives ceiling proposals but **no reads are served from it**.
- This allows TSO FSM state to warm up and be validated in production before
  the cutover.
- These independent proposals do **not** prove that the TSO ceiling is greater
  than every data-group ceiling. Phase A is warm-up only; Phase B provides the
  issuance-level serialization required for a safe transition.
- Rollback: remove group 0 before entering Phase B; no timestamp path changed.

### 7.2 Phase B — Serialized Shadow Migration

- Every node must run `--tsoShadowEnabled` before any node enables cutover.
- A legacy candidate is sent to the TSO leader as `min_timestamp`. Under the
  group-0 allocator mutex, the leader samples the preceding durable allocation
  floor, reserves and commits a timestamp above the candidate, and returns both.
- If the candidate is at or below the preceding floor, it may overlap an older
  TSO/shadow reservation and is discarded. The caller observes the newer TSO
  value into its HLC and retries; only a candidate proven above the preceding
  floor is returned to the legacy write path.
- Shadow RPC/proposal failure fails timestamp issuance closed. A fail-open
  shadow cannot prove the migration invariant and is therefore not eligible
  for production cutover.

### 7.3 Phase C — Durable TSO Cutover

- The backward-compatible startup flag `--tsoEnabled`, or runtime mode file
  value `cutover`, switches coordinator issuance to the leader-routed allocator.
  A live transition is accepted only after `shadow`; restart recovery may start
  directly in cutover when the durable marker already exists.
- The first production refill commits the group-0 cutover marker before
  committing and returning its timestamp window.
- The marker is encoded in a versioned TSO-specific envelope whose leading
  reserved byte remains fail-closed on pre-TSO and data-group FSMs.
- A node still running Phase B receives `cutover_active=true` on its next
  shadow reservation and returns the reserved TSO timestamp. Therefore a
  rolling restart does not keep issuing legacy values after the marker.
- The marker is intentionally one-way. Before it commits, rollback means
  returning every node to Phase B. After it commits, rollback must preserve the
  TSO path; clearing it requires a separate cluster-wide quiescence protocol.

### 7.4 Phase D — Legacy Cleanup

- Roll every member to a binary that understands the Phase-D entry and
  `ValidateTimestamp` RPC. Run all members on the dedicated TSO path before
  enabling `--tsoPhaseDEnabled` or reloading `phase-d`; an older member would
  correctly halt on the unknown control entry, so mixed-version activation is
  prohibited.
- The first allocation with Phase D requested commits cutover (if needed), then
  the Phase-D marker and its pre-Phase-D floor, then a new allocation window
  strictly above that floor. The switch is one-way and survives restart through
  the TSO V4 snapshot.
- `ShardedCoordinator` dynamically stops data-group ceiling proposals and
  fails closed instead of issuing from its legacy HLC. It continues group-0
  renewal only. Shadow mode observes durable cutover and directly uses the
  dedicated allocator without generating or comparing a legacy candidate.
- Every adapter read-modify-write attempt obtains its transaction snapshot
  from an applied store/catalog watermark before reading. If Phase D is required
  but not yet active locally, the read boundary first commits the marker and a
  post-marker allocation window, then discards the allocated value; it is never
  substituted for data-group apply progress. The same applied watermark is used
  for direct MVCC reads, active-snapshot pinning, and `OperationGroup.StartTS`;
  a retry reloads and revalidates the watermark and repeats all reads. An applied
  watermark at/below the Phase-D floor is admitted only through a bounded,
  process-local capability registered by that audited read boundary. Every
  dispatch sharing the snapshot reserves and consumes its own one-use voucher;
  the capability is timestamp-bound and cannot authorize another value.
  Arbitrary or repeated unvouched caller values at/below the floor still fail
  closed at group 0. Pre-Phase-D deployments retain the prior
  committed-watermark behavior.
- After Phase D, the coordinator asks the current group-0 leader to validate a
  caller-supplied cross-shard `StartTS` before allocating `CommitTS` or proposing
  to any data group. The allocator's configured `PhaseDRequired` state activates
  this check before the local group-0 replica has applied the marker. Values
  at/below the marker floor, beyond the committed TSO allocation floor, zero
  values, inactive Phase-D state, missing validation support, and unavailable
  leadership all fail closed. Existing single-shard caller-supplied `StartTS`
  remains compatible because it does not establish a cross-shard SSI snapshot.

### 7.5 Monotonicity Invariant Across Phases

At every Phase-B/C issuance boundary, the following invariant must hold:

```
returned_legacy_ts > previous_tso_allocation_floor
new_tso_allocation_floor >= returned_legacy_ts
```

Both comparisons occur in one group-0-serialized reservation before the legacy
candidate is returned. Once the cutover marker applies, shadow callers stop
returning legacy candidates. Independently, every new TSO leader term fences
its first window above the strict maximum committed data timestamp.

### 7.6 Runtime Reload and Operational Gates

`DynamicTimestampAllocator` publishes the process-selected allocator through
an atomic pointer. Before resolving that pointer, it checks the consensus-owned
cutover and Phase-D markers. An applied marker selects the batch/routed TSO path
and enables matching remote confirmation immediately, so a stale `legacy` or
`shadow` file cannot mint a legacy timestamp while waiting for the next poll.
A request that resolved its preceding mode before local marker apply may finish,
but every subsequent resolution observes the one-way durable override.

`TSORuntimeController` accepts only adjacent one-way live transitions:

```text
legacy -> shadow -> cutover -> phase-d
```

The initial process mode may be later in the sequence for restart recovery.
No-op polls refresh the durable-state gauges, and every failed poll increments
its bounded reload-result counter even when duplicate log messages are
suppressed. Exact rollout gates, alert expressions, atomic file replacement,
and benchmark evidence are maintained in
`docs/centralized_tso_operations.md`.

---

## 8. Milestones

| Phase | Scope | Priority |
|-------|-------|----------|
| M1 — shipped | Extend `RunHLCLeaseRenewal` to all shard groups with parallel proposals (Section 6) | High |
| M2 — shipped | Reserve group 0, keep its ceiling warm as a pre-migration compatibility step, and prevent user-data routes from entering the control group (Section 7.1). This warm-up alone is not a cutover proof. | High |
| M3 — shipped | Define `TSOAllocator` interface; implement backed by `defaultGroup` | Medium |
| M4 — shipped | `BatchAllocator` with atomic counter for low-latency timestamp serving | Medium |
| M5 — shipped | Preserve the default-group `LocalTSOAllocator` compatibility bridge when group 0 is absent; route coordinator-owned timestamp call sites through the allocator abstraction. | Medium |
| M6 — shipped | Run the dedicated group-0 FSM, fence each new TSO leader term above all authoritative data-group commit floors, redirect follower requests to the TSO leader over gRPC, synchronously serialize fail-closed shadow issuance, and commit the one-way rolling cutover marker before production windows. | Low |
| M7 — shipped | Commit the durable Phase-D floor marker, preserve V3 snapshots until activation, retire data-shard HLC renewal and legacy/shadow issuance after cutover, activate before read validation while preserving exact applied snapshots through timestamp-bound per-dispatch vouchers, invalidate pre-Phase-D batch windows, and validate unvouched caller-supplied cross-shard SSI timestamps at the group-0 leader from activation onward. | Low |
| M8 — shipped | Atomically reload one-way runtime modes, make durable markers override stale local mode before allocation, expose production latency/divergence/state metrics, install checked alert thresholds, and validate the default batch size with concurrent write-fanout evidence. | Low |

---

## 9. Resolved Questions and Future Extensions

1. **TSO RTT when TSO leader differs from the write leader (resolved):** The
   concurrent benchmark covers 1/3/8 fan-out legs with a modeled 1 ms remote
   TSO commit. Batch size 1 exposes serialized refill tails, while default 256
   keeps the recorded p99 below the 50 ms warning threshold.

2. **TSO group membership (intentional future extension):** Choosing all nodes
   or a dedicated subset is a topology policy outside the central timestamp
   subsystem. The implemented routing and fail-closed term fence support either.

3. **Clock floor semantics (intentional future extension):** The implementation
   retains the existing floor formula. Changing to `ceiling + 1` is an
   independent HLC policy decision, not unfinished centralized-TSO scope.

4. **Non-leader TSO requests (resolved):** Followers redirect to the current
   group-0 leader through `Distribution.GetTimestamp`. Timestamp allocation is
   a consensus write, so follower reads cannot replace the leader proposal.

5. **Backward compatibility (resolved for group 0):** Existing group-0 Raft
   logs already carry compatible HLC lease entries. `TSOStateMachine.Restore`
   recognizes legacy `kvFSM` snapshot headers, imports the ceiling, derives the
   allocation floor, and drains the old store payload before Raft resumes log
   replay. Valid historical encryption control entries are decoded and rejected
   as obsolete ordinary apply responses, so replay advances without mutating
   TSO state; new group-0 encryption mutators are disabled at the RPC boundary,
   while the group-0 engine remains wired as a leader view so read-only sidecar
   recovery cannot be served from a stale follower. Runtime wiring therefore
   does not require deleting group-0 state.
