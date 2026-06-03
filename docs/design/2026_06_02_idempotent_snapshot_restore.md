# Idempotent FSM Snapshot Restore on Cold Start

**Status**: Proposal (Round 4 — codex P2 fix: snapshot-persist meta key bump)
**Date**: 2026-06-02 (round 1), 2026-06-03 (rounds 2 / 3 / 4)
**Author**: bootjp
**Related**: PR #909 (`HEALTH_TIMEOUT_SECONDS` 60s → 300s)

## Problem

`loadWalState` (`internal/raftengine/etcd/wal_store.go:117`) unconditionally
calls `restoreSnapshotState(fsm, snapshot, fsmSnapDir)` (`:246`) whenever
the WAL's persisted snapshot pointer is non-empty.  For the EKV/token-format
snapshot case this dispatches to:

```go
return openAndRestoreFSMSnapshot(fsm, fsmSnapPath(fsmSnapDir, tok.Index), tok.CRC32C)
```

`openAndRestoreFSMSnapshot` invokes `fsm.Restore(reader)` which, on the
`pebbleStore` backend, lands in `restorePebbleNativeAtomic`
(`store/lsm_store.go:1816`).  That routine:

1. Creates a sibling temp directory (`pebble-native-*`).
2. Opens a fresh `pebble.DB` at the temp path (`Found 0 WALs` log line).
3. `restoreBatchLoopInto` writes every key from the snapshot reader.
4. `swapInTempDB` (`:2002`) calls `db.Close()` → `os.RemoveAll(s.dir)`
   → `os.Rename(tmpDir, s.dir)` → `pebble.Open(s.dir, ...)`.

For multi-GiB FSMs this is O(snapshot size) on the I/O path.  On a 5-node
192.168.0.x cluster with a ~5 M-key node we measured **~46 s** between the
first `pebble.Open(fsm.db)` log line and the post-swap re-open.  After the
swap the engine still has to replay the entries between the snapshot
index and the latest committed index, then become a raft follower and
bind the gRPC listener.

The cumulative cold-start budget routinely exceeded the 60-second
`HEALTH_TIMEOUT_SECONDS` (PR #909 raises it to 300 s as a band-aid).
The timeout also drives second-order instability — docker's restart
policy reinvestigates the non-responsive container, the remaining quorum
runs elections against a phantom voter, and the raft term inflates
linearly with restart attempts (we observed term 665 on a cluster that
should have seen single-digit-per-day elections).

The restore is **mostly redundant**.  Each successful `fsm.Apply` already
persists its data mutations durably (via Pebble's WriteBatch).  After
the FSM has applied entry `Y > snapshot.Metadata.Index = X`, the on-disk
fsm.db contains state `≥ X`.  On the next cold start we tear that state
down and rebuild it from the older snapshot, only to have raft replay
the same entries we already had on disk.

## Goal

Skip `restoreSnapshotState` when the on-disk FSM is already at least as
fresh as the persisted snapshot pointer (`stored.LastAppliedIndex ≥
snapshot.Metadata.Index`).  In the steady-state restart, restore becomes
a no-op and cold start collapses to:

1. `pebble.Open(fsm.db)` — already paid (Pebble's own WAL replay).
2. WAL replay of `(LastAppliedIndex, committed]` — small.
3. Raft follower-ization + gRPC bind.

Expected cold-start: **<5 s for any FSM size** in the steady-state case.
Restore still fires correctly when (a) the FSM truly is stale (e.g.
post-disaster recovery from someone else's snapshot), (b)
`LastAppliedIndex` is missing / corrupt, or (c) the snapshot header
contains state (HLC ceiling, Stage 8a cutover) that the skip path
cannot supply (see §5).

## Non-Goals

- Not changing the snapshot-install hot path
  (`Engine.applySnapshot`, `engine.go:1641`).  That path runs at
  runtime when a leader ships us a snapshot we genuinely don't have —
  the FSM is stale by construction there and `Restore` must run.
- Not changing the legacy non-token snapshot path
  (`fsm.Restore(bytes.NewReader(snapshot.Data))` at `wal_store.go:258`).
  Those payloads are only encountered during the one-shot hashicorp →
  etcd migration; the cost is paid once and the legacy branch is on a
  removal trajectory.
- Not designing for Stage 6/7/8 encryption opcodes we haven't shipped
  yet beyond the existing `applyReservedOpcode` dispatch
  (`raftEncodeHLCLease`=0x02 + `OpRegistration`=0x03 + `OpBootstrap`=0x04 +
  `OpRotation`=0x05).

## Design

### 1. Reuse the existing `ApplyIndexAware` seam

The current `StateMachine` interface
(`internal/raftengine/statemachine.go:14`) keeps its public shape:

```go
type StateMachine interface {
    Apply(data []byte) any
    Snapshot() (Snapshot, error)
    Restore(r io.Reader) error
}
```

The repo already has the seam that delivers `entry.Index` to the FSM
without a breaking signature change — `ApplyIndexAware`
(`statemachine.go:46`):

```go
type ApplyIndexAware interface {
    SetApplyIndex(idx uint64)
}
```

`engine.applyNormalEntry` (`engine.go:2292-2293`) already calls it
before every `Apply`:

```go
if aware, ok := e.fsm.(raftengine.ApplyIndexAware); ok {
    aware.SetApplyIndex(entry.Index)
}
return e.fsm.Apply(payload), nil
```

And `kvFSM` already implements it (`kv/fsm.go:122`) — `SetApplyIndex`
stashes the index in `f.pendingApplyIdx` (`kv/fsm.go:53`), which is
currently consumed only by `applyEncryption` for the encryption
sidecar's `RaftAppliedIndex` field.

**The Branch-2 work** is therefore narrow: extend the **already-set**
`f.pendingApplyIdx` to also be threaded through the data-Apply path so
the leaf MVCC mutation can persist it as a Pebble meta key — the
mechanism `applyEncryption` uses for the sidecar's index, applied to
the kvFSM's data store.  **No new interface, no new SetApplyIndex
plumbing, no engine.go change.**

Round-1 / round-2 of this doc proposed (or pivoted to) other shapes
here; both were corrections of the wrong baseline.  Round-3 uses
what `origin/main` actually has.

### 2. Thread `f.pendingApplyIdx` into the data-Apply leaves

`kvFSM.Apply` already begins with the reserved-opcode dispatch
(`applyReservedOpcode` at `kv/fsm.go` returns `(any, bool)`):

```go
func (f *kvFSM) Apply(data []byte) any {
    if resp, handled := f.applyReservedOpcode(data); handled {
        return resp
    }
    // ... data-Apply path ...
}
```

This path stays unchanged.  Inside `applyRequest` / `applyRequestErr`,
the leaf store calls (`ApplyMutationsRaft`, `DeletePrefixAtRaft`) pick
up a new optional `appliedIndex uint64` parameter that defaults to 0
(direct-write paths) and is non-zero for raft-apply paths.

The leaf helpers `applyMutationsWithOpts` (`lsm_store.go:1130`) and
`deletePrefixAtWithOpts` (`lsm_store.go:1196`) bundle a new
`metaAppliedIndexBytes` key in the same `pebble.Batch` they already use
for `metaLastCommitTSBytes` (`lsm_store.go:1162` and `:1231`
respectively):

```go
// store/lsm_store.go — applyMutationsWithOpts (existing, around :1162)
if appliedIndex > 0 {
    if err := setPebbleUint64InBatch(b, metaAppliedIndexBytes, appliedIndex); err != nil {
        return errors.WithStack(err)
    }
}

// store/lsm_store.go — deletePrefixAtWithOpts (existing, around :1231)
if appliedIndex > 0 {
    if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex); err != nil {
        return errors.WithStack(err)
    }
}
```

**Why both leaves**: `handleDelPrefix` builds an independent `pebble.Batch`
through `deletePrefixAtWithOpts`, separate from `applyMutationsWithOpts`.
Threading the index only through one would let DEL_PREFIX entries land
without bumping the meta key, leaving `LastAppliedIndex` behind the
true applied count.  Cost: +16 bytes per batch, zero additional fsync.

The Encryption opcodes (`OpRegistration`/`OpBootstrap`/`OpRotation`,
fsmwire 0x03..0x05) reach `applyEncryption(f.pendingApplyIdx, op, payload)`
in `kv/fsm.go`'s `applyReservedOpcode`.  That path already takes
`pendingApplyIdx` and persists it in the encryption sidecar's
`RaftAppliedIndex` field via `WriteSidecar`.  It does **not** mutate
the kvFSM data store, so it does **not** advance `metaAppliedIndex`.
That's accepted: a run of encryption-only entries between two
snapshots produces a `LastAppliedIndex` below the snapshot index, the
skip gate falls back to full restore — safe.  (See §6 for the
analogous HLC-lease case.)

### 3. Read-back exposes the index (with `dbMu.RLock()`)

```go
// store/lsm_store.go (new method, lock-ordering compliant)
//
// LastAppliedIndex returns the largest raft applied-index that any
// raft-apply Batch has persisted via this pebbleStore. Lock order is
// dbMu (RLock) before any db.Get, per the discipline documented at
// lsm_store.go:153 and exemplified at :553 / :675. Without the lock,
// a concurrent swapInTempDB could replace s.db between the Get call
// and our access of the returned value/closer pair, racing the
// snapshot install path.
func (s *pebbleStore) LastAppliedIndex() (uint64, bool, error) {
    s.dbMu.RLock()
    defer s.dbMu.RUnlock()
    val, closer, err := s.db.Get(metaAppliedIndexBytes)
    if errors.Is(err, pebble.ErrNotFound) {
        return 0, false, nil
    }
    if err != nil {
        return 0, false, errors.WithStack(err)
    }
    defer closer.Close()
    if len(val) != 8 {
        return 0, false, errors.Newf("corrupt applied-index meta key: %d bytes", len(val))
    }
    return binary.BigEndian.Uint64(val), true, nil
}
```

`metaAppliedIndexBytes` is `[]byte("_meta_applied_index")` — sibling to
`metaLastCommitTSBytes` (`lsm_store.go:145`), outside the MVCC user-key
space, disjoint from every existing meta key.  Branch 2 extends
`isReservedMetaKey` (`lsm_store.go:454`) to include it.

The reader interface lets `restoreSnapshotState` inspect the store
without coupling the etcd raft engine to `*pebbleStore`:

```go
// internal/raftengine/statemachine.go (new alongside ApplyIndexAware)
type AppliedIndexReader interface {
    LastAppliedIndex() (uint64, bool, error)
}
```

`kvFSM` exposes its store through a typed accessor so wal_store.go can
reach it without importing the concrete pebble type:

```go
type AppliedIndexReporter interface {
    AppliedIndexReader() AppliedIndexReader
}
func (f *kvFSM) AppliedIndexReader() AppliedIndexReader {
    if r, ok := f.store.(AppliedIndexReader); ok {
        return r
    }
    return nil
}
```

### 4. Conditional restore (with conservative error fallback)

```go
// internal/raftengine/etcd/wal_store.go
func restoreSnapshotState(fsm StateMachine, snapshot raftpb.Snapshot, fsmSnapDir string) error {
    if etcdraft.IsEmptySnap(snapshot) || len(snapshot.Data) == 0 || fsm == nil {
        return nil
    }
    if isSnapshotToken(snapshot.Data) {
        tok, err := decodeSnapshotToken(snapshot.Data)
        if err != nil { return err }
        if fsmAlreadyAtIndex(fsm, tok.Index) {
            // The body restore is skipped, but we MUST still consume
            // the v1/v2 snapshot header so the FSM picks up the HLC
            // ceiling AND the Stage 8a cutover (see §5).
            return applyHeaderStateOnSkip(fsm, fsmSnapPath(fsmSnapDir, tok.Index))
        }
        return openAndRestoreFSMSnapshot(fsm, fsmSnapPath(fsmSnapDir, tok.Index), tok.CRC32C)
    }
    // Legacy non-token path: unchanged.
    return errors.WithStack(fsm.Restore(bytes.NewReader(snapshot.Data)))
}

// fsmAlreadyAtIndex returns true ONLY when we can prove the FSM is
// already at or past `want`. Any uncertainty -- FSM doesn't expose
// the reporter, store doesn't expose the reader, read error, or
// missing meta key -- returns false so we fall back to the full
// restore. A stale-but-incorrect skip is far worse than a wasteful
// full restore; the fallback errs strictly toward restoring.
func fsmAlreadyAtIndex(fsm StateMachine, want uint64) bool {
    reporter, ok := fsm.(AppliedIndexReporter)
    if !ok { return false }
    reader := reporter.AppliedIndexReader()
    if reader == nil { return false }
    have, present, err := reader.LastAppliedIndex()
    if err != nil || !present { return false }
    return have >= want
}
```

### 5. Header state preservation when skipping (P1)

`kvFSM.Restore` (`kv/fsm.go`) parses the snapshot header via
`kv.ReadSnapshotHeader(*bufio.Reader)` and applies **two** pieces of
state:

```go
// kv/fsm.go -- existing Restore
ceilingU, cutover, err := ReadSnapshotHeader(br)
if err != nil { return errors.WithStack(err) }
if f.hlc != nil && ceilingU > 0 {
    f.hlc.SetPhysicalCeiling(int64(ceilingU))
}
f.restoredCutover = cutover
return errors.WithStack(f.store.Restore(io.NopCloser(br)))
```

`ReadSnapshotHeader` handles three live cases:

- v1 magic (`EKVTHLC1`): 16-byte header, ceiling only.
- v2 magic (`EKVTHLC2`): variable-length header (≥ 26 bytes), ceiling +
  cutover + forward-compat trailing bytes.
- Unknown `EKVTHLC*`: fails closed with `ErrSnapshotHeaderUnknownMagic`
  (Stage 8a §3.2 step 4 — operator must upgrade).
- Headerless legacy / short stream: returns `(0, 0, nil)`, no state to
  apply.

If we skip `Restore` we lose **both** the HLC ceiling assignment and
the `f.restoredCutover` write.  After skip:

- `f.hlc.physicalCeiling` stays at the engine's wall-clock-now seed —
  not the snapshotted floor.  Subsequent HLC lease entries replayed
  from the WAL bump it, but lease entries whose index ≤
  `snapshot.Metadata.Index` are compacted out of the WAL and not
  replayed, so the in-memory ceiling can finish below the snapshotted
  ceiling.  On the leader-election that follows, this node can mint
  HLC timestamps below the snapshotted floor — a cluster-wide
  monotonicity violation.
- `f.restoredCutover` stays at zero, which Stage 6E's apply-hook reads
  as "no envelope cutover seen", so the next data entry the FSM
  applies would be treated as below-cutover regardless of its index.
  In a Stage-8a cluster this silently disables the encryption
  cutover gate for one entry.

**The skip path MUST consume the snapshot header and apply the same
two side-effects as `kvFSM.Restore` does.**  Reuse the existing
parser, do not invent a v1-only probe:

```go
// internal/raftengine/etcd/wal_store.go (new)
func applyHeaderStateOnSkip(fsm StateMachine, snapPath string) error {
    setter, ok := fsm.(SnapshotHeaderApplier)
    if !ok {
        return nil // FSM has no header state; skip is harmless.
    }
    f, err := os.Open(snapPath)
    if err != nil { return errors.WithStack(err) }
    defer f.Close()
    br := bufio.NewReader(f)
    ceiling, cutover, err := kv.ReadSnapshotHeader(br)
    if err != nil {
        // Includes ErrSnapshotHeaderUnknownMagic / InvalidLength.
        // Fail closed -- the operator must upgrade rather than have
        // us silently skip a forward-incompatible header.
        return errors.WithStack(err)
    }
    setter.ApplySnapshotHeader(ceiling, cutover)
    return nil
}

// kv/fsm.go gains:
type SnapshotHeaderApplier interface {
    ApplySnapshotHeader(ceiling, cutover uint64)
}

func (f *kvFSM) ApplySnapshotHeader(ceiling, cutover uint64) {
    if f.hlc != nil && ceiling > 0 {
        f.hlc.SetPhysicalCeiling(int64(ceiling))
    }
    f.restoredCutover = cutover
}
```

By reusing `kv.ReadSnapshotHeader` the skip path inherits v1 + v2
parity, the `ErrSnapshotHeaderUnknownMagic` fail-closed branch, and
any future v3 the snapshot header gains.  Cost: a single
`bufio.NewReader(f)` + ~26-byte read.  We do **not** consume any of
the inner-store payload — `f` is closed before returning.

### 6. Crash-safety argument

We claim: `LastAppliedIndex = N` durably implies "every raft apply for
indices `[snapshotIndex+1 .. N]` that produced a `pebbleStore`
mutation is durably present in fsm.db."

#### Default mode (`pebble.Sync`)

The meta key is written in the same `pebble.Batch` as the data
mutation.  Pebble commits batches atomically: either both records make
it to the Pebble WAL and a successful fsync confirms durability, or
neither does.  No crash window exists between persisting the mutation
and persisting the index.

#### `ELASTICKV_FSM_SYNC_MODE=nosync` mode

When `raftApplyWriteOpts()` returns `pebble.NoSync` (configured at
`lsm_store.go:1094-1104`), the data + meta key still land in the same
Pebble WAL record, but the OS may delay the fsync.  Durability shifts
to the raft layer: the raft WAL is the canonical source of truth, and
on crash recovery raft replays entries from the last fsync'd Pebble
position forward.  The invariant still holds because:

- Pebble's atomic batch property is independent of the sync option.
- On recovery, Pebble's WAL replay reconstructs the most recent
  committed batch (or none of it), giving us either `(data + meta)`
  at index N or `(neither)`.
- raft will then replay any entries that the OS-buffered fsync lost,
  and each Apply re-bundles its own index.

The skip gate stays correct: a `nosync`-induced loss merely means
`LastAppliedIndex` reports a lower value than the post-restart applied
count would imply, which only causes us to over-restore
conservatively.

#### Encryption opcodes (`OpRegistration`/`OpBootstrap`/`OpRotation`)

These opcodes reach `applyEncryption(f.pendingApplyIdx, op, payload)`,
which mutates the encryption sidecar (`WriteSidecar`), not the
kvFSM data store.  `metaAppliedIndex` therefore does NOT advance for
these entries.  Consequence: in a Stage 6/7/8 maintenance window where
the only entries between two snapshots are encryption ops,
`LastAppliedIndex` stays below the snapshot index and the skip falls
back to full restore.  Safe; rare.

(The sidecar separately carries its own `RaftAppliedIndex` for §9.1
`ErrSidecarBehindRaftLog`; that mechanism is orthogonal to this
proposal.)

#### HLC lease entries — checkpoint at snapshot persist (codex round-3 P2)

`f.applyHLCLease` is an in-memory mutation only; it does not touch
`f.store`, so `metaAppliedIndex` does NOT advance for HLC lease
entries individually.

Round-3 of this doc claimed the resulting full-restore fallback was
"safe and rare."  Codex correctly pointed out it is neither:
`RunHLCLeaseRenewal` (`kv/coordinator.go:650`) proposes a lease every
`hlcRenewalInterval = 1 * time.Second` while the local node is leader,
so even an active cluster will routinely accumulate a tail of lease
entries between any two data writes.  When the next snapshot is
persisted at index `X`, the gap between the last data-Apply index `Y`
and `X` always contains lease entries — and once `metaAppliedIndex`
sits at `Y < X` on restart, `fsmAlreadyAtIndex(X)` returns false, the
full restore runs, and the same stale `metaAppliedIndex = Y` is
re-installed from the snapshot.  Idle clusters degenerate to
"`metaAppliedIndex` never advances past the very last data write" and
the skip never fires.  Round-3 was wrong; round-4 fixes it.

**Mechanism**: bump `metaAppliedIndex` to `snapshot.Metadata.Index`
when persisting a created snapshot, in `persistCreatedSnapshot`
(`internal/raftengine/etcd/engine.go:2683`), **before**
`e.persist.SaveSnap`:

```go
// internal/raftengine/etcd/engine.go (revised)
func (e *Engine) persistCreatedSnapshot(snap raftpb.Snapshot) error {
    if etcdraft.IsEmptySnap(snap) || e.persist == nil {
        return nil
    }
    // Round-4: bump metaAppliedIndex BEFORE SaveSnap so a successful
    // snapshot persist always implies LastAppliedIndex >= snap.Metadata.Index.
    // Skip the bump silently when the FSM does not expose the writer
    // seam (legacy fakes / test shims).
    if w, ok := e.fsm.(AppliedIndexWriter); ok {
        if err := w.SetDurableAppliedIndex(snap.Metadata.Index); err != nil {
            return errors.WithStack(err)
        }
    }
    if err := e.persist.SaveSnap(snap); err != nil {
        return errors.WithStack(err)
    }
    // ... existing Release + purge ...
}
```

The new `AppliedIndexWriter` interface lives next to
`AppliedIndexReader` in `internal/raftengine/statemachine.go`:

```go
type AppliedIndexWriter interface {
    SetDurableAppliedIndex(idx uint64) error
}
```

`kvFSM` implements it by forwarding to a new `pebbleStore` method
that runs a single-key `pebble.Batch` write with the same Sync mode
as the raft-apply path (`s.raftApplyWriteOpts()`):

```go
// kv/fsm.go
func (f *kvFSM) SetDurableAppliedIndex(idx uint64) error {
    w, ok := f.store.(interface {
        SetDurableAppliedIndex(idx uint64) error
    })
    if !ok { return nil }
    return w.SetDurableAppliedIndex(idx)
}

// store/lsm_store.go
func (s *pebbleStore) SetDurableAppliedIndex(idx uint64) error {
    s.dbMu.RLock()
    defer s.dbMu.RUnlock()
    b := s.db.NewBatch()
    defer b.Close()
    if err := setPebbleUint64InBatch(b, metaAppliedIndexBytes, idx); err != nil {
        return errors.WithStack(err)
    }
    return errors.WithStack(b.Commit(s.raftApplyWriteOpts()))
}
```

**Crash ordering**.  The bump runs before `SaveSnap`, which means the
invariant is:

| State at crash | metaAppliedIndex on disk | snapshot pointer on disk | After restart |
|---|---|---|---|
| Before bump | last data Apply index `Y` | previous snapshot at `X' < X` | skip if `Y ≥ X'` (correct) |
| Bump done, SaveSnap not yet | `X` | still `X'` | skip succeeds against `X'` (over-restore impossible) |
| Both done | `X` | `X` | skip succeeds against `X` (correct — the optimisation works) |
| Both done + later data Apply at `Z > X` | `Z` | `X` | skip succeeds against `X` (correct) |

In particular, there is no ordering where `snapshot pointer = X` but
`metaAppliedIndex < X`: the snapshot pointer is only persisted after
the meta key, so the only way to observe a snapshot pointer at `X` is
that the meta key already reached `X` (or moved past it).  Round-3's
permanent fallback case is closed.

**Cost**: one extra pebble `Batch.Commit` (Sync per
`ELASTICKV_FSM_SYNC_MODE`) per snapshot persist.  Snapshots fire on the
etcd raft `SnapshotCount` cadence (default 10000 entries), so this is
~one extra fsync per ~10000 entries — negligible.

**Why not bump on every HLC lease apply**.  Option A (1 pebble batch
per lease tick) costs ~1 fsync/sec/group continuously.  Option B (the
snapshot-persist hook) costs ~1 fsync per 10000 entries.  Both close
the skip gap; B costs ~10⁴× less and aligns with the natural
durability boundary the engine already maintains.

### 7. Idempotency of replay after skip (OCC two-case)

After we skip restore, raft replays entries `[snapshotIndex+1, committed]`.
Some of those entries may already be present in fsm.db (if a previous
restart reached `applied=K > snapshotIndex` before crashing).  We do
**not** suppress replay of already-applied entries.  Two cases cover
the invariant:

**(a) Raw requests** (`startTS = commitTS = T`).  The kvFSM dispatch
reaches `f.store.ApplyMutationsRaft(... T, T ...)`.  Inside
`checkConflicts`, `latestCommitTS(T) > T` evaluates to false (equal,
not greater), so no conflict is raised and the Pebble write is a
deterministic overwrite of the same MVCC cell.  Re-applying is a
no-op at the byte level.

**(b) OCC one-phase transactions** (`startTS < commitTS`).  After the
first apply, `latestCommitTS(key) = commitTS`.  On re-apply,
`checkConflicts` evaluates `latestCommitTS(commitTS) > startTS` to
**true**, returning `ErrWriteConflict`.  The key is NOT re-written.
This is safe because `ErrWriteConflict` is returned as the FSM
**response value** (not as the `error` from `applyNormalEntry`); it
does not implement `HaltApply`.  The engine still calls
`setApplied(entry.Index)`, the FSM state is already correct from the
first apply, and the meta key is overwritten with the same value.
End state is observationally identical to "produced the same write."

Other request types are individually safe by similar reasoning:
`handleCommitRequest` short-circuits via
`applyCommitWithIdempotencyFallback`; `handleAbortRequest` is safe via
`shouldClearAbortKey`; `handlePrepareRequest` collapses through the
OCC write-conflict path when intents from prior applies are still
present.

### 8. Compatibility & rollback

- `StateMachine.Apply`'s public signature is **unchanged**.
- `ApplyIndexAware` is **already** in `main`; this design only adds
  consumers.
- The new opt-in interfaces (`AppliedIndexReader`,
  `AppliedIndexReporter`, `SnapshotHeaderApplier`) are additive.
  FSMs that don't implement them fall back to the current behaviour.
- `metaAppliedIndexBytes` is new.  Older fsm.db files don't have it.
  The `present=false` branch makes the first restart after upgrade
  fall back to full restore, populating the meta key from the next
  data Apply onward.
- Rollback: revert the wal_store.go skip branch.  The meta key
  remains in fsm.db (harmless dead data) and gets overwritten by
  future Applies.  No data migration in either direction.

### 9. Observability

Two metrics + one log line:

```text
fsm_cold_start_restore_total{outcome="executed|skipped|fallback"}
fsm_cold_start_applied_index_gap{outcome="executed|skipped"}
```

The gap label gets emitted in both directions so we can confirm the
**skip path actually closes the gap** (positive values when skipped
mean `LastAppliedIndex - snapshot.Index ≥ 0`) AND confirm restore
sizes when executed (`snapshot.Index - LastAppliedIndex > 0`).
Asymmetric emission would hide regressions where the skip succeeds
but the meta key is drifting behind.

A `fallback_reason` label on the `outcome=fallback` counter surfaces
why we conservatively restored even though the FSM was potentially
recent enough:

```text
fsm_cold_start_restore_total{outcome="fallback", fallback_reason="not_reporter|no_reader|read_err|missing_meta|behind_snapshot"}
```

`fallback_reason="behind_snapshot"` is the expected outcome for the
HLC-lease-only and encryption-only windows discussed in §6 — a
non-zero count there is healthy, not a regression.

Log line at INFO when skipping:

```text
restoreSnapshotState skipped (FSM at index %d, snapshot at %d, ceiling=%d, cutover=%d)
```

## Implementation Plan

| Branch | Content | Behaviour change |
|---|---|---|
| **B1** (this PR) | Design doc | None |
| **B2** | `ApplyMutationsRaftAt` / `DeletePrefixAtRaftAt` overloads + meta-key bundling in both leaves + `pebbleStore.LastAppliedIndex()` + `pebbleStore.SetDurableAppliedIndex()` (both with `dbMu.RLock()`) + `kvFSM.AppliedIndexReader()` accessor + `kvFSM.SetDurableAppliedIndex` forwarding + thread `f.pendingApplyIdx` into the data-Apply leaves + `persistCreatedSnapshot` calls `SetDurableAppliedIndex` BEFORE `e.persist.SaveSnap` | Meta key starts being written on every data Apply AND at every snapshot persist. Skip is still disabled. Soak in production for one release. |
| **B3** | `restoreSnapshotState` skip gate + `applyHeaderStateOnSkip` reusing `kv.ReadSnapshotHeader` + `SnapshotHeaderApplier` seam on `kvFSM` + metrics + INFO log | **User-visible cold-start win.** |
| **B4** | Lower `HEALTH_TIMEOUT_SECONDS` default once production data shows steady-state skip rate ≥ 90 % | Tighter ceiling; the env override remains honoured. |

Each of B2–B3 ships behind tests:

- **B2**: pebble-level tests round-trip the meta key in
  `applyMutationsWithOpts` AND `deletePrefixAtWithOpts`; torn-batch
  test simulates pebble WAL replay across the meta key boundary.
  A `kvFSM` unit test asserts that `SetApplyIndex(K)` immediately
  before a data `Apply` produces `LastAppliedIndex() == K`.  An
  engine-level test drives `persistCreatedSnapshot(snap)` against a
  store whose latest data Apply was at index `Y < snap.Metadata.Index`
  and asserts `LastAppliedIndex() == snap.Metadata.Index` after the
  call returns — verifying the snapshot-persist bump closes the
  codex round-3 P2 gap.  A separate test simulates a crash between
  `SetDurableAppliedIndex` and `SaveSnap` by injecting a SaveSnap
  failure and asserts the post-restart `LastAppliedIndex` is at least
  as fresh as the previous-snapshot pointer (over-restore impossible).
- **B3**: integration test seeds a fsm.db with `LastAppliedIndex = K`
  and pairs it with a snapshot at index `K-N` (N varies), asserting
  skip is taken for `N ≤ 0` and restore for `N > 0`.  A separate
  test asserts that `applyHeaderStateOnSkip` sets
  `f.hlc.PhysicalCeiling()` **and** `f.restoredCutover` for both v1
  and v2 snapshot headers — the ceiling+cutover are invariant under
  the optimisation.  An idle-cluster test runs a 3-node cluster with
  no data writes for `2 * SnapshotCount * hlcRenewalInterval`
  seconds, takes a snapshot, restarts a node, and asserts the skip
  fires — proving the codex P2 scenario is closed end-to-end.

## Open Questions

- **Multi-group**: each shard's `pebbleStore` has its own meta key.
  No shared state; this design is per-shard naturally.  B2 will
  verify with an integration test on a 4-group cluster.
- **HashiCorp backend** (if it ever returns): the raft library
  exposes `log.Index` to `(raft.FSM).Apply(log *raft.Log)`, so the
  `ApplyIndexAware` seam is satisfiable on that side too.
- **Future v3 snapshot header**: reusing `kv.ReadSnapshotHeader` means
  the skip path inherits the parser's forward-compat behaviour
  automatically.  If a new version adds a side-effect (e.g. a
  cluster-membership token), `SnapshotHeaderApplier` needs an
  additional method; the §3.2 read-path doc updates first, then this
  proposal extends.

## Out of Scope (future)

- Compressing/streaming the FSM snapshot file so the
  restore-execute path itself is faster.  Orthogonal; helps cases
  where we genuinely need to restore.
- Switching `restorePebbleNativeAtomic` to in-place ingest (e.g.
  Pebble's `Ingest`).  Risky and unrelated.

## Round-2 retraction

Round 2 of this doc claimed that `ApplyIndexAware`, `SetApplyIndex`,
`pendingApplyIdx`, `applyReservedOpcode`, `applyEncryption`,
`WriteSidecar`, and the v2 snapshot header were "fabricated" by
gemini and claude review bots — and proposed a fresh
`ApplyIndexAware` introduction as if the seam had to be created from
scratch.

That round-2 self-audit was wrong.  My `grep` was running against my
local working tree on `test/event-driven-leader-readiness`, which is
27 commits behind `origin/main`.  All of the entities above DO exist
on `origin/main`:

- `ApplyIndexAware` at `internal/raftengine/statemachine.go:46`.
- `kvFSM.SetApplyIndex` at `kv/fsm.go:122`, writing `f.pendingApplyIdx`
  at `kv/fsm.go:53`.
- `engine.applyNormalEntry` at `internal/raftengine/etcd/engine.go:2292-2293`
  calling `aware.SetApplyIndex(entry.Index)`.
- `applyReservedOpcode` at `kv/fsm.go` dispatching `raftEncodeHLCLease`
  (0x02) and the fsmwire encryption opcodes `OpRegistration` (0x03),
  `OpBootstrap` (0x04), `OpRotation` (0x05) — gemini's claimed
  `0x06..0x07` are reserved-for-future per
  `internal/encryption/fsmwire/wire.go:41`.
- `applyEncryption(f.pendingApplyIdx, op, payload)` — already
  consuming the index for the sidecar's `RaftAppliedIndex` via
  `encryption.WriteSidecar`.
- v2 snapshot header `EKVTHLC2` with ceiling + cutover and the
  `ReadSnapshotHeader(*bufio.Reader)` parser at `kv/snapshot.go`.

Round 3 rebases the whole design onto those existing seams.
Branch 2's scope shrinks from "create new seam + plumb" to "plumb
into the data-Apply leaves" — `ApplyIndexAware` and `pendingApplyIdx`
are already in place; we just start consuming the index for the
data-store meta key the way the encryption applier already consumes
it for the sidecar.

The codex P1 about v2 snapshot header state preservation (which
round-2 mis-classified as fabricated) is the SAME class of bug as
the round-1 HLC ceiling P1: both are real, both ride on the same
`ReadSnapshotHeader` parser, and §5 now handles them together via
the `SnapshotHeaderApplier` seam.

Apologies to the review bots for the round-2 push-back.  Round 3
proceeds against the actual code.

## Round-3 retraction (codex P2)

Round 3 of this doc declared the HLC-lease-only fallback to be "safe
and rare" and rejected adding any synthetic pebble write for it.
Codex's round-3 review (P2 at `:438`) pointed out the actual
production cadence:

- `RunHLCLeaseRenewal` (`kv/coordinator.go:650`) ticks at
  `hlcRenewalInterval = 1 * time.Second` while the local node is
  leader.
- `applyHLCLease` is memory-only; `metaAppliedIndex` does not advance
  on lease apply.
- For any cluster with a leader running for >1 s, lease entries trail
  every snapshot.  Snapshots persist at index `X`, the last
  data-Apply index `Y < X`, and `metaAppliedIndex` stays at `Y` on
  restart.  `fsmAlreadyAtIndex(X)` checks `Y >= X` → false → full
  restore.  Idle clusters degenerate to "the skip never fires";
  active clusters have a meaningful window where it doesn't fire.

Round 3's framing of this as "rare" was wrong.  Round 4 (§6 HLC lease
subsection + B2 row + B2 test list) closes the gap by bumping
`metaAppliedIndex` to `snapshot.Metadata.Index` inside
`persistCreatedSnapshot`, **before** `e.persist.SaveSnap`.  After a
successful snapshot persist, `LastAppliedIndex >= snapshot.Index`
holds unconditionally, so the skip fires reliably on the next
restart.  Cost: one extra pebble `Batch.Commit` per snapshot persist
(~one extra fsync per `SnapshotCount` entries, default 10000) versus
Option A's continuous ~1 fsync/sec/group.

Lesson: "rare" should be a quantitative claim against the actual
production timer cadence, not an intuition.  Codex's review process
explicitly cited `kv/coordinator.go:641-663` — a file:line that I
could have consulted before making the round-3 claim.
