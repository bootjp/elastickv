# Idempotent FSM Snapshot Restore on Cold Start

**Status**: Proposal (Round 2 — revised after PR #910 feedback)
**Date**: 2026-06-02 (round 1), 2026-06-03 (round 2 revision)
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
`LastAppliedIndex` is missing / corrupt, or (c) the HLC physical
ceiling embedded in the snapshot file is newer than what the FSM has
in memory (see §HLC).

## Non-Goals

- We are **not** changing the snapshot-install hot path
  (`Engine.applySnapshot`, `engine.go:1641`).  That path runs at
  runtime when a leader ships us a snapshot we genuinely don't have —
  the FSM is stale by construction there and `Restore` must run.
- We are **not** changing the legacy non-token snapshot path
  (`fsm.Restore(bytes.NewReader(snapshot.Data))` at `wal_store.go:258`).
  Those payloads are only encountered during the one-shot hashicorp →
  etcd migration; the cost is paid once and the legacy branch is on a
  removal trajectory.

## Design

### 1. Plumb `entry.Index` to `kvFSM.Apply` via a new `ApplyIndexAware` seam

The current `StateMachine` interface (`internal/raftengine/statemachine.go:14`)
does not carry the raft index:

```go
type StateMachine interface {
    Apply(data []byte) any
    Snapshot() (Snapshot, error)
    Restore(r io.Reader) error
}
```

**Round-1 mistake**: the round-1 design proposed breaking this signature
to `Apply(index uint64, data []byte)`.  That works but rebinds every
StateMachine implementation in tree.

**Round-2 design**: introduce a **new** opt-in interface alongside it
(no breaking change to `StateMachine.Apply`):

```go
// internal/raftengine/statemachine.go (new — does not exist today)
//
// ApplyIndexAware is an opt-in seam that delivers the raft applied-index
// to the FSM without changing StateMachine.Apply's signature. The engine
// calls SetApplyIndex on the same goroutine, immediately before Apply,
// so the FSM can read it back from a field stashed during SetApplyIndex.
//
// FSMs that do not implement this interface continue to work as before;
// their cold start will pay the full restoreSnapshotState cost because
// they cannot self-report LastAppliedIndex().
type ApplyIndexAware interface {
    SetApplyIndex(idx uint64)
}
```

`engine.applyNormalEntry` (`engine.go:1769`) calls it before `Apply`:

```go
func (e *Engine) applyNormalEntry(entry raftpb.Entry) any {
    if len(entry.Data) == 0 { return nil }
    _, payload, ok := decodeProposalEnvelope(entry.Data)
    if !ok { return nil }
    if aware, ok := e.fsm.(ApplyIndexAware); ok {
        aware.SetApplyIndex(entry.Index)
    }
    return e.fsm.Apply(payload)
}
```

Single-writer invariant: the engine's raft run loop is the only caller
of `applyNormalEntry` and the only caller of `setApplied` (`engine.go:1764`).
SetApplyIndex / Apply pair is observed by no other goroutine; no
synchronisation between them is required.

**Compatibility audit** — concrete `StateMachine` implementations in
this repository (count: **6**, verified by `grep 'func.*Apply(data \[\]byte) any'`):

| File | Symbol | Action in Branch 2 |
|---|---|---|
| `kv/fsm.go:60` | `(*kvFSM).Apply` | Add `SetApplyIndex(idx)` storing into `f.pendingApplyIdx`. Read it inside `Apply`. |
| `internal/raftengine/raftenginetest/conformance.go:27` | `(*TestStateMachine).Apply` | No change unless test wants to assert index. |
| `internal/raftengine/etcd/engine_test.go:33` | `testStateMachine` | No change. |
| `internal/raftengine/etcd/engine_test.go:139` | `blockingSnapshotStateMachine` | No change. |
| `internal/raftengine/etcd/engine_test.go:170` | `countingSnapshotStateMachine` | No change. |
| `internal/raftengine/etcd/fsm_snapshot_file_test.go:124` | `dummyFSM` | No change. |

(Round-1 doc claimed "8 in `internal/raftengine/`" — that was wrong;
the actual count is 5 non-`kvFSM` impls.  Corrected.)

Only `kvFSM` needs to implement `ApplyIndexAware`; the rest stay on the
non-aware path.  Cold-start skip is enabled only when both the FSM
implements `ApplyIndexAware` **and** the store implements
`AppliedIndexReader`.

### 2. `kvFSM` bundles the index in the SAME pebble.Batch as the mutation

`kvFSM.Apply` doesn't directly touch Pebble — it dispatches through
`f.store.ApplyMutationsRaft(...)`, `f.store.DeletePrefixAtRaft(...)`,
etc., which build their own `pebble.Batch` inside `pebbleStore`.  To
land the index in the same batch as the mutation, we plumb it from
`kvFSM.Apply` down to the leaf `applyMutationsWithOpts` /
`deletePrefixAtWithOpts` helpers (both already bundle
`metaLastCommitTSBytes` — the precedent for batched meta keys lives at
`lsm_store.go:1162` and `:1231`).

```go
// kv/fsm.go — round-2: keep the StateMachine.Apply signature
type kvFSM struct {
    store           store.MVCCStore
    log             *slog.Logger
    hlc             *HLC
    pendingApplyIdx uint64    // set by SetApplyIndex immediately before Apply
}

func (f *kvFSM) SetApplyIndex(idx uint64) {
    f.pendingApplyIdx = idx
}

func (f *kvFSM) Apply(data []byte) any {
    idx := f.pendingApplyIdx
    // CRITICAL: leave dispatch (HLC-lease vs request) UNCHANGED. We only
    // pass `idx` down to the leaf MVCC mutation. The original opcode
    // discrimination (`data[0] == raftEncodeHLCLease`) at kv/fsm.go:63
    // and the existing decodeRaftRequests / applyRequest sequence
    // continue verbatim. Only the leaf store call gains an index
    // parameter, which is bundled atomically with the data write.
    // ... existing dispatch ...
}
```

The store interface gains an index-carrying overload, so the raft-apply
path can carry it down without breaking direct-write paths:

```go
// store/store.go (new method on MVCCStore)
ApplyMutationsRaftAt(
    ctx context.Context,
    muts []*KVPairMutation,
    readKeys [][]byte,
    startTS, commitTS uint64,
    appliedIndex uint64,
) error

DeletePrefixAtRaftAt(
    ctx context.Context,
    prefix, excludePrefix []byte,
    commitTS uint64,
    appliedIndex uint64,
) error
```

(The existing `ApplyMutationsRaft` / `DeletePrefixAtRaft` become thin
wrappers that pass `appliedIndex=0` — the leaf helper treats `0` as
"don't write the meta key", preserving direct-write semantics for
callers outside the raft apply loop.)

Implementation in the leaf:

```go
// store/lsm_store.go — applyMutationsWithOpts (existing, around :1162)
if appliedIndex > 0 {
    if err := setPebbleUint64InBatch(b, metaAppliedIndexBytes, appliedIndex); err != nil {
        return errors.WithStack(err)
    }
}
// ... existing metaLastCommitTSBytes write at :1162 stays as-is ...

// store/lsm_store.go — deletePrefixAtWithOpts (existing, around :1231)
if appliedIndex > 0 {
    if err := setPebbleUint64InBatch(batch, metaAppliedIndexBytes, appliedIndex); err != nil {
        return errors.WithStack(err)
    }
}
// ... existing metaLastCommitTSBytes write at :1231 stays as-is ...
```

**Why both leaves**: `handleDelPrefix` → `DeletePrefixAtRaft` builds an
independent `pebble.Batch` via `deletePrefixAtWithOpts` (round-1 doc
missed this).  Threading the index ONLY through `applyMutationsWithOpts`
would let DEL_PREFIX entries land in fsm.db without bumping
`metaAppliedIndex`, which would silently leave the meta key behind the
true applied index — and on the next cold start, `LastAppliedIndex`
would underestimate, triggering a conservative full restore.  Cost: +16
bytes per batch, zero additional fsync.

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

`metaAppliedIndexBytes` is a new well-known prefix sibling to
`metaLastCommitTSBytes` (which is defined at `lsm_store.go:145` as
`[]byte("_meta_last_commit_ts")`).  Recommended value:
`[]byte("_meta_applied_index")` — outside the MVCC user-key space and
disjoint from every existing meta key (verified by reading
`isReservedMetaKey` at `lsm_store.go:454`, which currently only checks
`metaLastCommitTSBytes` and `metaMinRetainedTSBytes`).  Branch 3 extends
`isReservedMetaKey` to include the new key.

A narrow consumer interface lets `restoreSnapshotState` inspect the
store without coupling the etcd raft engine to `*pebbleStore`:

```go
// internal/raftengine/statemachine.go (new)
type AppliedIndexReader interface {
    LastAppliedIndex() (uint64, bool, error)
}
```

**Reaching the store from inside `restoreSnapshotState`**: `kvFSM` holds
its store via `f.store`, but `kvFSM` itself does not satisfy
`AppliedIndexReader`.  The cleanest seam is to add a typed accessor on
`kvFSM` (`func (f *kvFSM) AppliedIndexReader() AppliedIndexReader { return f.store }`)
which `restoreSnapshotState` calls when the FSM implements a tiny
companion interface:

```go
type AppliedIndexReporter interface {
    AppliedIndexReader() AppliedIndexReader
}
```

This keeps the public `StateMachine` interface untouched and avoids
fattening `kvFSM` with a forwarding method.  FSMs that don't expose the
reporter fall back to the current full-restore behaviour.

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
            // The body restore is skipped, but we MUST still read the
            // HLC ceiling header from the snapshot file so the FSM's
            // in-memory HLC starts at the right floor (see §HLC).
            return applyHLCCeilingFromSnapshot(fsm, fsmSnapPath(fsmSnapDir, tok.Index))
        }
        return openAndRestoreFSMSnapshot(fsm, fsmSnapPath(fsmSnapDir, tok.Index), tok.CRC32C)
    }
    // Legacy non-token path: unchanged.
    return errors.WithStack(fsm.Restore(bytes.NewReader(snapshot.Data)))
}

// fsmAlreadyAtIndex returns true ONLY when we can prove the FSM is
// already at or past `want`. Any uncertainty -- FSM doesn't expose the
// reporter, store doesn't expose the reader, read error, or missing
// meta key -- returns false so we fall back to the full restore. The
// idea is that a stale-but-incorrect skip is far worse than a wasteful
// full restore, so we err strictly toward restoring.
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

**Round-1 mistake (fixed)**: the round-1 pseudocode propagated
`LastAppliedIndex()` errors upward, which would fail node startup on a
corrupt meta key.  Round-2 fallback policy: any error / missing /
uncertainty maps to `false` (run full restore).  This honours the
"strictly additive" guarantee — adoption can never make startup worse
than the current behaviour, only better.

### 5. HLC ceiling preservation when skipping (P1 from review)

`kvFSM.Restore` (`kv/fsm.go:264`) is currently the only place that reads
the HLC physical-ceiling header from the snapshot file and applies it
to the in-memory `f.hlc`:

```go
// kv/fsm.go:278-286 (existing)
if bytes.Equal(hdr[:8], hlcSnapshotMagic[:]) {
    ceilingMs := int64(binary.BigEndian.Uint64(hdr[8:]))
    if f.hlc != nil && ceilingMs > 0 {
        f.hlc.SetPhysicalCeiling(ceilingMs)
    }
    return errors.WithStack(f.store.Restore(io.NopCloser(r)))
}
```

If we skip `Restore` we also skip this assignment.  After skip:

- `f.hlc.physicalCeiling` stays at whatever the in-memory HLC was
  initialised to (typically wall-clock-now, no snapshot floor).
- Subsequent HLC lease entries in the WAL still bump the ceiling when
  replayed by raft, BUT the lease entries are **between the snapshot
  index and the latest committed index** — those whose ceiling was
  baked into the snapshot itself (lease entries whose index ≤
  `snapshot.Metadata.Index`) are compacted away from the WAL and never
  replayed.
- Net effect: if the snapshot was taken at ceiling C and no fresh
  lease has bumped past C since, this node could mint timestamps
  below C after becoming leader.

This violates the HLC monotonicity invariant cluster-wide.  **The skip
path MUST set the HLC ceiling from the snapshot header even when the
body restore is skipped.**

Mechanism — a new helper that reads the first 16 bytes of the snapshot
file and dispatches:

```go
// internal/raftengine/etcd/wal_store.go (new)
func applyHLCCeilingFromSnapshot(fsm StateMachine, snapPath string) error {
    setter, ok := fsm.(HLCCeilingSetter)
    if !ok { return nil } // FSM doesn't carry HLC; nothing to set.
    f, err := os.Open(snapPath)
    if err != nil { return errors.WithStack(err) }
    defer f.Close()
    var hdr [hlcSnapshotHeaderLen]byte
    n, err := io.ReadFull(f, hdr[:])
    if err != nil {
        // The snapshot file is too short for an HLC header. Predates
        // the magic/ceiling format -- nothing to apply, fall through.
        if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
            return nil
        }
        return errors.WithStack(err)
    }
    _ = n
    if !bytes.Equal(hdr[:8], hlcSnapshotMagic[:]) {
        return nil // old format, no ceiling.
    }
    ceilingMs := int64(binary.BigEndian.Uint64(hdr[8:]))
    if ceilingMs > 0 {
        setter.SetHLCPhysicalCeiling(ceilingMs)
    }
    return nil
}

// kv/fsm.go gains:
type HLCCeilingSetter interface {
    SetHLCPhysicalCeiling(ms int64)
}
func (f *kvFSM) SetHLCPhysicalCeiling(ms int64) {
    if f.hlc != nil { f.hlc.SetPhysicalCeiling(ms) }
}
```

Cost: a 16-byte read from the snapshot file (single syscall, no
deserialisation).  Negligible compared to the 46 s body restore we
elide.

### 6. Crash-safety argument (revised)

We claim: `LastAppliedIndex = N` durably implies "every raft apply for
indices `[snapshotIndex+1 .. N]` that produced a `pebbleStore` mutation
is durably present in fsm.db."

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
  committed batch (or none of it), giving us either `(data + meta)` at
  index N or `(neither)`.
- raft will then replay any entries that the OS-buffered fsync lost,
  and each Apply re-bundles its own index.

The skip gate therefore stays correct: a `nosync`-induced loss merely
means `LastAppliedIndex` reports a lower value than the post-restart
applied count would imply, which only causes us to over-restore
conservatively.

#### DEL_PREFIX path

`DeletePrefixAtRaft` → `deletePrefixAtWithOpts` builds its own batch
(`lsm_store.go:1196`) and bundles `metaLastCommitTSBytes`
(`lsm_store.go:1231`).  Branch 3 extends that bundling to also include
`metaAppliedIndex`.  Without this, DEL_PREFIX entries land without
bumping the meta key and `LastAppliedIndex` drifts behind the true
applied count (still safe — over-restoring conservatively — but
defeats the optimisation for any workload that uses DEL_PREFIX).

#### HLC lease entries (Open Question 1 — committed to Option 2)

`f.applyHLCLease` (kv/fsm.go around `:63`) is currently an in-memory
mutation only; it does not touch `f.store`.  We accept that
`LastAppliedIndex` does NOT advance for these entries.  Consequence:
in a workload where the only entries between two snapshots are lease
ticks, the skip gate will fall back to full restore — which is safe
and rare.

Adding a synthetic pebble batch per lease tick (Option 1) would cost
~1 extra pebble batch per second per group (lease cadence is 1 s),
which is undesirable.  Option 2 (do nothing extra) is correct and
cheaper; the trade-off is the rare false-positive restore.

### 7. Idempotency of replay after skip (revised — OCC two-case)

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
`shouldClearAbortKey` (no-op if the lock is already gone);
`handlePrepareRequest` collapses through the OCC write-conflict path
when intents from prior applies are still present.

### 8. Compatibility & rollback

- `StateMachine.Apply`'s public signature is **unchanged**.  External
  state machines (none known) continue to work.
- The new opt-in interfaces (`ApplyIndexAware`, `AppliedIndexReader`,
  `AppliedIndexReporter`, `HLCCeilingSetter`) are additive.  FSMs that
  don't implement them fall back to the current behaviour.
- The `metaAppliedIndex` key is new; older fsm.db files don't have
  it.  The `present=false` branch makes the first restart after upgrade
  fall back to full restore, populating the meta key from the next
  Apply onward.
- Rollback: revert the wal_store.go skip branch.  The meta key remains
  in fsm.db (harmless dead data) and gets overwritten by future
  Applies.  No data migration in either direction.

### 9. Observability

Two metrics + one log line:

```
fsm_cold_start_restore_total{outcome="executed|skipped|fallback"}
fsm_cold_start_applied_index_gap{outcome="executed|skipped"}
```

The gap label gets emitted in both directions so we can confirm the
**skip path actually closes the gap** (positive values when skipped
mean `LastAppliedIndex - snapshot.Index ≥ 0`) AND confirm restore
sizes when executed (`snapshot.Index - LastAppliedIndex > 0`).
Asymmetric emission would hide regressions where the skip succeeds but
the meta key is drifting behind.

Log line at INFO when skipping:

```
restoreSnapshotState skipped (FSM at index %d, snapshot at %d, HLC ceiling applied from header)
```

## Implementation Plan

| Branch | Content | Behaviour change |
|---|---|---|
| **B1** (this PR) | Design doc | None |
| **B2** | New `ApplyIndexAware` interface + `kvFSM.SetApplyIndex` + engine.go call site | None (engine starts feeding index; FSM stashes it; nothing reads it yet) |
| **B3** | Thread `appliedIndex` through `ApplyMutationsRaftAt` / `DeletePrefixAtRaftAt` + bundle meta key in both leaves + `pebbleStore.LastAppliedIndex()` with `dbMu.RLock()` + `kvFSM.AppliedIndexReader()` accessor | Meta key starts being written; skip is still disabled. Soak in production for one release. |
| **B4** | `restoreSnapshotState` skip gate + `applyHLCCeilingFromSnapshot` + `kvFSM.SetHLCPhysicalCeiling` + metrics + INFO log | **User-visible cold-start win.** |
| **B5** | Lower `HEALTH_TIMEOUT_SECONDS` default once production data shows steady-state skip rate ≥ 90 % | Tighter ceiling, but tracker the env override still honoured |

Each of B2–B4 ships behind tests:

- **B2**: `engine_test.go` asserts that a recorder FSM observes
  `SetApplyIndex(entry.Index)` immediately before `Apply(data)` for
  every Apply.
- **B3**: pebble-level tests round-trip the meta key in
  `applyMutationsWithOpts` AND `deletePrefixAtWithOpts`; torn-batch
  test simulates pebble WAL replay across the meta key boundary.
- **B4**: integration test seeds a fsm.db with `LastAppliedIndex = K`,
  pairs it with a snapshot at index `K-N` (N varies), asserts skip is
  taken for `N ≤ 0` and restore for `N > 0`.  Separate test asserts
  that `applyHLCCeilingFromSnapshot` sets `f.hlc.PhysicalCeiling()`
  for both skip and restore paths (i.e., the ceiling is invariant
  under the optimisation).

## Errata against Round-1 reviewer feedback

For honesty about the review process: round 1 received feedback from
`gemini-code-assist[bot]` and `claude[bot]` that referenced
codebase entities that **do not exist in this repository**.  Verified
by `grep -r ... --include='*.go' .` over `main` HEAD at SHA
`94579fc0`.

| Cited entity | Grep matches | Reviewer claim | Reality |
|---|---|---|---|
| `ApplyIndexAware` | 0 | "already in production" (gemini), "engine.go:2292" (claude) | Does not exist. Engine.go:2292 is `failPending`. |
| `SetApplyIndex` | 0 | "already implemented on kvFSM" | Does not exist on `kvFSM`. |
| `pendingApplyIdx` | 0 | "already holds entry.Index" | Field does not exist. |
| `applyReservedOpcode` | 0 | "dispatches HLC lease + encryption opcodes 0x03..0x07" | Function does not exist. |
| `applyEncryption` | 0 | "goes via WriteSidecar" | Function does not exist. |
| `WriteSidecar` | 0 | (same as above) | Method does not exist. |
| Encryption opcodes `0x03..0x07` | 0 | "Stage 6/7/8 encryption dispatch" | Only `raftEncodeSingle=0x00`, `raftEncodeBatch=0x01`, `raftEncodeHLCLease=0x02` exist (`kv/fsm.go:110-116`). |

This design therefore proceeds with **Branch 2 in place** (we have to
create the `ApplyIndexAware` seam; we cannot reuse one that doesn't
exist).  The reviewers' reasoning about "avoid a breaking
`StateMachine.Apply` signature change" was sound regardless of the
factual error — we adopt it by creating the seam they hallucinated as
existing.  Encryption-opcode dispatch is not addressed because the
opcodes do not exist in main today; once Stages 6/7/8 land, the
opcode dispatch will need to plumb `appliedIndex` through whatever
path it takes (a follow-up problem, not a round-2 problem).

The **substantively correct** findings from round-1 reviews — all
incorporated:

- 🔴 codex P1 on `:319`: HLC ceiling preservation when skipping
  body restore (`kv/fsm.go:264-286` does read the header).  **Real
  bug**, fixed in §5.
- 🟠 gemini medium on `:152`: `LastAppliedIndex()` needs
  `s.dbMu.RLock()` per `lsm_store.go:153 / :162 / :553 / :675` lock
  ordering.  **Real**, fixed in §3.
- 🟠 claude §3 caveat 1: `ELASTICKV_FSM_SYNC_MODE=nosync`
  (`lsm_store.go:86`) shifts durability boundary.  **Real**, fixed
  in §6.
- 🟠 claude §3 caveat 2: `deletePrefixAtWithOpts` (`lsm_store.go:1196`)
  builds an independent batch and already bundles
  `metaLastCommitTSBytes` (`:1231`).  **Real**, fixed in §2.
- 🟠 claude §3 error handling: `fsmAlreadyAtIndex` propagating errors
  would fail cold start on a corrupt meta key.  **Sound design
  improvement**, fixed in §4.
- 🟢 claude §4 OCC two-case argument and call-site count (6 not 8).
  **Real**, fixed in §7 and §1's table.

## Open Questions

- **Multi-group**: each shard's `pebbleStore` has its own meta key.
  No shared state; this design is per-shard naturally.  Branch 3 will
  verify with an integration test on a 4-group cluster.
- **HashiCorp backend** (if it ever returns): the raft library exposes
  `log.Index` to `(raft.FSM).Apply(log *raft.Log)`, so the
  `ApplyIndexAware` seam is satisfiable on that side too.

## Out of Scope (future)

- Compressing/streaming the FSM snapshot file so the restore-execute
  path itself is faster.  Orthogonal; helps cases where we genuinely
  need to restore.
- Switching `restorePebbleNativeAtomic` to in-place ingest (e.g.
  Pebble's `Ingest`).  Risky and unrelated to the skip-when-safe
  optimisation this proposal targets.
- Plumbing `appliedIndex` through future encryption-opcode dispatch
  (Stages 6/7/8) — once those opcodes exist on `main`, they will need
  the same treatment as DEL_PREFIX.
