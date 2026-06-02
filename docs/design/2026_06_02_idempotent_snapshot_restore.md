# Idempotent FSM Snapshot Restore on Cold Start

**Status**: Proposal
**Date**: 2026-06-02
**Author**: bootjp
**Related**: PR #909 (`HEALTH_TIMEOUT_SECONDS` 60s → 300s)

## Problem

`loadWalState` (`internal/raftengine/etcd/wal_store.go:117`) unconditionally calls
`restoreSnapshotState(fsm, snapshot, fsmSnapDir)` (`:246`) whenever the WAL's
persisted snapshot pointer is non-empty.  For the EKV/token-format snapshot
case this dispatches to:

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
192.168.0.x cluster with a ~5 M-key node we measured **~46 s** between
the first `pebble.Open(fsm.db)` log line and the post-swap re-open.
After the swap the engine still has to:

- replay WAL entries between the snapshot index and the latest committed
  index (currently ~4 k entries → sub-second), and
- become a raft follower, then bind the gRPC listener.

The cumulative cold-start budget routinely exceeded the 60-second
`HEALTH_TIMEOUT_SECONDS` (PR #909 raises it to 300 s as a band-aid).
Beyond the wall-clock pain, the timeout creates **second-order
instability**: docker's restart policy re-investigates a non-responsive
container, the remaining quorum runs elections against a phantom voter,
and the raft term inflates linearly with restart attempts (we observed
term 665 on a cluster that should have seen single-digit-per-day
elections).

The restore is **mostly redundant**.  Each successful `fsm.Apply` already
persists its mutation durably (via Pebble's WriteBatch).  After the FSM
applied entry `Y > snapshot.Metadata.Index = X`, the on-disk fsm.db
contains state `≥ X`.  On the next cold start we tear down that state
and rebuild it from the older snapshot — only to have raft replay the
same entries we already had on disk.

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
post-disaster recovery from someone else's snapshot), or (b)
`LastAppliedIndex` is missing / corrupt.

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

### 1. Persist `LastAppliedIndex` atomically with each Apply

The smallest correct primitive is to bundle the raft-applied-index into
the same `pebble.Batch` that the FSM uses for its data mutation.  Pebble
batches are atomic, so the index and the data move together — never
torn.

#### Interface change

```go
// internal/raftengine/statemachine.go
type StateMachine interface {
    Apply(index uint64, data []byte) any  // was: Apply(data []byte) any
    Snapshot() (Snapshot, error)
    Restore(r io.Reader) error
}
```

The single producer (`Engine.applyNormalEntry`, `engine.go:1769`)
becomes:

```go
return e.fsm.Apply(entry.Index, payload)
```

All in-tree implementations and tests inherit the new signature.
Inventoried sites (8 in `internal/raftengine/`, 1 in `kv/fsm.go`, plus
the `raftenginetest/conformance.go` shim, plus a handful of unit-test
fakes).  The change is mechanical; reviewers can audit by grepping for
`Apply(data []byte)` after the rebase.

#### kvFSM commits index + mutation in one batch

```go
// kv/fsm.go
func (f *kvFSM) Apply(index uint64, data []byte) any {
    if len(data) > 0 && data[0] == raftEncodeHLCLease {
        return f.applyHLCLease(index, data[1:])
    }
    ctx := context.TODO()
    reqs, err := decodeRaftRequests(data)
    if err != nil { return errors.WithStack(err) }
    return f.applyAtIndex(ctx, index, reqs)
}
```

`applyAtIndex` threads `index` through `applyRequest` /
`applyRequestErr`; the leaf MVCC mutation acquires the `pebble.Batch`,
adds `key=metaAppliedIndex, value=binary.BigEndian.PutUint64(index)`,
and commits.  The cost is +16 bytes per batch and zero extra fsync
calls (the batch was already going to commit).

#### pebbleStore exposes the index

```go
// store/lsm_store.go (new method)
func (s *pebbleStore) LastAppliedIndex() (uint64, bool, error) {
    val, closer, err := s.db.Get(metaAppliedIndexKey)
    if errors.Is(err, pebble.ErrNotFound) {
        return 0, false, nil
    }
    if err != nil { return 0, false, errors.WithStack(err) }
    defer closer.Close()
    if len(val) != 8 {
        return 0, false, errors.Newf("corrupt applied-index meta key: %d bytes", len(val))
    }
    return binary.BigEndian.Uint64(val), true, nil
}
```

A narrow `AppliedIndexReader` interface lets `restoreSnapshotState`
inspect any store that implements it, without coupling the wal_store
package to `*pebbleStore`:

```go
// internal/raftengine/statemachine.go (or sibling)
type AppliedIndexReader interface {
    LastAppliedIndex() (uint64, bool, error)
}
```

### 2. Conditional restore

```go
// internal/raftengine/etcd/wal_store.go
func restoreSnapshotState(fsm StateMachine, snapshot raftpb.Snapshot, fsmSnapDir string) error {
    if etcdraft.IsEmptySnap(snapshot) || len(snapshot.Data) == 0 || fsm == nil {
        return nil
    }
    if isSnapshotToken(snapshot.Data) {
        tok, err := decodeSnapshotToken(snapshot.Data)
        if err != nil { return err }
        if skip, err := fsmAlreadyAtIndex(fsm, tok.Index); err != nil {
            return err
        } else if skip {
            // FSM data on disk is at least as fresh as the snapshot.
            // Raft will replay [tok.Index+1, committed] from the WAL
            // and bring us to the latest committed index without
            // touching the LSM's bulk state.
            return nil
        }
        return openAndRestoreFSMSnapshot(fsm, fsmSnapPath(fsmSnapDir, tok.Index), tok.CRC32C)
    }
    // Legacy non-token path: unchanged.
    return errors.WithStack(fsm.Restore(bytes.NewReader(snapshot.Data)))
}

func fsmAlreadyAtIndex(fsm StateMachine, want uint64) (bool, error) {
    r, ok := fsm.(AppliedIndexReader)
    if !ok {
        return false, nil // FSM cannot self-report; restore conservatively.
    }
    have, present, err := r.LastAppliedIndex()
    if err != nil { return false, err }
    if !present { return false, nil }
    return have >= want, nil
}
```

The fall-back behaviour when the FSM does not implement
`AppliedIndexReader` or has no meta-key is **the current restore**, so
the change is strictly additive.  The first cold start after deployment
populates the meta key on every Apply going forward; from the second
restart onward the skip path kicks in.

### 3. Crash-safety argument

We claim: `LastAppliedIndex = N` durably implies "every mutation
produced by `fsm.Apply` for indices `[snapshotIndex+1 .. N]` is durably
present in fsm.db."

Proof: the meta key is written **in the same Pebble batch** as the data
mutation for index N.  Pebble commits batches atomically.  Either both
land or neither does.  After commit, both are durable per the batch's
`pebble.Sync` option.  No crash window exists between persisting the
mutation and persisting the index.

Suppose a crash mid-Apply.  Pebble's WAL replay either:
- restores the batch (data + index both present), or
- discards the torn batch (data + index both absent).

Either way the invariant holds; on the second startup the meta-key is
either exactly the index of the last committed Apply, or strictly less.
`LastAppliedIndex ≥ snapshot.Metadata.Index` correctly implies the FSM
is at least as fresh as the snapshot.

The converse risk — "skip restore when we shouldn't" — would require
the meta key to read **a value larger than the last durable Apply**.
Since the only writer is `fsm.Apply` and it always batches index with
data, that's impossible barring physical corruption (in which case the
8-byte length check rejects the read and we restore conservatively).

### 4. Idempotency of replay after skip

After we skip restore, raft replays entries `[snapshotIndex+1, committed]`.
Some of those entries may have already been applied (if a previous
restart reached `applied=K > snapshotIndex` before crashing).  We do
**not** suppress replay of already-applied entries — `fsm.Apply` is
required to be idempotent for snapshot recovery in any raft FSM, and
the current kvFSM upholds that (every mutation is keyed by its raft
metadata; re-applying the same entry produces the same Pebble write).
The meta-key gets overwritten with the same value.  No correctness
gap; the cost is the same replay we'd pay after a full restore.

### 5. Compatibility & rollback

- The interface change `Apply(data) → Apply(index, data)` is breaking
  for **external** state machines that embed `raftengine.StateMachine`.
  No such consumers exist in-tree.  External consumers (none known)
  would notice immediately at compile time.
- The meta key is new; older fsm.db files don't have it.  The
  `present == false` branch makes the first restart after upgrade
  fall back to the current (full restore) behaviour, populating the
  meta key from the next Apply onward.
- Rollback: revert the wal_store.go change to remove the skip branch.
  The meta key remains in fsm.db (harmless dead data) and gets
  overwritten by future Applies.  No data migration required either
  direction.

### 6. Observability

Two metrics + one log line:

```go
// monitoring/metrics.go
fsm_cold_start_restore_total{outcome="executed|skipped|fallback"}
fsm_cold_start_applied_index_gap // snapshot.Index - lastAppliedIndex when restore is executed
```

Log line at INFO when skipping:

```
restoreSnapshotState skipped (FSM already at index N >= snapshot index X)
```

These let us verify the skip rate in production and catch regressions
where Apply stops bundling the index.

## Implementation Plan

1. **Branch 1 (this design)** — PR-2-design, lands docs only.
2. **Branch 2 (interface change)** — modify `StateMachine.Apply`
   signature across all callers + the conformance shim.  No behaviour
   change; index is accepted and discarded.  Pure mechanical PR with
   ~200-line diff, easy review.
3. **Branch 3 (meta key + batch bundling)** — `kvFSM.Apply` threads
   `index` to leaf mutation; `pebbleStore` defines the meta key and
   exposes `LastAppliedIndex()`; unit tests cover round-trip + torn
   batch.  Activates the data side; the skip is **not yet wired**.
4. **Branch 4 (wal_store.go skip)** — adds the `fsmAlreadyAtIndex` gate
   to `restoreSnapshotState`, with the metrics and the log line.  This
   is the user-visible perf win.
5. **Branch 5 (cleanup)** — once branch 4 has soaked in production
   for a release, consider lowering `HEALTH_TIMEOUT_SECONDS` back
   toward a tighter ceiling.

Branches 2–4 each ship behind tests that prove the corresponding
invariant.  Branch 4 specifically asserts:

- Skip happens when `stored ≥ snapshot.Metadata.Index`.
- Restore runs when `stored < snapshot.Metadata.Index`.
- Restore runs when the meta key is missing.
- Restore runs when `LastAppliedIndex()` returns an error.

## Open Questions

- **Multi-group**: each shard's pebbleStore has its own meta key.  No
  shared state; this design is per-shard naturally.  Sanity-check during
  branch 3.
- **HLC lease entries**: `kvFSM.applyHLCLease` advances `f.hlc` but does
  not currently touch the store.  We must still bump the meta key on
  lease applies (a degenerate batch with just the meta key, or — better
  — fold the lease index into the next data Apply).  Decision deferred
  to branch 3; the in-memory HLC state is reconstructed at startup from
  the lease entries replayed by raft, so this is purely about getting
  the index counter monotonic.
- **HashiCorp backend** (if it ever returns): the raft library exposes
  `log.Index` to `(raft.FSM).Apply(log *raft.Log)`, so the interface
  change is satisfiable on that side too.

## Out of Scope (future)

- Compressing/streaming the FSM snapshot file so the restore-execute
  path itself is faster.  Orthogonal; helps cases where we genuinely
  need to restore.
- Switching `restorePebbleNativeAtomic` to in-place ingest (e.g.
  Pebble's `Ingest`).  Risky and unrelated to the skip-when-safe
  optimisation this proposal targets.
