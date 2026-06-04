package raftengine

import "io"

// Snapshot is an owned export handle from the state machine. Callers are
// responsible for closing it after WriteTo completes.
type Snapshot interface {
	WriteTo(w io.Writer) (int64, error)
	Close() error
}

// StateMachine is the interface that engine-agnostic state machines must
// implement. Both the hashicorp and etcd backends use this contract.
type StateMachine interface {
	Apply(data []byte) any
	// Snapshot should capture a stable export handle quickly. Expensive snapshot
	// serialization belongs in Snapshot.WriteTo, which the engine can run off
	// the main raft loop.
	Snapshot() (Snapshot, error)
	Restore(r io.Reader) error
}

// ApplyIndexAware is an OPTIONAL extension of StateMachine that lets
// the engine communicate the Raft entry index of the entry being
// applied. The engine calls SetApplyIndex IMMEDIATELY before each
// successful Apply (i.e. on the same goroutine that will then call
// Apply for the same entry), giving the state machine a chance to
// thread the index into any downstream sinks that need to record
// it durably alongside the apply's other side-effects.
//
// Motivation: the §9.1 ErrSidecarBehindRaftLog guard compares the
// encryption sidecar's recorded raft_applied_index against the
// engine's AppliedIndex on startup. For that comparison to be
// useful, the sidecar must record an index inside the SAME
// crash-durable fsync that mutates the keys[] map — which means
// the encryption applier needs to know the entry index it is
// applying. The StateMachine.Apply(data) signature does not carry
// it, so this opt-in interface is the seam that delivers it
// without forcing every existing implementation to change.
//
// Implementations MUST treat SetApplyIndex as a strictly local
// hint (not a replicated input). The engine guarantees no
// concurrent Apply / SetApplyIndex calls — Raft apply is serial
// at the engine boundary — so plain field assignment is sufficient
// for the field this hint backs.
type ApplyIndexAware interface {
	SetApplyIndex(idx uint64)
}

// AppliedIndexReader is an OPTIONAL extension that lets the engine
// query the FSM's durable applied-index for the cold-start skip gate.
// See docs/design/2026_06_02_idempotent_snapshot_restore.md §3.
//
// The returned value MUST be the largest Raft entry index whose Apply
// produced a durable mutation on the FSM's primary store (i.e. the
// metaAppliedIndex Pebble meta key, bundled in the same WriteBatch
// as the data mutation). FSMs that cannot self-report return
// (0, false, nil) — the caller treats that as "missing" and falls
// back to the full restore path, preserving the strictly-additive
// invariant.
//
// Returning a non-nil error MUST NOT abort cold start. The
// fsmAlreadyAtIndex caller (restoreSnapshotState) intentionally
// collapses (false, _, err) to "fall back to restore" rather than
// surface the error, because over-restoring on a corrupt meta key is
// strictly safer than skipping incorrectly.
type AppliedIndexReader interface {
	LastAppliedIndex() (uint64, bool, error)
}

// AppliedIndexWriter is an OPTIONAL extension that lets the engine
// pin the FSM's durable applied-index to a known value at snapshot
// persist time. See docs/design/2026_06_02_idempotent_snapshot_restore.md
// §6 "HLC lease entries — checkpoint at snapshot persist".
//
// The engine calls SetDurableAppliedIndex(snap.Metadata.Index)
// before it calls persist.SaveSnap, so that on every successful
// snapshot persist the invariant `LastAppliedIndex >=
// snapshot.Metadata.Index` holds unconditionally — closing the
// HLC-lease-only / encryption-only fallback that would otherwise
// leave LastAppliedIndex stuck at the last data-Apply index.
//
// Implementations MUST persist the value with pebble.Sync (or the
// equivalent strong-durability flag for the backing store)
// regardless of ELASTICKV_FSM_SYNC_MODE. The checkpoint is the only
// durable carrier of metaAppliedIndex at this point — once
// persist.SaveSnap returns, WAL compaction discards every log entry
// at or before snap.Metadata.Index, so there is no source to replay
// the meta key bump from.
type AppliedIndexWriter interface {
	SetDurableAppliedIndex(idx uint64) error
}
