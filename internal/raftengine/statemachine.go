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
