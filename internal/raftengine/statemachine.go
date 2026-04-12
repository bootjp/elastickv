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
