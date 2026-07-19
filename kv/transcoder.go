package kv

import "github.com/bootjp/elastickv/keyviz"

// OP is an operation type.
type OP int

// Operation types.
const (
	Put OP = iota
	Del
	// DelPrefix deletes all visible keys matching the prefix stored in Key.
	// An empty Key means "all keys". Transaction-internal keys are excluded.
	DelPrefix
)

// Elem is an element of a transaction.
type Elem[T OP] struct {
	Op    T
	Key   []byte
	Value []byte
	// CommitTSValueOffset, when non-zero, asks the coordinator or forwarded
	// leader to stamp the resolved transaction commit timestamp into Value at
	// this byte offset before committing the mutation.
	CommitTSValueOffset uint64
}

// OperationGroup is a group of operations that should be executed atomically.
type OperationGroup[T OP] struct {
	Elems []*Elem[T]
	IsTxn bool
	// KeyVizLabel tags this operation group for KeyViz attribution.
	// The zero value is the legacy unlabeled route-only view.
	KeyVizLabel keyviz.Label
	// StartTS is a logical timestamp captured at transaction begin.
	// It is ignored for non-transactional groups.
	StartTS uint64
	// CommitTS optionally pins the transaction commit timestamp.
	// Coordinators choose one automatically when this is zero.
	CommitTS uint64
	// PrevCommitTS carries the commit timestamp of a failed previous attempt
	// of the same single-shard transaction (option-2 one-phase idempotency
	// dedup). It is set only on a retry that reuses the prior attempt's write
	// set, and only flows to the one-phase apply path, where the FSM probes
	// whether that attempt already landed and no-ops the apply if so. Zero on
	// first attempts and on every non-retry caller. See
	// docs/design/2026_05_21_proposed_txn_secondary_idempotency.md.
	PrevCommitTS uint64
	// ReadKeys carries the transaction's read set so the FSM can validate
	// read-write conflicts atomically with the commit.
	ReadKeys [][]byte
	// ObservedRouteVersion is the durable catalog version this
	// transaction's read set was captured at (typically set on
	// BeginTxn from distribution.Engine.Version()).  Zero means
	// "unpinned" — every existing caller leaves it at zero so this
	// is behaviour-neutral on the M1 plumbing PR.  M3 of the
	// Composed-1 design
	// (docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md)
	// will gate the FSM apply path on this version so a route shift
	// between BeginTxn and Commit is caught before it can produce a
	// G1c anomaly across a cross-group MoveRange / SplitRange.
	ObservedRouteVersion uint64
}
