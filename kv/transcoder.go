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

// ObservedRouteVersionZero encodes a transaction pinned to catalog version 0.
// The protobuf field's literal zero remains the legacy/unpinned sentinel, so a
// real version-0 observation needs a distinct value. MaxUint64 is reserved for
// this internal encoding; normal catalog versions are far below this boundary.
const ObservedRouteVersionZero = ^uint64(0)

// observedRouteVersionZeroWireEncodingEnabled stays disabled until every Raft
// member advertises support for ObservedRouteVersionZero. Mixed-version groups
// must keep literal zero on the wire so old followers do not treat the sentinel
// as an impossibly new catalog version and diverge from a new leader.
var observedRouteVersionZeroWireEncodingEnabled = false

// EncodeObservedRouteVersion converts a tracked catalog version into the wire
// value carried by OperationGroup. Version zero is left as the legacy unpinned
// zero until the version-zero sentinel is capability-gated across Raft members.
func EncodeObservedRouteVersion(version uint64) uint64 {
	if version == 0 && observedRouteVersionZeroWireEncodingEnabled {
		return ObservedRouteVersionZero
	}
	return version
}

// DecodeObservedRouteVersion converts OperationGroup.ObservedRouteVersion back
// to a catalog version and reports whether it was explicitly pinned.
func DecodeObservedRouteVersion(observed uint64) (uint64, bool) {
	switch observed {
	case 0:
		return 0, false
	case ObservedRouteVersionZero:
		return 0, true
	default:
		return observed, true
	}
}

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
	// ObservedRouteVersion is the encoded durable catalog version this
	// transaction's read set was captured at (typically set on BeginTxn
	// from distribution.Engine.Version()). Zero means "unpinned"; the
	// version-0 sentinel is decoded for compatibility but is not emitted
	// until every Raft member advertises support. M3 of the Composed-1 design
	// (docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md)
	// will gate the FSM apply path on this version so a route shift
	// between BeginTxn and Commit is caught before it can produce a
	// G1c anomaly across a cross-group MoveRange / SplitRange.
	ObservedRouteVersion uint64
}
