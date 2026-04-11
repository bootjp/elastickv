package kv

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
}

// OperationGroup is a group of operations that should be executed atomically.
type OperationGroup[T OP] struct {
	Elems []*Elem[T]
	IsTxn bool
	// StartTS is a logical timestamp captured at transaction begin.
	// It is ignored for non-transactional groups.
	StartTS uint64
	// CommitTS optionally pins the transaction commit timestamp.
	// Coordinators choose one automatically when this is zero.
	CommitTS uint64
}
