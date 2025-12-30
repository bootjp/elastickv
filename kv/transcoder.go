package kv

// OP is an operation type.
type OP int

// Operation types.
const (
	Put OP = iota
	Del
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
}
