package kv

type OP int

const (
	Put OP = iota
	Del
)

type Elem[T OP] struct {
	Op    T
	Key   []byte
	Value []byte
}

type OperationGroup[T OP] struct {
	Elems []*Elem[T]
	IsTxn bool
}
