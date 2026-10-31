package kv

import (
	"bytes"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var ErrStoreMutationNotRepresentable = errors.New("store mutation cannot be represented as a coordinator element")

// StoreMutationsToElems converts store-layer mutations without dropping an
// apply precondition. TTL-bearing mutations are rejected because Elem has no
// ExpireAt field; callers must use a protocol that carries TTL explicitly.
func StoreMutationsToElems(mutations []*store.KVPairMutation) ([]*Elem[OP], error) {
	elems := make([]*Elem[OP], 0, len(mutations))
	for _, mutation := range mutations {
		elem, err := storeMutationToElem(mutation)
		if err != nil {
			return nil, err
		}
		elems = append(elems, elem)
	}
	return elems, nil
}

func storeMutationToElem(mutation *store.KVPairMutation) (*Elem[OP], error) {
	if mutation == nil || mutation.ExpireAt != 0 {
		return nil, errors.WithStack(ErrStoreMutationNotRepresentable)
	}
	var op OP
	switch mutation.Op {
	case store.OpTypePut:
		op = Put
	case store.OpTypeDelete:
		op = Del
	default:
		return nil, errors.Wrapf(ErrStoreMutationNotRepresentable, "unknown operation %d", mutation.Op)
	}
	return &Elem[OP]{
		Op:            op,
		Key:           bytes.Clone(mutation.Key),
		Value:         bytes.Clone(mutation.Value),
		RequireAbsent: mutation.RequireAbsent,
	}, nil
}
