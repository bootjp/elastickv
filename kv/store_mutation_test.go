package kv

import (
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestStoreMutationsToElemsPreservesRepresentableFields(t *testing.T) {
	t.Parallel()
	key := []byte("key")
	value := []byte("value")
	elems, err := StoreMutationsToElems([]*store.KVPairMutation{
		{Op: store.OpTypePut, Key: key, Value: value, RequireAbsent: true},
		{Op: store.OpTypeDelete, Key: []byte("deleted")},
	})
	require.NoError(t, err)
	require.Equal(t, []*Elem[OP]{
		{Op: Put, Key: []byte("key"), Value: []byte("value"), RequireAbsent: true},
		{Op: Del, Key: []byte("deleted")},
	}, elems)

	key[0], value[0] = 'X', 'X'
	require.Equal(t, []byte("key"), elems[0].Key)
	require.Equal(t, []byte("value"), elems[0].Value)
}

func TestStoreMutationsToElemsRejectsLossyShapes(t *testing.T) {
	t.Parallel()
	for _, mutations := range [][]*store.KVPairMutation{
		{nil},
		{{Op: store.OpType(99), Key: []byte("unknown")}},
		{{Op: store.OpTypePut, Key: []byte("ttl"), ExpireAt: 1}},
	} {
		_, err := StoreMutationsToElems(mutations)
		require.ErrorIs(t, err, ErrStoreMutationNotRepresentable)
	}
}
