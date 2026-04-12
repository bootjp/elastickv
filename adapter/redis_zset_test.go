package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestBuildZSetDiffElemsScoreChange(t *testing.T) {
	t.Parallel()
	key := []byte("mykey")
	orig := map[string]float64{"a": 1.0}
	next := map[string]float64{"a": 2.0}
	elems, err := buildZSetDiffElems(key, orig, next)
	require.NoError(t, err)

	ops := elemOps(elems)
	// Old score index deleted, new member+score written, no meta change (count unchanged).
	require.Contains(t, ops, opKey{kv.Del, string(store.ZSetScoreKey(key, 1.0, []byte("a")))})
	require.Contains(t, ops, opKey{kv.Put, string(store.ZSetMemberKey(key, []byte("a")))})
	require.Contains(t, ops, opKey{kv.Put, string(store.ZSetScoreKey(key, 2.0, []byte("a")))})
	// Meta not updated when count stays the same.
	for _, e := range elems {
		require.NotEqual(t, string(store.ZSetMetaKey(key)), string(e.Key),
			"meta should not be written when count is unchanged")
	}
}

func TestBuildZSetDiffElemsRemoveMember(t *testing.T) {
	t.Parallel()
	key := []byte("mykey")
	orig := map[string]float64{"a": 1.0, "b": 2.0}
	next := map[string]float64{"a": 1.0}
	elems, err := buildZSetDiffElems(key, orig, next)
	require.NoError(t, err)

	ops := elemOps(elems)
	require.Contains(t, ops, opKey{kv.Del, string(store.ZSetMemberKey(key, []byte("b")))})
	require.Contains(t, ops, opKey{kv.Del, string(store.ZSetScoreKey(key, 2.0, []byte("b")))})
	// Meta updated with new length.
	require.Contains(t, ops, opKey{kv.Put, string(store.ZSetMetaKey(key))})
}

func TestBuildZSetDiffElemsAddMember(t *testing.T) {
	t.Parallel()
	key := []byte("mykey")
	orig := map[string]float64{"a": 1.0}
	next := map[string]float64{"a": 1.0, "b": 2.0}
	elems, err := buildZSetDiffElems(key, orig, next)
	require.NoError(t, err)

	ops := elemOps(elems)
	require.Contains(t, ops, opKey{kv.Put, string(store.ZSetMemberKey(key, []byte("b")))})
	require.Contains(t, ops, opKey{kv.Put, string(store.ZSetScoreKey(key, 2.0, []byte("b")))})
	require.Contains(t, ops, opKey{kv.Put, string(store.ZSetMetaKey(key))})
}

func TestBuildZSetDiffElemsNoChange(t *testing.T) {
	t.Parallel()
	key := []byte("mykey")
	orig := map[string]float64{"a": 1.0, "b": 2.0}
	next := map[string]float64{"a": 1.0, "b": 2.0}
	elems, err := buildZSetDiffElems(key, orig, next)
	require.NoError(t, err)
	require.Empty(t, elems)
}

func TestBuildZSetDiffElemsDeleteAll(t *testing.T) {
	t.Parallel()
	key := []byte("mykey")
	orig := map[string]float64{"a": 1.0}
	next := map[string]float64{}
	elems, err := buildZSetDiffElems(key, orig, next)
	require.NoError(t, err)

	ops := elemOps(elems)
	require.Contains(t, ops, opKey{kv.Del, string(store.ZSetMemberKey(key, []byte("a")))})
	require.Contains(t, ops, opKey{kv.Del, string(store.ZSetScoreKey(key, 1.0, []byte("a")))})
	require.Contains(t, ops, opKey{kv.Del, string(store.ZSetMetaKey(key))})
}

type opKey struct {
	op  kv.OP
	key string
}

func elemOps(elems []*kv.Elem[kv.OP]) []opKey {
	out := make([]opKey, len(elems))
	for i, e := range elems {
		out[i] = opKey{e.Op, string(e.Key)}
	}
	return out
}
