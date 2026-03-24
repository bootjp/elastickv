package store

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCStore_ReverseScanAt(t *testing.T) {
	t.Parallel()
	testReverseScanAt(t, func(t *testing.T) MVCCStore {
		t.Helper()
		return NewMVCCStore()
	})
}

func TestPebbleStore_ReverseScanAt(t *testing.T) {
	t.Parallel()
	testReverseScanAt(t, func(t *testing.T) MVCCStore {
		t.Helper()
		dir, err := os.MkdirTemp("", "pebble-reverse-scan-test")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(dir) })

		st, err := NewPebbleStore(dir)
		require.NoError(t, err)
		t.Cleanup(func() { _ = st.Close() })
		return st
	})
}

func TestPebbleStore_ReverseScanAt_IgnoresInternalMetaKeys(t *testing.T) {
	t.Parallel()

	dir, err := os.MkdirTemp("", "pebble-reverse-scan-meta-test")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 20, 0))
	requirePebbleRetentionController(t, st).SetMinRetainedTS(5)

	kvs, err := st.ReverseScanAt(ctx, []byte(""), nil, 10, 20)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("b"), kvs[0].Key)
	require.Equal(t, []byte("a"), kvs[1].Key)
}

func testReverseScanAt(t *testing.T, newStore func(*testing.T) MVCCStore) {
	t.Helper()

	ctx := context.Background()
	st := newStore(t)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va1"), 100, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb1"), 90, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc1"), 110, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb2"), 200, 0))
	require.NoError(t, st.PutAt(ctx, []byte("d"), []byte("vd1"), 120, 0))
	require.NoError(t, st.DeleteAt(ctx, []byte("d"), 150))

	kvs, err := st.ReverseScanAt(ctx, []byte("a"), []byte("d"), 2, 150)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("c"), kvs[0].Key)
	require.Equal(t, []byte("vc1"), kvs[0].Value)
	require.Equal(t, []byte("b"), kvs[1].Key)
	require.Equal(t, []byte("vb1"), kvs[1].Value)

	kvs, err = st.ReverseScanAt(ctx, []byte("a"), []byte("d"), 10, 250)
	require.NoError(t, err)
	require.Len(t, kvs, 3)
	require.Equal(t, []byte("c"), kvs[0].Key)
	require.Equal(t, []byte("b"), kvs[1].Key)
	require.Equal(t, []byte("vb2"), kvs[1].Value)
	require.Equal(t, []byte("a"), kvs[2].Key)

	kvs, err = st.ReverseScanAt(ctx, []byte("b"), []byte("c"), 10, 250)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("b"), kvs[0].Key)
	require.Equal(t, []byte("vb2"), kvs[0].Value)
}
