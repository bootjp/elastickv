package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCStoreScanKeysAtMatchesScanAtVisibility(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := newTestMVCCStore(t)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 20, 0))
	require.NoError(t, st.DeleteAt(ctx, []byte("b"), 30))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 15, 40))
	require.NoError(t, st.PutAt(ctx, []byte("d"), []byte("vd"), 50, 0))

	keys, err := st.ScanKeysAt(ctx, []byte("a"), []byte("z"), 2, 35)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("c")}, keys)

	kvs, err := st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 35)
	require.NoError(t, err)
	require.Equal(t, keysFromStoreKVs(kvs), keys)

	keys, err = st.ScanKeysAt(ctx, []byte("a"), []byte("z"), 10, 45)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)
}

func TestMVCCStoreScanKeysAtReturnsCompactionError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := newTestMVCCStore(t)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	st.SetMinRetainedTS(20)

	_, err := st.ScanKeysAt(ctx, []byte("a"), nil, 10, 19)
	require.ErrorIs(t, err, ErrReadTSCompacted)
}

func TestPebbleStoreScanKeysAtMatchesScanAtVisibility(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 20, 0))
	require.NoError(t, st.DeleteAt(ctx, []byte("b"), 30))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 15, 40))
	require.NoError(t, st.PutAt(ctx, []byte("d"), []byte("vd"), 50, 0))

	keys, err := st.ScanKeysAt(ctx, []byte("a"), []byte("z"), 2, 35)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("c")}, keys)

	kvs, err := st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 35)
	require.NoError(t, err)
	require.Equal(t, keysFromStoreKVs(kvs), keys)

	keys, err = st.ScanKeysAt(ctx, []byte("a"), []byte("z"), 10, 45)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)
}

func keysFromStoreKVs(kvs []*KVPair) [][]byte {
	keys := make([][]byte, 0, len(kvs))
	for _, kvp := range kvs {
		if kvp == nil {
			continue
		}
		keys = append(keys, kvp.Key)
	}
	return keys
}
