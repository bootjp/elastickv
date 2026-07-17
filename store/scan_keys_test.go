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

func TestPebbleStoreScanAtCursorAfterKeyDoesNotRepeat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 20, 0))

	kvs, err := st.ScanAt(ctx, cursorAfterTestKey([]byte("a")), nil, 1, 20)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("b"), kvs[0].Key)
}

func TestPebbleStoreScanKeysAtCursorAfterKeyDoesNotRepeat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	require.NoError(t, st.PutAt(ctx, []byte(""), []byte("empty"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 20, 0))

	keys, err := st.ScanKeysAt(ctx, cursorAfterTestKey([]byte("")), nil, 1, 20)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)
}

func TestPebbleStoreScanAtPreservesPrefixExtensionKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	highTS := ^uint64(0) - 1
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), highTS, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a\x00"), []byte("vax"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 20, 0))

	kvs, err := st.ScanAt(ctx, []byte("a"), []byte("c"), 3, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00"), []byte("b")}, keysFromStoreKVs(kvs))
}

func TestPebbleStoreScanKeysAtPreservesPrefixExtensionKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	highTS := ^uint64(0) - 1
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), highTS, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a\x00"), []byte("vax"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 20, 0))

	keys, err := st.ScanKeysAt(ctx, []byte("a"), []byte("c"), 3, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("a\x00"), []byte("b")}, keys)
}

func TestPebbleStoreScanKeysAtSortsPrefixBeforeLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	require.NoError(t, st.PutAt(ctx, []byte(""), []byte("empty"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 20, 0))
	require.NoError(t, st.PutAt(ctx, []byte("\x00"), []byte("prefix"), 30, 0))
	require.NoError(t, st.PutAt(ctx, []byte("\x00a"), []byte("extension"), 40, 0))

	keys, err := st.ScanKeysAt(ctx, nil, nil, 1, 40)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("")}, keys)

	keys, err = st.ScanKeysAt(ctx, cursorAfterTestKey([]byte("")), nil, 1, 40)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("\x00")}, keys)
}

func TestPebbleStoreScanKeysAtProbesExactPrefixPastBinaryExtension(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a\x80"), []byte("vax"), 20, 0))

	keys, err := st.ScanKeysAt(ctx, nil, nil, 1, 20)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)

	keys, err = st.ScanKeysAt(ctx, cursorAfterTestKey([]byte("a")), nil, 1, 20)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a\x80")}, keys)
}

func TestPebbleStoreScanKeysAtProbesExactPrefixBeforeBoundedEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a\x80"), []byte("vax"), 20, 0))

	keys, err := st.ScanKeysAt(ctx, []byte("a"), []byte("a\x80"), 1, 20)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)
}

func TestPebbleStoreScanKeysAtPreservesExtensionAfterInvisiblePrefix(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	highTS := ^uint64(0) - 1
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("future"), highTS, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a\x80"), []byte("visible-extension"), 10, 0))

	keys, err := st.ScanKeysAt(ctx, []byte("a"), []byte("b"), 2, 10)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a\x80")}, keys)
}

func TestPebbleStoreScanKeysAtKeepsAllFFKeyWithoutFiniteUpperBound(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st, err := NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()

	key := []byte{0xff, 0xff}
	require.NoError(t, st.PutAt(ctx, key, []byte("visible"), 18, 0))
	require.NoError(t, st.PutAt(ctx, []byte(""), []byte("future-empty"), 45, 0))
	require.NoError(t, st.DeleteAt(ctx, key, 47))

	val, err := st.GetAt(ctx, key, 30)
	require.NoError(t, err)
	require.Equal(t, []byte("visible"), val)

	keys, err := st.ScanKeysAt(ctx, nil, nil, 2, 30)
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, keys)
}

func cursorAfterTestKey(key []byte) []byte {
	cursor := make([]byte, len(key)+1)
	copy(cursor, key)
	return cursor
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
