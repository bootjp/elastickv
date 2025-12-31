package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestMVCCStore(t *testing.T) *mvccStore {
	t.Helper()
	st := NewMVCCStore()
	ms, ok := st.(*mvccStore)
	require.True(t, ok)
	return ms
}

func TestMVCCStore_GetAtSnapshots(t *testing.T) {
	ctx := context.Background()
	st := newTestMVCCStore(t)

	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 100, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v2"), 200, 0))

	_, err := st.GetAt(ctx, []byte("k"), 50)
	require.ErrorIs(t, err, ErrKeyNotFound)

	v, err := st.GetAt(ctx, []byte("k"), 150)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)

	v, err = st.GetAt(ctx, []byte("k"), 250)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), v)
}

func TestMVCCStore_DeleteAtTombstone(t *testing.T) {
	ctx := context.Background()
	st := newTestMVCCStore(t)

	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 100, 0))
	require.NoError(t, st.DeleteAt(ctx, []byte("k"), 180))

	v, err := st.GetAt(ctx, []byte("k"), 170)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)

	_, err = st.GetAt(ctx, []byte("k"), 190)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMVCCStore_ScanAtSortedAndSnapshot(t *testing.T) {
	ctx := context.Background()
	st := newTestMVCCStore(t)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va1"), 100, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb1"), 90, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc1"), 110, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb2"), 200, 0))

	kvs, err := st.ScanAt(ctx, nil, nil, 10, 150)
	require.NoError(t, err)
	require.Equal(t, 3, len(kvs))
	require.Equal(t, []byte("a"), kvs[0].Key)
	require.Equal(t, []byte("va1"), kvs[0].Value)
	require.Equal(t, []byte("b"), kvs[1].Key)
	require.Equal(t, []byte("vb1"), kvs[1].Value)
	require.Equal(t, []byte("c"), kvs[2].Key)
	require.Equal(t, []byte("vc1"), kvs[2].Value)

	kvs, err = st.ScanAt(ctx, nil, nil, 10, 250)
	require.NoError(t, err)
	require.Equal(t, 3, len(kvs))
	require.Equal(t, []byte("b"), kvs[1].Key)
	require.Equal(t, []byte("vb2"), kvs[1].Value)
}

func TestMVCCStore_TTLAt(t *testing.T) {
	ctx := context.Background()
	st := newTestMVCCStore(t)

	// expireAt of 180 should be visible before, invisible after.
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 100, 180))

	v, err := st.GetAt(ctx, []byte("x"), 150)
	require.NoError(t, err)
	require.Equal(t, []byte("vx"), v)

	_, err = st.GetAt(ctx, []byte("x"), 190)
	require.ErrorIs(t, err, ErrKeyNotFound)
}
