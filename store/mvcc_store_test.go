package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCStore_PutWithTTL_Expires(t *testing.T) {
	ctx := context.Background()
	ms := newTestMVCCStore(t)

	commitTS := uint64(100)
	expireAt := uint64(180)
	require.NoError(t, ms.PutWithTTLAt(ctx, []byte("k"), []byte("v"), commitTS, expireAt))

	v, err := ms.GetAt(ctx, []byte("k"), 150)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)

	_, err = ms.GetAt(ctx, []byte("k"), 190)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMVCCStore_ExpireExisting(t *testing.T) {
	ctx := context.Background()
	ms := newTestMVCCStore(t)

	require.NoError(t, ms.PutAt(ctx, []byte("k"), []byte("v"), 100, 0))
	require.NoError(t, ms.ExpireAt(ctx, []byte("k"), 220, 150))

	v, err := ms.GetAt(ctx, []byte("k"), 180)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), v)

	_, err = ms.GetAt(ctx, []byte("k"), 230)
	require.ErrorIs(t, err, ErrKeyNotFound)
}
