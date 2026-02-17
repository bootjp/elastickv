package store

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCStore_SnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := newTestMVCCStore(t)
	require.NoError(t, src.PutAt(ctx, []byte("k1"), []byte("v1"), 10, 0))
	require.NoError(t, src.PutAt(ctx, []byte("k2"), []byte("v2"), 20, 0))

	snap, err := src.Snapshot()
	require.NoError(t, err)

	raw, err := io.ReadAll(snap)
	require.NoError(t, err)

	// Mutate source after snapshot so restore must reflect snapshot point-in-time.
	require.NoError(t, src.PutAt(ctx, []byte("k1"), []byte("v3"), 30, 0))

	dst := newTestMVCCStore(t)
	require.NoError(t, dst.Restore(bytes.NewReader(raw)))
	require.Equal(t, uint64(20), dst.LastCommitTS())

	v, err := dst.GetAt(ctx, []byte("k1"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), v)

	v, err = dst.GetAt(ctx, []byte("k2"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), v)
}

func TestMVCCStore_RestoreRejectsInvalidChecksum(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v"), 10, 0))

	snap, err := st.Snapshot()
	require.NoError(t, err)

	raw, err := io.ReadAll(snap)
	require.NoError(t, err)
	require.NotEmpty(t, raw)
	raw[len(raw)-1] ^= 0xFF

	require.ErrorIs(t, st.Restore(bytes.NewReader(raw)), ErrInvalidChecksum)
}

func TestMVCCStore_ApplyMutations_WriteConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 20, 0))

	err := st.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k"), Value: []byte("v2")},
	}, 10, 30)
	require.ErrorIs(t, err, ErrWriteConflict)
}

func TestMVCCStore_ApplyMutations_UnknownOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	err := st.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpType(99), Key: []byte("k"), Value: []byte("v")},
	}, 10, 20)
	require.ErrorIs(t, err, ErrUnknownOp)
}
