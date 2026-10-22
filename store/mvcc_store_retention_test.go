package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCStore_ReadTSCompacted(t *testing.T) {
	forEachMVCCStore(t, func(t *testing.T, newStore func(*testing.T) MVCCStore) {
		st := newStore(t)
		ctx := context.Background()

		require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))

		retention, ok := st.(RetentionController)
		require.True(t, ok)
		retention.SetMinRetainedTS(15)

		_, err := st.GetAt(ctx, []byte("k"), 10)
		require.ErrorIs(t, err, ErrReadTSCompacted)
		_, err = st.ScanAt(ctx, nil, nil, 10, 10)
		require.ErrorIs(t, err, ErrReadTSCompacted)
		_, err = st.ReverseScanAt(ctx, nil, nil, 10, 10)
		require.ErrorIs(t, err, ErrReadTSCompacted)

		val, err := st.GetAt(ctx, []byte("k"), 20)
		require.NoError(t, err)
		require.True(t, bytes.Equal([]byte("v20"), val))
	})
}

func TestMVCCStore_SnapshotRestorePreservesMinRetainedTS(t *testing.T) {
	forEachMVCCStore(t, func(t *testing.T, newStore func(*testing.T) MVCCStore) {
		src := newStore(t)
		ctx := context.Background()

		require.NoError(t, src.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
		require.NoError(t, src.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))
		require.NoError(t, src.Compact(ctx, 20))

		snap, err := src.Snapshot()
		require.NoError(t, err)
		defer func() { require.NoError(t, snap.Close()) }()

		var buf bytes.Buffer
		_, err = snap.WriteTo(&buf)
		require.NoError(t, err)

		dst := newStore(t)
		require.NoError(t, dst.Restore(bytes.NewReader(buf.Bytes())))
		retention, ok := dst.(RetentionController)
		require.True(t, ok)
		require.Equal(t, uint64(20), retention.MinRetainedTS())

		_, err = dst.GetAt(ctx, []byte("k"), 10)
		require.ErrorIs(t, err, ErrReadTSCompacted)

		val, err := dst.GetAt(ctx, []byte("k"), 20)
		require.NoError(t, err)
		require.Equal(t, []byte("v20"), val)
	})
}
