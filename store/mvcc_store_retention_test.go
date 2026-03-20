package store

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMVCCStore_ReadTSCompacted(t *testing.T) {
	st := NewMVCCStore()
	ctx := context.Background()

	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))

	retention, ok := st.(RetentionController)
	require.True(t, ok)
	retention.SetMinRetainedTS(15)

	_, err := st.GetAt(ctx, []byte("k"), 10)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	val, err := st.GetAt(ctx, []byte("k"), 20)
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte("v20"), val))
}

func TestMVCCStore_SnapshotRestorePreservesMinRetainedTS(t *testing.T) {
	src := NewMVCCStore()
	ctx := context.Background()

	require.NoError(t, src.PutAt(ctx, []byte("k"), []byte("v10"), 10, 0))
	require.NoError(t, src.PutAt(ctx, []byte("k"), []byte("v20"), 20, 0))
	require.NoError(t, src.Compact(ctx, 20))

	snap, err := src.Snapshot()
	require.NoError(t, err)
	defer snap.Close()

	var buf bytes.Buffer
	_, err = snap.WriteTo(&buf)
	require.NoError(t, err)

	dst := NewMVCCStore()
	require.NoError(t, dst.Restore(bytes.NewReader(buf.Bytes())))

	_, err = dst.GetAt(ctx, []byte("k"), 10)
	require.ErrorIs(t, err, ErrReadTSCompacted)

	val, err := dst.GetAt(ctx, []byte("k"), 20)
	require.NoError(t, err)
	require.Equal(t, []byte("v20"), val)
}
