package store

import (
	"bytes"
	"context"
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
	defer snap.Close()

	raw := snapshotBytes(t, snap)

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
	defer snap.Close()

	raw := snapshotBytes(t, snap)
	require.NotEmpty(t, raw)
	raw[len(raw)-1] ^= 0xFF

	require.ErrorIs(t, st.Restore(bytes.NewReader(raw)), ErrInvalidChecksum)
}

func TestMVCCStore_RestoreClearsMigrationMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)
	require.NoError(t, st.PutAt(ctx, []byte("base"), []byte("v1"), 10, 0))

	snap, err := st.Snapshot()
	require.NoError(t, err)
	defer snap.Close()
	raw := snapshotBytes(t, snap)

	_, err = st.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("stale"),
		Versions:  []MVCCVersion{{Key: []byte("imported"), CommitTS: 50, Value: []byte("v50")}},
	})
	require.NoError(t, err)
	floor, err := st.MigrationHLCFloor(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, uint64(50), floor)

	require.NoError(t, st.Restore(bytes.NewReader(raw)))
	floor, err = st.MigrationHLCFloor(ctx, 7)
	require.NoError(t, err)
	require.Zero(t, floor)
	_, err = st.GetAt(ctx, []byte("imported"), 50)
	require.ErrorIs(t, err, ErrKeyNotFound)

	res, err := st.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("fresh"),
		Versions:  []MVCCVersion{{Key: []byte("imported"), CommitTS: 60, Value: []byte("v60")}},
	})
	require.NoError(t, err)
	require.False(t, res.Duplicate)
	require.Equal(t, []byte("fresh"), res.AckedCursor)
}

func TestMVCCStore_SnapshotRestorePreservesMigrationMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)
	res, err := st.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("snap-cursor"),
		Versions:  []MVCCVersion{{Key: []byte("imported"), CommitTS: 50, Value: []byte("v50")}},
	})
	require.NoError(t, err)
	require.False(t, res.Duplicate)
	require.Equal(t, uint64(50), res.MaxImportedTS)

	snap, err := st.Snapshot()
	require.NoError(t, err)
	defer snap.Close()
	raw := snapshotBytes(t, snap)

	_, err = st.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  2,
		Cursor:    []byte("post-snapshot-cursor"),
		Versions:  []MVCCVersion{{Key: []byte("post-snapshot"), CommitTS: 60, Value: []byte("v60")}},
	})
	require.NoError(t, err)

	require.NoError(t, st.Restore(bytes.NewReader(raw)))
	floor, err := st.MigrationHLCFloor(ctx, 7)
	require.NoError(t, err)
	require.Equal(t, uint64(50), floor)

	_, err = st.GetAt(ctx, []byte("post-snapshot"), 60)
	require.ErrorIs(t, err, ErrKeyNotFound)

	res, err = st.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("ignored"),
		Versions:  []MVCCVersion{{Key: []byte("duplicate"), CommitTS: 70, Value: []byte("v70")}},
	})
	require.NoError(t, err)
	require.True(t, res.Duplicate)
	require.Equal(t, []byte("snap-cursor"), res.AckedCursor)
	_, err = st.GetAt(ctx, []byte("duplicate"), 70)
	require.ErrorIs(t, err, ErrKeyNotFound)
}

func TestMVCCStore_ApplyMutations_WriteConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v1"), 20, 0))

	err := st.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpTypePut, Key: []byte("k"), Value: []byte("v2")},
	}, nil, 10, 30)
	require.ErrorIs(t, err, ErrWriteConflict)
}

func TestMVCCStore_ApplyMutations_UnknownOp(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newTestMVCCStore(t)

	err := st.ApplyMutations(ctx, []*KVPairMutation{
		{Op: OpType(99), Key: []byte("k"), Value: []byte("v")},
	}, nil, 10, 20)
	require.ErrorIs(t, err, ErrUnknownOp)
}

func snapshotBytes(t *testing.T, snap Snapshot) []byte {
	t.Helper()

	var buf bytes.Buffer
	_, err := snap.WriteTo(&buf)
	require.NoError(t, err)
	return buf.Bytes()
}
