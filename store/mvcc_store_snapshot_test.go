package store

import (
	"bytes"
	"context"
	"encoding/binary"
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
	require.Equal(t, mvccSnapshotLegacyVersion, binary.LittleEndian.Uint32(raw[len(mvccSnapshotMagic):]))

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

func TestMVCCStore_SnapshotRestorePreservesTargetReadiness(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	src := newTestMVCCStore(t)

	stale := TargetStagedReadinessState{
		JobID:                  8,
		RouteStart:             []byte("old"),
		RouteEnd:               []byte("oldz"),
		ExpectedCutoverVersion: 1,
		MigrationJobID:         8,
		MinWriteTSExclusive:    80,
		Armed:                  true,
	}
	require.NoError(t, src.ApplyTargetStagedReadiness(ctx, stale))

	snapState := TargetStagedReadinessState{
		JobID:                  9,
		RouteStart:             []byte("a"),
		RouteEnd:               []byte("z"),
		ExpectedCutoverVersion: 12,
		MigrationJobID:         9,
		MinWriteTSExclusive:    100,
		Armed:                  true,
	}
	require.NoError(t, src.ApplyTargetStagedReadiness(ctx, snapState))

	snap, err := src.Snapshot()
	require.NoError(t, err)
	defer snap.Close()
	raw := snapshotBytes(t, snap)
	require.Equal(t, mvccSnapshotVersion, binary.LittleEndian.Uint32(raw[len(mvccSnapshotMagic):]))

	dst := newTestMVCCStore(t)
	require.NoError(t, dst.ApplyTargetStagedReadiness(ctx, TargetStagedReadinessState{
		JobID:                  77,
		RouteStart:             []byte("dst-only"),
		RouteEnd:               []byte("dst-z"),
		ExpectedCutoverVersion: 2,
		MigrationJobID:         77,
		MinWriteTSExclusive:    700,
		Armed:                  true,
	}))

	require.NoError(t, dst.Restore(bytes.NewReader(raw)))
	states, err := dst.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Equal(t, []TargetStagedReadinessState{stale, snapState}, states)

	states[0].RouteStart[0] = 'x'
	states, err = dst.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("old"), states[0].RouteStart)
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
	promoter, ok := any(st).(MigrationPromoter)
	require.True(t, ok)
	stateReader, ok := any(st).(MigrationPromotionStateReader)
	require.True(t, ok)

	prefix := []byte("stage|")
	stage := func(raw string) []byte {
		return append([]byte("stage|"), []byte(raw)...)
	}
	targetKey := func(staged []byte) ([]byte, bool) {
		return bytes.TrimPrefix(staged, prefix), bytes.HasPrefix(staged, prefix)
	}

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

	require.NoError(t, st.PutAt(ctx, stage("stale"), []byte("old"), 70, 0))
	promoted, err := promoter.PromoteVersions(ctx, PromoteVersionsOptions{
		JobID:       7,
		StartKey:    prefix,
		EndKey:      PrefixScanEnd(prefix),
		MaxVersions: 10,
		TargetKey:   targetKey,
	})
	require.NoError(t, err)
	require.True(t, promoted.Done)
	state, ok, err := stateReader.MigrationPromotionState(ctx, 7)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, state.Done)

	require.NoError(t, st.Restore(bytes.NewReader(raw)))
	floor, err = st.MigrationHLCFloor(ctx, 7)
	require.NoError(t, err)
	require.Zero(t, floor)
	_, ok, err = stateReader.MigrationPromotionState(ctx, 7)
	require.NoError(t, err)
	require.False(t, ok)
	_, err = st.GetAt(ctx, []byte("imported"), 50)
	require.ErrorIs(t, err, ErrKeyNotFound)

	require.NoError(t, st.PutAt(ctx, stage("fresh"), []byte("new"), 80, 0))
	promoted, err = promoter.PromoteVersions(ctx, PromoteVersionsOptions{
		JobID:       7,
		StartKey:    prefix,
		EndKey:      PrefixScanEnd(prefix),
		MaxVersions: 10,
		TargetKey:   targetKey,
	})
	require.NoError(t, err)
	require.True(t, promoted.Done)
	require.Equal(t, uint64(1), promoted.PromotedRows)
	got, err := st.GetAt(ctx, []byte("fresh"), 80)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), got)

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
