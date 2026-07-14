package store

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func runMigrationStoreSuite(t *testing.T, test func(t *testing.T, st MVCCStore)) {
	t.Helper()
	t.Run("memory", func(t *testing.T) {
		test(t, NewMVCCStore())
	})
	t.Run("pebble", func(t *testing.T) {
		dir, err := os.MkdirTemp("", "migration-versions-*")
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
		st, err := NewPebbleStore(dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		test(t, st)
	})
}

func TestExportVersionsPreservesRawVersionMetadata(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, []byte("k1"), []byte("v10"), 10, 0))
		require.NoError(t, st.PutWithTTLAt(ctx, []byte("k1"), []byte("v20"), 20, 55))
		require.NoError(t, st.DeleteAt(ctx, []byte("k1"), 30))
		require.NoError(t, st.PutAt(ctx, []byte("k2"), []byte("v15"), 15, 0))

		result, err := st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:             []byte("k1"),
			EndKey:               []byte("k3"),
			MinCommitTSExclusive: 9,
			MaxCommitTSInclusive: 30,
			MaxVersions:          10,
			KeyFamily:            7,
		})
		require.NoError(t, err)
		require.True(t, result.Done)
		require.Empty(t, result.NextCursor)
		require.Equal(t, []MVCCVersion{
			{Key: []byte("k1"), CommitTS: 30, Tombstone: true, KeyFamily: 7},
			{Key: []byte("k1"), CommitTS: 20, Value: []byte("v20"), KeyFamily: 7, ExpireAt: 55},
			{Key: []byte("k1"), CommitTS: 10, Value: []byte("v10"), KeyFamily: 7},
			{Key: []byte("k2"), CommitTS: 15, Value: []byte("v15"), KeyFamily: 7},
		}, result.Versions)
	})
}

func TestExportVersionsCursorResumesWithinHotKey(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, []byte("hot"), []byte("v10"), 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("hot"), []byte("v20"), 20, 0))
		require.NoError(t, st.PutAt(ctx, []byte("hot"), []byte("v30"), 30, 0))
		require.NoError(t, st.PutAt(ctx, []byte("tail"), []byte("v15"), 15, 0))

		first, err := st.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 2})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Len(t, first.Versions, 2)
		require.Equal(t, uint64(30), first.Versions[0].CommitTS)
		require.Equal(t, uint64(20), first.Versions[1].CommitTS)
		require.NotEmpty(t, first.NextCursor)

		second, err := st.ExportVersions(ctx, ExportVersionsOptions{
			Cursor:      first.NextCursor,
			MaxVersions: 10,
		})
		require.NoError(t, err)
		require.True(t, second.Done)
		require.Equal(t, []MVCCVersion{
			{Key: []byte("hot"), CommitTS: 10, Value: []byte("v10")},
			{Key: []byte("tail"), CommitTS: 15, Value: []byte("v15")},
		}, second.Versions)
	})
}

func TestExportVersionsSparseScanBudgetAdvancesRejectedRows(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, []byte("drop-a"), []byte("a"), 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("keep"), []byte("b"), 20, 0))

		first, err := st.ExportVersions(ctx, ExportVersionsOptions{
			MaxVersions:     10,
			MaxScannedBytes: 1,
			AcceptKey: func(key []byte) bool {
				return string(key) == "keep"
			},
		})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Empty(t, first.Versions)
		require.NotEmpty(t, first.NextCursor)

		second, err := st.ExportVersions(ctx, ExportVersionsOptions{
			Cursor:      first.NextCursor,
			MaxVersions: 10,
			AcceptKey: func(key []byte) bool {
				return string(key) == "keep"
			},
		})
		require.NoError(t, err)
		require.True(t, second.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("keep"), CommitTS: 20, Value: []byte("b")}}, second.Versions)
	})
}

func TestImportVersionsIdempotencyAndMetadata(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		first := ImportVersionsOptions{
			JobID:     1,
			BracketID: 2,
			BatchSeq:  1,
			Cursor:    []byte("c1"),
			Versions: []MVCCVersion{
				{Key: []byte("ttl"), CommitTS: 20, Value: []byte("v20"), ExpireAt: 50},
				{Key: []byte("gone"), CommitTS: 30, Tombstone: true},
			},
		}
		res, err := st.ImportVersions(ctx, first)
		require.NoError(t, err)
		require.Equal(t, []byte("c1"), res.AckedCursor)
		require.Equal(t, uint64(30), res.MaxImportedTS)
		require.False(t, res.Duplicate)
		require.Equal(t, uint64(30), st.LastCommitTS())
		floor, err := st.MigrationHLCFloor(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, uint64(30), floor)

		val, err := st.GetAt(ctx, []byte("ttl"), 25)
		require.NoError(t, err)
		require.Equal(t, []byte("v20"), val)
		_, err = st.GetAt(ctx, []byte("ttl"), 55)
		require.ErrorIs(t, err, ErrKeyNotFound)
		_, err = st.GetAt(ctx, []byte("gone"), 35)
		require.ErrorIs(t, err, ErrKeyNotFound)

		dup := first
		dup.Cursor = []byte("changed")
		dup.Versions = []MVCCVersion{{Key: []byte("ttl"), CommitTS: 40, Value: []byte("bad")}}
		res, err = st.ImportVersions(ctx, dup)
		require.NoError(t, err)
		require.True(t, res.Duplicate)
		require.Equal(t, []byte("c1"), res.AckedCursor)
		val, err = st.GetAt(ctx, []byte("ttl"), 45)
		require.NoError(t, err)
		require.Equal(t, []byte("v20"), val)

		_, err = st.ImportVersions(ctx, ImportVersionsOptions{
			JobID:     1,
			BracketID: 2,
			BatchSeq:  3,
			Cursor:    []byte("gap"),
			Versions:  []MVCCVersion{{Key: []byte("gap"), CommitTS: 60, Value: []byte("bad")}},
		})
		require.ErrorIs(t, err, ErrImportBatchGap)
		_, err = st.GetAt(ctx, []byte("gap"), 60)
		require.ErrorIs(t, err, ErrKeyNotFound)

		res, err = st.ImportVersions(ctx, ImportVersionsOptions{
			JobID:     1,
			BracketID: 2,
			BatchSeq:  2,
			Cursor:    []byte("c2"),
		})
		require.NoError(t, err)
		require.Equal(t, []byte("c2"), res.AckedCursor)
		require.Zero(t, res.MaxImportedTS)
		require.Equal(t, uint64(30), st.LastCommitTS())
		floor, err = st.MigrationHLCFloor(ctx, 1)
		require.NoError(t, err)
		require.Equal(t, uint64(30), floor)
	})
}

func TestPromoteVersionsMovesStagedVersionsAndDeletesStagedRows(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		promoter, ok := st.(MigrationPromoter)
		require.True(t, ok)
		stateReader, ok := st.(MigrationPromotionStateReader)
		require.True(t, ok)

		stage := func(raw string) []byte {
			return append([]byte("stage|"), []byte(raw)...)
		}
		targetKey := func(staged []byte) ([]byte, bool) {
			return bytes.TrimPrefix(staged, []byte("stage|")), bytes.HasPrefix(staged, []byte("stage|"))
		}
		prefix := []byte("stage|")

		require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("old"), 5, 0))
		require.NoError(t, st.PutAt(ctx, stage("k"), []byte("v10"), 10, 0))
		require.NoError(t, st.PutWithTTLAt(ctx, stage("k"), []byte("v20"), 20, 55))
		require.NoError(t, st.DeleteAt(ctx, stage("k"), 30))
		require.NoError(t, st.PutAt(ctx, stage("z"), []byte("z15"), 15, 0))

		first, err := promoter.PromoteVersions(ctx, PromoteVersionsOptions{
			JobID:       99,
			StartKey:    prefix,
			EndKey:      PrefixScanEnd(prefix),
			MaxVersions: 2,
			TargetKey:   targetKey,
		})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Equal(t, uint64(2), first.PromotedRows)
		require.Equal(t, uint64(2), first.TotalPromotedRows)
		require.Equal(t, uint64(30), first.MaxPromotedTS)
		require.NotEmpty(t, first.NextCursor)
		state, ok, err := stateReader.MigrationPromotionState(ctx, 99)
		require.NoError(t, err)
		require.True(t, ok)
		require.False(t, state.Done)
		require.Equal(t, first.NextCursor, state.Cursor)
		require.Equal(t, uint64(2), state.PromotedRows)

		got, err := st.GetAt(ctx, []byte("k"), 25)
		require.NoError(t, err)
		require.Equal(t, []byte("v20"), got)
		_, err = st.GetAt(ctx, []byte("k"), 35)
		require.ErrorIs(t, err, ErrKeyNotFound)

		stagedLeft, err := st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:    prefix,
			EndKey:      PrefixScanEnd(prefix),
			MaxVersions: 10,
		})
		require.NoError(t, err)
		require.Equal(t, []MVCCVersion{
			{Key: stage("k"), CommitTS: 10, Value: []byte("v10")},
			{Key: stage("z"), CommitTS: 15, Value: []byte("z15")},
		}, stagedLeft.Versions)

		second, err := promoter.PromoteVersions(ctx, PromoteVersionsOptions{
			JobID:       99,
			StartKey:    prefix,
			EndKey:      PrefixScanEnd(prefix),
			MaxVersions: 10,
			TargetKey:   targetKey,
		})
		require.NoError(t, err)
		require.True(t, second.Done)
		require.Empty(t, second.NextCursor)
		require.Equal(t, uint64(2), second.PromotedRows)
		require.Equal(t, uint64(4), second.TotalPromotedRows)
		require.Equal(t, uint64(15), second.MaxPromotedTS)
		state, ok, err = stateReader.MigrationPromotionState(ctx, 99)
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, state.Done)
		require.Empty(t, state.Cursor)
		require.Equal(t, uint64(4), state.PromotedRows)

		got, err = st.GetAt(ctx, []byte("k"), 10)
		require.NoError(t, err)
		require.Equal(t, []byte("v10"), got)
		got, err = st.GetAt(ctx, []byte("z"), 15)
		require.NoError(t, err)
		require.Equal(t, []byte("z15"), got)

		stagedLeft, err = st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:    prefix,
			EndKey:      PrefixScanEnd(prefix),
			MaxVersions: 10,
		})
		require.NoError(t, err)
		require.True(t, stagedLeft.Done)
		require.Empty(t, stagedLeft.Versions)
	})
}

func TestPebblePromoteVersionsAdvancesLastCommitTS(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, st.Close())
		}
	})
	ps, ok := st.(*pebbleStore)
	require.True(t, ok)

	stage := func(raw string) []byte {
		return append([]byte("stage|"), []byte(raw)...)
	}
	targetKey := func(staged []byte) ([]byte, bool) {
		return bytes.TrimPrefix(staged, []byte("stage|")), bytes.HasPrefix(staged, []byte("stage|"))
	}
	prefix := []byte("stage|")

	const promotedTS uint64 = 100
	require.NoError(t, ps.db.Set(encodeKey(stage("k"), promotedTS), encodeValue([]byte("v100"), false, 0, encStateCleartext), pebble.NoSync))
	require.Zero(t, ps.LastCommitTS())

	result, err := ps.PromoteVersions(ctx, PromoteVersionsOptions{
		JobID:       101,
		StartKey:    prefix,
		EndKey:      PrefixScanEnd(prefix),
		MaxVersions: 10,
		TargetKey:   targetKey,
	})
	require.NoError(t, err)
	require.True(t, result.Done)
	require.Equal(t, uint64(1), result.PromotedRows)
	require.Equal(t, promotedTS, result.MaxPromotedTS)
	require.Equal(t, promotedTS, ps.LastCommitTS())

	metaTS, err := readPebbleUint64(ps.db, metaLastCommitTSBytes)
	require.NoError(t, err)
	require.Equal(t, promotedTS, metaTS)
	val, err := ps.GetAt(ctx, []byte("k"), ps.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("v100"), val)

	require.NoError(t, st.Close())
	closed = true
	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	require.Equal(t, promotedTS, reopened.LastCommitTS())
	val, err = reopened.GetAt(ctx, []byte("k"), reopened.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, []byte("v100"), val)
}

func TestPebbleImportMetadataPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-import-persist-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })

	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	_, err = st.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     9,
		BracketID: 4,
		BatchSeq:  1,
		Cursor:    []byte("persisted"),
		Versions:  []MVCCVersion{{Key: []byte("k"), CommitTS: 99, Value: []byte("v")}},
	})
	require.NoError(t, err)
	require.NoError(t, st.Close())

	reopened, err := NewPebbleStore(dir)
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()
	floor, err := reopened.MigrationHLCFloor(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, uint64(99), floor)
	res, err := reopened.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     9,
		BracketID: 4,
		BatchSeq:  1,
		Cursor:    []byte("different"),
	})
	require.NoError(t, err)
	require.True(t, res.Duplicate)
	require.Equal(t, []byte("persisted"), res.AckedCursor)
}
