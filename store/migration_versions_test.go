package store

import (
	"bytes"
	"context"
	"math"
	"os"
	"testing"

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

func TestExportVersionsPreservesEmptyKeyCursor(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, nil, []byte("v10"), 10, 0))
		require.NoError(t, st.PutAt(ctx, nil, []byte("v20"), 20, 0))

		first, err := st.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 1})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Len(t, first.Versions, 1)
		require.Empty(t, first.Versions[0].Key)
		require.Equal(t, uint64(20), first.Versions[0].CommitTS)
		require.Equal(t, []byte("v20"), first.Versions[0].Value)
		require.NotEmpty(t, first.NextCursor)

		second, err := st.ExportVersions(ctx, ExportVersionsOptions{
			Cursor:      first.NextCursor,
			MaxVersions: 10,
		})
		require.NoError(t, err)
		require.True(t, second.Done)
		require.Len(t, second.Versions, 1)
		require.Empty(t, second.Versions[0].Key)
		require.Equal(t, uint64(10), second.Versions[0].CommitTS)
		require.Equal(t, []byte("v10"), second.Versions[0].Value)
	})
}

func TestExportVersionsAppliesDefaultSparseScanBudget(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		value := bytes.Repeat([]byte("x"), defaultSparseExportMaxScannedBytes)
		require.NoError(t, st.PutAt(ctx, []byte("drop"), value, 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("keep"), []byte("v"), 20, 0))

		first, err := st.ExportVersions(ctx, ExportVersionsOptions{
			MaxVersions: 10,
			AcceptKey: func(key []byte) bool {
				return bytes.Equal(key, []byte("keep"))
			},
		})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Empty(t, first.Versions)
		require.GreaterOrEqual(t, first.ScannedBytes, uint64(defaultSparseExportMaxScannedBytes))
		require.NotEmpty(t, first.NextCursor)

		second, err := st.ExportVersions(ctx, ExportVersionsOptions{
			Cursor:      first.NextCursor,
			MaxVersions: 10,
			AcceptKey: func(key []byte) bool {
				return bytes.Equal(key, []byte("keep"))
			},
		})
		require.NoError(t, err)
		require.True(t, second.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("keep"), CommitTS: 20, Value: []byte("v")}}, second.Versions)
	})
}

func TestExportVersionsAppliesDefaultScanBudgetForTimestampFilter(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		value := bytes.Repeat([]byte("x"), defaultSparseExportMaxScannedBytes)
		require.NoError(t, st.PutAt(ctx, []byte("drop-new"), value, 30, 0))
		require.NoError(t, st.PutAt(ctx, []byte("keep-old"), []byte("v"), 10, 0))

		first, err := st.ExportVersions(ctx, ExportVersionsOptions{
			MaxCommitTSInclusive: 20,
			MaxVersions:          10,
		})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Empty(t, first.Versions)
		require.GreaterOrEqual(t, first.ScannedBytes, uint64(defaultSparseExportMaxScannedBytes))
		require.NotEmpty(t, first.NextCursor)

		second, err := st.ExportVersions(ctx, ExportVersionsOptions{
			Cursor:               first.NextCursor,
			MaxCommitTSInclusive: 20,
			MaxVersions:          10,
		})
		require.NoError(t, err)
		require.True(t, second.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("keep-old"), CommitTS: 10, Value: []byte("v")}}, second.Versions)
	})
}

func TestExportVersionsMinTSPruneDoesNotSkipPrefixedKeys(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		prefixed := []byte{'a', 0xff, 0x00}
		require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("old"), 5, 0))
		require.NoError(t, st.PutAt(ctx, prefixed, []byte("new"), 20, 0))

		res, err := st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:             []byte("a"),
			EndKey:               []byte("b"),
			MinCommitTSExclusive: 10,
			MaxVersions:          10,
		})
		require.NoError(t, err)
		require.True(t, res.Done)
		require.Equal(t, []MVCCVersion{{Key: prefixed, CommitTS: 20, Value: []byte("new")}}, res.Versions)
	})
}

func TestExportVersionsMinTSSkipHonorsScanBudget(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		value := bytes.Repeat([]byte("x"), defaultSparseExportMaxScannedBytes)
		require.NoError(t, st.PutAt(ctx, []byte("old"), value, 5, 0))
		require.NoError(t, st.PutAt(ctx, []byte("tail"), []byte("v"), 20, 0))

		first, err := st.ExportVersions(ctx, ExportVersionsOptions{
			MinCommitTSExclusive: 10,
			MaxVersions:          10,
		})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Empty(t, first.Versions)
		require.GreaterOrEqual(t, first.ScannedBytes, uint64(defaultSparseExportMaxScannedBytes))
		require.NotEmpty(t, first.NextCursor)

		second, err := st.ExportVersions(ctx, ExportVersionsOptions{
			Cursor:               first.NextCursor,
			MinCommitTSExclusive: 10,
			MaxVersions:          10,
		})
		require.NoError(t, err)
		require.True(t, second.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("tail"), CommitTS: 20, Value: []byte("v")}}, second.Versions)
	})
}

func TestExportVersionsUsesUserKeyRangeBounds(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("a-value"), 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("aa"), []byte("aa-value"), 20, 0))
		require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("b-value"), 30, 0))

		mid, err := st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:    []byte("aa"),
			EndKey:      []byte("b"),
			MaxVersions: 10,
		})
		require.NoError(t, err)
		require.True(t, mid.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("aa"), CommitTS: 20, Value: []byte("aa-value")}}, mid.Versions)

		before, err := st.ExportVersions(ctx, ExportVersionsOptions{
			EndKey:      []byte("aa"),
			MaxVersions: 10,
		})
		require.NoError(t, err)
		require.True(t, before.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("a"), CommitTS: 10, Value: []byte("a-value")}}, before.Versions)
	})
}

func TestExportVersionsRejectsCursorOutsideRequestedRange(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("a10"), 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("a20"), 20, 0))
		require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("b30"), 30, 0))

		res, err := st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:    []byte("b"),
			EndKey:      []byte("c"),
			Cursor:      encodeExportCursor([]byte("a"), 20, exportCursorTagEmitted),
			MaxVersions: 10,
		})
		require.ErrorIs(t, err, ErrInvalidExportCursor)
		require.Empty(t, res.Versions)
	})
}

func TestExportVersionsDoesNotTreatMigrationPrefixUserKeyAsMetadata(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		key := append([]byte(migrationAckPrefix), bytes.Repeat([]byte{0x7}, migrationUint64Bytes)...)
		require.NoError(t, st.PutAt(ctx, key, []byte("value"), 10, 0))

		res, err := st.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 10})
		require.NoError(t, err)
		require.True(t, res.Done)
		require.Equal(t, []MVCCVersion{{Key: key, CommitTS: 10, Value: []byte("value")}}, res.Versions)
	})
}

func TestPebbleExportStopsAtEndKey(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-end-key-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	ps, ok := st.(*pebbleStore)
	require.True(t, ok)
	require.NoError(t, ps.PutAt(ctx, []byte("b"), []byte("later"), 10, 0))

	iter, err := ps.db.NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, iter.Close()) }()
	require.True(t, iter.SeekGE(encodeKey([]byte("b"), math.MaxUint64)))

	result := newExportVersionsResult(10)
	advance, done, err := ps.exportPebbleIteratorPosition(ctx, iter, ExportVersionsOptions{
		StartKey: []byte("a"),
		EndKey:   []byte("b"),
	}, exportCursorPosition{}, &result)
	require.ErrorIs(t, err, errExportReachedEnd)
	require.False(t, advance)
	require.True(t, done)
	require.Empty(t, result.Versions)
	require.Zero(t, result.ScannedBytes)
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

func TestPebbleSnapshotExcludesMigrationMetadata(t *testing.T) {
	ctx := context.Background()
	srcDir, err := os.MkdirTemp("", "migration-snapshot-src-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(srcDir)) })
	src, err := NewPebbleStore(srcDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })

	_, err = src.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("stale"),
		Versions:  []MVCCVersion{{Key: []byte("snapshotted"), CommitTS: 50, Value: []byte("v50")}},
	})
	require.NoError(t, err)
	snap, err := src.Snapshot()
	require.NoError(t, err)
	raw := snapshotBytes(t, snap)
	require.NoError(t, snap.Close())

	dstDir, err := os.MkdirTemp("", "migration-snapshot-dst-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dstDir)) })
	dst, err := NewPebbleStore(dstDir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, dst.Close()) })
	require.NoError(t, dst.Restore(bytes.NewReader(raw)))

	val, err := dst.GetAt(ctx, []byte("snapshotted"), 50)
	require.NoError(t, err)
	require.Equal(t, []byte("v50"), val)
	floor, err := dst.MigrationHLCFloor(ctx, 7)
	require.NoError(t, err)
	require.Zero(t, floor)

	res, err := dst.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("fresh"),
		Versions:  []MVCCVersion{{Key: []byte("fresh"), CommitTS: 60, Value: []byte("v60")}},
	})
	require.NoError(t, err)
	require.False(t, res.Duplicate)
	require.Equal(t, []byte("fresh"), res.AckedCursor)
	val, err = dst.GetAt(ctx, []byte("fresh"), 60)
	require.NoError(t, err)
	require.Equal(t, []byte("v60"), val)
}
