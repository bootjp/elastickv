package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"os"
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
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

func TestExportVersionsExcludesTxnLocks(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		lockKey := append(bytes.Clone(txnLockKeyPrefix), []byte("user")...)
		require.NoError(t, st.PutAt(ctx, lockKey, []byte("lock"), 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("user"), []byte("value"), 20, 0))

		result, err := st.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 10})
		require.NoError(t, err)
		require.True(t, result.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("user"), CommitTS: 20, Value: []byte("value")}}, result.Versions)
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

func TestExportVersionsMinTSPruneCursorSkipsWholeKey(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		value := bytes.Repeat([]byte("x"), defaultSparseExportMaxScannedBytes)
		require.NoError(t, st.PutAt(ctx, []byte("old"), value, 1, 0))
		require.NoError(t, st.PutAt(ctx, []byte("old"), value, 2, 0))
		require.NoError(t, st.PutAt(ctx, []byte("old"), value, 3, 0))
		require.NoError(t, st.PutAt(ctx, []byte("tail"), []byte("v20"), 20, 0))

		first, err := st.ExportVersions(ctx, ExportVersionsOptions{
			MinCommitTSExclusive: 10,
			MaxVersions:          10,
		})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Empty(t, first.Versions)
		require.NotEmpty(t, first.NextCursor)

		cursor := first.NextCursor
		for attempts := 0; attempts < 4; attempts++ {
			next, err := st.ExportVersions(ctx, ExportVersionsOptions{
				Cursor:               cursor,
				MinCommitTSExclusive: 10,
				MaxVersions:          10,
			})
			require.NoError(t, err)
			if next.Done {
				require.Equal(t, []MVCCVersion{{Key: []byte("tail"), CommitTS: 20, Value: []byte("v20")}}, next.Versions)
				return
			}
			require.Empty(t, next.Versions)
			require.NotEmpty(t, next.NextCursor)
			cursor = next.NextCursor
		}
		t.Fatal("export did not finish after bounded pruned-key cursor resumes")
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

func TestExportVersionsEmptyEndKeyIsUnbounded(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, []byte("m"), []byte("m-value"), 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("z-value"), 20, 0))

		first, err := st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:    []byte("m"),
			EndKey:      []byte{},
			MaxVersions: 1,
		})
		require.NoError(t, err)
		require.False(t, first.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("m"), CommitTS: 10, Value: []byte("m-value")}}, first.Versions)
		require.NotEmpty(t, first.NextCursor)

		second, err := st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:    []byte("m"),
			EndKey:      []byte{},
			Cursor:      first.NextCursor,
			MaxVersions: 10,
		})
		require.NoError(t, err)
		require.True(t, second.Done)
		require.Empty(t, second.NextCursor)
		require.Equal(t, []MVCCVersion{{Key: []byte("z"), CommitTS: 20, Value: []byte("z-value")}}, second.Versions)
	})
}

func TestPebbleExportDoesNotStopBeforeTrailingEmptyKey(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-trailing-empty-key-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("a-value"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("b-value"), 20, 0))
	require.NoError(t, st.PutAt(ctx, nil, []byte("empty-value"), 30, 0))

	res, err := st.ExportVersions(ctx, ExportVersionsOptions{
		EndKey:      []byte("aa"),
		MaxVersions: 10,
	})
	require.NoError(t, err)
	require.True(t, res.Done)
	require.Len(t, res.Versions, 2)
	require.Equal(t, []byte("a"), res.Versions[0].Key)
	require.Equal(t, uint64(10), res.Versions[0].CommitTS)
	require.Equal(t, []byte("a-value"), res.Versions[0].Value)
	require.Empty(t, res.Versions[1].Key)
	require.Equal(t, uint64(30), res.Versions[1].CommitTS)
	require.Equal(t, []byte("empty-value"), res.Versions[1].Value)
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

func TestExportVersionsSkippedCursorBeforeStartResumesAtStartKey(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("a10"), 10, 0))
		require.NoError(t, st.PutAt(ctx, []byte("m"), []byte("m20"), 20, 0))

		res, err := st.ExportVersions(ctx, ExportVersionsOptions{
			StartKey:    []byte("m"),
			EndKey:      []byte("z"),
			Cursor:      encodeExportCursor([]byte("a"), 10, exportCursorTagSkippedKey),
			MaxVersions: 10,
		})
		require.NoError(t, err)
		require.True(t, res.Done)
		require.Equal(t, []MVCCVersion{{Key: []byte("m"), CommitTS: 20, Value: []byte("m20")}}, res.Versions)
	})
}

func TestValidateExportCursorForRangeRejectsSkippedCursorInsideRange(t *testing.T) {
	t.Parallel()

	err := ValidateExportCursorForRange(
		encodeExportCursor([]byte("stage|k"), 10, exportCursorTagSkippedKey),
		[]byte("stage|"),
		PrefixScanEnd([]byte("stage|")),
	)
	require.ErrorIs(t, err, ErrInvalidExportCursor)

	err = ValidateExportCursorForRange(
		encodeExportCursor([]byte("outside|k"), 10, exportCursorTagSkippedKey),
		[]byte("stage|"),
		PrefixScanEnd([]byte("stage|")),
	)
	require.NoError(t, err)
}

func TestValidatePromotionCursorForRangeAcceptsOnlyEmittedPositions(t *testing.T) {
	t.Parallel()

	prefix := []byte("stage|")
	key := []byte("stage|k")
	for _, tc := range []struct {
		name    string
		cursor  []byte
		wantErr bool
	}{
		{name: "empty cursor"},
		{name: "emitted cursor", cursor: encodeExportCursor(key, 10, exportCursorTagEmitted)},
		{name: "scanned cursor", cursor: encodeExportCursor(key, 10, exportCursorTagScanned), wantErr: true},
		{name: "pruned-key cursor", cursor: encodeExportCursor(key, 10, exportCursorTagPrunedKey), wantErr: true},
		{name: "skipped-key cursor", cursor: encodeExportCursor(key, 10, exportCursorTagSkippedKey), wantErr: true},
		{name: "emitted cursor outside range", cursor: encodeExportCursor([]byte("other|k"), 10, exportCursorTagEmitted), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidatePromotionCursorForRange(tc.cursor, prefix, PrefixScanEnd(prefix))
			if tc.wantErr {
				require.ErrorIs(t, err, ErrInvalidExportCursor)
				return
			}
			require.NoError(t, err)
		})
	}
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

func TestPebbleExportSkipsWriterRegistryRows(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-writer-registry-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	registry, err := WriterRegistryFor(st)
	require.NoError(t, err)
	require.NoError(t, registry.SetRegistryRow(
		encryption.RegistryKey(1, 2),
		encryption.EncodeRegistryValue(encryption.RegistryValue{
			FullNodeID:          2,
			FirstSeenLocalEpoch: 1,
			LastSeenLocalEpoch:  1,
		}),
	))
	require.NoError(t, st.PutAt(ctx, []byte("user"), []byte("value"), 10, 0))

	res, err := st.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 10})
	require.NoError(t, err)
	require.True(t, res.Done)
	require.Equal(t, []MVCCVersion{{Key: []byte("user"), CommitTS: 10, Value: []byte("value")}}, res.Versions)
}

func TestPebbleExportSkipsWriterRegistryRowsWithoutUserVersions(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-writer-registry-only-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	registry, err := WriterRegistryFor(st)
	require.NoError(t, err)
	require.NoError(t, registry.SetRegistryRow(
		encryption.RegistryKey(1, 2),
		encryption.EncodeRegistryValue(encryption.RegistryValue{
			FullNodeID:          2,
			FirstSeenLocalEpoch: 1,
			LastSeenLocalEpoch:  1,
		}),
	))

	res, err := st.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 10})
	require.NoError(t, err)
	require.True(t, res.Done)
	require.Empty(t, res.NextCursor)
	require.Empty(t, res.Versions)
	require.Zero(t, res.ScannedBytes)
	require.Zero(t, res.AcceptedRows)
}

func TestPebbleExportDoesNotDropWriterRegistryPrefixUserKey(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-writer-registry-user-key-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	registryKey := encryption.RegistryKey(1, 2)
	registry, err := WriterRegistryFor(st)
	require.NoError(t, err)
	require.NoError(t, registry.SetRegistryRow(
		registryKey,
		encryption.EncodeRegistryValue(encryption.RegistryValue{
			FullNodeID:          2,
			FirstSeenLocalEpoch: 1,
			LastSeenLocalEpoch:  1,
		}),
	))
	require.NoError(t, st.PutAt(ctx, registryKey, []byte("user-value"), 10, 0))

	res, err := st.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 10})
	require.NoError(t, err)
	require.True(t, res.Done)
	require.Equal(t, []MVCCVersion{{Key: registryKey, CommitTS: 10, Value: []byte("user-value")}}, res.Versions)
}

func TestPebbleRejectsMVCCKeyThatEncodesAsWriterRegistryRow(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-writer-registry-collision-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	rawRegistryKey := encryption.RegistryKey(1, 2)
	userKey := bytes.Clone(rawRegistryKey[:len(rawRegistryKey)-timestampSize])
	commitTS := ^binary.BigEndian.Uint64(rawRegistryKey[len(rawRegistryKey)-timestampSize:])
	require.True(t, isPebbleWriterRegistryKey(encodeKey(userKey, commitTS)))

	require.ErrorIs(t, st.PutAt(ctx, userKey, []byte("value"), commitTS, 0), errMVCCMetadataKeyCollision)
	_, err = st.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     1,
		BracketID: 1,
		BatchSeq:  1,
		Versions: []MVCCVersion{{
			Key:      userKey,
			CommitTS: commitTS,
			Value:    []byte("value"),
		}},
	})
	require.ErrorIs(t, err, errMVCCMetadataKeyCollision)
}

func TestPebbleWriterRegistryRowIsNotVisibleAsMVCCCollision(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-writer-registry-read-collision-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	registryKey := encryption.RegistryKey(1, 2)
	registry, err := WriterRegistryFor(st)
	require.NoError(t, err)
	require.NoError(t, registry.SetRegistryRow(
		registryKey,
		encryption.EncodeRegistryValue(encryption.RegistryValue{
			FullNodeID:          2,
			FirstSeenLocalEpoch: 1,
			LastSeenLocalEpoch:  1,
		}),
	))

	userKey := bytes.Clone(registryKey[:len(registryKey)-timestampSize])
	commitTS := ^binary.BigEndian.Uint64(registryKey[len(registryKey)-timestampSize:])
	_, err = st.GetAt(ctx, userKey, commitTS)
	require.ErrorIs(t, err, ErrKeyNotFound)
	ok, err := st.CommittedVersionAt(ctx, userKey, commitTS)
	require.NoError(t, err)
	require.False(t, ok)
	latest, ok, err := st.LatestCommitTS(ctx, userKey)
	require.NoError(t, err)
	require.False(t, ok)
	require.Zero(t, latest)
}

func TestPebbleExportAuthenticatesEncryptedTombstoneHeader(t *testing.T) {
	ctx := context.Background()
	f := newEncryptedStoreFixture(t, 81)
	require.NoError(t, f.mvcc.PutAt(ctx, []byte("tampered-export"), []byte("payload"), 100, 0))
	f.tamperPebbleValue(t, []byte("tampered-export"), 100, func(raw []byte) []byte {
		raw[0] |= tombstoneMask
		return raw
	})

	res, err := f.mvcc.ExportVersions(ctx, ExportVersionsOptions{MaxVersions: 10})
	require.ErrorIs(t, err, ErrEncryptedReadIntegrity)
	require.Empty(t, res.Versions)
}

func TestPebbleExportStopsAtEndKeyWhenNoLaterInRangeKeyCanTrail(t *testing.T) {
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

func TestPebbleExportStopsLeadingRangeWithExplicitEmptyStartAtEndKey(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-leading-end-key-*")
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
		StartKey: []byte{},
		EndKey:   []byte("b"),
	}, exportCursorPosition{}, &result)
	require.ErrorIs(t, err, errExportReachedEnd)
	require.False(t, advance)
	require.True(t, done)
	require.Empty(t, result.Versions)
	require.Zero(t, result.ScannedBytes)
}

func TestPebbleExportOutOfRangeEndSkipReturnsResumableCursor(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-out-of-range-end-skip-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("later"), 10, 0))
	require.NoError(t, st.PutAt(ctx, nil, []byte("empty"), 20, 0))

	first, err := st.ExportVersions(ctx, ExportVersionsOptions{
		EndKey:          []byte("a"),
		MaxVersions:     10,
		MaxScannedBytes: 1,
	})
	require.NoError(t, err)
	require.False(t, first.Done)
	require.Empty(t, first.Versions)
	require.NotEmpty(t, first.NextCursor)
	require.Greater(t, first.ScannedBytes, uint64(0))

	second, err := st.ExportVersions(ctx, ExportVersionsOptions{
		EndKey:      []byte("a"),
		Cursor:      first.NextCursor,
		MaxVersions: 10,
	})
	require.NoError(t, err)
	require.True(t, second.Done)
	require.Equal(t, []MVCCVersion{{Key: []byte{}, CommitTS: 20, Value: []byte("empty")}}, second.Versions)
}

func TestPebbleExportOutOfRangeEndSkipChargesAllVersions(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-out-of-range-end-skip-versions-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("v10"), 10, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("v20"), 20, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("v30"), 30, 0))
	require.NoError(t, st.PutAt(ctx, nil, []byte("empty"), 40, 0))

	first, err := st.ExportVersions(ctx, ExportVersionsOptions{
		EndKey:          []byte("a"),
		MaxVersions:     10,
		MaxScannedBytes: versionExportSize([]byte("b"), len("v30")) + 1,
	})
	require.NoError(t, err)
	require.False(t, first.Done)
	require.Empty(t, first.Versions)
	require.NotEmpty(t, first.NextCursor)
	require.GreaterOrEqual(t, first.ScannedBytes, versionExportSize([]byte("b"), len("v30"))+1)

	second, err := st.ExportVersions(ctx, ExportVersionsOptions{
		EndKey:      []byte("a"),
		Cursor:      first.NextCursor,
		MaxVersions: 10,
	})
	require.NoError(t, err)
	require.True(t, second.Done)
	require.Equal(t, []MVCCVersion{{Key: []byte{}, CommitTS: 40, Value: []byte("empty")}}, second.Versions)
}

func TestPebbleExportAppliesDefaultScanBudgetForRangeBoundSkip(t *testing.T) {
	ctx := context.Background()
	dir, err := os.MkdirTemp("", "migration-range-bound-scan-budget-*")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	value := bytes.Repeat([]byte("x"), defaultSparseExportMaxScannedBytes)
	require.NoError(t, st.PutAt(ctx, []byte("b"), value, 10, 0))
	require.NoError(t, st.PutAt(ctx, nil, []byte("empty"), 20, 0))

	first, err := st.ExportVersions(ctx, ExportVersionsOptions{
		EndKey:      []byte("a"),
		MaxVersions: 10,
	})
	require.NoError(t, err)
	require.False(t, first.Done)
	require.Empty(t, first.Versions)
	require.NotEmpty(t, first.NextCursor)
	require.GreaterOrEqual(t, first.ScannedBytes, uint64(defaultSparseExportMaxScannedBytes))

	second, err := st.ExportVersions(ctx, ExportVersionsOptions{
		EndKey:      []byte("a"),
		Cursor:      first.NextCursor,
		MaxVersions: 10,
	})
	require.NoError(t, err)
	require.True(t, second.Done)
	require.Equal(t, []MVCCVersion{{Key: []byte{}, CommitTS: 20, Value: []byte("empty")}}, second.Versions)
}

func TestPebbleWriterRegistryKeyShapeRejectsOrdinaryRows(t *testing.T) {
	registryKey := encryption.RegistryKey(1, 2)
	require.True(t, isPebbleWriterRegistryKey(registryKey))

	ordinary := encodeKey([]byte("ordinary"), 10)
	require.False(t, isPebbleWriterRegistryKey(ordinary))

	malformed := bytes.Clone(registryKey)
	malformed[len(encryption.WriterRegistryPrefix)+4] = '#'
	require.False(t, isPebbleWriterRegistryKey(malformed))
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
		require.Equal(t, uint64(30), state.MaxPromotedTS)

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
		require.Equal(t, uint64(30), second.MaxPromotedTS)
		state, ok, err = stateReader.MigrationPromotionState(ctx, 99)
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, state.Done)
		require.Empty(t, state.Cursor)
		require.Equal(t, uint64(4), state.PromotedRows)
		require.Equal(t, uint64(30), state.MaxPromotedTS)

		retry, err := promoter.PromoteVersions(ctx, PromoteVersionsOptions{
			JobID:       99,
			StartKey:    prefix,
			EndKey:      PrefixScanEnd(prefix),
			MaxVersions: 10,
			TargetKey:   targetKey,
		})
		require.NoError(t, err)
		require.True(t, retry.Done)
		require.Zero(t, retry.PromotedRows)
		require.Equal(t, uint64(4), retry.TotalPromotedRows)
		require.Equal(t, uint64(30), retry.MaxPromotedTS)

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

func TestPromoteVersionsIgnoresClientCursorWhenStateMissing(t *testing.T) {
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

		require.NoError(t, st.PutAt(ctx, stage("a"), []byte("a10"), 10, 0))
		require.NoError(t, st.PutAt(ctx, stage("z"), []byte("z20"), 20, 0))
		staleCursor := encodeExportCursor(stage("m"), 1, exportCursorTagEmitted)

		result, err := promoter.PromoteVersions(ctx, PromoteVersionsOptions{
			JobID:       202,
			StartKey:    prefix,
			EndKey:      PrefixScanEnd(prefix),
			Cursor:      staleCursor,
			MaxVersions: 10,
			TargetKey:   targetKey,
		})
		require.NoError(t, err)
		require.True(t, result.Done)
		require.Equal(t, uint64(2), result.PromotedRows)
		require.Equal(t, uint64(2), result.TotalPromotedRows)

		state, ok, err := stateReader.MigrationPromotionState(ctx, 202)
		require.NoError(t, err)
		require.True(t, ok)
		require.True(t, state.Done)
		require.Equal(t, uint64(2), state.PromotedRows)
		require.Equal(t, uint64(20), state.MaxPromotedTS)

		got, err := st.GetAt(ctx, []byte("a"), 10)
		require.NoError(t, err)
		require.Equal(t, []byte("a10"), got)
		got, err = st.GetAt(ctx, []byte("z"), 20)
		require.NoError(t, err)
		require.Equal(t, []byte("z20"), got)
		stagedLeft, err := st.ExportVersions(ctx, ExportVersionsOptions{
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
	state, ok, err := ps.MigrationPromotionState(ctx, 101)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, promotedTS, state.MaxPromotedTS)

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
	reopenedPromoter, ok := reopened.(MigrationPromoter)
	require.True(t, ok)
	retry, err := reopenedPromoter.PromoteVersions(ctx, PromoteVersionsOptions{
		JobID:       101,
		StartKey:    prefix,
		EndKey:      PrefixScanEnd(prefix),
		MaxVersions: 10,
		TargetKey:   targetKey,
	})
	require.NoError(t, err)
	require.True(t, retry.Done)
	require.Zero(t, retry.PromotedRows)
	require.Equal(t, uint64(1), retry.TotalPromotedRows)
	require.Equal(t, promotedTS, retry.MaxPromotedTS)
}

func TestPromotionStateCodecPreservesMaxPromotedTS(t *testing.T) {
	t.Parallel()

	state := PromotionState{
		Cursor:        []byte("cursor"),
		Done:          true,
		PromotedRows:  7,
		MaxPromotedTS: 42,
		LastError:     "boom",
	}
	decoded, ok := decodePromotionState(encodePromotionState(state))
	require.True(t, ok)
	require.Equal(t, state, decoded)

	old := []byte{migrationPromotionDoneFlag}
	old = binary.BigEndian.AppendUint64(old, 3)
	old = binary.AppendUvarint(old, lenAsUint64(len("old-cursor")))
	old = append(old, "old-cursor"...)
	old = binary.AppendUvarint(old, lenAsUint64(len("old-error")))
	old = append(old, "old-error"...)
	decoded, ok = decodePromotionState(old)
	require.True(t, ok)
	require.True(t, decoded.Done)
	require.Equal(t, uint64(3), decoded.PromotedRows)
	require.Zero(t, decoded.MaxPromotedTS)
	require.Equal(t, []byte("old-cursor"), decoded.Cursor)
	require.Equal(t, "old-error", decoded.LastError)
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

func TestPebbleSnapshotPreservesMigrationMetadata(t *testing.T) {
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
	require.Equal(t, uint64(50), floor)

	res, err := dst.ImportVersions(ctx, ImportVersionsOptions{
		JobID:     7,
		BracketID: 3,
		BatchSeq:  1,
		Cursor:    []byte("fresh"),
		Versions:  []MVCCVersion{{Key: []byte("fresh"), CommitTS: 60, Value: []byte("v60")}},
	})
	require.NoError(t, err)
	require.True(t, res.Duplicate)
	require.Equal(t, []byte("stale"), res.AckedCursor)
	_, err = dst.GetAt(ctx, []byte("fresh"), 60)
	require.ErrorIs(t, err, ErrKeyNotFound)
}
