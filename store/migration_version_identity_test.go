package store

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationVersionIdentityImportIsAtomic(t *testing.T) {
	t.Parallel()
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		key := []byte("staged")
		original := MVCCVersion{Key: key, CommitTS: 10, Value: []byte("observed"), ExpireAt: 20}
		opts := ImportVersionsOptions{JobID: 1, BracketID: 1, BatchSeq: 1, Versions: []MVCCVersion{original}}
		_, err := st.ImportVersions(ctx, opts)
		require.NoError(t, err)

		opts.BatchSeq++
		opts.Cursor = []byte("batch-2")
		newRow := MVCCVersion{Key: []byte("unrelated"), CommitTS: 40, Value: []byte("new")}
		for _, replacement := range []MVCCVersion{
			{Key: key, CommitTS: 10, Value: []byte("different"), ExpireAt: 20},
			{Key: key, CommitTS: 10, Value: original.Value, ExpireAt: 21},
			{Key: key, CommitTS: 10, Tombstone: true},
		} {
			opts.Versions = []MVCCVersion{newRow, replacement}
			_, err := st.ImportVersions(ctx, opts)
			require.ErrorIs(t, err, ErrMigrationVersionConflict)
			require.Equal(t, uint64(10), st.LastCommitTS())
			floor, err := st.MigrationHLCFloor(ctx, opts.JobID)
			require.NoError(t, err)
			require.Equal(t, uint64(10), floor)
		}

		// A conflicting pair for a new physical key is also an overwrite.
		opts.Versions = []MVCCVersion{newRow, {Key: newRow.Key, CommitTS: newRow.CommitTS, Tombstone: true}}
		_, err = st.ImportVersions(ctx, opts)
		require.ErrorIs(t, err, ErrMigrationVersionConflict)
		_, err = st.GetAt(ctx, newRow.Key, newRow.CommitTS)
		require.ErrorIs(t, err, ErrKeyNotFound)

		// The rejected batch did not advance the acknowledgement.
		opts.Versions = []MVCCVersion{original, original, newRow}
		result, err := st.ImportVersions(ctx, opts)
		require.NoError(t, err)
		require.False(t, result.Duplicate)
		require.Equal(t, opts.Cursor, result.AckedCursor)
		result, err = st.ImportVersions(ctx, opts)
		require.NoError(t, err)
		require.True(t, result.Duplicate)

		// A retry of an acknowledged sequence cannot replace a version.
		opts.Versions = []MVCCVersion{{Key: key, CommitTS: 10, Tombstone: true}}
		result, err = st.ImportVersions(ctx, opts)
		require.NoError(t, err)
		require.True(t, result.Duplicate)
		got, err := st.GetAt(ctx, key, 10)
		require.NoError(t, err)
		require.Equal(t, original.Value, got)
	})
}

func TestMigrationVersionIdentityRaftVerdictIgnoresPrunedHistory(t *testing.T) {
	t.Parallel()
	forEachMVCCStore(t, func(t *testing.T, newStore func(*testing.T) MVCCStore) {
		ctx := context.Background()
		key := []byte("staged")
		old := MVCCVersion{Key: key, CommitTS: 10, Value: []byte("previous")}
		head := MVCCVersion{Key: key, CommitTS: 20, Value: []byte("current")}
		for _, prune := range []bool{false, true} {
			st := newStore(t)
			opts := ImportVersionsOptions{JobID: 1, BracketID: 1, BatchSeq: 1, Versions: []MVCCVersion{old, head}}
			_, err := st.ImportVersionsRaft(ctx, opts)
			require.NoError(t, err)
			if prune {
				require.NoError(t, st.Compact(ctx, 20))
				found, err := st.CommittedVersionAt(ctx, key, 10)
				require.NoError(t, err)
				require.False(t, found)
			}

			opts.BatchSeq++
			opts.Versions = []MVCCVersion{{Key: key, CommitTS: 10, Value: []byte("historical import")}}
			_, err = st.ImportVersionsRaft(ctx, opts)
			require.NoError(t, err)

			opts.BatchSeq++
			opts.Versions = []MVCCVersion{{Key: key, CommitTS: 20, Tombstone: true}}
			_, err = st.ImportVersionsRaft(ctx, opts)
			require.ErrorIs(t, err, ErrMigrationVersionConflict)
			got, err := st.GetAt(ctx, key, 20)
			require.NoError(t, err)
			require.Equal(t, head.Value, got)
		}
	})
}

func TestMigrationVersionIdentityIdenticalEncryptedImport(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := newEncryptedStoreFixture(t, 1).mvcc
	key := []byte("staged")
	value := []byte("plaintext")
	version := MVCCVersion{Key: key, CommitTS: 10, Value: value, ExpireAt: 20}
	opts := ImportVersionsOptions{JobID: 1, BracketID: 1, BatchSeq: 1, Versions: []MVCCVersion{version}}
	_, err := st.ImportVersionsRaft(ctx, opts)
	require.NoError(t, err)
	opts.BatchSeq++
	result, err := st.ImportVersionsRaft(ctx, opts)
	require.NoError(t, err)
	require.False(t, result.Duplicate)

	opts.BatchSeq++
	opts.Versions = []MVCCVersion{{Key: key, CommitTS: 10, Value: value, ExpireAt: 19}}
	_, err = st.ImportVersionsRaft(ctx, opts)
	require.ErrorIs(t, err, ErrMigrationVersionConflict)
	got, err := st.GetAt(ctx, key, 19)
	require.NoError(t, err)
	require.Equal(t, value, got)
}

func TestMigrationVersionIdentityPromotionConflictIsAtomic(t *testing.T) {
	t.Parallel()
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		live := []byte("b")
		stagedPrefix := []byte("stage/")
		firstStaged := []byte("stage/a")
		conflictingStaged := []byte("stage/b")
		// An empty live value and a tombstone are distinct immutable records.
		require.NoError(t, st.PutAt(ctx, live, []byte{}, 10, 0))
		require.NoError(t, st.PutAt(ctx, firstStaged, []byte("first"), 10, 0))
		require.NoError(t, st.DeleteAt(ctx, conflictingStaged, 10))
		promoter, ok := st.(MigrationPromoter)
		require.True(t, ok)
		_, err := promoter.PromoteVersions(ctx, PromoteVersionsOptions{
			JobID: 1, AppliedIndex: 1, StartKey: stagedPrefix, EndKey: PrefixScanEnd(stagedPrefix), MaxVersions: 16,
			TargetKey: func(key []byte) ([]byte, bool) { return key[len(stagedPrefix):], true },
		})
		require.ErrorIs(t, err, ErrMigrationVersionConflict)
		_, err = st.GetAt(ctx, live, 10)
		require.NoError(t, err)
		_, err = st.GetAt(ctx, []byte("a"), 10)
		require.ErrorIs(t, err, ErrKeyNotFound)
		got, err := st.GetAt(ctx, firstStaged, 10)
		require.NoError(t, err)
		require.Equal(t, []byte("first"), got)
		found, err := st.CommittedVersionAt(ctx, conflictingStaged, 10)
		require.NoError(t, err)
		require.True(t, found)
	})
}
