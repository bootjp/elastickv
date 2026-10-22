package store

import (
	"bytes"
	"context"
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestMetadataCleanupPermanentlyRejectsLateMigrationMutations(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		ctx := context.Background()
		cleaner, ok := st.(MigrationCleaner)
		require.True(t, ok)
		require.NoError(t, st.PutAt(ctx, []byte("customer"), []byte("active"), 100, 0))
		_, err := st.ImportVersions(ctx, ImportVersionsOptions{JobID: 7, BracketID: 1, BatchSeq: 1, Cursor: []byte("old-progress")})
		require.NoError(t, err)
		require.NoError(t, cleaner.ClearMigrationState(ctx, 7, 10))
		assertLateMigrationRejected(t, st)
		require.NoError(t, cleaner.ClearMigrationState(ctx, 7, 11), "cleanup must remain idempotent")
		require.NoError(t, cleaner.ClearMigrationState(ctx, 99, 12))
		_, err = st.ImportVersions(ctx, ImportVersionsOptions{JobID: 8, BracketID: 1, BatchSeq: 1})
		require.NoError(t, err, "retirement is per job")
	})
}

func assertLateMigrationRejected(t *testing.T, st MVCCStore) {
	t.Helper()
	ctx := context.Background()
	writer, ok := st.(MigrationTargetReadinessWriter)
	require.True(t, ok)
	err := writer.ApplyTargetStagedReadiness(ctx, TargetStagedReadinessState{JobID: 7, MigrationJobID: 7,
		RouteStart: []byte("a"), RouteEnd: []byte("z"), Armed: true, SourceWriteFence: true, MinWriteTSExclusive: 1, RetentionPinTS: 1})
	require.ErrorContains(t, err, "migration job is retired")
	raftWriter, ok := st.(MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.ErrorContains(t, raftWriter.ApplyTargetStagedReadinessAt(ctx, TargetStagedReadinessState{JobID: 7, MigrationJobID: 7}, 20), "migration job is retired")
	_, err = st.ImportVersions(ctx, ImportVersionsOptions{JobID: 7, BracketID: 1, BatchSeq: 1,
		Versions: []MVCCVersion{{Key: []byte("late-staging"), CommitTS: 200, Value: []byte("late")}}})
	require.ErrorContains(t, err, "migration job is retired")
	_, err = st.ImportVersionsRaft(ctx, ImportVersionsOptions{JobID: 7, BracketID: 1, BatchSeq: 1, AppliedIndex: 21})
	require.ErrorContains(t, err, "migration job is retired", "a duplicate/empty batch cannot bypass retirement")
	promoter, ok := st.(MigrationPromoter)
	require.True(t, ok)
	_, err = promoter.PromoteVersions(ctx, PromoteVersionsOptions{JobID: 7, MaxVersions: 10, TargetKey: func(key []byte) ([]byte, bool) { return key, true }})
	require.ErrorContains(t, err, "migration job is retired")
	_, err = promoter.PromoteVersions(ctx, PromoteVersionsOptions{JobID: 7, TargetKey: func(key []byte) ([]byte, bool) { return key, true }})
	require.ErrorContains(t, err, "migration job is retired", "zero budgets must not report Done")
	cleaner, ok := st.(MigrationCleaner)
	require.True(t, ok)
	_, err = cleaner.CleanupVersions(ctx, CleanupVersionsOptions{JobID: 7, StartKey: []byte("customer"), MaxVersions: 10, MaxCommitTS: 1000})
	require.ErrorContains(t, err, "migration job is retired")
	_, err = cleaner.CleanupVersions(ctx, CleanupVersionsOptions{JobID: 7})
	require.ErrorContains(t, err, "migration job is retired")
	require.NoError(t, st.RetireMigration(ctx, 7), "legacy retirement must preserve the witness")
	retirement, ok := st.(MigrationRetirementReader)
	require.True(t, ok)
	retired, err := retirement.MigrationRetired(ctx, 7)
	require.NoError(t, err)
	require.True(t, retired)
	reader, ok := st.(MigrationImportMetadataReader)
	require.True(t, ok)
	present, err := reader.MigrationImportMetadataPresent(ctx, 7)
	require.NoError(t, err)
	require.False(t, present, "a retirement witness is not live import progress")
	readiness, ok := st.(MigrationTargetReadinessReader)
	require.True(t, ok)
	states, err := readiness.MigrationTargetReadinessStates(ctx)
	require.NoError(t, err)
	require.Empty(t, states, "retirement must not retain an active guard or retention pin")
	require.Equal(t, uint64(100), st.LastCommitTS(), "rejected requests must not move the clock")
	require.NoError(t, st.Compact(ctx, 100), "ordinary compaction must not collect retirement")
	retired, err = retirement.MigrationRetired(ctx, 7)
	require.NoError(t, err)
	require.True(t, retired)
	value, err := st.GetAt(ctx, []byte("customer"), 100)
	require.NoError(t, err)
	require.Equal(t, []byte("active"), value)
}

func TestMigrationRetirementSurvivesSnapshotRestoreAndReopen(t *testing.T) {
	source := NewMVCCStore()
	initial, err := source.Snapshot()
	require.NoError(t, err)
	initialRaw := snapshotBytes(t, initial)
	require.NoError(t, initial.Close())
	require.Equal(t, mvccSnapshotVersionV1, binary.LittleEndian.Uint32(initialRaw[len(mvccSnapshotMagic):]), "no witness must retain the old snapshot format")
	cleaner, ok := source.(MigrationCleaner)
	require.True(t, ok)
	require.NoError(t, source.PutAt(t.Context(), []byte("customer"), []byte("active"), 100, 0))
	require.NoError(t, cleaner.ClearMigrationState(t.Context(), 7, 10))
	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	raw := snapshotBytes(t, snapshot)
	require.Equal(t, mvccSnapshotVersionV5, binary.LittleEndian.Uint32(raw[len(mvccSnapshotMagic):]))
	require.NoError(t, snapshot.Close())
	runMigrationStoreSuite(t, func(t *testing.T, destination MVCCStore) {
		require.NoError(t, destination.Restore(bytes.NewReader(raw)))
		assertLateMigrationRejected(t, destination)
	})
	dir := t.TempDir()
	pebble, err := NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, pebble.Restore(bytes.NewReader(raw)))
	require.NoError(t, pebble.Close())
	pebble, err = NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pebble.Close()) })
	assertLateMigrationRejected(t, pebble)
}

func TestMigrationRetirementNativeAndSSTSnapshots(t *testing.T) {
	for _, sst := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "sst"}[sst], func(t *testing.T) {
			source, err := NewPebbleStore(t.TempDir(), WithSSTIngestSnapshots(sst))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			require.NoError(t, source.PutAt(t.Context(), []byte("customer"), []byte("active"), 100, 0))
			cleaner, ok := source.(MigrationCleaner)
			require.True(t, ok)
			require.NoError(t, cleaner.ClearMigrationState(t.Context(), 7, 10))
			snapshot, err := source.Snapshot()
			require.NoError(t, err)
			raw := snapshotBytes(t, snapshot)
			require.NoError(t, snapshot.Close())
			target, err := NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, target.Close()) })
			require.NoError(t, target.Restore(bytes.NewReader(raw)))
			assertLateMigrationRejected(t, target)
		})
	}
}

func TestMigrationRetirementCleanupBatchFailureIsAtomic(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{DisableWAL: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	s := &pebbleStore{db: db, log: slog.Default(), fsmApplyWriteOpts: pebble.NoSync}
	state := TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, Armed: true, SourceWriteFence: true,
		MinWriteTSExclusive: 100, RetentionPinTS: 1}
	require.NoError(t, s.ApplyTargetStagedReadinessAt(t.Context(), state, 5))
	// A synced commit on a no-WAL DB deterministically fails before publication.
	s.fsmApplyWriteOpts = pebble.Sync
	require.ErrorContains(t, s.ClearMigrationState(t.Context(), 7, 6), "WAL disabled")
	retired, err := s.MigrationRetired(t.Context(), 7)
	require.NoError(t, err)
	require.False(t, retired)
	states, err := s.MigrationTargetReadinessStates(t.Context())
	require.NoError(t, err)
	require.Len(t, states, 1)
	index, found, err := s.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(5), index)
	s.fsmApplyWriteOpts = pebble.NoSync
	require.NoError(t, s.ClearMigrationState(t.Context(), 7, 6))
	retired, err = s.MigrationRetired(t.Context(), 7)
	require.NoError(t, err)
	require.True(t, retired)
	states, err = s.MigrationTargetReadinessStates(t.Context())
	require.NoError(t, err)
	require.Empty(t, states)
	index, found, err = s.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(6), index)
}

func TestCorruptMigrationRetirementFailsClosed(t *testing.T) {
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	ps, ok := st.(*pebbleStore)
	require.True(t, ok)
	require.NoError(t, ps.db.Set(migrationRetiredMetaKeyBytes, []byte("corrupt"), pebble.Sync))
	_, err = ps.ImportVersions(t.Context(), ImportVersionsOptions{JobID: 8, BracketID: 1, BatchSeq: 1})
	require.ErrorContains(t, err, "corrupt migration retirement")
	require.NotErrorIs(t, err, ErrMigrationRetired, "corruption must not be mistaken for an ordinary retired-job verdict")
	require.ErrorContains(t, ps.ClearMigrationState(t.Context(), 8, 10), "corrupt migration retirement")
	require.NoError(t, st.Close())
	_, err = NewPebbleStore(dir)
	require.ErrorContains(t, err, "corrupt migration retirement")
}
