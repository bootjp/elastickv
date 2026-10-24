package store

import (
	"bytes"
	"encoding/binary"
	"log/slog"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func ownershipFixture(t *testing.T, st MVCCStore) MigrationOwnershipState {
	t.Helper()
	writer, ok := st.(MigrationTargetReadinessRaftWriter)
	require.True(t, ok)
	require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), TargetStagedReadinessState{
		JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true,
		SourceWriteFence: true, SourceReadFence: true, RouteStart: []byte("a"), RouteEnd: []byte("z"),
		ExpectedCutoverVersion: 10, MinWriteTSExclusive: 100, RetentionPinTS: 1,
	}, 8))
	return MigrationOwnershipState{JobID: 7, LocalGroupID: 1, OwnerGroupID: 2, CatalogVersion: 10,
		ConfigurationIndex: 3, RouteStart: []byte("a"), RouteEnd: []byte("z"),
		MinWriteTSExclusive: 100, Role: MigrationNonOwner}
}

func ownershipStore(t *testing.T, st MVCCStore) MigrationOwnershipStore {
	t.Helper()
	proofs, ok := st.(MigrationOwnershipStore)
	require.True(t, ok)
	return proofs
}

func assertOwnershipCleared(t *testing.T, st MVCCStore) {
	t.Helper()
	state, err := ownershipStore(t, st).MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Empty(t, state.Readiness)
	require.Len(t, state.Ownership, 1)
	require.Equal(t, MigrationNonOwner, state.Ownership[0].Role)
	require.Equal(t, uint64(10), state.Ownership[0].CatalogVersion)
	require.Equal(t, uint64(9), state.Ownership[0].AppliedIndex)
	retirement, ok := st.(MigrationRetirementReader)
	require.True(t, ok)
	retired, err := retirement.MigrationRetired(t.Context(), 7)
	require.NoError(t, err)
	require.True(t, retired)
}

func TestMigrationOwnershipAtomicClearAndImmutableRetry(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		state := ownershipFixture(t, st)
		proofs := ownershipStore(t, st)
		wrong := state
		wrong.RouteEnd = []byte("y")
		require.ErrorIs(t, proofs.ClearMigrationStateWithOwnership(t.Context(), wrong, 9), ErrMigrationOwnership)
		view, err := proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Len(t, view.Readiness, 1)
		require.Empty(t, view.Ownership)
		require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), state, 9))
		assertOwnershipCleared(t, st)
		require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), state, 10))
		state.CatalogVersion++
		require.ErrorIs(t, proofs.ClearMigrationStateWithOwnership(t.Context(), state, 11), ErrMigrationOwnership,
			"cache advancement or a retry cannot revise the old ownership certificate")
		require.NoError(t, st.Compact(t.Context(), 100))
		assertOwnershipCleared(t, st)
		_, err = st.ImportVersions(t.Context(), ImportVersionsOptions{JobID: 8, BracketID: 1, BatchSeq: 1, Versions: []MVCCVersion{{
			Key: migrationOwnershipMetaKeyBytes, Value: []byte("forged"), CommitTS: 200,
		}}})
		require.ErrorIs(t, err, ErrInvalidImportVersion)
	})
}

func TestMigrationOwnershipSnapshotRestoreAndReopen(t *testing.T) {
	source := NewMVCCStore()
	state := ownershipFixture(t, source)
	require.NoError(t, ownershipStore(t, source).ClearMigrationStateWithOwnership(t.Context(), state, 9))
	snapshot, err := source.Snapshot()
	require.NoError(t, err)
	raw := snapshotBytes(t, snapshot)
	require.NoError(t, snapshot.Close())
	require.Equal(t, mvccSnapshotVersionV6, binary.LittleEndian.Uint32(raw[len(mvccSnapshotMagic):]))
	runMigrationStoreSuite(t, func(t *testing.T, destination MVCCStore) {
		require.NoError(t, destination.Restore(bytes.NewReader(raw)))
		assertOwnershipCleared(t, destination)
	})
	dir := t.TempDir()
	destination, err := NewPebbleStore(dir)
	require.NoError(t, err)
	require.NoError(t, destination.Restore(bytes.NewReader(raw)))
	require.NoError(t, destination.Close())
	destination, err = NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, destination.Close()) })
	assertOwnershipCleared(t, destination)
}

func TestMigrationOwnershipNativeAndSSTSnapshots(t *testing.T) {
	for _, sst := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "sst"}[sst], func(t *testing.T) {
			source, err := NewPebbleStore(t.TempDir(), WithSSTIngestSnapshots(sst))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, source.Close()) })
			state := ownershipFixture(t, source)
			require.NoError(t, ownershipStore(t, source).ClearMigrationStateWithOwnership(t.Context(), state, 9))
			snapshot, err := source.Snapshot()
			require.NoError(t, err)
			raw := snapshotBytes(t, snapshot)
			require.NoError(t, snapshot.Close())
			target, err := NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, target.Close()) })
			require.NoError(t, target.Restore(bytes.NewReader(raw)))
			assertOwnershipCleared(t, target)
		})
	}
}

func TestMigrationOwnershipFailedBatchKeepsGuardAndAppliedIndex(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{DisableWAL: true})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	st := &pebbleStore{db: db, log: slog.Default(), fsmApplyWriteOpts: pebble.NoSync}
	state := ownershipFixture(t, st)
	st.fsmApplyWriteOpts = pebble.Sync
	require.ErrorContains(t, st.ClearMigrationStateWithOwnership(t.Context(), state, 9), "WAL disabled")
	view, err := st.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Len(t, view.Readiness, 1)
	require.Empty(t, view.Ownership)
	retired, err := st.MigrationRetired(t.Context(), 7)
	require.NoError(t, err)
	require.False(t, retired)
	index, found, err := st.LastAppliedIndex()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(8), index)
}

func TestMigrationOwnershipCorruptionRejectsReopen(t *testing.T) {
	dir := t.TempDir()
	st, err := NewPebbleStore(dir)
	require.NoError(t, err)
	ps, ok := st.(*pebbleStore)
	require.True(t, ok)
	require.NoError(t, ps.db.Set(migrationOwnershipMetaKeyBytes, []byte("corrupt"), pebble.Sync))
	_, err = ps.MigrationServingState(t.Context())
	require.ErrorContains(t, err, "invalid migration ownership encoding")
	require.NoError(t, st.Close())
	_, err = NewPebbleStore(dir)
	require.ErrorContains(t, err, "invalid migration ownership encoding")
}

func TestMigrationOwnershipMoveBackRequiresNewerGuardBoundAcquisition(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		state := ownershipFixture(t, st)
		proofs := ownershipStore(t, st)
		require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), state, 9))
		writer, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		guard := TargetStagedReadinessState{JobID: 8, MigrationJobID: 8, Armed: true,
			RouteStart: []byte("a"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 12, MinWriteTSExclusive: 200}
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 10))
		acquisition := MigrationOwnershipState{JobID: 8, LocalGroupID: 1, OwnerGroupID: 1, CatalogVersion: 12,
			ConfigurationIndex: 4, MinWriteTSExclusive: 200, RouteStart: []byte("a"), RouteEnd: []byte("z"),
			Role: MigrationOwner, Staged: true}
		for _, version := range []uint64{10, 11, 13} {
			wrong := acquisition
			wrong.CatalogVersion = version
			require.ErrorIs(t, proofs.AcquireMigrationOwnership(t.Context(), wrong, 11), ErrMigrationOwnership)
		}
		wrong := acquisition
		wrong.MinWriteTSExclusive--
		require.ErrorIs(t, proofs.AcquireMigrationOwnership(t.Context(), wrong, 11), ErrMigrationOwnership)
		view, err := proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Len(t, view.Ownership, 1, "cache/version changes cannot erase NONOWNER evidence")
		require.NoError(t, proofs.AcquireMigrationOwnership(t.Context(), acquisition, 11))
		view, err = proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Len(t, view.Ownership, 2, "the newer replicated certificate supersedes, not deletes, the old range proof")
		require.NoError(t, proofs.AcquireMigrationOwnership(t.Context(), acquisition, 12), "exact replay is idempotent")
	})
}
