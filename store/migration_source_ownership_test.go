package store

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSourceOwnershipZeroFloorBoundarySurvivesNativeRestore(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		guards, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		guard := TargetStagedReadinessState{JobID: 7, MigrationJobID: 7, Armed: true, TrackWrites: true,
			RouteStart: []byte("m"), RouteEnd: []byte("z"), ExpectedCutoverVersion: 1, MinWriteTSExclusive: 1, RetentionPinTS: 1}
		require.NoError(t, guards.ApplyTargetStagedReadinessAt(t.Context(), guard, 10))
		state := MigrationOwnershipState{Generation: "0123456789abcdef0123456789abcdef", JobID: 7, LocalGroupID: 1, OwnerGroupID: 1,
			Role: MigrationOwner, CatalogVersion: 1, ConfigurationIndex: 9, RouteID: 1, DescriptorHash: bytes.Repeat([]byte{1}, 32),
			RouteStart: []byte("m"), RouteEnd: []byte("z"), WriteFloorKnown: true, SourceTrackerVersion: 1}
		proofs := ownershipStore(t, st)
		wrong := state
		wrong.WriteFloorKnown = false
		require.ErrorIs(t, proofs.AcquireMigrationOwnership(t.Context(), wrong, 11), ErrMigrationOwnership)
		wrong = state
		wrong.SourceTrackerVersion++
		require.ErrorIs(t, proofs.AcquireMigrationOwnership(t.Context(), wrong, 11), ErrMigrationOwnership)
		require.NoError(t, proofs.AcquireMigrationOwnership(t.Context(), state, 11))
		view, err := proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.True(t, MigrationOwnershipEvidenceValid(view.Ownership[0]))
		require.Zero(t, view.Ownership[0].MinWriteTSExclusive)
		require.Equal(t, uint64(11), view.Ownership[0].SourceAppliedIndex)
		snapshot, err := st.Snapshot()
		require.NoError(t, err)
		raw := snapshotBytes(t, snapshot)
		require.NoError(t, snapshot.Close())
		dir := t.TempDir()
		restored, err := NewPebbleStore(dir)
		require.NoError(t, err)
		require.NoError(t, restored.Restore(bytes.NewReader(raw)))
		require.NoError(t, restored.Close())
		restored, err = NewPebbleStore(dir)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, restored.Close()) })
		view, err = ownershipStore(t, restored).MigrationServingState(t.Context())
		require.NoError(t, err)
		require.True(t, MigrationOwnershipEvidenceValid(view.Ownership[0]))
		require.Equal(t, uint64(11), view.Ownership[0].SourceAppliedIndex)
		guard.SourceWriteFence, guard.SourceReadFence, guard.ExpectedCutoverVersion, guard.MinWriteTSExclusive = true, true, 2, 100
		require.NoError(t, guards.ApplyTargetStagedReadinessAt(t.Context(), guard, 12))
		state.OwnerGroupID, state.Role, state.CatalogVersion, state.MinWriteTSExclusive = 2, MigrationNonOwner, 2, 100
		state.SourceTrackerVersion, state.SourceAppliedIndex, state.DescriptorHash = 0, 0, bytes.Repeat([]byte{2}, 32)
		require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), state, 13))
		view, err = proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Empty(t, view.Readiness)
		require.True(t, MigrationOwnershipEvidenceValid(view.Ownership[0]))
		require.Len(t, view.Ownership[0].Receipts, 2)
	})
}

func TestLegacyOwnershipRemainsDecodableButNotOperational(t *testing.T) {
	st := NewMVCCStore()
	state := ownershipFixture(t, st)
	require.NoError(t, ownershipStore(t, st).ClearMigrationStateWithOwnership(t.Context(), state, 9))
	view, err := ownershipStore(t, st).MigrationServingState(t.Context())
	require.NoError(t, err)
	encoded, err := encodeMigrationOwnership(view.Ownership)
	require.NoError(t, err)
	decoded, err := decodeMigrationOwnership(encoded)
	require.NoError(t, err)
	require.False(t, MigrationOwnershipEvidenceValid(decoded[0]))
}
