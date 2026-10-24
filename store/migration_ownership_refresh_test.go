package store

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMigrationOwnershipRefreshPreservesImmutableReceipt(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		state := ownershipFixture(t, st)
		state.Generation, state.RouteID, state.ParentRouteID = "0123456789abcdef0123456789abcdef", 3, 1
		state.DescriptorHash = bytes.Repeat([]byte{1}, 32)
		writer := ownershipStore(t, st)
		require.NoError(t, writer.ClearMigrationStateWithOwnership(t.Context(), state, 9))
		refresher, ok := st.(MigrationOwnershipRefresher)
		require.True(t, ok)
		refreshed := state
		refreshed.CatalogVersion, refreshed.ConfigurationIndex = 15, 4
		require.NoError(t, refresher.RefreshMigrationOwnership(t.Context(), refreshed, 12))
		view, err := writer.MigrationServingState(t.Context())
		require.NoError(t, err)
		proof := view.Ownership[0]
		require.Equal(t, uint64(15), proof.CatalogVersion)
		require.Equal(t, uint64(4), proof.ConfigurationIndex)
		require.Len(t, proof.Receipts, 1)
		require.Equal(t, uint64(10), proof.Receipts[0].CatalogVersion)
		require.Equal(t, uint64(3), proof.Receipts[0].ConfigurationIndex)
		require.Equal(t, uint64(9), proof.Receipts[0].AppliedIndex)
		olderEpoch := refreshed
		olderEpoch.ConfigurationIndex = 3
		require.ErrorIs(t, refresher.RefreshMigrationOwnership(t.Context(), olderEpoch, 13), ErrMigrationOwnership)
		require.ErrorIs(t, refresher.RefreshMigrationOwnership(t.Context(), refreshed, 11), ErrMigrationOwnership)
		require.NoError(t, writer.ClearMigrationStateWithOwnership(t.Context(), state, 13), "operation replay must not regress its newer attestation")
		for _, mutate := range []func(*MigrationOwnershipState){
			func(s *MigrationOwnershipState) { s.Generation = "different-native-generation" },
			func(s *MigrationOwnershipState) { s.Role, s.OwnerGroupID = MigrationOwner, 1 },
			func(s *MigrationOwnershipState) { s.MinWriteTSExclusive++ },
			func(s *MigrationOwnershipState) { s.RouteID++ },
			func(s *MigrationOwnershipState) { s.DescriptorHash = bytes.Repeat([]byte{2}, 32) },
		} {
			wrong := refreshed
			mutate(&wrong)
			require.ErrorIs(t, refresher.RefreshMigrationOwnership(t.Context(), wrong, 14), ErrMigrationOwnership)
		}
		snapshot, err := st.Snapshot()
		require.NoError(t, err)
		raw := snapshotBytes(t, snapshot)
		require.NoError(t, snapshot.Close())
		target, err := NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, target.Close()) })
		require.NoError(t, target.Restore(bytes.NewReader(raw)))
		restored, err := ownershipStore(t, target).MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Equal(t, proof, restored.Ownership[0])
	})
}

func TestMigrationOwnershipPromotedClearKeepsAcquireReceipt(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		writer, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), TargetStagedReadinessState{JobID: 8, MigrationJobID: 8,
			Armed: true, ExpectedCutoverVersion: 12, MinWriteTSExclusive: 200, RouteStart: []byte("m"), RouteEnd: []byte("z")}, 8))
		state := MigrationOwnershipState{Generation: "0123456789abcdef0123456789abcdef", JobID: 8,
			LocalGroupID: 1, OwnerGroupID: 1, Role: MigrationOwner, Staged: true, CatalogVersion: 12,
			ConfigurationIndex: 3, RouteID: 3, ParentRouteID: 1, DescriptorHash: bytes.Repeat([]byte{1}, 32),
			RouteStart: []byte("m"), RouteEnd: []byte("z"), MinWriteTSExclusive: 200}
		proofs := ownershipStore(t, st)
		require.NoError(t, proofs.AcquireMigrationOwnership(t.Context(), state, 9))
		cleared := state
		cleared.Staged, cleared.CatalogVersion, cleared.DescriptorHash = false, 13, bytes.Repeat([]byte{2}, 32)
		require.ErrorIs(t, proofs.ClearMigrationStateWithOwnership(t.Context(), cleared, 10), ErrMigrationOwnership)
		promoter, ok := st.(MigrationPromoter)
		require.True(t, ok)
		_, err := promoter.PromoteVersions(t.Context(), PromoteVersionsOptions{JobID: 8, AppliedIndex: 10,
			StartKey: []byte("staged/"), EndKey: []byte("staged0"), MaxVersions: 10, MaxBytes: 1024,
			TargetKey: func(key []byte) ([]byte, bool) { return key, true }})
		require.NoError(t, err)
		require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), cleared, 11))
		view, err := proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Empty(t, view.Readiness)
		require.Len(t, view.Ownership[0].Receipts, 2)
		require.Equal(t, uint64(12), view.Ownership[0].Receipts[0].CatalogVersion)
		require.Equal(t, uint64(13), view.Ownership[0].Receipts[1].CatalogVersion)
	})
}
