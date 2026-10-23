package store

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOwnershipPartitionGuardedMoveBackAndClear(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		_, tx := partitionFixture(t, st)
		proofs := ownershipStore(t, st)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
		initial, err := proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		incoming := MigrationOwnershipState{Generation: tx.Generation, JobID: 8, LocalGroupID: 1, OwnerGroupID: 1,
			CatalogVersion: 14, ConfigurationIndex: 3, RouteID: 12, ParentRouteID: 10, RouteStart: []byte("m"), RouteEnd: []byte("z"),
			DescriptorHash: bytes.Repeat([]byte{3}, 32), Role: MigrationOwner, MinWriteTSExclusive: 200, WriteFloorKnown: true, SourceTrackerVersion: 14}
		guard := TargetStagedReadinessState{JobID: 8, MigrationJobID: 8, Armed: true, TrackWrites: true,
			RouteStart: incoming.RouteStart, RouteEnd: incoming.RouteEnd, ExpectedCutoverVersion: 14, MinWriteTSExclusive: 200, RetentionPinTS: 1}
		writer, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		require.ErrorIs(t, proofs.AcquireMigrationOwnership(t.Context(), incoming, 11), ErrMigrationOwnership)
		unguarded := incoming
		unguarded.SourceTrackerVersion = 0
		require.ErrorIs(t, proofs.ClearMigrationStateWithOwnership(t.Context(), unguarded, 11), ErrMigrationOwnership,
			"an unguarded ordinary Clear must not elevate a durable NONOWNER")
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 11))
		require.NoError(t, proofs.AcquireMigrationOwnership(t.Context(), incoming, 12))
		acquired, err := proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Len(t, acquired.Partitions.Partitions, 2)
		require.Equal(t, initial.Partitions.Partitions[0], acquired.Partitions.Partitions[0], "retained left is neither fenced nor raised")
		right := acquired.Partitions.Partitions[1]
		require.Equal(t, MigrationOwner, right.Role)
		require.Equal(t, uint64(8), right.MigrationJobID)
		require.Equal(t, uint64(12), right.SourceAppliedIndex)
		require.True(t, MigrationOwnershipPartitionEvidenceValid(acquired.Partitions, right))
		require.NoError(t, proofs.AcquireMigrationOwnership(t.Context(), incoming, 13))
		guard.SourceWriteFence, guard.SourceReadFence, guard.ExpectedCutoverVersion = true, true, 15
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 14))
		outgoing := incoming
		outgoing.Role, outgoing.OwnerGroupID, outgoing.CatalogVersion, outgoing.SourceTrackerVersion = MigrationNonOwner, 2, 15, 0
		outgoing.DescriptorHash = bytes.Repeat([]byte{9}, 32)
		require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), outgoing, 15))
		cleared, err := proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Empty(t, cleared.Readiness)
		require.Equal(t, MigrationNonOwner, cleared.Partitions.Partitions[1].Role)
		require.Zero(t, cleared.Partitions.Partitions[1].SourceTrackerVersion)
		require.Equal(t, initial.Partitions.Partitions[0], cleared.Partitions.Partitions[0])
		require.Len(t, cleared.Partitions.Receipts, 3)
		require.NoError(t, proofs.ClearMigrationStateWithOwnership(t.Context(), outgoing, 16))
		after, err := proofs.MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Equal(t, cleared, after, "retry must not restore the superseded SOURCE OWNER")
	})
}

func TestOwnershipPartitionPartialGuardCannotClipFrontier(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		_, tx := partitionFixture(t, st)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
		incoming := MigrationOwnershipState{Generation: tx.Generation, JobID: 8, LocalGroupID: 1, OwnerGroupID: 1,
			CatalogVersion: 14, ConfigurationIndex: 3, RouteID: 20, RouteStart: []byte("n"), RouteEnd: []byte("z"),
			DescriptorHash: bytes.Repeat([]byte{5}, 32), Role: MigrationOwner, MinWriteTSExclusive: 200, WriteFloorKnown: true, Staged: true}
		guard := TargetStagedReadinessState{JobID: 8, MigrationJobID: 8, Armed: true, RouteStart: incoming.RouteStart,
			RouteEnd: incoming.RouteEnd, ExpectedCutoverVersion: 14, MinWriteTSExclusive: 200}
		writer, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 11))
		before, err := ownershipStore(t, st).MigrationServingState(t.Context())
		require.NoError(t, err)
		require.ErrorIs(t, ownershipStore(t, st).AcquireMigrationOwnership(t.Context(), incoming, 12), ErrMigrationOwnership)
		after, err := ownershipStore(t, st).MigrationServingState(t.Context())
		require.NoError(t, err)
		require.Equal(t, before, after, "all CAS/union checks precede any ledger, guard or receipt mutation")
	})
}

func TestOwnershipPartitionMoveBackRequiresNewerActiveCatalog(t *testing.T) {
	runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
		_, tx := partitionFixture(t, st)
		require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
		incoming := MigrationOwnershipState{Generation: tx.Generation, JobID: 8, LocalGroupID: 1, OwnerGroupID: 1,
			CatalogVersion: tx.CatalogVersion, ConfigurationIndex: 3, RouteID: 12, ParentRouteID: 10,
			RouteStart: []byte("m"), RouteEnd: []byte("z"), DescriptorHash: bytes.Repeat([]byte{3}, 32),
			Role: MigrationOwner, MinWriteTSExclusive: 200, WriteFloorKnown: true, Staged: true}
		guard := TargetStagedReadinessState{JobID: 8, MigrationJobID: 8, Armed: true, RouteStart: incoming.RouteStart,
			RouteEnd: incoming.RouteEnd, ExpectedCutoverVersion: incoming.CatalogVersion, MinWriteTSExclusive: 200}
		writer, ok := st.(MigrationTargetReadinessRaftWriter)
		require.True(t, ok)
		require.NoError(t, writer.ApplyTargetStagedReadinessAt(t.Context(), guard, 11))
		before, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.ErrorIs(t, ownershipStore(t, st).AcquireMigrationOwnership(t.Context(), incoming, 12), ErrMigrationOwnership)
		after, err := admissionReader(t, st).MigrationServingAdmissionState(t.Context())
		require.NoError(t, err)
		require.Equal(t, before, after)
		incoming.CatalogVersion++
		require.NoError(t, ownershipStore(t, st).AcquireMigrationOwnership(t.Context(), incoming, 13))
	})
}

func TestOwnershipPartitionLegacyRefreshRequiresExactSingleFrontier(t *testing.T) {
	for _, split := range []bool{false, true} {
		t.Run(map[bool]string{false: "one", true: "descendants"}[split], func(t *testing.T) {
			runMigrationStoreSuite(t, func(t *testing.T, st MVCCStore) {
				old, tx := partitionFixture(t, st)
				if !split {
					tx.After = []MigrationOwnershipPartition{partitionFromLegacy(old)}
					finalizePartitionTransition(t, &tx)
				}
				require.NoError(t, partitionStore(t, st).ApplyMigrationOwnershipPartition(t.Context(), tx, 10))
				before, err := ownershipStore(t, st).MigrationServingState(t.Context())
				require.NoError(t, err)
				old.CatalogVersion = 13
				refresher, ok := st.(MigrationOwnershipRefresher)
				require.True(t, ok)
				err = refresher.RefreshMigrationOwnership(t.Context(), old, 11)
				after, readErr := ownershipStore(t, st).MigrationServingState(t.Context())
				require.NoError(t, readErr)
				if split {
					require.ErrorIs(t, err, ErrMigrationOwnership)
					require.Equal(t, before, after)
				} else {
					require.NoError(t, err)
					require.Equal(t, uint64(13), after.Partitions.Partitions[0].CatalogVersion)
					require.Equal(t, before.Partitions.Partitions[0].PartitionID, after.Partitions.Partitions[0].PartitionID)
					require.Equal(t, before.Partitions.Partitions[0].Origins, after.Partitions.Partitions[0].Origins)
					require.Len(t, after.Partitions.Receipts, 2)
					require.True(t, MigrationOwnershipPartitionEvidenceValid(after.Partitions, after.Partitions.Partitions[0]))
				}
			})
		})
	}
}
