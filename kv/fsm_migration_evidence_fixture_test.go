package kv

import (
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func installStagedOwnershipEvidence(t *testing.T, f *kvFSM, version uint64, route distribution.RouteDescriptor) store.MigrationOwnershipState {
	t.Helper()
	require.NoError(t, f.RecordAppliedConfiguration(1))
	guard, err := MarshalTargetStagedReadinessCommand(&pb.TargetStagedReadinessRequest{
		JobId: route.MigrationJobID, MigrationJobId: route.MigrationJobID, Armed: true,
		RouteStart: route.Start, RouteEnd: route.End, ExpectedCutoverVersion: version,
		MinWriteTsExclusive: route.MinWriteTSExclusive,
	})
	require.NoError(t, err)
	f.SetApplyIndex(2)
	require.Nil(t, f.Apply(guard))
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, true)
	require.NoError(t, err)
	digest := sha256.Sum256(encoded)
	acquire, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
		JobId: route.MigrationJobID, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_ACQUIRE,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: 1,
		Ownership: &pb.MigrationOwnershipProof{
			Generation: "0123456789abcdef0123456789abcdef", LocalGroupId: f.shardGroupID, OwnerGroupId: route.GroupID,
			CatalogVersion: version, RouteId: route.RouteID, RouteStart: route.Start, RouteEnd: route.End,
			MinWriteTsExclusive: route.MinWriteTSExclusive, WriteFloorKnown: true, DescriptorHash: digest[:],
			Role: pb.MigrationOwnershipRole_MIGRATION_OWNERSHIP_ROLE_OWNER, Staged: true,
		},
	})
	require.NoError(t, err)
	f.SetApplyIndex(3)
	response := f.Apply(acquire)
	result, ok := response.(store.CleanupVersionsResult)
	require.True(t, ok, "ownership control returned %v", response)
	require.True(t, result.Done)
	reader, ok := f.store.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := reader.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Len(t, view.Readiness, 1)
	require.Len(t, view.Ownership, 1)
	proof := view.Ownership[0]
	require.True(t, store.MigrationOwnershipEvidenceValid(proof))
	require.True(t, migrationTargetOwnershipMatches(proof, view.Readiness[0]))
	require.Len(t, proof.Receipts, 1)
	require.Equal(t, store.MigrationOwnershipAcquire, proof.Receipts[0].Operation)
	require.Equal(t, uint64(1), proof.ConfigurationIndex)
	require.Equal(t, uint64(3), proof.AppliedIndex)
	require.Equal(t, digest[:], proof.Receipts[0].DescriptorHash)
	return proof
}

func TestStagedOwnershipEvidenceRequiresDescriptorIdentity(t *testing.T) {
	f := &kvFSM{store: store.NewMVCCStore(), shardGroupID: 1}
	proof := installStagedOwnershipEvidence(t, f, 7, applyRouteContextDescriptor())
	for _, field := range []string{"generation", "route ID", "descriptor hash", "receipt hash", "receipt"} {
		t.Run(field, func(t *testing.T) {
			incomplete := proof
			incomplete.Receipts = append([]store.MigrationOwnershipReceipt(nil), proof.Receipts...)
			switch field {
			case "generation":
				incomplete.Generation = ""
			case "route ID":
				incomplete.RouteID = 0
			case "descriptor hash":
				incomplete.DescriptorHash = nil
			case "receipt hash":
				incomplete.Receipts[0].DescriptorHash = nil
			case "receipt":
				incomplete.Receipts = nil
			}
			require.False(t, store.MigrationOwnershipEvidenceValid(incomplete), field)
		})
	}
}
