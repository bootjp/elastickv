package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestSplitSourceOwnershipCommitsActualZeroFloorAndDataBoundary(t *testing.T) {
	root := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(root, distribution.WithCatalogRouteDescriptorV2Writes(true))
	snapshot, err := catalog.Save(t.Context(), 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
	require.NoError(t, err)
	job := sampleDistributionSplitJob(11)
	job.Phase, job.SourceRouteID, job.SourceGroupID, job.TargetGroupID, job.SplitKey = distribution.SplitJobPhasePlanned, 1, 7, 9, []byte("m")
	local := store.NewMVCCStore()
	source, _ := newOwnershipRuntimeClient(t, local, 7)
	server := NewDistributionServer(distribution.NewEngine(), catalog,
		WithDistributionCoordinator(&ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(root, true)}),
		WithSplitMigrationVoterFactory(func(context.Context, uint64) ([]SplitMigrationVoter, error) {
			return []SplitMigrationVoter{{ID: "source", Address: "source:1", Client: source}}, nil
		}))
	_, complete, err := server.armSplitSourceOwnership(t.Context(), job, 7, source)
	require.NoError(t, err)
	require.True(t, complete)
	proofs, ok := local.(store.MigrationOwnershipStore)
	require.True(t, ok)
	view, err := proofs.MigrationServingState(t.Context())
	require.NoError(t, err)
	proof := view.Ownership[0]
	require.Equal(t, snapshot.Version, proof.CatalogVersion, "never the predicted next catalog version")
	require.True(t, proof.WriteFloorKnown)
	require.Zero(t, proof.MinWriteTSExclusive)
	require.Positive(t, proof.SourceAppliedIndex)
	require.Equal(t, uint64(9), proof.ConfigurationIndex)
	require.True(t, store.MigrationOwnershipEvidenceValid(proof))
	tx := kv.NewTransactionWithProposer(source.internal.migrationProposer, kv.WithMigrationRequestAdmission(local, 7))
	t.Cleanup(tx.Close)
	_, err = tx.Commit(t.Context(), []*pb.Request{{Ts: 50, ObservedRouteVersion: kv.EncodeObservedRouteVersion(snapshot.Version),
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("n"), Value: []byte("tracked")}}}})
	require.NoError(t, err)
	view, err = proofs.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(50), view.Readiness[0].MinAdmittedTS)
	_, err = catalog.Save(t.Context(), snapshot.Version, append(snapshot.Routes,
		distribution.RouteDescriptor{RouteID: 2, Start: []byte("z"), GroupID: 9}))
	require.NoError(t, err)
	_, complete, err = server.armSplitSourceOwnership(t.Context(), job, 7, source)
	require.NoError(t, err)
	require.True(t, complete)
	view, err = proofs.MigrationServingState(t.Context())
	require.NoError(t, err)
	require.Equal(t, snapshot.Version+1, view.Ownership[0].CatalogVersion)
	require.Equal(t, proof.SourceAppliedIndex, view.Ownership[0].SourceAppliedIndex)
	require.Equal(t, proof.Receipts, view.Ownership[0].Receipts)
}
