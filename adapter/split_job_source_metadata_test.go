package adapter

import (
	"context"
	"math"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func testSourceMetadataOwnershipBarrier(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	base := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(base, distribution.WithCatalogRouteDescriptorV2Writes(true))
	routes := promotionCompleteDistributionRoutes()
	routes[1].StagedVisibilityActive, routes[1].MigrationJobID = false, 0
	snapshot, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	job := promotionCompleteDistributionJob()
	job.TargetPromotionDone, job.SourceRetentionPinTS = true, 90
	job.TargetClearedDescriptorAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
	job.Cursor = distribution.CloneBytes(splitSourceCleanupDoneCursor)
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	source, _ := newOwnershipRuntimeClient(t, store.NewMVCCStore(), job.SourceGroupID)
	follower, _ := newOwnershipRuntimeClient(t, store.NewMVCCStore(), job.SourceGroupID)
	primaryApply, ok := source.internal.migrationProposer.(*ownershipRuntimeProposer)
	require.True(t, ok)
	followerApply, ok := follower.internal.migrationProposer.(*ownershipRuntimeProposer)
	require.True(t, ok)
	primaryApply.followers = append(primaryApply.followers, followerApply.fsm)
	for _, voter := range []*ownershipRuntimeClient{source, follower} {
		engine := distribution.NewEngine()
		require.NoError(t, engine.ApplySnapshot(snapshot))
		voter.internal.routeEngine = engine
	}
	_, err = source.ApplyTargetStagedReadiness(ctx, &pb.TargetStagedReadinessRequest{JobId: job.JobID, MigrationJobId: job.JobID,
		Armed: true, SourceWriteFence: true, SourceReadFence: true, RouteStart: job.SplitKey,
		ExpectedCutoverVersion: snapshot.Version, MinWriteTsExclusive: job.FenceTS, RetentionPinTs: 1})
	require.NoError(t, err)
	follower.blockMetadata = true
	server := NewDistributionServer(distribution.NewEngine(), catalog,
		WithDistributionCoordinator(&ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(base, true)}),
		WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
			return source, &splitMigrationClientStub{}, nil
		}),
		WithSplitMigrationVoterFactory(func(context.Context, uint64) ([]SplitMigrationVoter, error) {
			return []SplitMigrationVoter{{ID: "source", Address: "source:1", Client: source}, {ID: "follower", Address: "follower:1", Client: follower}}, nil
		}))
	require.ErrorIs(t, server.cleanupSplitJobSourceProofs(ctx, job), errSplitMigrationVoterBarrierIncomplete)
	require.Len(t, source.cleanups, 1)
	require.Equal(t, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_OWNERSHIP_CLEAR, source.cleanups[0].GetMode())
	current, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(90), current.SourceRetentionPinTS, "no job pin release until every metadata ACK")
	follower.blockMetadata = false
	require.NoError(t, server.cleanupSplitJobSourceProofs(ctx, current))
	current, found, err = catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(math.MaxUint64), current.SourceRetentionPinTS)
}
