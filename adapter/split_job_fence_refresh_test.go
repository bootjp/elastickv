package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fenceDrainCommitClient struct {
	splitMigrationClientStub
	drained bool
}

func (s *fenceDrainCommitClient) ApplyTargetStagedReadiness(_ context.Context, _ *pb.TargetStagedReadinessRequest, _ ...grpc.CallOption) (*pb.TargetStagedReadinessResponse, error) {
	minimum := uint64(50)
	if s.drained {
		minimum = 5
	}
	return &pb.TargetStagedReadinessResponse{MinAdmittedTs: minimum}, nil
}

func (s *fenceDrainCommitClient) ProbeMigrationLocks(context.Context, *pb.ProbeMigrationLocksRequest, ...grpc.CallOption) (*pb.ProbeMigrationLocksResponse, error) {
	s.drained = true
	return &pb.ProbeMigrationLocksResponse{}, nil
}

func TestSplitJobFenceRefreshesMinimumAfterDrainingLocks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
	require.NoError(t, err)
	client := &fenceDrainCommitClient{}
	server := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(newDistributionCoordinatorStub(st, true)), WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
		return client, client, nil
	}))
	job := splitJobForSourceGroupTest(t)
	job.SnapshotTS = 40
	job.Phase = distribution.SplitJobPhaseFence
	require.NoError(t, catalog.CreateSplitJob(ctx, job))
	require.NoError(t, server.finalizeSplitJobFence(ctx, job))
	persisted, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, client.drained)
	require.Equal(t, distribution.SplitJobPhaseDeltaCopy, persisted.Phase)
	require.True(t, persisted.PostFenceDrainCompleted)
	require.Equal(t, uint64(5), persisted.SnapshotMinAdmittedTS)
	require.Equal(t, uint64(4), persisted.DeltaFloor, "the late commit must be included in the durable delta-copy window")
}
