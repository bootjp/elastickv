package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestSplitJobSourceCleanupReopensCompletedMembershipProofs(t *testing.T) {
	for _, dataDone := range []bool{false, true} {
		t.Run(map[bool]string{false: "before data deletion", true: "before guard release"}[dataDone], func(t *testing.T) {
			ctx := context.Background()
			st := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(st)
			_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
			require.NoError(t, err)
			job := sampleDistributionSplitJob(17)
			job.Phase = distribution.SplitJobPhaseCleanup
			job.SourceGroupID = 7
			job.TargetPromotionDone = true
			job.TargetClearedDescriptorAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			job.SourceCutoverAckCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			job.SourceReadDrainCursor = distribution.CloneBytes(splitCleanupDoneCursor)
			job.SourceRetentionPinTS = 90
			if dataDone {
				job.Cursor = distribution.CloneBytes(splitSourceCleanupDoneCursor)
			}
			require.NoError(t, catalog.CreateSplitJob(ctx, job))
			source := &splitMigrationClientStub{}
			n1 := &splitMigrationClientStub{}
			n2 := &splitMigrationClientStub{}
			voters := []SplitMigrationVoter{{ID: "n1", Address: "n1:50051", Client: n1}}
			server := NewDistributionServer(distribution.NewEngine(), catalog,
				WithDistributionCoordinator(newDistributionCoordinatorStub(st, true)),
				WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
					return source, &splitMigrationClientStub{}, nil
				}),
				WithSplitMigrationVoterFactory(func(context.Context, uint64) ([]SplitMigrationVoter, error) { return voters, nil }))
			complete, err := server.refreshSplitJobSourceCleanupBarriers(ctx, job)
			require.NoError(t, err)
			require.True(t, complete)
			job, _, err = catalog.SplitJob(ctx, job.JobID)
			require.NoError(t, err)
			acks, err := decodeSplitVoterAckCursor(job.SourceCutoverAckCursor)
			require.NoError(t, err)
			require.Len(t, acks, 1, "completion must retain voter identity, not a done sentinel")
			n2.probeFn = func(*pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
				return &pb.ProbeMigrationStateResponse{}, nil
			}
			voters = append(voters, SplitMigrationVoter{ID: "n2", Address: "n2:50051", Client: n2})
			require.NoError(t, server.cleanupVerifiedSplitJob(ctx, job))
			require.Empty(t, source.cleanups, "a new stale voter must block data deletion and guard removal")
			job, _, err = catalog.SplitJob(ctx, job.JobID)
			require.NoError(t, err)
			require.Equal(t, uint64(90), job.SourceRetentionPinTS)
			acks, err = decodeSplitVoterAckCursor(job.SourceCutoverAckCursor)
			require.NoError(t, err)
			require.Len(t, acks, 2)
			require.False(t, acks[1].acked)
			n2.probeFn = nil
			complete, err = server.refreshSplitJobSourceCleanupBarriers(ctx, job)
			require.NoError(t, err)
			require.True(t, complete)
		})
	}
}
