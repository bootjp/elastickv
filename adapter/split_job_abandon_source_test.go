package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type abandonUnavailableTarget struct{ splitMigrationClientStub }

func (c *abandonUnavailableTarget) CleanupMigration(context.Context, *pb.CleanupMigrationRequest, ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	return nil, status.Error(codes.Unavailable, "target offline")
}

type abandonSourceClient struct {
	splitMigrationClientStub
	cleaner store.MigrationCleaner
}

func (c *abandonSourceClient) CleanupMigration(ctx context.Context, req *pb.CleanupMigrationRequest, _ ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	c.cleanups = append(c.cleanups, req)
	err := c.cleaner.ClearMigrationState(ctx, req.GetJobId(), 0)
	return &pb.CleanupMigrationResponse{Done: err == nil}, err
}

func TestSplitJobAbandonRestoresSourceBeforeUnavailableTarget(t *testing.T) {
	t.Parallel()
	for _, phase := range []distribution.SplitJobPhase{distribution.SplitJobPhaseFence, distribution.SplitJobPhaseDeltaCopy} {
		for _, barrierReady := range []bool{false, true} {
			t.Run(map[distribution.SplitJobPhase]string{distribution.SplitJobPhaseFence: "fence", distribution.SplitJobPhaseDeltaCopy: "delta"}[phase]+map[bool]string{false: "/pending-voter", true: "/ready"}[barrierReady], func(t *testing.T) {
				t.Parallel()
				ctx := context.Background()
				st := store.NewMVCCStore()
				catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
				parent := distribution.RouteDescriptor{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}
				job, err := distribution.InitializeSplitJobPlan(distribution.SplitJob{
					JobID: 7, SourceRouteID: 1, TargetGroupID: 9, SplitKey: []byte("m"),
				}, parent, time.Now().UnixMilli())
				require.NoError(t, err)
				job.Phase, job.AbandonFromPhase = distribution.SplitJobPhaseAbandoning, phase
				_, err = catalog.Save(ctx, 0, []distribution.RouteDescriptor{
					{RouteID: 2, ParentRouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 7},
					{RouteID: 3, ParentRouteID: 1, Start: []byte("m"), End: []byte("z"), GroupID: 7, State: distribution.RouteStateWriteFenced},
				})
				require.NoError(t, err)
				require.NoError(t, catalog.CreateSplitJob(ctx, job))
				sourceStore := store.NewMVCCStore()
				writer, ok := sourceStore.(store.MigrationTargetReadinessWriter)
				require.True(t, ok)
				require.NoError(t, writer.ApplyTargetStagedReadiness(ctx, store.TargetStagedReadinessState{
					JobID: job.JobID, MigrationJobID: job.JobID, Armed: true, SourceWriteFence: true,
					RouteStart: job.SplitKey, RouteEnd: []byte("z"), MinWriteTSExclusive: 1, RetentionPinTS: 1,
				}))
				source, _ := newOwnershipRuntimeClient(t, sourceStore, 7)
				source.blockGuard = !barrierReady
				source.beforeCleanup = func() {
					snapshot, loadErr := catalog.Snapshot(ctx)
					require.NoError(t, loadErr)
					require.Nil(t, abandonedSplitJobFencedRoute(snapshot.Routes, job), "actual rollback must precede atomic OWNER/guard replacement")
				}
				target := &abandonUnavailableTarget{}
				server := NewDistributionServer(distribution.NewEngine(), catalog,
					WithDistributionCoordinator(&ownershipCatalogCoordinator{distributionCoordinatorStub: newDistributionCoordinatorStub(st, true)}),
					WithSplitMigrationVoterFactory(func(context.Context, uint64) ([]SplitMigrationVoter, error) {
						return []SplitMigrationVoter{{ID: "source", Address: "source:1", Client: source}}, nil
					}),
					WithSplitMigrationGroupClientFactory(func(_ context.Context, groupID uint64) (SplitMigrationClient, error) {
						require.Equal(t, uint64(7), groupID)
						return source, nil
					}),
					WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
						require.NotEmpty(t, source.cleanups, "target lookup must follow source cleanup")
						return source, target, nil
					}),
				)
				for range 3 {
					_ = server.abandonSplitJob(ctx, job)
					var found bool
					job, found, err = catalog.SplitJob(ctx, job.JobID)
					require.NoError(t, err)
					require.True(t, found)
				}
				snapshot, err := catalog.Snapshot(ctx)
				require.NoError(t, err)
				reader, ok := sourceStore.(store.MigrationTargetReadinessReader)
				require.True(t, ok)
				states, err := reader.MigrationTargetReadinessStates(ctx)
				require.NoError(t, err)
				if barrierReady {
					require.Empty(t, states, "a proven source recovers without waiting for the target")
					require.Nil(t, abandonedSplitJobFencedRoute(snapshot.Routes, job))
					require.Equal(t, splitCleanupDoneCursor, job.SourceCutoverAckCursor)
					require.ErrorContains(t, server.abandonSplitJob(ctx, job), "target offline")
				} else {
					require.Len(t, states, 1, "an unproven voter must retain the source guard")
					require.NotNil(t, abandonedSplitJobFencedRoute(snapshot.Routes, job))
					require.Empty(t, job.SourceCutoverAckCursor)
				}
				require.Equal(t, distribution.SplitJobPhaseAbandoning, job.Phase)
			})
		}
	}
}
