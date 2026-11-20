package adapter

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestPreflightRouteChangeWithoutVersionInvalidatesBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, server, catalog, job, source := transferPreflightFixture(t)
		require.NoError(t, server.runSplitJobPhase(ctx, job))
		job = loadTransferJob(t, ctx, catalog, job.JobID)
		snapshot, route, err := server.splitJobSourceRoute(ctx, job)
		require.NoError(t, err)
		coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
		require.True(t, ok)
		st := coordinator.store
		calls := 0
		source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
			calls++
			if calls == 2 {
				// Change the route key alone before the checkpoint takes its
				// OCC snapshot. A read set can detect later changes only.
				route.GroupID++
				raw, err := distribution.EncodeRouteDescriptor(route)
				require.NoError(t, err)
				require.NoError(t, st.PutAt(ctx, distribution.CatalogRouteKey(route.RouteID), raw, st.LastCommitTS()+1, 0))
			}
			return &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Done: true}}}, nil
		}
		require.NoError(t, server.runSplitJobPhase(ctx, job))
		require.Equal(t, 2, calls)
		current, err := catalog.Snapshot(ctx)
		require.NoError(t, err)
		require.Equal(t, snapshot.Version, current.Version)
		require.True(t, distribution.SplitJobsEquivalent(job, loadTransferJob(t, ctx, catalog, job.JobID)))
	})
}
