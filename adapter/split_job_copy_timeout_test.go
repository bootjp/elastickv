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
)

type stalledSplitCopyClient struct {
	splitMigrationClientStub
	export bool
}

func (s *stalledSplitCopyClient) ExportRangeVersions(ctx context.Context, req *pb.ExportRangeVersionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
	if s.export {
		return &stalledSplitCopyStream{ctx: ctx}, nil
	}
	return s.splitMigrationClientStub.ExportRangeVersions(ctx, req, opts...)
}

func (s *stalledSplitCopyClient) ImportRangeVersions(ctx context.Context, _ *pb.ImportRangeVersionsRequest, _ ...grpc.CallOption) (*pb.ImportRangeVersionsResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

type stalledSplitCopyStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *stalledSplitCopyStream) Recv() (*pb.ExportRangeVersionsResponse, error) {
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func TestSplitJobCopyAttemptBoundsStalledExportAndImport(t *testing.T) {
	t.Parallel()
	for _, export := range []bool{true, false} {
		t.Run(map[bool]string{true: "export stream", false: "import call"}[export], func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			st := store.NewMVCCStore()
			catalog := distribution.NewCatalogStore(st)
			_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
			require.NoError(t, err)
			job := splitJobForSourceGroupTest(t)
			job.Phase = distribution.SplitJobPhaseBackfill
			job.SnapshotTS = catalog.LatestCommitTS()
			require.NoError(t, catalog.CreateSplitJob(ctx, job))
			client := &stalledSplitCopyClient{export: export}
			server := NewDistributionServer(distribution.NewEngine(), catalog, WithSplitMigrationClientFactory(func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
				return client, client, nil
			}))
			start := time.Now()
			err = server.copySplitJobPhase(ctx, job, distribution.SplitJobExportPhaseBackfill, 0, job.SnapshotTS)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, ctx.Err())
			require.Less(t, time.Since(start), 5*defaultSplitMigrationCopyTimeout)
			persisted, found, err := catalog.SplitJob(ctx, job.JobID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, job.BracketProgress, persisted.BracketProgress, "failed attempts must retain resumable durable progress")
		})
	}
}
