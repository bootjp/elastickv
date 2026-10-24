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
	*splitRuntimeClient
	export        bool
	exportStarted bool
	importStarted bool
}

func (s *stalledSplitCopyClient) ExportRangeVersions(ctx context.Context, req *pb.ExportRangeVersionsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
	if s.export {
		s.exportStarted = true
		return &stalledSplitCopyStream{ctx: ctx}, nil
	}
	return s.splitRuntimeClient.ExportRangeVersions(ctx, req, opts...)
}

func (s *stalledSplitCopyClient) ImportRangeVersions(ctx context.Context, _ *pb.ImportRangeVersionsRequest, _ ...grpc.CallOption) (*pb.ImportRangeVersionsResponse, error) {
	s.importStarted = true
	<-ctx.Done()
	return nil, ctx.Err()
}

type stalledSplitCopyStream struct {
	grpc.ClientStream
	ctx context.Context
}

type freshBudgetImportClient struct {
	*realImportTargetStub
	t *testing.T
}

func (s *freshBudgetImportClient) ImportRangeVersions(ctx context.Context, req *pb.ImportRangeVersionsRequest, opts ...grpc.CallOption) (*pb.ImportRangeVersionsResponse, error) {
	s.t.Helper()
	require.NoError(s.t, ctx.Err())
	deadline, ok := ctx.Deadline()
	require.True(s.t, ok)
	require.Greater(s.t, time.Until(deadline), defaultSplitMigrationAttemptTimeout/2)
	return s.realImportTargetStub.ImportRangeVersions(ctx, req, opts...)
}

type expiredExportBudgetStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *expiredExportBudgetStream) Recv() (*pb.ExportRangeVersionsResponse, error) {
	<-s.ctx.Done()
	return &pb.ExportRangeVersionsResponse{NextCursor: []byte("cursor"), Done: true}, nil
}

func TestSplitJobImportBudgetIsIndependentOfExport(t *testing.T) {
	t.Parallel()
	ctx, server, catalog, job, bracket, target := importReplayFixture(t)
	exportCtx, cancel := context.WithCancel(ctx)
	cancel()
	stream := &expiredExportBudgetStream{ctx: exportCtx}
	client := &freshBudgetImportClient{realImportTargetStub: target, t: t}
	require.NoError(t, server.copySplitJobStream(ctx, job, 0, bracket, job.BracketProgress[0], stream, client))
	persisted, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []byte("cursor"), persisted.BracketProgress[0].Cursor)
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
			catalog := distribution.NewCatalogStore(st, distribution.WithCatalogRouteDescriptorV2Writes(true))
			_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("z"), GroupID: 7}})
			require.NoError(t, err)
			job := splitJobForSourceGroupTest(t)
			job.Phase = distribution.SplitJobPhaseBackfill
			job.SnapshotTS = catalog.LatestCommitTS()
			require.NoError(t, catalog.CreateSplitJob(ctx, job))
			server, source, target := newSplitRuntimeRunner(t, st, catalog, job)
			_, complete, err := server.armSplitSourceOwnership(ctx, job, job.SourceGroupID, source)
			require.NoError(t, err)
			require.True(t, complete)
			sourceClient := &stalledSplitCopyClient{splitRuntimeClient: source, export: export}
			targetClient := &stalledSplitCopyClient{splitRuntimeClient: target}
			server.splitMigrationClientFactory = func(context.Context, distribution.SplitJob, uint64) (SplitMigrationClient, SplitMigrationClient, error) {
				return sourceClient, targetClient, nil
			}
			start := time.Now()
			err = server.copySplitJobPhase(ctx, job, distribution.SplitJobExportPhaseBackfill, 0, job.SnapshotTS)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, ctx.Err())
			require.Less(t, time.Since(start), 5*defaultSplitMigrationAttemptTimeout)
			requireRuntimeTransferGuard(t, source)
			require.Equal(t, export, sourceClient.exportStarted)
			require.Equal(t, !export, targetClient.importStarted)
			if !export {
				require.NotEmpty(t, source.exports, "the import timeout must follow a real source export")
			}
			persisted, found, err := catalog.SplitJob(ctx, job.JobID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, job.BracketProgress, persisted.BracketProgress, "failed attempts must retain resumable durable progress")
		})
	}
}
