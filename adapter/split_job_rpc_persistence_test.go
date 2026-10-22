package adapter

import (
	"context"
	"testing"
	"time"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type lateImportAckClient struct {
	*realImportTargetStub
}

func (s *lateImportAckClient) ImportRangeVersions(ctx context.Context, req *pb.ImportRangeVersionsRequest, opts ...grpc.CallOption) (*pb.ImportRangeVersionsResponse, error) {
	resp, err := s.realImportTargetStub.ImportRangeVersions(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	<-ctx.Done()
	return resp, nil
}

func TestSplitJobCopyPersistsLateAndDuplicateAcknowledgements(t *testing.T) {
	t.Parallel()
	for _, duplicate := range []bool{false, true} {
		t.Run(map[bool]string{false: "new ack", true: "duplicate ack"}[duplicate], func(t *testing.T) {
			t.Parallel()
			ctx, server, catalog, job, bracket, target := importReplayFixture(t)
			versions := []*pb.MVCCVersion{{Key: []byte("n"), CommitTs: 10, Value: []byte("v")}}
			cursor := []byte("new-cursor")
			if duplicate {
				cursor = []byte("accepted-cursor")
				_, err := target.ImportRangeVersions(ctx, &pb.ImportRangeVersionsRequest{JobId: job.JobID, BracketId: bracket.BracketID, BatchSeq: 1, Cursor: cursor, Versions: versions})
				require.NoError(t, err)
			}
			rpcCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancel()
			stream := &splitMigrationStreamStub{responses: []*pb.ExportRangeVersionsResponse{{Versions: versions, NextCursor: []byte("new-cursor"), Done: true}}}
			require.NoError(t, server.copySplitJobStream(ctx, rpcCtx, job, 0, bracket, job.BracketProgress[0], stream, &lateImportAckClient{realImportTargetStub: target}))
			require.ErrorIs(t, rpcCtx.Err(), context.DeadlineExceeded)
			require.NoError(t, ctx.Err())
			persisted, found, err := catalog.SplitJob(ctx, job.JobID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, cursor, persisted.BracketProgress[0].Cursor)
			require.Equal(t, uint64(1), persisted.BracketProgress[0].LastAckedBatchSeq)
		})
	}
}

type stalledCleanupClient struct {
	splitMigrationClientStub
}

func (*stalledCleanupClient) CleanupMigration(ctx context.Context, _ *pb.CleanupMigrationRequest, _ ...grpc.CallOption) (*pb.CleanupMigrationResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSplitJobCleanupAttemptsHaveIndependentDeadlines(t *testing.T) {
	t.Parallel()
	for _, mode := range []pb.MigrationCleanupMode{pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS, pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_METADATA} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			_, err := cleanupSplitMigration(ctx, &stalledCleanupClient{}, &pb.CleanupMigrationRequest{JobId: 7, Mode: mode})
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NoError(t, ctx.Err())
		})
	}
}
