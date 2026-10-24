package adapter

import (
	"context"
	"io"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	internalutil "github.com/bootjp/elastickv/internal"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestMigrationExportOversizeHasExplicitDetail(t *testing.T) {
	resp := &pb.ExportRangeVersionsResponse{Versions: []*pb.MVCCVersion{{
		Key: []byte("n"), Value: make([]byte, store.MigrationPagePayloadBudget+1), CommitTs: 17,
	}}}
	err := checkMigrationExportPageSize(resp)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	details := status.Convert(err).Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, "elastickv.migration.transfer", info.GetDomain())
	require.Equal(t, "OVERSIZED_MVCC_VERSION", info.GetReason())
}

func TestMigrationTransferBoundaryFitsExportAndImportRequests(t *testing.T) {
	key := make([]byte, store.MaxSnapshotKeySize)
	value := make([]byte, store.MigrationPagePayloadBudget-2*len(key)-128)
	version := &pb.MVCCVersion{Key: key, Value: value, CommitTs: ^uint64(0), KeyFamily: distribution.MigrationFamilyUser}
	export := &pb.ExportRangeVersionsResponse{Versions: []*pb.MVCCVersion{version}, NextCursor: append(make([]byte, 12), key...)}
	require.True(t, store.MigrationVersionFitsTransfer(key, value))
	require.NoError(t, checkMigrationExportPageSize(export))
	require.Less(t, proto.Size(export), internalutil.GRPCMaxMessageBytes)
	request := &pb.ImportRangeVersionsRequest{JobId: ^uint64(0), Versions: export.Versions, Cursor: export.NextCursor, BracketId: ^uint64(0), BatchSeq: ^uint64(0)}
	require.Less(t, proto.Size(request), internalutil.GRPCMaxMessageBytes)
	encoded, err := proto.Marshal(request)
	require.NoError(t, err)
	require.Len(t, encoded, proto.Size(request))
	version.Value = append(version.Value, 0)
	require.Equal(t, codes.FailedPrecondition, status.Code(checkMigrationExportPageSize(export)))
}

func TestMigrationTransferErrorDetailsAreNotGenericBackpressure(t *testing.T) {
	typed := migrationOversizedVersionError(&pb.MVCCVersion{Key: []byte("n"), CommitTs: 17})
	wire, err := proto.Marshal(status.Convert(typed).Proto())
	require.NoError(t, err)
	decoded := status.New(codes.OK, "").Proto()
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.True(t, errors.Is(migrationSourceExportError(status.FromProto(decoded).Err()), errMigrationOversizedSourceVersion))
	wrong, err := status.New(codes.FailedPrecondition, "oversized").WithDetails(&errdetails.ErrorInfo{Domain: "other", Reason: migrationOversizedVersionReason})
	require.NoError(t, err)
	for _, candidate := range []error{status.Error(codes.ResourceExhausted, "disk full"), status.Error(codes.FailedPrecondition, "migration export page exceeds limit"), wrong.Err(), context.Canceled} {
		require.False(t, errors.Is(migrationSourceExportError(candidate), errMigrationOversizedSourceVersion))
	}
}

type transferFailureStream struct {
	grpc.ClientStream
	first *pb.ExportRangeVersionsResponse
	err   error
}

func (s *transferFailureStream) Recv() (*pb.ExportRangeVersionsResponse, error) {
	if s.first != nil {
		first := s.first
		s.first = nil
		return first, nil
	}
	return nil, s.err
}

func TestMigrationTypedSourceFailureAfterAcknowledgedPageAutoAbandons(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	job.Phase = distribution.SplitJobPhaseBackfill
	require.NoError(t, server.updateSplitJobViaCoordinator(ctx, job.JobID, func(distribution.SplitJob) (distribution.SplitJob, error) { return job, nil }))
	source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		return &transferFailureStream{first: &pb.ExportRangeVersionsResponse{Versions: []*pb.MVCCVersion{{Key: []byte("n"), Value: []byte("v"), CommitTs: 5}}, NextCursor: []byte("acked")}, err: migrationOversizedVersionError(&pb.MVCCVersion{Key: []byte("o"), CommitTs: 7})}, nil
	}
	err := server.runSplitJobAttempt(ctx, job)
	require.True(t, errors.Is(err, errMigrationOversizedSourceVersion))
	current := loadTransferJob(t, ctx, catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseAbandoning, current.Phase)
	require.Equal(t, []byte("acked"), current.Cursor, "retain current durable copy progress, not the stale attempt's cursor")
	require.Contains(t, current.LastError, "transfer budget")
}

type transferTargetFailure struct {
	splitMigrationClientStub
	err error
}

func (c *transferTargetFailure) ImportRangeVersions(context.Context, *pb.ImportRangeVersionsRequest, ...grpc.CallOption) (*pb.ImportRangeVersionsResponse, error) {
	return nil, c.err
}

func TestMigrationTargetFailureCannotAuthorizeAbandon(t *testing.T) {
	ctx, server, _, job, _, _ := importReplayFixture(t)
	err := migrationOversizedVersionError(&pb.MVCCVersion{Key: []byte("n")})
	target := &transferTargetFailure{err: err}
	stream := &transferFailureStream{first: &pb.ExportRangeVersionsResponse{Versions: []*pb.MVCCVersion{{Key: []byte("n"), CommitTs: 5}}, Done: true}, err: io.EOF}
	copyErr := server.copySplitJobStream(ctx, job, 0, distribution.MigrationBracket{}, job.BracketProgress[0], stream, target)
	require.Error(t, copyErr)
	require.False(t, errors.Is(copyErr, errMigrationOversizedSourceVersion))
}

func TestMigrationLegacySourceErrorKeepsExplicitFailure(t *testing.T) {
	ctx, server, catalog, job, source := transferPreflightFixture(t)
	require.NoError(t, server.finalizeSplitJobFence(ctx, job))
	job = loadTransferJob(t, ctx, catalog, job.JobID)
	source.exportFn = func(context.Context, *pb.ExportRangeVersionsRequest) (grpc.ServerStreamingClient[pb.ExportRangeVersionsResponse], error) {
		return nil, status.Error(codes.FailedPrecondition, "migration export page exceeds limit")
	}
	for range maxSplitJobPhaseFailures {
		err := server.runSplitJobAttempt(ctx, job)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.False(t, errors.Is(err, errMigrationOversizedSourceVersion))
	}
	failed := loadTransferJob(t, ctx, catalog, job.JobID)
	require.Equal(t, distribution.SplitJobPhaseFailed, failed.Phase)
	require.Equal(t, distribution.SplitJobPhaseFence, failed.RetryPhase)
	require.Equal(t, distribution.SplitJobPhaseNone, failed.AbandonFromPhase)
	require.Contains(t, failed.LastError, "migration export page exceeds limit")
}
