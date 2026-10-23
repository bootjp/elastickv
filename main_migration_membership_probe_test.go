package main

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type migrationMemberProbeServer struct {
	pb.UnimplementedInternalServer
	response *pb.ProbeMigrationStateResponse
	err      error
}

func (s migrationMemberProbeServer) ProbeMigrationState(_ context.Context, req *pb.ProbeMigrationStateRequest) (*pb.ProbeMigrationStateResponse, error) {
	if req.GetJobId() == 0 || req.GetKind() != pb.MigrationStateProbeKind_MIGRATION_STATE_PROBE_KIND_OWNERSHIP_CAPABILITY || req.GetRequireConfigurationIndex() {
		return nil, status.Error(codes.InvalidArgument, "expected static capability query")
	}
	return s.response, s.err
}

func TestMigrationMemberProtocolProbeAcceptsFreshLearnerNotLegacyEcho(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name      string
		transfer  uint32
		partition uint32
		want      codes.Code
	}{
		{name: "fresh-learner", transfer: 1, partition: 1, want: codes.OK},
		{name: "legacy-response", want: codes.FailedPrecondition},
		{name: "size-only", transfer: 1, want: codes.FailedPrecondition},
		{name: "partition-only", partition: 1, want: codes.FailedPrecondition},
		{name: "unknown-size-version", transfer: 2, partition: 1, want: codes.FailedPrecondition},
		{name: "unknown-partition-version", transfer: 1, partition: 2, want: codes.FailedPrecondition},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			address := grpctest.Serve(t, func(server *grpc.Server) {
				pb.RegisterInternalServer(server, migrationMemberProbeServer{response: &pb.ProbeMigrationStateResponse{
					TransferSizeGuardVersion: test.transfer, OwnershipPartitionProtocol: test.partition,
				}})
			})
			probe := newMigrationMemberProtocolProbe("")
			err := probe(t.Context(), address)
			require.Equal(t, test.want, status.Code(err), "%v", err)
		})
	}
}

func TestMigrationMemberProtocolProbeDoesNotBypassMissingRPCOrCancellation(t *testing.T) {
	t.Parallel()
	address := grpctest.Serve(t, func(server *grpc.Server) {
		pb.RegisterInternalServer(server, &pb.UnimplementedInternalServer{})
	})
	probe := newMigrationMemberProtocolProbe("")
	require.Equal(t, codes.Unimplemented, status.Code(probe(t.Context(), address)))
	require.Equal(t, codes.FailedPrecondition, status.Code(probe(t.Context(), " ")))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	started := time.Now()
	require.Equal(t, codes.Canceled, status.Code(probe(ctx, address)))
	require.Less(t, time.Since(started), migrationMemberProbeTimeout)
}

func TestMigrationMemberProtocolProbeUsesDedicatedTLSCredential(t *testing.T) {
	cert, key := writeSelfSignedCert(t)
	require.NoError(t, internalutil.ConfigureGRPCTLS(cert, cert, key))
	t.Cleanup(func() { require.NoError(t, internalutil.ConfigureGRPCTLS("", "", "")) })
	auth, _ := adapter.SplitMigrationTokenAuth("migration-secret", false, true)
	address := grpctest.Serve(t, func(server *grpc.Server) {
		pb.RegisterInternalServer(server, migrationMemberProbeServer{response: &pb.ProbeMigrationStateResponse{
			TransferSizeGuardVersion: 1, OwnershipPartitionProtocol: 1,
		}})
	}, grpc.ChainUnaryInterceptor(auth, func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		require.Equal(t, []string{"Bearer migration-secret"}, md.Get("authorization"))
		require.Equal(t, []string{"preserved"}, md.Get("trace-id"))
		return handler(ctx, req)
	}))
	ctx := metadata.NewOutgoingContext(t.Context(), metadata.Pairs("authorization", "Bearer stale", "trace-id", "preserved"))
	require.NoError(t, newMigrationMemberProtocolProbe("migration-secret")(ctx, address))
	require.Equal(t, codes.Unauthenticated, status.Code(newMigrationMemberProtocolProbe("wrong-token")(ctx, address)))
	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{"Bearer stale"}, md.Get("authorization"))
}

func TestMigrationMemberProtocolProbeRefusesCredentialOnPlaintext(t *testing.T) {
	t.Parallel()
	address := grpctest.Serve(t, func(server *grpc.Server) {
		pb.RegisterInternalServer(server, migrationMemberProbeServer{response: &pb.ProbeMigrationStateResponse{
			TransferSizeGuardVersion: 1, OwnershipPartitionProtocol: 1,
		}})
	})
	require.Error(t, newMigrationMemberProtocolProbe("migration-secret")(t.Context(), address))
}
