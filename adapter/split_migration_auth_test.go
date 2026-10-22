package adapter

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestSplitMigrationTokenAuthProtectsControlAndExport(t *testing.T) {
	t.Parallel()
	methods := []string{
		pb.Internal_Forward_FullMethodName,
		pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName,
		pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName,
		pb.Internal_CleanupMigration_FullMethodName,
		pb.Internal_ExportRangeVersions_FullMethodName,
		pb.Internal_ProbeMigrationState_FullMethodName,
		pb.Internal_ProbeMigrationLocks_FullMethodName,
		pb.Internal_IssueMigrationTimestamp_FullMethodName,
	}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			unary, stream := SplitMigrationTokenAuth("migration-secret", false)
			for _, credential := range []string{"", "Bearer wrong", "Bearer migration-secret"} {
				ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", credential))
				ctx = peer.NewContext(ctx, &peer.Peer{AuthInfo: credentials.TLSInfo{}})
				called := false
				_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
					called = true
					return nil, nil
				})
				want := codes.Unauthenticated
				if credential == "Bearer migration-secret" {
					want = codes.OK
				}
				require.Equal(t, want, status.Code(err))
				require.Equal(t, want == codes.OK, called)
				called = false
				err = stream(nil, &adminAuthTestServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: method}, func(any, grpc.ServerStream) error {
					called = true
					return nil
				})
				require.Equal(t, want, status.Code(err))
				require.Equal(t, want == codes.OK, called)
			}
		})
	}
}

func TestSplitMigrationTokenAuthRejectsPlaintextEvenWithValidToken(t *testing.T) {
	t.Parallel()
	unary, stream := SplitMigrationTokenAuth("migration-secret", false)
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer migration-secret"))
	_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: pb.Internal_CleanupMigration_FullMethodName}, func(context.Context, any) (any, error) {
		t.Fatal("plaintext token must not reach the handler")
		return nil, nil
	})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.ErrorContains(t, err, "requires TLS")
	err = stream(nil, &adminAuthTestServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: pb.Internal_ExportRangeVersions_FullMethodName}, func(any, grpc.ServerStream) error {
		t.Fatal("plaintext export must not reach the handler")
		return nil
	})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.ErrorContains(t, err, "requires TLS")
}

func TestSplitMigrationTokenAuthNoTokenRequiresExplicitOptIn(t *testing.T) {
	t.Parallel()
	unary, _ := SplitMigrationTokenAuth("", false)
	for _, method := range []string{
		pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName,
		pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName,
		pb.Internal_CleanupMigration_FullMethodName,
		pb.Internal_ExportRangeVersions_FullMethodName,
		pb.Internal_ProbeMigrationState_FullMethodName,
		pb.Internal_ProbeMigrationLocks_FullMethodName,
		pb.Internal_IssueMigrationTimestamp_FullMethodName,
	} {
		_, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
			t.Fatal("unauthenticated migration must not reach the handler")
			return nil, nil
		})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}
	resp, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: pb.Distribution_ListRoutes_FullMethodName}, func(context.Context, any) (any, error) {
		return "routes", nil
	})
	require.NoError(t, err)
	require.Equal(t, "routes", resp)
	insecureUnary, insecureStream := SplitMigrationTokenAuth("", true)
	require.Nil(t, insecureUnary)
	require.Nil(t, insecureStream)
}

func TestSplitMigrationTokenAuthLeavesNormalPeerRPCsUnchanged(t *testing.T) {
	t.Parallel()
	for _, token := range []string{"", "migration-secret"} {
		unary, stream := SplitMigrationTokenAuth(token, false)
		for _, method := range []string{
			pb.Internal_Forward_FullMethodName,
			pb.Distribution_ListRoutes_FullMethodName,
			pb.Distribution_GetSplitMigrationCapability_FullMethodName,
		} {
			if token != "" && method == pb.Internal_Forward_FullMethodName {
				continue
			}
			ctx := context.Background()
			called := false
			_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
				called = true
				return nil, nil
			})
			require.NoError(t, err)
			require.True(t, called)
			called = false
			err = stream(nil, &adminAuthTestServerStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: method}, func(any, grpc.ServerStream) error {
				called = true
				return nil
			})
			require.NoError(t, err)
			require.True(t, called)
		}
	}
}
