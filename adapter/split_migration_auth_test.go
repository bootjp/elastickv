package adapter

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestSplitMigrationTokenAuthProtectsMutators(t *testing.T) {
	t.Parallel()
	methods := []string{
		pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName,
	}
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			unary, stream := SplitMigrationTokenAuth("admin-secret", false)
			for _, credential := range []string{"", "Bearer wrong", "Bearer admin-secret"} {
				ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", credential))
				called := false
				_, err := unary(ctx, nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) {
					called = true
					return nil, nil
				})
				want := codes.Unauthenticated
				if credential == "Bearer admin-secret" {
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

func TestSplitMigrationTokenAuthNoTokenRequiresExplicitOptIn(t *testing.T) {
	t.Parallel()
	unary, _ := SplitMigrationTokenAuth("", false)
	for _, method := range []string{
		pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName,
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
