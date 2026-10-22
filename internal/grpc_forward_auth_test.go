package internal

import (
	"context"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestGRPCForwardTokenOnlyAuthenticatesPeerForward(t *testing.T) {
	ConfigureGRPCForwardToken("peer-secret")
	t.Cleanup(func() { ConfigureGRPCForwardToken("") })
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs("authorization", "Bearer stale", "trace-id", "keep"))
	for _, method := range []string{pb.Internal_Forward_FullMethodName, pb.Internal_ForwardLeaseRead_FullMethodName, pb.Admin_GetNodeVersion_FullMethodName} {
		called := false
		err := forwardTokenClientInterceptor(ctx, method, nil, nil, nil, func(callCtx context.Context, _ string, _, _ any, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
			called = true
			md, _ := metadata.FromOutgoingContext(callCtx)
			require.Equal(t, []string{"keep"}, md.Get("trace-id"))
			if method == pb.Internal_Forward_FullMethodName {
				require.Empty(t, md.Get("authorization"))
				require.Len(t, opts, 1)
				option, ok := opts[0].(grpc.PerRPCCredsCallOption)
				require.True(t, ok)
				require.True(t, option.Creds.RequireTransportSecurity())
				token, tokenErr := option.Creds.GetRequestMetadata(callCtx)
				require.NoError(t, tokenErr)
				require.Equal(t, "Bearer peer-secret", token["authorization"])
			} else {
				require.Equal(t, []string{"Bearer stale"}, md.Get("authorization"))
				require.Empty(t, opts)
			}
			return nil
		})
		require.NoError(t, err)
		require.True(t, called)
	}
	md, _ := metadata.FromOutgoingContext(ctx)
	require.Equal(t, []string{"Bearer stale"}, md.Get("authorization"))
}
