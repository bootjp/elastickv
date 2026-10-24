package internal

import (
	"context"
	"sync/atomic"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

var grpcForwardToken atomic.Pointer[string]

// ConfigureGRPCForwardToken configures node-to-node Forward calls in clusters
// with authenticated migration enabled. Other RPC credentials are unchanged.
func ConfigureGRPCForwardToken(token string) {
	if token == "" {
		grpcForwardToken.Store(nil)
		return
	}
	grpcForwardToken.Store(&token)
}

func forwardTokenClientInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if token := grpcForwardToken.Load(); token != nil && method == pb.Internal_Forward_FullMethodName {
		md, _ := metadata.FromOutgoingContext(ctx)
		md = md.Copy()
		md.Delete("authorization")
		ctx = metadata.NewOutgoingContext(ctx, md)
		opts = append(opts, grpc.PerRPCCredentials(GRPCBearerToken(*token)))
	}
	return errors.WithStack(invoker(ctx, method, req, reply, cc, opts...))
}
