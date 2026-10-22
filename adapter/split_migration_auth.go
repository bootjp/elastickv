package adapter

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func splitMigrationMutatingMethod(method string) bool {
	switch method {
	case pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName:
		return true
	default:
		return false
	}
}

// SplitMigrationTokenAuth requires the admin bearer token for migration control.
// Without a configured token, only an explicit insecure opt-in enables mutation.
func SplitMigrationTokenAuth(token string, insecureNoAuth bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	if token != "" {
		return bearerTokenAuth(token, splitMigrationMutatingMethod, "split migration admin")
	}
	if insecureNoAuth {
		return nil, nil
	}
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if splitMigrationMutatingMethod(info.FullMethod) {
			return nil, status.Error(codes.Unauthenticated, "split migration requires a configured admin token")
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if splitMigrationMutatingMethod(info.FullMethod) {
			return status.Error(codes.Unauthenticated, "split migration requires a configured admin token")
		}
		return handler(srv, ss)
	}
	return unary, stream
}
