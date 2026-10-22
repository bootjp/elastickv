package adapter

import (
	"context"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func splitMigrationMutatingMethod(method string) bool {
	switch method {
	case pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName,
		pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName,
		pb.Internal_CleanupMigration_FullMethodName:
		return true
	default:
		return false
	}
}

func splitMigrationProtectedMethod(method string) bool {
	switch method {
	case pb.Internal_ExportRangeVersions_FullMethodName,
		pb.Internal_ProbeMigrationLocks_FullMethodName,
		pb.Internal_ProbeMigrationState_FullMethodName,
		pb.Internal_IssueMigrationTimestamp_FullMethodName:
		return true
	default:
		return splitMigrationMutatingMethod(method)
	}
}

func splitMigrationAuthenticatedMethod(method, token string) bool {
	// Forward can write arbitrary system/transaction metadata. In a migration
	// cluster it is a privileged peer RPC, not an alternative public write API.
	return splitMigrationProtectedMethod(method) || (token != "" && method == pb.Internal_Forward_FullMethodName)
}

// SplitMigrationTokenAuth protects migration control and raw MVCC exports.
// Without a configured token, only an explicit insecure opt-in enables access.
func SplitMigrationTokenAuth(token string, insecureNoAuth bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	if token == "" && insecureNoAuth {
		return nil, nil
	}
	authUnary, authStream := bearerTokenAuth(token, func(method string) bool {
		return splitMigrationAuthenticatedMethod(method, token)
	}, "split migration controller")
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkSplitMigrationTransport(ctx, info.FullMethod, token); err != nil {
			return nil, err
		}
		if authUnary != nil {
			return authUnary(ctx, req, info, handler)
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkSplitMigrationTransport(ss.Context(), info.FullMethod, token); err != nil {
			return err
		}
		if authStream != nil {
			return authStream(srv, ss, info, handler)
		}
		return handler(srv, ss)
	}
	return unary, stream
}

func checkSplitMigrationTransport(ctx context.Context, method, token string) error {
	if !splitMigrationAuthenticatedMethod(method, token) {
		return nil
	}
	if token == "" {
		return errors.WithStack(status.Error(codes.Unauthenticated, "split migration requires a configured migration token"))
	}
	p, ok := peer.FromContext(ctx)
	if !ok {
		return errors.WithStack(status.Error(codes.Unauthenticated, "split migration token requires TLS"))
	}
	if _, ok := p.AuthInfo.(credentials.TLSInfo); !ok {
		return errors.WithStack(status.Error(codes.Unauthenticated, "split migration token requires TLS"))
	}
	return nil
}
