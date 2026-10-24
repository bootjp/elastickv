package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
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
	case pb.Distribution_SplitRange_FullMethodName,
		pb.Distribution_StartSplitMigration_FullMethodName,
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
		pb.Distribution_AttestMigrationOwnership_FullMethodName,
		pb.Internal_ProbeMigrationLocks_FullMethodName,
		pb.Internal_ProbeMigrationState_FullMethodName,
		pb.Internal_IssueMigrationTimestamp_FullMethodName:
		return true
	default:
		return splitMigrationMutatingMethod(method)
	}
}

func splitMigrationAuthenticatedMethod(method string, peerAuthEnforced bool) bool {
	// Forward can write arbitrary system/transaction metadata. In a migration
	// cluster it is a privileged peer RPC, not an alternative public write API.
	return splitMigrationProtectedMethod(method) || (peerAuthEnforced && method == pb.Internal_Forward_FullMethodName)
}

// SplitMigrationTokenAuth protects catalog splits, migration control and raw MVCC exports.
// Even a proof-free SplitRange advances the global catalog and needs operator authority.
// Without a configured token, only an explicit insecure opt-in enables access.
// Peer enforcement follows a send-first rollout; the runner must stay paused
// until enforcement is enabled (except in explicit isolated-test no-auth mode).
func SplitMigrationTokenAuth(token string, insecureNoAuth, peerAuthEnforced bool) (grpc.UnaryServerInterceptor, grpc.StreamServerInterceptor) {
	if token == "" && insecureNoAuth {
		return nil, nil
	}
	authUnary, authStream := bearerTokenAuth(token, func(method string) bool {
		return splitMigrationAuthenticatedMethod(method, peerAuthEnforced)
	}, "split migration controller")
	controlAuth, _ := bearerTokenAuth(token, func(string) bool { return true }, "split migration peer")
	unary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == pb.Internal_Forward_FullMethodName && splitForwardWritesJobControl(req) {
			if err := checkSplitMigrationTransport(ctx, info.FullMethod, token, true); err != nil {
				return nil, err
			}
			return controlAuth(ctx, req, info, handler)
		}
		if err := checkSplitMigrationTransport(ctx, info.FullMethod, token, peerAuthEnforced); err != nil {
			return nil, err
		}
		if authUnary != nil {
			return authUnary(ctx, req, info, handler)
		}
		return handler(ctx, req)
	}
	stream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkSplitMigrationTransport(ss.Context(), info.FullMethod, token, peerAuthEnforced); err != nil {
			return err
		}
		if authStream != nil {
			return authStream(srv, ss, info, handler)
		}
		return handler(srv, ss)
	}
	return unary, stream
}

// Job authority and typed migration metadata must never be planted during
// send-first rollout. Ordinary legacy peer transaction writes remain compatible.
func splitForwardWritesJobControl(req any) bool {
	forward, ok := req.(*pb.ForwardRequest)
	if !ok {
		return false
	}
	for _, request := range forward.GetRequests() {
		for _, mutation := range request.GetMutations() {
			if splitJobControlMutation(mutation) {
				return true
			}
		}
	}
	return false
}

func splitJobControlMutation(mutation *pb.Mutation) bool {
	key := mutation.GetKey()
	embedded := kv.ExtractTxnUserKey(key)
	if splitMigrationControlKey(key) || splitMigrationControlKey(embedded) {
		return true
	}
	if mutation.GetOp() != pb.Op_DEL_PREFIX {
		return false
	}
	return distribution.MigrationOnlyControlPrefixIntersects(key) ||
		distribution.MigrationOnlyControlPrefixIntersects(embedded) ||
		splitPartitionControlPrefix(key, embedded) ||
		bytes.HasPrefix(distribution.CatalogSplitJobKey(0), key) ||
		bytes.HasPrefix(distribution.CatalogSplitJobHistoryKey(0, 0), key) ||
		bytes.HasPrefix(distribution.CatalogNextSplitJobIDKey(), key)
}

func splitMigrationControlKey(key []byte) bool {
	return distribution.IsMigrationOnlyControlKey(key) || distribution.IsCatalogOwnershipPartitionKey(key) || distribution.IsCatalogSplitJobKey(key) ||
		distribution.IsCatalogSplitJobHistoryKey(key) || bytes.Equal(key, distribution.CatalogNextSplitJobIDKey())
}

func splitPartitionControlPrefix(key, embedded []byte) bool {
	return distribution.CatalogOwnershipPartitionPrefixIntersects(key) ||
		(embedded != nil && distribution.CatalogOwnershipPartitionPrefixIntersects(embedded))
}

func checkSplitMigrationTransport(ctx context.Context, method, token string, peerAuthEnforced bool) error {
	if !splitMigrationAuthenticatedMethod(method, peerAuthEnforced) {
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
