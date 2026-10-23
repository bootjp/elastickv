package main

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newInternalMigrationTransaction(proposer raftengine.Proposer, observer kv.ProposalObserver, st store.MVCCStore, groupID uint64) *kv.TransactionManager {
	opts := []kv.TransactionOption{kv.WithProposalObserver(observer)}
	if groupID != 0 {
		opts = append(opts, kv.WithMigrationRequestAdmission(st, groupID))
	}
	return kv.NewTransactionWithProposer(proposer, opts...)
}

func migrationOwnershipRuntimeHooksInstalled(tx kv.Transactional, shards *kv.ShardStore, server *adapter.DistributionServer, groupID uint64) bool {
	return kv.MigrationPartitionRequestAdmissionReady(tx) && shards.MigrationPartitionReadAdmissionReady(groupID) && server.MigrationPartitionRuntimeConfigured(groupID)
}

func migrationOwnershipRuntimeReady(tx kv.Transactional, shards *kv.ShardStore, server *adapter.DistributionServer, groupID uint64) bool {
	return migrationOwnershipRuntimeHooksInstalled(tx, shards, server, groupID) && migrationOwnershipActivationGate() == nil
}

// This Draft foundation intentionally has no activation override. Installing
// the hooks is not sequential-split geometry or whole-runtime acceptance.
// Revisit only after the chosen protocol and its integrated evidence exist.
func migrationOwnershipActivationGate() error {
	return errors.WithStack(status.Error(codes.FailedPrecondition,
		"migration protocol not enabled: ownership geometry and whole-runtime coverage acceptance are incomplete"))
}

// Old controllers may cache cap_migration_v2. Refuse their side-effecting RPCs
// too, while leaving ordinary KV, authenticated 2PC resolution, same-group
// SplitRange, and separately authenticated general Admin traffic unchanged.
func migrationCoverageUnaryGate(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod == pb.Internal_CleanupMigration_FullMethodName {
		if control, ok := req.(*pb.CleanupMigrationRequest); ok && control.GetMode() == pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_NAMESPACE_ADMISSION {
			if err := kv.ValidateNamespaceAdmissionRequest(control); err != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid namespace admission control")
			}
			return handler(ctx, req)
		}
	}
	if migrationCoverageMutationMethod(info.FullMethod) {
		return nil, migrationOwnershipActivationGate()
	}
	return handler(ctx, req)
}

func migrationCoverageMutationMethod(method string) bool {
	switch method {
	case pb.Distribution_StartSplitMigration_FullMethodName,
		pb.Distribution_AbandonSplitJob_FullMethodName,
		pb.Distribution_RetrySplitJob_FullMethodName,
		pb.Internal_ApplyTargetStagedReadiness_FullMethodName,
		pb.Internal_ImportRangeVersions_FullMethodName,
		pb.Internal_PromoteStagedVersions_FullMethodName,
		pb.Internal_CleanupMigration_FullMethodName,
		pb.Internal_IssueMigrationTimestamp_FullMethodName:
		return true
	default:
		return false
	}
}
