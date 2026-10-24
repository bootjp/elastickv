package main

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
)

func migrationInternalProtocolOptions(tx kv.Transactional, shards *kv.ShardStore, server *adapter.DistributionServer, runtime *raftGroupRuntime) []adapter.InternalOption {
	installed := func() bool { return migrationOwnershipRuntimeHooksInstalled(tx, shards, server, runtime.spec.id) }
	return []adapter.InternalOption{
		adapter.WithInternalMigrationTransferSizeGuardReady(installed),
		adapter.WithInternalMigrationPartitionProtocolReady(installed),
		adapter.WithMigrationNamespaceAdmissionReady(func() bool {
			return installed() && kv.MigrationNamespaceInstalledVersion(runtime.store, runtime.stateMachine) == kv.MigrationNamespaceVersion
		}),
	}
}

func migrationPartitionRuntimeOption(reads *kv.ShardStore, groups map[uint64]*kv.ShardGroup, cache *kv.GRPCConnCache,
	token string, enforced bool,
) adapter.DistributionServerOption {
	if token == "" || !enforced || reads == nil {
		return nil
	}
	return adapter.WithMigrationPartitionRuntime(reads, migrationApplyMemberClients(splitMigrationApplyMemberSnapshotFactory(groups, cache, token)))
}

func migrationApplyMemberClients(factory adapter.SplitMigrationApplyMemberSnapshotFactory) adapter.SplitMigrationVoterFactory {
	return func(ctx context.Context, groupID uint64) ([]adapter.SplitMigrationVoter, error) {
		snapshot, err := factory(ctx, groupID)
		return snapshot.Members, err
	}
}
