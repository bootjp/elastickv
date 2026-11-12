package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
)

func MigrationPartitionRuntimeOption(reads *kv.ShardStore, groups map[uint64]*kv.ShardGroup, cache *kv.GRPCConnCache,
	token string, enforced bool,
) DistributionServerOption {
	if token == "" || !enforced || reads == nil {
		return nil
	}
	return WithMigrationPartitionRuntime(reads, migrationApplyMemberClients(NewSplitMigrationApplyMemberSnapshotGRPCFactory(groups, cache, token)))
}

func migrationApplyMemberClients(factory SplitMigrationApplyMemberSnapshotFactory) SplitMigrationVoterFactory {
	return func(ctx context.Context, groupID uint64) ([]SplitMigrationVoter, error) {
		snapshot, err := factory(ctx, groupID)
		return snapshot.Members, err
	}
}
