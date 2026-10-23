package main

import (
	"context"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func migrationOwnershipRootLeaderResolver(runtime *raftGroupRuntime) func() (string, error) {
	return func() (string, error) {
		if runtime == nil {
			return "", errors.WithStack(kv.ErrLeaderNotFound)
		}
		engine := runtime.snapshotEngine()
		if engine == nil {
			return "", errors.WithStack(kv.ErrLeaderNotFound)
		}
		address := strings.TrimSpace(engine.Leader().Address)
		if address == "" {
			return "", errors.WithStack(kv.ErrLeaderNotFound)
		}
		return address, nil
	}
}

func migrationOwnershipRootClientFactory(resolve func() (string, error), cache *kv.GRPCConnCache, token string) adapter.MigrationOwnershipAttestorFactory {
	return func(context.Context) (adapter.MigrationOwnershipAttestor, error) {
		// This endpoint is never downgraded to anonymous catalog reads, even
		// during peer send-first rollout or on an unsupported old root binary.
		if token == "" || resolve == nil || cache == nil {
			return nil, errors.New("ownership refresh requires authenticated root client configuration")
		}
		address, err := resolve()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		conn, err := cache.ConnFor(address)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return pb.NewDistributionClient(splitMigrationAuthConn{ClientConnInterface: conn, token: token}), nil
	}
}

func migrationOwnershipRefreshOption(groups map[uint64]*kv.ShardGroup, root *raftGroupRuntime, cache *kv.GRPCConnCache, token string, enforced bool) adapter.DistributionServerOption {
	if token == "" || !enforced {
		return nil
	}
	return adapter.WithMigrationOwnershipRefresh(groups, migrationOwnershipRootClientFactory(migrationOwnershipRootLeaderResolver(root), cache, token))
}

func startMigrationOwnershipRefresh(ctx context.Context, eg *errgroup.Group, server *adapter.DistributionServer) {
	if eg == nil || server == nil || splitMigrationLocalReadinessGate(ctx) != nil {
		return
	}
	eg.Go(func() error { return server.RunMigrationOwnershipRefresh(ctx) })
}
