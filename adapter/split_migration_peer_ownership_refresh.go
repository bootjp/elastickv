package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func NewMigrationOwnershipRootGRPCClientFactory(resolve func() (string, error), cache *kv.GRPCConnCache, token string) MigrationOwnershipAttestorFactory {
	return func(context.Context) (MigrationOwnershipAttestor, error) {
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

func StartMigrationOwnershipRefresh(ctx context.Context, eg *errgroup.Group, server *DistributionServer, ready MigrationReadinessGate) {
	if eg == nil || server == nil || ready.Check(ctx) != nil {
		return
	}
	eg.Go(func() error { return server.RunMigrationOwnershipRefresh(ctx) })
}
