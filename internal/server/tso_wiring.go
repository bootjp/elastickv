package server

import (
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/tsowiring"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func configureCoordinatorTSO(
	f tsowiring.Flags,
	coordinate *kv.ShardedCoordinator,
	shardGroups map[uint64]*kv.ShardGroup,
	floorProviders ...kv.TSOCutoverFloorProvider,
) (tsowiring.Wiring, error) {
	var floorProvider kv.TSOCutoverFloorProvider
	if len(floorProviders) > 0 {
		floorProvider = floorProviders[0]
	}
	return configureCoordinatorTSOWithObserver(f, coordinate, shardGroups, floorProvider, nil)
}

func configureCoordinatorTSOWithObserver(
	f tsowiring.Flags,
	coordinate *kv.ShardedCoordinator,
	shardGroups map[uint64]*kv.ShardGroup,
	floorProvider kv.TSOCutoverFloorProvider,
	observer kv.TSOObserver,
	networks ...*clusterbootstrap.BootstrapNetwork,
) (tsowiring.Wiring, error) {
	tsoGroup, dedicated := shardGroups[noderuntime.DedicatedTSORaftGroupID]
	wiring, err := tsowiring.Configure(f, coordinate, noderuntime.DedicatedTSORaftGroupID, tsoGroup, dedicated,
		floorProvider, observer, clusterbootstrap.BootstrapTSORoutingOptions(networks)...)
	return wiring, errors.WithStack(err)
}
