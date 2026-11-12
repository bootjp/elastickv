package tsowiring

import (
	"flag"

	"github.com/bootjp/elastickv/kv"
)

// dedicatedTSOGroupID mirrors noderuntime.DedicatedTSORaftGroupID, which the
// binary passes to configure.
const dedicatedTSOGroupID = 0

// configureCoordinatorTSO mirrors the binary's wrapper: it looks the
// dedicated group up by ID and configures the coordinator through it.
func configureCoordinatorTSO(
	f Flags,
	coordinate *kv.ShardedCoordinator,
	shardGroups map[uint64]*kv.ShardGroup,
	floorProviders ...kv.TSOCutoverFloorProvider,
) (Wiring, error) {
	var floorProvider kv.TSOCutoverFloorProvider
	if len(floorProviders) > 0 {
		floorProvider = floorProviders[0]
	}
	tsoGroup, dedicated := shardGroups[dedicatedTSOGroupID]
	return Configure(f, coordinate, dedicatedTSOGroupID, tsoGroup, dedicated, floorProvider, nil)
}

// defaultTSOFlags returns the registered flag defaults.
func defaultTSOFlags() Flags {
	var f Flags
	f.Register(flag.NewFlagSet("defaults", flag.ContinueOnError))
	return f
}
