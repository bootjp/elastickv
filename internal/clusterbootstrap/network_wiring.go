package clusterbootstrap

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

func ConfigureBootstrapNetwork(ctx context.Context, runtimes []*GroupRuntime, shards *kv.ShardStore, groups map[uint64]*kv.ShardGroup, cleanup *internalutil.CleanupStack) (*BootstrapNetwork, error) {
	domain := BootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return nil, nil
	}
	configs, err := bootstrapNetworkConfigs(runtimes, domain)
	if err != nil {
		return nil, err
	}
	network, err := NewBootstrapNetwork(ctx, domain, configs)
	if err != nil {
		return nil, err
	}
	cleanup.Add(network.Close)
	if err := shards.SetInternalGRPCDialOptions(network.DialOptions); err != nil {
		return nil, errors.WithStack(err)
	}
	shards.SetStartupControlRPC(network.Startup)
	if err := installBootstrapProxyNetwork(network, groups); err != nil {
		return nil, err
	}
	for _, runtime := range runtimes {
		runtime.BootstrapNetwork = network
	}
	return network, nil
}

func bootstrapNetworkConfigs(runtimes []*GroupRuntime, domain *BootstrapDomain) (map[uint64]*BootstrapPeerConfig, error) {
	configs := make(map[uint64]*BootstrapPeerConfig, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.BootstrapDomain != domain || runtime.BootstrapPeerConfig == nil {
			return nil, errors.New("protected network requires every runtime's original peer configuration")
		}
		configs[runtime.Spec.ID] = runtime.BootstrapPeerConfig
	}
	return configs, nil
}

func installBootstrapProxyNetwork(network *BootstrapNetwork, groups map[uint64]*kv.ShardGroup) error {
	for _, group := range groups {
		if group == nil {
			continue
		}
		if proxy, ok := group.Txn.(*kv.LeaderProxy); ok {
			if err := proxy.SetInternalGRPCDialOptions(network.DialOptions); err != nil {
				return errors.WithStack(err)
			}
		}
	}
	return nil
}

func bootstrapNetworkForRuntimes(runtimes []*GroupRuntime) *BootstrapNetwork {
	for _, runtime := range runtimes {
		if runtime != nil && runtime.BootstrapNetwork != nil {
			return runtime.BootstrapNetwork
		}
	}
	return nil
}

func NewBootstrapRuntimeConnCache(runtimes []*GroupRuntime) *kv.GRPCConnCache {
	return kv.NewGRPCConnCache(BootstrapRuntimeDialOptions(runtimes))
}

func BootstrapRuntimeDialOptions(runtimes []*GroupRuntime) kv.GRPCDialOptionsForAddress {
	if network := bootstrapNetworkForRuntimes(runtimes); network != nil {
		return network.DialOptions
	}
	if BootstrapDomainForRuntimes(runtimes) != nil {
		return func(string) ([]grpc.DialOption, error) {
			return nil, errors.New("protected runtime network was not installed")
		}
	}
	return nil
}

func BootstrapTSORoutingOptions(networks []*BootstrapNetwork) []kv.LeaderRoutedTSOAllocatorOption {
	if len(networks) == 0 || networks[0] == nil {
		return nil
	}
	network := networks[0]
	return []kv.LeaderRoutedTSOAllocatorOption{kv.WithTSOInternalGRPCDialOptions(network.DialOptions), kv.WithTSOStartupControlRPC(network.Startup)}
}

func BootstrapSplitMigrationProbe(runtimes []*GroupRuntime) adapter.SplitMigrationCapabilityProbe {
	provider := BootstrapRuntimeDialOptions(runtimes)
	return func(ctx context.Context, address string) error {
		return adapter.ProbeSplitMigrationCapabilityPeerWithDial(ctx, address, provider)
	}
}
