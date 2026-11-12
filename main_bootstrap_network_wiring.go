package main

import (
	"context"

	"github.com/bootjp/elastickv/adapter"
	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
)

func configureBootstrapNetwork(ctx context.Context, runtimes []*raftGroupRuntime, shards *kv.ShardStore, groups map[uint64]*kv.ShardGroup, cleanup *internalutil.CleanupStack) (*bootstrapNetwork, error) {
	domain := bootstrapDomainForRuntimes(runtimes)
	if domain == nil {
		return nil, nil
	}
	configs, err := bootstrapNetworkConfigs(runtimes, domain)
	if err != nil {
		return nil, err
	}
	network, err := newBootstrapNetwork(ctx, domain, configs)
	if err != nil {
		return nil, err
	}
	cleanup.Add(network.Close)
	if err := shards.SetInternalGRPCDialOptions(network.DialOptions); err != nil {
		return nil, errors.WithStack(err)
	}
	shards.SetStartupControlRPC(network.startup)
	if err := installBootstrapProxyNetwork(network, groups); err != nil {
		return nil, err
	}
	for _, runtime := range runtimes {
		runtime.bootstrapNetwork = network
	}
	return network, nil
}

func bootstrapNetworkConfigs(runtimes []*raftGroupRuntime, domain *bootstrapDomain) (map[uint64]*bootstrapPeerConfig, error) {
	configs := make(map[uint64]*bootstrapPeerConfig, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.bootstrapDomain != domain || runtime.bootstrapPeerConfig == nil {
			return nil, errors.New("protected network requires every runtime's original peer configuration")
		}
		configs[runtime.Spec.ID] = runtime.bootstrapPeerConfig
	}
	return configs, nil
}

func installBootstrapProxyNetwork(network *bootstrapNetwork, groups map[uint64]*kv.ShardGroup) error {
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

func bootstrapNetworkForRuntimes(runtimes []*raftGroupRuntime) *bootstrapNetwork {
	for _, runtime := range runtimes {
		if runtime != nil && runtime.bootstrapNetwork != nil {
			return runtime.bootstrapNetwork
		}
	}
	return nil
}

func newBootstrapRuntimeConnCache(runtimes []*raftGroupRuntime) *kv.GRPCConnCache {
	return kv.NewGRPCConnCache(bootstrapRuntimeDialOptions(runtimes))
}

func bootstrapRuntimeDialOptions(runtimes []*raftGroupRuntime) kv.GRPCDialOptionsForAddress {
	if network := bootstrapNetworkForRuntimes(runtimes); network != nil {
		return network.DialOptions
	}
	if bootstrapDomainForRuntimes(runtimes) != nil {
		return func(string) ([]grpc.DialOption, error) {
			return nil, errors.New("protected runtime network was not installed")
		}
	}
	return nil
}

func bootstrapTSORoutingOptions(networks []*bootstrapNetwork) []kv.LeaderRoutedTSOAllocatorOption {
	if len(networks) == 0 || networks[0] == nil {
		return nil
	}
	network := networks[0]
	return []kv.LeaderRoutedTSOAllocatorOption{kv.WithTSOInternalGRPCDialOptions(network.DialOptions), kv.WithTSOStartupControlRPC(network.startup)}
}

func bootstrapSplitMigrationProbe(runtimes []*raftGroupRuntime) adapter.SplitMigrationCapabilityProbe {
	provider := bootstrapRuntimeDialOptions(runtimes)
	return func(ctx context.Context, address string) error {
		return adapter.ProbeSplitMigrationCapabilityPeerWithDial(ctx, address, provider)
	}
}
