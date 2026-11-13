package main

import (
	"context"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func splitMigrationCapabilityPeerSourceForRuntimes(runtimes []*clusterbootstrap.GroupRuntime) adapter.SplitMigrationCapabilityPeerSource {
	return func(ctx context.Context) ([]adapter.SplitMigrationCapabilityPeer, error) {
		if len(runtimes) == 0 {
			return nil, errors.New("raft group runtimes are not configured")
		}
		peers := make([]adapter.SplitMigrationCapabilityPeer, 0)
		seen := make(map[string]struct{})
		for _, rt := range runtimes {
			if rt == nil {
				return nil, errors.New("raft group runtime is not configured")
			}
			engine := clusterbootstrap.RuntimeCore(rt).SnapshotEngine()
			if engine == nil {
				return nil, errors.Errorf("raft group %d engine is not configured", rt.Spec.ID)
			}
			cfg, err := engine.Configuration(ctx)
			if err != nil {
				return nil, errors.Wrapf(err, "raft group %d configuration", rt.Spec.ID)
			}
			groupPeers := adapter.SplitMigrationCapabilityPeersFromConfiguration(cfg)
			if len(groupPeers) == 0 {
				return nil, errors.Errorf("raft group %d configuration has no split migration capability peers", rt.Spec.ID)
			}
			for _, peer := range groupPeers {
				key := peer.ID + "\x00" + peer.Address
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				peers = append(peers, peer)
			}
		}
		return peers, nil
	}
}

func migrationInternalProtocolOptions(tx kv.Transactional, shards *kv.ShardStore, server *adapter.DistributionServer, runtime *clusterbootstrap.GroupRuntime) []adapter.InternalOption {
	installed := func() bool {
		return adapter.MigrationOwnershipRuntimeHooksInstalled(tx, shards, server, runtime.Spec.ID)
	}
	return []adapter.InternalOption{
		adapter.WithInternalMigrationTransferSizeGuardReady(installed),
		adapter.WithInternalMigrationPartitionProtocolReady(installed),
		adapter.WithMigrationNamespaceAdmissionReady(func() bool {
			return installed() && kv.MigrationNamespaceInstalledVersion(runtime.Store, runtime.StateMachine) == kv.MigrationNamespaceVersion
		}),
	}
}

func migrationOwnershipRootLeaderResolver(runtime *clusterbootstrap.GroupRuntime) func() (string, error) {
	return func() (string, error) {
		if runtime == nil {
			return "", errors.WithStack(kv.ErrLeaderNotFound)
		}
		engine := clusterbootstrap.RuntimeCore(runtime).SnapshotEngine()
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

func migrationOwnershipRefreshOption(groups map[uint64]*kv.ShardGroup, root *clusterbootstrap.GroupRuntime, cache *kv.GRPCConnCache, token string, enforced bool) adapter.DistributionServerOption {
	if token == "" || !enforced {
		// Keep durable ownership visible to metadata preflight even when the
		// authenticated mutation protocol is disabled. Do not install a root
		// client or advertise a configured refresh runtime in this case.
		return adapter.WithMigrationOwnershipRefresh(groups, nil)
	}
	return adapter.WithMigrationOwnershipRefresh(groups, adapter.NewMigrationOwnershipRootGRPCClientFactory(migrationOwnershipRootLeaderResolver(root), cache, token))
}

func migrationMembershipInterceptor(runtime *clusterbootstrap.GroupRuntime, shards *kv.ShardStore, proposer raftengine.Proposer,
	token string, next raftadmin.MembershipChangeInterceptor,
) raftadmin.MembershipChangeInterceptor {
	if runtime == nil {
		return next
	}
	prepare := newPrepareEvidenceMembershipGate(shards, runtime.Spec.ID, next)
	// Dedicated TSO has neither KV migration controls nor an MVCC namespace.
	if runtime.Spec.ID == 0 {
		return prepare
	}
	protocols := adapter.NewMigrationMembershipGate(runtime.Store, adapter.MigrationMemberProtocolsConfigured(),
		adapter.NewMigrationMemberProtocolProbe(token, clusterbootstrap.BootstrapRuntimeDialOptions([]*clusterbootstrap.GroupRuntime{runtime})), prepare)
	return adapter.NewMigrationNamespaceMembershipGate(runtime.Store, clusterbootstrap.RuntimeCore(runtime).SnapshotEngine(), proposer, token, protocols, etcdraftengine.DeriveNodeID, clusterbootstrap.BootstrapRuntimeDialOptions([]*clusterbootstrap.GroupRuntime{runtime}))
}
