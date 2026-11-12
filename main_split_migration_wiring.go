package main

import (
	"context"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/raftadmin"
	"github.com/bootjp/elastickv/internal/raftengine"
	etcdraftengine "github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/bootjp/elastickv/internal/splitmigration"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func splitMigrationCapabilityPeerSourceForRuntimes(runtimes []*raftGroupRuntime) splitmigration.CapabilityPeerSource {
	return func(ctx context.Context) ([]splitmigration.CapabilityPeer, error) {
		if len(runtimes) == 0 {
			return nil, errors.New("raft group runtimes are not configured")
		}
		peers := make([]splitmigration.CapabilityPeer, 0)
		seen := make(map[string]struct{})
		for _, rt := range runtimes {
			if rt == nil {
				return nil, errors.New("raft group runtime is not configured")
			}
			engine := rt.snapshotEngine()
			if engine == nil {
				return nil, errors.Errorf("raft group %d engine is not configured", rt.Spec.ID)
			}
			cfg, err := engine.Configuration(ctx)
			if err != nil {
				return nil, errors.Wrapf(err, "raft group %d configuration", rt.Spec.ID)
			}
			groupPeers := splitmigration.CapabilityPeersFromConfiguration(cfg)
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

// splitMigrationLocalReadinessGate reports whether this node may run split
// migration. Its method value is the readiness callback handed to the
// distribution server, the gRPC coverage gate and the background runners.
func (f nodeAuthFlags) splitMigrationLocalReadinessGate(ctx context.Context) error {
	if err := f.splitMigrationPeerAuthReadinessGate(ctx); err != nil {
		return err
	}
	if !adapter.MigrationImportOpcodeEnabledFromEnv() {
		return errors.Errorf("%s is disabled", adapter.MigrationImportOpcodeEnv)
	}
	if !adapter.MigrationPromoteOpcodeEnabledFromEnv() {
		return errors.Errorf("%s is disabled", adapter.MigrationPromoteOpcodeEnv)
	}
	if !adapter.MigrationCleanupOpcodeEnabledFromEnv() {
		return errors.Errorf("%s is disabled", adapter.MigrationCleanupOpcodeEnv)
	}
	return nil
}

func (f nodeAuthFlags) splitMigrationPeerAuthReadinessGate(context.Context) error {
	if f.adminInsecureNoAuth || f.splitMigrationPeerAuthEnforced {
		return nil
	}
	return errors.New("migration is paused during peer auth rollout; enable --splitMigrationPeerAuthEnforced only after every node sends the token")
}

func migrationInternalProtocolOptions(tx kv.Transactional, shards *kv.ShardStore, server *adapter.DistributionServer, runtime *raftGroupRuntime) []adapter.InternalOption {
	installed := func() bool { return splitmigration.OwnershipRuntimeHooksInstalled(tx, shards, server, runtime.Spec.ID) }
	return []adapter.InternalOption{
		adapter.WithInternalMigrationTransferSizeGuardReady(installed),
		adapter.WithInternalMigrationPartitionProtocolReady(installed),
		adapter.WithMigrationNamespaceAdmissionReady(func() bool {
			return installed() && kv.MigrationNamespaceInstalledVersion(runtime.Store, runtime.StateMachine) == kv.MigrationNamespaceVersion
		}),
	}
}

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

func migrationOwnershipRefreshOption(groups map[uint64]*kv.ShardGroup, root *raftGroupRuntime, cache *kv.GRPCConnCache, token string, enforced bool) adapter.DistributionServerOption {
	if token == "" || !enforced {
		// Keep durable ownership visible to metadata preflight even when the
		// authenticated mutation protocol is disabled. Do not install a root
		// client or advertise a configured refresh runtime in this case.
		return adapter.WithMigrationOwnershipRefresh(groups, nil)
	}
	return adapter.WithMigrationOwnershipRefresh(groups, splitmigration.OwnershipRootClientFactory(migrationOwnershipRootLeaderResolver(root), cache, token))
}

func migrationMembershipInterceptor(runtime *raftGroupRuntime, shards *kv.ShardStore, proposer raftengine.Proposer,
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
	protocols := splitmigration.NewMembershipGate(runtime.Store, splitmigration.MemberProtocolsConfigured(),
		splitmigration.NewMemberProtocolProbe(token, bootstrapRuntimeDialOptions([]*raftGroupRuntime{runtime})), prepare)
	return splitmigration.NewNamespaceMembershipGate(runtime.Store, runtime.snapshotEngine(), proposer, token, protocols, etcdraftengine.DeriveNodeID, bootstrapRuntimeDialOptions([]*raftGroupRuntime{runtime}))
}
