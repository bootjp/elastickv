package main

import (
	"testing"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

func bootstrapStartupFixture() (*bootstrapPeerConfig, raftbootstrap.Intent, raftengine.FactoryConfig) {
	node := peersession.Node{Cluster: raftbootstrap.UUID{1}, Member: etcd.DeriveNodeID("n1"), Address: "localhost:50051", Key: raftbootstrap.Digest{2}}
	config := &bootstrapPeerConfig{local: node, provisions: []peersession.Provision{{Node: node, Groups: []uint64{1}}}}
	intent := raftbootstrap.Intent{
		Identity: raftbootstrap.Identity{Cluster: node.Cluster, Group: 1}, BaselineKind: raftbootstrap.Seed,
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1},
		Baseline: raftbootstrap.Configuration{State: raftbootstrap.ConfState{Voters: []uint64{node.Member}},
			Members: []raftbootstrap.Member{{ID: node.Member, Address: node.Address, PublicKeyDigest: node.Key}}},
	}
	return config, intent, raftengine.FactoryConfig{GroupID: 1, LocalID: "n1", LocalAddress: node.Address}
}

func TestBootstrapStartupProvisionedIdentity(t *testing.T) {
	t.Parallel()
	config, intent, cfg := bootstrapStartupFixture()
	id, err := validateBootstrapStartup(config, intent, cfg)
	require.NoError(t, err)
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	require.NoError(t, err)
	require.Equal(t, etcd.WALIdentity{Identity: intent.Identity, NodeID: config.local.Member, BaselineDigest: digest}, id)
}

func TestBootstrapStartupRejectsMismatchesBeforeTransport(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"group", "node", "address", "cluster", "unknown-group", "invalid-intent"} {
		t.Run(name, func(t *testing.T) {
			config, intent, cfg := bootstrapStartupFixture()
			switch name {
			case "group":
				cfg.GroupID++
			case "node":
				cfg.LocalID = "n2"
			case "address":
				cfg.LocalAddress = "localhost:50052"
			case "cluster":
				intent.Identity.Cluster[0]++
			case "unknown-group":
				config.provisions[0].Groups = []uint64{2}
			case "invalid-intent":
				intent.Minimum.Entry = 0
			}
			_, err := validateBootstrapStartup(config, intent, cfg)
			require.Error(t, err)
			_, err = newBootstrapStartupForGroup(config, nil, intent, nil, nil)(cfg, nil)
			require.Error(t, err)
		})
	}
}

func TestBootstrapStartupRejectsTSOWithoutPolicyStore(t *testing.T) {
	t.Parallel()
	config, intent, cfg := bootstrapStartupFixture()
	_, err := newBootstrapStartupForGroup(config, nil, intent, nil, nil)(cfg, nil)
	require.ErrorContains(t, err, "TSO policy recovery is not implemented")
}

func TestBootstrapStartupRejectsUnverifiedRecovery(t *testing.T) {
	t.Parallel()
	config, intent, cfg := bootstrapStartupFixture()
	id, err := validateBootstrapStartup(config, intent, cfg)
	require.NoError(t, err)
	require.ErrorIs(t, publishBootstrapRecovery(nil, nil, nil, id, etcd.RecoveredState{}, 1), etcd.ErrWALIdentity)
}

func TestBootstrapStartupPeerAddressReconciliation(t *testing.T) {
	t.Parallel()
	config, intent, _ := bootstrapStartupFixture()
	peer := etcd.Peer{NodeID: config.local.Member, Address: config.local.Address}
	require.NoError(t, validateBootstrapPeerAddresses(intent.Baseline, []etcd.Peer{peer}))
	changed := peer
	changed.Address = "localhost:50052"
	require.Error(t, validateBootstrapPeerAddresses(intent.Baseline, []etcd.Peer{changed}))
	changed = peer
	changed.NodeID++
	require.Error(t, validateBootstrapPeerAddresses(intent.Baseline, []etcd.Peer{changed}))
	require.Error(t, validateBootstrapPeerAddresses(intent.Baseline, nil))
	require.Error(t, validateBootstrapPeerAddresses(intent.Baseline, []etcd.Peer{peer, peer}))
}
