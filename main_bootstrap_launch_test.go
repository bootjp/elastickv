package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

func TestProtectedBootstrapLaunchRequiresExactDurableFormat(t *testing.T) {
	old := *raftBootstrapPeerConfigDir
	t.Cleanup(func() { *raftBootstrapPeerConfigDir = old })
	*raftBootstrapPeerConfigDir = t.TempDir()
	raw := bootstrapConfigFixture(t)
	raw.Local = etcd.DeriveNodeID("n1")
	raw.Members[0].ID = raw.Local
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(*raftBootstrapPeerConfigDir, "group-1.json"), encoded, 0600))
	config, err := buildBootstrapPeerConfig(raw)
	require.NoError(t, err)
	dir := t.TempDir()
	files := raftbootstrap.NewStore(dir)
	identity := raftbootstrap.Identity{Cluster: config.local.Cluster, Group: 1}
	intent := raftbootstrap.Intent{Identity: identity, BaselineKind: raftbootstrap.Accepted,
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, Floor: raftbootstrap.Floor{ActivationIndex: 2, Generation: 1},
		Baseline: raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{raw.Local}},
			Members: []raftbootstrap.Member{{ID: raw.Local, Address: config.local.Address, PublicKeyDigest: config.local.Key}}}}
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	require.NoError(t, err)
	group := groupSpec{id: 1, address: config.local.Address}
	_, err = loadGroupBootstrapSpec("n1", group, dir)
	require.Error(t, err)
	policy := raftbootstrap.Policy{Identity: identity, BaselineDigest: digest, Minimum: intent.Minimum, Floor: intent.Floor}
	require.NoError(t, files.Publish(intent, raftbootstrap.Evidence{Identity: &identity, BaselineDigest: &digest, RaftState: raftbootstrap.Intact, Policy: &policy}))
	spec, err := loadGroupBootstrapSpec("n1", group, dir)
	require.NoError(t, err)
	factory, err := bootstrapFactoryForGroup(etcd.NewFactory(etcd.FactoryConfig{}), spec, nil)
	require.NoError(t, err)
	require.NoError(t, ensureRaftEngineDataDirForFactory(dir, raftEngineEtcd, factory))
	marker := filepath.Join(dir, raftbootstrap.MarkerFile)
	require.NoError(t, os.WriteFile(marker, []byte("etcd\n"), 0600))
	require.Error(t, ensureRaftEngineDataDirForFactory(dir, raftEngineEtcd, factory))
	got, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, []byte("etcd\n"), got, "preflight must not rewrite an invalid protected marker")
}

func TestProtectedRuntimeReadinessRemainsRevocable(t *testing.T) {
	ready := false
	runtimes := []*raftGroupRuntime{{publicReady: func() bool { return ready }}}
	gate := &startupPublicKVGate{blockRuntime: func() bool { return protectedRuntimesBlocked(runtimes) }}
	gate.markReady()
	require.True(t, gate.blocked())
	ready = true
	require.False(t, gate.blocked())
	ready = false
	require.True(t, gate.blocked())
}

func TestProtectedDomainRejectsIncompleteConfigurationBeforeCreatingState(t *testing.T) {
	old := *raftBootstrapPeerConfigDir
	t.Cleanup(func() { *raftBootstrapPeerConfigDir = old })
	*raftBootstrapPeerConfigDir = t.TempDir()
	base := t.TempDir()
	err := validateProtectedBootstrapDomain("n1", base, []groupSpec{{id: 1, address: "localhost:50051"}}, true)
	require.Error(t, err)
	require.NoDirExists(t, groupDataDir(base, "n1", 1, true))
	err = validateProtectedBootstrapDomain("n1", base, []groupSpec{{id: dedicatedTSORaftGroupID}}, true)
	require.Error(t, err, "a missing group-zero configuration must not fall back to legacy startup")
	require.NoDirExists(t, groupDataDir(base, "n1", dedicatedTSORaftGroupID, true))
}
