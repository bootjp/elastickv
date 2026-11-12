package server

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/bootjp/elastickv/internal/clusterbootstrap"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine/etcd"
	"github.com/stretchr/testify/require"
)

func TestProtectedBootstrapLaunchRequiresExactDurableFormat(t *testing.T) {
	t.Parallel()
	peerConfigDir := t.TempDir()
	raw := bootstrapConfigFixture(t)
	raw.Local = etcd.DeriveNodeID("n1")
	raw.Members[0].ID = raw.Local
	encoded, err := json.Marshal(raw)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(peerConfigDir, "group-1.json"), encoded, 0600))
	config, err := clusterbootstrap.BuildBootstrapPeerConfig(raw)
	require.NoError(t, err)
	dir := t.TempDir()
	files := raftbootstrap.NewStore(dir)
	identity := raftbootstrap.Identity{Cluster: config.Local.Cluster, Group: 1}
	intent := raftbootstrap.Intent{Identity: identity, BaselineKind: raftbootstrap.Accepted,
		Minimum: raftbootstrap.Versions{Entry: 1, Snapshot: 1, Session: 1}, Floor: raftbootstrap.Floor{ActivationIndex: 2, Generation: 1},
		Baseline: raftbootstrap.Configuration{Index: 1, State: raftbootstrap.ConfState{Voters: []uint64{raw.Local}},
			Members: []raftbootstrap.Member{{ID: raw.Local, Address: config.Local.Address, PublicKeyDigest: config.Local.Key}}}}
	digest, err := raftbootstrap.ConfigurationDigest(intent.Baseline)
	require.NoError(t, err)
	group := noderuntime.GroupSpec{ID: 1, Address: config.Local.Address}
	_, err = clusterbootstrap.LoadGroupBootstrapSpec("n1", group, dir, peerConfigDir)
	require.Error(t, err)
	policy := raftbootstrap.Policy{Identity: identity, BaselineDigest: digest, Minimum: intent.Minimum, Floor: intent.Floor}
	require.NoError(t, files.Publish(intent, raftbootstrap.Evidence{Identity: &identity, BaselineDigest: &digest, RaftState: raftbootstrap.Intact, Policy: &policy}))
	spec, err := clusterbootstrap.LoadGroupBootstrapSpec("n1", group, dir, peerConfigDir)
	require.NoError(t, err)
	factory, err := clusterbootstrap.BootstrapFactoryForGroup(etcd.NewFactory(etcd.FactoryConfig{}), spec, nil)
	require.NoError(t, err)
	require.NoError(t, noderuntime.EnsureRaftEngineDataDirForFactory(dir, noderuntime.RaftEngineEtcd, factory))
	marker := filepath.Join(dir, raftbootstrap.MarkerFile)
	require.NoError(t, os.WriteFile(marker, []byte("etcd\n"), 0600))
	require.Error(t, noderuntime.EnsureRaftEngineDataDirForFactory(dir, noderuntime.RaftEngineEtcd, factory))
	got, err := os.ReadFile(marker)
	require.NoError(t, err)
	require.Equal(t, []byte("etcd\n"), got, "preflight must not rewrite an invalid protected marker")
}

func TestProtectedRuntimeReadinessRemainsRevocable(t *testing.T) {
	t.Parallel()
	ready := false
	runtimes := []*clusterbootstrap.GroupRuntime{{GroupRuntime: &noderuntime.GroupRuntime{PublicReady: func() bool { return ready }}}}
	gate := &clusterbootstrap.StartupPublicKVGate{BlockRuntime: func() bool { return clusterbootstrap.ProtectedRuntimesBlocked(runtimes) }}
	gate.MarkReady()
	require.True(t, gate.Blocked())
	ready = true
	require.False(t, gate.Blocked())
	ready = false
	require.True(t, gate.Blocked())
}

func TestProtectedDomainRejectsIncompleteConfigurationBeforeCreatingState(t *testing.T) {
	t.Parallel()
	peerConfigDir := t.TempDir()
	base := t.TempDir()
	err := clusterbootstrap.ValidateProtectedBootstrapDomain("n1", base, []noderuntime.GroupSpec{{ID: 1, Address: "localhost:50051"}}, true, peerConfigDir)
	require.Error(t, err)
	require.NoDirExists(t, noderuntime.GroupDataDir(base, "n1", 1, true))
	err = clusterbootstrap.ValidateProtectedBootstrapDomain("n1", base, []noderuntime.GroupSpec{{ID: noderuntime.DedicatedTSORaftGroupID}}, true, peerConfigDir)
	require.Error(t, err, "a missing group-zero configuration must not fall back to legacy startup")
	require.NoDirExists(t, noderuntime.GroupDataDir(base, "n1", noderuntime.DedicatedTSORaftGroupID, true))
}
