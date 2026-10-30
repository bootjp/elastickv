package etcd

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
)

func TestEngineShutdownStatusRefreshDoesNotReviveAuthority(t *testing.T) {
	storage := etcdraft.NewMemoryStorage()
	snapshot := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snapshot))
	peers := map[uint64]Peer{1: {NodeID: 1, ID: "n1", Address: "127.0.0.1:7001"}}
	e := &Engine{
		nodeID: 1, localID: "n1", storage: storage, rawNode: mustRawNode(t, storage, 1),
		fsm: &testStateMachine{}, raftCutoverIndex: orInertCutover(nil), peers: peers,
		config:     configurationFromConfState(peers, confStateValue(snapshot.GetMetadata().GetConfState())),
		voterCount: 1, applied: 1, leaderReady: make(chan struct{}),
	}
	require.NoError(t, e.rawNode.Campaign())
	require.NoError(t, e.drainReady())
	e.refreshStatus()
	require.Equal(t, raftengine.StateLeader, e.State())
	acquired, lost := 0, 0
	e.RegisterLeaderAcquiredCallback(func() { acquired++ })
	e.RegisterLeaderLossCallback(func() { lost++ })
	require.True(t, e.publishShutdown(nil))
	require.Equal(t, 1, lost)
	// Quiescing must still pump storage responses. A refresh of the retained
	// RawNode role is not permission to publish leadership after shutdown.
	e.refreshStatus()
	require.Equal(t, raftengine.StateShutdown, e.State())
	require.True(t, e.LastQuorumAck().IsZero())
	require.False(t, e.isLeader.Load())
	require.Zero(t, acquired)
	require.Equal(t, 1, lost)
}
