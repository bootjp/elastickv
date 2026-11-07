package etcd

import (
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
)

type nativeConfigurationFSM struct {
	entryIdentityFSM
	configuration raftengine.NativeConfiguration
	cleared       bool
}

func (f *nativeConfigurationFSM) SetApplyNativeConfiguration(configuration raftengine.NativeConfiguration) {
	f.configuration = configuration
	f.cleared = false
}

func (f *nativeConfigurationFSM) ClearApplyNativeConfiguration() {
	f.configuration = raftengine.NativeConfiguration{}
	f.cleared = true
}

func TestApplyNativeConfigurationUsesActualRawNodeRoster(t *testing.T) {
	t.Parallel()
	fsm := &nativeConfigurationFSM{}
	e := newTestEngine(fsm, nil, nil)
	raw, err := etcdraft.NewRawNode(&etcdraft.Config{ID: 1, Storage: etcdraft.NewMemoryStorage(), ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsgs: 1})
	require.NoError(t, err)
	require.NoError(t, raw.Bootstrap([]etcdraft.Peer{{ID: 1}, {ID: 2}}))
	e.rawNode = raw
	e.recoveredIdentity = &WALIdentity{}
	e.nativeConfigurationIndex = 2
	e.configIndex.Store(99)
	e.peers = map[uint64]Peer{1: {NodeID: 1, Address: "node1:50051"}, 2: {NodeID: 2, Address: "node2:50051"}}
	clear := e.deliverApplyNativeConfiguration()
	require.Equal(t, uint64(2), fsm.configuration.Index)
	require.Equal(t, []uint64{1, 2}, fsm.configuration.Voters)
	require.Equal(t, []raftengine.NativeConfigurationMember{{ID: 1, Address: "node1:50051"}, {ID: 2, Address: "node2:50051"}}, fsm.configuration.Members)
	e.peers[1] = Peer{NodeID: 1, Address: "changed:50051"}
	require.Equal(t, "node1:50051", fsm.configuration.Members[0].Address)
	clear()
	require.True(t, fsm.cleared)
	require.Zero(t, fsm.configuration)
}

func TestApplyNativeConfigurationCannotReuseAnUnverifiedImage(t *testing.T) {
	t.Parallel()
	fsm := &nativeConfigurationFSM{configuration: raftengine.NativeConfiguration{Index: 99}}
	e := newTestEngine(fsm, nil, nil)
	clear := e.deliverApplyNativeConfiguration()
	require.True(t, fsm.cleared)
	require.Zero(t, fsm.configuration)
	clear()
}
