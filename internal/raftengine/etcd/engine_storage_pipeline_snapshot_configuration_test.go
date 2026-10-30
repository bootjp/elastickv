package etcd

import (
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func TestEnginePipelineCapturedPrefixThenDeferredConfigurationReopens(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) { testPipelineCapturedPrefixConfiguration(t, v2) })
	}
}

func testPipelineCapturedPrefixConfiguration(t *testing.T, v2 bool) {
	t.Helper()
	c := newNativeSnapshotCluster(t, "save", "")
	old := c.propose(t, []byte("old-owned-prefix"))
	c.pumpUntil(t, func() bool { return c.gates[0].used.Load() }, "old checkpoint SaveSnap")
	for _, fsm := range c.fsms {
		fsm.deferCapture.Store(true)
	}
	message := nativeSnapshotConfigurationProposal(t, v2)
	c.pumpUntil(t, func() bool {
		err := c.nodes[0].engine.handleTransportMessage(t.Context(), message)
		if errors.Is(err, errStepQueueFull) {
			return false // Explicit rejection precedes Step; there was no proposal.
		}
		require.NoError(t, err)
		return true
	}, "configuration admission")
	c.gates[0].unblock()
	c.pumpUntil(t, func() bool { return c.nodes[0].engine.currentConfigIndex() > old }, "deferred configuration publication")
	snapshot, err := c.nodes[0].engine.storage.Snapshot()
	require.NoError(t, err)
	require.Equal(t, old, snapshot.GetMetadata().GetIndex())
	require.Equal(t, []uint64{1, 2, 3}, snapshot.GetMetadata().GetConfState().GetVoters())
	require.Empty(t, snapshot.GetMetadata().GetConfState().GetLearners(), "old capture must not inherit newer membership")
	restoredOld := &testStateMachine{}
	token, err := decodeSnapshotToken(snapshot.GetData())
	require.NoError(t, err)
	require.NoError(t, openAndRestoreFSMSnapshot(restoredOld, fsmSnapPath(c.nodes[0].engine.fsmSnapDir, token.Index), token.CRC32C))
	require.Equal(t, [][]byte{[]byte("old-owned-prefix")}, restoredOld.Applied())
	for _, fsm := range c.fsms {
		fsm.deferCapture.Store(false)
	}
	newIndex := c.propose(t, []byte("new-owned-prefix"))
	c.pumpUntil(t, func() bool {
		current, _ := c.nodes[0].engine.storage.Snapshot()
		return current.GetMetadata().GetIndex() >= newIndex
	}, "local checkpoint after deferred configuration")
	e := c.nodes[0].engine
	require.NoError(t, e.Close())
	fsm := &testStateMachine{}
	selected, err := loadWalState(zap.NewNop(), filepath.Join(e.dataDir, walDirName), filepath.Join(e.dataDir, snapDirName), e.fsmSnapDir, fsm, nil)
	require.NoError(t, err)
	require.GreaterOrEqual(t, selected.LocalSnap.GetMetadata().GetIndex(), newIndex)
	require.Equal(t, []uint64{4}, selected.LocalSnap.GetMetadata().GetConfState().GetLearners())
	require.Equal(t, [][]byte{[]byte("old-owned-prefix"), []byte("new-owned-prefix")}, fsm.Applied())
	require.NoError(t, closePersist(selected.Persist))
	reopened, err := Open(t.Context(), OpenConfig{NodeID: 1, LocalID: c.nodes[0].peer.ID,
		LocalAddress: c.nodes[0].peer.Address, DataDir: e.dataDir, StateMachine: &testStateMachine{}})
	require.NoError(t, err)
	require.NoError(t, reopened.Close())
}

func nativeSnapshotConfigurationProposal(t *testing.T, v2 bool) raftpb.Message {
	t.Helper()
	context, err := encodeConfChangeContext(0, Peer{NodeID: 4, ID: "n4", Address: "127.0.0.1:7044"})
	require.NoError(t, err)
	entry := &raftpb.Entry{Type: entryTypePtr(raftpb.EntryConfChange)}
	var change proto.Message = &raftpb.ConfChange{Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(4), Context: context}
	if v2 {
		entry.Type = entryTypePtr(raftpb.EntryConfChangeV2)
		change = &raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(4)}}, Context: context}
	}
	entry.Data, err = proto.Marshal(change)
	require.NoError(t, err)
	return raftpb.Message{Type: messageTypePtr(raftpb.MsgProp), From: proto.Uint64(2), To: proto.Uint64(1), Entries: []*raftpb.Entry{entry}}
}
