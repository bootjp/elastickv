package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestEnginePipelineOldTermSnapshotReceiptStillAppliesSnapshot(t *testing.T) {
	c := newNativeSnapshotCluster(t, "", "save")
	c.dropThird = true
	index := c.propose(t, []byte("snapshot-term-two"))
	c.pumpUntil(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > index }, "snapshot checkpoint")
	c.gates[2].minimum.Store(index)
	c.dropThird = false
	c.pumpUntil(t, func() bool { return c.gates[2].used.Load() }, "held native SaveSnap")
	c.requireReceiverHeartbeats(t, func() { require.Less(t, c.ackThird.Load(), index) })
	c.dropThird = true
	e := c.nodes[2].engine
	require.NoError(t, e.handleTransportMessage(t.Context(), raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat),
		From: proto.Uint64(2), To: proto.Uint64(3), Term: proto.Uint64(99), Commit: proto.Uint64(index)}))
	c.pumpUntil(t, func() bool { return e.Status().Term == 99 }, "higher native term before snapshot receipt")
	c.gates[2].unblock()
	c.pumpUntil(t, func() bool { return e.AppliedIndex() >= index }, "old-term snapshot publication")
	require.NoError(t, e.handleTransportMessage(t.Context(), raftpb.Message{Type: messageTypePtr(raftpb.MsgApp),
		From: proto.Uint64(2), To: proto.Uint64(3), Term: proto.Uint64(99), Index: proto.Uint64(index), LogTerm: proto.Uint64(2), Commit: proto.Uint64(index + 1),
		Entries: []*raftpb.Entry{{Type: entryTypePtr(raftpb.EntryNormal), Index: proto.Uint64(index + 1), Term: proto.Uint64(99),
			Data: encodeProposalEnvelope(1000, []byte("after-old-term-snapshot-receipt"))}}}))
	c.pumpUntil(t, func() bool { return e.AppliedIndex() == index+1 }, "new-term apply after the genuine stale snapshot response")
	require.Equal(t, [][]byte{[]byte("snapshot-term-two"), []byte("after-old-term-snapshot-receipt")}, c.fsms[2].Applied())
	require.NoError(t, e.Close())
	state, _, err := e.storage.InitialState()
	require.NoError(t, err)
	require.Equal(t, uint64(99), state.GetTerm())
	require.Equal(t, index+1, state.GetCommit())
	require.Empty(t, e.storagePipeline.snapshotPins)
}

func TestEnginePipelineReceivedSnapshotWaitsForActualEarlierApply(t *testing.T) {
	c := newNativeSnapshotCluster(t, "", "apply")
	c.gates[2].minimum.Store(3)
	old := c.propose(t, []byte("earlier-held-data"))
	c.pumpUntil(t, func() bool { return c.gates[2].used.Load() }, "older native Apply")
	c.dropThird = true
	// Do not deliver pre-partition append frames after reconnect: a durable
	// ordinary append may legitimately ACK before Apply, and could cover the
	// target snapshot so RawNode would no longer need to restore it at all.
	// Empty append probes still pass so the leader learns it needs a snapshot.
	c.dropAppendThird = true
	newIndex := c.propose(t, []byte("snapshot-newer-data"))
	c.pumpUntil(t, func() bool { first, _ := c.nodes[0].engine.storage.FirstIndex(); return first > newIndex }, "newer checkpoint")
	c.dropThird = false
	c.pumpUntil(t, func() bool { return c.nodes[0].engine.SnapshotStreamSendCount() > 0 }, "snapshot arrived behind executing Apply")
	c.requireReceiverHeartbeats(t, func() {
		require.Less(t, c.nodes[2].engine.AppliedIndex(), old)
		require.Zero(t, c.fsms[2].restores.Load(), "restore overtook an executing earlier Apply")
		require.Less(t, c.ackThird.Load(), newIndex)
	})
	c.gates[2].unblock()
	c.pumpUntil(t, func() bool { return c.nodes[2].engine.AppliedIndex() >= newIndex }, "ordered restore after actual Apply")
	require.Positive(t, c.fsms[2].restores.Load())
	require.Equal(t, [][]byte{[]byte("earlier-held-data"), []byte("snapshot-newer-data")}, c.fsms[2].Applied())
}
