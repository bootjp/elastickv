package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestPipelineDebtNativeBatchThousand(t *testing.T) {
	c := newNativeSnapshotCluster(t, "", "")
	e := c.nodes[0].engine
	before := c.propose(t, []byte("durable-baseline"))
	entries := make([]*raftpb.Entry, 1000)
	for i := range entries {
		entries[i] = &raftpb.Entry{Data: encodeProposalEnvelope(uint64(i+100), []byte("native-small"))}
	}
	// Genuine native gRPC proposal batch, not manufactured append/storage ACKs.
	require.NoError(t, c.nodes[1].transport.Dispatch(t.Context(), raftpb.Message{
		Type: messageTypePtr(raftpb.MsgProp), From: uint64Ptr(2), To: uint64Ptr(1), Entries: entries,
	}))
	c.pumpUntil(t, func() bool {
		require.NoError(t, e.Err(), "one 1000-entry Ready must not exhaust a per-entry work quota")
		return e.AppliedIndex() >= before+uint64(len(entries))
	}, "actual three-voter WAL/quorum/FSM batch completion")
	actual := c.fsms[0].Applied()
	require.Len(t, actual, 1001)
	for _, payload := range actual[1:] {
		require.Equal(t, []byte("native-small"), payload)
	}
}
