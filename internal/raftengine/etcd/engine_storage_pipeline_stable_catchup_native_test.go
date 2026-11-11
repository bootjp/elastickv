package etcd

import (
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func newDebtRecoveredCatchupNodes(t *testing.T, count int) ([]*etcdraft.RawNode, []*etcdraft.MemoryStorage) {
	t.Helper()
	nodes := make([]*etcdraft.RawNode, 3)
	stores := make([]*etcdraft.MemoryStorage, 3)
	for i := range nodes {
		stores[i] = etcdraft.NewMemoryStorage()
		require.NoError(t, stores[i].ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
			Index: uint64Ptr(1), Term: uint64Ptr(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		}}))
		applied := uint64(1)
		if i == 0 {
			entries := make([]*raftpb.Entry, count)
			for j := range entries {
				entries[j] = ledgerEntry(uint64(j+2), 1, "")
				entries[j].Data = encodeProposalEnvelope(uint64(j+100), []byte("x"))
			}
			require.NoError(t, stores[i].Append(entries))
			applied += uint64(count)
		}
		require.NoError(t, stores[i].SetHardState(&raftpb.HardState{Term: uint64Ptr(1), Commit: uint64Ptr(applied)}))
		node, err := newStorageRawNode(OpenConfig{NodeID: uint64(i + 1), ElectionTick: 10, HeartbeatTick: 1,
			MaxInflightMsg: 1024, MaxSizePerMsg: 1 << 20}, stores[i], applied)
		require.NoError(t, err)
		nodes[i] = node
	}
	return nodes, stores
}

// Volatile recovered-state shape witness, not a WAL durability test. Apply
// responses follow actual FSM Apply and append responses follow MemoryStorage.
func TestPipelineDebtRecoveredStableCatchupFitsReady(t *testing.T) {
	const count = 600000
	nodes, stores := newDebtRecoveredCatchupNodes(t, count)
	e := newPipelineAdmissionFixture(t)
	e.rawNode, e.storage = nodes[0], stores[0]
	p := e.storagePipeline
	t.Cleanup(func() {
		e.rawNode = nil
		p.failedReady = nil
		p.failedReadyPermit.Release()
		retirePipelineDeferred(&p.diskDeferred, &p.diskOutstanding)
		retirePipelineDeferred(&p.applyDeferred, &p.applyOutstanding)
		p.ledger.discard()
	})
	require.NoError(t, nodes[0].Campaign())
	var queue []*raftpb.Message
	machines := make([]testStateMachine, len(nodes))
	for turns := 0; turns < 100; turns++ {
		for i, node := range nodes {
			if !node.HasReady() {
				continue
			}
			rd := node.Ready()
			for _, message := range rd.Messages {
				queue = append(queue, publishDebtCatchupMessage(t, stores[i], &machines[i], message)...)
			}
		}
		require.NotEmpty(t, queue, "native catch-up must reach an actual acknowledgement")
		batch := queue
		queue = nil
		for _, message := range batch {
			if isDebtCatchupAck(message) {
				require.NoError(t, e.enqueuePipelineStep(t.Context(), *message))
				err := e.consumePipelineIngress(<-p.priority)
				if rd := p.failedReady; rd != nil {
					t.Logf("commands=%d actualAck=%d ReadyMessages=%d cappedBill=%d R=%d", count, message.GetIndex(), len(rd.Messages), pipelineReadyBytes(*rd), pipelineReadyOvershootBytes)
				}
				require.NoError(t, err, "an admitted genuine catch-up ACK must not kill the pipeline")
				return
			}
			require.NoError(t, nodes[message.GetTo()-1].Step(message))
		}
	}
	t.Fatal("bounded native catch-up pump did not reach acknowledgement")
}

func isDebtCatchupAck(message *raftpb.Message) bool {
	return message.GetType() == raftpb.MsgAppResp && message.GetTo() == 1 && message.GetFrom() != 1 &&
		!etcdraft.IsLocalMsgTarget(message.GetFrom()) && !message.GetReject() && message.GetIndex() > 1
}

func publishDebtCatchupMessage(t *testing.T, storage *etcdraft.MemoryStorage, fsm *testStateMachine, message *raftpb.Message) []*raftpb.Message {
	t.Helper()
	switch {
	case message.GetType() == raftpb.MsgStorageAppend:
		require.NoError(t, storage.Append(message.Entries))
		state := &raftpb.HardState{Term: message.Term, Vote: message.Vote, Commit: message.Commit}
		if !etcdraft.IsEmptyHardState(state) {
			require.NoError(t, storage.SetHardState(state))
		}
		return message.Responses
	case message.GetType() == raftpb.MsgStorageApply:
		for _, entry := range message.Entries {
			if len(entry.Data) != 0 {
				_, data, ok := decodeProposalEnvelope(entry.Data)
				require.True(t, ok)
				fsm.Apply(data)
			}
		}
		return message.Responses
	default:
		return []*raftpb.Message{message}
	}
}
