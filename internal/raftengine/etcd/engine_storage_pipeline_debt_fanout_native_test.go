package etcd

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type debtBoundVolatileNode struct {
	node    *etcdraft.RawNode
	storage *etcdraft.MemoryStorage
}

// This is an upstream allocation/Ready witness with volatile storage, not a
// disk durability or gRPC acceptance claim. All protocol responses come from
// actual RawNodes; local opaque responses follow real MemoryStorage writes.
func newDebtBoundVolatileCluster(t *testing.T, voters, learners int) []*debtBoundVolatileNode {
	return newDebtBoundVolatileClusterWithConfig(t, voters, learners, nil)
}

func newDebtBoundVolatileClusterWithConfig(t *testing.T, voters, learners int,
	configure func(int, *OpenConfig, *etcdraft.MemoryStorage, *raftpb.Snapshot),
) []*debtBoundVolatileNode {
	t.Helper()
	members := voters + learners
	conf := &raftpb.ConfState{}
	peers := make([]Peer, members)
	for i := range peers {
		id := uint64(i + 1)
		peers[i] = Peer{NodeID: id, ID: "node-" + strconv.Itoa(i+1), Address: "127.0.0.1:" + strconv.Itoa(21000+i)}
		if i < voters {
			conf.Voters = append(conf.Voters, id)
		} else {
			conf.Learners = append(conf.Learners, id)
			peers[i].Suffrage = SuffrageLearner
		}
	}
	_, _, err := normalizePeerList(peers[0], peers, false)
	require.NoError(t, err)
	require.NoError(t, validateConfState(*conf, peers))
	pool := make([]*debtBoundVolatileNode, members)
	for i := range pool {
		storage := etcdraft.NewMemoryStorage()
		selected := &raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
			Index: uint64Ptr(1), Term: uint64Ptr(1), ConfState: conf,
		}}
		require.NoError(t, storage.ApplySnapshot(selected))
		require.NoError(t, storage.SetHardState(&raftpb.HardState{Term: uint64Ptr(1), Commit: uint64Ptr(1)}))
		cfg := OpenConfig{NodeID: uint64(i + 1), ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsg: 16, MaxSizePerMsg: pipelineEntryRoom}
		if configure != nil {
			configure(i, &cfg, storage, selected)
		}
		node, err := newStorageRawNode(cfg, storage, 1)
		require.NoError(t, err)
		pool[i] = &debtBoundVolatileNode{node: node, storage: storage}
	}
	require.NoError(t, pool[0].node.Campaign())
	for turn := 0; turn < 4096; turn++ {
		progress := false
		for _, member := range pool {
			if !member.node.HasReady() {
				continue
			}
			progress = true
			foldDebtBoundVolatileReady(t, pool, member)
		}
		if !progress {
			return pool
		}
	}
	t.Fatal("bounded upstream boot/election pump did not quiesce")
	return nil
}

func foldDebtBoundVolatileReady(t *testing.T, pool []*debtBoundVolatileNode, member *debtBoundVolatileNode) {
	t.Helper()
	rd := member.node.Ready()
	for _, message := range rd.Messages {
		switch {
		case message.GetType() == raftpb.MsgStorageAppend:
			require.NoError(t, member.storage.Append(message.Entries))
			state := &raftpb.HardState{Term: message.Term, Vote: message.Vote, Commit: message.Commit}
			if !etcdraft.IsEmptyHardState(state) {
				require.NoError(t, member.storage.SetHardState(state))
			}
			for _, response := range message.Responses {
				require.NoError(t, pool[response.GetTo()-1].node.Step(response))
			}
		case message.GetType() == raftpb.MsgStorageApply:
			for _, entry := range message.Entries {
				require.Empty(t, entry.Data, "only the real initial no-op is applied by this volatile witness")
			}
			for _, response := range message.Responses {
				require.NoError(t, member.node.Step(response))
			}
		default:
			require.NoError(t, pool[message.GetTo()-1].node.Step(message))
		}
	}
}

func TestPipelineDebtReadyAllowanceFanout(t *testing.T) {
	entries := make([]*raftpb.Entry, 65536)
	for i := range entries {
		entries[i] = &raftpb.Entry{Data: encodeProposalEnvelope(uint64(i+100), []byte("x"))}
	}
	requireDebtBoundFanout(t, 1, 40, entries)
}

func TestPipelineDebtReadyAllowanceThreeVoterFanout(t *testing.T) {
	entries := make([]*raftpb.Entry, 40000)
	for i := range entries {
		entries[i] = &raftpb.Entry{Data: encodeProposalEnvelope(uint64(i+100), []byte("x"))}
	}
	requireDebtBoundFanout(t, 3, 57, entries)
}

func TestPipelineDebtReadyAllowanceLargeFanout(t *testing.T) {
	entries := []*raftpb.Entry{{Data: encodeProposalEnvelope(100, bytes.Repeat([]byte{0xa5}, asyncNativeLargeValueBytes))}}
	requireDebtBoundFanout(t, 1, 40, entries)
}

func requireDebtBoundFanout(t *testing.T, voters, learners int, entries []*raftpb.Entry) {
	t.Helper()
	pool := newDebtBoundVolatileCluster(t, voters, learners)
	require.Equal(t, etcdraft.StateLeader, pool[0].node.BasicStatus().RaftState)
	e := newPipelineAdmissionFixture(t)
	e.rawNode, e.storage, e.nodeID = pool[0].node, pool[0].storage, 1
	p := e.storagePipeline
	// Exercise the real dispatch clone/enqueue boundary, but deliberately hold
	// every consumer. No remote delivery/ACK is invented by this shape witness.
	transport := NewGRPCTransport(nil)
	e.transport = transport
	e.peerDispatchers = make(map[uint64]*peerQueues)
	for i := 1; i < len(pool); i++ {
		e.peerDispatchers[uint64(i+1)] = &peerQueues{normal: make(chan dispatchRequest, 1)}
	}
	t.Cleanup(func() {
		for _, queues := range e.peerDispatchers {
			retireQueuedDispatchRequests(queues.normal)
		}
		require.NoError(t, transport.Close())
		e.rawNode = nil
		retirePipelineDeferred(&p.diskDeferred, &p.diskOutstanding)
		retirePipelineDeferred(&p.applyDeferred, &p.applyOutstanding)
		p.failedReady = nil
		p.failedReadyPermit.Release()
		p.ledger.discard()
	})
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgProp), From: uint64Ptr(1), To: uint64Ptr(1), Entries: entries}
	require.LessOrEqual(t, pipelineOwnedMessageBytes(&message), uint64(pipelineMaxOwnedInputBytes))
	require.NoError(t, e.enqueuePipelineStep(t.Context(), message))
	input := <-p.bulk
	err := e.consumePipelineIngress(input)
	if failed := p.failedReady; failed != nil {
		var remote uint64
		for _, emitted := range failed.Messages {
			if !etcdraft.IsLocalMsgTarget(emitted.GetTo()) {
				remote += pipelineMessageHeaders(emitted)
			}
		}
		t.Logf("admitted clone bill=%d U=%d remote Ready bill=%d R=%d messages=%d",
			pipelineCloneMessageBytes(&message), pipelineMaxOwnedInputBytes, remote, pipelineReadyOvershootBytes, len(failed.Messages))
	}
	require.NoError(t, err, "accepted input must route its genuine shared-backing Ready")
	requireStorageWorkUsage(t, p.readyBudget, 0, 0)
	queued := 0
	for _, queues := range e.peerDispatchers {
		queued += len(queues.normal)
		if len(queues.normal) != 0 {
			request := <-queues.normal
			require.NotSame(t, entries[0], request.msg.Entries[0])
			require.Equal(t, entries[0].Data, request.msg.Entries[0].Data)
			queues.normal <- request
		}
	}
	require.Positive(t, queued, "actual outbound clones must survive the Ready permit")
	bytes, work := p.outboundBulk.usage()
	require.Positive(t, bytes)
	require.Equal(t, queued, work)
	require.LessOrEqual(t, bytes, uint64(pipelineBulkBytes))
	for _, queues := range e.peerDispatchers {
		retireQueuedDispatchRequests(queues.normal)
	}
	requireStorageWorkUsage(t, p.outboundBulk, 0, 0)
	bytes, _ = p.bulkOwnership.usage()
	require.Positive(t, bytes, "outbound retirement must not retire local append/unstable owners")
}
