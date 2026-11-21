package etcd

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func TestReadIndexQueueSharesOnlyTheNewQuorumRound(t *testing.T) {
	t.Parallel()
	nodes := map[uint64]*Engine{}
	for id := uint64(1); id <= 3; id++ {
		nodes[id] = identityElectionEngine(t, id)
	}
	leader := nodes[1]
	require.NoError(t, leader.rawNode.Campaign())
	for range 8 {
		for _, node := range nodes {
			require.NoError(t, node.drainReady())
			for target, queues := range node.peerDispatchers {
				for _, queue := range []chan dispatchRequest{queues.normal, queues.heartbeat, queues.readIndexResp} {
					for len(queue) > 0 {
						request := <-queue
						require.NoError(t, nodes[target].rawNode.Step(&request.msg))
						require.NoError(t, request.Close())
					}
				}
			}
		}
	}
	require.Equal(t, raftengine.StateLeader, leader.State())

	leader.readCh = make(chan readRequest, 2)
	cancelCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	cancelled := readRequest{ctx: cancelCtx, id: leader.nextID(), done: make(chan readResult, 1)}
	first := readRequest{ctx: t.Context(), id: leader.nextID(), done: make(chan readResult, 1), waitApplied: true}
	late := readRequest{ctx: t.Context(), id: leader.nextID(), done: make(chan readResult, 1), waitApplied: true}
	leader.readCh <- first
	leader.handleReadQueue(cancelled)
	cancel()
	leader.cancelPendingRead(cancelled.id)
	require.NoError(t, leader.drainReady())
	heartbeats := leader.peerDispatchers[2].heartbeat
	require.Len(t, heartbeats, 1)
	request := <-heartbeats
	require.Equal(t, raftpb.MsgHeartbeat, request.msg.GetType())
	require.NotEmpty(t, request.msg.GetContext())
	require.NoError(t, nodes[2].rawNode.Step(&request.msg))
	require.NoError(t, request.Close())
	require.NoError(t, nodes[2].drainReady())

	leader.handleReadQueue(late)
	require.NoError(t, leader.drainReady())
	require.Len(t, heartbeats, 1, "the later read must issue a separate quorum round")
	require.Empty(t, first.done)
	followerReplies := nodes[2].peerDispatchers[1].readIndexResp
	require.Len(t, followerReplies, 1)
	reply := <-followerReplies
	leader.handleStep(reply.msg)
	require.NoError(t, reply.Close())
	require.NoError(t, leader.drainReady())
	require.Len(t, first.done, 1)
	require.Equal(t, readResult{index: leader.applied}, <-first.done)
	require.Empty(t, cancelled.done)
	require.Empty(t, late.done)
}

func TestReadIndexBatchWaitsForAppliedIndexAndDropsCancelledMembers(t *testing.T) {
	t.Parallel()
	engine := &Engine{pendingReads: make(map[uint64]readRequest)}
	verified := readRequest{ctx: t.Context(), id: 1, done: make(chan readResult, 1)}
	read := readRequest{ctx: t.Context(), id: 2, done: make(chan readResult, 1), waitApplied: true}
	require.Equal(t, verified.id, engine.storeReadIndexBatch([]readRequest{verified, read}))
	engine.handleReadStates([]etcdraft.ReadState{{RequestCtx: encodeReadContext(verified.id), Index: 9}})
	require.Equal(t, readResult{index: 9}, <-verified.done)
	engine.applied = 8
	engine.resolveReadyReads()
	require.Empty(t, read.done)
	engine.applied = 9
	engine.resolveReadyReads()
	require.Equal(t, readResult{index: 9}, <-read.done)
	require.Empty(t, engine.pendingReadBatches)
	require.Empty(t, engine.pendingReads)

	require.Equal(t, verified.id, engine.storeReadIndexBatch([]readRequest{verified, read}))
	engine.cancelPendingRead(verified.id)
	engine.cancelPendingRead(read.id)
	require.Empty(t, engine.pendingReads)
	require.Empty(t, engine.pendingReadBatches)
}

func TestReadIndexBatchLosesPendingAuthorityOnLeadershipLoss(t *testing.T) {
	t.Parallel()
	engine := &Engine{pendingReads: make(map[uint64]readRequest)}
	first := readRequest{ctx: t.Context(), id: 1, done: make(chan readResult, 1)}
	second := readRequest{ctx: t.Context(), id: 2, done: make(chan readResult, 1), waitApplied: true}
	require.Equal(t, first.id, engine.storeReadIndexBatch([]readRequest{first, second}))
	engine.failPending(raftengine.ErrLeadershipLost)
	require.ErrorIs(t, (<-first.done).err, raftengine.ErrLeadershipLost)
	require.ErrorIs(t, (<-second.done).err, raftengine.ErrLeadershipLost)
	engine.handleReadStates([]etcdraft.ReadState{{RequestCtx: encodeReadContext(first.id), Index: 9}})
	require.Empty(t, engine.pendingReadBatches)
	require.Empty(t, engine.pendingReads)
	require.Empty(t, first.done)
	require.Empty(t, second.done)
}
