package etcd

import (
	"context"
	"reflect"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

type debtReadCluster struct {
	nodes        []*etcdraft.RawNode
	stores       []*etcdraft.MemoryStorage
	partition    bool
	owner        *Engine
	holdReadAcks bool
}

func newDebtReadCluster(t *testing.T) *debtReadCluster {
	t.Helper()
	c := &debtReadCluster{nodes: make([]*etcdraft.RawNode, 3), stores: make([]*etcdraft.MemoryStorage, 3), partition: true}
	for i := range c.nodes {
		c.stores[i] = etcdraft.NewMemoryStorage()
		require.NoError(t, c.stores[i].ApplySnapshot(&raftpb.Snapshot{Metadata: &raftpb.SnapshotMetadata{
			Index: uint64Ptr(1), Term: uint64Ptr(1), ConfState: &raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		}}))
		require.NoError(t, c.stores[i].SetHardState(&raftpb.HardState{Term: uint64Ptr(1), Commit: uint64Ptr(1)}))
		node, err := newStorageRawNode(OpenConfig{NodeID: uint64(i + 1), ElectionTick: 10, HeartbeatTick: 1,
			MaxInflightMsg: 16, MaxSizePerMsg: pipelineEntryRoom}, c.stores[i], 1)
		require.NoError(t, err)
		c.nodes[i] = node
	}
	return c
}

// This witness uses actual local append/apply responses after volatile
// MemoryStorage publication. It is not a disk-durability acceptance fixture.
func (c *debtReadCluster) pump(t *testing.T) {
	t.Helper()
	for turn := 0; turn < 100; turn++ {
		progress := false
		var messages []*raftpb.Message
		for i, node := range c.nodes {
			if node.HasReady() {
				progress = true
				messages = append(messages, c.ready(t, i)...)
			}
		}
		for _, message := range messages {
			if c.drop(message) {
				continue
			}
			require.NoError(t, c.nodes[message.GetTo()-1].Step(message))
		}
		if !progress {
			return
		}
	}
	t.Fatal("native read/election pump did not quiesce")
}

func (c *debtReadCluster) drop(message *raftpb.Message) bool {
	return (c.partition && (message.GetType() == raftpb.MsgApp || message.GetType() == raftpb.MsgHeartbeat || message.GetType() == raftpb.MsgHeartbeatResp)) ||
		(c.holdReadAcks && message.GetType() == raftpb.MsgHeartbeatResp)
}

func (c *debtReadCluster) ready(t *testing.T, i int) []*raftpb.Message {
	t.Helper()
	if i == 0 && c.owner != nil {
		c.owner.rebindPipelinePendingReads()
	}
	rd := c.nodes[i].Ready()
	var messages []*raftpb.Message
	for _, message := range rd.Messages {
		messages = append(messages, c.publish(t, i, message)...)
		if i == 0 && c.owner != nil {
			c.owner.retirePipelineNetworkRead(message)
		}
	}
	if i == 0 && c.owner != nil {
		c.owner.handlePipelineReadStates(rd.ReadStates)
		c.owner.storagePipeline.retireReadyReads()
	}
	return messages
}

func (c *debtReadCluster) publish(t *testing.T, i int, message *raftpb.Message) []*raftpb.Message {
	t.Helper()
	switch {
	case message.GetType() == raftpb.MsgStorageAppend:
		require.NoError(t, c.stores[i].Append(message.Entries))
		state := &raftpb.HardState{Term: message.Term, Vote: message.Vote, Commit: message.Commit}
		if !etcdraft.IsEmptyHardState(state) {
			require.NoError(t, c.stores[i].SetHardState(state))
		}
		return message.Responses
	case message.GetType() == raftpb.MsgStorageApply:
		for _, entry := range message.Entries {
			require.Empty(t, entry.Data, "only actual election no-ops are applied")
		}
		return message.Responses
	default:
		return []*raftpb.Message{message}
	}
}

func debtNativePendingReads(node *etcdraft.RawNode) int {
	return reflect.ValueOf(node).Elem().FieldByName("raft").Elem().FieldByName("pendingReadIndexMessages").Len()
}

func TestPipelineDebtReadOwnershipSurvivesNativeTerms(t *testing.T) {
	c := newDebtReadCluster(t)
	e := newPipelineAdmissionFixture(t)
	e.rawNode, e.storage, e.nodeID = c.nodes[0], c.stores[0], 1
	c.owner = e
	e.pendingReads = make(map[uint64]readRequest)
	t.Cleanup(func() { e.rawNode = nil; e.retirePipelineReadOwnership() })
	for round := 1; round <= 3; round++ {
		require.NoError(t, c.nodes[0].Campaign())
		c.pump(t)
		require.Equal(t, etcdraft.StateLeader, c.nodes[0].BasicStatus().RaftState)
		e.status.State = raftengine.StateLeader
		e.admitPipelineRead(readRequest{ctx: context.Background(), id: uint64(round), done: make(chan readResult, 1)})
		e.cancelPendingRead(uint64(round))
		remote := raftpb.Message{Type: messageTypePtr(raftpb.MsgReadIndex), From: uint64Ptr(2), To: uint64Ptr(1),
			Entries: []*raftpb.Entry{{Data: encodeReadContext(uint64(round + 100))}}}
		require.NoError(t, e.enqueuePipelineStep(t.Context(), remote))
		require.NoError(t, e.consumePipelineIngress(<-e.storagePipeline.priority))
		require.Equal(t, 2*round, debtNativePendingReads(c.nodes[0]))
		for tick := 0; tick < 10; tick++ {
			c.nodes[0].Tick()
		}
		c.pump(t)
		require.Equal(t, etcdraft.StateFollower, c.nodes[0].BasicStatus().RaftState)
		e.retirePipelineReadOwnership()
		_, work := e.storagePipeline.controlOwnership.usage()
		require.Equal(t, 2*round, work, "term reset must not retire native pending local/network reads")
		for _, node := range c.nodes[1:] {
			for tick := 0; tick < 10; tick++ {
				node.Tick()
			}
		}
		c.pump(t)
	}
	c.partition = false
	c.holdReadAcks = true
	require.NoError(t, c.nodes[0].Campaign())
	c.pump(t)
	require.Zero(t, debtNativePendingReads(c.nodes[0]), "real current-term commit must consume the upstream queue")
	e.retirePipelineReadOwnership()
	_, work := e.storagePipeline.controlOwnership.usage()
	require.Equal(t, 6, work, "transfer to current-term readOnly is not last-reference retirement")
	c.holdReadAcks = false
	c.nodes[0].Tick()
	c.pump(t)
	e.retirePipelineReadOwnership()
	requireStorageWorkUsage(t, e.storagePipeline.controlOwnership, 0, 0)
}
