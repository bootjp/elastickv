package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func newPipelineAdmissionFixture(t *testing.T) *Engine {
	t.Helper()
	pipeline, err := newEngineStoragePipeline(&recordingPersistStorage{})
	require.NoError(t, err)
	e := &Engine{nodeID: 1, storagePipeline: pipeline, closeCh: make(chan struct{}), doneCh: make(chan struct{})}
	t.Cleanup(func() {
		pipeline.disk.quiesce()
		pipeline.apply.quiesce()
		<-pipeline.disk.done
		<-pipeline.apply.done
		pipeline.retireIngress()
	})
	return e
}

func TestEngineStoragePipelineIngressRejectsForgedLocalResponses(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	for _, kind := range []raftpb.MessageType{raftpb.MsgStorageAppend, raftpb.MsgStorageAppendResp, raftpb.MsgStorageApply, raftpb.MsgStorageApplyResp} {
		err := e.enqueueStep(context.Background(), raftpb.Message{
			Type: messageTypePtr(kind), From: uint64Ptr(2), To: uint64Ptr(1),
			Term: uint64Ptr(2), Index: uint64Ptr(100), LogTerm: uint64Ptr(2),
		})
		require.ErrorIs(t, err, etcdraft.ErrStepLocalMsg)
	}
	for _, local := range []uint64{etcdraft.LocalAppendThread, etcdraft.LocalApplyThread} {
		require.ErrorIs(t, e.enqueueStep(context.Background(), raftpb.Message{
			Type: messageTypePtr(raftpb.MsgAppResp), From: uint64Ptr(local), To: uint64Ptr(1),
		}), etcdraft.ErrStepLocalMsg)
	}
	bytes, items := e.storagePipeline.priorityIngressBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
	require.Empty(t, e.storagePipeline.priority)
	require.Empty(t, e.storagePipeline.bulk)
}

func TestEngineStoragePipelineControlIngressSurvivesBulkPressure(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	p.bulkPaused.Store(true)
	require.ErrorIs(t, e.enqueueStep(context.Background(), raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(2), To: uint64Ptr(1),
	}), errStepQueueFull)
	for _, kind := range []raftpb.MessageType{raftpb.MsgHeartbeat, raftpb.MsgHeartbeatResp, raftpb.MsgAppResp, raftpb.MsgReadIndex} {
		message := raftpb.Message{Type: messageTypePtr(kind), From: uint64Ptr(2), To: uint64Ptr(1)}
		require.NoError(t, e.enqueueStep(context.Background(), message))
		input := <-p.priority
		require.Equal(t, kind, input.message.GetType())
		bytes, items := p.priorityIngressBudget.usage()
		require.Positive(t, bytes)
		require.Equal(t, 1, items)
		input.permit.Release()
	}
	bytes, items := p.priorityIngressBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
}

func TestEngineStoragePipelineProposalReservationPrecedesCopy(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	full, err := p.proposalBudget.tryAcquire(pipelineBulkBytes)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	result, err := e.Propose(context.Background(), []byte("not admitted"))
	require.ErrorIs(t, err, errStorageWorkBackpressure)
	require.Nil(t, result)
	require.Zero(t, e.nextRequestID.Load(), "rejected admission must precede request construction")
	full.Release()
	permit, err := e.reservePipelineProposal(64 << 20)
	require.NoError(t, err, "empty admission must fit a maximum-size command and copy peak")
	bytes, items := p.proposalBudget.usage()
	require.GreaterOrEqual(t, bytes, uint64(3*(64<<20)))
	require.Equal(t, 1, items)
	permit.Release()
	bytes, items = p.proposalBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
}

func TestEnginePendingProposalDoesNotRetainInputPayload(t *testing.T) {
	e := &Engine{pendingProposals: map[uint64]proposalRequest{}}
	request := proposalRequest{id: 7, payload: []byte("input"), done: make(chan proposalResult, 1)}
	e.storePendingProposal(request)
	stored, ok := e.popPendingProposal(request.id)
	require.True(t, ok)
	require.Nil(t, stored.payload)
	require.Equal(t, request.done, stored.done)
	require.Equal(t, []byte("input"), request.payload, "owner still encodes its original request copy")
}

func TestEngineStoragePipelineCloseStopsProposalAdmission(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	e.requestShutdown()
	result, err := e.Propose(context.Background(), []byte("not admitted after Close"))
	require.ErrorIs(t, err, errClosed)
	require.Nil(t, result)
	require.Zero(t, e.nextRequestID.Load())
	bytes, items := e.storagePipeline.proposalBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
}
