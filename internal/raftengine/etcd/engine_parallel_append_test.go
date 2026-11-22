package etcd

import (
	"bytes"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestEngineParallelAppendOverlapsSaveWithoutEarlyAck(t *testing.T) {
	payload := []byte("parallel-append-held-save")
	gate := &asyncStorageSaveGate{payload: payload, entered: make(chan uint64, 1), release: make(chan struct{}), saved: make(chan struct{})}
	fsm := &asyncStorageFSM{}
	h := newNativeStorageRuntime(t, fsm, gate, newRawNode, nil)
	asyncStorageAwait(t, h, h.engine.startedCh, "startup")
	require.NoError(t, h.engine.handleTransportMessage(h.ctx, raftpb.Message{Type: messageTypePtr(raftpb.MsgTimeoutNow), From: uint64Ptr(2), To: uint64Ptr(1), Term: uint64Ptr(1)}))
	asyncStorageAwait(t, h, h.engine.leaderReady, "leader")
	first := asyncStorageAwait(t, h, h.propose([]byte("before")), "baseline")
	require.NoError(t, first.err)
	before := h.engine.AppliedIndex()
	done := h.propose(payload)
	index := asyncStorageAwait(t, h, gate.entered, "held local Save")
	// asyncStorageAwait may already have delivered the append while waiting for
	// Save entry. Otherwise drain the actual dispatch channel while Save is held.
	deadline := time.NewTimer(250 * time.Millisecond)
	defer deadline.Stop()
	for {
		last, err := h.storage.LastIndex()
		require.NoError(t, err)
		if last >= index {
			break
		}
		select {
		case msg := <-h.outbound:
			h.deliver(t, msg)
		case <-deadline.C:
			t.Fatal("follower append was serialized behind the leader WAL Save")
		}
	}
	require.Empty(t, done, "follower ACK cannot bypass local WAL Save")
	require.Empty(t, gate.saved)
	require.Equal(t, before, h.engine.AppliedIndex())
	require.Equal(t, [][]byte{[]byte("before")}, fsm.Applied())
	gate.unblock()
	result := asyncStorageAwait(t, h, done, "durable proposal")
	require.NoError(t, result.err)
	require.Equal(t, index, result.result.CommitIndex)
	require.True(t, bytes.Equal(payload, fsm.Applied()[1]))
	require.Len(t, fsm.Applied(), 2, "each entry must apply once")
}

type parallelAppendFailStorage struct {
	etcdstorage.Storage
	payload []byte
	failure error
}

func (s *parallelAppendFailStorage) Save(state *raftpb.HardState, entries []*raftpb.Entry) error {
	for _, entry := range entries {
		_, payload, ok := decodeProposalEnvelope(entry.GetData())
		if ok && bytes.Equal(payload, s.payload) {
			return s.failure
		}
	}
	return s.Storage.Save(state, entries)
}

func TestEngineParallelAppendSaveFailureCannotAck(t *testing.T) {
	payload := []byte("parallel-append-failed-save")
	failure := errors.New("injected durable Save failure")
	gate := &asyncStorageSaveGate{payload: payload, entered: make(chan uint64, 1), release: make(chan struct{}), expectedCloseError: failure}
	fsm := &asyncStorageFSM{}
	h := newNativeStorageRuntime(t, fsm, gate, newRawNode, func(e *Engine) {
		gate.Storage = &parallelAppendFailStorage{Storage: gate.Storage, payload: payload, failure: failure}
	})
	asyncStorageAwait(t, h, h.engine.startedCh, "startup")
	require.NoError(t, h.engine.handleTransportMessage(h.ctx, raftpb.Message{Type: messageTypePtr(raftpb.MsgTimeoutNow), From: uint64Ptr(2), To: uint64Ptr(1), Term: uint64Ptr(1)}))
	asyncStorageAwait(t, h, h.engine.leaderReady, "leader")
	first := asyncStorageAwait(t, h, h.propose([]byte("before")), "baseline")
	require.NoError(t, first.err)
	before := h.engine.AppliedIndex()
	done := h.propose(payload)
	asyncStorageAwait(t, h, gate.entered, "held failing Save")
	gate.unblock()
	select {
	case result := <-done:
		require.Error(t, result.err)
		require.Nil(t, result.result)
	case <-time.After(5 * time.Second):
		t.Fatal("failed Save did not terminate caller")
	}
	select {
	case <-h.engine.Done():
		require.ErrorIs(t, h.engine.Err(), failure)
	case <-time.After(5 * time.Second):
		t.Fatal("failed Save did not halt the engine")
	}
	require.Equal(t, before, h.engine.AppliedIndex())
	require.Equal(t, [][]byte{[]byte("before")}, fsm.Applied())
}

func parallelAppendTestEngine(t *testing.T) *Engine {
	t.Helper()
	storage := etcdraft.NewMemoryStorage()
	snap := raftTestSnapshot(1, 1, []uint64{1}, nil)
	require.NoError(t, storage.ApplySnapshot(&snap))
	raw := mustRawNode(t, storage, 1)
	require.NoError(t, raw.Campaign())
	for raw.HasReady() {
		rd := raw.Ready()
		require.NoError(t, storage.Append(rd.Entries))
		if !etcdraft.IsEmptyHardState(rd.HardState) {
			require.NoError(t, storage.SetHardState(rd.HardState))
		}
		raw.Advance(rd)
	}
	require.Equal(t, etcdraft.StateLeader, raw.BasicStatus().RaftState)
	return &Engine{nodeID: 1, rawNode: raw}
}
func TestEngineParallelAppendDurabilityEligibility(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Engine, *etcdraft.Ready)
	}{
		{"hard-state", func(_ *Engine, r *etcdraft.Ready) { r.HardState = &raftpb.HardState{Commit: uint64Ptr(2)} }},
		{"snapshot", func(_ *Engine, r *etcdraft.Ready) { s := raftTestSnapshot(2, 1, []uint64{1}, nil); r.Snapshot = &s }},
		{"committed", func(_ *Engine, r *etcdraft.Ready) { r.CommittedEntries = r.Entries }},
		{"membership", func(_ *Engine, r *etcdraft.Ready) { r.Entries[0].Type = entryTypePtr(raftpb.EntryConfChange) }},
		{"no-entries", func(_ *Engine, r *etcdraft.Ready) { r.Entries = nil }},
		{"native-admission", func(e *Engine, _ *etcdraft.Ready) { e.nativeAdmission = fixtureNativeAdmission{} }},
		{"protected-callback", func(e *Engine, _ *etcdraft.Ready) { e.admissionChanged = func() {} }},
		{"protected-recovery", func(e *Engine, _ *etcdraft.Ready) { e.recoveredAdmission = func(RecoveredState) error { return nil } }},
		{"pipeline", func(e *Engine, _ *etcdraft.Ready) { e.storagePipeline = &engineStoragePipeline{} }},
		{"follower", func(e *Engine, _ *etcdraft.Ready) {
			require.NoError(t, e.rawNode.Step(&raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), From: uint64Ptr(2), Term: uint64Ptr(99)}))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := parallelAppendTestEngine(t)
			r := etcdraft.Ready{Entries: []*raftpb.Entry{{Type: entryTypePtr(raftpb.EntryNormal), Index: uint64Ptr(2)}}}
			require.True(t, e.canDispatchParallelAppend(r))
			tc.change(e, &r)
			require.False(t, e.canDispatchParallelAppend(r))
		})
	}
}

func TestEngineParallelAppendDefersResponsesAndCopiesPayload(t *testing.T) {
	e := parallelAppendTestEngine(t)
	queue := make(chan dispatchRequest, 4)
	e.transport = NewGRPCTransport(nil)
	e.peerDispatchers = map[uint64]*peerQueues{2: {normal: queue}}
	entry := &raftpb.Entry{Type: entryTypePtr(raftpb.EntryNormal), Index: uint64Ptr(2), Data: []byte("original")}
	appendMessage := &raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), To: uint64Ptr(2), Entries: []*raftpb.Entry{entry}}
	response := &raftpb.Message{Type: messageTypePtr(raftpb.MsgAppResp), To: uint64Ptr(2)}
	ready := etcdraft.Ready{Entries: []*raftpb.Entry{entry}, Messages: []*raftpb.Message{appendMessage, response}}
	deferred, err := e.dispatchParallelAppend(ready)
	require.NoError(t, err)
	require.Len(t, ready.Messages, 2, "Advance retains the original Ready")
	require.Equal(t, []*raftpb.Message{response}, deferred.Messages)
	request := <-queue
	require.Equal(t, raftpb.MsgApp, request.msg.GetType())
	entry.Data[0] = 'X'
	require.Equal(t, []byte("original"), request.msg.GetEntries()[0].GetData())
	require.NoError(t, request.Close())
	require.Empty(t, queue)
	require.NoError(t, e.sendReadyMessages(deferred))
	request = <-queue
	require.Equal(t, raftpb.MsgAppResp, request.msg.GetType())
	require.NoError(t, request.Close())
	require.Empty(t, queue, "early append cannot be dispatched a second time")
}
