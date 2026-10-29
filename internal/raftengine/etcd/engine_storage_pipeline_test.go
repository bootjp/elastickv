package etcd

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// This is an explicit experimental Engine path, not the default Open runtime.
// Configuration, snapshot and whole-engine accounting acceptance remain separate.
func newExplicitAsyncStorageRuntime(t *testing.T, fsm *asyncStorageFSM, gate *asyncStorageSaveGate) *asyncStorageRuntime {
	t.Helper()
	return newNativeStorageRuntime(t, fsm, gate, newStorageRawNode, func(e *Engine) {
		pipeline, err := newEngineStoragePipeline(gate)
		require.NoError(t, err)
		e.storagePipeline = pipeline
	})
}

func TestEngineExplicitAsyncStoragePipelineHeartbeatProgressWhileUserSaveBlocked(t *testing.T) {
	// Same nonce, two fresh ticks, safety assertions, budgets and real
	// post-release success, using the shared corrected v3.7 protocol witness.
	runEngineHeldUserSaveScenario(t, newExplicitAsyncStorageRuntime)
}

// These synthetic messages test only the witness classifier. The Engine tests
// below exchange genuine messages with the sole-owned native follower.
func TestAsyncStorageHeartbeatWitness(t *testing.T) {
	nonce := []byte("post-gate-nonce")
	heartbeat := func(ctx []byte) raftpb.Message {
		return raftpb.Message{Type: messageTypePtr(raftpb.MsgHeartbeat), Context: ctx}
	}
	response := func(data []byte) raftpb.Message {
		return raftpb.Message{
			Type: messageTypePtr(raftpb.MsgReadIndexResp), Entries: []*raftpb.Entry{{Data: data}},
		}
	}
	tick := heartbeat(nil)
	round := heartbeat([]byte{1, 0, 0, 0, 0, 0, 0, 0})
	confirmed := response(nonce)
	old := response([]byte("old-nonce"))
	cases := []struct {
		name      string
		messages  []raftpb.Message
		round     uint64
		confirmed bool
		periodic  int
	}{
		{
			name:     "prechallenge ticks and nonce cannot confirm",
			messages: []raftpb.Message{tick, tick, confirmed, tick, tick},
		},
		{
			name:     "nonce before round is not retroactive",
			messages: []raftpb.Message{confirmed, round, tick, tick}, round: 1,
		},
		{
			name:     "old nonce does not confirm",
			messages: []raftpb.Message{round, old, tick, tick}, round: 1,
		},
		{
			name: "response Context is not request Data",
			messages: []raftpb.Message{round, {
				Type: messageTypePtr(raftpb.MsgReadIndexResp), Context: nonce,
			}, tick, tick}, round: 1,
		},
		{
			name:     "empty request Data does not confirm",
			messages: []raftpb.Message{round, response(nil), tick, tick}, round: 1,
		},
		{
			name: "multiple request entries do not confirm",
			messages: []raftpb.Message{round, {
				Type:    messageTypePtr(raftpb.MsgReadIndexResp),
				Entries: []*raftpb.Entry{{Data: nonce}, {Data: nonce}},
			}, tick, tick}, round: 1,
		},
		{
			name: "wrong response type does not confirm",
			messages: []raftpb.Message{round, {
				Type: messageTypePtr(raftpb.MsgApp), Entries: []*raftpb.Entry{{Data: nonce}},
			}, tick, tick}, round: 1,
		},
		{
			name:     "literal nonce and zero counter are not native read rounds",
			messages: []raftpb.Message{heartbeat(nonce), heartbeat(make([]byte, 8)), confirmed, tick, tick},
		},
		{
			name:     "one postconfirmation tick is not two",
			messages: []raftpb.Message{round, confirmed, tick}, round: 1, confirmed: true, periodic: 1,
		},
		{
			name:     "only two ticks after round and matching nonce count",
			messages: []raftpb.Message{tick, tick, round, tick, old, tick, confirmed, tick, tick},
			round:    1, confirmed: true, periodic: 2,
		},
		{
			name:     "new response after round can confirm but earlier ticks cannot",
			messages: []raftpb.Message{confirmed, round, tick, confirmed, tick, tick},
			round:    1, confirmed: true, periodic: 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			progress := asyncStorageHeartbeatProgress{nonce: nonce}
			for _, msg := range tc.messages {
				progress.observe(msg)
			}
			require.Equal(t, tc.round, progress.round)
			require.Equal(t, tc.confirmed, progress.confirmed)
			require.Equal(t, tc.periodic, progress.periodic)
		})
	}
}

func establishExplicitAsyncStorageLeader(t *testing.T, h *asyncStorageRuntime, fsm *asyncStorageFSM,
	gate *asyncStorageSaveGate, baseline []byte,
) uint64 {
	t.Helper()
	asyncStorageAwait(t, h, h.engine.startedCh, "engine startup")
	require.NoError(t, h.engine.handleTransportMessage(h.ctx, raftpb.Message{
		Type: messageTypePtr(raftpb.MsgTimeoutNow), From: uint64Ptr(2), To: uint64Ptr(1), Term: uint64Ptr(1),
	}))
	asyncStorageAwait(t, h, h.engine.leaderReady, "leader election")
	first := asyncStorageAwait(t, h, h.propose(baseline), "durable baseline proposal")
	require.NoError(t, first.err)
	require.NotNil(t, first.result)
	require.Equal(t, string(baseline), first.result.Response)
	require.Equal(t, raftengine.StateLeader, h.engine.State())
	require.Equal(t, h.engine.Status().Term, gate.term.Load())
	require.Equal(t, uint64(1), gate.vote.Load())
	require.GreaterOrEqual(t, gate.commit.Load(), first.result.CommitIndex)
	require.GreaterOrEqual(t, gate.lastIndex.Load(), first.result.CommitIndex)
	require.Equal(t, first.result.CommitIndex, h.engine.AppliedIndex())
	require.Equal(t, [][]byte{baseline}, fsm.Applied())
	return first.result.CommitIndex
}

func requireAsyncStorageProposalPending(t *testing.T, done <-chan proposalResult) {
	t.Helper()
	select {
	case result := <-done:
		t.Fatalf("proposal returned before gated operation completed: %+v", result)
	default:
	}
}

func requireAsyncStorageSignalPending(t *testing.T, done <-chan struct{}, description string) {
	t.Helper()
	select {
	case <-done:
		t.Fatalf("%s completed before release", description)
	default:
	}
}

func requireExplicitAsyncStorageSuccess(t *testing.T, h *asyncStorageRuntime, fsm *asyncStorageFSM,
	done <-chan proposalResult, index uint64, baseline, payload []byte,
) {
	t.Helper()
	result := asyncStorageAwait(t, h, done, "real proposal after gate release")
	require.NoError(t, result.err)
	require.NotNil(t, result.result)
	require.Equal(t, index, result.result.CommitIndex)
	require.Equal(t, string(payload), result.result.Response)
	require.Equal(t, [][]byte{baseline, payload}, fsm.Applied())
	require.Equal(t, index, h.engine.AppliedIndex())
	t.Logf("released gate: real proposal response=%q commit=%d applied=%d", result.result.Response, result.result.CommitIndex, h.engine.AppliedIndex())
}

func TestEngineExplicitAsyncStoragePipelineHeartbeatProgressWhileUserSyncBlocked(t *testing.T) {
	baseline, payload := []byte("durable-leader-baseline"), []byte("sync-gated-user-entry")
	// Save is not held. Sync is armed only by the later user payload's real
	// Save; all election/no-op/baseline Save and Sync calls delegate unchanged.
	gate := &asyncStorageSaveGate{
		payload: payload, saved: make(chan struct{}),
		syncEntered: make(chan uint64, 1), syncRelease: make(chan struct{}), synced: make(chan struct{}),
	}
	fsm := &asyncStorageFSM{}
	h := newExplicitAsyncStorageRuntime(t, fsm, gate)
	beforeApplied := establishExplicitAsyncStorageLeader(t, h, fsm, gate, baseline)
	beforeFSMIndex, beforeDurableIndex := fsm.applyIndex.Load(), fsm.durableIndex.Load()
	done := h.propose(payload)
	index := asyncStorageAwait(t, h, gate.syncEntered, "later user-entry required Sync gate")
	require.Greater(t, index, beforeApplied)
	select {
	case <-gate.saved:
	default:
		t.Fatal("Sync gate entered before real user-entry Save returned")
	}
	assertBlocked := func() {
		t.Helper()
		requireAsyncStorageSignalPending(t, gate.synced, "required WAL Sync")
		requireAsyncStorageProposalPending(t, done)
		require.Equal(t, beforeApplied, h.engine.AppliedIndex(), "owner applied before required Sync")
		require.Equal(t, beforeFSMIndex, fsm.applyIndex.Load(), "FSM apply started before required Sync")
		require.Equal(t, beforeDurableIndex, fsm.durableIndex.Load(), "durable index advanced before required Sync")
		require.Equal(t, [][]byte{baseline}, fsm.Applied())
	}
	assertBlocked()
	h.verifyBlockedHeartbeats(t, assertBlocked)
	assertBlocked()
	gate.unblock()
	requireExplicitAsyncStorageSuccess(t, h, fsm, done, index, baseline, payload)
	select {
	case <-gate.synced:
	default:
		t.Fatal("proposal succeeded before required WAL Sync returned")
	}
}

func TestEngineExplicitAsyncStoragePipelineHeartbeatProgressWhileUserApplyBlocked(t *testing.T) {
	baseline, payload := []byte("durable-leader-baseline"), []byte("apply-gated-user-entry")
	gate := &asyncStorageSaveGate{}
	fsm := &asyncStorageFSM{
		payload: payload, applyEntered: make(chan uint64, 1),
		applyRelease: make(chan struct{}), applyReturned: make(chan struct{}),
	}
	h := newExplicitAsyncStorageRuntime(t, fsm, gate)
	beforeApplied := establishExplicitAsyncStorageLeader(t, h, fsm, gate, baseline)
	beforeDurableIndex := fsm.durableIndex.Load()
	done := h.propose(payload)
	index := asyncStorageAwait(t, h, fsm.applyEntered, "later user-entry actual FSM Apply gate")
	require.Greater(t, index, beforeApplied)
	assertBlocked := func() {
		t.Helper()
		requireAsyncStorageSignalPending(t, fsm.applyReturned, "actual FSM Apply")
		requireAsyncStorageProposalPending(t, done)
		require.Equal(t, beforeApplied, h.engine.AppliedIndex(), "owner published an unfinished Apply")
		// SetApplyIndex is a pending execution input and may already equal
		// index. It is not the owner's completed/applied publication.
		require.Equal(t, beforeDurableIndex, fsm.durableIndex.Load())
		require.Equal(t, [][]byte{baseline}, fsm.Applied())
	}
	assertBlocked()
	h.verifyBlockedHeartbeats(t, assertBlocked)
	assertBlocked()
	fsm.unblock()
	requireExplicitAsyncStorageSuccess(t, h, fsm, done, index, baseline, payload)
	select {
	case <-fsm.applyReturned:
	default:
		t.Fatal("proposal succeeded before actual FSM Apply returned")
	}
}

func requireExplicitAsyncStorageFailedProposal(t *testing.T, done <-chan proposalResult) {
	t.Helper()
	select {
	case result := <-done:
		require.Error(t, result.err, "quiescing must not manufacture a successful proposal receipt")
		require.Nil(t, result.result)
	case <-time.After(time.Second):
		t.Fatal("proposal caller did not join after shutdown")
	}
}

func TestEngineExplicitAsyncStoragePipelineCloseWhileUserSaveBlocked(t *testing.T) {
	baseline, payload := []byte("durable-leader-baseline"), []byte("close-gated-user-entry")
	gate := &asyncStorageSaveGate{
		payload: payload, entered: make(chan uint64, 1),
		release: make(chan struct{}), saved: make(chan struct{}),
	}
	fsm := &asyncStorageFSM{}
	h := newExplicitAsyncStorageRuntime(t, fsm, gate)
	beforeApplied := establishExplicitAsyncStorageLeader(t, h, fsm, gate, baseline)
	require.False(t, h.engine.LastQuorumAck().IsZero(), "native follower must establish the lease anchor")
	var invalidations atomic.Int32
	invalidated := make(chan struct{})
	unregister := h.engine.RegisterLeaderLossCallback(func() {
		if invalidations.Add(1) == 1 {
			close(invalidated)
		}
	})
	t.Cleanup(unregister)
	done := h.propose(payload)
	index := asyncStorageAwait(t, h, gate.entered, "later user-entry Save gate")
	require.Greater(t, index, beforeApplied)
	closeDone := make(chan error, 1)
	h.proposals.Add(1)
	go func() {
		defer h.proposals.Done()
		closeDone <- h.engine.Close()
	}()
	// No disk release is needed to revoke authority. The fixture cleanup
	// releases every gate and joins both this Close and all proposal callers.
	testutil.Closed(t, invalidated, time.Second, "Close did not revoke the lease while Save was blocked")
	require.Equal(t, raftengine.StateShutdown, h.engine.State())
	require.False(t, h.engine.isLeader.Load())
	require.True(t, h.engine.LastQuorumAck().IsZero())
	require.Equal(t, int32(1), invalidations.Load())
	require.False(t, gate.closeStarted.Load(), "storage closed underneath the executing real Save")
	require.False(t, gate.closeFinished.Load())
	requireAsyncStorageSignalPending(t, gate.saved, "real WAL Save")
	requireAsyncStorageSignalPending(t, h.engine.Done(), "Engine shutdown")
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before real Save completed: %v", err)
	default:
	}
	require.Equal(t, beforeApplied, h.engine.AppliedIndex())
	require.Equal(t, [][]byte{baseline}, fsm.Applied())
	var early *proposalResult
	select {
	case result := <-done:
		require.Error(t, result.err, "shutdown fabricated a client success while Save was held")
		require.Nil(t, result.result)
		early = &result
	default:
	}
	gate.unblock()
	select {
	case err := <-closeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Close did not join after real Save returned")
	}
	testutil.Closed(t, gate.saved, time.Second, "real Save never completed")
	testutil.Closed(t, h.engine.Done(), time.Second, "Engine owner did not exit")
	testutil.Closed(t, h.engine.storagePipeline.disk.done, time.Second, "disk actor was not joined")
	testutil.Closed(t, h.engine.storagePipeline.apply.done, time.Second, "apply actor was not joined")
	require.True(t, gate.closeStarted.Load())
	require.True(t, gate.closeFinished.Load())
	require.False(t, gate.closeDuringSave.Load(), "storage Close raced the real WAL Save")
	if early == nil {
		requireExplicitAsyncStorageFailedProposal(t, done)
	}
	require.Equal(t, int32(1), invalidations.Load())
	t.Log("shutdown joined disk/apply actors and client caller after real Save; no fabricated client success")
}
