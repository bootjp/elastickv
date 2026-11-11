package etcd

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestEngineStoragePipelineRetainsRejectedReadyThroughActorJoin(t *testing.T) {
	payload := []byte("ready-held-user-entry")
	gate := &asyncStorageSaveGate{
		payload: payload, entered: make(chan uint64, 1), release: make(chan struct{}),
	}
	peers := []Peer{
		{NodeID: 1, ID: "n1", Address: "127.0.0.1:7011"},
		{NodeID: 2, ID: "n2", Address: "127.0.0.1:7012"},
	}
	prepared, err := prepareOpenState(OpenConfig{
		NodeID: 2, LocalID: "n2", DataDir: t.TempDir(), Peers: peers,
		Bootstrap: true, StateMachine: &asyncStorageFSM{},
	})
	require.NoError(t, err)
	gate.Storage = prepared.disk.Persist
	// Install cleanup before constructing actors or emitting local work.
	var e *Engine
	running := false
	t.Cleanup(func() {
		gate.unblock()
		if e == nil || e.storagePipeline == nil {
			require.NoError(t, gate.Close())
			return
		}
		if running {
			testutil.Closed(t, e.doneCh, time.Second, "owner did not join")
			return
		}
		p := e.storagePipeline
		p.disk.quiesce()
		p.apply.quiesce()
		for _, actor := range []*storageWorker{p.disk, p.apply} {
			for completion := range actor.completions {
				completion.Release()
				task, ok := completion.token.(*pipelineTask)
				if ok && task != nil {
					task.retire()
				} else {
					t.Error("actor completion lost its accepted pipeline task")
				}
			}
			<-actor.done
		}
		e.rawNode = nil
		p.ledger.discard()
		require.NoError(t, gate.Close())
	})
	applied := coldStartApplied(prepared.disk)
	rawNode, err := newStorageRawNode(prepared.cfg, prepared.disk.Storage, applied)
	require.NoError(t, err)
	e = &Engine{
		nodeID: 2, localID: "n2", rawNode: rawNode, storage: prepared.disk.Storage,
		persist: gate, fsm: prepared.cfg.StateMachine, tickInterval: prepared.cfg.TickInterval,
		applied: applied, closeCh: make(chan struct{}), doneCh: make(chan struct{}),
	}
	e.storagePipeline, err = newEngineStoragePipeline(gate)
	require.NoError(t, err)
	p := e.storagePipeline
	// This is a real upstream Ready and real WAL Save, not a synthetic token.
	require.NoError(t, e.rawNode.Step(&raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2),
		Term: uint64Ptr(2), Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Commit: uint64Ptr(1),
		Entries: []*raftpb.Entry{{
			Type: entryTypePtr(raftpb.EntryNormal), Index: uint64Ptr(2), Term: uint64Ptr(2),
			Data: encodeProposalEnvelope(7, payload),
		}},
	}))
	require.NoError(t, e.foldPipelineReady())
	require.NoError(t, p.flush())
	select {
	case index := <-gate.entered:
		require.Equal(t, uint64(2), index)
	case <-time.After(time.Second):
		t.Fatal("actual WAL actor did not reach Save")
	}
	// Only the owner changes routing headroom. The disk actor is still held
	// inside the earlier append, and a higher-term native Ready is now issued.
	p.readyBudget = newStorageWorkBudget(1, 1)
	require.NoError(t, e.rawNode.Step(&raftpb.Message{
		Type: messageTypePtr(raftpb.MsgHeartbeat), From: uint64Ptr(1), To: uint64Ptr(2),
		Term: uint64Ptr(3), Commit: uint64Ptr(1),
	}))
	require.True(t, e.rawNode.HasReady())
	err = e.foldPipelineReady()
	require.ErrorIs(t, err, errStoragePipelineResource)
	require.NotNil(t, p.failedReady, "accepted Ready must remain owned even without a routing permit")
	require.NotEmpty(t, p.failedReady.Messages, "opaque local response ownership was discarded")
	require.Nil(t, p.failedReadyPermit, "reservation rejection cannot fabricate a permit")
	require.False(t, e.rawNode.HasReady(), "Ready was accepted, not left upstream for retry")
	requireStorageWorkUsage(t, p.readyBudget, 0, 0)
	retainedReady, retainedNode := p.failedReady, e.rawNode
	e.beginPipelineShutdown(err)
	// Execute the real owner failure turn before transferring ownership to
	// run. This proves quiescence was reached with Save still held, without a
	// scheduling-dependent assertion immediately after launching a goroutine.
	again, turnError := e.preparePipelineTurn()
	require.NoError(t, turnError)
	require.False(t, again)
	require.True(t, p.actorsQuiesced)
	require.Same(t, retainedReady, p.failedReady, "quiescing must retain the exact accepted Ready")
	require.Same(t, retainedNode, e.rawNode)
	require.True(t, gate.saveActive.Load())
	requireAsyncStorageSignalPending(t, p.disk.done, "quiescing disk actor with actual Save held")
	require.False(t, gate.closeStarted.Load())
	running = true
	go e.run()
	requireAsyncStorageSignalPending(t, e.Done(), "shutdown while real Save is held")
	gate.unblock()
	testutil.Closed(t, e.Done(), time.Second, "shutdown did not pump the actual disk completion")
	require.ErrorIs(t, e.Err(), errStoragePipelineResource)
	require.Nil(t, e.rawNode)
	require.Nil(t, p.failedReady)
	require.True(t, gate.closeFinished.Load())
	require.False(t, gate.closeDuringSave.Load())
	requireStorageWorkUsage(t, p.diskBulk, 0, 0)
	requireStorageWorkUsage(t, p.diskQueueBudget, 0, 0)
	requireStorageWorkUsage(t, p.ledger.budget, 0, 0)
	testutil.Closed(t, p.disk.done, time.Second, "disk actor not joined")
	testutil.Closed(t, p.apply.done, time.Second, "apply actor not joined")
}
