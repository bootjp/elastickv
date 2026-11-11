package etcd

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

// This fixture deliberately leaves the test goroutine as the sole RawNode
// owner. It complements the full run-loop fault tests with direct observation
// of the actual native task range and applied cursor, without production hooks.
func newOwnedPipelineApplyWitness(t *testing.T, fsm StateMachine) *Engine {
	t.Helper()
	prepared, err := prepareOpenState(OpenConfig{
		NodeID: 2, LocalID: "n2", DataDir: t.TempDir(), Bootstrap: true, StateMachine: fsm,
		Peers: []Peer{
			{NodeID: 1, ID: "n1", Address: "127.0.0.1:7011"},
			{NodeID: 2, ID: "n2", Address: "127.0.0.1:7012"},
		},
	})
	require.NoError(t, err)
	e := &Engine{persist: prepared.disk.Persist}
	t.Cleanup(func() {
		if p := e.storagePipeline; p != nil {
			p.stopActorsOnFailure()
			for _, actor := range []*storageWorker{p.disk, p.apply} {
				for completion := range actor.completions {
					completion.Release()
					task, ok := completion.token.(*pipelineTask)
					if ok && task != nil {
						task.retire()
					} else {
						t.Error("pipeline actor completion lost its task")
					}
				}
				<-actor.done
			}
			e.rawNode = nil
			p.failedReady = nil
			p.failedReadyPermit.Release()
			p.ledger.discard()
		}
		require.NoError(t, closePersist(e.persist))
	})
	e.nodeID, e.fsm, e.storage = 2, fsm, prepared.disk.Storage
	e.dataDir = prepared.cfg.DataDir
	e.fsmSnapDir = filepath.Join(e.dataDir, fsmSnapDirName)
	e.applied = coldStartApplied(prepared.disk)
	e.raftCutoverIndex = orInertCutover(nil)
	e.pendingProposals = map[uint64]proposalRequest{}
	e.rawNode, err = newStorageRawNode(prepared.cfg, e.storage, e.applied)
	require.NoError(t, err)
	e.storagePipeline, err = newEngineStoragePipeline(e.persist)
	require.NoError(t, err)
	e.storagePipeline.applyCursor = e.applied
	return e
}

func awaitOwnedPipelineCompletion(t *testing.T, worker *storageWorker) storageWorkCompletion {
	t.Helper()
	select {
	case completion, ok := <-worker.completions:
		require.True(t, ok, "actor exited without its actual completion")
		return completion
	case <-time.After(time.Second):
		t.Fatal("actual pipeline actor did not complete")
		return storageWorkCompletion{}
	}
}

func TestEngineStoragePipelineFailedNativeApplyDoesNotAcknowledgeWholeTask(t *testing.T) {
	for _, panicApply := range []bool{false, true} {
		name := "HaltApply"
		if panicApply {
			name = "panic"
		}
		t.Run(name, func(t *testing.T) {
			runOwnedPipelineApplyFailureWitness(t, panicApply)
		})
	}
}

func runOwnedPipelineApplyFailureWitness(t *testing.T, panicApply bool) {
	t.Helper()
	failure := errors.New("native middle-entry failure")
	fsm := newPipelineFailureFSM(nil)
	fsm.failPayload, fsm.failError, fsm.panicApply = []byte("bad"), failure, panicApply
	e := newOwnedPipelineApplyWitness(t, fsm)
	p := e.storagePipeline
	done := make([]chan proposalResult, 3)
	entries := make([]*raftpb.Entry, 3)
	for i, payload := range []string{"good", "bad", "never"} {
		id, index := uint64(i+1), uint64(i+2)
		done[i] = make(chan proposalResult, 1)
		e.storePendingProposal(proposalRequest{id: id, done: done[i]})
		entries[i] = &raftpb.Entry{
			Type: entryTypePtr(raftpb.EntryNormal), Index: uint64Ptr(index), Term: uint64Ptr(2),
			Data: encodeProposalEnvelope(id, []byte(payload)),
		}
	}
	require.NoError(t, e.rawNode.Step(&raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2),
		Term: uint64Ptr(2), Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Commit: uint64Ptr(4), Entries: entries,
	}))
	require.NoError(t, e.foldPipelineReady())
	require.NoError(t, p.flush())
	require.NoError(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, p.disk)))
	// The owner has folded the authentic append response, but has not yet
	// submitted the apply work. Inspect the exact native task, not a counter.
	require.Len(t, p.applyDeferred, 1)
	task := p.applyDeferred[0]
	require.Len(t, task.message.Entries, 3)
	for i, entry := range task.message.Entries {
		require.Equal(t, uint64(i+2), entry.GetIndex())
	}
	require.Len(t, task.message.Responses, 1)
	response := task.message.Responses[0]
	require.Equal(t, raftpb.MsgStorageApplyResp, response.GetType())
	// v3.7 apply responses carry the acknowledged range in Entries, not Index.
	require.Len(t, response.Entries, 3)
	require.Equal(t, uint64(4), response.Entries[2].GetIndex())
	require.Equal(t, uint64(1), e.rawNode.BasicStatus().Applied)
	require.NoError(t, p.flush())
	err := e.completePipelineApply(awaitOwnedPipelineCompletion(t, p.apply))
	want := failure
	if panicApply {
		want = errStorageWorkerPanic
	}
	require.ErrorIs(t, err, want)
	good := awaitPipelineFailureProposal(t, done[0])
	require.NoError(t, good.err)
	require.Equal(t, uint64(2), good.result.CommitIndex)
	require.Equal(t, "good", good.result.Response)
	requireAsyncStorageProposalPending(t, done[1])
	requireAsyncStorageProposalPending(t, done[2])
	require.Equal(t, []string{"good", "bad"}, fsm.attempted())
	require.Equal(t, uint64(2), e.AppliedIndex())
	require.Equal(t, uint64(1), e.rawNode.BasicStatus().Applied, "failed whole-task response must not reach RawNode")
	requireStorageWorkUsage(t, p.applyBudget, 0, 0)
	requireStorageWorkUsage(t, p.applyQueueBudget, 0, 0)
	// Negative control on this disposable, test-owned RawNode: deliberately
	// Step the ORIGINAL forbidden response to prove the native cursor witness
	// would detect an early whole-task acknowledgment. This is not a runtime
	// durability receipt or fabricated quorum ACK, and no more work is run.
	require.NoError(t, e.rawNode.Step(response))
	require.Equal(t, uint64(4), e.rawNode.BasicStatus().Applied)
}
