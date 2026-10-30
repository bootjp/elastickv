package etcd

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

// The test is the sole native RawNode owner, so it can inspect the authentic
// multi-entry response's applied cursor without a production interception hook.
func TestEnginePipelineConfigurationNativeWholeTaskResponse(t *testing.T) {
	for _, v2 := range []bool{false, true} {
		name := "v1"
		if v2 {
			name = "v2"
		}
		t.Run(name, func(t *testing.T) { testOwnedConfigurationResponse(t, v2) })
	}
}

func testOwnedConfigurationResponse(t *testing.T, v2 bool) {
	t.Helper()
	gate := &pipelineConfigurationGate{stage: "save", entered: make(chan uint64, 1), release: make(chan struct{})}
	fsm := &pipelineConfigurationFSM{asyncStorageFSM: &asyncStorageFSM{}, gate: gate}
	e := newOwnedPipelineApplyWitness(t, fsm)
	e.dataDir = t.TempDir()
	e.peers = map[uint64]Peer{
		1: {NodeID: 1, ID: "n1", Address: "127.0.0.1:7011"},
		2: {NodeID: 2, ID: "n2", Address: "127.0.0.1:7012"},
	}
	e.configIndex.Store(1)
	e.appliedIndex.Store(1)
	p := e.storagePipeline
	disk := &pipelineConfigurationStorage{Storage: e.persist, gate: gate, tailSaved: make(chan uint64, 1)}
	e.persist, p.persist = disk, disk
	t.Cleanup(func() { retireOwnedConfigurationWitness(p, gate) })
	context, err := encodeConfChangeContext(0, Peer{NodeID: 3, ID: "n3", Address: "127.0.0.1:7013"})
	require.NoError(t, err)
	config := &raftpb.Entry{Index: proto.Uint64(3), Term: proto.Uint64(2)}
	if v2 {
		config.Type = entryTypePtr(raftpb.EntryConfChangeV2)
		config.Data, err = proto.Marshal(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{
			{Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(3)},
		}, Context: context})
	} else {
		config.Type = entryTypePtr(raftpb.EntryConfChange)
		config.Data, err = proto.Marshal(&raftpb.ConfChange{Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: proto.Uint64(3), Context: context})
	}
	require.NoError(t, err)
	entries := []*raftpb.Entry{
		{Index: proto.Uint64(2), Term: proto.Uint64(2), Data: encodeProposalEnvelope(1, []byte("good"))},
		config,
		{Index: proto.Uint64(4), Term: proto.Uint64(2), Data: encodeProposalEnvelope(2, []byte("tail"))},
	}
	require.NoError(t, e.rawNode.Step(&raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), From: proto.Uint64(1), To: proto.Uint64(2),
		Term: proto.Uint64(2), Index: proto.Uint64(1), LogTerm: proto.Uint64(1), Commit: proto.Uint64(4), Entries: entries}))
	require.NoError(t, e.foldPipelineReady())
	require.NoError(t, p.flush())
	require.NoError(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, p.disk)))
	require.Len(t, p.applyDeferred, 1)
	task := p.applyDeferred[0]
	require.Len(t, task.message.Entries, 3)
	require.Len(t, task.message.Responses, 1)
	response := task.message.Responses[0]
	require.Equal(t, raftpb.MsgStorageApplyResp, response.GetType())
	require.Len(t, response.Entries, 3)
	require.NoError(t, p.flush())
	pumpOwnedConfigurationRequest(t, e, pipelineConfigurationFront)
	require.Equal(t, uint64(2), e.AppliedIndex(), "front callback publishes only the real earlier normal receipt")
	pumpOwnedConfigurationRequest(t, e, pipelineConfigurationPeers)
	require.NoError(t, p.flush())
	require.NoError(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, p.disk)))
	pumpOwnedConfigurationRequest(t, e, pipelineConfigurationSave)
	require.NoError(t, p.flush())
	select {
	case index := <-gate.entered:
		require.Equal(t, uint64(3), index)
	case <-time.After(time.Second):
		t.Fatal("real configuration SaveSnap did not enter")
	}
	require.Equal(t, uint64(1), e.rawNode.BasicStatus().Applied, "whole-task opaque response cannot precede configuration durability")
	require.Equal(t, uint64(1), e.currentConfigIndex())
	require.Equal(t, [][]byte{[]byte("good")}, fsm.Applied(), "tail must not overtake configuration")
	gate.unblock()
	require.NoError(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, p.disk)))
	pumpOwnedConfigurationRequest(t, e, pipelineConfigurationRelease)
	require.NoError(t, p.flush())
	require.NoError(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, p.disk)))
	pumpOwnedConfigurationRequest(t, e, pipelineConfigurationPublish)
	require.Equal(t, uint64(3), e.AppliedIndex())
	require.Equal(t, uint64(3), e.currentConfigIndex())
	require.Equal(t, uint64(1), e.rawNode.BasicStatus().Applied, "publication alone is not a whole-task completion")
	require.NoError(t, e.completePipelineApply(awaitOwnedPipelineCompletion(t, p.apply)))
	require.Equal(t, uint64(4), e.rawNode.BasicStatus().Applied)
	require.Equal(t, uint64(4), e.AppliedIndex())
	require.Equal(t, [][]byte{[]byte("good"), []byte("tail")}, fsm.Applied())
	requireStorageWorkUsage(t, p.configurationBudget, 0, 0)
}

func pumpOwnedConfigurationRequest(t *testing.T, e *Engine, phase pipelineConfigurationPhase) {
	t.Helper()
	completion := awaitOwnedPipelineCompletion(t, e.storagePipeline.apply)
	request, ok := completion.token.(*pipelineConfigurationRequest)
	require.True(t, ok)
	require.Equal(t, phase, request.phase)
	require.NoError(t, e.handlePipelineConfiguration(*request))
}

func retireOwnedConfigurationWitness(p *engineStoragePipeline, gate *pipelineConfigurationGate) {
	gate.unblock()
	p.firstFailure(errStorageWorkerClosed)
	p.stopActorsOnFailure()
	disk, apply := p.disk.completions, p.apply.completions
	for disk != nil || apply != nil {
		select {
		case completion, open := <-disk:
			if !open {
				disk = nil
				continue
			}
			retireOwnedConfigurationCompletion(completion)
		case completion, open := <-apply:
			if !open {
				apply = nil
				continue
			}
			retireOwnedConfigurationCompletion(completion)
		}
	}
	p.retireConfigurationHistory()
}

func retireOwnedConfigurationCompletion(completion storageWorkCompletion) {
	if request, ok := completion.token.(*pipelineConfigurationRequest); ok {
		request.reply <- pipelineConfigurationReply{err: errStorageWorkerClosed}
		return
	}
	completion.Release()
	if task, ok := completion.token.(*pipelineTask); ok {
		if task.configDisk != nil {
			task.configDisk.reply <- pipelineConfigurationReply{err: errStorageWorkerClosed}
		}
		task.retire()
	}
}
