package etcd

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestEnginePipelineNativeSnapshotIsOrderedWork(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	e.dataDir = t.TempDir()
	e.peers = map[uint64]Peer{
		1: {NodeID: 1, ID: "n1", Address: "127.0.0.1:7011"},
		2: {NodeID: 2, ID: "n2", Address: "127.0.0.1:7012"},
	}
	snapshot := raftTestSnapshot(10, 2, []uint64{1, 2}, []byte{0, 0, 0, 0})
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), From: proto.Uint64(1), To: proto.Uint64(2), Term: proto.Uint64(2), Snapshot: &snapshot}
	require.NoError(t, validatePipelineIngress(&message), "a genuine remote snapshot is not a forged local storage completion")
	require.NoError(t, e.stepPipelineOwner(&message))
	require.NoError(t, e.foldPipelineReady())
	require.NoError(t, e.storagePipeline.flush())
	completion := awaitOwnedPipelineCompletion(t, e.storagePipeline.apply)
	request, ok := completion.token.(*pipelineSnapshotRequest)
	require.True(t, ok)
	require.NoError(t, e.handlePipelineSnapshotRequest(request))
	require.NotZero(t, e.storagePipeline.diskOutstanding, "the native snapshot must become real ordered disk work")
	require.NotZero(t, e.storagePipeline.applyOutstanding, "the native snapshot must fence real ordered restore work")
}

func TestEnginePipelineSnapshotTupleOwnsJointPrefixAndPinsHistory(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	e.peers = map[uint64]Peer{}
	for id := uint64(1); id <= 5; id++ {
		e.peers[id] = Peer{NodeID: id, ID: "member", Address: "old-address"}
	}
	for index := uint64(2); index <= 6; index++ {
		require.NoError(t, e.storage.Append([]*raftpb.Entry{{Index: proto.Uint64(index), Term: proto.Uint64(2)}}))
	}
	joint := &raftpb.ConfState{Voters: []uint64{1, 2}, VotersOutgoing: []uint64{1, 2, 3},
		Learners: []uint64{4}, LearnersNext: []uint64{3}, AutoLeave: proto.Bool(true)}
	later := &raftpb.ConfState{Voters: []uint64{1, 2}, Learners: []uint64{3, 4, 5}}
	e.snapshotConfigurations.reset(1, &raftpb.ConfState{Voters: []uint64{1, 2}})
	require.NoError(t, e.snapshotConfigurations.record(3, joint))
	require.NoError(t, e.snapshotConfigurations.record(5, later))
	plan, err := e.newPipelineSnapshotPlan(nil)
	require.NoError(t, err)
	t.Cleanup(func() { e.retirePipelineSnapshot(plan) })
	_, err = e.storage.CreateSnapshot(5, later, []byte("newer-prefix"))
	require.NoError(t, err)
	require.NoError(t, e.capturePipelineSnapshotTuple(plan, 3))
	require.True(t, proto.Equal(joint, plan.snapshot.GetMetadata().GetConfState()))
	require.Equal(t, uint64(3), plan.snapshot.GetMetadata().GetIndex())
	require.Equal(t, uint64(2), plan.snapshot.GetMetadata().GetTerm())
	require.Len(t, plan.peers, 4)
	require.Equal(t, SuffrageVoter, plan.peers[2].Suffrage, "outgoing voters are still voters")
	require.Equal(t, SuffrageLearner, plan.peers[3].Suffrage)
	e.peers[3] = Peer{NodeID: 3, Address: "new-address"}
	e.snapshotConfigurations.entries[1].state.Learners[0] = 5
	require.Equal(t, []uint64{4}, plan.snapshot.GetMetadata().GetConfState().GetLearners())
	require.Equal(t, "old-address", plan.peers[2].Address)
	require.NoError(t, e.prunePipelineConfigurationHistory())
	require.Equal(t, uint64(3), e.snapshotConfigurations.entries[0].index)
	e.retirePipelineSnapshot(plan)
	require.NoError(t, e.prunePipelineConfigurationHistory())
	require.Len(t, e.snapshotConfigurations.entries, 1)
	require.Equal(t, uint64(5), e.snapshotConfigurations.entries[0].index)
	bytes, items := e.storagePipeline.snapshotBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
}

func TestEnginePipelineReceivedSnapshotSupersedesQueuedVolatileApply(t *testing.T) {
	fsm := &volatileTagFakeFSM{}
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	e.fsm = fsm
	p := e.storagePipeline
	// An append completion can emit older committed work after the incoming
	// snapshot barrier was queued. That work must not run after restore.
	p.applyCursor, p.receivedApplyFloor = 10, 10
	entry := &raftpb.Entry{Type: entryTypePtr(raftpb.EntryNormal), Index: proto.Uint64(5),
		Data: encodeProposalEnvelope(42, []byte{0x02, 0xde, 0xad})}
	task := &pipelineTask{message: &raftpb.Message{Entries: []*raftpb.Entry{entry}},
		inputs: []normalApplyInput{e.prepareNormalApply(*entry, false)}}
	require.NoError(t, p.executePipelineApply(task))
	require.Zero(t, fsm.calls.Load(), "restored snapshot already includes this volatile prefix")
	require.Empty(t, task.receipts, "covered work must not manufacture proposal receipts")
}

func TestEnginePipelineUnconsumedSnapshotIngressProtectionJoins(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	e.initPipelineSnapshotFiles()
	e.protectedReceivedFSMSnaps[10] = 1
	snapshot := raftTestSnapshot(10, 2, []uint64{1, 2}, encodeSnapshotToken(10, 123))
	require.NoError(t, e.enqueuePipelineStep(context.Background(), raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap),
		From: proto.Uint64(1), To: proto.Uint64(2), Term: proto.Uint64(2), Snapshot: &snapshot}))
	p := e.storagePipeline
	p.apply.quiesce()
	p.disk.quiesce()
	for _, actor := range []*storageWorker{p.apply, p.disk} {
		for completion := range actor.completions {
			require.NoError(t, completion.err)
			completion.Release()
		}
		<-actor.done
	}
	require.Empty(t, e.protectedReceivedFSMSnaps, "unconsumed ingress owns a received-file protection until the actual actor join")
	p.retireIngress()
}
