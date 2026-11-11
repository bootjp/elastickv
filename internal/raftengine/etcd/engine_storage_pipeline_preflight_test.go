package etcd

import (
	"bytes"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestPipelineDebtReceivedFrontReservesBeforeClone(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	p := e.storagePipeline
	full, err := p.snapshotBudget.tryAcquire(pipelineBulkBytes)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	const payloadBytes = 16 << 20
	snapshot := raftTestSnapshot(10, 2, []uint64{1, 2}, bytes.Repeat([]byte{0xa5}, payloadBytes))
	message := &raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), From: uint64Ptr(1), To: uint64Ptr(2), Snapshot: &snapshot}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err = e.queuePipelineReceivedFront(message)
	runtime.ReadMemStats(&after)
	require.ErrorIs(t, err, errStoragePipelineResource)
	require.Empty(t, p.snapshotPins)
	require.Empty(t, p.applyDeferred)
	// This isolated sequential test detects one rejected payload clone. It is
	// allocation evidence for this call, not a concurrent heap/R theorem.
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(payloadBytes/2), "rejection must precede the actual full-payload clone")
}

func TestPipelineDebtConfigurationReservationPrecedesMutation(t *testing.T) {
	e := newOwnedPipelineApplyWitness(t, &testStateMachine{})
	e.peers = map[uint64]Peer{1: {NodeID: 1, ID: "n1", Address: "127.0.0.1:7011"}, 2: {NodeID: 2, ID: "n2", Address: "127.0.0.1:7012"}}
	context, err := encodeConfChangeContext(0, Peer{NodeID: 3, ID: "n3", Address: "127.0.0.1:7013"})
	require.NoError(t, err)
	data, err := proto.Marshal(&raftpb.ConfChange{Type: confChangeTypePtr(raftpb.ConfChangeAddLearnerNode), NodeId: uint64Ptr(3), Context: context})
	require.NoError(t, err)
	entry := &raftpb.Entry{Index: uint64Ptr(2), Term: uint64Ptr(2), Type: entryTypePtr(raftpb.EntryConfChange), Data: data}
	require.NoError(t, e.rawNode.Step(&raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2),
		Term: uint64Ptr(2), Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Commit: uint64Ptr(2), Entries: []*raftpb.Entry{entry}}))
	require.NoError(t, e.foldPipelineReady())
	require.NoError(t, e.storagePipeline.flush())
	require.NoError(t, e.completePipelineDisk(awaitOwnedPipelineCompletion(t, e.storagePipeline.disk)))
	require.Len(t, e.storagePipeline.applyDeferred, 1)
	task := e.storagePipeline.applyDeferred[0]
	before := e.rawNode.Status().Config.String()
	full, err := e.storagePipeline.configurationBudget.tryAcquire(pipelineControlBytes)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	_, err = e.frontPipelineConfiguration(pipelineConfigurationRequest{task: task, entry: task.message.Entries[0]})
	require.Error(t, err)
	require.Equal(t, before, e.rawNode.Status().Config.String(), "failed reservation must precede native ApplyConfChange")
}

func TestPipelineDebtRecoveryRejectsUnboundedBorrowBeforeRawNode(t *testing.T) {
	storage := newLedgerNativeWitness(t).store.MemoryStorage
	entry := ledgerEntry(2, 2, "")
	entry.Data = make([]byte, pipelineMaxOwnedInputBytes+1)[:1]
	require.NoError(t, storage.Append([]*raftpb.Entry{entry}))
	require.NoError(t, storage.SetHardState(&raftpb.HardState{Term: uint64Ptr(2), Commit: uint64Ptr(2)}))
	node, err := newStorageRawNode(OpenConfig{NodeID: 2, ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsg: 16, MaxSizePerMsg: pipelineEntryRoom}, storage, 1)
	require.ErrorIs(t, err, errStorageWorkInvalidReservation, "recovery must validate retained backing, not just one wire byte")
	require.Nil(t, node)
}

func TestPipelineDebtConfigurationPreflightPreservesMaximumContext(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	peer := Peer{NodeID: 3, ID: strings.Repeat("i", 65535), Address: strings.Repeat("a", 65535)}
	context, err := encodeConfChangeContext(7, peer)
	require.NoError(t, err)
	for _, v2 := range []bool{false, true} {
		var entry raftpb.Entry
		var change proto.Message
		if v2 {
			entry.Type = entryTypePtr(raftpb.EntryConfChangeV2)
			change = &raftpb.ConfChangeV2{Context: context, Changes: []*raftpb.ConfChangeSingle{{NodeId: uint64Ptr(3)}}}
		} else {
			entry.Type = entryTypePtr(raftpb.EntryConfChange)
			change = &raftpb.ConfChange{Context: context, NodeId: uint64Ptr(3)}
		}
		entry.Data, err = proto.Marshal(change)
		require.NoError(t, err)
		size, err := e.pipelineConfigurationPreflightBytes(&entry)
		require.NoError(t, err, "resource preflight must preserve the existing uint16 context fields")
		require.GreaterOrEqual(t, size, pipelineConfigurationBytes(change))
	}
}

func TestPipelineDebtConfigurationPreflightCountsWireDescriptors(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	var data []byte
	for i := 0; i <= pipelineControlBytes/pipelineConfigurationMemberBytes; i++ {
		data = protowire.AppendTag(data, 2, protowire.BytesType)
		data = protowire.AppendBytes(data, nil)
	}
	_, err := e.pipelineConfigurationPreflightBytes(&raftpb.Entry{Type: entryTypePtr(raftpb.EntryConfChangeV2), Data: data})
	require.ErrorIs(t, err, errStorageWorkInvalidReservation, "short repeated-message encodings still allocate descriptors/maps")
	err = e.enqueuePipelineStep(t.Context(), raftpb.Message{Type: messageTypePtr(raftpb.MsgProp), From: uint64Ptr(1), To: uint64Ptr(1),
		Entries: []*raftpb.Entry{{Type: entryTypePtr(raftpb.EntryConfChangeV2), Data: data}}})
	require.ErrorIs(t, err, errStorageWorkInvalidReservation, "public ingress must reject before clone or RawNode's proposal decoder")
	require.Empty(t, e.storagePipeline.bulk)
	_, err = e.pipelineConfigurationPreflightBytes(&raftpb.Entry{Type: entryTypePtr(raftpb.EntryConfChangeV2), Data: []byte{0x12, 0x7f}})
	require.Error(t, err, "malformed wire fields must not reach decode or RawNode mutation")
}

func TestPipelineDebtConfigurationDecodeReservationRetiresOnDrain(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	data, err := proto.Marshal(&raftpb.ConfChangeV2{Changes: []*raftpb.ConfChangeSingle{{NodeId: uint64Ptr(3)}}})
	require.NoError(t, err)
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgProp), From: uint64Ptr(1), To: uint64Ptr(1),
		Entries: []*raftpb.Entry{{Type: entryTypePtr(raftpb.EntryConfChangeV2), Data: data}}}
	require.NoError(t, e.enqueuePipelineStep(t.Context(), message))
	bytes, work := e.storagePipeline.controlOwnership.usage()
	require.Positive(t, bytes)
	require.Equal(t, 1, work)
	e.storagePipeline.retireIngress()
	requireStorageWorkUsage(t, e.storagePipeline.controlOwnership, 0, 0)
	requireStorageWorkUsage(t, e.storagePipeline.bulkOwnership, 0, 0)
}

func TestPipelineDebtFailedReadyRetainsAdmittedInputThroughJoin(t *testing.T) {
	w := newLedgerNativeWitness(t)
	e := newPipelineAdmissionFixture(t)
	e.nodeID, e.rawNode, e.storage = 2, w.node, w.store.MemoryStorage
	p := e.storagePipeline
	p.readyBudget = newStorageWorkBudget(1, 1)
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(1), To: uint64Ptr(2),
		Term: uint64Ptr(2), Index: uint64Ptr(1), LogTerm: uint64Ptr(1), Entries: []*raftpb.Entry{ledgerEntry(2, 2, "retained input")}}
	require.NoError(t, e.enqueuePipelineStep(t.Context(), message))
	require.ErrorIs(t, e.consumePipelineIngress(<-p.bulk), errStoragePipelineResource)
	require.NotNil(t, p.failedReady)
	require.Len(t, p.failedIngress, 1)
	bytes, work := p.bulkIngressBudget.usage()
	require.Positive(t, bytes)
	require.Equal(t, 1, work, "failed unreserved Ready cannot release its admitted input owner")
	p.disk.quiesce()
	p.apply.quiesce()
	<-p.disk.done
	<-p.apply.done
	e.rawNode = nil
	p.failedReady = nil
	p.ledger.discard()
	p.retireIngress()
	requireStorageWorkUsage(t, p.bulkOwnership, 0, 0)
}

func TestPipelineDebtRecoveryPreservesMaximumCommandEnvelope(t *testing.T) {
	storage := newLedgerNativeWitness(t).store.MemoryStorage
	entry := ledgerEntry(2, 2, "")
	entry.Data = make([]byte, (64<<20)+1024)
	require.NoError(t, storage.Append([]*raftpb.Entry{entry}))
	require.NoError(t, storage.SetHardState(&raftpb.HardState{Term: uint64Ptr(2), Commit: uint64Ptr(2)}))
	node, err := newStorageRawNode(OpenConfig{NodeID: 2, ElectionTick: 10, HeartbeatTick: 1, MaxInflightMsg: 16, MaxSizePerMsg: pipelineEntryRoom}, storage, 1)
	require.NoError(t, err)
	require.NotNil(t, node)
	rd := node.Ready()
	require.LessOrEqual(t, pipelineReadyBytes(rd), uint64(pipelineReadyOvershootBytes))
}

func TestPipelineDebtSnapshotRejectsConfigurationExpansionBeforeClone(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	state := &raftpb.ConfState{Voters: []uint64{1}}
	for i := uint64(2); i < 3002; i++ {
		state.Learners = append(state.Learners, i)
	}
	snapshot := raftTestSnapshot(10, 2, []uint64{1}, []byte("valid snapshot body belongs to the FSM"))
	snapshot.Metadata.ConfState = state
	_, err := e.newPipelineSnapshotPlan(&raftpb.Message{Snapshot: &snapshot})
	require.ErrorIs(t, err, errStorageWorkInvalidReservation)
	require.Empty(t, e.storagePipeline.snapshotPins)
	requireStorageWorkUsage(t, e.storagePipeline.snapshotBudget, 0, 0)
}
