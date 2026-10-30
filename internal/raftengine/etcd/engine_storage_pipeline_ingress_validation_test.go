package etcd

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestPipelineDebtPublicReceivedConfigurationAdmission(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	state := &raftpb.ConfState{Voters: []uint64{1}}
	for id := uint64(2); id < 3002; id++ {
		state.Learners = append(state.Learners, id)
	}
	require.ErrorIs(t, validatePipelineConfigurationState(state), errStorageWorkInvalidReservation)
	snapshot := raftTestSnapshot(10, 2, []uint64{1}, []byte("valid snapshot body belongs to the FSM"))
	snapshot.Metadata.ConfState = state
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), From: uint64Ptr(2), To: uint64Ptr(1), Snapshot: &snapshot}
	err := e.enqueuePipelineStep(t.Context(), message)
	if err == nil {
		input := <-e.storagePipeline.priority
		t.Logf("public ingress admitted snapshot; learners=%d; cloned array=%v", len(input.message.Snapshot.Metadata.ConfState.Learners),
			&input.message.Snapshot.Metadata.ConfState.Learners[0] != &state.Learners[0])
		require.ErrorIs(t, e.consumePipelineIngress(input), errStorageWorkInvalidReservation)
	}
	require.ErrorIs(t, err, errStorageWorkInvalidReservation, "ConfState guard must precede public ingress clone and acceptance")
}

func TestPipelineDebtPublicReceivedSnapshotUnknownAdmission(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	snapshot := raftTestSnapshot(10, 2, []uint64{1}, []byte("valid-inline-body"))
	unknown := protowire.AppendTag(nil, 65535, protowire.BytesType)
	unknown = protowire.AppendBytes(unknown, bytes.Repeat([]byte{0xa5}, pipelineControlBytes))
	snapshot.Metadata.ProtoReflect().SetUnknown(unknown)
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap), From: uint64Ptr(2), To: uint64Ptr(1), Snapshot: &snapshot}
	require.ErrorIs(t, e.enqueuePipelineStep(t.Context(), message), errStorageWorkInvalidReservation)
	require.Empty(t, e.storagePipeline.priority, "descriptor rejection precedes ownership clone and acceptance")
	requireStorageWorkUsage(t, e.storagePipeline.bulkOwnership, 0, 0)
}

func TestPipelineDebtPublicNestedMalformedConfiguration(t *testing.T) {
	c := newDebtReadCluster(t)
	c.partition = false
	require.NoError(t, c.nodes[0].Campaign())
	c.pump(t)
	require.Equal(t, etcdraft.StateLeader, c.nodes[0].BasicStatus().RaftState)
	e := newPipelineAdmissionFixture(t)
	e.rawNode, e.storage, e.nodeID = c.nodes[0], c.stores[0], 1
	t.Cleanup(func() { e.rawNode = nil; e.storagePipeline.ledger.discard() })
	data := []byte{0x12, 0x01, 0x80}
	require.Error(t, proto.Unmarshal(data, &raftpb.ConfChangeV2{}))
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgProp), From: uint64Ptr(2), To: uint64Ptr(1),
		Entries: []*raftpb.Entry{{Type: entryTypePtr(raftpb.EntryConfChangeV2), Data: data}}}
	err := e.enqueuePipelineStep(t.Context(), message)
	if err == nil {
		input := <-e.storagePipeline.bulk
		require.NotPanics(t, func() { _ = e.consumePipelineIngress(input) }, "malformed nested wire must not panic in RawNode")
	}
	require.Error(t, err, "nested malformed proposal must be rejected before cloning and Step")
}
