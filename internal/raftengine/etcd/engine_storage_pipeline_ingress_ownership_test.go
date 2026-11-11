package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/testutil"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestPipelineIngressOwnsCompleteMessage(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	message := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(2), To: uint64Ptr(1),
		Term: uint64Ptr(3), Context: []byte("context"),
		Entries: []*raftpb.Entry{{Index: uint64Ptr(4), Term: uint64Ptr(3), Data: []byte("entry")}},
	}
	message.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	message.Entries[0].ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x02})
	want := proto.CloneOf(&message)
	require.NoError(t, e.enqueuePipelineStep(context.Background(), message))
	input := testutil.Recv(t, e.storagePipeline.bulk, time.Second, "accepted message missing from bulk ingress")
	t.Cleanup(input.permit.Release)
	*message.Term = 99
	message.Context[0] = 'X'
	message.Entries[0].Data[0] = 'X'
	message.ProtoReflect().GetUnknown()[2] = 9
	message.Entries[0].ProtoReflect().GetUnknown()[2] = 9
	require.True(t, proto.Equal(want, &input.message), "accepted ingress must not borrow mutable headers, payloads or unknown fields")
}

func TestPipelineIngressReservationCoversRetainedBacking(t *testing.T) {
	for _, field := range []string{"entry", "context", "unknown", "entry-unknown"} {
		t.Run(field, func(t *testing.T) {
			e := newPipelineAdmissionFixture(t)
			message := raftpb.Message{
				Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(2), To: uint64Ptr(1),
				Entries: []*raftpb.Entry{{Index: uint64Ptr(1), Term: uint64Ptr(1)}},
			}
			backing := make([]byte, 1<<20)
			copy(backing, []byte{0xa0, 0x06, 0x01})
			switch field {
			case "entry":
				message.Entries[0].Data = backing[:1]
			case "context":
				message.Context = backing[:1]
			case "unknown":
				message.ProtoReflect().SetUnknown(backing[:3])
			case "entry-unknown":
				message.Entries[0].ProtoReflect().SetUnknown(backing[:3])
			}
			require.NoError(t, e.enqueuePipelineStep(context.Background(), message))
			input := testutil.Recv(t, e.storagePipeline.bulk, time.Second, "accepted message missing from bulk ingress")
			t.Cleanup(input.permit.Release)
			retained := cap(input.message.Context) + cap(input.message.ProtoReflect().GetUnknown())
			for _, entry := range input.message.Entries {
				retained += cap(entry.Data) + cap(entry.ProtoReflect().GetUnknown())
			}
			require.GreaterOrEqual(t, input.permit.bytes, uint64(retained), "ingress reservation cannot release a borrowed large backing array")
			require.Less(t, retained, len(backing), "small wire messages must not pin an unrelated large buffer")
		})
	}
}

func TestPipelineIngressRejectedOwnershipLeavesBudgetEmpty(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	p := e.storagePipeline
	p.bulkIngressBudget = newStorageWorkBudget(1, 1)
	message := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(2), To: uint64Ptr(1),
		Entries: []*raftpb.Entry{{Index: uint64Ptr(1), Term: uint64Ptr(1), Data: []byte("rejected")}},
	}
	require.ErrorIs(t, e.enqueuePipelineStep(context.Background(), message), errStepQueueFull)
	require.Empty(t, p.bulk)
	bytes, items := p.bulkIngressBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
}

func TestPipelineIngressOwnsSnapshotBacking(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	data := make([]byte, 1<<20)
	data[0] = 7
	voters := make([]uint64, 1024)
	voters[0], voters[1] = 1, 2
	snapshot := raftTestSnapshot(10, 2, voters[:2], data[:1])
	snapshot.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	snapshot.Metadata.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x02})
	snapshot.Metadata.ConfState.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x03})
	message := raftpb.Message{Type: messageTypePtr(raftpb.MsgSnap),
		From: uint64Ptr(2), To: uint64Ptr(1), Term: uint64Ptr(2), Snapshot: &snapshot}
	want := proto.CloneOf(&message)
	require.NoError(t, e.enqueuePipelineStep(context.Background(), message))
	input := testutil.Recv(t, e.storagePipeline.priority, time.Second, "snapshot missing from priority ingress")
	t.Cleanup(input.permit.Release)
	data[0], voters[0] = 9, 9
	snapshot.ProtoReflect().GetUnknown()[2] = 9
	snapshot.Metadata.ProtoReflect().GetUnknown()[2] = 9
	snapshot.Metadata.ConfState.ProtoReflect().GetUnknown()[2] = 9
	require.True(t, proto.Equal(want, &input.message))
	owned := input.message.Snapshot
	retained := cap(owned.Data) + 8*cap(owned.Metadata.ConfState.Voters)
	retained += cap(owned.ProtoReflect().GetUnknown()) + cap(owned.Metadata.ProtoReflect().GetUnknown())
	retained += cap(owned.Metadata.ConfState.ProtoReflect().GetUnknown())
	require.GreaterOrEqual(t, input.permit.bytes, uint64(retained))
	require.Less(t, cap(owned.Data), cap(data))
	require.Less(t, cap(owned.Metadata.ConfState.Voters), cap(voters))
	bytes, items := e.storagePipeline.snapshotIngressBudget.usage()
	require.Equal(t, input.permit.bytes, bytes)
	require.Equal(t, 1, items)
	require.Nil(t, input.lease, "inline data has no received-file lease")
	input.permit.Release()
	bytes, items = e.storagePipeline.snapshotIngressBudget.usage()
	require.Zero(t, bytes)
	require.Zero(t, items)
}

func TestPipelineIngressOwnsMaximumCommandWithEnvelope(t *testing.T) {
	e := newPipelineAdmissionFixture(t)
	const payloadBytes = (64 << 20) + 1024
	data := make([]byte, payloadBytes)
	data[0], data[len(data)-1] = 1, 2
	message := raftpb.Message{
		Type: messageTypePtr(raftpb.MsgApp), From: uint64Ptr(2), To: uint64Ptr(1),
		Entries: []*raftpb.Entry{{Index: uint64Ptr(1), Term: uint64Ptr(1), Data: data}},
	}
	require.NoError(t, e.enqueuePipelineStep(context.Background(), message), "one maximum RawKV command plus envelope must fit")
	input := testutil.Recv(t, e.storagePipeline.bulk, time.Second, "accepted large message missing from bulk ingress")
	t.Cleanup(input.permit.Release)
	data[0], data[len(data)-1] = 3, 4
	owned := input.message.Entries[0].Data
	require.Len(t, owned, payloadBytes)
	require.Equal(t, byte(1), owned[0])
	require.Equal(t, byte(2), owned[len(owned)-1])
	require.GreaterOrEqual(t, input.permit.bytes, uint64(cap(owned)))
}
