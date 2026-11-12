package main

import (
	"bytes"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

const bootstrapMethodsTestGroup uint64 = 17

func bootstrapMethodForTest(t *testing.T, group uint64, name string) peersession.Method {
	t.Helper()
	for _, method := range bootstrapSharedGRPCMethods(group) {
		if method.Name == name {
			return method
		}
	}
	t.Fatalf("missing method %s", name)
	return peersession.Method{}
}

func bootstrapBindingForTest(group uint64) peersession.RequestBinding {
	return peersession.RequestBinding{Group: group, From: peersession.Node{Member: 2}, To: peersession.Node{Member: 1}}
}

func TestBootstrapSharedGRPCMethodsExactInventory(t *testing.T) {
	t.Parallel()
	methods := bootstrapSharedGRPCMethods(bootstrapMethodsTestGroup)
	names := make([]string, 0, len(methods))
	for _, method := range methods {
		names = append(names, method.Name)
		require.True(t, method.AllowPublic)
		require.Nil(t, method.Request)
		require.Nil(t, method.Stream)
		require.NotNil(t, method.BoundRequest)
		require.NotNil(t, method.Reply)
	}
	require.ElementsMatch(t, []string{pb.RawKV_RawLatestCommitTS_FullMethodName, pb.Distribution_GetTimestamp_FullMethodName,
		pb.Internal_GetPrepareEvidenceState_FullMethodName, pb.Internal_ForwardAdminProposal_FullMethodName,
		pb.EncryptionAdmin_RegisterEncryptionWriter_FullMethodName}, names)
}

func TestBootstrapSharedGRPCMethodsBoundRoutingAndReplies(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		group uint64
		req   proto.Message
		reply proto.Message
	}{
		{pb.RawKV_RawLatestCommitTS_FullMethodName, bootstrapMethodsTestGroup,
			&pb.RawLatestCommitTSRequest{GroupId: bootstrapMethodsTestGroup}, &pb.RawLatestCommitTSResponse{GroupId: bootstrapMethodsTestGroup, LeaderFenced: true}},
		{pb.Distribution_GetTimestamp_FullMethodName, noderuntime.DedicatedTSORaftGroupID, &pb.GetTimestampRequest{Count: 1}, &pb.GetTimestampResponse{Timestamp: 10, Count: 1}},
		{pb.Internal_GetPrepareEvidenceState_FullMethodName, bootstrapMethodsTestGroup, &pb.PrepareEvidenceStateRequest{Floor: 10}, &pb.PrepareEvidenceStateResponse{}},
		{pb.Internal_ForwardAdminProposal_FullMethodName, bootstrapMethodsTestGroup,
			&pb.ForwardAdminProposalRequest{Payload: kv.EncodePrepareEvidenceFence(0)}, &pb.ForwardAdminProposalResponse{CommitIndex: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			method := bootstrapMethodForTest(t, tt.group, tt.name)
			binding := bootstrapBindingForTest(tt.group)
			message, err := method.BoundRequest(binding, tt.req)
			require.NoError(t, err)
			require.Equal(t, peersession.Message{Group: tt.group, From: 2, To: 1, Traffic: peersession.Control}, message)
			reply, err := method.Reply(message, tt.reply)
			require.NoError(t, err)
			require.Equal(t, peersession.Message{Group: tt.group, From: 1, To: 2, Traffic: peersession.Control}, reply)
			binding.Group++
			_, err = method.BoundRequest(binding, tt.req)
			require.Error(t, err)
			_, err = method.BoundRequest(bootstrapBindingForTest(tt.group), &pb.Empty{})
			require.Error(t, err)
			_, err = method.Reply(message, &pb.Empty{})
			require.Error(t, err)
			unknown := proto.Clone(tt.req)
			unknown.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			_, err = method.BoundRequest(bootstrapBindingForTest(tt.group), unknown)
			require.Error(t, err)
			unknownReply := proto.Clone(tt.reply)
			unknownReply.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
			_, err = method.Reply(message, unknownReply)
			require.Error(t, err)
		})
	}
}

func TestBootstrapFloorMethodRejectsNonFloorRequests(t *testing.T) {
	t.Parallel()
	method := bootstrapMethodForTest(t, bootstrapMethodsTestGroup, pb.RawKV_RawLatestCommitTS_FullMethodName)
	for _, req := range []*pb.RawLatestCommitTSRequest{
		nil, {}, {GroupId: bootstrapMethodsTestGroup + 1},
		{GroupId: bootstrapMethodsTestGroup, Key: []byte("key")},
		{GroupId: bootstrapMethodsTestGroup, KeyBatch: []byte{1}},
		{GroupId: bootstrapMethodsTestGroup, ReadRouteVersion: 1},
		{GroupId: bootstrapMethodsTestGroup, VersionVisibleAtTs: 1},
		{GroupId: bootstrapMethodsTestGroup, ExactVersionAtTs: 1},
	} {
		_, err := method.BoundRequest(bootstrapBindingForTest(bootstrapMethodsTestGroup), req)
		require.Error(t, err)
	}
	request := peersession.Message{Group: bootstrapMethodsTestGroup, From: 2, To: 1, Traffic: peersession.Control}
	for _, reply := range []*pb.RawLatestCommitTSResponse{nil, {},
		{GroupId: bootstrapMethodsTestGroup}, {GroupId: bootstrapMethodsTestGroup + 1, LeaderFenced: true}} {
		_, err := method.Reply(request, reply)
		require.Error(t, err)
	}
}

func TestBootstrapTimestampMethodRetainsBoundsAndTargetGroup(t *testing.T) {
	t.Parallel()
	method := bootstrapMethodForTest(t, noderuntime.DedicatedTSORaftGroupID, pb.Distribution_GetTimestamp_FullMethodName)
	for _, req := range []*pb.GetTimestampRequest{{}, {Count: 1}, {Count: uint32(kv.MaxTSOBatchSize)},
		{MinTimestamp: 10, ActivateCutover: true, ActivatePhaseD: true}} {
		_, err := method.BoundRequest(bootstrapBindingForTest(noderuntime.DedicatedTSORaftGroupID), req)
		require.NoError(t, err)
	}
	for _, req := range []*pb.GetTimestampRequest{nil, {Count: uint32(kv.MaxTSOBatchSize) + 1}, {ActivatePhaseD: true}} {
		_, err := method.BoundRequest(bootstrapBindingForTest(noderuntime.DedicatedTSORaftGroupID), req)
		require.Error(t, err)
	}
	wrong := bootstrapMethodForTest(t, bootstrapMethodsTestGroup, pb.Distribution_GetTimestamp_FullMethodName)
	_, err := wrong.BoundRequest(bootstrapBindingForTest(bootstrapMethodsTestGroup), &pb.GetTimestampRequest{})
	require.Error(t, err)
}

func TestBootstrapPrepareProposalAcceptsOnlyCanonicalControls(t *testing.T) {
	t.Parallel()
	method := bootstrapMethodForTest(t, bootstrapMethodsTestGroup, pb.Internal_ForwardAdminProposal_FullMethodName)
	binding := bootstrapBindingForTest(bootstrapMethodsTestGroup)
	for _, payload := range [][]byte{kv.EncodePrepareEvidenceFence(0), kv.EncodePrepareEvidenceFence(10),
		kv.EncodePrepareEvidenceSeal(), kv.EncodePrepareEvidenceRelease(10)} {
		_, err := method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: payload})
		require.NoError(t, err)
		_, err = method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: payload, ForceEntry: true})
		require.Error(t, err)
		for n := 0; n < len(payload); n++ {
			_, err = method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: payload[:n]})
			require.Error(t, err)
		}
		_, err = method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: append(bytes.Clone(payload), 0)})
		require.Error(t, err)
	}
	for _, payload := range [][]byte{nil, {0xff}, {0x0e, 0xff}, kv.EncodeWatermarkAdvance(10),
		kv.EncodeBackupPinEntry(kv.BackupPinEntry{ReadTS: 10, Deadline: time.Unix(1, 0)}),
		kv.EncodeBackupReserveEntry(kv.BackupReserveEntry{}), kv.EncodeBackupExtendEntry(kv.BackupExtendEntry{}),
		kv.EncodeBackupReleaseEntry(kv.BackupReleaseEntry{}), kv.EncodeBackupUnreserveEntry(kv.BackupUnreserveEntry{})} {
		_, err := method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: payload})
		require.Error(t, err)
	}
}

func TestBootstrapPrepareMethodsRejectNilMessages(t *testing.T) {
	t.Parallel()
	binding := bootstrapBindingForTest(bootstrapMethodsTestGroup)
	_, err := bootstrapPrepareStateRequest(binding, (*pb.PrepareEvidenceStateRequest)(nil))
	require.Error(t, err)
	_, err = bootstrapPrepareProposalRequest(binding, (*pb.ForwardAdminProposalRequest)(nil))
	require.Error(t, err)
	_, err = bootstrapTimestampReply(peersession.Message{}, (*pb.GetTimestampResponse)(nil))
	require.Error(t, err)
	_, err = bootstrapPrepareStateReply(peersession.Message{}, (*pb.PrepareEvidenceStateResponse)(nil))
	require.Error(t, err)
	_, err = bootstrapPrepareProposalReply(peersession.Message{}, (*pb.ForwardAdminProposalResponse)(nil))
	require.Error(t, err)
}

func TestBootstrapForwardActivationUsesOnlyPolicyControl(t *testing.T) {
	t.Parallel()
	method := bootstrapMethodForTest(t, bootstrapMethodsTestGroup, pb.Internal_ForwardAdminProposal_FullMethodName)
	binding := bootstrapBindingForTest(bootstrapMethodsTestGroup)
	payload := bootstrapPayloadActivation(t, bootstrapMethodsTestGroup)
	message, err := method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: payload})
	require.NoError(t, err)
	require.Equal(t, peersession.Message{Group: binding.Group, From: binding.From.Member, To: binding.To.Member, Traffic: peersession.PolicyControl}, message)
	reply, err := method.Reply(message, &pb.ForwardAdminProposalResponse{CommitIndex: 10})
	require.NoError(t, err)
	require.Equal(t, peersession.PolicyControl, reply.Traffic)
	require.Equal(t, message.From, reply.To)
	require.Equal(t, message.To, reply.From)
	_, err = method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: payload, ForceEntry: true})
	require.Error(t, err)
	_, err = method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: bootstrapPayloadActivation(t, binding.Group+1)})
	require.Error(t, err)
	unknown := &pb.ForwardAdminProposalRequest{Payload: payload}
	unknown.ProtoReflect().SetUnknown([]byte{0xf8, 0x07, 0x01})
	_, err = method.BoundRequest(binding, unknown)
	require.Error(t, err)
	for n := 0; n < len(payload); n++ {
		_, err := method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: payload[:n]})
		require.Error(t, err, "length %d", n)
	}
	_, err = method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: append(bytes.Clone(payload), 0)})
	require.Error(t, err)
	for _, kind := range []raftbootstrap.ControlKind{raftbootstrap.ControlReserve, raftbootstrap.ControlAbort, 255} {
		candidate := bytes.Clone(payload)
		candidate[4] = byte(kind)
		_, err := method.BoundRequest(binding, &pb.ForwardAdminProposalRequest{Payload: candidate})
		require.Error(t, err)
	}
}
