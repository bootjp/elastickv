package main

import (
	"bytes"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"google.golang.org/protobuf/proto"
)

// bootstrapSharedGRPCMethods describes only the typed startup-control lane.
// Header-free calls still need the ordinary handler's DataReady/auth checks;
// peer calls additionally need the separate startup-token and runtime arbiter
// gates. These descriptors grant neither exception on their own.
func bootstrapSharedGRPCMethods(groupID uint64) []peersession.Method {
	return []peersession.Method{
		bootstrapControlMethod(groupID, pb.RawKV_RawLatestCommitTS_FullMethodName, bootstrapFloorRequest, bootstrapFloorReply),
		bootstrapControlMethod(groupID, pb.Distribution_GetTimestamp_FullMethodName, bootstrapTimestampRequest, bootstrapTimestampReply),
		bootstrapControlMethod(groupID, pb.Internal_GetPrepareEvidenceState_FullMethodName, bootstrapPrepareStateRequest, bootstrapPrepareStateReply),
		bootstrapControlMethod(groupID, pb.Internal_ForwardAdminProposal_FullMethodName, bootstrapPrepareProposalRequest, bootstrapPrepareProposalReply),
	}
}

func bootstrapControlMethod(groupID uint64, name string, request peersession.BindingDecoder, reply peersession.ReplyDecoder) peersession.Method {
	return peersession.Method{Name: name, AllowPublic: true, Reply: reply,
		BoundRequest: func(binding peersession.RequestBinding, payload any) (peersession.Message, error) {
			if binding.Group != groupID {
				return peersession.Message{}, peersession.ErrDenied
			}
			return request(binding, payload)
		},
	}
}

func bootstrapControlMessage(binding peersession.RequestBinding) peersession.Message {
	return peersession.Message{Group: binding.Group, From: binding.From.Member, To: binding.To.Member, Traffic: peersession.Control}
}

func bootstrapExactProto(message proto.Message) bool {
	return message != nil && message.ProtoReflect().IsValid() && len(message.ProtoReflect().GetUnknown()) == 0
}

func bootstrapFloorRequest(binding peersession.RequestBinding, payload any) (peersession.Message, error) {
	req, ok := payload.(*pb.RawLatestCommitTSRequest)
	if !ok || !bootstrapExactProto(req) || req.GetGroupId() == 0 || req.GetGroupId() != binding.Group ||
		len(req.GetKey()) != 0 || len(req.GetKeyBatch()) != 0 || req.GetReadRouteVersion() != 0 ||
		req.GetVersionVisibleAtTs() != 0 || req.GetExactVersionAtTs() != 0 {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlMessage(binding), nil
}

func bootstrapFloorReply(request peersession.Message, payload any) (peersession.Message, error) {
	reply, ok := payload.(*pb.RawLatestCommitTSResponse)
	if !ok || !bootstrapExactProto(reply) || reply.GetGroupId() != request.Group || !reply.GetLeaderFenced() {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlReply(request), nil
}

func bootstrapTimestampRequest(binding peersession.RequestBinding, payload any) (peersession.Message, error) {
	req, ok := payload.(*pb.GetTimestampRequest)
	if !ok || !bootstrapExactProto(req) || binding.Group != dedicatedTSORaftGroupID ||
		req.GetCount() > uint32(kv.MaxTSOBatchSize) || (req.GetActivatePhaseD() && !req.GetActivateCutover()) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlMessage(binding), nil
}

func bootstrapTimestampReply(request peersession.Message, payload any) (peersession.Message, error) {
	reply, ok := payload.(*pb.GetTimestampResponse)
	if !ok || !bootstrapExactProto(reply) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlReply(request), nil
}

func bootstrapPrepareStateRequest(binding peersession.RequestBinding, payload any) (peersession.Message, error) {
	req, ok := payload.(*pb.PrepareEvidenceStateRequest)
	if !ok || !bootstrapExactProto(req) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlMessage(binding), nil
}

func bootstrapPrepareStateReply(request peersession.Message, payload any) (peersession.Message, error) {
	reply, ok := payload.(*pb.PrepareEvidenceStateResponse)
	if !ok || !bootstrapExactProto(reply) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlReply(request), nil
}

func bootstrapPrepareProposalRequest(binding peersession.RequestBinding, payload any) (peersession.Message, error) {
	req, ok := payload.(*pb.ForwardAdminProposalRequest)
	if !ok || !bootstrapExactProto(req) || req.GetForceEntry() {
		return peersession.Message{}, peersession.ErrFormat
	}
	if command, matched, err := raftbootstrap.ParseActivationCommand(req.GetPayload()); matched {
		if err != nil || command.Witness.Group != binding.Group {
			return peersession.Message{}, peersession.ErrFormat
		}
		message := bootstrapControlMessage(binding)
		message.Traffic = peersession.PolicyControl
		return message, nil
	}
	if !bootstrapPreparePayload(req.GetPayload()) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlMessage(binding), nil
}

// Canonical re-encoding accepts only the existing complete prepare controls,
// not the broader forwarded-admin validator (which also accepts backup pins
// and watermarks). Zero-floor Fence is the protocol activation entry.
func bootstrapPreparePayload(payload []byte) bool {
	if bytes.Equal(payload, kv.EncodePrepareEvidenceSeal()) {
		return true
	}
	if len(payload) != len(kv.EncodePrepareEvidenceFence(0)) {
		return false
	}
	floor := binary.BigEndian.Uint64(payload[len(payload)-8:])
	return bytes.Equal(payload, kv.EncodePrepareEvidenceFence(floor)) || bytes.Equal(payload, kv.EncodePrepareEvidenceRelease(floor))
}

func bootstrapPrepareProposalReply(request peersession.Message, payload any) (peersession.Message, error) {
	reply, ok := payload.(*pb.ForwardAdminProposalResponse)
	if !ok || !bootstrapExactProto(reply) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlReply(request), nil
}

func bootstrapControlReply(request peersession.Message) peersession.Message {
	request.From, request.To = request.To, request.From
	return request
}
