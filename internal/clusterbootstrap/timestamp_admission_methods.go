package clusterbootstrap

import (
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
)

// TimestampAdmissionPeerMethods reserves the native peer-session lane. Handler
// installation and all-member activation remain separate, default-off gates.
func TimestampAdmissionPeerMethods(group uint64) []peersession.Method {
	if group == noderuntime.DedicatedTSORaftGroupID {
		return []peersession.Method{issueBoundTimestampMethod(group)}
	}
	return []peersession.Method{lookupTimestampBindingMethod(group)}
}

func issueBoundTimestampMethod(group uint64) peersession.Method {
	return peersession.Method{
		Name: pb.Internal_IssueBoundTimestamp_FullMethodName,
		BoundRequest: func(binding peersession.RequestBinding, value any) (peersession.Message, error) {
			request, ok := value.(*pb.IssueBoundTimestampRequest)
			if !ok || !bootstrapExactProto(request) || binding.Group != group ||
				request.GetPhysicalBandProtocolVersion() != kv.TSOPhysicalBandProtocolVersion {
				return peersession.Message{}, peersession.ErrFormat
			}
			return bootstrapControlMessage(binding), nil
		},
		Reply: issueBoundTimestampReply,
	}
}

func issueBoundTimestampReply(request peersession.Message, value any) (peersession.Message, error) {
	response, ok := value.(*pb.IssueBoundTimestampResponse)
	if !ok || !validIssueBoundTimestampResponse(response) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlReply(request), nil
}

func validIssueBoundTimestampResponse(response *pb.IssueBoundTimestampResponse) bool {
	if !bootstrapExactProto(response) || response.GetTimestamp() == 0 ||
		response.GetPhysicalBandProtocolVersion() != kv.TSOPhysicalBandProtocolVersion ||
		response.GetCommittedBandEnd() < response.GetTimestamp() ||
		response.GetCommittedBandEnd()>>16 != response.GetTimestamp()>>16 {
		return false
	}
	return response.GetCommittedBandEnd()&0xffff == 0xffff &&
		response.GetPreviousIssuedCursor() < response.GetTimestamp() &&
		(response.GetPreviousIssuedCursor() == 0 || response.GetPreviousIssuedCursor()>>16 == response.GetTimestamp()>>16)
}

func lookupTimestampBindingMethod(group uint64) peersession.Method {
	return peersession.Method{
		Name: pb.Internal_LookupTimestampBinding_FullMethodName,
		BoundRequest: func(binding peersession.RequestBinding, value any) (peersession.Message, error) {
			request, ok := value.(*pb.LookupTimestampBindingRequest)
			if !ok || !bootstrapExactProto(request) || binding.Group != group || len(request.GetOperationId()) != 16 {
				return peersession.Message{}, peersession.ErrFormat
			}
			return bootstrapControlMessage(binding), nil
		},
		Reply: lookupTimestampBindingReply,
	}
}

func lookupTimestampBindingReply(request peersession.Message, value any) (peersession.Message, error) {
	response, ok := value.(*pb.LookupTimestampBindingResponse)
	if !ok || !validLookupTimestampBindingResponse(response) {
		return peersession.Message{}, peersession.ErrFormat
	}
	return bootstrapControlReply(request), nil
}

func validLookupTimestampBindingResponse(response *pb.LookupTimestampBindingResponse) bool {
	if !bootstrapExactProto(response) {
		return false
	}
	if response.GetFound() {
		return response.GetTimestamp() != 0 && len(response.GetManifestDigest()) == 32
	}
	return response.GetTimestamp() == 0 && len(response.GetManifestDigest()) == 0 &&
		!response.GetIsTxn() && response.GetPhase() == pb.Phase_NONE
}
