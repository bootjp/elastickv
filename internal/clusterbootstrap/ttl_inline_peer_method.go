package clusterbootstrap

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	pb "github.com/bootjp/elastickv/proto"
)

const ttlInlineChallengeSize = 32

// TTLInlinePeerMethod is a protected peer-only probe. The challenge and
// native cut are bound into the session declaration, not trusted from metadata.
func TTLInlinePeerMethod(group uint64) peersession.Method {
	return peersession.Method{
		Name: pb.Internal_GetTTLInlineCapability_FullMethodName,
		BoundRequest: func(binding peersession.RequestBinding, payload any) (peersession.Message, error) {
			req, ok := payload.(*pb.GetTTLInlineCapabilityRequest)
			if !ok || !validTTLInlinePeerRequest(binding, req, group) {
				return peersession.Message{}, peersession.ErrFormat
			}
			return peersession.Message{Group: group, From: binding.From.Member, To: binding.To.Member,
				Traffic: peersession.Probe, Declaration: ttlInlineProofChallenge(group,
					req.GetExpectedConfigurationIndex(), req.GetExpectedTerm(), req.GetChallenge())}, nil
		},
		Reply: ttlInlinePeerReply,
	}
}

func validTTLInlinePeerRequest(binding peersession.RequestBinding, req *pb.GetTTLInlineCapabilityRequest, group uint64) bool {
	return bootstrapExactProto(req) && group != 0 && binding.Group == group &&
		binding.From.Member != 0 && binding.To.Member != 0 && binding.From.Member != binding.To.Member &&
		req.GetGroupId() == group && req.GetExpectedConfigurationIndex() != 0 && req.GetExpectedTerm() != 0 &&
		len(req.GetChallenge()) == ttlInlineChallengeSize
}

func ttlInlinePeerReply(request peersession.Message, payload any) (peersession.Message, error) {
	reply, ok := payload.(*pb.GetTTLInlineCapabilityResponse)
	if !ok || !validTTLInlinePeerReply(request, reply) {
		return peersession.Message{}, peersession.ErrFormat
	}
	if ttlInlineProofChallenge(request.Group, reply.GetConfigurationIndex(), reply.GetTerm(), reply.GetChallenge()) != request.Declaration {
		return peersession.Message{}, peersession.ErrFormat
	}
	roles := raftbootstrap.ConfState{Voters: reply.GetVoters(), VotersOutgoing: reply.GetVotersOutgoing(),
		Learners: reply.GetLearners(), LearnersNext: reply.GetLearnersNext(), AutoLeave: reply.GetAutoLeave()}
	if !validTTLInlineRoles(roles, reply.GetMemberId()) {
		return peersession.Message{}, peersession.ErrFormat
	}
	request.From, request.To = request.To, request.From
	return request, nil
}

func validTTLInlinePeerReply(request peersession.Message, reply *pb.GetTTLInlineCapabilityResponse) bool {
	return bootstrapExactProto(reply) && reply.GetSupported() && reply.GetFenced() &&
		reply.GetGroupId() == request.Group && reply.GetMemberId() == request.To &&
		reply.GetConfigurationIndex() != 0 && reply.GetTerm() != 0 &&
		len(reply.GetChallenge()) == ttlInlineChallengeSize
}

func ttlInlineProofChallenge(group, configurationIndex, term uint64, challenge []byte) raftbootstrap.Digest {
	var input [len("elastickv/ttl-inline/peer-proof/v1") + 24 + ttlInlineChallengeSize]byte
	n := copy(input[:], "elastickv/ttl-inline/peer-proof/v1")
	binary.BigEndian.PutUint64(input[n:], group)
	binary.BigEndian.PutUint64(input[n+8:], configurationIndex)
	binary.BigEndian.PutUint64(input[n+16:], term)
	copy(input[n+24:], challenge)
	return sha256.Sum256(input[:])
}
