package clusterbootstrap

import (
	"context"

	"github.com/bootjp/elastickv/internal/peersession"
	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ttlInlineNativeSnapshot interface {
	NativeConfigurationSnapshot() (raftengine.NativeConfiguration, uint64, bool)
}

type ttlInlineFenceState interface {
	TTLInlineFenceReady() bool
}

// NewTTLInlineLiveProbe serves only a live, protected local member proof.
// A remote result is evidence for a future leader-side certificate, not authority
// to activate the marker by itself.
func NewTTLInlineLiveProbe(group, member uint64, engine ttlInlineNativeSnapshot, fsm ttlInlineFenceState) func(context.Context, *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
	return func(ctx context.Context, req *pb.GetTTLInlineCapabilityRequest) (*pb.GetTTLInlineCapabilityResponse, error) {
		if err := validateTTLInlineProbeReceipt(ctx, req, group, member); err != nil {
			return nil, err
		}
		if engine == nil || fsm == nil || !fsm.TTLInlineFenceReady() {
			return nil, status.Error(codes.FailedPrecondition, "TTL-inline recovery fence unavailable")
		}
		native, term, ok := engine.NativeConfigurationSnapshot()
		if !ok || native.Index != req.GetExpectedConfigurationIndex() || term != req.GetExpectedTerm() ||
			!validTTLInlineRoles(raftbootstrap.ConfState{Voters: native.Voters, VotersOutgoing: native.VotersOutgoing,
				Learners: native.Learners, LearnersNext: native.LearnersNext, AutoLeave: native.AutoLeave}, member) ||
			!fsm.TTLInlineFenceReady() {
			return nil, status.Error(codes.FailedPrecondition, "TTL-inline native cut unavailable")
		}
		return &pb.GetTTLInlineCapabilityResponse{GroupId: group, MemberId: member,
			ConfigurationIndex: native.Index, Term: term,
			Voters: native.Voters, VotersOutgoing: native.VotersOutgoing,
			Learners: native.Learners, LearnersNext: native.LearnersNext, AutoLeave: native.AutoLeave,
			Fenced: true, Supported: true, Challenge: append([]byte(nil), req.GetChallenge()...)}, nil
	}
}

func validateTTLInlineProbeReceipt(ctx context.Context, req *pb.GetTTLInlineCapabilityRequest, group, member uint64) error {
	method, ok := grpc.Method(ctx)
	receipt := peersession.CapturedReceipt(ctx)
	if !ok || method != pb.Internal_GetTTLInlineCapability_FullMethodName || receipt == nil || !validTTLInlineProbeRequest(req, group, member) {
		return errors.WithStack(status.Error(codes.PermissionDenied, "typed TTL-inline peer session required"))
	}
	message := receipt.Message()
	if !validTTLInlineProbeMessage(message, req, group, member) {
		return errors.WithStack(status.Error(codes.PermissionDenied, "TTL-inline peer proof binding mismatch"))
	}
	if err := receipt.Check(ctx); err != nil {
		return errors.WithStack(status.Error(codes.PermissionDenied, "TTL-inline peer session revoked"))
	}
	return nil
}

func validTTLInlineProbeRequest(req *pb.GetTTLInlineCapabilityRequest, group, member uint64) bool {
	return bootstrapExactProto(req) && req.GetGroupId() == group && group != 0 && member != 0 &&
		req.GetExpectedConfigurationIndex() != 0 && req.GetExpectedTerm() != 0 &&
		len(req.GetChallenge()) == ttlInlineChallengeSize
}

func validTTLInlineProbeMessage(message peersession.Message, req *pb.GetTTLInlineCapabilityRequest, group, member uint64) bool {
	return message.Traffic == peersession.Probe && message.Group == group && message.To == member &&
		message.From != 0 && message.From != member &&
		message.Declaration == ttlInlineProofChallenge(group, req.GetExpectedConfigurationIndex(), req.GetExpectedTerm(), req.GetChallenge())
}
