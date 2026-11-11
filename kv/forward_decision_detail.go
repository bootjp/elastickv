package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const forwardDecisionSchemaVersion = 1

// ForwardDecisionStatus encodes a verified Internal.Forward result. Callers must
// establish the decision from admission and durable state before calling it.
func ForwardDecisionStatus(phase pb.ForwardFailurePhase, reason pb.ForwardFailureReason, decision pb.ForwardDecisionClass) error {
	if !validForwardDecision(phase, reason, decision) {
		return errors.WithStack(status.Error(codes.Internal, "invalid forward decision"))
	}
	st, err := status.New(forwardDecisionCode(decision), forwardDecisionMessage(reason)).WithDetails(&pb.ForwardFailureDetail{
		SchemaVersion: forwardDecisionSchemaVersion,
		Phase:         phase,
		Reason:        reason,
		Decision:      decision,
	})
	if err != nil {
		return errors.WithStack(status.Error(codes.Internal, "forward decision detail unavailable"))
	}
	return errors.WithStack(st.Err())
}

// DecodeForwardDecisionStatus only validates the wire contract. Its caller must
// additionally authenticate the expected Internal.Forward peer and invocation.
func DecodeForwardDecisionStatus(err error, expectedPhase pb.ForwardFailurePhase) (pb.ForwardFailureReason, pb.ForwardDecisionClass, bool) {
	st, ok := status.FromError(err)
	if !ok || !validForwardPhase(expectedPhase) {
		return 0, 0, false
	}
	found, ok := uniqueForwardDecisionDetail(st)
	if !ok || found.GetSchemaVersion() != forwardDecisionSchemaVersion || found.GetPhase() != expectedPhase ||
		!validForwardDecision(found.GetPhase(), found.GetReason(), found.GetDecision()) ||
		st.Code() != forwardDecisionCode(found.GetDecision()) {
		return 0, 0, false
	}
	return found.GetReason(), found.GetDecision(), true
}

func uniqueForwardDecisionDetail(st *status.Status) (*pb.ForwardFailureDetail, bool) {
	var found *pb.ForwardFailureDetail
	for _, raw := range st.Proto().GetDetails() {
		if !raw.MessageIs(&pb.ForwardFailureDetail{}) {
			continue
		}
		if found != nil {
			return nil, false
		}
		found = new(pb.ForwardFailureDetail)
		if err := anypb.UnmarshalTo(raw, found, proto.UnmarshalOptions{}); err != nil {
			return nil, false
		}
	}
	return found, found != nil
}

func validForwardPhase(phase pb.ForwardFailurePhase) bool {
	return phase >= pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY &&
		phase <= pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ABORT
}

func validForwardDecision(phase pb.ForwardFailurePhase, reason pb.ForwardFailureReason, decision pb.ForwardDecisionClass) bool {
	if !validForwardPhase(phase) {
		return false
	}
	if phase == pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY {
		return validOnePhaseForwardDecision(reason, decision)
	}
	return validMultiPhaseForwardDecision(reason, decision)
}

func validOnePhaseForwardDecision(reason pb.ForwardFailureReason, decision pb.ForwardDecisionClass) bool {
	if decision == pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_REJECTED_BEFORE_SUBMISSION {
		return reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_NO_LEADER || reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE
	}
	if decision == pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED {
		return reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_CONFLICT || reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE
	}
	return decision == pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN && validUnknownForwardReason(reason)
}

func validMultiPhaseForwardDecision(reason pb.ForwardFailureReason, decision pb.ForwardDecisionClass) bool {
	if decision == pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED {
		return reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_ABORTED
	}
	return decision == pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_OUTCOME_UNKNOWN && validUnknownForwardReason(reason)
}

func validUnknownForwardReason(reason pb.ForwardFailureReason) bool {
	return reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_CONFLICT || reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE ||
		reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_NO_LEADER || reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_INTERRUPTED
}

func forwardDecisionCode(decision pb.ForwardDecisionClass) codes.Code {
	if decision == pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_REJECTED_BEFORE_SUBMISSION {
		return codes.Unavailable
	}
	if decision == pb.ForwardDecisionClass_FORWARD_DECISION_CLASS_DEFINITIVELY_ABORTED {
		return codes.Aborted
	}
	return codes.Unknown
}

func forwardDecisionMessage(reason pb.ForwardFailureReason) string {
	if reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_CONFLICT {
		return "forward conflict"
	}
	if reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_FENCE {
		return "forward fence"
	}
	if reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_NO_LEADER {
		return "forward leader unavailable"
	}
	if reason == pb.ForwardFailureReason_FORWARD_FAILURE_REASON_INTERRUPTED {
		return "forward interrupted"
	}
	return "forward aborted"
}
