package kv

import pb "github.com/bootjp/elastickv/proto"

// ForwardFailurePhaseForRequests selects a single phase only when the batch
// contains exactly one request. Mixed batches cannot carry a decision detail.
func ForwardFailurePhaseForRequests(reqs []*pb.Request) (pb.ForwardFailurePhase, bool) {
	if len(reqs) != 1 || reqs[0] == nil {
		return 0, false
	}
	req := reqs[0]
	if !req.IsTxn && req.Phase != pb.Phase_NONE {
		return 0, false
	}
	switch req.Phase {
	case pb.Phase_NONE:
		return pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY, true
	case pb.Phase_PREPARE:
		return pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_PREPARE, true
	case pb.Phase_COMMIT:
		return pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_COMMIT, true
	case pb.Phase_ABORT:
		return pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ABORT, true
	default:
		return 0, false
	}
}

// ForwardOnePhaseDecisionEligible rejects known prior attempts before an
// indexed apply rejection is promoted to a logical noncommit decision. It
// does not reconcile a separate caller invocation without a durable identity.
func ForwardOnePhaseDecisionEligible(reqs []*pb.Request) bool {
	phase, ok := ForwardFailurePhaseForRequests(reqs)
	if !ok || phase != pb.ForwardFailurePhase_FORWARD_FAILURE_PHASE_ONE_PHASE_APPLY {
		return false
	}
	req := reqs[0]
	if !req.IsTxn {
		return true
	}
	return forwardTxnHasNoPriorAttempt(req)
}

func forwardTxnHasNoPriorAttempt(req *pb.Request) bool {
	if len(req.Mutations) == 0 || req.Mutations[0] == nil || req.Mutations[0].Op != pb.Op_PUT {
		return false
	}
	meta, mutations, err := extractTxnMeta(req.Mutations)
	if err != nil || len(meta.PrimaryKey) == 0 || meta.PrevCommitTS != 0 {
		return false
	}
	for _, mut := range mutations {
		if mut == nil || isTxnMetaKey(mut.Key) {
			return false
		}
	}
	return true
}
