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
