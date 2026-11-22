package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// These witnesses have different scopes: a request can be rejected after an
// earlier request in the same logical mutation committed. Only the coordinator
// can promote a request witness to a whole-mutation result.
type dataRequestNotSubmitted struct{ cause error }

func (e *dataRequestNotSubmitted) Error() string { return "data request was not submitted" }
func (e *dataRequestNotSubmitted) Unwrap() error { return e.cause }

type mutationNotAdmitted struct{ cause error }

func (e *mutationNotAdmitted) Error() string { return "mutation was not admitted" }
func (e *mutationNotAdmitted) Unwrap() error { return e.cause }

var secondaryNonadmissionExample = errors.WithSecondaryError(ErrInvalidRequest, ErrInvalidRequest)

// IsDataRequestNotSubmitted proves only the current single raw request was
// rejected before data proposal; it says nothing about other requests.
func IsDataRequestNotSubmitted(err error) bool { return isolatedNonadmission(err, false) }

// IsMutationNotAdmitted proves no request of this dispatch was submitted.
// Additional or hidden secondary errors invalidate that proof.
func IsMutationNotAdmitted(err error) bool { return isolatedNonadmission(err, true) }

func isolatedNonadmission(err error, mutation bool) bool {
	if !isolatedMutationFailure(err) {
		return false
	}
	var logical *mutationNotAdmitted
	var request *dataRequestNotSubmitted
	found := (mutation && errors.As(err, &logical)) || (!mutation && errors.As(err, &request))
	return found
}

func isolatedMutationFailure(err error) bool {
	if err == nil || errors.Is(err, ErrTransactionOutcomeUnknown) || errors.HasType(err, secondaryNonadmissionExample) {
		return false
	}
	for current := err; current != nil; current = errors.UnwrapOnce(current) {
		if _, joined := current.(interface{ Unwrap() []error }); joined {
			return false
		}
	}
	return true
}

// IsMigrationAdmissionRejection recognizes an isolated migration denial.
// It is not by itself proof of noncommit: the caller must be before proposal.
func IsMigrationAdmissionRejection(err error) bool {
	return isolatedMutationFailure(err) && (errors.Is(err, ErrRouteWriteFenced) || errors.Is(err, ErrRouteCutoverPending))
}

func markDataRequestNotSubmitted(reqs []*pb.Request, err error) error {
	if !IsMigrationAdmissionRejection(err) || len(reqs) != 1 || reqs[0] == nil || reqs[0].IsTxn || reqs[0].Phase != pb.Phase_NONE {
		return err
	}
	return &dataRequestNotSubmitted{cause: err}
}

func singleRawPointMutation(reqs *OperationGroup[OP]) bool {
	return reqs != nil && !reqs.IsTxn && reqs.PrevCommitTS == 0 && len(reqs.Elems) == 1 && reqs.Elems[0] != nil && reqs.Elems[0].GroupID == 0 && (reqs.Elems[0].Op == Put || reqs.Elems[0].Op == Del)
}

func markRawPointAdmission(reqs *OperationGroup[OP], err error) error {
	if singleRawPointMutation(reqs) && IsMigrationAdmissionRejection(err) {
		return &dataRequestNotSubmitted{cause: err}
	}
	return err
}

func rawPointAdmissionShape(reqs *OperationGroup[OP], err error) rawDispatchShape {
	if singleRawPointMutation(reqs) && IsDataRequestNotSubmitted(err) {
		return rawDispatchShape{groupCount: 1, requestCount: 1}
	}
	return rawDispatchShape{}
}

func (c *ShardedCoordinator) initialRawAdmission(reqs *OperationGroup[OP]) error {
	if !singleRawPointMutation(reqs) {
		return nil
	}
	if err := c.rejectWriteFencedPointElems(reqs.Elems); err != nil {
		return &mutationNotAdmitted{cause: err}
	}
	return nil
}
