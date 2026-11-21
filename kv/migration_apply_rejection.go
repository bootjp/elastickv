package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// migrationApplyRejection is minted only by the FSM's pre-data guards. Unlike
// a preproposal check, this request was submitted and needs an indexed verdict.
type migrationApplyRejection struct{ cause error }

func (e *migrationApplyRejection) Error() string { return "raw migration apply guard rejected request" }
func (e *migrationApplyRejection) Unwrap() error { return e.cause }

type mutationApplyRejected struct{ cause error }

func (e *mutationApplyRejected) Error() string { return "raw mutation rejected at apply" }
func (e *mutationApplyRejected) Unwrap() error { return e.cause }

func markMigrationApplyRejection(request *pb.Request, err error) error {
	if request == nil || request.IsTxn || request.Phase != pb.Phase_NONE ||
		!IsMigrationAdmissionRejection(err) || errors.Is(err, errTargetReadinessUnproven) {
		return err
	}
	return &migrationApplyRejection{cause: err}
}

func rawIndexedMigrationRejection(reqs []*pb.Request, index uint64, err error) error {
	if index == 0 || len(reqs) != 1 || reqs[0] == nil || reqs[0].IsTxn || reqs[0].Phase != pb.Phase_NONE || !isolatedMutationFailure(err) {
		return err
	}
	var guard *migrationApplyRejection
	if !errors.As(err, &guard) {
		return err
	}
	return &AppliedRequestRejection{commitIndex: index, cause: err}
}

func indexedMigrationRejection(err error) bool {
	if !isolatedMutationFailure(err) {
		return false
	}
	var indexed *AppliedRequestRejection
	var guard *migrationApplyRejection
	if errors.As(err, &indexed) && indexed.CommitIndex() != 0 && errors.As(err, &guard) {
		return true
	}
	var forwarded *ForwardApplyRejection
	return errors.As(err, &forwarded) && errors.Is(err, ErrRouteWriteFenced)
}

// IsMutationApplyRejected proves the first complete raw dispatch has an
// indexed pre-data rejection. It does not authorize replay of an unknown call.
func IsMutationApplyRejected(err error) bool {
	var rejected *mutationApplyRejected
	return isolatedMutationFailure(err) && errors.As(err, &rejected)
}

func classifyFirstRawRejection(err error, shape rawDispatchShape, first bool) (error, bool) {
	if !first || shape.retried || !shape.singleRequest() {
		return nil, false
	}
	if IsDataRequestNotSubmitted(err) {
		return &mutationNotAdmitted{cause: err}, true
	}
	if indexedMigrationRejection(err) {
		return &mutationApplyRejected{cause: err}, true
	}
	return nil, false
}
