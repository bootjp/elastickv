package kv

import (
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

// ForwardFailure keeps a validated unknown wire reason separate from the
// logical outcome. Only proven one-phase apply conflicts and route fences are
// reconstructed as definitive errors by the receiver.
type ForwardFailure struct {
	Reason   pb.ForwardFailureReason
	Decision pb.ForwardDecisionClass
}

func (f *ForwardFailure) Error() string {
	return ErrTransactionOutcomeUnknown.Error()
}

func (f *ForwardFailure) Is(target error) bool {
	return target == ErrTransactionOutcomeUnknown
}

// ForwardApplyRejection marks a phase-bound, validated Forward verdict for
// one submitted one-phase entry. A plain error with the same cause carries no
// such evidence and may represent an earlier attempt that already committed.
type ForwardApplyRejection struct {
	cause error
}

func (r *ForwardApplyRejection) Error() string { return r.cause.Error() }

func (r *ForwardApplyRejection) Unwrap() error { return r.cause }

// IsCertifiedOnePhaseConflict distinguishes a proven apply rejection from a
// raw conflict that could have followed a committed write.
func IsCertifiedOnePhaseConflict(err error) bool {
	if !isolatedMutationFailure(err) {
		return false
	}
	var applied *AppliedRequestRejection
	var forwarded *ForwardApplyRejection
	return (errors.As(err, &applied) && applied.CommitIndex() != 0) || errors.As(err, &forwarded)
}
