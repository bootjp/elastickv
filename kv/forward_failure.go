package kv

import pb "github.com/bootjp/elastickv/proto"

// ForwardFailure keeps the validated wire reason separate from the logical
// outcome. The current Forward producer cannot exclude earlier attempts, so
// only outcome-unknown failures are constructed from this detail.
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
