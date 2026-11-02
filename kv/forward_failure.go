package kv

import pb "github.com/bootjp/elastickv/proto"

// ForwardFailure keeps a validated unknown wire reason separate from the
// logical outcome. Only proven one-phase apply conflicts are reconstructed
// as definitive errors by the receiver.
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
