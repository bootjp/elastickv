package kv

import "github.com/cockroachdb/errors"

// unknownTransactionFailure retains a local cause for classification without
// allowing its diagnostic text or physical key to become a public message.
type unknownTransactionFailure struct {
	cause error
}

func (f *unknownTransactionFailure) Error() string {
	return ErrTransactionOutcomeUnknown.Error()
}

func (f *unknownTransactionFailure) Is(target error) bool {
	return target == ErrTransactionOutcomeUnknown || errors.Is(f.cause, target)
}
