package adapter

import "context"

// RequestOwnership retains receiver-local admission for a request and all of
// its children. Context carries that existing ownership downstream; consumers
// must not replace it with fresh admission when queued work becomes stale.
// Validate checks before work and publication. Finish validates the terminal
// result after joining owned children, without discarding the original error.
// CancelAndJoin cancels and joins
// owned children and is required on every exit, including panic and detachment.
// Implementations must keep authority alive until child joins and result checks
// complete; neither this interface nor its context grants wire authority.
type RequestOwnership interface {
	Context() context.Context
	Validate() error
	Finish(error) error
	CancelAndJoin()
}

// RequestAdmission is injected at startup. A nil hook preserves legacy behavior.
// A successful hook must return non-nil ownership with a non-nil Context.
type RequestAdmission func(context.Context) (RequestOwnership, error)
