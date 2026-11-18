package kv

import (
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const forwardErrorDomain = "elastickv.kv.forward"

const (
	forwardWriteConflictReason = "WRITE_CONFLICT"
	forwardTxnLockedReason     = "TXN_LOCKED"
	forwardNotLeaderReason     = "NOT_LEADER_BEFORE_SUBMIT"
	forwardComposed1Reason     = "COMPOSED1_VIOLATION"
	forwardComposed1GCdReason  = "COMPOSED1_VERSION_GCD"
)

// ForwardWriteConflictStatus is the legacy wire encoding, retained to model
// old peers. Its ErrorInfo lacks phase and decision proof and cannot certify
// a retry, even when received by Internal.Forward.
func ForwardWriteConflictStatus() error {
	return forwardStatus(codes.Aborted, store.ErrWriteConflict.Error(), forwardWriteConflictReason)
}

// ForwardTxnLockedStatus is the legacy wire encoding for a lock cause.
func ForwardTxnLockedStatus() error {
	return forwardStatus(codes.Aborted, ErrTxnLocked.Error(), forwardTxnLockedReason)
}

// ForwardComposed1Status preserves a route rejection without parsing its
// diagnostic text after forwarding.
func ForwardComposed1Status(err error) error {
	switch {
	case errors.Is(err, ErrComposed1Violation):
		return forwardStatus(codes.FailedPrecondition, ErrComposed1Violation.Error(), forwardComposed1Reason)
	case errors.Is(err, ErrComposed1VersionGCd):
		return forwardStatus(codes.FailedPrecondition, ErrComposed1VersionGCd.Error(), forwardComposed1GCdReason)
	default:
		return err
	}
}

// ForwardNotLeaderStatus is the legacy wire encoding for a leader cause.
func ForwardNotLeaderStatus() error {
	return forwardStatus(codes.Unavailable, "not leader", forwardNotLeaderReason)
}

func forwardStatus(code codes.Code, message, reason string) error {
	st, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{
		Domain: forwardErrorDomain,
		Reason: reason,
	})
	if err != nil {
		return errors.WithStack(status.Error(codes.Internal, "forward status detail unavailable"))
	}
	return errors.WithStack(st.Err())
}
