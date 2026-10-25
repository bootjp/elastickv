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

// ForwardWriteConflictStatus marks a conflict rejected before commit. Storage
// keys remain in store errors and never cross the forwarding seam.
func ForwardWriteConflictStatus() error {
	return forwardStatus(codes.Aborted, store.ErrWriteConflict.Error(), forwardWriteConflictReason)
}

// ForwardTxnLockedStatus marks a transaction rejected by an existing lock.
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

// ForwardNotLeaderStatus marks a request rejected before timestamp admission
// or transaction submission on the receiving node.
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

// IsForwardWriteConflict accepts only the status produced by the forwarding
// endpoint, not an arbitrary Aborted status with similar display text.
func IsForwardWriteConflict(err error) bool {
	return hasForwardStatusDetail(err, codes.Aborted, forwardWriteConflictReason)
}

// IsForwardTxnLocked identifies a definitive lock rejection.
func IsForwardTxnLocked(err error) bool {
	return hasForwardStatusDetail(err, codes.Aborted, forwardTxnLockedReason)
}

// IsForwardComposed1Violation identifies a route rejection by its detail.
func IsForwardComposed1Violation(err error) bool {
	return hasForwardStatusDetail(err, codes.FailedPrecondition, forwardComposed1Reason)
}

// IsForwardComposed1VersionGCd identifies an expired-route rejection.
func IsForwardComposed1VersionGCd(err error) bool {
	return hasForwardStatusDetail(err, codes.FailedPrecondition, forwardComposed1GCdReason)
}

// IsForwardNotLeader identifies the pre-submission leader rejection.
func IsForwardNotLeader(err error) bool {
	return hasForwardStatusDetail(err, codes.Unavailable, forwardNotLeaderReason)
}

func hasForwardStatusDetail(err error, code codes.Code, reason string) bool {
	_, ok := forwardStatusInfo(err, code, reason)
	return ok
}

func forwardStatusInfo(err error, code codes.Code, reason string) (*errdetails.ErrorInfo, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != code {
		return nil, false
	}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok &&
			info.GetDomain() == forwardErrorDomain && info.GetReason() == reason {
			return info, true
		}
	}
	return nil, false
}
