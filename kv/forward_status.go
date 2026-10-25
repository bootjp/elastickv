package kv

import (
	"encoding/base64"

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

// ForwardWriteConflictStatus marks a conflict rejected by the leader before
// the forwarded transaction can commit. A message alone is never this proof.
func ForwardWriteConflictStatus(err error) error {
	metadata := map[string]string{}
	if key, ok := store.WriteConflictKey(err); ok {
		metadata["key_b64"] = base64.StdEncoding.EncodeToString(key)
	}
	return forwardStatus(codes.Aborted, err.Error(), forwardWriteConflictReason, metadata)
}

// ForwardTxnLockedStatus marks a transaction rejected by an existing lock.
func ForwardTxnLockedStatus(err error) error {
	metadata := map[string]string{}
	if key, detail, ok := TxnLockedDetails(err); ok {
		metadata["key_b64"] = base64.StdEncoding.EncodeToString(key)
		metadata["detail"] = detail
	}
	return forwardStatus(codes.Aborted, err.Error(), forwardTxnLockedReason, metadata)
}

// ForwardComposed1Status preserves a route rejection without parsing its
// diagnostic text after forwarding.
func ForwardComposed1Status(err error) error {
	switch {
	case errors.Is(err, ErrComposed1Violation):
		return forwardStatus(codes.FailedPrecondition, err.Error(), forwardComposed1Reason, nil)
	case errors.Is(err, ErrComposed1VersionGCd):
		return forwardStatus(codes.FailedPrecondition, err.Error(), forwardComposed1GCdReason, nil)
	default:
		return err
	}
}

// ForwardNotLeaderStatus marks a request rejected before timestamp admission
// or transaction submission on the receiving node.
func ForwardNotLeaderStatus() error {
	return forwardStatus(codes.Unavailable, "not leader", forwardNotLeaderReason, nil)
}

func forwardStatus(code codes.Code, message, reason string, metadata map[string]string) error {
	st, err := status.New(code, message).WithDetails(&errdetails.ErrorInfo{
		Domain:   forwardErrorDomain,
		Reason:   reason,
		Metadata: metadata,
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

// ForwardWriteConflictKey returns the rejected predicate key when the leader
// supplied it as structured metadata.
func ForwardWriteConflictKey(err error) ([]byte, bool) {
	info, ok := forwardStatusInfo(err, codes.Aborted, forwardWriteConflictReason)
	if !ok {
		return nil, false
	}
	return decodeForwardKey(info)
}

// IsForwardTxnLocked identifies a definitive lock rejection.
func IsForwardTxnLocked(err error) bool {
	return hasForwardStatusDetail(err, codes.Aborted, forwardTxnLockedReason)
}

// ForwardTxnLockedDetails returns the structured rejected key and lock detail.
func ForwardTxnLockedDetails(err error) ([]byte, string, bool) {
	info, ok := forwardStatusInfo(err, codes.Aborted, forwardTxnLockedReason)
	if !ok {
		return nil, "", false
	}
	key, ok := decodeForwardKey(info)
	return key, info.GetMetadata()["detail"], ok
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

func decodeForwardKey(info *errdetails.ErrorInfo) ([]byte, bool) {
	encoded, present := info.GetMetadata()["key_b64"]
	if !present {
		return nil, false
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	return key, err == nil
}
