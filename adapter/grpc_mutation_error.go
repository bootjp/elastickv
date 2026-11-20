package adapter

import (
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const publicGRPCFailureDomain = "elastickv.public.grpc"

const (
	publicGRPCNotAdmittedReason = "NOT_ADMITTED"
	publicGRPCAbortedReason     = "PROVEN_NONCOMMIT"
	publicGRPCUnknownReason     = "INDETERMINATE"
)

func projectPublicGRPCMutationError(err error) error {
	if err == nil {
		return nil
	}
	code, message, reason := publicGRPCMutationFailure(err)
	st, detailErr := status.New(code, message).WithDetails(&errdetails.ErrorInfo{
		Domain: publicGRPCFailureDomain,
		Reason: reason,
	})
	if detailErr != nil {
		//nolint:wrapcheck // Wrapping rewrites the public gRPC status message.
		return status.Error(codes.Unknown, "Operation outcome unknown")
	}
	//nolint:wrapcheck // Preserve the fixed message and public failure detail.
	return st.Err()
}

func publicGRPCMutationFailure(err error) (codes.Code, string, string) {
	if kv.IsMutationNotAdmitted(err) {
		return codes.Unavailable, "Route unavailable", publicGRPCNotAdmittedReason
	}
	if kv.IsMutationApplyRejected(err) {
		return codes.Aborted, "Operation aborted", publicGRPCAbortedReason
	}
	if uncertainPublicMutationFailure(err) {
		return codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason
	}
	switch {
	case errors.Is(err, kv.ErrLeaderProxyCircuitOpen):
		return codes.Unavailable, "Route unavailable", publicGRPCNotAdmittedReason
	case errors.Is(err, store.ErrWriteConflict):
		return codes.Aborted, "Operation aborted", publicGRPCAbortedReason
	}
	return codes.Unknown, "Operation outcome unknown", publicGRPCUnknownReason
}

func uncertainPublicMutationFailure(err error) bool {
	return errors.Is(err, kv.ErrTransactionOutcomeUnknown) || errors.Is(err, kv.ErrRouteWriteFenced) || errors.Is(err, kv.ErrTxnLocked) || isIndeterminateMutationDispatchFailure(err)
}
