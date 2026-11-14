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
	code := codes.Unknown
	message := "Operation outcome unknown"
	reason := publicGRPCUnknownReason
	if !errors.Is(err, kv.ErrTransactionOutcomeUnknown) &&
		!errors.Is(err, kv.ErrRouteWriteFenced) &&
		!errors.Is(err, kv.ErrTxnLocked) &&
		!isIndeterminateMutationDispatchFailure(err) {
		switch {
		case errors.Is(err, kv.ErrTimestampBindingInvalid):
			code = codes.FailedPrecondition
			message = "Timestamp binding admission unavailable"
			reason = publicGRPCNotAdmittedReason
		case errors.Is(err, kv.ErrLeaderProxyCircuitOpen):
			code = codes.Unavailable
			message = "Route unavailable"
			reason = publicGRPCNotAdmittedReason
		case errors.Is(err, store.ErrWriteConflict):
			code = codes.Aborted
			message = "Operation aborted"
			reason = publicGRPCAbortedReason
		}
	}
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
