package adapter

import (
	"context"
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (d *DynamoDBServer) dispatchWriteWithReadTimestamp(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := kv.DispatchWithReadTimestamp(ctx, d.coordinator, req)
	return resp, normalizeDynamoWriteDispatchError(err)
}

// Forwarded OCC conflicts lose their Go error chain. Restore only the known
// wire envelope so item retries retain the existing exact-timestamp dedup path.
func normalizeDynamoWriteDispatchError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return err
	}
	type statusCarrier interface{ GRPCStatus() *status.Status }
	var carrier statusCarrier
	if !errors.As(err, &carrier) {
		return err
	}
	st := carrier.GRPCStatus()
	if st == nil {
		return err
	}
	if sentinel := forwardedDynamoWriteSentinel(st); sentinel != nil {
		return errors.WithStack(errors.Mark(err, sentinel))
	}
	return err
}

func forwardedDynamoWriteSentinel(st *status.Status) error {
	message := forwardedStatusMessage(st)
	if st.Code() == codes.Aborted {
		marker := store.ErrWriteConflict.Error()
		if message == marker || (strings.HasPrefix(message, "key: ") && strings.HasSuffix(message, ": "+marker)) {
			return store.ErrWriteConflict
		}
	}
	if (st.Code() != codes.Unknown && st.Code() != codes.Aborted) || !strings.HasPrefix(message, "key: ") {
		return nil
	}
	for _, sentinel := range []error{store.ErrWriteConflict, kv.ErrTxnLocked} {
		if strings.HasSuffix(message, ": "+sentinel.Error()) {
			return sentinel
		}
	}
	return nil
}
