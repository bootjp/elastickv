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
	type statusCarrier interface{ GRPCStatus() *status.Status }
	var carrier statusCarrier
	if !errors.As(err, &carrier) {
		return err
	}
	st := carrier.GRPCStatus()
	if st == nil || (st.Code() != codes.Unknown && st.Code() != codes.Aborted) || !strings.HasPrefix(st.Message(), "key: ") {
		return err
	}
	for _, sentinel := range []error{store.ErrWriteConflict, kv.ErrTxnLocked} {
		if strings.HasSuffix(st.Message(), ": "+sentinel.Error()) {
			return errors.WithStack(errors.Mark(err, sentinel))
		}
	}
	return err
}
