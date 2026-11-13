package adapter

import (
	"context"
	"io"
	"net"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/status"
)

func (d *DynamoDBServer) dispatchWriteWithReadTimestamp(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := kv.DispatchWithReadTimestamp(ctx, d.coordinator, req)
	return resp, normalizeDynamoWriteDispatchError(err)
}

// Restore only structured forwarded rejections for the exact-timestamp dedup
// path. A status or lost response without a decision cannot prove noncommit.
func normalizeDynamoWriteDispatchError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return err
	}
	if kv.IsForwardWriteConflict(err) {
		return errors.WithStack(errors.Mark(err, store.ErrWriteConflict))
	}
	if kv.IsForwardTxnLocked(err) {
		return errors.WithStack(errors.Mark(err, kv.ErrTxnLocked))
	}
	if isIndeterminateDynamoWriteDispatch(err) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	return err
}

func isIndeterminateDynamoWriteDispatch(err error) bool {
	if _, ok := status.FromError(err); ok {
		return true
	}
	var netErr net.Error
	return errors.Is(err, raftengine.ErrLeadershipLost) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.As(err, &netErr)
}
