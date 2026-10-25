package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (d *DynamoDBServer) dispatchWriteWithReadTimestamp(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := kv.DispatchWithReadTimestamp(ctx, d.coordinator, req)
	return resp, normalizeDynamoWriteDispatchError(err)
}

// Restore only structured forwarded rejections so item retries retain the
// existing exact-timestamp dedup path without parsing display messages.
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
	return err
}
