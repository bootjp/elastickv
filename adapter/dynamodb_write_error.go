package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func (d *DynamoDBServer) dispatchWriteWithReadTimestamp(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := kv.DispatchWithReadTimestamp(ctx, d.coordinator, req)
	return resp, normalizeDynamoWriteDispatchError(err)
}

// Forward has already classified any authenticated decision at the KV boundary.
// A raw status or lost response cannot prove noncommit and stays unknown here.
func normalizeDynamoWriteDispatchError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return err
	}
	if isIndeterminateMutationDispatchFailure(err) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	return err
}
