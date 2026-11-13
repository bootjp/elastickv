package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func (s *SQSServer) dispatchSQSMutation(ctx context.Context, req *kv.OperationGroup[kv.OP]) error {
	_, err := kv.DispatchWithReadTimestamp(ctx, s.coordinator, req)
	return normalizeSQSDispatchError(err)
}

func normalizeSQSDispatchError(err error) error {
	if err == nil || errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return err
	}
	if isIndeterminateMutationDispatchFailure(err) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	return err
}
