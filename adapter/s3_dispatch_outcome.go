package adapter

import (
	"context"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func (s *S3Server) dispatchS3MetadataMutation(ctx context.Context, req *kv.OperationGroup[kv.OP]) error {
	_, err := kv.DispatchWithReadTimestamp(ctx, s.coordinator, req)
	return normalizeS3MetadataDispatchError(err)
}

func normalizeS3MetadataDispatchError(err error) error {
	if err == nil || errors.Is(err, kv.ErrTransactionOutcomeUnknown) {
		return err
	}
	if isIndeterminateMutationDispatchFailure(err) {
		return errors.WithStack(kv.ErrTransactionOutcomeUnknown)
	}
	return err
}
