package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (s *S3Server) dispatchBucketDeleteTxn(ctx context.Context, snapshot kv.ReadTimestamp, bucket string, generation uint64) error {
	elems, err := s.bucketDeleteTxnElems(ctx, bucket, generation, snapshot.Timestamp())
	if err != nil {
		return err
	}
	_, err = kv.DispatchWithReadTimestamp(snapshot.WithDispatchVoucher(ctx), s.coordinator, &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: snapshot.Timestamp(), Elems: elems,
	})
	return errors.WithStack(err)
}

// Every object writer updates bucket metadata, but only create/delete update
// the generation key. Re-putting it fences streaming parts without making
// unrelated object writes abort them; preserving its maximum prevents reuse.
func (s *S3Server) bucketDeleteTxnElems(ctx context.Context, bucket string, generation, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	key := s3keys.BucketGenerationKey(bucket)
	raw, err := s.store.GetAt(ctx, key, readTS)
	switch {
	case errors.Is(err, store.ErrKeyNotFound):
	case err != nil:
		return nil, errors.WithStack(err)
	default:
		stored, err := decodeS3Generation(raw)
		if err != nil {
			return nil, err
		}
		generation = max(generation, stored)
	}
	return []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: s3keys.BucketMetaKey(bucket)},
		{Op: kv.Put, Key: key, Value: encodeS3Generation(generation)},
	}, nil
}
