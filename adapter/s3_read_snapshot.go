package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// fencedObjectReadTS samples only after the relevant groups' committed-log
// barriers. A follower-local watermark can predate a successful remote PUT,
// even if a later store read correctly forwards that old timestamp to a leader.
func (s *S3Server) fencedObjectReadTS(ctx context.Context, keys ...[]byte) (uint64, error) {
	stamp, err := beginFencedReadSnapshot(ctx, s.coordinator, s.store, "s3 object read", keys...)
	return stamp.Timestamp(), err
}

// objectReadSnapshot fences bucket metadata and the current generation's object
// group, then re-reads metadata at their common timestamp. Retry if recreation
// changed the generation while its object group was being resolved.
func (s *S3Server) objectReadSnapshot(ctx context.Context, bucket, objectKey string) (*s3BucketMeta, uint64, *kv.ActiveTimestampToken, error) {
	bucketKey := s3keys.BucketMetaKey(bucket)
	for range 3 {
		ts, err := s.fencedObjectReadTS(ctx, bucketKey)
		if err != nil {
			return nil, 0, nil, err
		}
		pin := s.pinReadTS(ts)
		meta, exists, err := s.loadBucketMetaAt(ctx, bucket, ts)
		if err != nil {
			pin.Release()
			return nil, 0, nil, err
		}
		if !exists || meta == nil {
			return nil, ts, pin, nil
		}
		pin.Release()
		headKey := s3keys.ObjectManifestKey(bucket, meta.Generation, objectKey)
		ts, err = s.fencedObjectReadTS(ctx, bucketKey, headKey)
		if err != nil {
			return nil, 0, nil, err
		}
		pin = s.pinReadTS(ts)
		current, exists, err := s.loadBucketMetaAt(ctx, bucket, ts)
		if err != nil {
			pin.Release()
			return nil, 0, nil, err
		}
		if s3BucketGenerationUnchanged(current, exists, meta.Generation) {
			return current, ts, pin, nil
		}
		pin.Release()
	}
	return nil, 0, nil, errors.New("s3 object read: bucket generation kept changing")
}

func (s *S3Server) snapshotReadTS(ctx context.Context) (uint64, error) {
	stamp, err := beginFencedReadSnapshot(ctx, s.coordinator, s.store, "S3Server read")
	return stamp.Timestamp(), err
}

func s3BucketGenerationUnchanged(meta *s3BucketMeta, exists bool, generation uint64) bool {
	return !exists || meta == nil || meta.Generation == generation
}
