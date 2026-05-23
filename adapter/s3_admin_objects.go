package adapter

import (
	"context"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// Phase 2b — object-level admin RPCs on *S3Server. Mirrors the
// item-level admin surface DynamoDB ships in dynamodb_admin_items.go
// (Scan / Get / Put / Delete) and follows the same gating model:
// role check, leader check, delegation to the internal helpers the
// SigV4 path already exercises. The design lives in
// docs/design/2026_05_22_proposed_admin_data_browser.md §3.1.2.

// ErrAdminObjectNotFound signals that AdminGetObject / AdminDeleteObject
// targeted a missing object. AdminGetObject returns it as the error
// path; AdminDeleteObject treats absence as success (S3 delete is
// idempotent — matches AWS semantics and the SigV4 deleteObject
// path) so the sentinel does not surface on the delete return.
var ErrAdminObjectNotFound = errors.New("s3 admin: object not found")

// ErrAdminInvalidObjectKey signals an empty or otherwise malformed
// key. The HTTP bridge already rejects path-segment violations
// before reaching this layer; the sentinel exists so a direct Go
// caller (tests, future bridges) gets the documented contract.
var ErrAdminInvalidObjectKey = errors.New("s3 admin: invalid object key")

// AdminDeleteObject removes one object from a bucket. Write role
// required. Idempotent: a missing object returns nil (matches the
// SigV4 deleteObject path and AWS S3 semantics).
//
// Sentinels:
//   - ErrAdminForbidden          — principal lacks write role
//   - ErrAdminNotLeader          — follower
//   - ErrAdminBucketNotFound     — bucket absent
//   - ErrAdminInvalidObjectKey   — empty key
func (s *S3Server) AdminDeleteObject(ctx context.Context, principal AdminPrincipal, bucket, key string) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !s.isVerifiedS3Leader(ctx) {
		return ErrAdminNotLeader
	}
	if key == "" {
		return ErrAdminInvalidObjectKey
	}
	if err := validateS3BucketName(bucket); err != nil {
		return errors.Wrapf(ErrAdminInvalidBucketName, "%s", err.Error())
	}

	var cleanupManifest *s3ObjectManifest
	var generation uint64
	err := s.retryS3Mutation(ctx, func() error {
		manifest, gen, err := s.adminDeleteObjectTxn(ctx, bucket, key)
		if err != nil {
			return err
		}
		cleanupManifest = manifest
		generation = gen
		return nil
	})
	if err != nil {
		return err //nolint:wrapcheck // sentinels propagate as-is; wrapped values already carry context.
	}
	if cleanupManifest != nil {
		s.cleanupManifestBlobsAsync(bucket, generation, key, cleanupManifest)
	}
	return nil
}

// adminDeleteObjectTxn is the per-attempt body retryS3Mutation
// invokes. Returns (manifest, generation, err); manifest is non-nil
// only when the dispatch succeeded and there is blob-chunk cleanup
// to fire. (manifest=nil, err=nil) is the "object did not exist;
// idempotent no-op" return shape, matching the SigV4 deleteObject
// silent-no-op semantics and AWS S3.
func (s *S3Server) adminDeleteObjectTxn(ctx context.Context, bucket, key string) (*s3ObjectManifest, uint64, error) {
	readTS := s.readTS()
	startTS := s.txnStartTS(readTS)
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, readTS)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return nil, 0, ErrAdminBucketNotFound
	}

	headKey := s3keys.ObjectManifestKey(bucket, meta.Generation, key)
	manifest, found, err := s.loadObjectManifestAt(ctx, headKey, readTS)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	if !found {
		return nil, 0, nil
	}
	_, err = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:   true,
		StartTS: startTS,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Del, Key: headKey},
		},
	})
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return manifest, meta.Generation, nil
}
