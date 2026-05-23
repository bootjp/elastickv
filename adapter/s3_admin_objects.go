package adapter

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETag compatibility requires MD5.
	"encoding/hex"
	"io"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// adminS3UploadMaxBytes caps single-PUT admin uploads at 100 MiB
// per the design doc §3.3.3. Larger objects need the AWS SDK
// multipart path against the public endpoint. Enforced at the
// HTTP boundary too (Phase 3) via http.MaxBytesReader; this
// in-process cap is the second line of defence for direct Go
// callers (tests, future bridges).
const adminS3UploadMaxBytes int64 = 100 * 1024 * 1024

// ErrAdminUploadTooLarge signals AdminPutObject received a body
// larger than adminS3UploadMaxBytes. The HTTP bridge maps this
// to 413 Payload Too Large.
var ErrAdminUploadTooLarge = errors.New("s3 admin: object exceeds upload cap")

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

// AdminPutObject creates-or-replaces one object via a streaming
// upload. Write role required. Caps body size at adminS3UploadMaxBytes
// (100 MiB) per design §3.3.3; larger objects must use the public
// SigV4 endpoint with the SDK's multipart path.
//
// contentType is stored verbatim on the manifest (defaults to
// application/octet-stream when empty). body is consumed exactly
// once; the caller owns its lifecycle (close, etc.) — AdminPutObject
// does NOT close the reader.
//
// Sentinels:
//   - ErrAdminForbidden          — principal lacks write role
//   - ErrAdminNotLeader          — follower
//   - ErrAdminBucketNotFound     — bucket absent
//   - ErrAdminInvalidObjectKey   — empty key
//   - ErrAdminInvalidBucketName  — malformed bucket name
//   - ErrAdminUploadTooLarge     — body exceeds adminS3UploadMaxBytes
func (s *S3Server) AdminPutObject(ctx context.Context, principal AdminPrincipal, bucket, key string, body io.Reader, contentType string) error {
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
	if body == nil {
		body = strings.NewReader("")
	}

	previous, generation, err := s.adminPutObjectStream(ctx, bucket, key, body, contentType)
	if err != nil {
		return err //nolint:wrapcheck // sentinels propagate as-is; wrapped values already carry context.
	}
	if previous != nil {
		s.cleanupManifestBlobsAsync(bucket, generation, key, previous)
	}
	return nil
}

// adminPutObjectStream performs the chunked upload + manifest
// commit in a single attempt (no retry — the chunk fan-out has
// no idempotency token, and a partial-then-retry would leak
// blobs that adminCleanupManifestBlobs on the leaked uploadID
// would never reach).
//
// Returns (previousManifest, generation, err); previousManifest
// is non-nil when the put REPLACED an existing object and the
// caller should asynchronously clean up its blob chunks.
//
// Mirrors the SigV4 putObject precedent at s3.go (which carries
// the same `//nolint:cyclop,gocognit,gocyclo,nestif` because the
// chunk pipeline is a sequential N-stage flow with per-stage
// cleanup-on-error that needs the local manifest view at every
// stage; splitting further would obscure the lifecycle).
//
//nolint:cyclop,gocognit,nestif // see comment above
func (s *S3Server) adminPutObjectStream(ctx context.Context, bucket, key string, body io.Reader, contentType string) (*s3ObjectManifest, uint64, error) {
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
	previous, _, err := s.loadObjectManifestAt(ctx, headKey, readTS)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}

	uploadID := newS3UploadID(s.clock())
	hasher := md5.New() //nolint:gosec // S3 ETag compatibility requires MD5.
	part := s3ObjectPart{PartNo: 1}
	sizeBytes := int64(0)
	chunkNo := uint64(0)
	buf := make([]byte, s3ChunkSize)
	pendingBatch := make([]*kv.Elem[kv.OP], 0, s3ChunkBatchOps)
	pendingChunkSizes := make([]uint64, 0, s3ChunkBatchOps)

	uploadedManifest := func() *s3ObjectManifest {
		if len(part.ChunkSizes) == 0 {
			return &s3ObjectManifest{UploadID: uploadID}
		}
		clonedPart := part
		clonedPart.ChunkSizes = append([]uint64(nil), part.ChunkSizes...)
		clonedPart.ChunkCount = uint64(len(clonedPart.ChunkSizes))
		return &s3ObjectManifest{UploadID: uploadID, Parts: []s3ObjectPart{clonedPart}}
	}
	flushBatch := func() error {
		if len(pendingBatch) == 0 {
			return nil
		}
		if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: pendingBatch}); err != nil {
			return errors.WithStack(err)
		}
		part.ChunkSizes = append(part.ChunkSizes, pendingChunkSizes...)
		part.ChunkCount = uint64(len(part.ChunkSizes))
		pendingBatch = pendingBatch[:0]
		pendingChunkSizes = pendingChunkSizes[:0]
		return nil
	}

	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			if sizeBytes+int64(n) > adminS3UploadMaxBytes {
				s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
				return nil, 0, ErrAdminUploadTooLarge
			}
			chunk := append([]byte(nil), buf[:n]...)
			if _, err := hasher.Write(chunk); err != nil {
				s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
				return nil, 0, errors.WithStack(err)
			}
			chunkKey := s3keys.BlobKey(bucket, meta.Generation, key, uploadID, part.PartNo, chunkNo)
			pendingBatch = append(pendingBatch, &kv.Elem[kv.OP]{Op: kv.Put, Key: chunkKey, Value: chunk})
			chunkSize, err := uint64FromInt(n)
			if err != nil {
				s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
				return nil, 0, errors.WithStack(err)
			}
			pendingChunkSizes = append(pendingChunkSizes, chunkSize)
			if len(pendingBatch) >= s3ChunkBatchOps {
				if err := flushBatch(); err != nil {
					s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
					return nil, 0, err
				}
			}
			sizeBytes += int64(n)
			chunkNo++
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
			return nil, 0, errors.WithStack(readErr)
		}
	}
	if err := flushBatch(); err != nil {
		s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
		return nil, 0, err
	}

	etag := hex.EncodeToString(hasher.Sum(nil))
	part.ETag = etag
	part.SizeBytes = sizeBytes
	part.ChunkCount = uint64(len(part.ChunkSizes))
	commitTS, err := s.nextTxnCommitTS(startTS)
	if err != nil {
		s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
		return nil, 0, errors.WithStack(err)
	}
	ct := strings.TrimSpace(contentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	manifest := &s3ObjectManifest{
		UploadID:        uploadID,
		ETag:            etag,
		SizeBytes:       sizeBytes,
		LastModifiedHLC: commitTS,
		ContentType:     ct,
	}
	if sizeBytes > 0 {
		manifest.Parts = []s3ObjectPart{part}
	}
	bodyEnc, err := encodeS3ObjectManifest(manifest)
	if err != nil {
		s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
		return nil, 0, errors.WithStack(err)
	}
	bucketFence, err := encodeS3BucketMeta(meta)
	if err != nil {
		s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
		return nil, 0, errors.WithStack(err)
	}
	if _, err := s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: s3keys.BucketMetaKey(bucket), Value: bucketFence},
			{Op: kv.Put, Key: headKey, Value: bodyEnc},
		},
	}); err != nil {
		s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
		return nil, 0, errors.WithStack(err)
	}
	return previous, meta.Generation, nil
}
