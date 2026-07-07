package adapter

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETag compatibility requires MD5.
	"encoding/hex"
	"io"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
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

// defaultS3ContentType is the MIME type stored on / surfaced by
// admin RPCs when the caller (or the manifest) does not name one
// explicitly. Matches the AWS S3 default and the SigV4 putObject
// path's headerOrDefault fallback.
const defaultS3ContentType = "application/octet-stream"

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

// ErrAdminInvalidContinuationToken signals AdminListObjects got a
// continuation token that is either malformed, references a
// different bucket / generation / prefix / delimiter than the
// current request, or names a readTS that has been MVCC-GC'd
// (store.ErrReadTSCompacted on the underlying scan). All three
// shapes map to 400 at the HTTP bridge; the wrapped message
// distinguishes the cases for operator diagnostics
// (claude-bot r1 + gemini r1 mediums on PR #811: the earlier
// ErrAdminInvalidBucketName reuse conflated token errors with
// bucket-name errors).
var ErrAdminInvalidContinuationToken = errors.New("s3 admin: invalid continuation token")

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
	startTS, err := s.txnStartTS(ctx, readTS)
	if err != nil {
		return nil, 0, errors.Wrap(err, "s3 admin: allocate startTS for adminDeleteObjectTxn")
	}
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
	startTS, err := s.txnStartTS(ctx, readTS)
	if err != nil {
		return nil, 0, errors.Wrap(err, "s3 admin: allocate startTS for adminPutObjectStream")
	}
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
	commitTS, err := s.nextTxnCommitTS(ctx, startTS)
	if err != nil {
		s.cleanupManifestBlobs(ctx, bucket, meta.Generation, key, uploadedManifest())
		return nil, 0, errors.WithStack(err)
	}
	ct := strings.TrimSpace(contentType)
	if ct == "" {
		ct = defaultS3ContentType
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

// AdminObject is the admin-facing metadata projection. Mirrors the
// fields the design doc §3.1.2 names — no internal-only state
// (uploadID, chunk sizes, etc.) leaks to admin handlers / SPA.
type AdminObject struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ContentType  string    `json:"content_type"`
	ETag         string    `json:"etag"`
	LastModified time.Time `json:"last_modified"`
	StorageClass string    `json:"storage_class"`
}

// AdminGetObject fetches one object's body + metadata. Read role
// required. Returns (body, meta, nil) on success; the caller MUST
// Close the body to release the read-tracker pin.
//
// Sentinels:
//   - ErrAdminForbidden          — principal lacks read role
//   - ErrAdminNotLeader          — follower
//   - ErrAdminBucketNotFound     — bucket absent
//   - ErrAdminObjectNotFound     — object absent
//   - ErrAdminInvalidObjectKey   — empty key
//   - ErrAdminInvalidBucketName  — malformed bucket name
func (s *S3Server) AdminGetObject(ctx context.Context, principal AdminPrincipal, bucket, key string) (io.ReadCloser, AdminObject, error) {
	if !principal.Role.canRead() {
		return nil, AdminObject{}, ErrAdminForbidden
	}
	if !s.isVerifiedS3Leader(ctx) {
		return nil, AdminObject{}, ErrAdminNotLeader
	}
	if key == "" {
		return nil, AdminObject{}, ErrAdminInvalidObjectKey
	}
	if err := validateS3BucketName(bucket); err != nil {
		return nil, AdminObject{}, errors.Wrapf(ErrAdminInvalidBucketName, "%s", err.Error())
	}

	manifest, generation, readTS, readPin, err := s.adminLoadObjectForRead(ctx, bucket, key)
	if err != nil {
		return nil, AdminObject{}, err
	}

	body := newS3AdminObjectReader(ctx, s, bucket, generation, key, manifest, readTS, readPin)

	adminMeta := AdminObject{
		Key:          key,
		Size:         manifest.SizeBytes,
		ContentType:  manifest.ContentType,
		ETag:         manifest.ETag,
		LastModified: hlcToTime(manifest.LastModifiedHLC),
		StorageClass: "STANDARD",
	}
	if adminMeta.ContentType == "" {
		adminMeta.ContentType = defaultS3ContentType
	}
	return body, adminMeta, nil
}

// adminLoadObjectForRead centralises the read-path preamble after
// the gate checks: pin the read timestamp, load the bucket meta,
// load the object manifest. On any error path the pin is released
// before returning so the caller never has to handle release on
// the error path. On success the caller takes ownership of the
// pin (typically by handing it to the streaming reader's Close).
func (s *S3Server) adminLoadObjectForRead(ctx context.Context, bucket, key string) (*s3ObjectManifest, uint64, uint64, *kv.ActiveTimestampToken, error) {
	readTS := s.readTS()
	readPin := s.pinReadTS(readTS)
	releaseOnError := readPin
	defer func() {
		if releaseOnError != nil {
			releaseOnError.Release()
		}
	}()

	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, readTS)
	if err != nil {
		return nil, 0, 0, nil, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return nil, 0, 0, nil, ErrAdminBucketNotFound
	}

	headKey := s3keys.ObjectManifestKey(bucket, meta.Generation, key)
	manifest, found, err := s.loadObjectManifestAt(ctx, headKey, readTS)
	if err != nil {
		return nil, 0, 0, nil, errors.WithStack(err)
	}
	if !found || manifest == nil {
		return nil, 0, 0, nil, ErrAdminObjectNotFound
	}
	releaseOnError = nil
	return manifest, meta.Generation, readTS, readPin, nil
}

// s3AdminObjectReader streams an object's bytes by lazily fetching
// chunks from the store. Memory bounded to one chunk (~s3ChunkSize)
// regardless of total object size — the 100 MiB admin PUT cap
// doesn't apply to GET (older objects predating the cap may be
// larger).
//
// Lifecycle: holds a read-tracker pin (the readTS the manifest was
// loaded at) so concurrent MVCC GC cannot reclaim chunks mid-read.
// The pin is released by Close. Calling Close more than once is
// safe (the underlying token's Release is idempotent on nil).
type s3AdminObjectReader struct {
	ctx        context.Context
	server     *S3Server
	bucket     string
	generation uint64
	key        string
	manifest   *s3ObjectManifest
	readTS     uint64
	pin        *kv.ActiveTimestampToken

	// Cursor state.
	partIdx  int    // index into manifest.Parts
	chunkIdx uint64 // index within Parts[partIdx].ChunkSizes
	buf      []byte // unread bytes of the current chunk
	closed   bool
	err      error // sticky error after first Read failure
}

func newS3AdminObjectReader(
	ctx context.Context,
	server *S3Server,
	bucket string,
	generation uint64,
	key string,
	manifest *s3ObjectManifest,
	readTS uint64,
	pin *kv.ActiveTimestampToken,
) *s3AdminObjectReader {
	return &s3AdminObjectReader{
		ctx:        ctx,
		server:     server,
		bucket:     bucket,
		generation: generation,
		key:        key,
		manifest:   manifest,
		readTS:     readTS,
		pin:        pin,
	}
}

func (r *s3AdminObjectReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	if r.err != nil {
		return 0, r.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ensureBuffered(); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		r.err = err
		return 0, err
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

// ensureBuffered makes r.buf hold at least 1 unread byte from the
// next chunk, or returns io.EOF when the manifest is fully drained.
// Skips zero-byte chunks (defensive — shouldn't normally appear).
func (r *s3AdminObjectReader) ensureBuffered() error {
	if len(r.buf) > 0 {
		return nil
	}
	for r.partIdx < len(r.manifest.Parts) {
		part := r.manifest.Parts[r.partIdx]
		if r.chunkIdx >= uint64(len(part.ChunkSizes)) {
			r.partIdx++
			r.chunkIdx = 0
			continue
		}
		chunkKey := s3keys.VersionedBlobKey(r.bucket, r.generation, r.key,
			r.manifest.UploadID, part.PartNo, r.chunkIdx, part.PartVersion)
		chunk, err := r.server.store.GetAt(r.ctx, chunkKey, r.readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		r.chunkIdx++
		if len(chunk) == 0 {
			continue
		}
		r.buf = chunk
		return nil
	}
	return io.EOF
}

func (r *s3AdminObjectReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.pin != nil {
		r.pin.Release()
		r.pin = nil
	}
	return nil
}

// AdminListObjectsOptions controls one AdminListObjects call.
// Defaults match the design doc §3.1.2: MaxKeys=100, clamped to
// [1, adminListObjectsMaxKeys=1000].
type AdminListObjectsOptions struct {
	Prefix            string `json:"prefix,omitempty"`
	Delimiter         string `json:"delimiter,omitempty"`
	ContinuationToken string `json:"continuation_token,omitempty"`
	MaxKeys           int    `json:"max_keys,omitempty"`
}

// AdminObjectListing is the AdminListObjects response shape.
// CommonPrefixes are populated only when Options.Delimiter is set;
// they represent the pseudo-directories the SPA renders as
// foldable rows. NextContinuationToken is empty when the page
// fully drained the prefix scan.
type AdminObjectListing struct {
	Objects               []AdminObject `json:"objects"`
	CommonPrefixes        []string      `json:"common_prefixes,omitempty"`
	NextContinuationToken string        `json:"next_continuation_token,omitempty"`
}

const (
	adminListObjectsDefaultMaxKeys = 100
	adminListObjectsMaxKeysCap     = 1000
)

// clampAdminListMaxKeys folds the user-supplied MaxKeys into the
// legal [1, 1000] range, mapping 0 (unset) to the default 100.
func clampAdminListMaxKeys(n int) int {
	if n <= 0 {
		return adminListObjectsDefaultMaxKeys
	}
	if n > adminListObjectsMaxKeysCap {
		return adminListObjectsMaxKeysCap
	}
	return n
}

// AdminListObjects returns a bounded page of objects under a
// prefix. Read role required. Delimiter collapses pseudo-folders
// into CommonPrefixes the same way SigV4 ListObjectsV2 does.
//
// Sentinels:
//   - ErrAdminForbidden                — principal lacks read role
//   - ErrAdminNotLeader                — follower
//   - ErrAdminBucketNotFound           — bucket absent
//   - ErrAdminInvalidBucketName        — malformed bucket name argument
//   - ErrAdminInvalidContinuationToken — token references a different
//     bucket / generation / prefix / delimiter than the current
//     request, OR the token's readTS has been MVCC-GC'd past
//     (store.ErrReadTSCompacted on the underlying scan)
//
// A structurally invalid / non-base64-decodable continuation token
// surfaces as a wrapped json/decode error from decodeS3ContinuationToken,
// not as a sentinel — the bridge maps that to a generic 400 with the
// decode-error text preserved.
//
// Mirrors the SigV4 listObjectsV2 precedent at s3.go (same scan-
// and-collapse pipeline with one error-return per stage; carries
// the same nolint set). Splitting further would obscure the
// pagination cursor lifecycle.
//
//nolint:cyclop,gocognit,gocyclo,nestif // see comment above
func (s *S3Server) AdminListObjects(ctx context.Context, principal AdminPrincipal, bucket string, opts AdminListObjectsOptions) (AdminObjectListing, error) {
	if !principal.Role.canRead() {
		return AdminObjectListing{}, ErrAdminForbidden
	}
	if !s.isVerifiedS3Leader(ctx) {
		return AdminObjectListing{}, ErrAdminNotLeader
	}
	if err := validateS3BucketName(bucket); err != nil {
		return AdminObjectListing{}, errors.Wrapf(ErrAdminInvalidBucketName, "%s", err.Error())
	}

	token, err := decodeS3ContinuationToken(opts.ContinuationToken)
	if err != nil {
		return AdminObjectListing{}, errors.Wrap(err, "decode continuation token")
	}

	readTS := s.readTS()
	if token != nil && token.ReadTS != 0 {
		readTS = token.ReadTS
	}

	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, readTS)
	if err != nil {
		// Same compaction-error translation as the ScanAt path
		// below: when the caller passed a continuation token whose
		// readTS has been MVCC-GC'd past, loadBucketMetaAt's GetAt
		// also surfaces ErrReadTSCompacted — callers need to see
		// the dedicated sentinel rather than a generic 500 (Codex
		// r2 P2 on PR #811).
		if token != nil && errors.Is(err, store.ErrReadTSCompacted) {
			return AdminObjectListing{}, errors.Wrap(ErrAdminInvalidContinuationToken,
				"continuation token readTS has been compacted")
		}
		return AdminObjectListing{}, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return AdminObjectListing{}, ErrAdminBucketNotFound
	}

	if token != nil && (token.Bucket != bucket || token.Generation != meta.Generation ||
		token.Prefix != opts.Prefix || token.Delimiter != opts.Delimiter) {
		return AdminObjectListing{}, errors.Wrap(ErrAdminInvalidContinuationToken,
			"continuation token does not match request")
	}

	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	start := s3keys.ObjectManifestScanStart(bucket, meta.Generation, opts.Prefix)
	if token != nil {
		start = nextScanCursor(s3keys.ObjectManifestKey(bucket, token.Generation, token.LastKey))
	}
	end := prefixScanEnd(s3keys.ObjectManifestScanStart(bucket, meta.Generation, opts.Prefix))
	maxKeys := clampAdminListMaxKeys(opts.MaxKeys)

	out := AdminObjectListing{Objects: []AdminObject{}}
	commonPrefixSeen := map[string]struct{}{}
	if token != nil && token.LastCommonPrefix != "" {
		commonPrefixSeen[token.LastCommonPrefix] = struct{}{}
	}
	cursor := start
	truncated := false
	emitted := 0
	var lastKey, lastCommonPrefix string

	for emitted < maxKeys {
		pageLimit := s3ListPageSize
		if remaining := maxKeys - emitted; remaining < pageLimit {
			pageLimit = remaining
		}
		page, err := s.store.ScanAt(ctx, cursor, end, pageLimit, readTS)
		if err != nil {
			// Matches the SigV4 listObjectsV2 precedent at
			// s3.go:2105 — a continuation token whose readTS has
			// been MVCC-GC'd past surfaces as ErrReadTSCompacted
			// from the underlying scan; the admin caller needs the
			// dedicated sentinel so the bridge can render 400 with
			// a useful message instead of an opaque 500.
			if token != nil && errors.Is(err, store.ErrReadTSCompacted) {
				return AdminObjectListing{}, errors.Wrap(ErrAdminInvalidContinuationToken,
					"continuation token readTS has been compacted")
			}
			return AdminObjectListing{}, errors.WithStack(err)
		}
		if len(page) == 0 {
			break
		}
		for _, kvp := range page {
			_, generation, objectKey, ok := s3keys.ParseObjectManifestKey(kvp.Key)
			if !ok || generation != meta.Generation || !strings.HasPrefix(objectKey, opts.Prefix) {
				continue
			}
			if opts.Delimiter != "" {
				suffix := strings.TrimPrefix(objectKey, opts.Prefix)
				if idx := strings.Index(suffix, opts.Delimiter); idx >= 0 {
					commonPrefix := opts.Prefix + suffix[:idx+len(opts.Delimiter)]
					if _, exists := commonPrefixSeen[commonPrefix]; !exists {
						commonPrefixSeen[commonPrefix] = struct{}{}
						out.CommonPrefixes = append(out.CommonPrefixes, commonPrefix)
						emitted++
						lastKey = objectKey
						lastCommonPrefix = commonPrefix
					}
					if emitted >= maxKeys {
						truncated = true
						break
					}
					continue
				}
			}
			manifest, err := decodeS3ObjectManifest(kvp.Value)
			if err != nil {
				return AdminObjectListing{}, errors.WithStack(err)
			}
			ct := manifest.ContentType
			if ct == "" {
				ct = defaultS3ContentType
			}
			out.Objects = append(out.Objects, AdminObject{
				Key:          objectKey,
				Size:         manifest.SizeBytes,
				ContentType:  ct,
				ETag:         manifest.ETag,
				LastModified: hlcToTime(manifest.LastModifiedHLC),
				StorageClass: "STANDARD",
			})
			emitted++
			lastKey = objectKey
			lastCommonPrefix = ""
			if emitted >= maxKeys {
				truncated = true
				break
			}
		}
		if truncated {
			break
		}
		cursor = nextScanCursor(page[len(page)-1].Key)
		if len(page) < pageLimit {
			break
		}
	}

	if truncated {
		out.NextContinuationToken = encodeS3ContinuationToken(bucket, meta.Generation,
			opts.Prefix, opts.Delimiter, lastKey, lastCommonPrefix, readTS)
	}
	return out, nil
}
