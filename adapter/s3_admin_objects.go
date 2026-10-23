package adapter

import (
	"context"
	"io"
	"net/http"
	"strings"
	"time"

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

// ErrAdminUploadThrottled signals AdminPutObject was refused by the S3 PUT
// admission limiter, the memory budget admin and SigV4 uploads share. The
// HTTP bridge maps it to a retryable 503.
var ErrAdminUploadThrottled = errors.New("s3 admin: upload throttled by PUT admission")

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
//   - ErrAdminS3Stopping         — Stop has begun
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
	// Counted like the other admin object operations so Stop waits for
	// the delete before it closes the cleanup gate: the blob cleanup the
	// delete schedules once it has committed must not be refused.
	if err := s.enterAdminBlobUse(); err != nil {
		return err
	}
	defer s.exitAdminBlobUse()

	cleanup, err := s.reserveAdminCleanup()
	if err != nil {
		return err
	}
	defer cleanup.release()
	var cleanupManifest *s3ObjectManifest
	var generation uint64
	err = s.retryS3Mutation(ctx, func() error {
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
		cleanup.start(func(cleanupCtx context.Context) {
			s.cleanupManifestBlobs(cleanupCtx, bucket, generation, key, cleanupManifest)
		})
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
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, readTS, "s3 admin delete object: begin read timestamp")
	if err != nil {
		return nil, 0, errors.Wrap(err, "s3 admin: allocate startTS for adminDeleteObjectTxn")
	}
	readTS = readTimestamp.Timestamp()
	startTS := readTS
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
	dispatchCtx := readTimestamp.WithDispatchVoucher(ctx)
	_, err = kv.DispatchWithReadTimestamp(dispatchCtx, s.coordinator, &kv.OperationGroup[kv.OP]{
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
// SigV4 endpoint with the SDK's multipart path. The upload runs the
// same pipeline as SigV4 PutObject, so it is subject to PUT admission
// control and uses blob offload when that is enabled.
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
//   - ErrAdminUploadThrottled    — PUT admission budget exhausted
func (s *S3Server) AdminPutObject(ctx context.Context, principal AdminPrincipal, bucket, key string, body io.Reader, contentType string) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !s.isVerifiedS3Leader(ctx) {
		return ErrAdminNotLeader
	}
	if err := s.enterAdminBlobUse(); err != nil {
		return err
	}
	defer s.exitAdminBlobUse()
	if key == "" {
		return ErrAdminInvalidObjectKey
	}
	if err := validateS3BucketName(bucket); err != nil {
		return errors.Wrapf(ErrAdminInvalidBucketName, "%s", err.Error())
	}
	if body == nil {
		body = strings.NewReader("")
	}
	return s.adminPutObject(ctx, &s3PutObjectInput{
		// MaxBytesReader with no ResponseWriter only bounds the read; the
		// overflow surfaces as *http.MaxBytesError, which the upload maps to
		// a 413 body error.
		body:            http.MaxBytesReader(nil, io.NopCloser(body), adminS3UploadMaxBytes),
		contentLength:   -1,
		streamBody:      &s3StreamingBody{},
		tooLargeMessage: ErrAdminUploadTooLarge.Error(),
		contentType:     s3ContentTypeOrDefault(strings.TrimSpace(contentType)),
	}, bucket, key)
}

// adminPutObject runs one admin upload through the PutObject stages SigV4
// uses (prepare, admission pre-check, chunk upload with blob offload when
// enabled, manifest commit, cleanup on failure) and maps their failures onto
// the admin sentinels. It makes a single attempt: the chunk fan-out has no
// idempotency token, so a retry would leak the first attempt's chunks.
func (s *S3Server) adminPutObject(ctx context.Context, in *s3PutObjectInput, bucket, key string) error {
	cleanup, err := s.reserveAdminCleanup()
	if err != nil {
		return err
	}
	defer cleanup.release()
	state, err := s.prepareS3PutObject(ctx, in, bucket, key)
	if err != nil {
		var respErr *s3ResponseError
		if errors.As(err, &respErr) && respErr.Code == s3ErrNoSuchBucket {
			return ErrAdminBucketNotFound
		}
		return errors.WithStack(err)
	}
	defer state.readPin.Release()

	// The body length is unknown, so the pre-check reserves one chunk, as it
	// does for streaming SigV4 uploads.
	if err := s.checkS3PutHeadroom(s3ChunkSize, s3PutAdmissionProtocolFixed); err != nil {
		return errors.WithStack(errors.Mark(err, ErrAdminUploadThrottled))
	}
	upload, bodyErr, err := s.uploadS3ObjectData(ctx, in, state, bucket, key, s.s3BlobOffloadSelected(ctx))
	if err != nil || bodyErr != nil {
		s.cleanupS3PutObjectChunks(ctx, state, upload, bucket, key)
		return adminPutUploadError(bodyErr, err)
	}
	if _, err := s.commitS3PutObject(ctx, in, state, upload, bucket); err != nil {
		s.cleanupS3PutObjectChunks(ctx, state, upload, bucket, key)
		return errors.WithStack(err)
	}
	if state.previous != nil {
		cleanup.start(func(cleanupCtx context.Context) {
			s.cleanupManifestBlobs(cleanupCtx, bucket, state.meta.Generation, key, state.previous)
		})
	}
	return nil
}

// adminPutUploadError maps a failed chunk upload onto the admin sentinels.
func adminPutUploadError(bodyErr *s3PutBodyError, err error) error {
	switch {
	case bodyErr != nil && bodyErr.Status == http.StatusRequestEntityTooLarge:
		return ErrAdminUploadTooLarge
	case bodyErr != nil:
		return errors.Errorf("s3 admin: read upload body: %s", bodyErr.Message)
	case errors.Is(err, errS3PutAdmissionExhausted):
		return errors.WithStack(errors.Mark(err, ErrAdminUploadThrottled))
	default:
		return errors.WithStack(err)
	}
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

	// The body fetches chunks (offloaded ones through the blob peer
	// cluster) until the caller closes it, so the registration is
	// released by the body's Close, not on return.
	if err := s.enterAdminBlobUse(); err != nil {
		return nil, AdminObject{}, err
	}
	manifest, generation, readTS, readPin, err := s.adminLoadObjectForRead(ctx, bucket, key)
	if err != nil {
		s.exitAdminBlobUse()
		return nil, AdminObject{}, err
	}

	body := newS3AdminObjectReader(ctx, s, bucket, generation, key, manifest, readTS, readPin)
	body.release = s.exitAdminBlobUse

	adminMeta := AdminObject{
		Key:          key,
		Size:         manifest.SizeBytes,
		ContentType:  manifest.ContentType,
		ETag:         manifest.ETag,
		LastModified: hlcToTime(manifest.LastModifiedHLC),
		StorageClass: s3StorageClassStandard,
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
// The pin, and the server's admin blob-use registration that keeps
// Stop from closing the blob peer cluster under the read, are released
// by Close. Calling Close more than once is safe (both are cleared on
// the first call).
type s3AdminObjectReader struct {
	ctx        context.Context
	server     *S3Server
	bucket     string
	generation uint64
	key        string
	manifest   *s3ObjectManifest
	readTS     uint64
	pin        *kv.ActiveTimestampToken
	// release ends the server's admin blob-use registration (see
	// S3Server.enterAdminBlobUse); called once, by Close.
	release func()

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
		expectedSize := part.ChunkSizes[r.chunkIdx]
		chunk, err := r.server.readS3ObjectChunk(
			r.ctx,
			r.bucket,
			r.generation,
			r.key,
			r.manifest.UploadID,
			part,
			r.chunkIdx,
			expectedSize,
			r.readTS,
		)
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
	if r.release != nil {
		r.release()
		r.release = nil
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
// prefix. Read role required. It runs the same scan as SigV4
// ListObjectsV2 (scanS3ObjectListing), so delimiter collapsing and
// continuation tokens behave identically on both paths.
//
// Sentinels:
//   - ErrAdminForbidden                — principal lacks read role
//   - ErrAdminNotLeader                — follower
//   - ErrAdminBucketNotFound           — bucket absent
//   - ErrAdminInvalidBucketName        — malformed bucket name argument
//   - ErrAdminInvalidContinuationToken — token is not base64 JSON,
//     references a different bucket / generation / prefix / delimiter
//     than the current request, or its readTS has been MVCC-GC'd past
//     (store.ErrReadTSCompacted on the metadata load or the scan)
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

	page, err := s.scanS3ObjectListing(ctx, s3ObjectListingRequest{
		Bucket:            bucket,
		Prefix:            opts.Prefix,
		Delimiter:         opts.Delimiter,
		ContinuationToken: opts.ContinuationToken,
		MaxKeys:           clampAdminListMaxKeys(opts.MaxKeys),
	})
	if err != nil {
		return AdminObjectListing{}, adminListingError(err)
	}

	out := AdminObjectListing{
		Objects:               make([]AdminObject, 0, len(page.Objects)),
		CommonPrefixes:        page.CommonPrefixes,
		NextContinuationToken: page.NextContinuationToken,
	}
	for _, obj := range page.Objects {
		ct := obj.Manifest.ContentType
		if ct == "" {
			ct = defaultS3ContentType
		}
		out.Objects = append(out.Objects, AdminObject{
			Key:          obj.Key,
			Size:         obj.Manifest.SizeBytes,
			ContentType:  ct,
			ETag:         obj.Manifest.ETag,
			LastModified: hlcToTime(obj.Manifest.LastModifiedHLC),
			StorageClass: s3StorageClassStandard,
		})
	}
	return out, nil
}

// adminListingError maps scanS3ObjectListing failures onto the admin
// sentinels. Token failures keep their reason in the message, which the
// admin bridge surfaces in the 400 body.
func adminListingError(err error) error {
	switch {
	case errors.Is(err, errS3ListingMalformedToken),
		errors.Is(err, errS3ListingTokenMismatch),
		errors.Is(err, errS3ListingTokenExpired):
		return errors.Wrap(ErrAdminInvalidContinuationToken, err.Error())
	case errors.Is(err, errS3ListingNoSuchBucket):
		return ErrAdminBucketNotFound
	default:
		return errors.WithStack(err)
	}
}
