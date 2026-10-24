package adapter

import (
	"context"
	"io"
	"net/http"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// s3PutObjectInput is one PutObject upload, independent of the protocol that
// delivered it. putObject fills it from the SigV4 request and AdminPutObject
// from its arguments; prepareS3PutObject, uploadS3ObjectData and
// commitS3PutObject read nothing else.
type s3PutObjectInput struct {
	// ifMatch and ifNoneMatch are the conditional-write preconditions.
	ifMatch     string
	ifNoneMatch string

	// body is the payload, already bounded by the caller's size cap;
	// contentLength is its length, or -1 when unknown. Both are set once the
	// caller has wrapped the body, after prepareS3PutObject.
	body          io.Reader
	contentLength int64
	streamBody    *s3StreamingBody
	// payloadSHA is the X-Amz-Content-Sha256 value. A hex digest is verified
	// against the body; payload markers and the empty string skip the check.
	payloadSHA      string
	tooLargeMessage string

	contentType        string
	contentEncoding    string
	cacheControl       string
	contentDisposition string
	userMetadata       map[string]string
}

// s3PutObjectInputFromHeaders reads the SigV4 PutObject headers. The body is
// attached once prepareStreamingPutBody has wrapped it.
func s3PutObjectInputFromHeaders(h http.Header) *s3PutObjectInput {
	return &s3PutObjectInput{
		ifMatch:            strings.TrimSpace(h.Get("If-Match")),
		ifNoneMatch:        strings.TrimSpace(h.Get("If-None-Match")),
		contentLength:      -1,
		payloadSHA:         normalizeS3PayloadHash(h.Get("X-Amz-Content-Sha256")),
		tooLargeMessage:    "object exceeds maximum allowed size",
		contentType:        s3ContentTypeOrDefault(h.Get("Content-Type")),
		contentEncoding:    cleanStoredContentEncoding(h.Get("Content-Encoding")),
		cacheControl:       h.Get("Cache-Control"),
		contentDisposition: h.Get("Content-Disposition"),
		userMetadata:       collectS3UserMetadata(h),
	}
}

type s3PutObjectState struct {
	startTS       uint64
	readTimestamp kv.ReadTimestamp
	meta          *s3BucketMeta
	headKey       []byte
	previous      *s3ObjectManifest
	uploadID      string
	readPin       *kv.ActiveTimestampToken
}

func (s *S3Server) prepareS3PutObject(ctx context.Context, in *s3PutObjectInput, bucket, objectKey string) (*s3PutObjectState, error) {
	readTS := s.readTS()
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, readTS, "s3 put object: begin read timestamp")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	readTS = readTimestamp.Timestamp()
	startTS := readTS
	state := &s3PutObjectState{startTS: startTS, readTimestamp: readTimestamp, readPin: s.pinReadTS(readTS)}
	prepared := false
	defer func() {
		if !prepared {
			state.readPin.Release()
		}
	}()

	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return nil, newS3ResponseError(http.StatusNotFound, s3ErrNoSuchBucket, s3ErrNoSuchBucketMsg, bucket, objectKey)
	}
	state.meta = meta
	state.headKey = s3keys.ObjectManifestKey(bucket, meta.Generation, objectKey)
	state.previous, _, err = s.loadObjectManifestAt(ctx, state.headKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := validateS3PutPreconditions(in.ifMatch, in.ifNoneMatch, state.previous); err != nil {
		return nil, newS3ResponseError(http.StatusPreconditionFailed, "PreconditionFailed", err.Error(), bucket, objectKey)
	}
	state.uploadID = newS3UploadID(s.clock())
	prepared = true
	return state, nil
}

func (s *S3Server) uploadS3ObjectData(ctx context.Context, in *s3PutObjectInput, state *s3PutObjectState, bucket, objectKey string, offloaded bool) (s3ChunkUploadResult, *s3PutBodyError, error) {
	payloadChecksum := in.payloadSHA
	if isS3PayloadMarker(payloadChecksum) {
		payloadChecksum = ""
	}
	return s.uploadS3Chunks(ctx, s3ChunkUploadConfig{
		body:               in.body,
		contentLength:      in.contentLength,
		streamBody:         in.streamBody,
		admissionProtocol:  s3PutAdmissionProtocolForPayload(in.payloadSHA),
		tooLargeMessage:    in.tooLargeMessage,
		expectedPayloadSHA: payloadChecksum,
		chunkKey: func(chunkNo uint64) []byte {
			return s3keys.BlobKey(bucket, state.meta.Generation, objectKey, state.uploadID, 1, chunkNo)
		},
		chunkRefKey: func(chunkNo uint64) []byte {
			return s3keys.ChunkRefKey(bucket, state.meta.Generation, objectKey, state.uploadID, 1, chunkNo)
		},
		offloaded: offloaded,
	})
}

func (s *S3Server) cleanupS3PutObjectChunks(ctx context.Context, state *s3PutObjectState, upload s3ChunkUploadResult, bucket, objectKey string) {
	chunkSizes := upload.ChunkSizes
	if !upload.Offloaded {
		chunkSizes = upload.ChunkSizes[:upload.PersistedChunks]
	}
	part := s3ObjectPart{PartNo: 1, ChunkSizes: chunkSizes, Offloaded: upload.Offloaded}
	manifest := &s3ObjectManifest{UploadID: state.uploadID}
	if len(part.ChunkSizes) > 0 {
		manifest.Parts = []s3ObjectPart{part}
	}
	s.cleanupManifestBlobs(ctx, bucket, state.meta.Generation, objectKey, manifest)
}

func (s *S3Server) commitS3PutObject(ctx context.Context, in *s3PutObjectInput, state *s3PutObjectState, upload s3ChunkUploadResult, bucket string) (bool, error) {
	commitTS, err := s.nextTxnCommitTS(ctx, state.startTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	manifest := &s3ObjectManifest{
		UploadID:           state.uploadID,
		ETag:               upload.ETag,
		SizeBytes:          upload.SizeBytes,
		LastModifiedHLC:    commitTS,
		ContentType:        in.contentType,
		ContentEncoding:    in.contentEncoding,
		CacheControl:       in.cacheControl,
		ContentDisposition: in.contentDisposition,
		UserMetadata:       in.userMetadata,
	}
	if upload.SizeBytes > 0 {
		manifest.Parts = []s3ObjectPart{{
			PartNo: 1, ETag: upload.ETag, SizeBytes: upload.SizeBytes,
			ChunkCount: upload.ChunkCount, ChunkSizes: upload.ChunkSizes, Offloaded: upload.Offloaded,
		}}
	}
	body, err := encodeS3ObjectManifest(manifest)
	if err != nil {
		return false, errors.WithStack(err)
	}
	bucketFence, err := encodeS3BucketMeta(state.meta)
	if err != nil {
		return false, errors.WithStack(err)
	}
	elems := append([]*kv.Elem[kv.OP](nil), upload.ChunkRefElems...)
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: s3keys.BucketMetaKey(bucket), Value: bucketFence},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: state.headKey, Value: body},
	)
	dispatchCtx := state.readTimestamp.WithDispatchVoucher(ctx)
	_, err = kv.DispatchWithReadTimestamp(dispatchCtx, s.coordinator, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  state.startTS,
		CommitTS: commitTS,
		Elems:    elems,
	})
	return true, errors.WithStack(err)
}

func writeS3PutObjectCommitError(w http.ResponseWriter, err error, mutationAttempted bool, bucket, objectKey string) {
	if mutationAttempted {
		writeS3MutationError(w, err, bucket, objectKey)
		return
	}
	writeS3InternalError(w, err)
}

func (s *S3Server) writeS3ChunkUploadError(w http.ResponseWriter, bodyErr *s3PutBodyError, err error, bucket, objectKey string) {
	if bodyErr != nil {
		writeS3Error(w, bodyErr.Status, bodyErr.Code, bodyErr.Message, bucket, objectKey)
		return
	}
	if errors.Is(err, errS3PutAdmissionExhausted) {
		writeS3AdmissionError(w, bucket, objectKey, s.s3PutAdmissionRetryAfter())
		return
	}
	if status.Code(err) == codes.Unavailable {
		writeS3Error(w, http.StatusServiceUnavailable, "ServiceUnavailable", "chunkblob durability is temporarily unavailable", bucket, objectKey)
		return
	}
	writeS3ResponseOrInternalError(w, err)
}
