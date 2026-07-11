package adapter

import (
	"context"
	"net/http"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

type s3PutObjectState struct {
	startTS  uint64
	meta     *s3BucketMeta
	headKey  []byte
	previous *s3ObjectManifest
	uploadID string
	readPin  *kv.ActiveTimestampToken
}

func (s *S3Server) prepareS3PutObject(ctx context.Context, request *http.Request, bucket, objectKey string) (*s3PutObjectState, error) {
	readTS := s.readTS()
	startTS, err := s.txnStartTS(ctx, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	state := &s3PutObjectState{startTS: startTS, readPin: s.pinReadTS(readTS)}
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
		return nil, newS3ResponseError(http.StatusNotFound, "NoSuchBucket", "bucket not found", bucket, objectKey)
	}
	state.meta = meta
	state.headKey = s3keys.ObjectManifestKey(bucket, meta.Generation, objectKey)
	state.previous, _, err = s.loadObjectManifestAt(ctx, state.headKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if err := validateS3PutPreconditions(request, state.previous); err != nil {
		return nil, newS3ResponseError(http.StatusPreconditionFailed, "PreconditionFailed", err.Error(), bucket, objectKey)
	}
	state.uploadID = newS3UploadID(s.clock())
	prepared = true
	return state, nil
}

func (s *S3Server) uploadS3ObjectData(ctx context.Context, request *http.Request, streamBody *s3StreamingBody, state *s3PutObjectState, bucket, objectKey, expectedPayloadSHA string) (s3ChunkUploadResult, *s3PutBodyError, error) {
	payloadChecksum := expectedPayloadSHA
	if isS3PayloadMarker(payloadChecksum) {
		payloadChecksum = ""
	}
	return s.uploadS3Chunks(ctx, s3ChunkUploadConfig{
		request:            request,
		streamBody:         streamBody,
		admissionProtocol:  s3PutAdmissionProtocolForPayload(expectedPayloadSHA),
		tooLargeMessage:    "object exceeds maximum allowed size",
		expectedPayloadSHA: payloadChecksum,
		chunkKey: func(chunkNo uint64) []byte {
			return s3keys.BlobKey(bucket, state.meta.Generation, objectKey, state.uploadID, 1, chunkNo)
		},
	})
}

func (s *S3Server) cleanupS3PutObjectChunks(ctx context.Context, state *s3PutObjectState, upload s3ChunkUploadResult, bucket, objectKey string) {
	part := s3ObjectPart{PartNo: 1, ChunkSizes: upload.ChunkSizes[:upload.PersistedChunks]}
	manifest := &s3ObjectManifest{UploadID: state.uploadID}
	if len(part.ChunkSizes) > 0 {
		manifest.Parts = []s3ObjectPart{part}
	}
	s.cleanupManifestBlobs(ctx, bucket, state.meta.Generation, objectKey, manifest)
}

func (s *S3Server) commitS3PutObject(ctx context.Context, request *http.Request, state *s3PutObjectState, upload s3ChunkUploadResult, bucket string) (bool, error) {
	commitTS, err := s.nextTxnCommitTS(ctx, state.startTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	manifest := &s3ObjectManifest{
		UploadID:           state.uploadID,
		ETag:               upload.ETag,
		SizeBytes:          upload.SizeBytes,
		LastModifiedHLC:    commitTS,
		ContentType:        headerOrDefault(request.Header.Get("Content-Type"), "application/octet-stream"),
		ContentEncoding:    cleanStoredContentEncoding(request.Header.Get("Content-Encoding")),
		CacheControl:       request.Header.Get("Cache-Control"),
		ContentDisposition: request.Header.Get("Content-Disposition"),
		UserMetadata:       collectS3UserMetadata(request.Header),
	}
	if upload.SizeBytes > 0 {
		manifest.Parts = []s3ObjectPart{{
			PartNo: 1, ETag: upload.ETag, SizeBytes: upload.SizeBytes,
			ChunkCount: upload.ChunkCount, ChunkSizes: upload.ChunkSizes,
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
	_, err = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  state.startTS,
		CommitTS: commitTS,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: s3keys.BucketMetaKey(bucket), Value: bucketFence},
			{Op: kv.Put, Key: state.headKey, Value: body},
		},
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
	writeS3ResponseOrInternalError(w, err)
}
