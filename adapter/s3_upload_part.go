package adapter

import (
	"context"
	"net/http"
	"strconv"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

type s3UploadPartState struct {
	partNo        uint64
	readTS        uint64
	meta          *s3BucketMeta
	uploadMetaKey []byte
	readPin       *kv.ActiveTimestampToken
}

type s3UploadPartVersion struct {
	startTS         uint64
	readTimestamp   kv.ReadTimestamp
	commitTS        uint64
	blobVersion     uint64
	chunkRefVersion uint64
}

func (v s3UploadPartVersion) partVersion(offloaded bool) uint64 {
	if !offloaded && v.blobVersion != 0 {
		return v.blobVersion
	}
	return v.commitTS
}

func (s *S3Server) prepareS3UploadPart(ctx context.Context, bucket, objectKey, uploadID, partNumberRaw string) (*s3UploadPartState, error) {
	partNo, err := parseS3UploadPartNumber(partNumberRaw, bucket, objectKey)
	if err != nil {
		return nil, err
	}
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, s.readTS(), "s3 upload part preparation: begin read timestamp")
	if err != nil {
		return nil, errors.WithStack(err)
	}
	state := &s3UploadPartState{partNo: partNo, readTS: readTimestamp.Timestamp()}
	state.readPin = s.pinReadTS(state.readTS)
	prepared := false
	defer func() {
		if !prepared {
			state.readPin.Release()
		}
	}()

	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, state.readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return nil, newS3ResponseError(http.StatusNotFound, s3ErrNoSuchBucket, s3ErrNoSuchBucketMsg, bucket, objectKey)
	}
	state.meta = meta
	state.uploadMetaKey = s3keys.UploadMetaKey(bucket, meta.Generation, objectKey, uploadID)
	if _, err := s.store.GetAt(ctx, state.uploadMetaKey, state.readTS); err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, newS3ResponseError(http.StatusNotFound, "NoSuchUpload", "upload not found", bucket, objectKey)
		}
		return nil, errors.WithStack(err)
	}
	prepared = true
	return state, nil
}

func parseS3UploadPartNumber(raw, bucket, objectKey string) (uint64, error) {
	partNumber, err := strconv.Atoi(raw)
	if err != nil || partNumber < s3MinPartNumber || partNumber > s3MaxPartNumber {
		return 0, newS3ResponseError(http.StatusBadRequest, "InvalidArgument", "part number must be between 1 and 10000", bucket, objectKey)
	}
	return uint64(partNumber), nil
}

func (s *S3Server) storeS3UploadPart(ctx context.Context, request *http.Request, streamBody *s3StreamingBody, state *s3UploadPartState, bucket, objectKey, uploadID, admissionProtocol string, offloaded bool) (s3ChunkUploadResult, *s3PartDescriptor, *s3PutBodyError, error) {
	version, err := s.allocateS3UploadPartVersionForMode(ctx, offloaded)
	if err != nil {
		return s3ChunkUploadResult{}, nil, nil, err
	}
	upload, bodyErr, err := s.uploadS3Chunks(ctx, s3ChunkUploadConfig{
		body:              request.Body,
		contentLength:     request.ContentLength,
		streamBody:        streamBody,
		admissionProtocol: admissionProtocol,
		tooLargeMessage:   "part exceeds maximum allowed size",
		chunkKey: func(chunkNo uint64) []byte {
			return s3keys.VersionedBlobKey(bucket, state.meta.Generation, objectKey, uploadID, state.partNo, chunkNo, version.blobVersion)
		},
		chunkRefKey: func(chunkNo uint64) []byte {
			return s3keys.VersionedChunkRefKey(bucket, state.meta.Generation, objectKey, uploadID, state.partNo, chunkNo, version.chunkRefVersion)
		},
		offloaded: offloaded,
	})
	committed := false
	cleanupAllowed := true
	defer func() {
		if !committed && cleanupAllowed && upload.ChunkCount > 0 {
			s.cleanupPartBlobsAsync(
				bucket, state.meta.Generation, objectKey, uploadID,
				state.partNo, upload.ChunkCount, version.partVersion(upload.Offloaded), version.chunkRefVersion, upload.Offloaded,
			)
		}
	}()
	if err != nil || bodyErr != nil {
		return upload, nil, bodyErr, err
	}
	previous, attempted, err := s.commitS3UploadPart(ctx, state, upload, bucket, objectKey, uploadID, version)
	if err != nil {
		cleanupAllowed = !attempted || isRetryableS3MutationErr(err)
		return upload, nil, nil, err
	}
	committed = true
	return upload, previous, nil, nil
}

func (s *S3Server) allocateS3UploadPartVersionForMode(ctx context.Context, offloaded bool) (s3UploadPartVersion, error) {
	readTimestamp, startTS, commitTS, err := s.allocateS3UploadPartVersion(ctx)
	if err != nil {
		return s3UploadPartVersion{}, err
	}
	version := s3UploadPartVersion{startTS: startTS, readTimestamp: readTimestamp, blobVersion: commitTS, chunkRefVersion: startTS}
	if offloaded {
		version.chunkRefVersion = commitTS
	}
	// Blob namespaces are immutable; the descriptor's commit timestamp is
	// allocated only after those chunks are durable in either storage mode.
	return version, nil
}

func (s *S3Server) finalizeS3UploadPartCommitTS(ctx context.Context, startTS uint64) (uint64, error) {
	ts, err := s.nextTxnCommitTS(ctx, startTS)
	return ts, errors.WithStack(err)
}

func (s *S3Server) allocateS3UploadPartVersion(ctx context.Context) (kv.ReadTimestamp, uint64, uint64, error) {
	readTS := s.readTS()
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, readTS, "s3 upload part: begin read timestamp")
	if err != nil {
		return kv.ReadTimestamp{}, 0, 0, errors.WithStack(err)
	}
	readTS = readTimestamp.Timestamp()
	startTS := readTS
	commitTS, err := s.nextTxnCommitTS(ctx, startTS)
	if err != nil {
		return kv.ReadTimestamp{}, 0, 0, errors.WithStack(err)
	}
	return readTimestamp, startTS, commitTS, nil
}

func (s *S3Server) commitS3UploadPart(ctx context.Context, state *s3UploadPartState, upload s3ChunkUploadResult, bucket, objectKey, uploadID string, version s3UploadPartVersion) (*s3PartDescriptor, bool, error) {
	partKey := s3keys.UploadPartKey(bucket, state.meta.Generation, objectKey, uploadID, state.partNo)
	previous, err := s.readS3UploadPartCommitState(ctx, state, partKey, bucket, objectKey, version.startTS)
	if err != nil {
		return nil, false, err
	}
	if version.commitTS == 0 {
		version.commitTS, err = s.finalizeS3UploadPartCommitTS(ctx, version.startTS)
		if err != nil {
			return nil, false, err
		}
	}
	descriptor := &s3PartDescriptor{
		PartNo: state.partNo, ETag: upload.ETag, SizeBytes: upload.SizeBytes,
		ChunkCount: upload.ChunkCount, ChunkSizes: upload.ChunkSizes, PartVersion: version.partVersion(upload.Offloaded),
		ChunkRefVersion: version.chunkRefVersion, Offloaded: upload.Offloaded,
	}
	body, err := json.Marshal(descriptor)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(upload.ChunkRefElems)+1)
	elems = append(elems, upload.ChunkRefElems...)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: partKey, Value: body})
	dispatchCtx := version.readTimestamp.WithDispatchVoucher(ctx)
	_, err = kv.DispatchWithReadTimestamp(dispatchCtx, s.coordinator, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  version.startTS,
		CommitTS: version.commitTS,
		Elems:    elems,
		ReadKeys: [][]byte{state.uploadMetaKey, s3keys.BucketGenerationKey(bucket), partKey},
	})
	if err != nil {
		return nil, true, errors.WithStack(err)
	}
	return previous, true, nil
}

func (s *S3Server) writeS3UploadPartError(w http.ResponseWriter, bodyErr *s3PutBodyError, err error, bucket, objectKey string) {
	if bodyErr == nil && isRetryableS3MutationErr(err) {
		writeS3SlowDown(w, bucket, objectKey)
		return
	}
	s.writeS3ChunkUploadError(w, bodyErr, err, bucket, objectKey)
}

func (s *S3Server) readS3UploadPartCommitState(ctx context.Context, state *s3UploadPartState, partKey []byte, bucket, objectKey string, startTS uint64) (*s3PartDescriptor, error) {
	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, startTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return nil, newS3ResponseError(http.StatusNotFound, s3ErrNoSuchBucket, s3ErrNoSuchBucketMsg, bucket, objectKey)
	}
	if meta.Generation != state.meta.Generation {
		return nil, newS3ResponseError(http.StatusNotFound, "NoSuchUpload", "upload not found (bucket was recreated)", bucket, objectKey)
	}
	if err := s.verifyS3UploadStillExists(ctx, state.uploadMetaKey, bucket, objectKey, startTS); err != nil {
		return nil, err
	}
	return s.loadPreviousS3PartDescriptor(ctx, partKey, startTS)
}

func (s *S3Server) loadPreviousS3PartDescriptor(ctx context.Context, partKey []byte, readTS uint64) (*s3PartDescriptor, error) {
	raw, err := s.store.GetAt(ctx, partKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	return decodeS3PartDescriptorOrNil(raw), nil
}

// Keep the legacy malformed-descriptor fallback separate from storage errors,
// which must prevent a replacement from committing after a failed read.
func decodeS3PartDescriptorOrNil(raw []byte) *s3PartDescriptor {
	var descriptor s3PartDescriptor
	if json.Unmarshal(raw, &descriptor) != nil {
		return nil
	}
	return &descriptor
}

func (s *S3Server) verifyS3UploadStillExists(ctx context.Context, uploadMetaKey []byte, bucket, objectKey string, readTS uint64) error {
	if _, err := s.store.GetAt(ctx, uploadMetaKey, readTS); err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return newS3ResponseError(http.StatusNotFound, "NoSuchUpload", "upload not found", bucket, objectKey)
		}
		return errors.WithStack(err)
	}
	return nil
}
