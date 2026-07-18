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

func (s *S3Server) prepareS3UploadPart(ctx context.Context, bucket, objectKey, uploadID, partNumberRaw string) (*s3UploadPartState, error) {
	partNo, err := parseS3UploadPartNumber(partNumberRaw, bucket, objectKey)
	if err != nil {
		return nil, err
	}
	state := &s3UploadPartState{partNo: partNo, readTS: s.readTS()}
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
		return nil, newS3ResponseError(http.StatusNotFound, "NoSuchBucket", "bucket not found", bucket, objectKey)
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
	startTS, commitTS, err := s.allocateS3UploadPartVersionForMode(ctx, offloaded)
	if err != nil {
		return s3ChunkUploadResult{}, nil, nil, err
	}
	upload, bodyErr, err := s.uploadS3Chunks(ctx, s3ChunkUploadConfig{
		request:           request,
		streamBody:        streamBody,
		admissionProtocol: admissionProtocol,
		tooLargeMessage:   "part exceeds maximum allowed size",
		chunkKey: func(chunkNo uint64) []byte {
			return s3keys.VersionedBlobKey(bucket, state.meta.Generation, objectKey, uploadID, state.partNo, chunkNo, commitTS)
		},
		chunkRefKey: func(chunkNo uint64) []byte {
			return s3keys.VersionedChunkRefKey(bucket, state.meta.Generation, objectKey, uploadID, state.partNo, chunkNo, startTS)
		},
		offloaded: offloaded,
	})
	committed := false
	defer func() {
		if !committed && upload.ChunkCount > 0 {
			s.cleanupPartBlobsAsync(
				bucket, state.meta.Generation, objectKey, uploadID,
				state.partNo, upload.ChunkCount, commitTS, startTS, upload.Offloaded,
			)
		}
	}()
	if err != nil || bodyErr != nil {
		return upload, nil, bodyErr, err
	}
	commitTS, err = s.finalizeS3UploadPartCommitTS(ctx, startTS, commitTS, offloaded)
	if err != nil {
		return upload, nil, nil, err
	}
	previous, err := s.commitS3UploadPart(ctx, state, upload, bucket, objectKey, uploadID, startTS, commitTS)
	if err != nil {
		return upload, nil, nil, err
	}
	committed = true
	return upload, previous, nil, nil
}

func (s *S3Server) allocateS3UploadPartVersionForMode(ctx context.Context, offloaded bool) (uint64, uint64, error) {
	startTS, commitTS, err := s.allocateS3UploadPartVersion(ctx)
	if err != nil {
		return 0, 0, err
	}
	if offloaded {
		// The descriptor timestamp is allocated after blob durability so the
		// chunkrefs cannot commit behind a side-channel write they make reachable.
		commitTS = 0
	}
	return startTS, commitTS, nil
}

func (s *S3Server) finalizeS3UploadPartCommitTS(ctx context.Context, startTS, commitTS uint64, offloaded bool) (uint64, error) {
	if !offloaded {
		return commitTS, nil
	}
	ts, err := s.nextTxnCommitTS(ctx, startTS)
	return ts, errors.WithStack(err)
}

func (s *S3Server) allocateS3UploadPartVersion(ctx context.Context) (uint64, uint64, error) {
	readTS := s.readTS()
	startTS, err := s.txnStartTS(ctx, readTS)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	commitTS, err := s.nextTxnCommitTS(ctx, startTS)
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	return startTS, commitTS, nil
}

func (s *S3Server) commitS3UploadPart(ctx context.Context, state *s3UploadPartState, upload s3ChunkUploadResult, bucket, objectKey, uploadID string, startTS, commitTS uint64) (*s3PartDescriptor, error) {
	descriptor := &s3PartDescriptor{
		PartNo: state.partNo, ETag: upload.ETag, SizeBytes: upload.SizeBytes,
		ChunkCount: upload.ChunkCount, ChunkSizes: upload.ChunkSizes, PartVersion: commitTS,
		ChunkRefVersion: startTS, Offloaded: upload.Offloaded,
	}
	body, err := json.Marshal(descriptor)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	partKey := s3keys.UploadPartKey(bucket, state.meta.Generation, objectKey, uploadID, state.partNo)
	previous := s.loadPreviousS3PartDescriptor(ctx, partKey, state.readTS)
	if err := s.verifyS3UploadStillExists(ctx, state.uploadMetaKey, bucket, objectKey); err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(upload.ChunkRefElems)+1)
	elems = append(elems, upload.ChunkRefElems...)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: partKey, Value: body})
	_, err = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: startTS, CommitTS: commitTS,
		Elems: elems,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return previous, nil
}

func (s *S3Server) loadPreviousS3PartDescriptor(ctx context.Context, partKey []byte, readTS uint64) *s3PartDescriptor {
	raw, err := s.store.GetAt(ctx, partKey, readTS)
	if err != nil {
		return nil
	}
	var descriptor s3PartDescriptor
	if json.Unmarshal(raw, &descriptor) != nil {
		return nil
	}
	return &descriptor
}

func (s *S3Server) verifyS3UploadStillExists(ctx context.Context, uploadMetaKey []byte, bucket, objectKey string) error {
	if _, err := s.store.GetAt(ctx, uploadMetaKey, s.readTS()); err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return newS3ResponseError(http.StatusNotFound, "NoSuchUpload", "upload not found", bucket, objectKey)
		}
		return errors.WithStack(err)
	}
	return nil
}
