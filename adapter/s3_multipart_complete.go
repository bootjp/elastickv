package adapter

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 composite ETag compatibility requires MD5.
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"hash"
	"io"
	"net/http"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

type s3MultipartCompletion struct {
	bucket        string
	objectKey     string
	uploadID      string
	generation    uint64
	uploadMetaKey []byte
	uploadMeta    s3UploadMeta
	manifestParts []s3ObjectPart
	totalSize     int64
	compositeETag string
}

func parseS3MultipartCompletion(body io.Reader, bucket, objectKey string) (s3CompleteMultipartUploadRequest, error) {
	var request s3CompleteMultipartUploadRequest
	bodyBytes, err := io.ReadAll(io.LimitReader(body, s3ChunkSize))
	if err != nil {
		return request, errors.WithStack(err)
	}
	if err := xml.Unmarshal(bodyBytes, &request); err != nil {
		return request, newS3ResponseError(http.StatusBadRequest, "MalformedXML", "request body is not valid XML", bucket, objectKey)
	}
	if err := validateS3MultipartCompletionParts(request.Parts, bucket, objectKey); err != nil {
		return request, err
	}
	return request, nil
}

func validateS3MultipartCompletionParts(parts []s3CompleteMultipartUploadPart, bucket, objectKey string) error {
	if len(parts) == 0 {
		return newS3ResponseError(http.StatusBadRequest, "InvalidArgument", "at least one part is required", bucket, objectKey)
	}
	if len(parts) > s3MaxPartsPerUpload {
		message := fmt.Sprintf("too many parts in CompleteMultipartUpload request (maximum %d)", s3MaxPartsPerUpload)
		return newS3ResponseError(http.StatusBadRequest, "InvalidArgument", message, bucket, objectKey)
	}
	for i, part := range parts {
		if part.PartNumber < s3MinPartNumber || part.PartNumber > s3MaxPartNumber {
			return newS3ResponseError(http.StatusBadRequest, "InvalidArgument", "part number out of allowed range", bucket, objectKey)
		}
		if i > 0 && part.PartNumber <= parts[i-1].PartNumber {
			return newS3ResponseError(http.StatusBadRequest, "InvalidPartOrder", "parts must be in ascending order", bucket, objectKey)
		}
	}
	return nil
}

func (s *S3Server) loadS3MultipartCompletion(ctx context.Context, bucket, objectKey, uploadID string, request s3CompleteMultipartUploadRequest) (s3MultipartCompletion, error) {
	completion := s3MultipartCompletion{bucket: bucket, objectKey: objectKey, uploadID: uploadID}
	readTS := s.readTS()
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	meta, exists, err := s.loadBucketMetaAt(ctx, bucket, readTS)
	if err != nil {
		return completion, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return completion, newS3ResponseError(http.StatusNotFound, "NoSuchBucket", "bucket not found", bucket, objectKey)
	}
	completion.generation = meta.Generation
	completion.uploadMetaKey = s3keys.UploadMetaKey(bucket, meta.Generation, objectKey, uploadID)
	if err := s.loadS3MultipartUploadMeta(ctx, readTS, &completion); err != nil {
		return completion, err
	}
	if err := s.loadS3MultipartParts(ctx, readTS, request.Parts, &completion); err != nil {
		return completion, err
	}
	return completion, nil
}

func (s *S3Server) loadS3MultipartUploadMeta(ctx context.Context, readTS uint64, completion *s3MultipartCompletion) error {
	raw, err := s.store.GetAt(ctx, completion.uploadMetaKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return newS3ResponseError(http.StatusNotFound, "NoSuchUpload", "upload not found", completion.bucket, completion.objectKey)
		}
		return errors.WithStack(err)
	}
	return errors.WithStack(json.Unmarshal(raw, &completion.uploadMeta))
}

func (s *S3Server) loadS3MultipartParts(ctx context.Context, readTS uint64, requested []s3CompleteMultipartUploadPart, completion *s3MultipartCompletion) error {
	completion.manifestParts = make([]s3ObjectPart, 0, len(requested))
	md5Concat := md5.New() //nolint:gosec // S3 composite ETag compatibility requires MD5.
	for i, requestedPart := range requested {
		part, partMD5, err := s.loadS3MultipartPart(ctx, readTS, requestedPart, i == len(requested)-1, completion)
		if err != nil {
			return err
		}
		if _, err := md5Concat.Write(partMD5); err != nil {
			return errors.WithStack(err)
		}
		completion.manifestParts = append(completion.manifestParts, part)
		completion.totalSize += part.SizeBytes
	}
	completion.compositeETag = multipartCompositeETag(md5Concat, len(requested))
	return nil
}

func (s *S3Server) loadS3MultipartPart(ctx context.Context, readTS uint64, requested s3CompleteMultipartUploadPart, last bool, completion *s3MultipartCompletion) (s3ObjectPart, []byte, error) {
	partNo, err := uint64FromInt(requested.PartNumber)
	if err != nil {
		return s3ObjectPart{}, nil, errors.WithStack(err)
	}
	partKey := s3keys.UploadPartKey(completion.bucket, completion.generation, completion.objectKey, completion.uploadID, partNo)
	raw, err := s.store.GetAt(ctx, partKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			message := fmt.Sprintf("part %d not found", requested.PartNumber)
			return s3ObjectPart{}, nil, newS3ResponseError(http.StatusBadRequest, "InvalidPart", message, completion.bucket, completion.objectKey)
		}
		return s3ObjectPart{}, nil, errors.WithStack(err)
	}
	var descriptor s3PartDescriptor
	if err := json.Unmarshal(raw, &descriptor); err != nil {
		return s3ObjectPart{}, nil, errors.WithStack(err)
	}
	requestedETag := strings.Trim(requested.ETag, `"`)
	if requestedETag != descriptor.ETag {
		message := fmt.Sprintf("part %d ETag mismatch: got %q, want %q", requested.PartNumber, requestedETag, descriptor.ETag)
		return s3ObjectPart{}, nil, newS3ResponseError(http.StatusBadRequest, "InvalidPart", message, completion.bucket, completion.objectKey)
	}
	if !last && descriptor.SizeBytes < int64(s3MinPartSize) {
		message := fmt.Sprintf("part %d is too small (%d bytes)", requested.PartNumber, descriptor.SizeBytes)
		return s3ObjectPart{}, nil, newS3ResponseError(http.StatusBadRequest, "EntityTooSmall", message, completion.bucket, completion.objectKey)
	}
	partMD5, err := hex.DecodeString(descriptor.ETag)
	if err != nil {
		return s3ObjectPart{}, nil, errors.WithStack(err)
	}
	return s3ObjectPart(descriptor), partMD5, nil
}

func multipartCompositeETag(md5Concat hash.Hash, partCount int) string {
	return hex.EncodeToString(md5Concat.Sum(nil)) + fmt.Sprintf("-%d", partCount)
}

func (s *S3Server) commitS3MultipartCompletion(ctx context.Context, completion s3MultipartCompletion) (*s3ObjectManifest, error) {
	var previous *s3ObjectManifest
	err := s.retryS3Mutation(ctx, func() error {
		var err error
		previous, err = s.commitS3MultipartCompletionAttempt(ctx, completion)
		return err
	})
	return previous, err
}

func (s *S3Server) commitS3MultipartCompletionAttempt(ctx context.Context, completion s3MultipartCompletion) (*s3ObjectManifest, error) {
	readTS := s.readTS()
	startTS, err := s.txnStartTS(ctx, readTS)
	if err != nil {
		return nil, errors.Wrap(err, "s3: allocate startTS for completeMultipartUpload retry")
	}
	readPin := s.pinReadTS(readTS)
	defer readPin.Release()

	meta, err := s.validateS3MultipartCommitFences(ctx, readTS, completion)
	if err != nil {
		return nil, err
	}
	commitTS, err := s.nextTxnCommitTS(ctx, startTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	headKey := s3keys.ObjectManifestKey(completion.bucket, meta.Generation, completion.objectKey)
	previous, _, err := s.loadObjectManifestAt(ctx, headKey, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	manifestBody, err := encodeS3ObjectManifest(&s3ObjectManifest{
		UploadID:        completion.uploadID,
		ETag:            completion.compositeETag,
		SizeBytes:       completion.totalSize,
		LastModifiedHLC: commitTS,
		ContentType:     completion.uploadMeta.ContentType,
		UserMetadata:    completion.uploadMeta.Metadata,
		Parts:           completion.manifestParts,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	bucketFence, err := encodeS3BucketMeta(meta)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	_, err = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: s3keys.BucketMetaKey(completion.bucket), Value: bucketFence},
			{Op: kv.Put, Key: headKey, Value: manifestBody},
			{Op: kv.Del, Key: completion.uploadMetaKey},
			{Op: kv.Del, Key: s3keys.GCUploadKey(completion.bucket, completion.generation, completion.objectKey, completion.uploadID)},
		},
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return previous, nil
}

func (s *S3Server) validateS3MultipartCommitFences(ctx context.Context, readTS uint64, completion s3MultipartCompletion) (*s3BucketMeta, error) {
	meta, exists, err := s.loadBucketMetaAt(ctx, completion.bucket, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists || meta == nil {
		return nil, newS3ResponseError(http.StatusNotFound, "NoSuchBucket", "bucket not found", completion.bucket, completion.objectKey)
	}
	if meta.Generation != completion.generation {
		return nil, newS3ResponseError(http.StatusNotFound, "NoSuchUpload", "upload not found (bucket was recreated)", completion.bucket, completion.objectKey)
	}
	if _, err := s.store.GetAt(ctx, completion.uploadMetaKey, readTS); err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, newS3ResponseError(http.StatusNotFound, "NoSuchUpload", "upload not found", completion.bucket, completion.objectKey)
		}
		return nil, errors.WithStack(err)
	}
	return meta, nil
}
