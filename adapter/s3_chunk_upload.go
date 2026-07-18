package adapter

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETag compatibility requires MD5.
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"net/http"
	"strings"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

type s3ChunkUploadConfig struct {
	request            *http.Request
	streamBody         *s3StreamingBody
	admissionProtocol  string
	tooLargeMessage    string
	expectedPayloadSHA string
	chunkKey           func(chunkNo uint64) []byte
	chunkRefKey        func(chunkNo uint64) []byte
	offloaded          bool
}

type s3ChunkUploadResult struct {
	ETag            string
	SizeBytes       int64
	ChunkSizes      []uint64
	PersistedChunks int
	ChunkCount      uint64
	Offloaded       bool
	ChunkRefElems   []*kv.Elem[kv.OP]
}

type s3ChunkUploader struct {
	server            *S3Server
	ctx               context.Context
	cfg               s3ChunkUploadConfig
	result            s3ChunkUploadResult
	etagHasher        hash.Hash
	payloadHasher     hash.Hash
	buf               []byte
	pendingBatch      []*kv.Elem[kv.OP]
	pendingChunkSizes []uint64
	pendingAdmission  []func()
}

// uploadS3Chunks owns the shared streaming data path for PutObject and
// UploadPart. It reads and hashes the request body, holds admission capacity
// until each batch is dispatched, and verifies payload and trailer checksums.
// The caller remains responsible for protocol setup, metadata transactions,
// and cleanup of any chunks reported in the result.
func (s *S3Server) uploadS3Chunks(ctx context.Context, cfg s3ChunkUploadConfig) (s3ChunkUploadResult, *s3PutBodyError, error) {
	uploader := &s3ChunkUploader{
		server:            s,
		ctx:               ctx,
		cfg:               cfg,
		etagHasher:        md5.New(), //nolint:gosec // S3 ETag compatibility requires MD5.
		buf:               make([]byte, s3ChunkSize),
		pendingBatch:      make([]*kv.Elem[kv.OP], 0, s3ChunkBatchOps),
		pendingChunkSizes: make([]uint64, 0, s3ChunkBatchOps),
		pendingAdmission:  make([]func(), 0, s3ChunkBatchOps),
	}
	uploader.result.Offloaded = cfg.offloaded
	if cfg.expectedPayloadSHA != "" {
		uploader.payloadHasher = sha256.New()
	}
	return uploader.run()
}

func (u *s3ChunkUploader) run() (s3ChunkUploadResult, *s3PutBodyError, error) {
	defer u.releasePendingAdmission()
	for {
		done, bodyErr, err := u.uploadNextChunk()
		if err != nil || bodyErr != nil {
			return u.result, bodyErr, err
		}
		if done {
			break
		}
	}
	if err := u.flushBatch(); err != nil {
		return u.result, nil, err
	}
	if bodyErr := u.verifyChecksums(); bodyErr != nil {
		return u.result, bodyErr, nil
	}
	u.result.ETag = hex.EncodeToString(u.etagHasher.Sum(nil))
	return u.result, nil, nil
}

func (u *s3ChunkUploader) uploadNextChunk() (bool, *s3PutBodyError, error) {
	if err := u.flushBeforeRead(); err != nil {
		return false, nil, err
	}
	request := u.cfg.request
	readBuf, finalRead := nextS3PutReadBuffer(u.buf, u.cfg.admissionProtocol, request.ContentLength, u.result.SizeBytes)
	if len(readBuf) == 0 {
		return true, nil, nil
	}
	release, err := u.server.acquireS3PutAdmissionForRead(u.ctx, len(readBuf), u.cfg.admissionProtocol, u.cfg.streamBody, u.result.SizeBytes)
	if err != nil {
		return false, nil, errors.WithStack(err)
	}
	allowFinalPartial := s3PutReadAllowsFinalPartial(u.cfg.admissionProtocol, request.ContentLength)
	n, readErr := readS3PutChunk(request.Body, readBuf, allowFinalPartial, finalRead)
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		release()
		return u.classifyReadError(readErr)
	}
	if n == 0 {
		release()
		return errors.Is(readErr, io.EOF), nil, nil
	}
	if err := u.appendChunk(readBuf[:n], release); err != nil {
		return false, nil, err
	}
	if err := u.flushFullBatch(); err != nil {
		return false, nil, err
	}
	return errors.Is(readErr, io.EOF), nil, nil
}

func (u *s3ChunkUploader) flushBeforeRead() error {
	if !u.server.shouldFlushS3PutBatchBeforeRead(u.cfg.admissionProtocol, len(u.pendingAdmission)) {
		return nil
	}
	return u.flushBatch()
}

func (u *s3ChunkUploader) flushFullBatch() error {
	if len(u.pendingBatch) < s3ChunkBatchOps {
		return nil
	}
	return u.flushBatch()
}

func (u *s3ChunkUploader) classifyReadError(err error) (bool, *s3PutBodyError, error) {
	if bodyErr, ok := classifyS3BodyReadErr(err, u.cfg.tooLargeMessage); ok {
		return false, bodyErr, nil
	}
	return false, nil, errors.WithStack(err)
}

func (u *s3ChunkUploader) appendChunk(data []byte, release func()) error {
	chunk := append([]byte(nil), data...)
	if _, err := u.etagHasher.Write(chunk); err != nil {
		return errors.WithStack(err)
	}
	if u.payloadHasher != nil {
		if _, err := u.payloadHasher.Write(chunk); err != nil {
			return errors.WithStack(err)
		}
	}
	u.cfg.streamBody.writeDecoded(chunk)
	if u.cfg.offloaded {
		if err := u.appendOffloadedChunk(chunk, release); err != nil {
			return err
		}
	} else {
		u.appendLegacyChunk(chunk, release)
	}
	chunkSize := uint64(len(data))
	u.result.ChunkSizes = append(u.result.ChunkSizes, chunkSize)
	u.pendingChunkSizes = append(u.pendingChunkSizes, chunkSize)
	u.result.SizeBytes += int64(len(data))
	u.result.ChunkCount++
	return nil
}

func (u *s3ChunkUploader) appendOffloadedChunk(chunk []byte, release func()) error {
	defer release()
	if u.cfg.chunkRefKey == nil {
		return errors.New("s3 chunkref key builder is not configured")
	}
	digest := sha256.Sum256(chunk)
	commitTS, err := u.server.nextTxnCommitTS(u.ctx, 0)
	if err != nil {
		return errors.WithStack(err)
	}
	ref, err := u.server.persistS3ChunkBlob(u.ctx, digest, chunk, commitTS)
	if err != nil {
		return err
	}
	refValue, err := s3keys.EncodeChunkRefValue(ref)
	if err != nil {
		return errors.WithStack(err)
	}
	u.result.ChunkRefElems = append(u.result.ChunkRefElems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   u.cfg.chunkRefKey(u.result.ChunkCount),
		Value: refValue,
	})
	u.result.PersistedChunks++
	return nil
}

func (u *s3ChunkUploader) appendLegacyChunk(chunk []byte, release func()) {
	u.pendingAdmission = append(u.pendingAdmission, release)
	u.pendingBatch = append(u.pendingBatch, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   u.cfg.chunkKey(u.result.ChunkCount),
		Value: chunk,
	})
}

func (u *s3ChunkUploader) flushBatch() error {
	if u.cfg.offloaded {
		return nil
	}
	if len(u.pendingBatch) == 0 {
		return nil
	}
	_, err := u.server.coordinator.Dispatch(u.ctx, &kv.OperationGroup[kv.OP]{Elems: u.pendingBatch})
	u.releasePendingAdmission()
	if err != nil {
		return errors.WithStack(err)
	}
	u.result.PersistedChunks += len(u.pendingChunkSizes)
	u.pendingBatch = u.pendingBatch[:0]
	u.pendingChunkSizes = u.pendingChunkSizes[:0]
	return nil
}

func (u *s3ChunkUploader) releasePendingAdmission() {
	for _, release := range u.pendingAdmission {
		release()
	}
	u.pendingAdmission = u.pendingAdmission[:0]
}

func (u *s3ChunkUploader) verifyChecksums() *s3PutBodyError {
	if u.payloadHasher != nil {
		actualPayloadSHA := hex.EncodeToString(u.payloadHasher.Sum(nil))
		if !strings.EqualFold(actualPayloadSHA, u.cfg.expectedPayloadSHA) {
			return &s3PutBodyError{
				Status:  http.StatusBadRequest,
				Code:    "XAmzContentSHA256Mismatch",
				Message: "payload SHA256 does not match x-amz-content-sha256",
			}
		}
	}
	if err := u.cfg.streamBody.verifyTrailer(); err != nil {
		return &s3PutBodyError{Status: http.StatusBadRequest, Code: "BadDigest", Message: err.Error()}
	}
	return nil
}
