package adapter

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
)

const (
	s3PutAdmissionDefaultMaxInflightBytes = 256 << 20
	s3PutAdmissionDefaultTimeout          = 30 * time.Second
	s3PutAdmissionDefaultRetryAfter       = time.Second
	s3PutAdmissionDisableMaxInflightBytes = int64(1<<63 - 1)

	s3PutAdmissionMaxInflightEnv = "ELASTICKV_S3_PUT_ADMISSION_MAX_INFLIGHT_BYTES"
	s3PutAdmissionTimeoutEnv     = "ELASTICKV_S3_DISPATCH_ADMISSION_TIMEOUT"
	s3PutAdmissionChunkedEnv     = "ELASTICKV_S3_PUT_ADMISSION_CHUNKED_INCREMENTAL"

	s3PutAdmissionStagePrereserve = "prereserve"
	s3PutAdmissionStagePerBatch   = "perbatch"
	s3PutAdmissionProtocolFixed   = "fixed-length"
	s3PutAdmissionProtocolChunked = "chunked"
)

var (
	errS3PutAdmissionExhausted      = errors.New("s3 put admission budget exhausted")
	errS3PutAdmissionEntityTooLarge = errors.New("s3 put body exceeds maximum allowed size")
	errS3PutAdmissionInvalidDecoded = errors.New("invalid X-Amz-Decoded-Content-Length")
)

type S3PutAdmissionObserver interface {
	ObserveS3PutAdmissionInflight(bytes int64)
	ObserveS3PutAdmissionRejection(stage, protocol string)
	ObserveS3PutAdmissionWait(stage, protocol string, duration time.Duration)
}

type s3PutAdmission struct {
	sem      chan struct{}
	inflight atomic.Int64
	timeout  time.Duration
	maxBytes int64
	chunked  bool
}

func newS3PutAdmissionFromEnv() *s3PutAdmission {
	maxBytes := parseS3AdmissionInt64Env(s3PutAdmissionMaxInflightEnv, s3PutAdmissionDefaultMaxInflightBytes)
	timeout := parseS3AdmissionDurationEnv(s3PutAdmissionTimeoutEnv, s3PutAdmissionDefaultTimeout)
	chunked := parseS3AdmissionBoolEnv(s3PutAdmissionChunkedEnv, true)
	return newS3PutAdmissionWithChunked(maxBytes, timeout, chunked)
}

func newS3PutAdmission(maxBytes int64, timeout time.Duration) *s3PutAdmission {
	return newS3PutAdmissionWithChunked(maxBytes, timeout, true)
}

func newS3PutAdmissionWithChunked(maxBytes int64, timeout time.Duration, chunked bool) *s3PutAdmission {
	if maxBytes <= 0 || maxBytes == s3PutAdmissionDisableMaxInflightBytes {
		return nil
	}
	units64 := 1 + (maxBytes-1)/s3ChunkSize
	maxInt := int64(int(^uint(0) >> 1))
	if units64 > maxInt {
		return nil
	}
	units := int(units64)
	if timeout <= 0 {
		timeout = s3PutAdmissionDefaultTimeout
	}
	roundedMaxBytes := units64 * s3ChunkSize
	if roundedMaxBytes < 0 || roundedMaxBytes < maxBytes {
		roundedMaxBytes = s3PutAdmissionDisableMaxInflightBytes
	}
	return &s3PutAdmission{
		sem:      make(chan struct{}, units),
		timeout:  timeout,
		maxBytes: roundedMaxBytes,
		chunked:  chunked,
	}
}

func parseS3AdmissionInt64Env(name string, fallback int64) int64 {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || v <= 0 {
		slog.Warn("invalid S3 admission integer env; using default", "name", name, "value", raw, "default", fallback)
		return fallback
	}
	return v
}

func parseS3AdmissionDurationEnv(name string, fallback time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	seconds, err := strconv.ParseInt(raw, 10, 64)
	if err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	slog.Warn("invalid S3 admission duration env; using default", "name", name, "value", raw, "default", fallback)
	return fallback
}

func parseS3AdmissionBoolEnv(name string, fallback bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		slog.Warn("invalid S3 admission boolean env; using default", "name", name, "value", raw, "default", fallback)
		return fallback
	}
	return v
}

func withS3PutAdmissionForTest(maxBytes int64, timeout time.Duration) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.putAdmission = newS3PutAdmission(maxBytes, timeout)
		server.observeS3PutAdmissionInflight()
	}
}

func WithS3PutAdmissionObserver(observer S3PutAdmissionObserver) S3ServerOption {
	return func(server *S3Server) {
		if server == nil {
			return
		}
		server.putAdmissionObserver = observer
		server.observeS3PutAdmissionInflight()
	}
}

func (a *s3PutAdmission) peekHeadroom(bytes int64) error {
	if a == nil || bytes <= 0 {
		return nil
	}
	units, ok := a.unitsFor(bytes)
	if !ok {
		return errS3PutAdmissionExhausted
	}
	if cap(a.sem)-len(a.sem) < units {
		return errS3PutAdmissionExhausted
	}
	return nil
}

func (a *s3PutAdmission) acquire(ctx context.Context, bytes int64) (func(), error) {
	if a == nil || bytes <= 0 {
		return func() {}, nil
	}
	units, ok := a.unitsFor(bytes)
	if !ok || units > 1 {
		return nil, errS3PutAdmissionExhausted
	}
	select {
	case a.sem <- struct{}{}:
		a.inflight.Add(s3ChunkSize)
		return func() { a.releaseUnits(1) }, nil
	case <-ctx.Done():
		return nil, errS3PutAdmissionExhausted
	}
}

func (a *s3PutAdmission) acquireWithTimeout(ctx context.Context, bytes int64) (func(), error) {
	if a == nil {
		return func() {}, nil
	}
	acquireCtx, cancel := context.WithTimeout(ctx, a.timeout)
	release, err := a.acquire(acquireCtx, bytes)
	cancel()
	return release, err
}

func (a *s3PutAdmission) releaseUnits(units int) {
	for i := 0; i < units; i++ {
		select {
		case <-a.sem:
			a.inflight.Add(-s3ChunkSize)
		default:
			return
		}
	}
}

func (a *s3PutAdmission) unitsFor(bytes int64) (int, bool) {
	if a == nil || bytes <= 0 {
		return 0, true
	}
	units64 := 1 + (bytes-1)/s3ChunkSize
	maxInt := int64(int(^uint(0) >> 1))
	if units64 > maxInt {
		return 0, false
	}
	units := int(units64)
	if units > cap(a.sem) {
		return 0, false
	}
	return units, true
}

func (s *S3Server) admitS3PutRequest(w http.ResponseWriter, r *http.Request, bucket, objectKey string, maxDecoded int64, tooLargeMessage string) bool {
	if s == nil || s.putAdmission == nil {
		return true
	}
	bytes, protocol, err := s.s3PutAdmissionProbeBytes(r, maxDecoded)
	if err != nil {
		s.observeS3PutAdmissionRejection(s3PutAdmissionStagePrereserve, protocol)
		if errors.Is(err, errS3PutAdmissionEntityTooLarge) {
			writeS3Error(w, http.StatusRequestEntityTooLarge, "EntityTooLarge", tooLargeMessage, bucket, objectKey)
			return false
		}
		if errors.Is(err, errS3PutAdmissionInvalidDecoded) {
			writeS3Error(w, http.StatusBadRequest, "InvalidRequest", err.Error(), bucket, objectKey)
			return false
		}
		writeS3Error(w, http.StatusLengthRequired, "MissingContentLength", err.Error(), bucket, objectKey)
		return false
	}
	if err := s.putAdmission.peekHeadroom(bytes); err != nil {
		s.observeS3PutAdmissionRejection(s3PutAdmissionStagePrereserve, protocol)
		writeS3AdmissionError(w, bucket, objectKey, s.s3PutAdmissionRetryAfter())
		return false
	}
	return true
}

func (s *S3Server) s3PutAdmissionProbeBytes(r *http.Request, maxDecoded int64) (int64, string, error) {
	payloadSHA := normalizeS3PayloadHash(r.Header.Get("X-Amz-Content-Sha256"))
	protocol := s3PutAdmissionProtocolForPayload(payloadSHA)
	if protocol == s3PutAdmissionProtocolChunked {
		bytes, err := s3ChunkedPutAdmissionProbeBytes(r, maxDecoded)
		return bytes, protocol, err
	}
	if r.ContentLength < 0 {
		return 0, protocol, errors.New("Content-Length is required for non-streaming S3 PUT admission")
	}
	if maxDecoded > 0 && r.ContentLength > maxDecoded {
		return 0, protocol, errS3PutAdmissionEntityTooLarge
	}
	if r.ContentLength > s3ChunkSize {
		return s3ChunkSize, protocol, nil
	}
	return r.ContentLength, protocol, nil
}

func s3ChunkedPutAdmissionProbeBytes(r *http.Request, maxDecoded int64) (int64, error) {
	declared, ok, err := s3DecodedContentLength(r)
	if err != nil {
		return 0, err
	}
	if ok && maxDecoded > 0 && declared > maxDecoded {
		return 0, errS3PutAdmissionEntityTooLarge
	}
	if maxDecoded > 0 && maxDecoded < s3ChunkSize {
		return maxDecoded, nil
	}
	return s3ChunkSize, nil
}

func s3DecodedContentLength(r *http.Request) (int64, bool, error) {
	declaredRaw := strings.TrimSpace(r.Header.Get("X-Amz-Decoded-Content-Length"))
	if declaredRaw == "" {
		return 0, false, nil
	}
	declared, err := strconv.ParseInt(declaredRaw, 10, 64)
	if err != nil || declared < 0 {
		return 0, true, errS3PutAdmissionInvalidDecoded
	}
	return declared, true, nil
}

func s3PutAdmissionProtocolForPayload(payloadSHA string) string {
	if isS3StreamingPayloadMarker(payloadSHA) {
		return s3PutAdmissionProtocolChunked
	}
	return s3PutAdmissionProtocolFixed
}

func (s *S3Server) acquireS3PutAdmission(ctx context.Context, bytes int64, protocol string) (func(), error) {
	if s == nil || s.putAdmission == nil {
		return func() {}, nil
	}
	if protocol == s3PutAdmissionProtocolChunked && !s.putAdmission.chunked {
		return func() {}, nil
	}
	start := time.Now()
	release, err := s.putAdmission.acquireWithTimeout(ctx, bytes)
	s.observeS3PutAdmissionWait(s3PutAdmissionStagePerBatch, protocol, time.Since(start))
	s.observeS3PutAdmissionInflight()
	if err != nil {
		s.observeS3PutAdmissionRejection(s3PutAdmissionStagePerBatch, protocol)
		return nil, err
	}
	return func() {
		release()
		s.observeS3PutAdmissionInflight()
	}, nil
}

func (s *S3Server) shouldFlushS3PutBatchBeforeRead(protocol string, pendingAdmission int) bool {
	if pendingAdmission == 0 || s == nil || s.putAdmission == nil {
		return false
	}
	if protocol == s3PutAdmissionProtocolChunked {
		return true
	}
	return pendingAdmission >= cap(s.putAdmission.sem)
}

func (s *S3Server) s3PutAdmissionRetryAfter() time.Duration {
	return s3PutAdmissionDefaultRetryAfter
}

func writeS3AdmissionError(w http.ResponseWriter, bucket, objectKey string, retryAfter time.Duration) {
	if retryAfter < time.Second {
		retryAfter = time.Second
	}
	w.Header().Set("Connection", "close")
	w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter/time.Second)))
	writeS3Error(w, http.StatusServiceUnavailable, "SlowDown", "Reduce your request rate", bucket, objectKey)
}

func (s *S3Server) observeS3PutAdmissionInflight() {
	if s == nil || s.putAdmissionObserver == nil {
		return
	}
	var bytes int64
	if s.putAdmission != nil {
		bytes = s.putAdmission.inflight.Load()
	}
	s.putAdmissionObserver.ObserveS3PutAdmissionInflight(bytes)
}

func (s *S3Server) observeS3PutAdmissionRejection(stage, protocol string) {
	if s == nil || s.putAdmissionObserver == nil {
		return
	}
	s.putAdmissionObserver.ObserveS3PutAdmissionRejection(stage, protocol)
}

func (s *S3Server) observeS3PutAdmissionWait(stage, protocol string, duration time.Duration) {
	if s == nil || s.putAdmissionObserver == nil {
		return
	}
	s.putAdmissionObserver.ObserveS3PutAdmissionWait(stage, protocol, duration)
}
