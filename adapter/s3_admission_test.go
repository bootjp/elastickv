package adapter

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type recordingS3PutAdmissionObserver struct {
	lastInflight int64
	rejections   map[string]int
	waits        map[string]int
}

func newRecordingS3PutAdmissionObserver() *recordingS3PutAdmissionObserver {
	return &recordingS3PutAdmissionObserver{
		rejections: map[string]int{},
		waits:      map[string]int{},
	}
}

func (o *recordingS3PutAdmissionObserver) ObserveS3PutAdmissionInflight(bytes int64) {
	o.lastInflight = bytes
}

func (o *recordingS3PutAdmissionObserver) ObserveS3PutAdmissionRejection(stage, protocol string) {
	o.rejections[stage+"|"+protocol]++
}

func (o *recordingS3PutAdmissionObserver) ObserveS3PutAdmissionWait(stage, protocol string, _ time.Duration) {
	o.waits[stage+"|"+protocol]++
}

func TestS3PutAdmissionAcquireReleaseAndHeadroom(t *testing.T) {
	t.Parallel()

	admission := newS3PutAdmission(2*s3ChunkSize, time.Second)
	require.NoError(t, admission.peekHeadroom(2*s3ChunkSize))
	require.ErrorIs(t, admission.peekHeadroom(2*s3ChunkSize+1), errS3PutAdmissionExhausted)

	release, err := admission.acquire(context.Background(), s3ChunkSize)
	require.NoError(t, err)
	require.EqualValues(t, s3ChunkSize, admission.inflight.Load())
	require.ErrorIs(t, admission.peekHeadroom(2*s3ChunkSize), errS3PutAdmissionExhausted)

	release()
	require.Zero(t, admission.inflight.Load())
	require.NoError(t, admission.peekHeadroom(2*s3ChunkSize))
}

func TestS3PutAdmissionAcquireRejectsMultiUnitRequest(t *testing.T) {
	t.Parallel()

	admission := newS3PutAdmission(2*s3ChunkSize, time.Second)
	release, err := admission.acquire(context.Background(), 2*s3ChunkSize)
	require.ErrorIs(t, err, errS3PutAdmissionExhausted)
	require.Nil(t, release)
	require.Zero(t, admission.inflight.Load())
	require.NoError(t, admission.peekHeadroom(2*s3ChunkSize))
}

func TestS3PutAdmissionAcquireTimeoutKeepsExistingLease(t *testing.T) {
	t.Parallel()

	admission := newS3PutAdmission(s3ChunkSize, 5*time.Millisecond)
	release, err := admission.acquire(context.Background(), s3ChunkSize)
	require.NoError(t, err)
	released := false
	t.Cleanup(func() {
		if !released {
			release()
		}
	})

	_, err = admission.acquireWithTimeout(context.Background(), s3ChunkSize)
	require.ErrorIs(t, err, errS3PutAdmissionExhausted)
	require.EqualValues(t, s3ChunkSize, admission.inflight.Load())

	release()
	released = true
	require.Zero(t, admission.inflight.Load())
}

func TestS3PutAdmissionFromEnvCanDisableChunkedIncremental(t *testing.T) {
	t.Setenv(s3PutAdmissionChunkedEnv, "false")

	admission := newS3PutAdmissionFromEnv()
	require.NotNil(t, admission)
	require.False(t, admission.chunked)
}

func TestS3PutAdmissionFromEnvCanDisableCapWithMaxInt(t *testing.T) {
	t.Setenv(s3PutAdmissionMaxInflightEnv, "9223372036854775807")

	require.Nil(t, newS3PutAdmissionFromEnv())
}

func TestS3PutAdmissionProbeUsesBootstrapForChunkedDecodedLength(t *testing.T) {
	t.Parallel()

	server := &S3Server{putAdmission: newS3PutAdmission(s3ChunkSize, time.Second)}
	req := newS3TestRequest(http.MethodPut, "/bucket/key", strings.NewReader("ignored"))
	req.Header.Set("X-Amz-Content-Sha256", s3StreamingUnsignedPayloadTrailer)
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.Itoa(s3ChunkSize+1))

	bytes, protocol, err := server.s3PutAdmissionProbeBytes(req, s3MaxObjectSizeBytes)
	require.NoError(t, err)
	require.EqualValues(t, s3ChunkSize, bytes)
	require.Equal(t, s3PutAdmissionProtocolChunked, protocol)
}

func TestS3PutAdmissionProbeRejectsChunkedDecodedLengthOverS3Limit(t *testing.T) {
	t.Parallel()

	server := &S3Server{putAdmission: newS3PutAdmission(s3ChunkSize, time.Second)}
	req := newS3TestRequest(http.MethodPut, "/bucket/key", strings.NewReader("ignored"))
	req.Header.Set("X-Amz-Content-Sha256", s3StreamingUnsignedPayloadTrailer)
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.FormatInt(s3MaxObjectSizeBytes+1, 10))

	bytes, protocol, err := server.s3PutAdmissionProbeBytes(req, s3MaxObjectSizeBytes)
	require.ErrorIs(t, err, errS3PutAdmissionEntityTooLarge)
	require.Zero(t, bytes)
	require.Equal(t, s3PutAdmissionProtocolChunked, protocol)
}

func TestS3PutAdmissionProbeUsesChunkHeadroomForFixedLength(t *testing.T) {
	t.Parallel()

	server := &S3Server{putAdmission: newS3PutAdmission(s3ChunkSize, time.Second)}
	req := newS3TestRequest(http.MethodPut, "/bucket/key", http.NoBody)
	req.ContentLength = 2*s3ChunkSize + 1

	bytes, protocol, err := server.s3PutAdmissionProbeBytes(req, s3MaxObjectSizeBytes)
	require.NoError(t, err)
	require.EqualValues(t, s3ChunkSize, bytes)
	require.Equal(t, s3PutAdmissionProtocolFixed, protocol)
}

func TestS3Server_PutObjectAdmissionAllowsFixedLengthLargerThanBudget(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, 2*time.Second)
	createS3AdmissionTestBucket(t, server, "admit-big")

	payload := strings.Repeat("x", s3ChunkSize+1)
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-big/larger-than-budget.bin", strings.NewReader(payload))
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Greater(t, observer.waits[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolFixed], 1)
	require.Zero(t, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolFixed])
	require.Zero(t, observer.rejections[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolFixed])
	require.Zero(t, observer.lastInflight)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/admit-big/larger-than-budget.bin", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, payload, rec.Body.String())
}

func TestS3Server_PutObjectAdmissionRejectsFixedLengthOverS3LimitAsEntityTooLarge(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	createS3AdmissionTestBucket(t, server, "admit-s3-limit")

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-s3-limit/too-large.bin", http.NoBody)
	req.ContentLength = s3MaxObjectSizeBytes + 1
	server.handle(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>EntityTooLarge</Code>")
	require.Contains(t, rec.Body.String(), "<Message>object exceeds maximum allowed size</Message>")
	require.Equal(t, 1, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolFixed])
	require.Zero(t, observer.lastInflight)
}

func TestS3Server_PutObjectAdmissionRejectsChunkedOverS3LimitBeforeSlowDown(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	createS3AdmissionTestBucket(t, server, "admit-chunked-limit")

	release, err := server.putAdmission.acquire(context.Background(), s3ChunkSize)
	require.NoError(t, err)
	defer release()

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-chunked-limit/too-large.bin", http.NoBody)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Content-Sha256", s3StreamingUnsignedPayloadTrailer)
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.FormatInt(s3MaxObjectSizeBytes+1, 10))
	server.handle(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>EntityTooLarge</Code>")
	require.Zero(t, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolChunked])
	require.Zero(t, observer.rejections[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolChunked])
}

func TestS3Server_PutObjectAdmissionValidatesProtocolBeforeSlowDown(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	createS3AdmissionTestBucket(t, server, "admit-protocol")

	release, err := server.putAdmission.acquire(context.Background(), s3ChunkSize)
	require.NoError(t, err)
	defer release()

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-protocol/signed.bin", strings.NewReader("ignored"))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Content-Sha256", s3StreamingSignedPayload)
	req.Header.Set("X-Amz-Decoded-Content-Length", "7")
	server.handle(rec, req)
	require.Equal(t, http.StatusNotImplemented, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>NotImplemented</Code>")

	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodPut, "/admit-protocol/malformed.bin", nil)
	req.Body = io.NopCloser(strings.NewReader("5\r\nhello\r\n0\r\n\r\n"))
	req.ContentLength = -1
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Content-Sha256", sha256Hex("hello"))
	server.handle(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>InvalidRequest</Code>")
	require.Zero(t, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolChunked])
	require.Zero(t, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolFixed])
}

func TestS3Server_PutObjectAdmissionRequiresContentLengthForPlainPut(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	createS3AdmissionTestBucket(t, server, "admit-length")

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-length/no-length.bin", nil)
	req.Body = io.NopCloser(strings.NewReader("payload"))
	req.ContentLength = -1
	server.handle(rec, req)

	require.Equal(t, http.StatusLengthRequired, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>MissingContentLength</Code>")
	require.Equal(t, 1, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolFixed])
	require.Zero(t, observer.lastInflight)
}

func TestS3Server_PutObjectAdmissionAllowsSmallRequestAndReleasesBudget(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	createS3AdmissionTestBucket(t, server, "admit-small")

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/admit-small/ok.txt", strings.NewReader("ok")))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Greater(t, observer.waits[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolFixed], 0)
	require.Zero(t, observer.lastInflight)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/admit-small/ok.txt", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "ok", rec.Body.String())
}

func TestS3Server_PutObjectAdmissionAcquiresBeforeBodyRead(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	createS3AdmissionTestBucket(t, server, "admit-before-read")

	reader := &s3AdmissionObservedReader{
		server:  server,
		payload: []byte("bounded"),
	}
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-before-read/object.bin", reader)
	req.ContentLength = int64(len(reader.payload))
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.EqualValues(t, s3ChunkSize, reader.firstReadInflight)
	require.Zero(t, observer.lastInflight)
}

func TestS3Server_PutObjectAdmissionRetryAfterIgnoresWaitTimeout(t *testing.T) {
	t.Parallel()

	server, _ := newS3AdmissionTestServer(t, 45*time.Second)
	createS3AdmissionTestBucket(t, server, "admit-retry-after")
	release, err := server.putAdmission.acquire(context.Background(), s3ChunkSize)
	require.NoError(t, err)
	defer release()

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/admit-retry-after/blocked.bin", strings.NewReader("blocked")))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>SlowDown</Code>")
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
}

func TestS3Server_PutObjectAdmissionExactFixedLengthChunkDoesNotChargeEOF(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	createS3AdmissionTestBucket(t, server, "admit-exact")

	payload := bytes.Repeat([]byte("x"), s3ChunkSize)
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/admit-exact/exact.bin", bytes.NewReader(payload)))

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, 1, observer.waits[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolFixed])
	require.Zero(t, observer.lastInflight)
}

func TestS3Server_PutObjectRejectsTruncatedFixedLengthBody(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	createS3AdmissionTestBucket(t, server, "admit-truncated")

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-truncated/short.bin", strings.NewReader("short"))
	req.ContentLength = int64(len("short") + 1)
	server.handle(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>IncompleteBody</Code>")
	require.Zero(t, observer.lastInflight)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/admit-truncated/short.bin", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Server_PutObjectAdmissionReportsReadErrorBeforeSlowDown(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, 5*time.Millisecond)
	createS3AdmissionTestBucket(t, server, "admit-read-error")

	reader := &s3AdmissionFillingErrorReader{
		server:  server,
		payload: []byte("bad"),
		err:     newAwsChunkedError(io.ErrUnexpectedEOF),
	}
	defer reader.releaseHeld()
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-read-error/body.bin", reader)
	req.ContentLength = int64(len(reader.payload))
	server.handle(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>InvalidRequest</Code>")
	require.Zero(t, observer.rejections[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolFixed])
}

func TestS3Server_PutObjectAdmissionChunkedStreamsPastSingleChunkBudget(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, 5*time.Millisecond)
	createS3AdmissionTestBucket(t, server, "admit-chunked")

	payload := bytes.Repeat([]byte("x"), s3ChunkSize+1)
	body := encodeAwsChunked(t, payload, len(payload), "", "")
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-chunked/midstream.bin", bytes.NewReader(body))
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Content-Sha256", s3StreamingUnsignedPayloadTrailer)
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Greater(t, observer.waits[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolChunked], 1)
	require.Zero(t, observer.rejections[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolChunked])
	require.Zero(t, observer.lastInflight)
	require.Zero(t, server.putAdmission.inflight.Load())

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/admit-chunked/midstream.bin", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, string(payload), rec.Body.String())
}

func TestS3Server_PutObjectAdmissionDisabledAllowsPlainChunkedEOF(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		withS3PutAdmissionForTest(s3PutAdmissionDisableMaxInflightBytes, time.Second),
	)
	createS3AdmissionTestBucket(t, server, "admit-disabled")

	payload := "plain chunked body"
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/admit-disabled/chunked.bin", strings.NewReader(payload))
	req.ContentLength = -1
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/admit-disabled/chunked.bin", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, payload, rec.Body.String())
}

func TestS3Server_UploadPartAdmissionAcquiresBeforeBodyRead(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	uploadID := initS3AdmissionMultipartUpload(t, server, "admit-part-before-read")

	reader := &s3AdmissionObservedReader{
		server:  server,
		payload: []byte("part"),
	}
	rec := httptest.NewRecorder()
	req := newS3TestRequest(
		http.MethodPut,
		fmt.Sprintf("/admit-part-before-read/object.bin?uploadId=%s&partNumber=1", uploadID),
		reader,
	)
	req.ContentLength = int64(len(reader.payload))
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.EqualValues(t, s3ChunkSize, reader.firstReadInflight)
	require.Zero(t, observer.lastInflight)
}

func TestS3Server_UploadPartAdmissionDisabledAllowsPlainChunkedEOF(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		withS3PutAdmissionForTest(s3PutAdmissionDisableMaxInflightBytes, time.Second),
	)
	uploadID := initS3AdmissionMultipartUpload(t, server, "admit-part-disabled")

	payload := "plain chunked part"
	rec := httptest.NewRecorder()
	req := newS3TestRequest(
		http.MethodPut,
		fmt.Sprintf("/admit-part-disabled/object.bin?uploadId=%s&partNumber=1", uploadID),
		strings.NewReader(payload),
	)
	req.ContentLength = -1
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, quoteS3ETag(md5Hex(payload)), rec.Header().Get("ETag"))
}

func TestS3Server_UploadPartAdmissionValidatesProtocolBeforeSlowDown(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, time.Second)
	uploadID := initS3AdmissionMultipartUpload(t, server, "admit-part-protocol")

	release, err := server.putAdmission.acquire(context.Background(), s3ChunkSize)
	require.NoError(t, err)
	defer release()

	rec := httptest.NewRecorder()
	req := newS3TestRequest(
		http.MethodPut,
		fmt.Sprintf("/admit-part-protocol/object.bin?uploadId=%s&partNumber=1", uploadID),
		strings.NewReader("ignored"),
	)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Content-Sha256", s3StreamingSignedPayload)
	req.Header.Set("X-Amz-Decoded-Content-Length", "7")
	server.handle(rec, req)
	require.Equal(t, http.StatusNotImplemented, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>NotImplemented</Code>")

	rec = httptest.NewRecorder()
	req = newS3TestRequest(
		http.MethodPut,
		fmt.Sprintf("/admit-part-protocol/object.bin?uploadId=%s&partNumber=2", uploadID),
		nil,
	)
	req.Body = io.NopCloser(strings.NewReader("5\r\nhello\r\n0\r\n\r\n"))
	req.ContentLength = -1
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Content-Sha256", sha256Hex("hello"))
	server.handle(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>InvalidRequest</Code>")
	require.Zero(t, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolChunked])
	require.Zero(t, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolFixed])
}

func TestS3Server_UploadPartAdmissionAllowsFixedLengthLargerThanBudget(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, 2*time.Second)
	createS3AdmissionTestBucket(t, server, "admit-part")

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPost, "/admit-part/object.bin?uploads=", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	require.NotEmpty(t, initResult.UploadId)

	payload := strings.Repeat("p", s3ChunkSize+1)
	rec = httptest.NewRecorder()
	req = newS3TestRequest(
		http.MethodPut,
		fmt.Sprintf("/admit-part/object.bin?uploadId=%s&partNumber=1", initResult.UploadId),
		strings.NewReader(payload),
	)
	server.handle(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, quoteS3ETag(md5Hex(payload)), rec.Header().Get("ETag"))
	require.Greater(t, observer.waits[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolFixed], 1)
	require.Zero(t, observer.rejections[s3PutAdmissionStagePrereserve+"|"+s3PutAdmissionProtocolFixed])
	require.Zero(t, observer.rejections[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolFixed])
	require.Zero(t, observer.lastInflight)
	require.Zero(t, server.putAdmission.inflight.Load())
}

func TestS3Server_UploadPartAdmissionReportsReadErrorBeforeSlowDown(t *testing.T) {
	t.Parallel()

	server, observer := newS3AdmissionTestServer(t, 5*time.Millisecond)
	uploadID := initS3AdmissionMultipartUpload(t, server, "admit-part-read-error")

	reader := &s3AdmissionFillingErrorReader{
		server:  server,
		payload: []byte("bad"),
		err:     newAwsChunkedError(io.ErrUnexpectedEOF),
	}
	defer reader.releaseHeld()
	rec := httptest.NewRecorder()
	req := newS3TestRequest(
		http.MethodPut,
		fmt.Sprintf("/admit-part-read-error/object.bin?uploadId=%s&partNumber=1", uploadID),
		reader,
	)
	req.ContentLength = int64(len(reader.payload))
	server.handle(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>InvalidRequest</Code>")
	require.Zero(t, observer.rejections[s3PutAdmissionStagePerBatch+"|"+s3PutAdmissionProtocolFixed])
}

type s3AdmissionFillingErrorReader struct {
	server  *S3Server
	payload []byte
	err     error
	release func()
	done    bool
}

type s3AdmissionObservedReader struct {
	server            *S3Server
	payload           []byte
	firstReadInflight int64
	done              bool
}

func (r *s3AdmissionObservedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.server != nil && r.server.putAdmission != nil {
		r.firstReadInflight = r.server.putAdmission.inflight.Load()
	}
	r.done = true
	return copy(p, r.payload), io.EOF
}

func (r *s3AdmissionFillingErrorReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	if r.server != nil && r.server.putAdmission != nil && r.server.putAdmission.inflight.Load() == 0 {
		release, err := r.server.putAdmission.acquire(context.Background(), s3ChunkSize)
		if err == nil {
			r.release = release
		}
	}
	r.done = true
	return copy(p, r.payload), r.err
}

func (r *s3AdmissionFillingErrorReader) releaseHeld() {
	if r.release == nil {
		return
	}
	r.release()
	r.release = nil
}

func newS3AdmissionTestServer(t *testing.T, timeout time.Duration) (*S3Server, *recordingS3PutAdmissionObserver) {
	t.Helper()

	st := store.NewMVCCStore()
	observer := newRecordingS3PutAdmissionObserver()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		withS3PutAdmissionForTest(s3ChunkSize, timeout),
		WithS3PutAdmissionObserver(observer),
	)
	return server, observer
}

func createS3AdmissionTestBucket(t *testing.T, server *S3Server, bucket string) {
	t.Helper()

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/"+bucket, nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func initS3AdmissionMultipartUpload(t *testing.T, server *S3Server, bucket string) string {
	t.Helper()

	createS3AdmissionTestBucket(t, server, bucket)
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, fmt.Sprintf("/%s/object.bin?uploads=", bucket), nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	require.NotEmpty(t, initResult.UploadId)
	return initResult.UploadId
}
