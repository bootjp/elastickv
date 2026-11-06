package adapter

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestS3ObjectKeyByteLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		key    string
		status int
	}{
		{"ascii boundary", strings.Repeat("a", 1024), http.StatusOK},
		{"ascii overflow", strings.Repeat("a", 1025), http.StatusBadRequest},
		{"utf8 boundary", strings.Repeat("\xc3\xa9", 512), http.StatusOK},
		{"utf8 overflow", strings.Repeat("\xc3\xa9", 513), http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newS3OCCReproHarness(t)
			h.startUpload("key-limit")
			response := h.do(http.MethodPost, "/key-limit/"+url.PathEscape(tc.key)+"?uploads", "")
			require.Equal(t, tc.status, response.Code, response.Body.String())
		})
	}
}

func TestS3AdminPutRejectsOverlongObjectKey(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproHarness(t)
	h.startUpload("admin-key-limit")
	err := h.server.AdminPutObject(t.Context(), fullAdminBucketsPrincipal(), "admin-key-limit", strings.Repeat("a", 1025), strings.NewReader("value"), "text/plain")
	require.ErrorIs(t, err, ErrAdminInvalidObjectKey)
}

type s3DescriptorReadCounter struct {
	store.MVCCStore
	reads int
}

func (s *s3DescriptorReadCounter) GetAt(context.Context, []byte, uint64) ([]byte, error) {
	s.reads++
	return nil, errors.WithStack(store.ErrKeyNotFound)
}

func TestS3MultipartReadKeyBudgetPrecedesAllocationAndReads(t *testing.T) {
	t.Parallel()
	const budget = 24 << 20
	for _, escaped := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "escaped"}[escaped], func(t *testing.T) {
			t.Parallel()
			segment := "a"
			if escaped {
				segment = "\x00"
			}
			completion := s3MultipartCompletion{bucket: strings.Repeat(segment, budget/10000), objectKey: "object", uploadID: "upload"}
			requests := make([]s3CompleteMultipartUploadPart, 10000)
			for i := range requests {
				requests[i].PartNumber = i + 1
			}
			st := &s3DescriptorReadCounter{}
			err := (&S3Server{store: st}).loadS3MultipartParts(t.Context(), 1, requests, &completion)
			var responseErr *s3ResponseError
			require.ErrorAs(t, err, &responseErr)
			require.Equal(t, "InvalidArgument", responseErr.Code)
			require.Zero(t, st.reads, "reject before loading even the first descriptor")
			require.Zero(t, cap(completion.partKeys), "reject before retaining the key array")
			require.Zero(t, cap(completion.manifestParts), "reject before retaining manifest storage")
		})
	}
}

func TestS3MaximumConformingMultipartKeysFitByteBudget(t *testing.T) {
	t.Parallel()
	key := s3keys.UploadPartKey(strings.Repeat("b", 63), ^uint64(0), strings.Repeat("\x00", 1024), strings.Repeat("u", 64), 10000)
	require.LessOrEqual(t, len(key)*10000, 24<<20)
	require.NoError(t, validateS3MultipartReadKeyBudget(&s3MultipartCompletion{
		bucket: strings.Repeat("b", 63), objectKey: strings.Repeat("\x00", 1024), uploadID: strings.Repeat("u", 64),
	}, 10000))
}

func TestS3MultipartReadKeyBudgetBoundary(t *testing.T) {
	t.Parallel()
	const count = 10000
	overhead := len(s3keys.UploadPartKey("", 0, "object", "upload", 0))
	completion := s3MultipartCompletion{bucket: strings.Repeat("a", s3MultipartReadKeyByteBudget/count-overhead), objectKey: "object", uploadID: "upload"}
	require.NoError(t, validateS3MultipartReadKeyBudget(&completion, count))
	completion.bucket += "a"
	require.Error(t, validateS3MultipartReadKeyBudget(&completion, count))
	for _, invalid := range []int{-1, 0, count + 1} {
		require.Error(t, validateS3MultipartReadKeyBudget(&completion, invalid))
	}
}

type s3NeverReadBody struct{ reads int }

func (b *s3NeverReadBody) Read([]byte) (int, error) {
	b.reads++
	return 0, io.EOF
}

type s3ProxyRoutingCounter struct {
	kv.Coordinator
	calls int
}

func (c *s3ProxyRoutingCounter) IsLeaderForKey([]byte) bool {
	c.calls++
	return false
}

func (*s3ProxyRoutingCounter) RaftLeaderForKey([]byte) string { return "" }

func TestS3OverlongObjectKeyRejectedBeforeProxyAndBody(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproHarness(t)
	h.startUpload("bucket")
	coordinator := &s3ProxyRoutingCounter{Coordinator: h.coord}
	h.server.coordinator = coordinator
	beforeReads := len(h.reads.readLog())
	body := &s3NeverReadBody{}
	request := httptest.NewRequest(http.MethodPut, "/bucket/"+strings.Repeat("a", 1025), body)
	response := httptest.NewRecorder()
	h.server.handle(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	require.Zero(t, body.reads)
	require.Zero(t, coordinator.calls)
	require.Len(t, h.reads.readLog(), beforeReads)
	// Positive control: entering the real proxy path does read and route.
	require.True(t, h.server.maybeProxyToLeader(httptest.NewRecorder(), request))
	require.Positive(t, coordinator.calls)
	require.Greater(t, len(h.reads.readLog()), beforeReads)
}

func TestS3AdminLegacyOverlongObjectRecovery(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproHarness(t)
	h.startUpload("legacy-key")
	key := strings.Repeat("a", 1025)
	// Seed a pre-limit object through the existing internal fixture pipeline.
	require.NoError(t, h.server.adminPutObject(t.Context(), &s3PutObjectInput{
		body: io.NopCloser(strings.NewReader("legacy")), contentLength: -1,
		streamBody: &s3StreamingBody{}, contentType: "text/plain",
	}, "legacy-key", key))
	body, _, err := h.server.AdminGetObject(t.Context(), fullAdminBucketsPrincipal(), "legacy-key", key)
	require.NoError(t, err)
	value, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, "legacy", string(value))
	require.NoError(t, h.server.AdminDeleteObject(t.Context(), fullAdminBucketsPrincipal(), "legacy-key", key))
}
