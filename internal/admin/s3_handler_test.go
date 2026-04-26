package admin

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// stubBucketsSource is the in-memory test double the S3 admin
// handler tests use. AdminListBuckets returns summaries in lex order
// of bucket name, matching the adapter contract; descErr / listErr
// let tests trigger the storage-failure paths without standing up a
// real adapter.
type stubBucketsSource struct {
	buckets map[string]BucketSummary
	listErr error
	descErr error
}

func (s *stubBucketsSource) AdminListBuckets(_ context.Context) ([]BucketSummary, error) {
	if s.listErr != nil {
		return nil, s.listErr
	}
	out := make([]BucketSummary, 0, len(s.buckets))
	names := make([]string, 0, len(s.buckets))
	for k := range s.buckets {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, n := range names {
		out = append(out, s.buckets[n])
	}
	return out, nil
}

func (s *stubBucketsSource) AdminDescribeBucket(_ context.Context, name string) (*BucketSummary, bool, error) {
	if s.descErr != nil {
		return nil, false, s.descErr
	}
	b, ok := s.buckets[name]
	if !ok {
		return nil, false, nil
	}
	return &b, true, nil
}

func newS3HandlerForTest(src BucketsSource) *S3Handler {
	return NewS3Handler(src)
}

func TestNewS3Handler_NilSourceReturnsNil(t *testing.T) {
	// A nil source is the well-known "S3 admin disabled" signal so a
	// build that ships without the S3 adapter can pass nil into
	// ServerDeps.Buckets and have the routes silently disappear.
	require.Nil(t, NewS3Handler(nil))
}

func TestS3Handler_ListBuckets_EmptyArrayNotNull(t *testing.T) {
	h := newS3HandlerForTest(&stubBucketsSource{buckets: map[string]BucketSummary{}})
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), `"buckets":[]`)
	require.NotContains(t, rec.Body.String(), `"next_token"`)
}

func TestS3Handler_ListBuckets_HappyPath(t *testing.T) {
	h := newS3HandlerForTest(&stubBucketsSource{buckets: map[string]BucketSummary{
		"alpha": {Name: "alpha", ACL: "private", Generation: 1},
		"bravo": {Name: "bravo", ACL: "public-read", Generation: 2},
	}})
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp s3ListResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Buckets, 2)
	require.Equal(t, "alpha", resp.Buckets[0].Name)
	require.Equal(t, "private", resp.Buckets[0].ACL)
	require.Equal(t, "bravo", resp.Buckets[1].Name)
	require.Equal(t, "public-read", resp.Buckets[1].ACL)
	require.Empty(t, resp.NextToken)
}

func TestS3Handler_ListBuckets_PaginationCursorRoundtrips(t *testing.T) {
	// Three buckets + limit=2 should produce a first page of 2 +
	// a next_token; passing that token back yields the third.
	src := &stubBucketsSource{buckets: map[string]BucketSummary{
		"a-bucket": {Name: "a-bucket"},
		"b-bucket": {Name: "b-bucket"},
		"c-bucket": {Name: "c-bucket"},
	}}
	h := newS3HandlerForTest(src)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, pathS3Buckets+"?limit=2", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var page1 s3ListResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	require.Len(t, page1.Buckets, 2)
	require.Equal(t, "a-bucket", page1.Buckets[0].Name)
	require.Equal(t, "b-bucket", page1.Buckets[1].Name)
	require.NotEmpty(t, page1.NextToken)

	// Cursor must be base64url-encoded "b-bucket" (the last name on
	// page 1) so the decoder can consume it without round-tripping
	// through encodeListNextToken.
	decoded, err := base64.RawURLEncoding.DecodeString(page1.NextToken)
	require.NoError(t, err)
	require.Equal(t, "b-bucket", string(decoded))

	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		pathS3Buckets+"?limit=2&next_token="+page1.NextToken, nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var page2 s3ListResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	require.Len(t, page2.Buckets, 1)
	require.Equal(t, "c-bucket", page2.Buckets[0].Name)
	require.Empty(t, page2.NextToken)
}

func TestS3Handler_ListBuckets_RejectsInvalidLimit(t *testing.T) {
	cases := []struct {
		name  string
		limit string
		want  string
	}{
		{"non-numeric", "abc", "limit must be an integer"},
		{"zero", "0", "limit must be positive"},
		{"negative", "-3", "limit must be positive"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newS3HandlerForTest(&stubBucketsSource{})
			req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"?limit="+tc.limit, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
			require.Contains(t, rec.Body.String(), tc.want)
		})
	}
}

func TestS3Handler_ListBuckets_OversizeLimitClamped(t *testing.T) {
	// limit beyond the ceiling is silently clamped (not rejected) so
	// the SPA's "request the maximum" pattern works without a probe
	// round-trip. Mirrors the Dynamo handler's policy.
	h := newS3HandlerForTest(&stubBucketsSource{buckets: map[string]BucketSummary{}})
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"?limit=99999", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestS3Handler_ListBuckets_RejectsInvalidNextToken(t *testing.T) {
	h := newS3HandlerForTest(&stubBucketsSource{})
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"?next_token=!!!", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "next_token is not valid base64url")
}

func TestS3Handler_ListBuckets_StorageErrorReturns500(t *testing.T) {
	src := &stubBucketsSource{listErr: errors.New("storage backing sentinel LIST-1")}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "s3_list_failed")
	require.NotContains(t, rec.Body.String(), "LIST-1",
		"server-side error detail must not leak to the client")
}

func TestS3Handler_ListBuckets_RejectsNonGet(t *testing.T) {
	// POST/PUT/DELETE on /buckets are reserved for the next slice;
	// for now the handler returns 405 so a SPA bug that calls them
	// against this build sees a sensible error rather than a 404.
	cases := []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch}
	for _, m := range cases {
		t.Run(m, func(t *testing.T) {
			h := newS3HandlerForTest(&stubBucketsSource{})
			req := httptest.NewRequest(m, pathS3Buckets, nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

func TestS3Handler_DescribeBucket_HappyPath(t *testing.T) {
	src := &stubBucketsSource{buckets: map[string]BucketSummary{
		"orders": {Name: "orders", ACL: "private", Region: "us-east-1", Generation: 7},
	}}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"/orders", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	var got BucketSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "orders", got.Name)
	require.Equal(t, "private", got.ACL)
	require.Equal(t, "us-east-1", got.Region)
	require.EqualValues(t, 7, got.Generation)
}

func TestS3Handler_DescribeBucket_MissingReturns404(t *testing.T) {
	h := newS3HandlerForTest(&stubBucketsSource{buckets: map[string]BucketSummary{}})
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"/missing", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "not_found")
}

func TestS3Handler_DescribeBucket_StorageErrorReturns500(t *testing.T) {
	src := &stubBucketsSource{descErr: errors.New("storage backing sentinel DESC-1")}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"/anything", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Contains(t, rec.Body.String(), "s3_describe_failed")
	require.NotContains(t, rec.Body.String(), "DESC-1")
}

func TestS3Handler_DescribeBucket_ForbiddenReturns403(t *testing.T) {
	// ErrBucketsForbidden is reserved for the next slice's role
	// gate (read-only is fine for any authenticated session today)
	// but the handler already maps the sentinel so the slice can
	// land without re-touching the error translator.
	src := &stubBucketsSource{descErr: ErrBucketsForbidden}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"/locked", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "forbidden")
}

func TestS3Handler_DescribeBucket_SubpathReturns404(t *testing.T) {
	// /buckets/foo/acl is reserved for the next slice. Until then,
	// any path with a slash inside the bucket-name segment must 404
	// rather than mistakenly reach handleDescribe with the full
	// "foo/acl" string.
	h := newS3HandlerForTest(&stubBucketsSource{buckets: map[string]BucketSummary{
		"foo": {Name: "foo"},
	}})
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"/foo/acl", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFormatBucketCreatedAt_ZeroProducesEmpty(t *testing.T) {
	// Zero HLC means "no creation time recorded" — the SPA renders
	// it as a dash, so we emit an empty string rather than the Unix
	// epoch (1970-01-01) which would be misleading.
	require.Empty(t, FormatBucketCreatedAt(0))
}

func TestFormatBucketCreatedAt_RoundtripsSecondPrecision(t *testing.T) {
	// HLC's upper 48 bits are Unix ms. 1_777_874_400_000 ms =
	// 2026-05-04T06:00:00Z; shift left by hlcPhysicalShift (16) to
	// produce a wire HLC, format, and confirm the formatter recovers
	// UTC RFC3339 with second precision (sub-second is intentionally
	// truncated — the SPA renders timestamps to the second).
	const wallMillis = int64(1_777_874_400_000)
	hlc := uint64(wallMillis) << hlcPhysicalShift
	require.Equal(t, "2026-05-04T06:00:00Z", FormatBucketCreatedAt(hlc))
}
