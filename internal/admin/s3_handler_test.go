package admin

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// stubBucketsSource is the in-memory test double the S3 admin
// handler tests use. AdminListBuckets returns summaries in lex order
// of bucket name, matching the adapter contract; the *Err fields let
// tests trigger the structured-failure paths without standing up a
// real adapter. lastCreate / lastDelete / lastPutACL pin the
// principal + payload that reached the source so tests can prove
// the role gate and body decode wired through correctly.
type stubBucketsSource struct {
	buckets   map[string]BucketSummary
	listErr   error
	descErr   error
	createErr error
	putACLErr error
	deleteErr error

	lastCreatePrincipal AuthPrincipal
	lastCreateInput     CreateBucketRequest
	lastPutACLPrincipal AuthPrincipal
	lastPutACLBucket    string
	lastPutACLValue     string
	lastDeletePrincipal AuthPrincipal
	lastDeleteName      string
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

func (s *stubBucketsSource) AdminCreateBucket(_ context.Context, principal AuthPrincipal, in CreateBucketRequest) (*BucketSummary, error) {
	s.lastCreatePrincipal = principal
	s.lastCreateInput = in
	if s.createErr != nil {
		return nil, s.createErr
	}
	if _, exists := s.buckets[in.BucketName]; exists {
		return nil, ErrBucketsAlreadyExists
	}
	acl := in.ACL
	if acl == "" {
		acl = "private"
	}
	summary := BucketSummary{
		Name:       in.BucketName,
		ACL:        acl,
		Generation: 1,
		Owner:      principal.AccessKey,
	}
	if s.buckets == nil {
		s.buckets = map[string]BucketSummary{}
	}
	s.buckets[in.BucketName] = summary
	return &summary, nil
}

func (s *stubBucketsSource) AdminPutBucketAcl(_ context.Context, principal AuthPrincipal, name, acl string) error {
	s.lastPutACLPrincipal = principal
	s.lastPutACLBucket = name
	s.lastPutACLValue = acl
	if s.putACLErr != nil {
		return s.putACLErr
	}
	bucket, ok := s.buckets[name]
	if !ok {
		return ErrBucketsNotFound
	}
	bucket.ACL = acl
	s.buckets[name] = bucket
	return nil
}

func (s *stubBucketsSource) AdminDeleteBucket(_ context.Context, principal AuthPrincipal, name string) error {
	s.lastDeletePrincipal = principal
	s.lastDeleteName = name
	if s.deleteErr != nil {
		return s.deleteErr
	}
	if _, ok := s.buckets[name]; !ok {
		return ErrBucketsNotFound
	}
	delete(s.buckets, name)
	return nil
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

// TestS3Handler_ListBuckets_ForbiddenReturns403 mirrors the
// describe-side coverage for the slice 2 role gate. handleList now
// maps ErrBucketsForbidden to 403 (Gemini medium on PR #658); this
// test pins that behaviour so a future refactor that drops the
// sentinel match does not silently downgrade the response to a
// generic 500.
func TestS3Handler_ListBuckets_ForbiddenReturns403(t *testing.T) {
	src := &stubBucketsSource{listErr: ErrBucketsForbidden}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "forbidden")
}

func TestS3Handler_ListBuckets_RejectsUnsupportedMethods(t *testing.T) {
	// POST is now the create endpoint (slice 2); PUT / DELETE /
	// PATCH on the collection root are still 405. The pinned set
	// guards against a future routing refactor that accidentally
	// accepts a non-listed method on the collection.
	cases := []string{http.MethodPut, http.MethodDelete, http.MethodPatch}
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

func TestS3Handler_PerBucket_UnknownSubpathReturns404(t *testing.T) {
	// /buckets/foo/policy (or any non-/acl sub-resource) must 404
	// rather than mistakenly reach handleDescribe with the full
	// "foo/policy" string — protects against a SPA bug that mis-
	// constructs the URL for a future sub-resource.
	h := newS3HandlerForTest(&stubBucketsSource{buckets: map[string]BucketSummary{
		"foo": {Name: "foo"},
	}})
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"/foo/policy", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Handler_PerBucket_AclSubpathRejectsGet(t *testing.T) {
	// /buckets/{name}/acl is a real sub-resource (PUT is the wire
	// shape for changing the ACL); GET on that exact path must
	// return 405 rather than mistakenly reaching handleDescribe.
	h := newS3HandlerForTest(&stubBucketsSource{buckets: map[string]BucketSummary{
		"foo": {Name: "foo"},
	}})
	req := httptest.NewRequest(http.MethodGet, pathS3Buckets+"/foo/acl", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
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

// withFullPrincipal injects a full-access AuthPrincipal into the
// request context so write-handler tests can bypass the SessionAuth
// middleware while keeping the role check live. Mirrors
// withWritePrincipal in dynamo_handler_test.go.
func withFullPrincipal(req *http.Request) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_FULL", Role: RoleFull}))
}

// withReadOnlyPrincipalForS3 mirrors withFullPrincipal but with the
// read-only role. Pinned helper makes the role intent obvious at
// the call site (the variable name in dynamo's withReadOnlyPrincipal
// is identical, hence the local rename to avoid cross-file
// confusion when both files are open).
func withReadOnlyPrincipalForS3(req *http.Request) *http.Request {
	return req.WithContext(context.WithValue(req.Context(), ctxKeyPrincipal,
		AuthPrincipal{AccessKey: "AKIA_RO", Role: RoleReadOnly}))
}

// validCreateBucketBody returns a minimal-but-valid POST body the
// happy-path tests share.
func validCreateBucketBody() string {
	return `{"bucket_name":"public-assets","acl":"public-read"}`
}

func TestS3Handler_CreateBucket_HappyPath(t *testing.T) {
	src := &stubBucketsSource{}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
		strings.NewReader(validCreateBucketBody()))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "public-assets", src.lastCreateInput.BucketName)
	require.Equal(t, "public-read", src.lastCreateInput.ACL)
	require.Equal(t, RoleFull, src.lastCreatePrincipal.Role)
	var got BucketSummary
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Equal(t, "public-assets", got.Name)
	require.Equal(t, "public-read", got.ACL)
}

func TestS3Handler_CreateBucket_ReadOnlyRoleRejected(t *testing.T) {
	src := &stubBucketsSource{}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
		strings.NewReader(validCreateBucketBody()))
	req = withReadOnlyPrincipalForS3(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastCreateInput.BucketName,
		"role gate must short-circuit before the source is reached")
}

func TestS3Handler_CreateBucket_AlreadyExistsReturns409(t *testing.T) {
	src := &stubBucketsSource{
		buckets:   map[string]BucketSummary{"public-assets": {Name: "public-assets"}},
		createErr: ErrBucketsAlreadyExists,
	}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
		strings.NewReader(validCreateBucketBody()))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Contains(t, rec.Body.String(), "already_exists")
}

func TestS3Handler_CreateBucket_NotLeaderReturns503(t *testing.T) {
	src := &stubBucketsSource{createErr: ErrBucketsNotLeader}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
		strings.NewReader(validCreateBucketBody()))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Contains(t, rec.Body.String(), "leader_unavailable")
}

func TestS3Handler_CreateBucket_RejectsInvalidJSON(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"empty body", "", "request body is empty"},
		{"not json", "not-a-json", "request body is not valid JSON"},
		{"missing bucket_name", `{"acl":"private"}`, "bucket_name is required"},
		{"unknown field", `{"bucket_name":"x","extra":"x"}`, "request body is not valid JSON"},
		{"trailing data", `{"bucket_name":"x"}{"extra":1}`, "trailing data"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newS3HandlerForTest(&stubBucketsSource{})
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(http.MethodPost, pathS3Buckets, body)
			req = withFullPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code, "body=%q", tc.body)
			require.Contains(t, rec.Body.String(), tc.want)
		})
	}
}

func TestS3Handler_CreateBucket_RejectsNULByte(t *testing.T) {
	h := newS3HandlerForTest(&stubBucketsSource{})
	body := "{\"bucket_name\":\"users\"}\x00{\"extra\":1}"
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets, strings.NewReader(body))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "NUL byte")
}

func TestS3Handler_CreateBucket_OversizeBodyReturns413(t *testing.T) {
	h := newS3HandlerForTest(&stubBucketsSource{})
	// 64 KiB + 1 byte: just over the limit.
	body := strings.Repeat("a", adminS3CreateBodyLimit+1)
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets, strings.NewReader(body))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

func TestS3Handler_PutBucketAcl_HappyPath(t *testing.T) {
	src := &stubBucketsSource{
		buckets: map[string]BucketSummary{"orders": {Name: "orders", ACL: "private"}},
	}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodPut, pathS3Buckets+"/orders/acl",
		strings.NewReader(`{"acl":"public-read"}`))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "orders", src.lastPutACLBucket)
	require.Equal(t, "public-read", src.lastPutACLValue)
}

func TestS3Handler_PutBucketAcl_ReadOnlyRoleRejected(t *testing.T) {
	src := &stubBucketsSource{
		buckets: map[string]BucketSummary{"orders": {Name: "orders"}},
	}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodPut, pathS3Buckets+"/orders/acl",
		strings.NewReader(`{"acl":"public-read"}`))
	req = withReadOnlyPrincipalForS3(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastPutACLBucket,
		"role gate must short-circuit before the source is reached")
}

func TestS3Handler_PutBucketAcl_RequiresAclField(t *testing.T) {
	h := newS3HandlerForTest(&stubBucketsSource{
		buckets: map[string]BucketSummary{"orders": {Name: "orders"}},
	})
	req := httptest.NewRequest(http.MethodPut, pathS3Buckets+"/orders/acl",
		strings.NewReader(`{}`))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "acl is required")
}

func TestS3Handler_PutBucketAcl_MissingBucketReturns404(t *testing.T) {
	src := &stubBucketsSource{putACLErr: ErrBucketsNotFound}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodPut, pathS3Buckets+"/missing/acl",
		strings.NewReader(`{"acl":"private"}`))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "not_found")
}

func TestS3Handler_PutBucketAcl_RejectsNonPut(t *testing.T) {
	cases := []string{http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodPatch}
	for _, m := range cases {
		t.Run(m, func(t *testing.T) {
			h := newS3HandlerForTest(&stubBucketsSource{})
			req := httptest.NewRequest(m, pathS3Buckets+"/foo/acl", nil)
			req = withFullPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

func TestS3Handler_DeleteBucket_HappyPath(t *testing.T) {
	src := &stubBucketsSource{
		buckets: map[string]BucketSummary{"orders": {Name: "orders"}},
	}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathS3Buckets+"/orders", nil)
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "orders", src.lastDeleteName)
	require.NotContains(t, src.buckets, "orders")
}

func TestS3Handler_DeleteBucket_ReadOnlyRoleRejected(t *testing.T) {
	src := &stubBucketsSource{
		buckets: map[string]BucketSummary{"orders": {Name: "orders"}},
	}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathS3Buckets+"/orders", nil)
	req = withReadOnlyPrincipalForS3(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Empty(t, src.lastDeleteName)
}

func TestS3Handler_DeleteBucket_NotEmptyReturns409(t *testing.T) {
	src := &stubBucketsSource{deleteErr: ErrBucketsNotEmpty}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathS3Buckets+"/orders", nil)
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Contains(t, rec.Body.String(), "bucket_not_empty")
}

func TestS3Handler_DeleteBucket_MissingReturns404(t *testing.T) {
	src := &stubBucketsSource{deleteErr: ErrBucketsNotFound}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathS3Buckets+"/missing", nil)
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "not_found")
}

func TestS3Handler_DeleteBucket_NotLeaderReturns503(t *testing.T) {
	src := &stubBucketsSource{deleteErr: ErrBucketsNotLeader}
	h := newS3HandlerForTest(src)
	req := httptest.NewRequest(http.MethodDelete, pathS3Buckets+"/orders", nil)
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
}

func TestS3Handler_WriteEndpoints_ValidationErrorReturns400(t *testing.T) {
	// Pin the writeBucketsError *ValidationError arm — exercised
	// end-to-end via the bridge (adapter ErrAdminInvalid* →
	// translateAdminBucketsError → &ValidationError{...}) but
	// previously had no handler-level coverage. Locking the arm
	// down protects against a future refactor that drops the
	// errors.As branch and silently downgrades typed validation
	// failures to 500 (Claude review on PR #669).
	const msg = "invalid bucket name: uppercase letters not allowed"
	t.Run("create", func(t *testing.T) {
		src := &stubBucketsSource{createErr: &ValidationError{Message: msg}}
		h := newS3HandlerForTest(src)
		req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
			strings.NewReader(validCreateBucketBody()))
		req = withFullPrincipal(req)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "invalid_request")
		require.Contains(t, rec.Body.String(), msg)
	})
	t.Run("put_acl", func(t *testing.T) {
		src := &stubBucketsSource{
			buckets:   map[string]BucketSummary{"orders": {Name: "orders"}},
			putACLErr: &ValidationError{Message: msg},
		}
		h := newS3HandlerForTest(src)
		req := httptest.NewRequest(http.MethodPut, pathS3Buckets+"/orders/acl",
			strings.NewReader(`{"acl":"public-read"}`))
		req = withFullPrincipal(req)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "invalid_request")
		require.Contains(t, rec.Body.String(), msg)
	})
	t.Run("delete", func(t *testing.T) {
		src := &stubBucketsSource{
			buckets:   map[string]BucketSummary{"orders": {Name: "orders"}},
			deleteErr: &ValidationError{Message: msg},
		}
		h := newS3HandlerForTest(src)
		req := httptest.NewRequest(http.MethodDelete, pathS3Buckets+"/orders", nil)
		req = withFullPrincipal(req)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		require.Equal(t, http.StatusBadRequest, rec.Code)
		require.Contains(t, rec.Body.String(), "invalid_request")
		require.Contains(t, rec.Body.String(), msg)
	})
}

// notLeaderBucketsSource simulates a follower's BucketsSource — every
// write path returns ErrBucketsNotLeader. Used to exercise the
// tryForward* integration path on S3Handler.
type notLeaderBucketsSource struct {
	stubBucketsSource
}

func (s *notLeaderBucketsSource) AdminCreateBucket(_ context.Context, _ AuthPrincipal, _ CreateBucketRequest) (*BucketSummary, error) {
	return nil, ErrBucketsNotLeader
}

func (s *notLeaderBucketsSource) AdminPutBucketAcl(_ context.Context, _ AuthPrincipal, _ string, _ string) error {
	return ErrBucketsNotLeader
}

func (s *notLeaderBucketsSource) AdminDeleteBucket(_ context.Context, _ AuthPrincipal, _ string) error {
	return ErrBucketsNotLeader
}

func TestS3Handler_CreateBucket_ForwardsOnNotLeader(t *testing.T) {
	src := &notLeaderBucketsSource{}
	fwd := &stubLeaderForwarder{createBucketRes: &ForwardResult{
		StatusCode:  http.StatusCreated,
		Payload:     []byte(`{"bucket_name":"public-assets","acl":"public-read"}`),
		ContentType: "application/json; charset=utf-8",
	}}
	h := NewS3Handler(src).WithLeaderForwarder(fwd)
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
		strings.NewReader(validCreateBucketBody()))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "public-assets", fwd.lastCreateBucketInput.BucketName,
		"forwarder must be invoked when source returns ErrBucketsNotLeader")
	require.JSONEq(t, `{"bucket_name":"public-assets","acl":"public-read"}`, rec.Body.String())
}

func TestS3Handler_DeleteBucket_ForwardsOnNotLeader(t *testing.T) {
	src := &notLeaderBucketsSource{}
	fwd := &stubLeaderForwarder{deleteBucketRes: &ForwardResult{
		StatusCode:  http.StatusNoContent,
		ContentType: "application/json; charset=utf-8",
	}}
	h := NewS3Handler(src).WithLeaderForwarder(fwd)
	req := httptest.NewRequest(http.MethodDelete, pathS3Buckets+"/orders", nil)
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "orders", fwd.lastDeleteBucketName)
}

func TestS3Handler_PutBucketAcl_ForwardsOnNotLeader(t *testing.T) {
	src := &notLeaderBucketsSource{}
	fwd := &stubLeaderForwarder{putACLRes: &ForwardResult{
		StatusCode:  http.StatusNoContent,
		ContentType: "application/json; charset=utf-8",
	}}
	h := NewS3Handler(src).WithLeaderForwarder(fwd)
	req := httptest.NewRequest(http.MethodPut, pathS3Buckets+"/orders/acl",
		strings.NewReader(`{"acl":"public-read"}`))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "orders", fwd.lastPutACLBucket)
	require.Equal(t, "public-read", fwd.lastPutACLValue)
}

func TestS3Handler_CreateBucket_ForwarderLeaderUnavailableReturns503(t *testing.T) {
	// ErrLeaderUnavailable from the forwarder layer maps to 503 +
	// Retry-After:1 — the SPA's retry contract is uniform whether
	// the leader is briefly absent or the network hiccupped.
	src := &notLeaderBucketsSource{}
	fwd := &stubLeaderForwarder{createBucketErr: ErrLeaderUnavailable}
	h := NewS3Handler(src).WithLeaderForwarder(fwd)
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
		strings.NewReader(validCreateBucketBody()))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.Contains(t, rec.Body.String(), "leader_unavailable")
}

func TestS3Handler_CreateBucket_ForwarderTransportErrorReturns503(t *testing.T) {
	// Generic gRPC error → 503 + Retry-After. The error is logged
	// on the server but never surfaces to the SPA.
	src := &notLeaderBucketsSource{}
	fwd := &stubLeaderForwarder{createBucketErr: errors.New("gRPC sentinel TX-1")}
	h := NewS3Handler(src).WithLeaderForwarder(fwd)
	req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
		strings.NewReader(validCreateBucketBody()))
	req = withFullPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	require.Equal(t, "1", rec.Header().Get("Retry-After"))
	require.NotContains(t, rec.Body.String(), "TX-1",
		"transport error detail must not leak to the client")
}

func TestS3Handler_CreateBucket_ForwarderNotInvokedForNonNotLeader(t *testing.T) {
	// The forwarder gate must run ONLY on ErrBucketsNotLeader; a
	// generic source error or AlreadyExists must fall through to
	// writeBucketsError. Otherwise a leader-direct 409 would be
	// silently re-applied at the leader.
	cases := []struct {
		name     string
		err      error
		wantCode int
	}{
		{"already_exists", ErrBucketsAlreadyExists, http.StatusConflict},
		{"forbidden", ErrBucketsForbidden, http.StatusForbidden},
		{"generic", errors.New("opaque storage failure"), http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fwd := &stubLeaderForwarder{}
			src := &stubBucketsSource{createErr: tc.err}
			h := NewS3Handler(src).WithLeaderForwarder(fwd)
			req := httptest.NewRequest(http.MethodPost, pathS3Buckets,
				strings.NewReader(validCreateBucketBody()))
			req = withFullPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, tc.wantCode, rec.Code)
			require.Empty(t, fwd.lastCreateBucketInput.BucketName,
				"forwarder must not be invoked for source error: %s", tc.name)
		})
	}
}

func TestS3Handler_WriteEndpoints_RejectMissingPrincipal(t *testing.T) {
	// Without a session principal in the context, writes must
	// 401 — SessionAuth normally enforces this; principalForWrite
	// is the second-line guard.
	cases := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{"create", http.MethodPost, pathS3Buckets, validCreateBucketBody()},
		{"put_acl", http.MethodPut, pathS3Buckets + "/foo/acl", `{"acl":"private"}`},
		{"delete", http.MethodDelete, pathS3Buckets + "/foo", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newS3HandlerForTest(&stubBucketsSource{})
			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, tc.path, body)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusUnauthorized, rec.Code)
		})
	}
}
