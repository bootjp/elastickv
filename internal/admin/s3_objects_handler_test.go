package admin

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"

	"github.com/goccy/go-json"
	"github.com/stretchr/testify/require"
)

// stubObjectsSource is the richer BucketsSource fake that backs the
// object-tier handler tests. It embeds stubBucketsSource so the
// existing bucket-tier methods stay available, and adds an in-memory
// object store keyed by (bucket, key). The four object-tier methods
// override the stubs in s3_handler_test.go.
type stubObjectsSource struct {
	stubBucketsSource

	// objects[bucket][key] = body bytes (the metadata projection is
	// synthesised from the body length and a fixed content type).
	objects map[string]map[string][]byte

	// nextErr, when non-nil, short-circuits the next call to any
	// object-tier method and returns that error verbatim. Used by
	// the sentinel-translation tests to inject specific failures.
	nextErr error

	lastPutPrincipal AuthPrincipal
	lastPutBucket    string
	lastPutKey       string
	lastPutCT        string
	lastPutBody      []byte

	lastDeletePrincipal AuthPrincipal
	lastDeleteBucket    string
	lastDeleteKey       string
}

func newStubObjectsSource() *stubObjectsSource {
	return &stubObjectsSource{
		stubBucketsSource: stubBucketsSource{buckets: map[string]BucketSummary{}},
		objects:           map[string]map[string][]byte{},
	}
}

// seedObject inserts an in-memory object for the named bucket.
// The bucket parameter is currently exercised with "photos" by
// every existing test but is left as a parameter so future
// multi-bucket tests can seed under a different bucket without
// reshaping every caller.
//
//nolint:unparam // bucket parameterised for future multi-bucket tests
func (s *stubObjectsSource) seedObject(bucket, key string, body []byte) {
	if s.objects[bucket] == nil {
		s.objects[bucket] = map[string][]byte{}
	}
	s.objects[bucket][key] = append([]byte(nil), body...)
}

func (s *stubObjectsSource) AdminListObjects(_ context.Context, _ AuthPrincipal, bucket string, opts AdminListObjectsOptions) (AdminObjectListing, error) {
	if s.nextErr != nil {
		return AdminObjectListing{}, s.nextErr
	}
	inBucket := s.objects[bucket]
	keys := make([]string, 0, len(inBucket))
	for k := range inBucket {
		if strings.HasPrefix(k, opts.Prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	// Clamp to default cap when opts.MaxKeys is unset by the handler
	// (Phase-3b handler always sets it but defend against zero).
	maxKeys := opts.MaxKeys
	if maxKeys <= 0 {
		maxKeys = defaultAdminObjectListMaxKeys
	}
	out := AdminObjectListing{}
	for _, k := range keys {
		if len(out.Objects) >= maxKeys {
			out.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(k))
			break
		}
		out.Objects = append(out.Objects, AdminObject{
			Key:         k,
			Size:        int64(len(inBucket[k])),
			ContentType: "application/octet-stream",
		})
	}
	return out, nil
}

func (s *stubObjectsSource) AdminGetObject(_ context.Context, _ AuthPrincipal, bucket, key string) (io.ReadCloser, AdminObject, error) {
	if s.nextErr != nil {
		return nil, AdminObject{}, s.nextErr
	}
	inBucket, ok := s.objects[bucket]
	if !ok {
		return nil, AdminObject{}, ErrObjectsBucketNotFound
	}
	body, ok := inBucket[key]
	if !ok {
		return nil, AdminObject{}, ErrObjectsNotFound
	}
	return io.NopCloser(bytes.NewReader(body)), AdminObject{
		Key:         key,
		Size:        int64(len(body)),
		ContentType: "application/octet-stream",
		ETag:        `"deadbeef"`,
	}, nil
}

func (s *stubObjectsSource) AdminPutObject(_ context.Context, principal AuthPrincipal, bucket, key string, body io.Reader, contentType string) error {
	s.lastPutPrincipal = principal
	s.lastPutBucket = bucket
	s.lastPutKey = key
	s.lastPutCT = contentType
	if s.nextErr != nil {
		return s.nextErr
	}
	buf, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	s.lastPutBody = append([]byte(nil), buf...)
	if s.objects[bucket] == nil {
		s.objects[bucket] = map[string][]byte{}
	}
	s.objects[bucket][key] = s.lastPutBody
	return nil
}

func (s *stubObjectsSource) AdminDeleteObject(_ context.Context, principal AuthPrincipal, bucket, key string) error {
	s.lastDeletePrincipal = principal
	s.lastDeleteBucket = bucket
	s.lastDeleteKey = key
	if s.nextErr != nil {
		return s.nextErr
	}
	if inBucket, ok := s.objects[bucket]; ok {
		delete(inBucket, key)
	}
	return nil
}

// objectPath builds the URL for an object resource the same way
// the SPA will: base64-url-encode the key and slot into the route.
// The bucket name is URL-path-escaped so the test can exercise
// bucket names with reserved characters (validateS3BucketName
// blocks them in production, but the route layer should still be
// defensive).
func objectPath(bucket, key string) string {
	return pathPrefixS3Buckets + url.PathEscape(bucket) +
		"/" + adminObjectSubResource +
		"/" + base64.RawURLEncoding.EncodeToString([]byte(key))
}

// objectsCollectionPath builds the /buckets/{name}/objects URL.
// Same caller-future parity rationale as objectPath above —
// bucket stays parameterised so future tests can target a
// different bucket without reshaping every call site.
//
//nolint:unparam // bucket parameterised for future multi-bucket tests
func objectsCollectionPath(bucket string) string {
	return pathPrefixS3Buckets + url.PathEscape(bucket) + "/" + adminObjectSubResource
}

// ---------- routing ----------

func TestS3Objects_RoutingDispatchesEachMethod(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	src.seedObject("photos", "k1", []byte("hello"))
	h := newS3HandlerForTest(src)

	cases := []struct {
		name   string
		method string
		path   string
		body   io.Reader
		expect int
	}{
		{"GET collection", http.MethodGet, objectsCollectionPath("photos"), nil, http.StatusOK},
		{"GET object", http.MethodGet, objectPath("photos", "k1"), nil, http.StatusOK},
		{"PUT object", http.MethodPut, objectPath("photos", "k2"), bytes.NewReader([]byte("body")), http.StatusNoContent},
		{"DELETE object", http.MethodDelete, objectPath("photos", "k1"), nil, http.StatusNoContent},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, tc.body)
			switch tc.method {
			case http.MethodGet:
				req = withReadOnlyPrincipal(req)
			default:
				req = withWritePrincipal(req)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, tc.expect, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func TestS3Objects_RoutingRejectsUnknownSubResource(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	// /buckets/photos/widgets — not a recognised sub-resource. The
	// "no slashes in bucket name" branch fires first, returning 404.
	req := httptest.NewRequest(http.MethodGet, pathPrefixS3Buckets+"photos/widgets", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Objects_RoutingRejectsTooManyKeySlashes(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	// /buckets/photos/objects/foo/bar — interior slash in the key
	// segment is rejected (keys must be base64-url-encoded).
	req := httptest.NewRequest(http.MethodGet, pathPrefixS3Buckets+"photos/objects/foo/bar", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "must not contain a slash")
}

// TestS3Objects_RoutingRejectsEncodedSlashInBucket pins the Codex P1
// on PR #814: r.URL.Path is pre-decoded by net/http, so an attacker
// URL like /buckets/victim%2Fobjects/{key} would (without the
// EscapedPath() switch) be interpreted as
// /buckets/victim/objects/{key} and run the operation against the
// "victim" bucket rather than the literal-named "victim/objects"
// segment. The fix: servePerBucket consults r.URL.EscapedPath() so
// the %2F stays opaque through the route split.
func TestS3Objects_RoutingRejectsEncodedSlashInBucket(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	src.seedObject("victim", "k1", []byte("secret"))
	h := newS3HandlerForTest(src)

	keySeg := base64.RawURLEncoding.EncodeToString([]byte("k1"))
	cases := []struct {
		name   string
		method string
		url    string
		body   io.Reader
	}{
		{"GET object", http.MethodGet,
			pathPrefixS3Buckets + "victim%2Fobjects/" + keySeg, nil},
		{"PUT object", http.MethodPut,
			pathPrefixS3Buckets + "victim%2Fobjects/" + keySeg, bytes.NewReader([]byte("hostile"))},
		{"DELETE object", http.MethodDelete,
			pathPrefixS3Buckets + "victim%2Fobjects/" + keySeg, nil},
		{"GET list", http.MethodGet,
			pathPrefixS3Buckets + "victim%2Fobjects", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.url, tc.body)
			switch tc.method {
			case http.MethodGet:
				req = withReadOnlyPrincipal(req)
			default:
				req = withWritePrincipal(req)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			// Reject must happen before the source is reached — any
			// 200 / 204 here would mean the request executed against
			// the wrong bucket (confused-deputy class).
			require.NotContains(t, []int{http.StatusOK, http.StatusNoContent}, rec.Code,
				"encoded slash in bucket segment must be rejected; got %d, body=%s",
				rec.Code, rec.Body.String())
		})
	}
}

func TestS3Objects_RoutingMethodNotAllowed(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"collection POST", http.MethodPost, objectsCollectionPath("photos")},
		{"object POST", http.MethodPost, objectPath("photos", "k1")},
		{"object PATCH", http.MethodPatch, objectPath("photos", "k1")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, nil)
			req = withWritePrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

// ---------- key segment decoding ----------

func TestS3Objects_RejectsMalformedKeySegment(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	// "!!!" is not valid base64-url-raw NOR padded; decoder rejects both.
	req := httptest.NewRequest(http.MethodGet,
		pathPrefixS3Buckets+"photos/"+adminObjectSubResource+"/!!!", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "base64-url")
}

// The empty-decoded-key path inside decodeAdminObjectKeySegment is
// defensive: a single non-empty URL segment can never decode to
// zero bytes via base64-url (the smallest valid segment "AA"
// decodes to a single 0x00 byte). The branch exists so a future
// refactor that loosens the routing layer (e.g. accepts a
// zero-length segment via the splitter) fails closed rather than
// dispatching to AdminGetObject with an empty key. Tested
// indirectly via the routing-rejection coverage above.

// ---------- list ----------

func TestS3Objects_List_ReturnsObjects(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	src.seedObject("photos", "a.png", []byte("a"))
	src.seedObject("photos", "b.png", []byte("bb"))
	h := newS3HandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet, objectsCollectionPath("photos"), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var got AdminObjectListing
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Objects, 2)
	require.Equal(t, "a.png", got.Objects[0].Key)
	require.Equal(t, int64(1), got.Objects[0].Size)
	require.Equal(t, "b.png", got.Objects[1].Key)
	require.Equal(t, int64(2), got.Objects[1].Size)
}

func TestS3Objects_List_EmptyListSerializesAsEmptyArray(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	req := httptest.NewRequest(http.MethodGet, objectsCollectionPath("photos"), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	// SPA distinguishes "no bucket" from "empty bucket" — empty
	// must serialize as `"objects":[]`, not `"objects":null`.
	require.Contains(t, rec.Body.String(), `"objects":[]`)
}

func TestS3Objects_List_MaxKeysClamped(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	for i := range 5 {
		src.seedObject("photos", string('a'+rune(i))+".png", []byte("x"))
	}
	h := newS3HandlerForTest(src)

	// Limit 2 → page has 2 entries plus next_continuation_token.
	req := httptest.NewRequest(http.MethodGet, objectsCollectionPath("photos")+"?max_keys=2", nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var got AdminObjectListing
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got.Objects, 2)
	require.NotEmpty(t, got.NextContinuationToken)
}

func TestS3Objects_List_RejectsBadMaxKeys(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	for _, raw := range []string{"abc", "-1", "0"} {
		t.Run(raw, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet,
				objectsCollectionPath("photos")+"?max_keys="+raw, nil)
			req = withReadOnlyPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------- get ----------

func TestS3Objects_Get_StreamsBodyAndSetsHeaders(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	src.seedObject("photos", "logo.png", []byte("png-bytes-here"))
	h := newS3HandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet, objectPath("photos", "logo.png"), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "png-bytes-here", rec.Body.String())
	// Security headers — defence-in-depth against a hostile upload
	// rendering in the admin origin.
	require.Equal(t, "nosniff", rec.Header().Get("X-Content-Type-Options"))
	require.Contains(t, rec.Header().Get("Content-Security-Policy"), "sandbox")
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.Contains(t, rec.Header().Get("Content-Disposition"), "attachment")
	require.Contains(t, rec.Header().Get("Content-Disposition"), `filename="logo.png"`)
	require.Equal(t, "14", rec.Header().Get("Content-Length"))
	require.Equal(t, `"deadbeef"`, rec.Header().Get("ETag"))
}

func TestS3Objects_Get_SanitisesContentDispositionFilename(t *testing.T) {
	t.Parallel()
	// Key with control characters / quote / backslash that must be
	// substituted before being interpolated into the quoted-string
	// form of Content-Disposition.
	src := newStubObjectsSource()
	src.seedObject("photos", "weird\"name\\.txt", []byte("x"))
	h := newS3HandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet, objectPath("photos", "weird\"name\\.txt"), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	cd := rec.Header().Get("Content-Disposition")
	require.NotContains(t, cd, `"name`, "quote inside filename must be substituted")
	require.NotContains(t, cd, `\\`, "backslash inside filename must be substituted")
	require.Contains(t, cd, `filename="`)
}

func TestS3Objects_Get_MissingObject404(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	src.seedObject("photos", "k1", []byte("x")) // ensure bucket exists
	h := newS3HandlerForTest(src)

	req := httptest.NewRequest(http.MethodGet, objectPath("photos", "missing"), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Objects_Get_MissingBucket404(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	req := httptest.NewRequest(http.MethodGet, objectPath("ghost", "k1"), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code)
}

// ---------- put ----------

func TestS3Objects_Put_RoundTripsBytes(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	h := newS3HandlerForTest(src)

	payload := []byte("payload-bytes-here")
	req := httptest.NewRequest(http.MethodPut, objectPath("photos", "new.png"),
		bytes.NewReader(payload))
	req.Header.Set("Content-Type", "image/png")
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "AKIA_FULL", src.lastPutPrincipal.AccessKey)
	require.Equal(t, "photos", src.lastPutBucket)
	require.Equal(t, "new.png", src.lastPutKey)
	require.Equal(t, "image/png", src.lastPutCT)
	require.Equal(t, payload, src.lastPutBody)
}

func TestS3Objects_Put_Rejects413OnOversizedBody(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	h := newS3HandlerForTest(src)

	// http.MaxBytesReader's *limit* is exclusive — a body whose
	// declared length exceeds the cap (or whose actual stream
	// overflows) trips on the first read past the cap. Use a
	// lazy infiniteX reader (capped via io.LimitReader at cap+1)
	// rather than a 100 MiB bytes.Repeat allocation — keeps the
	// test memory footprint under a kilobyte (Claude review on
	// PR #814 r1).
	oversize := io.LimitReader(&infiniteX{}, int64(adminObjectUploadCap)+1)
	req := httptest.NewRequest(http.MethodPut, objectPath("photos", "huge.bin"),
		oversize)
	// httptest.NewRequest with an io.Reader sets ContentLength=-1
	// so MaxBytesReader can't short-circuit on Content-Length; it
	// has to actually read past the cap. That's what we want here:
	// it exercises the read-side overflow path that production
	// uploads hit when ContentLength is absent or wrong.
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

// infiniteX is a zero-state io.Reader that yields 'x' indefinitely;
// callers cap it via io.LimitReader. Used by the oversize-PUT test
// to avoid allocating the full 100 MiB body up front.
type infiniteX struct{}

func (infiniteX) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

func TestS3Objects_Put_TranslatesUploadTooLargeSentinel(t *testing.T) {
	t.Parallel()
	// Adapter-side ErrObjectsUploadTooLarge surfaces from the bridge
	// regardless of the http.MaxBytesReader check (e.g. a manifest-
	// rejection at commit time). The handler must surface 413 either
	// way so the SPA's retry contract stays uniform.
	src := newStubObjectsSource()
	src.nextErr = ErrObjectsUploadTooLarge
	h := newS3HandlerForTest(src)

	req := httptest.NewRequest(http.MethodPut, objectPath("photos", "k1"),
		bytes.NewReader([]byte("small")))
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code)
}

// ---------- delete ----------

func TestS3Objects_Delete_HappyPath(t *testing.T) {
	t.Parallel()
	src := newStubObjectsSource()
	src.seedObject("photos", "k1", []byte("x"))
	h := newS3HandlerForTest(src)

	req := httptest.NewRequest(http.MethodDelete, objectPath("photos", "k1"), nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.Equal(t, "AKIA_FULL", src.lastDeletePrincipal.AccessKey)
	require.Equal(t, "photos", src.lastDeleteBucket)
	require.Equal(t, "k1", src.lastDeleteKey)
}

func TestS3Objects_Delete_IsIdempotent(t *testing.T) {
	t.Parallel()
	// A missing object surfaces as success (mirrors S3 SigV4
	// DeleteObject contract). The stub's AdminDeleteObject just
	// no-ops on a missing key.
	src := newStubObjectsSource()
	src.seedObject("photos", "k1", []byte("x")) // ensure bucket exists
	h := newS3HandlerForTest(src)

	req := httptest.NewRequest(http.MethodDelete, objectPath("photos", "ghost"), nil)
	req = withWritePrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
}

// ---------- authorisation ----------

func TestS3Objects_Put_RejectsReadOnly(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	req := httptest.NewRequest(http.MethodPut, objectPath("photos", "k1"),
		bytes.NewReader([]byte("body")))
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestS3Objects_Delete_RejectsReadOnly(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	req := httptest.NewRequest(http.MethodDelete, objectPath("photos", "k1"), nil)
	req = withReadOnlyPrincipal(req)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestS3Objects_List_RejectsAnonymous(t *testing.T) {
	t.Parallel()
	h := newS3HandlerForTest(newStubObjectsSource())

	// No principal injected — should fail with 401 unauthenticated.
	req := httptest.NewRequest(http.MethodGet, objectsCollectionPath("photos"), nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

// ---------- error translation ----------

func TestS3Objects_TranslatesSentinelsToStatus(t *testing.T) {
	t.Parallel()
	cases := map[string]struct {
		injected error
		wantCode int
	}{
		"forbidden":        {ErrObjectsForbidden, http.StatusForbidden},
		"not-leader":       {ErrObjectsNotLeader, http.StatusServiceUnavailable},
		"bucket-not-found": {ErrObjectsBucketNotFound, http.StatusNotFound},
		"object-not-found": {ErrObjectsNotFound, http.StatusNotFound},
		"validation":       {ErrObjectsValidation, http.StatusBadRequest},
		"upload-too-large": {ErrObjectsUploadTooLarge, http.StatusRequestEntityTooLarge},
		"internal": {errors.New("storage backing failure"),
			http.StatusInternalServerError},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			src := newStubObjectsSource()
			src.nextErr = tc.injected
			h := newS3HandlerForTest(src)

			req := httptest.NewRequest(http.MethodGet,
				objectsCollectionPath("photos"), nil)
			req = withReadOnlyPrincipal(req)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			require.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// ---------- objectKeyBasename ----------

func TestObjectKeyBasename(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":             "object",
		"plain":        "plain",
		"a/b/c.png":    "c.png",
		"trailing/":    "object",
		"//":           "object",
		"deep/nested/": "object",
	}
	for in, want := range cases {
		t.Run(in, func(t *testing.T) {
			require.Equal(t, want, objectKeyBasename(in))
		})
	}
}
