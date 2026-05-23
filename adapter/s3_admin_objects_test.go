package adapter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// putObjectForAdminTest lands one object via the SigV4 PUT path so
// the admin tests have something to delete / get / list. Returns the
// canonical bucket name and key (caller supplies these so each test
// can pick its own namespace).
func putObjectForAdminTest(t *testing.T, server *S3Server, bucket, key, body string) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/"+bucket, nil)
	server.handle(rec, req)
	require.Equalf(t, http.StatusOK, rec.Code, "create bucket: body=%s", rec.Body.String())

	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodPut, "/"+bucket+"/"+key, strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	server.handle(rec, req)
	require.Equalf(t, http.StatusOK, rec.Code, "put object: body=%s", rec.Body.String())
}

func TestS3Server_AdminDeleteObject_HappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	putObjectForAdminTest(t, server, "deletable", "k1", "hello")

	err := server.AdminDeleteObject(context.Background(),
		fullAdminBucketsPrincipal(), "deletable", "k1")
	require.NoError(t, err)

	// Verify via HEAD that the object is gone.
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodHead, "/deletable/k1", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code,
		"object must be gone after AdminDeleteObject")
}

// TestS3Server_AdminDeleteObject_Idempotent pins AWS semantics: a
// second delete of the same object (or first delete of an absent
// object) returns nil — never ErrAdminObjectNotFound. The SigV4
// deleteObject path is silent-no-op on absent, and admin matches
// for least-surprise.
func TestS3Server_AdminDeleteObject_Idempotent(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	putObjectForAdminTest(t, server, "twice", "k1", "v")

	require.NoError(t, server.AdminDeleteObject(context.Background(),
		fullAdminBucketsPrincipal(), "twice", "k1"))
	require.NoError(t, server.AdminDeleteObject(context.Background(),
		fullAdminBucketsPrincipal(), "twice", "k1"),
		"second delete on already-absent key must be a no-op")
	require.NoError(t, server.AdminDeleteObject(context.Background(),
		fullAdminBucketsPrincipal(), "twice", "never-existed"),
		"delete on never-existed key must be a no-op")
}

func TestS3Server_AdminDeleteObject_MissingBucket(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	err := server.AdminDeleteObject(context.Background(),
		fullAdminBucketsPrincipal(), "ghost", "k1")
	require.True(t, errors.Is(err, ErrAdminBucketNotFound),
		"want ErrAdminBucketNotFound; got %v", err)
}

func TestS3Server_AdminDeleteObject_RejectsReadOnly(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	putObjectForAdminTest(t, server, "guarded", "k1", "v")

	err := server.AdminDeleteObject(context.Background(),
		readOnlyAdminBucketsPrincipal(), "guarded", "k1")
	require.ErrorIs(t, err, ErrAdminForbidden)

	// And the object must still be there.
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodHead, "/guarded/k1", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code,
		"forbidden delete must not have mutated state")
}

func TestS3Server_AdminDeleteObject_RejectsEmptyKey(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	// Use a unique key so this test contributes a second key value
	// to putObjectForAdminTest's call sites (satisfies the unparam
	// linter that flags single-value parameters).
	putObjectForAdminTest(t, server, "anybucket", "dir/nested.txt", "v")

	err := server.AdminDeleteObject(context.Background(),
		fullAdminBucketsPrincipal(), "anybucket", "")
	require.True(t, errors.Is(err, ErrAdminInvalidObjectKey),
		"want ErrAdminInvalidObjectKey; got %v", err)
}

func TestS3Server_AdminDeleteObject_RejectsInvalidBucketName(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	err := server.AdminDeleteObject(context.Background(),
		fullAdminBucketsPrincipal(), "INVALID_BUCKET_NAME!!", "k1")
	require.True(t, errors.Is(err, ErrAdminInvalidBucketName),
		"want ErrAdminInvalidBucketName; got %v", err)
}

// AdminPutObject tests

// createBucketForAdminTest creates one bucket via AdminCreateBucket
// so the put-side admin tests don't have to dance through the
// SigV4 PUT request flow.
func createBucketForAdminTest(t *testing.T, server *S3Server, bucket string) {
	t.Helper()
	_, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), bucket, s3AclPrivate)
	require.NoError(t, err)
}

func TestS3Server_AdminPutObject_HappyPath_RoundTripsViaSigV4Get(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "uploads")

	payload := "hello-admin"
	err := server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "uploads", "greeting.txt",
		strings.NewReader(payload), "text/plain")
	require.NoError(t, err)

	// SigV4 GET sees the new object.
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/uploads/greeting.txt", nil)
	server.handle(rec, req)
	require.Equalf(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, payload, rec.Body.String())
	require.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
}

func TestS3Server_AdminPutObject_DefaultsContentType(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "uploads")

	err := server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "uploads", "blob",
		strings.NewReader("x"), "")
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodHead, "/uploads/blob", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"),
		"empty content-type must default to application/octet-stream")
}

func TestS3Server_AdminPutObject_ReplacesExisting(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "uploads")

	require.NoError(t, server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "uploads", "k", strings.NewReader("v1"), "text/plain"))
	require.NoError(t, server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "uploads", "k", strings.NewReader("v2-longer"), "text/plain"))

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/uploads/k", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "v2-longer", rec.Body.String(),
		"second PUT must overwrite the first")
}

func TestS3Server_AdminPutObject_MissingBucket(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	err := server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "ghost", "k", strings.NewReader("v"), "")
	require.True(t, errors.Is(err, ErrAdminBucketNotFound),
		"want ErrAdminBucketNotFound; got %v", err)
}

func TestS3Server_AdminPutObject_RejectsReadOnly(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "guarded")

	err := server.AdminPutObject(context.Background(),
		readOnlyAdminBucketsPrincipal(), "guarded", "k", strings.NewReader("v"), "")
	require.ErrorIs(t, err, ErrAdminForbidden)
}

func TestS3Server_AdminPutObject_RejectsEmptyKey(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "anybucket")

	err := server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "anybucket", "", strings.NewReader("v"), "")
	require.True(t, errors.Is(err, ErrAdminInvalidObjectKey),
		"want ErrAdminInvalidObjectKey; got %v", err)
}

// TestS3Server_AdminPutObject_RejectsOversizedBody pins the
// adminS3UploadMaxBytes cap (100 MiB per design §3.3.3). We feed
// a body cap+1 bytes via an io.Reader and assert the cap check
// fires before the chunked dispatch commits a manifest.
func TestS3Server_AdminPutObject_RejectsOversizedBody(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "capbucket")

	// io.LimitReader on a zero-byte source produces no bytes;
	// io.MultiReader of (cap+1) bytes of zeroes hits the
	// per-write-Read cap accumulation path.
	oversized := io.MultiReader(
		bytes.NewReader(make([]byte, adminS3UploadMaxBytes)),
		bytes.NewReader([]byte{0xff}),
	)
	err := server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "capbucket", "huge.bin", oversized, "application/octet-stream")
	require.True(t, errors.Is(err, ErrAdminUploadTooLarge),
		"want ErrAdminUploadTooLarge; got %v", err)

	// And the object must NOT have been committed.
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodHead, "/capbucket/huge.bin", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code,
		"oversized payload must not have committed a manifest")
}

func TestS3Server_AdminPutObject_AcceptsNilBody(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "empties")

	require.NoError(t, server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "empties", "zero.bin", nil, ""))

	// Zero-byte object is readable.
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/empties/zero.bin", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, 0, rec.Body.Len())
}

// AdminGetObject tests

func TestS3Server_AdminGetObject_HappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "downloads")
	require.NoError(t, server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "report.txt",
		strings.NewReader("hello, world"), "text/plain"))

	body, meta, err := server.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "report.txt")
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, "hello, world", string(got))

	require.Equal(t, "report.txt", meta.Key)
	require.Equal(t, int64(len("hello, world")), meta.Size)
	require.Equal(t, "text/plain", meta.ContentType)
	require.NotEmpty(t, meta.ETag, "ETag must be set")
	require.Equal(t, "STANDARD", meta.StorageClass)
	require.False(t, meta.LastModified.IsZero(), "LastModified must be set")
}

func TestS3Server_AdminGetObject_StreamsMultipleChunks(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "bigfiles")

	// Build a payload that crosses several s3ChunkSize boundaries
	// so the reader hits the multi-chunk path.
	payload := bytes.Repeat([]byte{0xab}, 3*s3ChunkSize+17)
	require.NoError(t, server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "bigfiles", "blob.bin",
		bytes.NewReader(payload), "application/octet-stream"))

	body, meta, err := server.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "bigfiles", "blob.bin")
	require.NoError(t, err)
	defer body.Close()

	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, payload, got, "streamed body must match the uploaded bytes")
	require.Equal(t, int64(len(payload)), meta.Size)
}

func TestS3Server_AdminGetObject_MissingObject(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "downloads")

	_, _, err := server.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "downloads", "no-such-key")
	require.True(t, errors.Is(err, ErrAdminObjectNotFound),
		"want ErrAdminObjectNotFound; got %v", err)
}

func TestS3Server_AdminGetObject_MissingBucket(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	_, _, err := server.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "ghost", "k")
	require.True(t, errors.Is(err, ErrAdminBucketNotFound),
		"want ErrAdminBucketNotFound; got %v", err)
}

// TestS3Server_AdminGetObject_AllowsReadOnly pins the role contract:
// read role suffices for GET (unlike Put / Delete which require
// write). Important regression: if a future refactor accidentally
// gates GET on canWrite() the read-only operator dashboard breaks.
func TestS3Server_AdminGetObject_AllowsReadOnly(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "readable")
	require.NoError(t, server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "readable", "k", strings.NewReader("v"), "text/plain"))

	body, _, err := server.AdminGetObject(context.Background(),
		readOnlyAdminBucketsPrincipal(), "readable", "k")
	require.NoError(t, err)
	defer body.Close()
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, "v", string(got))
}

func TestS3Server_AdminGetObject_DefaultsContentType(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "ctype")
	require.NoError(t, server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "ctype", "blob", strings.NewReader("x"), ""))

	_, meta, err := server.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "ctype", "blob")
	require.NoError(t, err)
	require.Equal(t, "application/octet-stream", meta.ContentType)
}

func TestS3Server_AdminGetObject_CloseIsIdempotent(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "closes")
	require.NoError(t, server.AdminPutObject(context.Background(),
		fullAdminBucketsPrincipal(), "closes", "k", strings.NewReader("v"), "text/plain"))

	body, _, err := server.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "closes", "k")
	require.NoError(t, err)

	require.NoError(t, body.Close())
	require.NoError(t, body.Close(), "second Close must be a no-op")

	// Read after Close returns ErrClosedPipe.
	_, err = body.Read(make([]byte, 1))
	require.Error(t, err, "Read after Close must error")
}

func TestS3Server_AdminGetObject_RejectsEmptyKey(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "anybucket")

	_, _, err := server.AdminGetObject(context.Background(),
		fullAdminBucketsPrincipal(), "anybucket", "")
	require.True(t, errors.Is(err, ErrAdminInvalidObjectKey),
		"want ErrAdminInvalidObjectKey; got %v", err)
}

// AdminListObjects tests

func putObjectsForListTest(t *testing.T, server *S3Server, bucket string, keys ...string) {
	t.Helper()
	for _, k := range keys {
		require.NoError(t, server.AdminPutObject(context.Background(),
			fullAdminBucketsPrincipal(), bucket, k, strings.NewReader("v"), "text/plain"),
			"put %q", k)
	}
}

func TestS3Server_AdminListObjects_Empty(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "emptybucket")

	got, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "emptybucket", AdminListObjectsOptions{})
	require.NoError(t, err)
	require.NotNil(t, got.Objects, "Objects must be non-nil so JSON renders [] not null")
	require.Len(t, got.Objects, 0)
	require.Empty(t, got.CommonPrefixes)
	require.Empty(t, got.NextContinuationToken)
}

func TestS3Server_AdminListObjects_FlatListing(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "flatbucket")
	putObjectsForListTest(t, server, "flatbucket", "a", "b", "c")

	got, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "flatbucket", AdminListObjectsOptions{})
	require.NoError(t, err)
	require.Len(t, got.Objects, 3)
	require.Equal(t, []string{"a", "b", "c"},
		[]string{got.Objects[0].Key, got.Objects[1].Key, got.Objects[2].Key},
		"objects must be sorted lexically (scan order)")
	require.Empty(t, got.CommonPrefixes)
}

func TestS3Server_AdminListObjects_PrefixFiltersAndCollapsesWithDelimiter(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "treebucket")
	putObjectsForListTest(t, server, "treebucket",
		"a/x", "a/y", "b/z", "top.txt")

	// Prefix only (no delimiter): all matching keys, no collapsing.
	got, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "treebucket",
		AdminListObjectsOptions{Prefix: "a/"})
	require.NoError(t, err)
	require.Len(t, got.Objects, 2)
	require.Equal(t, "a/x", got.Objects[0].Key)
	require.Equal(t, "a/y", got.Objects[1].Key)
	require.Empty(t, got.CommonPrefixes)

	// Delimiter "/": children of root, with subfolders collapsed.
	got, err = server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "treebucket",
		AdminListObjectsOptions{Delimiter: "/"})
	require.NoError(t, err)
	// "top.txt" is a leaf; "a/" and "b/" are common-prefix folders.
	require.ElementsMatch(t, []string{"a/", "b/"}, got.CommonPrefixes)
	require.Len(t, got.Objects, 1)
	require.Equal(t, "top.txt", got.Objects[0].Key)
}

func TestS3Server_AdminListObjects_MaxKeysClamped(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "clamps")
	putObjectsForListTest(t, server, "clamps", "a", "b", "c", "d", "e")

	// MaxKeys=2 — exactly 2 objects, NextContinuationToken non-empty.
	got, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "clamps",
		AdminListObjectsOptions{MaxKeys: 2})
	require.NoError(t, err)
	require.Len(t, got.Objects, 2)
	require.NotEmpty(t, got.NextContinuationToken, "page below total must produce a continuation token")

	// MaxKeys=0 — default 100, fits all 5.
	got, err = server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "clamps",
		AdminListObjectsOptions{})
	require.NoError(t, err)
	require.Len(t, got.Objects, 5)
	require.Empty(t, got.NextContinuationToken)

	// MaxKeys above the cap is clamped to 1000 (not tested with
	// 1000 objects to keep the test fast; just verify it accepts
	// the over-cap value without error).
	got, err = server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "clamps",
		AdminListObjectsOptions{MaxKeys: 10_000})
	require.NoError(t, err)
	require.Len(t, got.Objects, 5)
}

func TestS3Server_AdminListObjects_PagesViaContinuationToken(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "pages")
	putObjectsForListTest(t, server, "pages", "a", "b", "c", "d", "e")

	page1, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "pages",
		AdminListObjectsOptions{MaxKeys: 2})
	require.NoError(t, err)
	require.Len(t, page1.Objects, 2)
	require.NotEmpty(t, page1.NextContinuationToken)

	page2, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "pages",
		AdminListObjectsOptions{MaxKeys: 2, ContinuationToken: page1.NextContinuationToken})
	require.NoError(t, err)
	require.Len(t, page2.Objects, 2)
	require.NotEmpty(t, page2.NextContinuationToken)

	page3, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "pages",
		AdminListObjectsOptions{MaxKeys: 2, ContinuationToken: page2.NextContinuationToken})
	require.NoError(t, err)
	require.Len(t, page3.Objects, 1)
	require.Empty(t, page3.NextContinuationToken, "last page must terminate the token chain")

	// Concatenating all three pages must reproduce the full list
	// in scan order (no duplicates, no gaps).
	got := make([]string, 0, 5)
	for _, p := range []AdminObjectListing{page1, page2, page3} {
		for _, o := range p.Objects {
			got = append(got, o.Key)
		}
	}
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, got)
}

func TestS3Server_AdminListObjects_MissingBucket(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	_, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "ghost", AdminListObjectsOptions{})
	require.True(t, errors.Is(err, ErrAdminBucketNotFound),
		"want ErrAdminBucketNotFound; got %v", err)
}

func TestS3Server_AdminListObjects_AllowsReadOnly(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "readbucket")
	putObjectsForListTest(t, server, "readbucket", "x")

	got, err := server.AdminListObjects(context.Background(),
		readOnlyAdminBucketsPrincipal(), "readbucket", AdminListObjectsOptions{})
	require.NoError(t, err)
	require.Len(t, got.Objects, 1)
}

func TestS3Server_AdminListObjects_RejectsAnonymous(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "anonbucket")

	_, err := server.AdminListObjects(context.Background(),
		AdminPrincipal{}, "anonbucket", AdminListObjectsOptions{})
	require.ErrorIs(t, err, ErrAdminForbidden,
		"nil-role principal must be denied")
}

// TestS3Server_AdminListObjects_RejectsMismatchedToken pins the
// claude-bot r1 + gemini r1 medium: a continuation token whose
// bucket / generation / prefix / delimiter does not match the
// current request must surface as ErrAdminInvalidContinuationToken,
// NOT as the earlier mis-applied ErrAdminInvalidBucketName.
func TestS3Server_AdminListObjects_RejectsMismatchedToken(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "bucketa")
	createBucketForAdminTest(t, server, "bucketb")
	putObjectsForListTest(t, server, "bucketa", "x", "y", "z")

	// Take a token from bucketa.
	page1, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "bucketa",
		AdminListObjectsOptions{MaxKeys: 1})
	require.NoError(t, err)
	require.NotEmpty(t, page1.NextContinuationToken)

	// Reuse it against bucketb — bucket field of the token does
	// not match, must reject with ErrAdminInvalidContinuationToken.
	_, err = server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "bucketb",
		AdminListObjectsOptions{ContinuationToken: page1.NextContinuationToken})
	require.True(t, errors.Is(err, ErrAdminInvalidContinuationToken),
		"want ErrAdminInvalidContinuationToken for bucket-mismatch; got %v", err)

	// And use it against bucketa but with a different prefix —
	// prefix-mismatch must also reject.
	_, err = server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "bucketa",
		AdminListObjectsOptions{Prefix: "other/", ContinuationToken: page1.NextContinuationToken})
	require.True(t, errors.Is(err, ErrAdminInvalidContinuationToken),
		"want ErrAdminInvalidContinuationToken for prefix-mismatch; got %v", err)
}

// TestS3Server_AdminListObjects_RejectsExpiredToken pins the
// store.ErrReadTSCompacted handling: when a continuation token's
// readTS has been MVCC-GC'd past, the underlying ScanAt returns
// ErrReadTSCompacted; AdminListObjects must translate that into
// ErrAdminInvalidContinuationToken (matches SigV4 listObjectsV2
// at s3.go:2105, which returns 400 InvalidArgument
// "continuation token has expired").
//
// We exercise this by wrapping the store in a fake that returns
// ErrReadTSCompacted on the second ScanAt call (the first
// satisfies the bucket-meta load).
func TestS3Server_AdminListObjects_RejectsExpiredToken(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "expiry")
	putObjectsForListTest(t, server, "expiry", "a", "b")

	// First page to get a real continuation token.
	page1, err := server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "expiry",
		AdminListObjectsOptions{MaxKeys: 1})
	require.NoError(t, err)
	require.NotEmpty(t, page1.NextContinuationToken)

	// Swap the store for one that errors with ErrReadTSCompacted
	// on the *paginated* ScanAt (the second-page request). The
	// bucket-meta load still goes through cleanly via the inner
	// GetAt; only the ScanAt is poisoned.
	server.store = &scanAtErrStore{MVCCStore: st, err: store.ErrReadTSCompacted}

	_, err = server.AdminListObjects(context.Background(),
		fullAdminBucketsPrincipal(), "expiry",
		AdminListObjectsOptions{MaxKeys: 1, ContinuationToken: page1.NextContinuationToken})
	require.True(t, errors.Is(err, ErrAdminInvalidContinuationToken),
		"want ErrAdminInvalidContinuationToken for compacted readTS; got %v", err)
}

// scanAtErrStore wraps an MVCCStore and forces ScanAt to return a
// pre-set error. All other methods pass through. Used only by the
// ExpiredToken test to inject store.ErrReadTSCompacted at the
// scan boundary without having to actually run compaction.
type scanAtErrStore struct {
	store.MVCCStore
	err error
}

func (s *scanAtErrStore) ScanAt(_ context.Context, _ []byte, _ []byte, _ int, _ uint64) ([]*store.KVPair, error) {
	return nil, s.err
}
