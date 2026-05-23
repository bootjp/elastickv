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
