package adapter

import (
	"context"
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
