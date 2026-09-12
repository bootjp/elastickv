package adapter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// Every case here uses one bucket and one object key; only the ETag and the
// presence of the header vary.
const (
	conditionalDeleteBucket = "retention"
	conditionalDeleteKey    = "payload.bin"
)

// putS3TestObject stores body at the test bucket/key through the real handler
// and returns the ETag the server assigned.
func putS3TestObject(t *testing.T, server *S3Server, body string) string {
	t.Helper()

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut,
		"/"+conditionalDeleteBucket+"/"+conditionalDeleteKey, strings.NewReader(body))
	req.Header.Set("Content-Type", "text/plain")
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "PUT %s/%s", conditionalDeleteBucket, conditionalDeleteKey)

	etag := strings.Trim(rec.Header().Get("ETag"), `"`)
	require.NotEmpty(t, etag, "the server must assign an ETag")
	return etag
}

func newConditionalDeleteS3Server(t *testing.T) *S3Server {
	t.Helper()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	_, err := server.AdminCreateBucket(context.Background(),
		fullAdminBucketsPrincipal(), conditionalDeleteBucket, s3AclPrivate)
	require.NoError(t, err)
	return server
}

func deleteS3TestObject(t *testing.T, server *S3Server, key, ifMatch string) *httptest.ResponseRecorder {
	t.Helper()

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodDelete, "/"+conditionalDeleteBucket+"/"+key, nil)
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	server.handle(rec, req)
	return rec
}

func s3TestObjectExists(t *testing.T, server *S3Server, key string) bool {
	t.Helper()

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodHead, "/"+conditionalDeleteBucket+"/"+key, nil))
	return rec.Code == http.StatusOK
}

// TestS3ConditionalDeleteRefusesAStaleETag is the load-bearing case.
//
// The handler loaded the manifest -- so it had the current ETag -- and then
// dispatched kv.Del without looking at If-Match. A caller that read an object,
// decided it was reclaimable, and sent a conditional delete believed the
// condition protected it from a concurrent rewrite. It did not: the object was
// removed whatever its ETag, so the rewrite was silently lost. Snapshot-offload
// retention relies on exactly this precondition and maps the 412 to
// ErrObjectModified.
func TestS3ConditionalDeleteRefusesAStaleETag(t *testing.T) {
	t.Parallel()

	server := newConditionalDeleteS3Server(t)
	staleETag := putS3TestObject(t, server, "first")

	// A concurrent rewrite changes the ETag after the caller observed it.
	newETag := putS3TestObject(t, server, "rewritten")
	require.NotEqual(t, staleETag, newETag, "the rewrite must change the ETag")

	rec := deleteS3TestObject(t, server, conditionalDeleteKey, staleETag)
	require.Equal(t, http.StatusPreconditionFailed, rec.Code,
		"a delete conditioned on a superseded ETag must fail with 412")
	require.Contains(t, rec.Body.String(), "PreconditionFailed")
	require.True(t, s3TestObjectExists(t, server, conditionalDeleteKey),
		"the rewritten object must survive a refused conditional delete")
}

// A matching ETag must still delete, or retention could never reclaim anything.
func TestS3ConditionalDeleteAcceptsAMatchingETag(t *testing.T) {
	t.Parallel()

	server := newConditionalDeleteS3Server(t)
	etag := putS3TestObject(t, server, "first")

	rec := deleteS3TestObject(t, server, conditionalDeleteKey, etag)
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.False(t, s3TestObjectExists(t, server, conditionalDeleteKey))
}

// Quoted ETags are the wire form AWS uses, so both spellings must match.
func TestS3ConditionalDeleteAcceptsAQuotedETag(t *testing.T) {
	t.Parallel()

	server := newConditionalDeleteS3Server(t)
	etag := putS3TestObject(t, server, "first")

	rec := deleteS3TestObject(t, server, conditionalDeleteKey, `"`+etag+`"`)
	require.Equal(t, http.StatusNoContent, rec.Code)
}

// An unconditional delete must be unaffected: adding the header check must not
// change the behaviour of every existing caller that does not send one.
func TestS3UnconditionalDeleteIsUnchanged(t *testing.T) {
	t.Parallel()

	server := newConditionalDeleteS3Server(t)
	putS3TestObject(t, server, "first")

	rec := deleteS3TestObject(t, server, conditionalDeleteKey, "")
	require.Equal(t, http.StatusNoContent, rec.Code)
	require.False(t, s3TestObjectExists(t, server, conditionalDeleteKey))
}

// Delete stays idempotent for an absent key, with or without the header: S3
// delete is not an error on a missing object, and a precondition cannot be
// evaluated against something that does not exist.
func TestS3ConditionalDeleteOnAnAbsentKeyStaysIdempotent(t *testing.T) {
	t.Parallel()

	server := newConditionalDeleteS3Server(t)

	rec := deleteS3TestObject(t, server, "never-existed", "")
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = deleteS3TestObject(t, server, "never-existed", "deadbeef")
	require.Equal(t, http.StatusNoContent, rec.Code)
}

// TestValidateS3DeletePreconditions covers the predicate directly, including
// that If-None-Match is deliberately NOT honoured on a delete.
func TestValidateS3DeletePreconditions(t *testing.T) {
	t.Parallel()

	previous := &s3ObjectManifest{ETag: "abc123"}

	t.Run("no header passes", func(t *testing.T) {
		t.Parallel()
		req := newS3TestRequest(http.MethodDelete, "/b/k", nil)
		require.NoError(t, validateS3DeletePreconditions(req, previous))
	})

	t.Run("matching etag passes", func(t *testing.T) {
		t.Parallel()
		req := newS3TestRequest(http.MethodDelete, "/b/k", nil)
		req.Header.Set("If-Match", "abc123")
		require.NoError(t, validateS3DeletePreconditions(req, previous))
	})

	t.Run("stale etag fails", func(t *testing.T) {
		t.Parallel()
		req := newS3TestRequest(http.MethodDelete, "/b/k", nil)
		req.Header.Set("If-Match", "stale")
		require.Error(t, validateS3DeletePreconditions(req, previous))
	})

	t.Run("If-None-Match is not honoured on delete", func(t *testing.T) {
		t.Parallel()
		// "only if absent" is meaningless for an operation whose purpose is
		// to remove something that exists; honouring it would refuse every
		// delete of a present object.
		req := newS3TestRequest(http.MethodDelete, "/b/k", nil)
		req.Header.Set("If-None-Match", "*")
		require.NoError(t, validateS3DeletePreconditions(req, previous))
	})

	t.Run("nil manifest passes", func(t *testing.T) {
		t.Parallel()
		req := newS3TestRequest(http.MethodDelete, "/b/k", nil)
		req.Header.Set("If-Match", "abc123")
		require.NoError(t, validateS3DeletePreconditions(req, nil))
	})
}
