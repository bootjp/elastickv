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

// A continuation token carries the read timestamp of the first page. Once
// MVCC retention moves past it, the bucket-metadata load at that timestamp
// fails with ErrReadTSCompacted before any scan runs. ListObjectsV2 must
// answer that as an expired token (400 InvalidArgument), as it already does
// when the scan itself hits the compaction, not as a 500.
func TestS3Server_ListObjectsV2_CompactedContinuationTokenIsInvalidArgument(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	for _, objectKey := range []string{"a.txt", "b.txt"} {
		rec = httptest.NewRecorder()
		server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/"+objectKey, strings.NewReader(objectKey)))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a?list-type=2&max-keys=1", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	firstPage := decodeListBucketResult(t, rec.Body.Bytes())
	require.NotEmpty(t, firstPage.NextContinuationToken)
	token, err := decodeS3ContinuationToken(firstPage.NextContinuationToken)
	require.NoError(t, err)

	retention, ok := st.(store.RetentionController)
	require.True(t, ok)
	retention.SetMinRetainedTS(token.ReadTS + 1)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet,
		"/bucket-a?list-type=2&max-keys=1&continuation-token="+firstPage.NextContinuationToken, nil))
	require.Equal(t, http.StatusBadRequest, rec.Code, "body=%s", rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Code>InvalidArgument</Code>")
	require.Contains(t, rec.Body.String(), "continuation token has expired")
}

// A continuation token that is not base64 JSON is a client error on both
// listing paths. SigV4 answers 400 InvalidArgument; the admin path must
// return ErrAdminInvalidContinuationToken, which the admin bridge renders as
// 400, rather than a bare decode error the handler answers with 500.
func TestS3Server_AdminListObjects_MalformedContinuationTokenIsInvalid(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "malformed")

	for _, raw := range []string{"%%%not-base64", "bm90LWpzb24"} {
		_, err := server.AdminListObjects(context.Background(), fullAdminBucketsPrincipal(), "malformed",
			AdminListObjectsOptions{ContinuationToken: raw})
		require.True(t, errors.Is(err, ErrAdminInvalidContinuationToken),
			"token %q: want ErrAdminInvalidContinuationToken; got %v", raw, err)
	}
}
