package adapter

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Admin uploads share the node's PUT memory budget with SigV4 uploads. With
// the limiter exhausted, AdminPutObject must be refused like a SigV4 PUT is,
// and nothing may be committed.
func TestS3Server_AdminPutObject_HonoursPutAdmission(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)
	createBucketForAdminTest(t, server, "admitted")
	server.putAdmission = newS3PutAdmission(s3ChunkSize, 5*time.Millisecond)
	release, err := server.putAdmission.acquire(context.Background(), s3ChunkSize)
	require.NoError(t, err)
	defer release()

	err = server.AdminPutObject(context.Background(), fullAdminBucketsPrincipal(), "admitted", "object",
		strings.NewReader("payload"), "text/plain")
	require.True(t, errors.Is(err, errS3PutAdmissionExhausted), "want admission exhaustion; got %v", err)
	require.True(t, errors.Is(err, ErrAdminUploadThrottled), "the admin bridge keys its 503 off ErrAdminUploadThrottled; got %v", err)

	_, _, err = server.AdminGetObject(context.Background(), fullAdminBucketsPrincipal(), "admitted", "object")
	require.True(t, errors.Is(err, ErrAdminObjectNotFound), "a refused upload must not commit; got %v", err)
}

// With blob offload enabled, SigV4 PUT stores chunk bodies as replicated
// chunkblobs referenced from chunkref rows. Admin uploads must take the same
// path instead of writing legacy in-Raft blob rows.
func TestS3Server_AdminPutObject_OffloadsChunks(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3BlobOffloadEnabled(true),
		WithS3BlobCluster(newFakeS3BlobCluster()),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: st}),
		withS3BlobOffloadGCReadyForTest(),
	)
	createBucketForAdminTest(t, server, "offload")

	payload := bytes.Repeat([]byte("admin-offload"), s3ChunkSize/len("admin-offload")+100)
	require.NoError(t, server.AdminPutObject(context.Background(), fullAdminBucketsPrincipal(), "offload", "object",
		bytes.NewReader(payload), "application/octet-stream"))

	readTS := server.readTS()
	legacy, err := st.ScanAt(context.Background(), []byte(s3keys.BlobPrefix), prefixScanEnd([]byte(s3keys.BlobPrefix)), 10, readTS)
	require.NoError(t, err)
	require.Empty(t, legacy, "offloaded admin uploads must not write legacy blob rows")
	refs, err := st.ScanAt(context.Background(), []byte(s3keys.ChunkRefPrefix), prefixScanEnd([]byte(s3keys.ChunkRefPrefix)), 10, readTS)
	require.NoError(t, err)
	require.Len(t, refs, 2)

	body, _, err := server.AdminGetObject(context.Background(), fullAdminBucketsPrincipal(), "offload", "object")
	require.NoError(t, err)
	defer body.Close()
	got, err := io.ReadAll(body)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
