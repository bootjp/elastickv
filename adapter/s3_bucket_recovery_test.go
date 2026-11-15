package adapter

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3BucketCleanupRecoveryAfterMetadataDeletion(t *testing.T) {
	h := newS3OCCReproModeHarness(t, false, true)
	require.Equal(t, http.StatusOK, h.do(http.MethodPut, "/bucket", "").Code)
	chunkRefKey, sha := seedS3CleanupChunkRef(t, h)
	readTimestamp, err := h.server.beginTxnReadTimestamp(t.Context(), "bucket cleanup recovery test")
	require.NoError(t, err)
	require.NoError(t, h.server.dispatchBucketDeleteTxn(t.Context(), readTimestamp, "bucket", 1))
	marker := s3keys.BucketCleanupKey("bucket", 1)
	_, err = h.st.GetAt(t.Context(), marker, ^uint64(0))
	require.NoError(t, err)
	_, err = h.st.GetAt(t.Context(), chunkRefKey, ^uint64(0))
	require.NoError(t, err)

	require.NoError(t, h.server.recoverPendingBucketDeletes(t.Context()))
	_, err = h.st.GetAt(t.Context(), marker, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = h.st.GetAt(t.Context(), chunkRefKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Zero(t, readS3ChunkRefRCTest(t, h, sha).Count)
	require.NoError(t, h.server.recoverPendingBucketDeletes(t.Context()))
}

func TestS3BucketCleanupRecoveryRunsWithOffloadDisabled(t *testing.T) {
	h := newS3OCCReproModeHarness(t, false, false)
	h.server.blobOffloadGCReady.Store(false)
	require.Equal(t, http.StatusOK, h.do(http.MethodPut, "/bucket", "").Code)
	readTimestamp, err := h.server.beginTxnReadTimestamp(t.Context(), "legacy bucket cleanup recovery test")
	require.NoError(t, err)
	require.NoError(t, h.server.dispatchBucketDeleteTxn(t.Context(), readTimestamp, "bucket", 1))
	marker := s3keys.BucketCleanupKey("bucket", 1)
	require.NoError(t, h.server.StartBlobGC(t.Context()))
	require.Eventually(t, func() bool {
		_, getErr := h.st.GetAt(t.Context(), marker, ^uint64(0))
		return errors.Is(getErr, store.ErrKeyNotFound)
	}, time.Second, time.Millisecond)
	require.False(t, h.server.blobOffloadGCReady.Load())
}
