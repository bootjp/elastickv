package adapter

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func seedS3CleanupChunkRef(t *testing.T, h *s3OCCReproHarness) ([]byte, [32]byte) {
	t.Helper()
	sha := sha256.Sum256([]byte("cleanup chunk"))
	value, err := s3keys.EncodeChunkRefValue(s3keys.ChunkRefValue{ContentSHA256: sha, Size: 13})
	require.NoError(t, err)
	key := s3keys.VersionedChunkRefKey("bucket", 1, "object", "upload", 1, 0, 1)
	readTimestamp, err := h.server.beginTxnReadTimestamp(t.Context(), "seed cleanup chunkref")
	require.NoError(t, err)
	commitTS, err := h.server.nextTxnCommitTS(t.Context(), readTimestamp.Timestamp())
	require.NoError(t, err)
	req := &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: readTimestamp.Timestamp(), CommitTS: commitTS,
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: value}},
	}
	require.NoError(t, h.server.dispatchS3MetadataMutation(readTimestamp.WithDispatchVoucher(t.Context()), req))
	return key, sha
}

func TestS3CleanupChunkRefDeleteAccountsOnce(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproModeHarness(t, false, true)
	key, sha := seedS3CleanupChunkRef(t, h)
	deleteElem := []*kv.Elem[kv.OP]{{Op: kv.Del, Key: key}}
	require.NoError(t, h.server.dispatchS3CleanupBatch(t.Context(), deleteElem))
	raw, err := h.st.GetAt(t.Context(), s3keys.ChunkRefRCKey(sha), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Zero(t, rc.Count)
	require.NotZero(t, rc.QueuedAtTS)
	_, err = h.st.GetAt(t.Context(), s3keys.ChunkBlobGCQueueKey(rc.QueuedAtTS, sha), ^uint64(0))
	require.NoError(t, err)

	require.NoError(t, h.server.dispatchS3CleanupBatch(t.Context(), deleteElem))
	raw, err = h.st.GetAt(t.Context(), s3keys.ChunkRefRCKey(sha), ^uint64(0))
	require.NoError(t, err)
	repeated, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Equal(t, rc, repeated)
}

func TestS3CleanupManifestChunkRefAccountsDelete(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproModeHarness(t, false, true)
	key, sha := seedS3CleanupChunkRef(t, h)
	h.server.cleanupManifestBlobs(t.Context(), "bucket", 1, "object", &s3ObjectManifest{
		UploadID: "upload",
		Parts:    []s3ObjectPart{{PartNo: 1, Offloaded: true, ChunkRefVersion: 1, ChunkSizes: []uint64{13}}},
	})
	_, err := h.st.GetAt(t.Context(), key, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	raw, err := h.st.GetAt(t.Context(), s3keys.ChunkRefRCKey(sha), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Zero(t, rc.Count)
	require.NotZero(t, rc.QueuedAtTS)
}

func TestS3DeleteChunkRefsByPrefixAccountsDelete(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproModeHarness(t, false, true)
	key, sha := seedS3CleanupChunkRef(t, h)
	h.server.deleteByPrefix(t.Context(), s3keys.ChunkRefPrefixForUpload("bucket", 1, "object", "upload"), "bucket", 1, "object", "upload")
	_, err := h.st.GetAt(t.Context(), key, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	raw, err := h.st.GetAt(t.Context(), s3keys.ChunkRefRCKey(sha), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Zero(t, rc.Count)
	require.NotZero(t, rc.QueuedAtTS)
}

type s3LostCleanupReplyCoordinator struct {
	*s3OCCReproCoordinator
	deleteCalls int
}

func (c *s3LostCleanupReplyCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	resp, err := c.s3OCCReproCoordinator.Dispatch(ctx, req)
	if err != nil || req == nil || !req.IsTxn || !operationGroupHasDel(req.Elems) {
		return resp, err
	}
	c.deleteCalls++
	return nil, context.DeadlineExceeded
}

func TestS3CleanupChunkRefLostReplyDoesNotRetry(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproModeHarness(t, false, true)
	key, sha := seedS3CleanupChunkRef(t, h)
	coord := &s3LostCleanupReplyCoordinator{s3OCCReproCoordinator: h.coord}
	h.server.coordinator = coord
	err := h.server.dispatchS3CleanupBatch(t.Context(), []*kv.Elem[kv.OP]{{Op: kv.Del, Key: key}})
	require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
	require.Equal(t, 1, coord.deleteCalls)
	_, err = h.st.GetAt(t.Context(), key, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	raw, err := h.st.GetAt(t.Context(), s3keys.ChunkRefRCKey(sha), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	require.Zero(t, rc.Count)
}
