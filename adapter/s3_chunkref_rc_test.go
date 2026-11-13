package adapter

import (
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func dispatchS3ChunkRefAccountingTest(t *testing.T, h *s3OCCReproHarness, elems ...*kv.Elem[kv.OP]) uint64 {
	t.Helper()
	ctx := context.Background()
	readTimestamp, err := h.server.beginTxnReadTimestamp(ctx, h.server.readTS(), "s3 chunkref accounting test")
	require.NoError(t, err)
	commitTS, err := h.server.nextTxnCommitTS(ctx, readTimestamp.Timestamp())
	require.NoError(t, err)
	req := &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: readTimestamp.Timestamp(), CommitTS: commitTS, Elems: elems,
	}
	require.NoError(t, h.server.dispatchS3MetadataMutation(readTimestamp.WithDispatchVoucher(ctx), req))
	return commitTS
}

func readS3ChunkRefRCTest(t *testing.T, h *s3OCCReproHarness, sha [32]byte) s3keys.ChunkRefRC {
	t.Helper()
	raw, err := h.st.GetAt(t.Context(), s3keys.ChunkRefRCKey(sha), ^uint64(0))
	require.NoError(t, err)
	rc, ok := s3keys.DecodeChunkRefRC(raw)
	require.True(t, ok)
	return rc
}

func TestS3ChunkRefAccountingSameSHAAndQueueLifecycle(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproModeHarness(t, false, true)
	sha := sha256.Sum256([]byte("shared chunk"))
	ref, err := s3keys.EncodeChunkRefValue(s3keys.ChunkRefValue{ContentSHA256: sha, Size: 12})
	require.NoError(t, err)
	first := s3keys.VersionedChunkRefKey("bucket", 1, "object", "upload", 1, 0, 1)
	second := s3keys.VersionedChunkRefKey("bucket", 1, "object", "upload", 1, 1, 1)
	dispatchS3ChunkRefAccountingTest(t, h,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: first, Value: ref},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: second, Value: ref})
	require.Equal(t, uint64(2), readS3ChunkRefRCTest(t, h, sha).Count)

	dispatchS3ChunkRefAccountingTest(t, h, &kv.Elem[kv.OP]{Op: kv.Del, Key: first})
	require.Equal(t, uint64(1), readS3ChunkRefRCTest(t, h, sha).Count)
	queuedAt := dispatchS3ChunkRefAccountingTest(t, h, &kv.Elem[kv.OP]{Op: kv.Del, Key: second})
	rc := readS3ChunkRefRCTest(t, h, sha)
	require.Zero(t, rc.Count)
	require.Equal(t, queuedAt, rc.QueuedAtTS)
	queueKey := s3keys.ChunkBlobGCQueueKey(queuedAt, sha)
	_, err = h.st.GetAt(t.Context(), queueKey, ^uint64(0))
	require.NoError(t, err)

	dispatchS3ChunkRefAccountingTest(t, h, &kv.Elem[kv.OP]{Op: kv.Put, Key: first, Value: ref})
	rc = readS3ChunkRefRCTest(t, h, sha)
	require.Equal(t, uint64(1), rc.Count)
	require.Zero(t, rc.QueuedAtTS)
	_, err = h.st.GetAt(t.Context(), queueKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	dispatchS3ChunkRefAccountingTest(t, h,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: first, Value: ref},
		&kv.Elem[kv.OP]{Op: kv.Del, Key: second})
	require.Equal(t, uint64(1), readS3ChunkRefRCTest(t, h, sha).Count)
}

func TestS3ChunkRefAccountingRejectsMissingReferenceCount(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproModeHarness(t, false, true)
	sha := sha256.Sum256([]byte("unaccounted chunk"))
	ref, err := s3keys.EncodeChunkRefValue(s3keys.ChunkRefValue{ContentSHA256: sha, Size: 18})
	require.NoError(t, err)
	key := s3keys.VersionedChunkRefKey("bucket", 1, "object", "upload", 1, 0, 1)
	_, err = h.coord.Dispatch(t.Context(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: ref}}})
	require.NoError(t, err)
	readTimestamp, err := h.server.beginTxnReadTimestamp(t.Context(), h.server.readTS(), "s3 chunkref underflow test")
	require.NoError(t, err)
	commitTS, err := h.server.nextTxnCommitTS(t.Context(), readTimestamp.Timestamp())
	require.NoError(t, err)
	req := &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: readTimestamp.Timestamp(), CommitTS: commitTS,
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Del, Key: key}},
	}
	require.ErrorIs(t, h.server.addS3ChunkRefAccounting(t.Context(), req), s3keys.ErrChunkRefRCUnderflow)
	require.Len(t, req.Elems, 1)
	require.Empty(t, req.ReadKeys)
	replacement := &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: readTimestamp.Timestamp(), CommitTS: commitTS,
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: ref}},
	}
	require.ErrorIs(t, h.server.addS3ChunkRefAccounting(t.Context(), replacement), s3keys.ErrChunkRefRCUnderflow)
	require.Len(t, replacement.Elems, 1)
	require.Empty(t, replacement.ReadKeys)
	_, err = h.st.GetAt(t.Context(), key, ^uint64(0))
	require.NoError(t, err)
}

func TestS3OffloadedObjectUpdatesReferenceCountThroughHTTP(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproModeHarness(t, false, true)
	const bucket = "accounted-bucket"
	const object = "object"
	const body = "content-addressed payload"
	require.Equal(t, http.StatusOK, h.do(http.MethodPut, "/"+bucket, "").Code)
	require.Equal(t, http.StatusOK, h.do(http.MethodPut, "/"+bucket+"/"+object, body).Code)
	meta, exists, err := h.server.loadBucketMetaAt(t.Context(), bucket, ^uint64(0))
	require.NoError(t, err)
	require.True(t, exists)
	prefix := s3keys.ChunkRefPrefixForBucket(bucket, meta.Generation)
	refs, err := h.st.ScanAt(t.Context(), prefix, prefixScanEnd(prefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, refs, 1)
	sha := sha256.Sum256([]byte(body))
	require.Equal(t, uint64(1), readS3ChunkRefRCTest(t, h, sha).Count)

	require.Equal(t, http.StatusNoContent, h.do(http.MethodDelete, "/"+bucket+"/"+object, "").Code)
	h.waitCleanups()
	rc := readS3ChunkRefRCTest(t, h, sha)
	require.Zero(t, rc.Count)
	require.NotZero(t, rc.QueuedAtTS)
	_, err = h.st.GetAt(t.Context(), refs[0].Key, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	_, err = h.st.GetAt(t.Context(), s3keys.ChunkBlobGCQueueKey(rc.QueuedAtTS, sha), ^uint64(0))
	require.NoError(t, err)
}

func TestS3OffloadedObjectRepeatedChunkAcrossStagedAndFinalBatches(t *testing.T) {
	h := newS3OCCReproModeHarness(t, false, true)
	const bucket = "repeated-chunk-bucket"
	const object = "object"
	body := strings.Repeat("z", (s3MetaBatchOps+1)*s3ChunkSize)
	require.Equal(t, http.StatusOK, h.do(http.MethodPut, "/"+bucket, "").Code)
	response := h.do(http.MethodPut, "/"+bucket+"/"+object, body)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	sha := sha256.Sum256([]byte(strings.Repeat("z", s3ChunkSize)))
	require.Equal(t, uint64(s3MetaBatchOps+1), readS3ChunkRefRCTest(t, h, sha).Count)
	response = h.do(http.MethodGet, "/"+bucket+"/"+object, "")
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	require.Equal(t, body, response.Body.String())
}

func TestS3BucketDeleteAccountsOrphanChunkRefs(t *testing.T) {
	t.Parallel()
	for _, gcReady := range []bool{false, true} {
		name := "gc-not-ready"
		if gcReady {
			name = "gc-ready"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newS3OCCReproModeHarness(t, false, true)
			require.Equal(t, http.StatusOK, h.do(http.MethodPut, "/bucket", "").Code)
			key, sha := seedS3CleanupChunkRef(t, h)
			h.server.blobOffloadGCReady = gcReady
			require.Equal(t, uint64(1), readS3ChunkRefRCTest(t, h, sha).Count)
			require.NoError(t, h.server.AdminDeleteBucket(t.Context(), fullAdminBucketsPrincipal(), "bucket"))
			_, err := h.st.GetAt(t.Context(), key, ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			rc := readS3ChunkRefRCTest(t, h, sha)
			require.Zero(t, rc.Count)
			require.NotZero(t, rc.QueuedAtTS)
			_, err = h.st.GetAt(t.Context(), s3keys.ChunkBlobGCQueueKey(rc.QueuedAtTS, sha), ^uint64(0))
			require.NoError(t, err)
		})
	}
}
