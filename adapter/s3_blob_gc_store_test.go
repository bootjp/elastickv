package adapter

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3GCQueueStorePagesAndConditionalReclaim(t *testing.T) {
	h := newS3OCCReproModeHarness(t, false, true)
	entries := make([]s3keys.ChunkBlobGCQueueEntry, 0, 3)
	for _, body := range []string{"one", "two", "three"} {
		sha := sha256.Sum256([]byte(body))
		ref, err := s3keys.EncodeChunkRefValue(s3keys.ChunkRefValue{ContentSHA256: sha, Size: uint64(len(body))})
		require.NoError(t, err)
		key := s3keys.VersionedChunkRefKey("bucket", 1, body, "upload", 1, 0, 1)
		dispatchS3ChunkRefAccountingTest(t, h, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: ref})
		queuedAt := dispatchS3ChunkRefAccountingTest(t, h, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
		entries = append(entries, s3keys.ChunkBlobGCQueueEntry{CommitTS: queuedAt, ContentSHA256: sha})
	}
	start := s3keys.ChunkBlobGCQueueScanStart()
	end := s3keys.ChunkBlobGCQueueScanEnd(^uint64(0))
	first, next, err := h.server.ScanGCQueue(t.Context(), start, end, 2)
	require.NoError(t, err)
	require.Equal(t, entries[:2], first)
	require.NotEmpty(t, next)
	last, next, err := h.server.ScanGCQueue(t.Context(), next, end, 2)
	require.NoError(t, err)
	require.Equal(t, entries[2:], last)
	require.Empty(t, next)
	queued, err := h.server.GCQueueEntryExists(t.Context(), entries[0].ContentSHA256)
	require.NoError(t, err)
	require.True(t, queued)

	require.NoError(t, h.server.DeleteGCQueueEntryIfUnreferenced(t.Context(), entries[0]))
	queued, err = h.server.GCQueueEntryExists(t.Context(), entries[0].ContentSHA256)
	require.NoError(t, err)
	require.False(t, queued)
	_, found, err := h.server.ReadChunkRefRC(t.Context(), entries[0].ContentSHA256)
	require.NoError(t, err)
	require.False(t, found)
	_, err = h.st.GetAt(t.Context(), s3keys.ChunkBlobGCQueueKey(entries[0].CommitTS, entries[0].ContentSHA256), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.ErrorIs(t, h.server.DeleteGCQueueEntryIfUnreferenced(t.Context(), entries[0]), s3keys.ErrQueueEntryChanged)

	ref, err := s3keys.EncodeChunkRefValue(s3keys.ChunkRefValue{ContentSHA256: entries[1].ContentSHA256, Size: 3})
	require.NoError(t, err)
	key := s3keys.VersionedChunkRefKey("bucket", 1, "two-again", "upload", 1, 0, 1)
	dispatchS3ChunkRefAccountingTest(t, h, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: ref})
	require.ErrorIs(t, h.server.DeleteGCQueueEntryIfUnreferenced(t.Context(), entries[1]), s3keys.ErrQueueEntryChanged)
	require.Equal(t, uint64(1), readS3ChunkRefRCTest(t, h, entries[1].ContentSHA256).Count)
}

func TestS3GCQueueStoreClearsStaleMetadataAtomically(t *testing.T) {
	h := newS3OCCReproModeHarness(t, false, true)
	sha := sha256.Sum256([]byte("stale"))
	entry := s3keys.ChunkBlobGCQueueEntry{CommitTS: 123, ContentSHA256: sha}
	queueKey := s3keys.ChunkBlobGCQueueKey(entry.CommitTS, sha)
	rcKey := s3keys.ChunkRefRCKey(sha)
	_, err := h.coord.Dispatch(t.Context(), &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: queueKey},
		{Op: kv.Put, Key: rcKey, Value: s3keys.EncodeChunkRefRC(s3keys.ChunkRefRC{Count: 1, QueuedAtTS: entry.CommitTS})},
	}})
	require.NoError(t, err)
	require.NoError(t, h.server.ClearGCQueueMetadata(t.Context(), entry))
	rc := readS3ChunkRefRCTest(t, h, sha)
	require.Equal(t, uint64(1), rc.Count)
	require.Zero(t, rc.QueuedAtTS)
	_, err = h.st.GetAt(t.Context(), queueKey, ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestS3BlobSweeperReclaimsThroughRaftAndLocalStore(t *testing.T) {
	h := newS3OCCReproModeHarness(t, false, true)
	payload := []byte("reclaimable payload")
	sha := sha256.Sum256(payload)
	require.NoError(t, NewS3BlobFetchServer(h.st, nil).storeChunkBlob(t.Context(), sha, payload, 10))
	local, err := store.NewChunkBlobLocalStore(h.st)
	require.NoError(t, err)
	ref, err := s3keys.EncodeChunkRefValue(s3keys.ChunkRefValue{ContentSHA256: sha, Size: uint64(len(payload))})
	require.NoError(t, err)
	key := s3keys.VersionedChunkRefKey("bucket", 1, "object", "upload", 1, 0, 1)
	dispatchS3ChunkRefAccountingTest(t, h, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: ref})
	queuedAt := dispatchS3ChunkRefAccountingTest(t, h, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
	sweeper, err := s3keys.NewChunkBlobSweeper(s3keys.ChunkBlobSweeperOptions{
		Store: h.server, Local: local,
		GracePeriod: time.Millisecond,
		NowTS:       func() uint64 { return queuedAt + 2<<kv.HLCLogicalBits },
	})
	require.NoError(t, err)
	require.NoError(t, sweeper.SweepOnce(t.Context()))
	_, found, err := local.ChunkBlobWrittenAt(t.Context(), sha)
	require.NoError(t, err)
	require.False(t, found)
	_, found, err = h.server.ReadChunkRefRC(t.Context(), sha)
	require.NoError(t, err)
	require.False(t, found)
	_, err = h.st.GetAt(t.Context(), s3keys.ChunkBlobGCQueueKey(queuedAt, sha), ^uint64(0))
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}
