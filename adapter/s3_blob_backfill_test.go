package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestS3BlobBackfillerApplyHookDoesNotBlockOnFullQueue(t *testing.T) {
	t.Parallel()

	backfiller := NewS3BlobBackfiller(S3BlobBackfillConfig{QueueSize: 1})
	first := s3keys.ChunkRefKey("bucket", 1, "object", "upload", 1, 0)
	second := s3keys.ChunkRefKey("bucket", 1, "object", "upload", 1, 1)
	backfiller.OnApply(pb.Op_PUT, first)

	started := time.Now()
	backfiller.OnApply(pb.Op_PUT, second)
	require.Less(t, time.Since(started), 100*time.Millisecond)
	require.Len(t, backfiller.queue, 1)
	_, firstPending := backfiller.pending.Load(string(first))
	_, secondPending := backfiller.pending.Load(string(second))
	require.True(t, firstPending)
	require.False(t, secondPending)
}

func TestS3BlobBackfillerFetchesAppliedRefFromAlternatePeerAfterMismatch(t *testing.T) {
	t.Parallel()

	metadataStore := store.NewMVCCStore()
	blobStore := store.NewMVCCStore()
	stores := s3BlobBackfillTestStores{metadata: metadataStore, blobs: blobStore}
	cluster := newFakeS3BlobCluster()
	payload := []byte("backfilled payload")
	digest := sha256.Sum256(payload)
	cluster.blobs["n2"] = map[[sha256.Size]byte][]byte{digest: []byte("bad payload")}
	cluster.blobs["n3"] = map[[sha256.Size]byte][]byte{digest: payload}
	observer := &recordingS3BlobBackfillObserver{}
	refKey := putS3BlobBackfillRef(t, metadataStore, digest, uint64(len(payload)), "n2")
	backfiller := NewS3BlobBackfiller(s3BlobBackfillTestConfig())
	server := NewS3Server(
		nil, "", metadataStore, newLocalAdapterCoordinator(metadataStore), nil,
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(stores),
		WithS3BlobOffloadObserver(observer),
		WithS3BlobBackfiller(backfiller),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		backfiller.Stop()
	})
	require.NoError(t, server.StartBlobBackfill(ctx))

	backfiller.OnApply(pb.Op_PUT, refKey)
	require.Eventually(t, func() bool {
		got, err := metadataStore.GetAt(context.Background(), s3keys.ChunkBlobKey(digest), math.MaxUint64)
		return err == nil && bytes.Equal(got, payload)
	}, 3*time.Second, 10*time.Millisecond)
	_, err := blobStore.GetAt(context.Background(), s3keys.ChunkBlobKey(digest), math.MaxUint64)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Equal(t, refKey, cluster.lastRouteKey())
	require.GreaterOrEqual(t, observer.shaMismatches(), 1)
	require.Contains(t, observer.resultsSnapshot(), s3BlobBackfillResultFetched)
}

func TestS3BlobBackfillerStartupScanRecoversUnobservedRef(t *testing.T) {
	t.Parallel()

	metadataStore := store.NewMVCCStore()
	blobStore := store.NewMVCCStore()
	stores := s3BlobBackfillTestStores{metadata: metadataStore, blobs: blobStore}
	cluster := newFakeS3BlobCluster()
	payload := []byte("snapshot backfill")
	digest := sha256.Sum256(payload)
	cluster.blobs["n2"] = map[[sha256.Size]byte][]byte{digest: payload}
	refKey := putS3BlobBackfillRef(t, metadataStore, digest, uint64(len(payload)), "n2")
	backfiller := NewS3BlobBackfiller(s3BlobBackfillTestConfig())
	server := NewS3Server(
		nil, "", metadataStore, newLocalAdapterCoordinator(metadataStore), nil,
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(stores),
		WithS3BlobBackfiller(backfiller),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		backfiller.Stop()
	})
	require.NoError(t, server.StartBlobBackfill(ctx))

	require.Eventually(t, func() bool {
		got, err := metadataStore.GetAt(context.Background(), s3keys.ChunkBlobKey(digest), math.MaxUint64)
		return err == nil && bytes.Equal(got, payload)
	}, 3*time.Second, 10*time.Millisecond)
	_, err := blobStore.GetAt(context.Background(), s3keys.ChunkBlobKey(digest), math.MaxUint64)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	require.Equal(t, refKey, cluster.lastRouteKey())
}

func TestS3BlobBackfillConfigFromEnvRejectsUnboundedValues(t *testing.T) {
	t.Setenv(s3BlobBackfillWorkersEnvVar, "0")
	_, err := S3BlobBackfillConfigFromEnv()
	require.Error(t, err)

	t.Setenv(s3BlobBackfillWorkersEnvVar, "2")
	t.Setenv(s3BlobBackfillScanIntervalEnvVar, "not-a-duration")
	_, err = S3BlobBackfillConfigFromEnv()
	require.Error(t, err)
}

func TestS3BlobTokenBucketAllowsBurstThenRefills(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	bucket := &s3BlobTokenBucket{rate: 2, burst: 2, tokens: 2, last: now}
	require.Zero(t, bucket.reserve(now))
	require.Zero(t, bucket.reserve(now))
	require.Equal(t, 500*time.Millisecond, bucket.reserve(now))
	require.Equal(t, 750*time.Millisecond, bucket.reserve(now.Add(250*time.Millisecond)))
	require.Equal(t, time.Second, bucket.reserve(now.Add(500*time.Millisecond)))
}

func TestS3BlobTokenBucketReservesDistinctConcurrentSlots(t *testing.T) {
	t.Parallel()

	now := time.Unix(100, 0)
	bucket := &s3BlobTokenBucket{rate: 1, burst: 1, tokens: 1, last: now}
	waits := make(chan time.Duration, 4)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			waits <- bucket.reserve(now)
		}()
	}
	wg.Wait()
	close(waits)
	got := make([]time.Duration, 0, 4)
	for wait := range waits {
		got = append(got, wait)
	}
	slices.Sort(got)
	require.Equal(t, []time.Duration{0, time.Second, 2 * time.Second, 3 * time.Second}, got)
}

func TestS3BlobBackfillerStopClearsQueuedPendingKeys(t *testing.T) {
	t.Parallel()

	backfiller := NewS3BlobBackfiller(S3BlobBackfillConfig{QueueSize: 2})
	key := s3keys.ChunkRefKey("bucket", 1, "object", "upload", 1, 0)
	backfiller.OnApply(pb.Op_PUT, key)
	backfiller.Stop()

	require.Empty(t, backfiller.queue)
	_, pending := backfiller.pending.Load(string(key))
	require.False(t, pending)
	backfiller.OnApply(pb.Op_PUT, key)
	require.Len(t, backfiller.queue, 1)
}

func TestS3BlobBackfillerApplyWhileStartingPublishesServerSafely(t *testing.T) {
	t.Parallel()

	metadataStore := store.NewMVCCStore()
	backfiller := NewS3BlobBackfiller(s3BlobBackfillTestConfig())
	server := NewS3Server(
		nil, "", metadataStore, newLocalAdapterCoordinator(metadataStore), nil,
		WithS3BlobCluster(newFakeS3BlobCluster()),
		WithS3BlobLocalStoreResolver(s3BlobBackfillTestStores{metadata: metadataStore}),
		WithS3BlobBackfiller(backfiller),
	)
	key := s3keys.ChunkRefKey("bucket", 1, "object", "upload", 1, 0)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			backfiller.OnApply(pb.Op_PUT, key)
		}
	}()
	require.NoError(t, server.StartBlobBackfill(context.Background()))
	<-done
	backfiller.Stop()
}

func TestStoreFetchedS3ChunkBlobUsesReplicatedRefTimestamp(t *testing.T) {
	t.Parallel()

	local := &recordingS3BlobFetchStore{MVCCStore: store.NewMVCCStore()}
	server := &S3Server{blobLocalStores: &mutableS3BlobLocalStore{store: local}}
	refKey := s3keys.ChunkRefKey("bucket", 1, "object", "upload", 1, 0)
	payload := []byte("follower-safe repair")
	digest := sha256.Sum256(payload)

	require.NoError(t, server.storeFetchedS3ChunkBlob(context.Background(), refKey, digest, payload, 42))
	require.Equal(t, []uint64{42}, local.applyCommitTS)
}

func TestStoreFetchedS3ChunkBlobRepairsCorruptNewerVersion(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	payload := []byte("verified repair payload")
	digest := sha256.Sum256(payload)
	key := s3keys.ChunkBlobKey(digest)
	require.NoError(t, base.PutAt(context.Background(), key, []byte("corrupt"), 100, 0))
	local := &recordingS3BlobFetchStore{MVCCStore: base}
	server := &S3Server{blobLocalStores: &mutableS3BlobLocalStore{store: local}}
	refKey := s3keys.ChunkRefKey("bucket", 1, "object", "upload", 1, 0)

	require.NoError(t, server.storeFetchedS3ChunkBlob(context.Background(), refKey, digest, payload, 42))
	require.Equal(t, []uint64{100}, local.applyStartTS)
	require.Equal(t, []uint64{101}, local.applyCommitTS)
	got, err := base.GetAt(context.Background(), key, math.MaxUint64)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestS3BlobBackfillerRepairsCorruptLocalBlob(t *testing.T) {
	t.Parallel()

	metadataStore := store.NewMVCCStore()
	payload := []byte("background repair payload")
	digest := sha256.Sum256(payload)
	key := s3keys.ChunkBlobKey(digest)
	require.NoError(t, metadataStore.PutAt(context.Background(), key, []byte("corrupt"), 20, 0))
	cluster := newFakeS3BlobCluster()
	cluster.blobs["n2"] = map[[sha256.Size]byte][]byte{digest: payload}
	putS3BlobBackfillRef(t, metadataStore, digest, uint64(len(payload)), "n2")
	backfiller := NewS3BlobBackfiller(s3BlobBackfillTestConfig())
	server := NewS3Server(
		nil, "", metadataStore, newLocalAdapterCoordinator(metadataStore), nil,
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(s3BlobBackfillTestStores{metadata: metadataStore}),
		WithS3BlobBackfiller(backfiller),
	)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		backfiller.Stop()
	})
	require.NoError(t, server.StartBlobBackfill(ctx))

	require.Eventually(t, func() bool {
		got, err := metadataStore.GetAt(context.Background(), key, math.MaxUint64)
		return err == nil && bytes.Equal(got, payload)
	}, 3*time.Second, 10*time.Millisecond)
	latestTS, exists, err := metadataStore.LatestCommitTS(context.Background(), key)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(21), latestTS)
}

func s3BlobBackfillTestConfig() S3BlobBackfillConfig {
	return S3BlobBackfillConfig{
		Workers:      1,
		QueueSize:    8,
		RatePerPeer:  1000,
		BurstPerPeer: 8,
		ScanInterval: time.Hour,
		ScanPageSize: 2,
		MaxAttempts:  1,
		RetryInitial: time.Millisecond,
		RetryMax:     time.Millisecond,
	}
}

func putS3BlobBackfillRef(
	t *testing.T,
	st store.MVCCStore,
	digest [sha256.Size]byte,
	size uint64,
	source string,
) []byte {
	t.Helper()
	key := s3keys.ChunkRefKey("bucket", 1, "object", "upload", 1, 0)
	value, err := s3keys.EncodeChunkRefValue(s3keys.ChunkRefValue{
		ContentSHA256: digest,
		Size:          size,
		SourcePeer:    source,
		ReplicaPeers: []s3keys.ChunkRefPeer{
			{NodeID: "n2", Address: "n2:50051"},
			{NodeID: "n3", Address: "n3:50051"},
		},
	})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), key, value, 10, 0))
	return key
}

type s3BlobBackfillTestStores struct {
	metadata store.MVCCStore
	blobs    store.MVCCStore
}

func (s s3BlobBackfillTestStores) LocalStoreForKey(key []byte) (store.MVCCStore, bool) {
	if bytes.HasPrefix(key, []byte(s3keys.ChunkBlobPrefix)) {
		return s.blobs, s.blobs != nil
	}
	return s.metadata, s.metadata != nil
}

func (s s3BlobBackfillTestStores) LocalStores() []store.MVCCStore {
	return []store.MVCCStore{s.metadata, s.blobs}
}

type recordingS3BlobBackfillObserver struct {
	mu       sync.Mutex
	mismatch int
	results  []string
}

func (o *recordingS3BlobBackfillObserver) ObserveS3BlobOffloadDecision(string, string) {}
func (o *recordingS3BlobBackfillObserver) ObserveS3ChunkBlobReplicationDegraded()      {}
func (o *recordingS3BlobBackfillObserver) ObserveS3ChunkBlobUnrecoverable()            {}

func (o *recordingS3BlobBackfillObserver) ObserveS3ChunkBlobSHAMismatch() {
	o.mu.Lock()
	o.mismatch++
	o.mu.Unlock()
}

func (o *recordingS3BlobBackfillObserver) ObserveS3ChunkBlobBackfillQueueDepth(int) {}

func (o *recordingS3BlobBackfillObserver) ObserveS3ChunkBlobBackfillResult(result string) {
	o.mu.Lock()
	o.results = append(o.results, result)
	o.mu.Unlock()
}

func (o *recordingS3BlobBackfillObserver) shaMismatches() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.mismatch
}

func (o *recordingS3BlobBackfillObserver) resultsSnapshot() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.results...)
}
