package adapter

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestS3BlobOffloadPutAndProxyOnMissGet(t *testing.T) {
	t.Parallel()

	metadataStore := store.NewMVCCStore()
	localResolver := &mutableS3BlobLocalStore{store: metadataStore}
	cluster := newFakeS3BlobCluster()
	server := NewS3Server(
		nil, "", metadataStore, newLocalAdapterCoordinator(metadataStore), nil,
		WithS3BlobOffloadEnabled(true),
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(localResolver),
	)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	payload := bytes.Repeat([]byte("blob-offload"), s3ChunkSize/len("blob-offload")+100)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/object", bytes.NewReader(payload)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	readTS := server.readTS()
	legacy, err := metadataStore.ScanAt(context.Background(), []byte(s3keys.BlobPrefix), prefixScanEnd([]byte(s3keys.BlobPrefix)), 10, readTS)
	require.NoError(t, err)
	require.Empty(t, legacy)
	refs, err := metadataStore.ScanAt(context.Background(), []byte(s3keys.ChunkRefPrefix), prefixScanEnd([]byte(s3keys.ChunkRefPrefix)), 10, readTS)
	require.NoError(t, err)
	require.Len(t, refs, 2)
	blobs, err := metadataStore.ScanAt(context.Background(), []byte(s3keys.ChunkBlobPrefix), prefixScanEnd([]byte(s3keys.ChunkBlobPrefix)), 10, readTS)
	require.NoError(t, err)
	require.Len(t, blobs, 2)
	require.GreaterOrEqual(t, cluster.pushCount(), 2)

	// Simulate a follower that applied metadata but has no local chunkblobs.
	// GET must fetch from peers, verify the SHA, persist locally, and only then
	// send the successful HTTP response.
	followerLocal := store.NewMVCCStore()
	localResolver.set(followerLocal)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a/object", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, payload, rec.Body.Bytes())
	require.GreaterOrEqual(t, cluster.fetchCount(), 2)
	fetched, err := followerLocal.ScanAt(context.Background(), []byte(s3keys.ChunkBlobPrefix), prefixScanEnd([]byte(s3keys.ChunkBlobPrefix)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, fetched, 2)
}

func TestS3BlobOffloadFailsPutWithoutTwoDurableCopies(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	cluster := newFakeS3BlobCluster()
	cluster.pushErr = status.Error(codes.Unavailable, "peer unavailable")
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3BlobOffloadEnabled(true),
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: st}),
	)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/object", bytes.NewReader([]byte("payload"))))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	refs, err := st.ScanAt(context.Background(), []byte(s3keys.ChunkRefPrefix), prefixScanEnd([]byte(s3keys.ChunkRefPrefix)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Empty(t, refs)
}

func TestS3BlobOffloadStartsLocalAndRemoteWritesConcurrently(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	localStarted := make(chan struct{})
	release := make(chan struct{})
	blocking := &blockingS3BlobStore{MVCCStore: base, started: localStarted, release: release}
	cluster := newFakeS3BlobCluster()
	cluster.pushStarted = make(chan struct{}, 2)
	cluster.pushRelease = release
	server := NewS3Server(
		nil, "", base, newLocalAdapterCoordinator(base), nil,
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: blocking}),
	)

	done := make(chan error, 1)
	go func() {
		payload := []byte("concurrent durability")
		digest := sha256.Sum256(payload)
		_, err := server.persistS3ChunkBlob(context.Background(), digest, payload, 10)
		done <- err
	}()

	<-localStarted
	<-cluster.pushStarted
	close(release)
	require.NoError(t, <-done)
}

func TestS3BlobMinReplicasFromEnvRejectsLeaderOnly(t *testing.T) {
	t.Setenv(s3BlobMinReplicasEnvVar, "1")
	_, err := S3BlobMinReplicasFromEnv()
	require.Error(t, err)
}

func TestS3BlobOffloadRangeFetchesOnlyRequestedChunk(t *testing.T) {
	t.Parallel()

	server, cluster, localResolver := newS3BlobM1TestServer(t, nil)
	payload := bytes.Repeat([]byte("range-offload"), s3ChunkSize/len("range-offload")+100)
	putS3BlobM1Object(t, server, "/bucket-range/object", payload)

	localResolver.set(store.NewMVCCStore())
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/bucket-range/object", nil)
	req.Header.Set("Range", "bytes=0-9")
	server.handle(rec, req)
	require.Equal(t, http.StatusPartialContent, rec.Code, rec.Body.String())
	require.Equal(t, payload[:10], rec.Body.Bytes())
	require.Equal(t, 1, cluster.fetchCount())
}

func TestS3BlobOffloadRepairsCorruptLocalChunk(t *testing.T) {
	t.Parallel()

	observer := &recordingS3BlobOffloadObserver{}
	server, _, localResolver := newS3BlobM1TestServer(t, observer)
	payload := []byte("repair-corrupt-local-copy")
	putS3BlobM1Object(t, server, "/bucket-repair/object", payload)

	follower := store.NewMVCCStore()
	digest := sha256.Sum256(payload)
	require.NoError(t, follower.PutAt(context.Background(), s3keys.ChunkBlobKey(digest), []byte("corrupt"), 1, 0))
	localResolver.set(follower)
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-repair/object", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, payload, rec.Body.Bytes())
	require.GreaterOrEqual(t, observer.shaMismatch, 1)
	repaired, err := follower.GetAt(context.Background(), s3keys.ChunkBlobKey(digest), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, payload, repaired)
}

func TestS3BlobOffloadAllPeersMissingReturnsInternalError(t *testing.T) {
	t.Parallel()

	observer := &recordingS3BlobOffloadObserver{}
	server, cluster, localResolver := newS3BlobM1TestServer(t, observer)
	payload := []byte("unrecoverable-copy")
	putS3BlobM1Object(t, server, "/bucket-missing/object", payload)

	require.Eventually(t, func() bool { return cluster.pushCount() == 2 }, time.Second, time.Millisecond)
	cluster.clearBlobs()
	localResolver.set(store.NewMVCCStore())
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-missing/object", nil))
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, 1, observer.unrecoverable)
}

func TestS3BlobOffloadMultipartRoundTrip(t *testing.T) {
	t.Parallel()

	server, _, localResolver := newS3BlobM1TestServer(t, nil)
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-multipart", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-multipart/object?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initiated s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initiated))
	require.NotEmpty(t, initiated.UploadId)

	payload := []byte("offloaded multipart payload")
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(
		http.MethodPut,
		fmt.Sprintf("/bucket-multipart/object?uploadId=%s&partNumber=1", initiated.UploadId),
		bytes.NewReader(payload),
	))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	etag := strings.Trim(rec.Header().Get("ETag"), `"`)

	completeBody := fmt.Sprintf(
		`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part></CompleteMultipartUpload>`,
		etag,
	)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(
		http.MethodPost,
		fmt.Sprintf("/bucket-multipart/object?uploadId=%s", initiated.UploadId),
		strings.NewReader(completeBody),
	))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	localResolver.set(store.NewMVCCStore())
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-multipart/object", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, payload, rec.Body.Bytes())
}

func TestS3BlobDurableTarget(t *testing.T) {
	t.Parallel()

	replicas := newFakeS3BlobCluster().replicas
	server := &S3Server{}
	target, degraded, err := server.s3BlobDurableTarget(replicas, "n1")
	require.NoError(t, err)
	require.Equal(t, 2, target)
	require.False(t, degraded)

	server.blobMinReplicas = 3
	target, degraded, err = server.s3BlobDurableTarget(replicas, "n1")
	require.NoError(t, err)
	require.Equal(t, 3, target)
	require.False(t, degraded)

	target, degraded, err = server.s3BlobDurableTarget(replicas[:2], "n1")
	require.NoError(t, err)
	require.Equal(t, 2, target)
	require.True(t, degraded)

	five := append(append([]S3BlobReplica(nil), replicas...),
		S3BlobReplica{NodeID: "n4", Address: "n4:50051", Suffrage: "voter"},
		S3BlobReplica{NodeID: "n5", Address: "n5:50051", Suffrage: "voter"},
	)
	server.blobMinReplicas = 0
	target, degraded, err = server.s3BlobDurableTarget(five, "n1")
	require.NoError(t, err)
	require.Equal(t, 3, target)
	require.False(t, degraded)
}

func TestGRPCS3BlobClusterRequiresAndForwardsBearerToken(t *testing.T) {
	t.Parallel()

	withoutToken := requireGRPCS3BlobCluster(t, "")
	require.False(t, withoutToken.AllPeersSupportS3BlobOffload(context.Background()))

	cluster := requireGRPCS3BlobCluster(t, "peer-secret")
	ctx := cluster.authorizedContext(context.Background())
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	require.Equal(t, []string{"Bearer peer-secret"}, md.Get("authorization"))
}

func requireGRPCS3BlobCluster(t *testing.T, token string) *grpcS3BlobCluster {
	t.Helper()
	cluster, ok := NewGRPCS3BlobCluster("n1", nil, token).(*grpcS3BlobCluster)
	require.True(t, ok)
	return cluster
}

func newS3BlobM1TestServer(
	t *testing.T,
	observer S3BlobOffloadObserver,
) (*S3Server, *fakeS3BlobCluster, *mutableS3BlobLocalStore) {
	t.Helper()
	st := store.NewMVCCStore()
	cluster := newFakeS3BlobCluster()
	localResolver := &mutableS3BlobLocalStore{store: st}
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3BlobOffloadEnabled(true),
		WithS3BlobCluster(cluster),
		WithS3BlobLocalStoreResolver(localResolver),
		WithS3BlobOffloadObserver(observer),
	)
	return server, cluster, localResolver
}

func putS3BlobM1Object(t *testing.T, server *S3Server, path string, payload []byte) {
	t.Helper()
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	require.Len(t, parts, 2)
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/"+parts[0], nil))
	require.Equal(t, http.StatusOK, rec.Code)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, path, bytes.NewReader(payload)))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

type mutableS3BlobLocalStore struct {
	mu    sync.RWMutex
	store store.MVCCStore
}

func (r *mutableS3BlobLocalStore) LocalStoreForKey([]byte) (store.MVCCStore, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.store, r.store != nil
}

func (r *mutableS3BlobLocalStore) set(st store.MVCCStore) {
	r.mu.Lock()
	r.store = st
	r.mu.Unlock()
}

type blockingS3BlobStore struct {
	store.MVCCStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *blockingS3BlobStore) ApplyMutationsPreservingLastCommitTS(ctx context.Context, mutations []*store.KVPairMutation, readKeys [][]byte, startTS, commitTS uint64) error {
	s.once.Do(func() { close(s.started) })
	select {
	case <-ctx.Done():
		return errors.WithStack(ctx.Err())
	case <-s.release:
	}
	preserving, ok := s.MVCCStore.(interface {
		ApplyMutationsPreservingLastCommitTS(context.Context, []*store.KVPairMutation, [][]byte, uint64, uint64) error
	})
	if !ok {
		return errors.New("test store does not preserve last commit timestamp")
	}
	return preserving.ApplyMutationsPreservingLastCommitTS(ctx, mutations, readKeys, startTS, commitTS)
}

type fakeS3BlobCluster struct {
	mu          sync.Mutex
	self        string
	replicas    []S3BlobReplica
	blobs       map[string]map[[sha256.Size]byte][]byte
	pushErr     error
	pushes      int
	fetches     int
	pushStarted chan struct{}
	pushRelease chan struct{}
}

func newFakeS3BlobCluster() *fakeS3BlobCluster {
	return &fakeS3BlobCluster{
		self: "n1",
		replicas: []S3BlobReplica{
			{NodeID: "n1", Address: "n1:50051", Suffrage: "voter"},
			{NodeID: "n2", Address: "n2:50051", Suffrage: "voter"},
			{NodeID: "n3", Address: "n3:50051", Suffrage: "voter"},
		},
		blobs: map[string]map[[sha256.Size]byte][]byte{},
	}
}

func (c *fakeS3BlobCluster) AllPeersSupportS3BlobOffload(context.Context) bool { return true }
func (c *fakeS3BlobCluster) SelfNodeID() string                                { return c.self }
func (c *fakeS3BlobCluster) Close() error                                      { return nil }

func (c *fakeS3BlobCluster) ReplicasForChunk(context.Context, []byte) ([]S3BlobReplica, error) {
	return append([]S3BlobReplica(nil), c.replicas...), nil
}

func (c *fakeS3BlobCluster) PushChunkBlob(ctx context.Context, replica S3BlobReplica, digest [sha256.Size]byte, payload []byte, _ uint64) error {
	if c.pushStarted != nil {
		c.pushStarted <- struct{}{}
	}
	if c.pushRelease != nil {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-c.pushRelease:
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pushes++
	if c.pushErr != nil {
		return c.pushErr
	}
	if c.blobs[replica.NodeID] == nil {
		c.blobs[replica.NodeID] = map[[sha256.Size]byte][]byte{}
	}
	c.blobs[replica.NodeID][digest] = bytes.Clone(payload)
	return nil
}

func (c *fakeS3BlobCluster) FetchChunkBlob(_ context.Context, replica S3BlobReplica, digest [sha256.Size]byte) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fetches++
	payload := c.blobs[replica.NodeID][digest]
	if payload == nil {
		return nil, status.Error(codes.NotFound, "missing")
	}
	return bytes.Clone(payload), nil
}

func (c *fakeS3BlobCluster) pushCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pushes
}

func (c *fakeS3BlobCluster) fetchCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fetches
}

func (c *fakeS3BlobCluster) clearBlobs() {
	c.mu.Lock()
	c.blobs = map[string]map[[sha256.Size]byte][]byte{}
	c.mu.Unlock()
}
