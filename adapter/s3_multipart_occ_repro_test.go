package adapter

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// S3 multipart read-set reproduction harness (gap G2 of the serializable
// isolation audit).
//
// One single-node Raft group (newSingleRaftForS3Test) running the production
// kvFSM, fronted by a production kv.ShardedCoordinator with one route, so
// every S3 transaction goes through the real one-phase commit path and the
// store's OCC validation (write keys and ReadKeys against StartTS) under the
// apply lock. The S3Server reads through s3OCCReproStore, which can hold a
// handler right after a chosen GetAt returned, while the test drives a
// concurrent handler to completion. The FSM applies to the unwrapped store.
//
// The hold is at a read, not at Dispatch: the S3 handlers allocate their
// commit timestamp before they dispatch, so a handler held at Dispatch would
// commit below the concurrent writer and its late apply would be the
// apply-order gap (G0), not the read-set gap this file is about.

const s3OCCReproWaitTimeout = 10 * time.Second

// s3OCCReproPark holds the first GetAt whose key match accepts, after the
// read returned, until release is closed. Later matches pass straight
// through.
type s3OCCReproPark struct {
	name    string
	match   func(key []byte) bool
	armed   bool
	parked  chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *s3OCCReproPark) releaseNow() {
	p.once.Do(func() { close(p.release) })
}

type s3OCCReproStore struct {
	store.MVCCStore
	mu    sync.Mutex
	parks []*s3OCCReproPark
	reads []s3OCCReproRead
}

type s3OCCReproRead struct {
	key       []byte
	ts        uint64
	requestID uint64
}

type s3OCCReproRequestContextKey struct{}

func s3OCCReproRequestID(ctx context.Context) uint64 {
	id, _ := ctx.Value(s3OCCReproRequestContextKey{}).(uint64)
	return id
}

func (s *s3OCCReproStore) readLog() []s3OCCReproRead {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.reads)
}

func (s *s3OCCReproStore) addPark(name string, match func(key []byte) bool) *s3OCCReproPark {
	park := &s3OCCReproPark{
		name:    name,
		match:   match,
		armed:   true,
		parked:  make(chan struct{}),
		release: make(chan struct{}),
	}
	s.mu.Lock()
	s.parks = append(s.parks, park)
	s.mu.Unlock()
	return park
}

func (s *s3OCCReproStore) releaseAll() {
	s.mu.Lock()
	parks := append([]*s3OCCReproPark(nil), s.parks...)
	s.mu.Unlock()
	for _, park := range parks {
		park.releaseNow()
	}
}

func (s *s3OCCReproStore) claim(key []byte) *s3OCCReproPark {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, park := range s.parks {
		if park.armed && park.match(key) {
			park.armed = false
			return park
		}
	}
	return nil
}

func (s *s3OCCReproStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	val, err := s.MVCCStore.GetAt(ctx, key, ts)
	s.mu.Lock()
	s.reads = append(s.reads, s3OCCReproRead{key: bytes.Clone(key), ts: ts, requestID: s3OCCReproRequestID(ctx)})
	s.mu.Unlock()
	if park := s.claim(key); park != nil {
		close(park.parked)
		select {
		case <-park.release:
		case <-ctx.Done():
			return nil, errors.WithStack(ctx.Err())
		}
	}
	return val, errors.WithStack(err)
}

// s3OCCReproCoordinator forwards to the production ShardedCoordinator. It
// counts every Del it forwards (the async cleanups are the only S3 path that
// Dels blob keys) and, when failDelPrefix is set, fails DEL_PREFIX groups the
// way a failed bucket-delete safety net would.
type s3OCCReproCoordinator struct {
	*kv.ShardedCoordinator
	failDelPrefix atomic.Bool
	// maxReadKeys is the largest ReadKeys set dispatched so far.
	maxReadKeys atomic.Int64
	mu          sync.Mutex
	dels        map[string]int
	txns        []*kv.OperationGroup[kv.OP]
	requestIDs  map[*kv.OperationGroup[kv.OP]]uint64
}

func (c *s3OCCReproCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req != nil {
		if req.IsTxn {
			copyReq := *req
			copyReq.ReadKeys = slices.Clone(req.ReadKeys)
			copyReq.Elems = slices.Clone(req.Elems)
			c.mu.Lock()
			c.txns = append(c.txns, &copyReq)
			c.requestIDs[&copyReq] = s3OCCReproRequestID(ctx)
			c.mu.Unlock()
		}
		if c.failDelPrefix.Load() && s3OCCReproHasOp(req, kv.DelPrefix) {
			return nil, errors.New("s3 occ repro: injected DEL_PREFIX failure")
		}
		if !req.IsTxn {
			c.recordDels(req.Elems)
		}
		for n := int64(len(req.ReadKeys)); ; {
			seen := c.maxReadKeys.Load()
			if n <= seen || c.maxReadKeys.CompareAndSwap(seen, n) {
				break
			}
		}
	}
	resp, err := c.ShardedCoordinator.Dispatch(ctx, req)
	return resp, errors.WithStack(err)
}

func s3OCCReproHasOp(req *kv.OperationGroup[kv.OP], op kv.OP) bool {
	for _, elem := range req.Elems {
		if elem != nil && elem.Op == op {
			return true
		}
	}
	return false
}

func (c *s3OCCReproCoordinator) recordDels(elems []*kv.Elem[kv.OP]) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, elem := range elems {
		if elem != nil && elem.Op == kv.Del {
			c.dels[string(elem.Key)]++
		}
	}
}

func (c *s3OCCReproCoordinator) delCount(key []byte) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.dels[string(key)]
}

type s3OCCReproHarness struct {
	t          *testing.T
	st         store.MVCCStore
	reads      *s3OCCReproStore
	coord      *s3OCCReproCoordinator
	server     *S3Server
	handlers   sync.WaitGroup
	requestIDs atomic.Uint64
}

func newS3OCCReproHarness(t *testing.T) *s3OCCReproHarness {
	t.Helper()
	return newS3OCCReproVisibilityHarness(t, false)
}

func newS3OCCReproVisibilityHarness(t *testing.T, staged bool) *s3OCCReproHarness {
	t.Helper()
	return newS3OCCReproModeHarness(t, staged, true)
}

func newS3OCCReproModeHarness(t *testing.T, staged, offloaded bool) *s3OCCReproHarness {
	t.Helper()

	hlc := kv.NewHLC()
	st := store.NewMVCCStore()
	routes := distribution.NewEngine()
	route := distribution.RouteDescriptor{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive}
	if staged {
		route.StagedVisibilityActive = true
		route.MigrationJobID = 9
	}
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{route}}))
	fsm := kv.NewKvFSMWithHLC(st, hlc, kv.WithRouteHistory(kv.WrapDistributionEngine(routes), 1))
	engine, stop := newSingleRaftForS3Test(t, "s3-occ-repro", fsm)
	t.Cleanup(stop)

	groups := map[uint64]*kv.ShardGroup{
		1: {Engine: engine, Store: st, Txn: kv.NewLeaderProxyWithEngine(engine)},
	}
	shardStore := kv.NewShardStore(routes, groups)
	coord := &s3OCCReproCoordinator{
		ShardedCoordinator: kv.NewShardedCoordinator(routes, groups, 1, hlc, shardStore),
		dels:               map[string]int{},
		requestIDs:         map[*kv.OperationGroup[kv.OP]]uint64{},
	}
	reads := &s3OCCReproStore{MVCCStore: shardStore}
	h := &s3OCCReproHarness{
		t:     t,
		st:    st,
		reads: reads,
		coord: coord,
		server: NewS3Server(nil, "", reads, coord, nil,
			WithS3BlobOffloadEnabled(offloaded),
			WithS3BlobCluster(newFakeS3BlobCluster()),
			WithS3BlobLocalStoreResolver(&mutableS3BlobLocalStore{store: st}),
			withS3BlobOffloadGCReadyForTest()),
	}
	t.Cleanup(func() { require.NoError(t, coord.Close()) })
	t.Cleanup(h.server.Stop)
	// Release every park before the Raft engine stops so no handler
	// goroutine is left blocked on a failed test.
	t.Cleanup(func() {
		reads.releaseAll()
		h.handlers.Wait()
		h.waitCleanups()
	})
	return h
}

func (h *s3OCCReproHarness) do(method, target, body string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := newS3TestRequest(method, target, strings.NewReader(body))
	ctx := context.WithValue(req.Context(), s3OCCReproRequestContextKey{}, h.requestIDs.Add(1))
	h.server.handle(rec, req.WithContext(ctx))
	return rec
}

// s3OCCReproObject is the one object every reproduction uploads; each test
// uses its own bucket.
const s3OCCReproObject = "obj"

// s3OCCReproUpload is one in-progress multipart upload of s3OCCReproObject.
// The reproductions only race part 1.
type s3OCCReproUpload struct {
	bucket        string
	generation    uint64
	uploadID      string
	uploadMetaKey []byte
	partKey       []byte
}

func (u s3OCCReproUpload) objectPath() string {
	return "/" + u.bucket + "/" + s3OCCReproObject
}

func (u s3OCCReproUpload) chunkKey(chunkNo uint64, part s3ObjectPart) []byte {
	if part.Offloaded {
		return s3keys.VersionedChunkRefKey(u.bucket, u.generation, s3OCCReproObject, u.uploadID, part.PartNo, chunkNo, part.ChunkRefVersion)
	}
	return s3keys.VersionedBlobKey(u.bucket, u.generation, s3OCCReproObject, u.uploadID, part.PartNo, chunkNo, part.PartVersion)
}

// startUpload creates the bucket and a multipart upload in it.
func (h *s3OCCReproHarness) startUpload(bucket string) s3OCCReproUpload {
	h.t.Helper()
	rec := h.do(http.MethodPut, "/"+bucket, "")
	require.Equal(h.t, http.StatusOK, rec.Code, rec.Body.String())
	meta, exists, err := h.server.loadBucketMetaAt(context.Background(), bucket, ^uint64(0))
	require.NoError(h.t, err)
	require.True(h.t, exists)

	u := s3OCCReproUpload{bucket: bucket, generation: meta.Generation}
	rec = h.do(http.MethodPost, u.objectPath()+"?uploads=", "")
	require.Equal(h.t, http.StatusOK, rec.Code, rec.Body.String())
	var result s3InitiateMultipartUploadResult
	require.NoError(h.t, xml.Unmarshal(rec.Body.Bytes(), &result))
	require.NotEmpty(h.t, result.UploadId)
	u.uploadID = result.UploadId
	u.uploadMetaKey = s3keys.UploadMetaKey(bucket, u.generation, s3OCCReproObject, u.uploadID)
	u.partKey = s3keys.UploadPartKey(bucket, u.generation, s3OCCReproObject, u.uploadID, 1)
	return u
}

func (h *s3OCCReproHarness) uploadPart1(u s3OCCReproUpload, body string) *httptest.ResponseRecorder {
	return h.do(http.MethodPut, fmt.Sprintf("%s?uploadId=%s&partNumber=1", u.objectPath(), u.uploadID), body)
}

func (h *s3OCCReproHarness) completeWithPart1(u s3OCCReproUpload, etag string) *httptest.ResponseRecorder {
	body := fmt.Sprintf(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part></CompleteMultipartUpload>`, etag)
	return h.do(http.MethodPost, fmt.Sprintf("%s?uploadId=%s", u.objectPath(), u.uploadID), body)
}

// part1Descriptor returns the latest committed descriptor of part 1.
func (h *s3OCCReproHarness) part1Descriptor(u s3OCCReproUpload) (*s3PartDescriptor, bool) {
	h.t.Helper()
	raw, err := h.st.GetAt(context.Background(), u.partKey, ^uint64(0))
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false
	}
	require.NoError(h.t, err)
	var descriptor s3PartDescriptor
	require.NoError(h.t, json.Unmarshal(raw, &descriptor))
	return &descriptor, true
}

// blobVersions returns the live immutable blob or chunk-reference namespaces.
func (h *s3OCCReproHarness) blobVersions(u s3OCCReproUpload) map[uint64]int {
	h.t.Helper()
	prefix := s3keys.ChunkRefPrefixForUpload(u.bucket, u.generation, s3OCCReproObject, u.uploadID)
	if !h.server.blobOffloadEnabled {
		prefix = s3keys.BlobPrefixForUpload(u.bucket, u.generation, s3OCCReproObject, u.uploadID)
	}
	kvs, err := h.st.ScanAt(context.Background(), prefix, prefixScanEnd(prefix), s3MetaBatchOps, ^uint64(0))
	require.NoError(h.t, err)
	out := map[uint64]int{}
	for _, kvp := range kvs {
		_, _, _, _, _, _, version, ok := s3keys.ParseVersionedChunkRefKey(kvp.Key)
		if !h.server.blobOffloadEnabled {
			_, _, _, _, _, _, version, ok = s3keys.ParseBlobKey(kvp.Key)
		}
		require.True(h.t, ok, "unparseable blob key %q", kvp.Key)
		out[version]++
	}
	return out
}

// waitCleanups returns once every asynchronous cleanup launched so far has
// finished: each holds a cleanupSem slot for its whole run, so owning every
// slot proves none is still running.
func (h *s3OCCReproHarness) waitCleanups() {
	h.t.Helper()
	acquired := 0
	defer func() {
		for range acquired {
			<-h.server.cleanupSem
		}
	}()
	timer := time.NewTimer(s3OCCReproWaitTimeout)
	defer timer.Stop()
	for range cap(h.server.cleanupSem) {
		select {
		case h.server.cleanupSem <- struct{}{}:
			acquired++
		case <-timer.C:
			h.t.Fatal("asynchronous S3 cleanups did not finish")
		}
	}
}

func (h *s3OCCReproHarness) waitParked(park *s3OCCReproPark) {
	h.t.Helper()
	select {
	case <-park.parked:
	case <-time.After(s3OCCReproWaitTimeout):
		h.t.Fatalf("handler never reached park %q", park.name)
	}
}

func (h *s3OCCReproHarness) waitDone(done <-chan *httptest.ResponseRecorder) *httptest.ResponseRecorder {
	h.t.Helper()
	select {
	case rec := <-done:
		return rec
	case <-time.After(s3OCCReproWaitTimeout):
		h.t.Fatal("parked handler did not finish after release")
		return nil
	}
}

func (h *s3OCCReproHarness) goHandle(fn func() *httptest.ResponseRecorder) <-chan *httptest.ResponseRecorder {
	done := make(chan *httptest.ResponseRecorder, 1)
	h.handlers.Add(1)
	go func() {
		defer h.handlers.Done()
		done <- fn()
	}()
	return done
}

func TestS3UploadPart_InlineInitialUploadUsesFreshDescriptorCommitTimestamp(t *testing.T) {
	t.Parallel()
	h := newS3OCCReproModeHarness(t, false, false)
	u := h.startUpload("occ-inline-initial")
	const body = "inline-payload"
	rec := h.uploadPart1(u, body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	part, found := h.part1Descriptor(u)
	require.True(t, found)
	require.False(t, part.Offloaded)
	req := h.coord.lastTxnWriting(t, u.partKey)
	require.Less(t, part.PartVersion, req.CommitTS, "immutable blob namespace must precede descriptor commit")
	raw, err := h.st.GetAt(context.Background(), u.chunkKey(0, s3ObjectPart(*part)), ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, body, string(raw))
	rec = h.completeWithPart1(u, strings.Trim(rec.Header().Get("ETag"), `"`))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	rec = h.do(http.MethodGet, u.objectPath(), "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, body, rec.Body.String())
}

// TestS3CompleteMultipartUpload_RacingPartReuploadKeepsReferencedChunks:
// CompleteMultipartUpload has read part 1's descriptor when a re-upload of part
// 1 with the same bytes (same ETag) commits a new part version and
// asynchronously deletes the old version's blobs. The re-upload writes only
// the part key and its chunks, none of which the completion writes, so
// nothing detects it unless the part key is in the completion's validated
// read set: the manifest then points at the superseded version whose chunks
// are gone. The re-upload lands either right after the completion read the
// descriptor or after its last read, between its read phase and its commit.
func TestS3CompleteMultipartUpload_RacingPartReuploadKeepsReferencedChunks(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		parkKey func(u s3OCCReproUpload) []byte
	}{
		{
			name:    "after the part descriptor read",
			parkKey: func(u s3OCCReproUpload) []byte { return u.partKey },
		},
		{
			name: "between the read phase and the commit",
			parkKey: func(u s3OCCReproUpload) []byte {
				return s3keys.ObjectManifestKey(u.bucket, u.generation, s3OCCReproObject)
			},
		},
	}
	for _, offloaded := range []bool{false, true} {
		for _, changedETag := range []bool{false, true} {
			for i, tc := range cases {
				t.Run(fmt.Sprintf("offloaded=%t/changed-etag=%t/%s", offloaded, changedETag, tc.name), func(t *testing.T) {
					t.Parallel()
					h := newS3OCCReproModeHarness(t, false, offloaded)
					u := h.startUpload(fmt.Sprintf("occ-complete-%d", i))
					parkKey := tc.parkKey(u)
					h.checkCompleteRacingPartReupload(t, u, changedETag, func(key []byte) bool { return bytes.Equal(key, parkKey) })
				})
			}
		}
	}
}

func (h *s3OCCReproHarness) checkCompleteRacingPartReupload(t *testing.T, u s3OCCReproUpload, changedETag bool, parkAt func(key []byte) bool) {
	t.Helper()
	const body = "part-one-payload"
	rec := h.uploadPart1(u, body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	etag := strings.Trim(rec.Header().Get("ETag"), `"`)
	first, ok := h.part1Descriptor(u)
	require.True(t, ok)

	park := h.reads.addPark("complete: "+t.Name(), parkAt)
	done := h.goHandle(func() *httptest.ResponseRecorder { return h.completeWithPart1(u, etag) })
	h.waitParked(park)

	reuploadedBody := body
	if changedETag {
		reuploadedBody = "changed-part-payload"
	}
	rec = h.uploadPart1(u, reuploadedBody)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	if changedETag {
		require.NotEqual(t, etag, strings.Trim(rec.Header().Get("ETag"), `"`))
	} else {
		require.Equal(t, etag, strings.Trim(rec.Header().Get("ETag"), `"`))
	}
	second, ok := h.part1Descriptor(u)
	require.True(t, ok)
	require.NotEqual(t, first.PartVersion, second.PartVersion)

	park.releaseNow()
	completeRec := h.waitDone(done)
	if changedETag {
		require.Equal(t, http.StatusBadRequest, completeRec.Code, completeRec.Body.String())
		require.Contains(t, completeRec.Body.String(), "<Code>InvalidPart</Code>")
		_, found, err := h.server.loadObjectManifestAt(context.Background(), s3keys.ObjectManifestKey(u.bucket, u.generation, s3OCCReproObject), ^uint64(0))
		require.NoError(t, err)
		require.False(t, found, "failed completion must not publish a stale manifest")
		return
	}
	require.Equal(t, http.StatusOK, completeRec.Code, completeRec.Body.String())
	h.waitCleanups()

	manifest, found, err := h.server.loadObjectManifestAt(context.Background(), s3keys.ObjectManifestKey(u.bucket, u.generation, s3OCCReproObject), ^uint64(0))
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, manifest.Parts, 1)
	part := manifest.Parts[0]
	assert.Equalf(t, second.PartVersion, part.PartVersion,
		"manifest references part version %d; the re-upload superseded it with %d (first upload was %d)",
		part.PartVersion, second.PartVersion, first.PartVersion)
	for chunk := range part.ChunkCount {
		_, err := h.st.GetAt(context.Background(), u.chunkKey(chunk, part), ^uint64(0))
		missing := errors.Is(err, store.ErrKeyNotFound)
		if !missing {
			require.NoError(t, err)
		}
		assert.Falsef(t, missing, "manifest chunk %d of part version %d is missing", chunk, part.PartVersion)
	}

	rec = h.do(http.MethodGet, u.objectPath(), "")
	assert.Equal(t, http.StatusOK, rec.Code, "GET of the completed object")
	assert.Equal(t, body, rec.Body.String(), "GET of the completed object")
}

// TestS3UploadPart_RacingDeleteBucketDoesNotLandUnderDeadGeneration:
// UploadPart has read the bucket meta when DeleteBucket commits. When the
// bucket-delete safety net sweeps the upload meta, the upload-meta read key
// already stops the part; when that best-effort sweep fails (the path
// runBucketDeleteSafetyNet logs and tolerates), only the bucket meta and the
// bucket generation key record the delete, so the part and its chunks must
// not commit under the deleted generation. The delete lands either before
// the part's StartTS (right after its prepare reads) or after its last
// commit-phase read.
func TestS3UploadPart_RacingDeleteBucketDoesNotLandUnderDeadGeneration(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		parkKey func(u s3OCCReproUpload) []byte
		// The re-read at the commit watermark sees a delete committed up to
		// it; a later one is a write conflict on the generation key.
		wantStatus int
		wantCode   string
		recreate   bool
	}{
		{
			name:       "delete between the prepare reads and StartTS",
			parkKey:    func(u s3OCCReproUpload) []byte { return u.uploadMetaKey },
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchBucket",
		},
		{
			name:       "delete after the commit-phase reads",
			parkKey:    func(u s3OCCReproUpload) []byte { return u.partKey },
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "SlowDown",
		},
		{
			name:       "recreate between the prepare reads and StartTS",
			parkKey:    func(u s3OCCReproUpload) []byte { return u.uploadMetaKey },
			wantStatus: http.StatusNotFound,
			wantCode:   "NoSuchUpload",
			recreate:   true,
		},
		{
			name:       "recreate after the commit-phase reads",
			parkKey:    func(u s3OCCReproUpload) []byte { return u.partKey },
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "SlowDown",
			recreate:   true,
		},
	}
	for _, offloaded := range []bool{false, true} {
		for i, tc := range cases {
			t.Run(fmt.Sprintf("offloaded=%t/%s", offloaded, tc.name), func(t *testing.T) {
				t.Parallel()
				h := newS3OCCReproModeHarness(t, false, offloaded)
				u := h.startUpload(fmt.Sprintf("occ-delete-bucket-%d", i))
				parkKey := tc.parkKey(u)
				partRec := h.uploadPart1Racing(u, "orphan-candidate", parkKey, func() {
					h.coord.failDelPrefix.Store(true)
					rec := h.do(http.MethodDelete, "/"+u.bucket, "")
					require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
					_, exists, err := h.server.loadBucketMetaAt(context.Background(), u.bucket, ^uint64(0))
					require.NoError(t, err)
					require.False(t, exists)
					if tc.recreate {
						rec = h.do(http.MethodPut, "/"+u.bucket, "")
						require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
						meta, exists, err := h.server.loadBucketMetaAt(context.Background(), u.bucket, ^uint64(0))
						require.NoError(t, err)
						require.True(t, exists)
						require.Greater(t, meta.Generation, u.generation)
					}
				})

				descriptor, landed := h.part1Descriptor(u)
				assert.Falsef(t, landed,
					"part descriptor (version %v) committed under deleted generation %d",
					descriptorVersion(descriptor), u.generation)
				assert.Empty(t, h.blobVersions(u), "part chunks leaked under deleted generation")
				assert.NotEqual(t, http.StatusOK, partRec.Code, "upload part must not succeed after the bucket was deleted")
				assert.Equal(t, tc.wantStatus, partRec.Code, partRec.Body.String())
				assert.Contains(t, partRec.Body.String(), "<Code>"+tc.wantCode+"</Code>")
			})
		}
	}
}

// TestS3UploadPart_ConcurrentObjectWriteInBucketDoesNotAbortPart: every
// object writer in a bucket re-puts the bucket meta as deleteBucket's
// emptiness fence. A part upload holds its StartTS across the whole body
// stream, so it must not fence on the bucket meta: an unrelated PutObject or
// CreateMultipartUpload in the same bucket must not abort it.
func TestS3UploadPart_ConcurrentObjectWriteInBucketDoesNotAbortPart(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		method string
		target func(u s3OCCReproUpload) string
	}{
		{
			name:   "PutObject of another key",
			method: http.MethodPut,
			target: func(u s3OCCReproUpload) string { return "/" + u.bucket + "/other" },
		},
		{
			name:   "CreateMultipartUpload of another key",
			method: http.MethodPost,
			target: func(u s3OCCReproUpload) string { return "/" + u.bucket + "/other?uploads=" },
		},
	}
	for _, offloaded := range []bool{false, true} {
		for i, tc := range cases {
			t.Run(fmt.Sprintf("offloaded=%t/%s", offloaded, tc.name), func(t *testing.T) {
				t.Parallel()
				h := newS3OCCReproModeHarness(t, false, offloaded)
				u := h.startUpload(fmt.Sprintf("occ-bucket-writer-%d", i))
				partRec := h.uploadPart1Racing(u, "part-body", u.partKey, func() {
					rec := h.do(tc.method, tc.target(u), "other-body")
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				})
				require.Equal(t, http.StatusOK, partRec.Code, partRec.Body.String())
				_, landed := h.part1Descriptor(u)
				require.True(t, landed)
			})
		}
	}
}

// uploadPart1Racing uploads part 1, holds it right after its GetAt of parkKey
// returned, runs concurrent, then releases it and waits for its response and
// for every cleanup it launched.
func (h *s3OCCReproHarness) uploadPart1Racing(u s3OCCReproUpload, body string, parkKey []byte, concurrent func()) *httptest.ResponseRecorder {
	h.t.Helper()
	park := h.reads.addPark("upload part: "+h.t.Name(), func(key []byte) bool {
		return bytes.Equal(key, parkKey)
	})
	done := h.goHandle(func() *httptest.ResponseRecorder { return h.uploadPart1(u, body) })
	h.waitParked(park)
	concurrent()
	park.releaseNow()
	rec := h.waitDone(done)
	h.waitCleanups()
	return rec
}

// TestS3UploadPart_SamePartUploadsInWindowCleanExactlySupersededBlobs: upload A
// of part 1 reads the previous descriptor (v0) at its prepare snapshot, then
// upload B of part 1 commits before A's StartTS. B supersedes v0 and cleans
// it; A supersedes B, so A must clean B's blobs. Reading the previous part at
// the prepare snapshot makes A clean v0 a second time and leak B.
func TestS3UploadPart_SamePartUploadsInWindowCleanExactlySupersededBlobs(t *testing.T) {
	t.Parallel()
	for _, offloaded := range []bool{false, true} {
		t.Run(fmt.Sprintf("offloaded=%t", offloaded), func(t *testing.T) {
			t.Parallel()
			h := newS3OCCReproModeHarness(t, false, offloaded)
			h.checkSamePartUploadsInWindow(t)
		})
	}
}

func (h *s3OCCReproHarness) checkSamePartUploadsInWindow(t *testing.T) {
	t.Helper()
	u := h.startUpload("occ-same-part")

	rec := h.uploadPart1(u, "version-zero")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	v0, ok := h.part1Descriptor(u)
	require.True(t, ok)
	h.waitCleanups()

	park := h.reads.addPark("upload A: prepare upload-meta read", func(key []byte) bool {
		return bytes.Equal(key, u.uploadMetaKey)
	})
	doneA := h.goHandle(func() *httptest.ResponseRecorder { return h.uploadPart1(u, "version-a") })
	h.waitParked(park)

	rec = h.uploadPart1(u, "version-b")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	vB, ok := h.part1Descriptor(u)
	require.True(t, ok)

	park.releaseNow()
	recA := h.waitDone(doneA)
	require.Equal(t, http.StatusOK, recA.Code, recA.Body.String())
	vA, ok := h.part1Descriptor(u)
	require.True(t, ok)
	h.waitCleanups()

	namespace := vA.PartVersion
	if vA.Offloaded {
		namespace = vA.ChunkRefVersion
	}
	assert.Equalf(t, map[uint64]int{namespace: 1}, h.blobVersions(u),
		"live descriptor is version %d (v0=%d, B=%d); every other version's blobs must be cleaned",
		vA.PartVersion, v0.PartVersion, vB.PartVersion)
	assert.Equal(t, 1, h.coord.delCount(u.chunkKey(0, s3ObjectPart(*v0))), "v0 blobs must be cleaned exactly once")
	assert.Equal(t, 1, h.coord.delCount(u.chunkKey(0, s3ObjectPart(*vB))), "B's blobs must be cleaned exactly once")
	assert.Zero(t, h.coord.delCount(u.chunkKey(0, s3ObjectPart(*vA))), "the live version's blobs must not be cleaned")

	rec = h.completeWithPart1(u, strings.Trim(recA.Header().Get("ETag"), `"`))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	rec = h.do(http.MethodGet, u.objectPath(), "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "version-a", rec.Body.String())
}

func descriptorVersion(descriptor *s3PartDescriptor) any {
	if descriptor == nil {
		return nil
	}
	return descriptor.PartVersion
}
