package adapter

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // S3 ETag compatibility requires MD5.
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/bootjp/elastickv/distribution"
	hashicorpraftengine "github.com/bootjp/elastickv/internal/raftengine/hashicorp"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	json "github.com/goccy/go-json"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
)

const (
	testS3AccessKey = "test-access"
	testS3SecretKey = "test-secret"
	testS3Region    = "us-east-1"
)

func TestS3Server_BucketAndObjectLifecycle(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPut, "/bucket-a", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	payload := "hello world"
	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodPut, "/bucket-a/dir/file.txt", strings.NewReader(payload))
	req.Header.Set("Content-Type", "text/plain")
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, `"`+md5Hex(payload)+`"`, rec.Header().Get("ETag"))

	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodHead, "/bucket-a", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodHead, "/bucket-a/dir/file.txt", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "11", rec.Header().Get("Content-Length"))
	require.Equal(t, `"`+md5Hex(payload)+`"`, rec.Header().Get("ETag"))

	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodGet, "/bucket-a/dir/file.txt", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, payload, rec.Body.String())

	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodGet, "/bucket-a?list-type=2", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "<Key>dir/file.txt</Key>")

	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodDelete, "/bucket-a/dir/file.txt", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodDelete, "/bucket-a", nil)
	server.handle(rec, req)
	require.Equal(t, http.StatusNoContent, rec.Code)
}

func TestS3Server_ProxiesFollowerRequests(t *testing.T) {
	t.Parallel()

	var proxied bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied = true
		_, _ = io.WriteString(w, "proxied")
	}))
	defer upstream.Close()

	targetHost := strings.TrimPrefix(upstream.URL, "http://")
	server := NewS3Server(nil, "", store.NewMVCCStore(), &followerS3Coordinator{}, map[raft.ServerAddress]string{
		raft.ServerAddress("leader"): targetHost,
	})

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/", nil)
	server.handle(rec, req)

	require.True(t, proxied)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "proxied", rec.Body.String())
}

func TestS3Server_RejectsUnsignedRequestWhenCredentialsConfigured(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/", nil)
	server.handle(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "<Code>AccessDenied</Code>")
}

func TestS3Server_AcceptsSigV4SignedRequests(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		WithS3Region(testS3Region),
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)
	signingTime := currentS3SigningTime()

	rec := httptest.NewRecorder()
	req := newSignedS3Request(t, "/bucket-a", "", signingTime)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	req = newSignedS3Request(t, "/bucket-a/dir/file.txt", "hello world", signingTime)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, `"`+md5Hex("hello world")+`"`, rec.Header().Get("ETag"))
}

func TestS3Server_RejectsPayloadHashMismatch(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		WithS3Region(testS3Region),
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)
	signingTime := currentS3SigningTime()

	rec := httptest.NewRecorder()
	req := newSignedS3Request(t, "/bucket-a", "", signingTime)
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	req = newSignedS3Request(t, "/bucket-a/object.txt", "hello", signingTime)
	req.Body = io.NopCloser(strings.NewReader("HELLO"))
	req.ContentLength = int64(len("HELLO"))
	server.handle(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "<Code>XAmzContentSHA256Mismatch</Code>")
}

func TestS3Server_RejectsSigV4RequestWithExcessiveClockSkew(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil,
		"",
		st,
		newLocalAdapterCoordinator(st),
		nil,
		WithS3Region(testS3Region),
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	rec := httptest.NewRecorder()
	req := newSignedS3Request(t, "/bucket-a", "", time.Now().UTC().Add(-s3RequestTimeMaxSkew-time.Hour))
	server.handle(rec, req)

	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "<Code>RequestTimeTooSkewed</Code>")
}

func TestS3Server_ProxiesFollowerRequestsBeforeAuth(t *testing.T) {
	t.Parallel()

	var proxied bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied = true
		_, _ = io.WriteString(w, "proxied")
	}))
	defer upstream.Close()

	targetHost := strings.TrimPrefix(upstream.URL, "http://")
	server := NewS3Server(
		nil,
		"",
		store.NewMVCCStore(),
		&followerS3Coordinator{},
		map[raft.ServerAddress]string{
			raft.ServerAddress("leader"): targetHost,
		},
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/", nil)
	server.handle(rec, req)

	require.True(t, proxied)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "proxied", rec.Body.String())
}

func TestS3Server_ProxiesObjectRequestsUsingObjectRouteLeader(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	metaBody, err := encodeS3BucketMeta(&s3BucketMeta{
		BucketName:   "bucket-a",
		Generation:   7,
		CreatedAtHLC: 1,
		Region:       "us-east-1",
	})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, s3keys.BucketMetaKey("bucket-a"), metaBody, 1, 0))

	var proxied bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxied = true
		_, _ = io.WriteString(w, "proxied-object")
	}))
	defer upstream.Close()

	coord := &routeAwareS3Coordinator{
		localForKey: func(key []byte) bool {
			return !bytes.HasPrefix(key, []byte(s3keys.RoutePrefix))
		},
		leaderForKey: func(key []byte) raft.ServerAddress {
			if bytes.HasPrefix(key, []byte(s3keys.RoutePrefix)) {
				return raft.ServerAddress("object-leader")
			}
			return ""
		},
	}
	server := NewS3Server(nil, "", st, coord, map[raft.ServerAddress]string{
		raft.ServerAddress("object-leader"): strings.TrimPrefix(upstream.URL, "http://"),
	})

	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/bucket-a/dir/file.txt", nil)
	server.handle(rec, req)

	require.True(t, proxied)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "proxied-object", rec.Body.String())
}

func TestS3Server_ListObjectsV2DelimiterContinuationAvoidsDuplicateCommonPrefixes(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	for _, objectKey := range []string{"dir-a/one.txt", "dir-a/two.txt", "dir-b/three.txt"} {
		rec = httptest.NewRecorder()
		server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/"+objectKey, strings.NewReader(objectKey)))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a?list-type=2&delimiter=%2F&max-keys=1", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	firstPage := decodeListBucketResult(t, rec.Body.Bytes())
	require.Len(t, firstPage.CommonPrefixes, 1)
	require.Equal(t, "dir-a/", firstPage.CommonPrefixes[0].Prefix)
	require.NotEmpty(t, firstPage.NextContinuationToken)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a?list-type=2&delimiter=%2F&max-keys=1&continuation-token="+firstPage.NextContinuationToken, nil))
	require.Equal(t, http.StatusOK, rec.Code)

	secondPage := decodeListBucketResult(t, rec.Body.Bytes())
	require.Len(t, secondPage.CommonPrefixes, 1)
	require.Equal(t, "dir-b/", secondPage.CommonPrefixes[0].Prefix)
}

func TestS3Server_PutObjectConflictsWhenBucketDeletedDuringFinalize(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := &bucketDeleteRaceCoordinator{
		localAdapterCoordinator: newLocalAdapterCoordinator(st),
		bucket:                  "bucket-a",
	}
	server := NewS3Server(nil, "", st, coord, nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/object.txt", strings.NewReader("payload")))
	require.Equal(t, http.StatusConflict, rec.Code)
	require.Contains(t, rec.Body.String(), "<Code>OperationAborted</Code>")

	readTS := snapshotTS(coord.Clock(), st)
	_, exists, err := server.loadBucketMetaAt(context.Background(), "bucket-a", readTS)
	require.NoError(t, err)
	require.False(t, exists)

	_, found, err := server.loadObjectManifestAt(context.Background(), s3keys.ObjectManifestKey("bucket-a", 1, "object.txt"), readTS)
	require.NoError(t, err)
	require.False(t, found)

	kvs, err := st.ScanAt(context.Background(), []byte(s3keys.BlobPrefix), prefixScanEnd([]byte(s3keys.BlobPrefix)), 10, readTS)
	require.NoError(t, err)
	require.Empty(t, kvs)
}

func TestS3Server_ShardedStoreRoutesBucketAndObjectData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(s3keys.RoutePrefix), []byte("!s3|"), 2)
	engine.UpdateRoute([]byte("!s3|"), nil, 1)

	store1 := store.NewMVCCStore()
	raft1, stop1 := newSingleRaftForS3Test(t, "g1", kv.NewKvFSM(store1))
	defer stop1()

	store2 := store.NewMVCCStore()
	raft2, stop2 := newSingleRaftForS3Test(t, "g2", kv.NewKvFSM(store2))
	defer stop2()

	engine1 := hashicorpraftengine.New(raft1)
	engine2 := hashicorpraftengine.New(raft2)
	groups := map[uint64]*kv.ShardGroup{
		1: {Engine: engine1, Store: store1, Txn: kv.NewLeaderProxyWithEngine(engine1)},
		2: {Engine: engine2, Store: store2, Txn: kv.NewLeaderProxyWithEngine(engine2)},
	}
	shardStore := kv.NewShardStore(engine, groups)
	coord := kv.NewShardedCoordinator(engine, groups, 1, kv.NewHLC(), shardStore)
	server := NewS3Server(nil, "", shardStore, coord, nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-a/dir/file.txt", strings.NewReader("payload")))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a/dir/file.txt", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "payload", rec.Body.String())

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-a?list-type=2", nil))
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "<Key>dir/file.txt</Key>")

	readTS := shardStore.LastCommitTS()
	var err error
	_, err = store1.GetAt(ctx, s3keys.BucketMetaKey("bucket-a"), readTS)
	require.NoError(t, err)

	_, err = store1.GetAt(ctx, s3keys.ObjectManifestKey("bucket-a", 1, "dir/file.txt"), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	_, err = store2.GetAt(ctx, s3keys.ObjectManifestKey("bucket-a", 1, "dir/file.txt"), readTS)
	require.NoError(t, err)
}

type followerS3Coordinator struct {
	stubAdapterCoordinator
}

func (c *followerS3Coordinator) Dispatch(context.Context, *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	return &kv.CoordinateResponse{}, nil
}

func (c *followerS3Coordinator) IsLeader() bool {
	return false
}

func (c *followerS3Coordinator) VerifyLeader() error {
	return kv.ErrLeaderNotFound
}

func (c *followerS3Coordinator) RaftLeader() raft.ServerAddress {
	return raft.ServerAddress("leader")
}

type routeAwareS3Coordinator struct {
	stubAdapterCoordinator
	localForKey  func([]byte) bool
	leaderForKey func([]byte) raft.ServerAddress
}

func (c *routeAwareS3Coordinator) IsLeaderForKey(key []byte) bool {
	if c.localForKey == nil {
		return true
	}
	return c.localForKey(key)
}

func (c *routeAwareS3Coordinator) VerifyLeaderForKey(key []byte) error {
	if c.IsLeaderForKey(key) {
		return nil
	}
	return kv.ErrLeaderNotFound
}

func (c *routeAwareS3Coordinator) RaftLeaderForKey(key []byte) raft.ServerAddress {
	if c.leaderForKey == nil {
		return ""
	}
	return c.leaderForKey(key)
}

type bucketDeleteRaceCoordinator struct {
	*localAdapterCoordinator
	bucket string
	once   sync.Once
}

//nolint:cyclop // This test coordinator intentionally injects a specific race and conflict path.
func (c *bucketDeleteRaceCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	if req != nil && req.IsTxn && containsObjectManifestMutation(req.Elems) {
		c.once.Do(func() {
			deleteTS := req.CommitTS + 1
			if deleteTS == 0 {
				deleteTS = req.StartTS + 1
			}
			_ = c.store.DeleteAt(ctx, s3keys.BucketMetaKey(c.bucket), deleteTS)
		})
	}
	if req != nil && req.IsTxn {
		for _, elem := range req.Elems {
			if elem == nil {
				continue
			}
			latestTS, exists, err := c.store.LatestCommitTS(ctx, elem.Key)
			if err != nil {
				return nil, err
			}
			if exists && latestTS > req.StartTS {
				return nil, store.NewWriteConflictError(elem.Key)
			}
		}
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

func containsObjectManifestMutation(elems []*kv.Elem[kv.OP]) bool {
	for _, elem := range elems {
		if elem == nil {
			continue
		}
		if _, _, _, ok := s3keys.ParseObjectManifestKey(elem.Key); ok {
			return true
		}
	}
	return false
}

func decodeListBucketResult(t *testing.T, body []byte) s3ListBucketResult {
	t.Helper()

	out := s3ListBucketResult{}
	require.NoError(t, xml.Unmarshal(body, &out))
	return out
}

func newSingleRaftForS3Test(t *testing.T, id string, fsm raft.FSM) (*raft.Raft, func()) {
	t.Helper()

	addr, trans := raft.NewInmemTransport(raft.ServerAddress(id))
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(id)
	cfg.HeartbeatTimeout = 50 * time.Millisecond
	cfg.ElectionTimeout = 100 * time.Millisecond
	cfg.LeaderLeaseTimeout = 50 * time.Millisecond

	ldb := raft.NewInmemStore()
	sdb := raft.NewInmemStore()
	fss := raft.NewInmemSnapshotStore()
	r, err := raft.NewRaft(cfg, fsm, ldb, sdb, fss, trans)
	require.NoError(t, err)
	require.NoError(t, r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{
			Suffrage: raft.Voter,
			ID:       raft.ServerID(id),
			Address:  addr,
		}},
	}).Error())

	for range 100 {
		if r.State() == raft.Leader {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, raft.Leader, r.State())
	return r, func() { _ = r.Shutdown().Error() }
}

func md5Hex(v string) string {
	sum := md5.Sum([]byte(v)) //nolint:gosec // S3 ETag compatibility requires MD5.
	return hex.EncodeToString(sum[:])
}

func sha256Hex(v string) string {
	sum := sha256.Sum256([]byte(v))
	return hex.EncodeToString(sum[:])
}

func newS3TestRequest(method string, target string, body io.Reader) *http.Request {
	return httptest.NewRequestWithContext(context.Background(), method, target, body)
}

func currentS3SigningTime() time.Time {
	return time.Now().UTC().Add(-time.Minute).Truncate(time.Second)
}

func newSignedS3Request(
	t *testing.T,
	target string,
	body string,
	signingTime time.Time,
) *http.Request {
	t.Helper()

	req := newS3TestRequest(http.MethodPut, target, strings.NewReader(body))
	payloadHash := sha256Hex(body)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	err := signer.SignHTTP(
		context.Background(),
		aws.Credentials{
			AccessKeyID:     testS3AccessKey,
			SecretAccessKey: testS3SecretKey,
			Source:          "test",
		},
		req,
		payloadHash,
		"s3",
		testS3Region,
		signingTime,
	)
	require.NoError(t, err)
	expectedAuth, err := buildS3AuthorizationHeader(req, testS3AccessKey, testS3SecretKey, testS3Region, signingTime, payloadHash)
	require.NoError(t, err)
	require.Equal(t, strings.TrimSpace(req.Header.Get("Authorization")), expectedAuth)
	return req
}

func resignS3Request(t *testing.T, req *http.Request, signingTime time.Time) *http.Request {
	t.Helper()

	payloadHash := sha256Hex("")
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	req.Header.Del("Authorization")
	req.Header.Del("X-Amz-Date")

	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	err := signer.SignHTTP(
		context.Background(),
		aws.Credentials{
			AccessKeyID:     testS3AccessKey,
			SecretAccessKey: testS3SecretKey,
			Source:          "test",
		},
		req,
		payloadHash,
		"s3",
		testS3Region,
		signingTime,
	)
	require.NoError(t, err)
	return req
}

// --- Phase 2 Tests ---

func TestS3Server_MultipartUploadHappyPath(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	// Create bucket.
	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-mp", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// CreateMultipartUpload.
	rec = httptest.NewRecorder()
	req := newS3TestRequest(http.MethodPost, "/bucket-mp/large-file.bin?uploads=", nil)
	req.Header.Set("Content-Type", "application/octet-stream")
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	require.Equal(t, "bucket-mp", initResult.Bucket)
	require.Equal(t, "large-file.bin", initResult.Key)
	require.NotEmpty(t, initResult.UploadId)
	uploadID := initResult.UploadId

	// UploadPart 1 (5 MiB).
	part1Data := strings.Repeat("A", 5*1024*1024)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-mp/large-file.bin?uploadId=%s&partNumber=1", uploadID),
		strings.NewReader(part1Data)))
	require.Equal(t, http.StatusOK, rec.Code)
	part1ETag := strings.Trim(rec.Header().Get("ETag"), `"`)
	require.Equal(t, md5Hex(part1Data), part1ETag)

	// UploadPart 2 (smaller, last part).
	part2Data := "final-chunk-data"
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-mp/large-file.bin?uploadId=%s&partNumber=2", uploadID),
		strings.NewReader(part2Data)))
	require.Equal(t, http.StatusOK, rec.Code)
	part2ETag := strings.Trim(rec.Header().Get("ETag"), `"`)
	require.Equal(t, md5Hex(part2Data), part2ETag)

	// CompleteMultipartUpload.
	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>"%s"</ETag></Part>
	</CompleteMultipartUpload>`, part1ETag, part2ETag)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		fmt.Sprintf("/bucket-mp/large-file.bin?uploadId=%s", uploadID),
		strings.NewReader(completeBody)))
	require.Equal(t, http.StatusOK, rec.Code)

	var completeResult s3CompleteMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &completeResult))
	require.Equal(t, "bucket-mp", completeResult.Bucket)
	require.Equal(t, "large-file.bin", completeResult.Key)
	// Verify composite ETag format: hex-2
	require.Contains(t, completeResult.ETag, "-2")

	// Verify GetObject returns complete data.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-mp/large-file.bin", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, part1Data+part2Data, rec.Body.String())

	// HeadObject shows correct size.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodHead, "/bucket-mp/large-file.bin", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	expectedSize := fmt.Sprintf("%d", len(part1Data)+len(part2Data))
	require.Equal(t, expectedSize, rec.Header().Get("Content-Length"))

	// ListObjectsV2 includes the multipart object.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-mp?list-type=2", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "<Key>large-file.bin</Key>")
}

func TestS3Server_AbortMultipartUpload(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-abort", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// Create upload.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-abort/file.bin?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// Upload a part.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-abort/file.bin?uploadId=%s&partNumber=1", uploadID),
		strings.NewReader("some data")))
	require.Equal(t, http.StatusOK, rec.Code)

	// Abort.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodDelete,
		fmt.Sprintf("/bucket-abort/file.bin?uploadId=%s", uploadID),
		nil))
	require.Equal(t, http.StatusNoContent, rec.Code)

	// Re-abort should fail with NoSuchUpload.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodDelete,
		fmt.Sprintf("/bucket-abort/file.bin?uploadId=%s", uploadID),
		nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "NoSuchUpload")

	// Object should not be visible.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-abort/file.bin", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
}

func TestS3Server_ListParts(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-lp", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// Create upload.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-lp/obj.bin?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// Upload parts 1, 3, 5 (non-contiguous).
	for _, pn := range []int{1, 3, 5} {
		data := fmt.Sprintf("part-%d-data", pn)
		rec = httptest.NewRecorder()
		server.handle(rec, newS3TestRequest(http.MethodPut,
			fmt.Sprintf("/bucket-lp/obj.bin?uploadId=%s&partNumber=%d", uploadID, pn),
			strings.NewReader(data)))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// ListParts.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet,
		fmt.Sprintf("/bucket-lp/obj.bin?uploadId=%s", uploadID), nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var listResult s3ListPartsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &listResult))
	require.Len(t, listResult.Parts, 3)
	require.Equal(t, 1, listResult.Parts[0].PartNumber)
	require.Equal(t, 3, listResult.Parts[1].PartNumber)
	require.Equal(t, 5, listResult.Parts[2].PartNumber)

	// ListParts with pagination.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet,
		fmt.Sprintf("/bucket-lp/obj.bin?uploadId=%s&max-parts=2", uploadID), nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var page1Result s3ListPartsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &page1Result))
	require.Len(t, page1Result.Parts, 2)
	require.True(t, page1Result.IsTruncated)
	require.Equal(t, 3, page1Result.NextPartNumberMarker)

	// Continue from marker.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet,
		fmt.Sprintf("/bucket-lp/obj.bin?uploadId=%s&max-parts=2&part-number-marker=3", uploadID), nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var page2Result s3ListPartsResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &page2Result))
	require.Len(t, page2Result.Parts, 1)
	require.False(t, page2Result.IsTruncated)
	require.Equal(t, 5, page2Result.Parts[0].PartNumber)
}

func TestS3Server_RangeReadFullAndPartial(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-range", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	payload := "0123456789ABCDEF"
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-range/file.txt", strings.NewReader(payload)))
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		name       string
		rangeHdr   string
		wantStatus int
		wantBody   string
		wantCR     string // Content-Range
	}{
		{"first 5 bytes", "bytes=0-4", 206, "01234", "bytes 0-4/16"},
		{"middle range", "bytes=5-9", 206, "56789", "bytes 5-9/16"},
		{"open-ended", "bytes=10-", 206, "ABCDEF", "bytes 10-15/16"},
		{"suffix range", "bytes=-4", 206, "CDEF", "bytes 12-15/16"},
		{"full range", "bytes=0-15", 206, payload, "bytes 0-15/16"},
		{"beyond end clamped", "bytes=0-99", 206, payload, "bytes 0-15/16"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rr := httptest.NewRecorder()
			req := newS3TestRequest(http.MethodGet, "/bucket-range/file.txt", nil)
			req.Header.Set("Range", tc.rangeHdr)
			server.handle(rr, req)
			require.Equal(t, tc.wantStatus, rr.Code)
			require.Equal(t, tc.wantBody, rr.Body.String())
			require.Equal(t, tc.wantCR, rr.Header().Get("Content-Range"))
		})
	}
}

func TestS3Server_RangeReadInvalidRange(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-rinv", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-rinv/f.txt", strings.NewReader("hello")))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/bucket-rinv/f.txt", nil)
	req.Header.Set("Range", "bytes=99-100")
	server.handle(rec, req)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, rec.Code)
}

func TestS3Server_RangeReadEmptyObject(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-empty-range", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// Upload an empty object.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-empty-range/empty.txt", strings.NewReader("")))
	require.Equal(t, http.StatusOK, rec.Code)

	// Suffix range on empty object must return 416.
	rec = httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/bucket-empty-range/empty.txt", nil)
	req.Header.Set("Range", "bytes=-4")
	server.handle(rec, req)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, rec.Code)
}

func TestS3Server_HeadWithRange(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-head-range", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	payload := "hello ranged head"
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-head-range/obj.txt", strings.NewReader(payload)))
	require.Equal(t, http.StatusOK, rec.Code)

	// HEAD with valid Range: expect 206 + Content-Range, no body.
	rec = httptest.NewRecorder()
	req := newS3TestRequest(http.MethodHead, "/bucket-head-range/obj.txt", nil)
	req.Header.Set("Range", "bytes=0-4")
	server.handle(rec, req)
	require.Equal(t, http.StatusPartialContent, rec.Code)
	require.Equal(t, "bytes 0-4/17", rec.Header().Get("Content-Range"))
	require.Equal(t, "5", rec.Header().Get("Content-Length"))
	require.Empty(t, rec.Body.String())

	// HEAD with out-of-range Range: expect 416 + Content-Range header.
	rec = httptest.NewRecorder()
	req = newS3TestRequest(http.MethodHead, "/bucket-head-range/obj.txt", nil)
	req.Header.Set("Range", "bytes=999-1000")
	server.handle(rec, req)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, rec.Code)
	require.Equal(t, "bytes */17", rec.Header().Get("Content-Range"))
	require.Empty(t, rec.Body.String(), "HEAD response body must be empty")
}

func TestS3Server_InvalidRangeContentRangeHeader(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-inv-range", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	payload := "abcdefghij" // 10 bytes
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-inv-range/obj.txt", strings.NewReader(payload)))
	require.Equal(t, http.StatusOK, rec.Code)

	// GET with out-of-range Range: 416 response must include Content-Range header.
	rec = httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/bucket-inv-range/obj.txt", nil)
	req.Header.Set("Range", "bytes=100-200")
	server.handle(rec, req)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, rec.Code)
	require.Equal(t, "bytes */10", rec.Header().Get("Content-Range"))
}

func TestS3Server_PresignedURLMissingExpires(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3Region(testS3Region),
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	// Build a presigned URL without X-Amz-Expires: server must reject it.
	signingTime := currentS3SigningTime()
	presignReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/bucket/obj.txt", nil)
	require.NoError(t, err)
	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{AccessKeyID: testS3AccessKey, SecretAccessKey: testS3SecretKey, Source: "test"}
	presignedURL, _, err := signer.PresignHTTP(context.Background(), creds, presignReq,
		s3UnsignedPayload, "s3", testS3Region, signingTime)
	require.NoError(t, err)

	parsedURL, err := url.Parse(presignedURL)
	require.NoError(t, err)
	presignGetReq := newS3TestRequest(http.MethodGet, parsedURL.RequestURI(), nil)
	presignGetReq.Host = "localhost"
	rec := httptest.NewRecorder()
	server.handle(rec, presignGetReq)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "AuthorizationQueryParametersError")
	require.Contains(t, rec.Body.String(), "X-Amz-Expires")
}

func TestS3Server_MultipartUploadETagComputation(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-etag", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-etag/obj?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// Upload 3 parts (5MiB+5MiB+small).
	partData := make([]string, 3)
	partETags := make([]string, 3)
	partData[0] = strings.Repeat("X", 5*1024*1024)
	partData[1] = strings.Repeat("Y", 5*1024*1024)
	partData[2] = "final"
	for i, data := range partData {
		rec = httptest.NewRecorder()
		server.handle(rec, newS3TestRequest(http.MethodPut,
			fmt.Sprintf("/bucket-etag/obj?uploadId=%s&partNumber=%d", uploadID, i+1),
			strings.NewReader(data)))
		require.Equal(t, http.StatusOK, rec.Code)
		partETags[i] = strings.Trim(rec.Header().Get("ETag"), `"`)
	}

	// Complete.
	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>"%s"</ETag></Part>
		<Part><PartNumber>3</PartNumber><ETag>"%s"</ETag></Part>
	</CompleteMultipartUpload>`, partETags[0], partETags[1], partETags[2])
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		fmt.Sprintf("/bucket-etag/obj?uploadId=%s", uploadID),
		strings.NewReader(completeBody)))
	require.Equal(t, http.StatusOK, rec.Code)

	var completeResult s3CompleteMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &completeResult))

	// Compute expected composite ETag.
	concatMD5 := md5.New() //nolint:gosec
	for _, etag := range partETags {
		raw, _ := hex.DecodeString(etag)
		_, _ = concatMD5.Write(raw)
	}
	expectedETag := fmt.Sprintf(`"%s-3"`, hex.EncodeToString(concatMD5.Sum(nil)))
	require.Equal(t, expectedETag, completeResult.ETag)
}

func TestS3Server_MultipartUploadRejectsInvalidPartOrder(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-order", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-order/obj?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// Upload parts.
	for _, pn := range []int{1, 2} {
		rec = httptest.NewRecorder()
		server.handle(rec, newS3TestRequest(http.MethodPut,
			fmt.Sprintf("/bucket-order/obj?uploadId=%s&partNumber=%d", uploadID, pn),
			strings.NewReader(strings.Repeat("x", 5*1024*1024))))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// Complete with wrong order.
	completeBody := `<CompleteMultipartUpload>
		<Part><PartNumber>2</PartNumber><ETag>"x"</ETag></Part>
		<Part><PartNumber>1</PartNumber><ETag>"y"</ETag></Part>
	</CompleteMultipartUpload>`
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		fmt.Sprintf("/bucket-order/obj?uploadId=%s", uploadID),
		strings.NewReader(completeBody)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "InvalidPartOrder")
}

func TestS3Server_CompleteMultipartUploadTooManyParts(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-toomany", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-toomany/obj?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// Build a CompleteMultipartUpload request with too many parts (> 10000).
	var sb strings.Builder
	sb.WriteString("<CompleteMultipartUpload>")
	for i := 1; i <= s3MaxPartsPerUpload+1; i++ {
		fmt.Fprintf(&sb, "<Part><PartNumber>%d</PartNumber><ETag>\"abc\"</ETag></Part>", i)
	}
	sb.WriteString("</CompleteMultipartUpload>")

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		fmt.Sprintf("/bucket-toomany/obj?uploadId=%s", uploadID),
		strings.NewReader(sb.String())))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "InvalidArgument")
}

func TestS3Server_CompleteMultipartUploadOutOfRangePartNumber(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-partrange", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-partrange/obj?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// Use part number 0 (below s3MinPartNumber=1).
	completeBody := `<CompleteMultipartUpload>
		<Part><PartNumber>0</PartNumber><ETag>"abc"</ETag></Part>
	</CompleteMultipartUpload>`
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		fmt.Sprintf("/bucket-partrange/obj?uploadId=%s", uploadID),
		strings.NewReader(completeBody)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "InvalidArgument")
}

func TestS3Server_MultipartNoSuchUpload(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-nosu", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// UploadPart to nonexistent upload.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		"/bucket-nosu/obj?uploadId=nonexistent&partNumber=1",
		strings.NewReader("data")))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "NoSuchUpload")

	// Complete nonexistent upload.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		"/bucket-nosu/obj?uploadId=nonexistent",
		strings.NewReader(`<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>"x"</ETag></Part></CompleteMultipartUpload>`)))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "NoSuchUpload")

	// ListParts nonexistent upload.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet,
		"/bucket-nosu/obj?uploadId=nonexistent", nil))
	require.Equal(t, http.StatusNotFound, rec.Code)
	require.Contains(t, rec.Body.String(), "NoSuchUpload")
}

func TestS3Server_PresignedURL(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3Region(testS3Region),
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	// Create a bucket using a signed request.
	signingTime := currentS3SigningTime()
	rec := httptest.NewRecorder()
	server.handle(rec, newSignedS3Request(t, "/bucket-presign", "", signingTime))
	require.Equal(t, http.StatusOK, rec.Code)

	// PUT object with a signed request.
	rec = httptest.NewRecorder()
	server.handle(rec, newSignedS3Request(t, "/bucket-presign/obj.txt", "hello presign", signingTime))
	require.Equal(t, http.StatusOK, rec.Code)

	// Build a presigned GET URL using a proper absolute URL.
	presignReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/bucket-presign/obj.txt", nil)
	require.NoError(t, err)
	// Set X-Amz-Expires to satisfy the server's presigned URL requirement (900 s validity).
	presignQuery := presignReq.URL.Query()
	presignQuery.Set("X-Amz-Expires", "900")
	presignReq.URL.RawQuery = presignQuery.Encode()
	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{
		AccessKeyID:     testS3AccessKey,
		SecretAccessKey: testS3SecretKey,
		Source:          "test",
	}
	presignedURL, _, err := signer.PresignHTTP(
		context.Background(),
		creds,
		presignReq,
		s3UnsignedPayload,
		"s3",
		testS3Region,
		signingTime,
	)
	require.NoError(t, err)

	// Make the presigned request.
	parsedURL, err := url.Parse(presignedURL)
	require.NoError(t, err)
	presignGetReq := newS3TestRequest(http.MethodGet, parsedURL.RequestURI(), nil)
	presignGetReq.Host = "localhost"
	rec = httptest.NewRecorder()
	server.handle(rec, presignGetReq)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "hello presign", rec.Body.String())
}

func TestS3Server_PresignedURLExpired(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3Region(testS3Region),
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	// Build a presigned URL with a 60 s expiry that's already expired.
	// Signing 20 minutes ago means expiry was ~19 minutes ago.
	oldTime := time.Now().UTC().Add(-20 * time.Minute)
	presignReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/bucket-presign/obj.txt", nil)
	require.NoError(t, err)
	// Set a 60 s expiry that will have elapsed given the signing time of 20 minutes ago,
	// ensuring the presigned URL is expired when the server validates it.
	presignQuery := presignReq.URL.Query()
	presignQuery.Set("X-Amz-Expires", "60")
	presignReq.URL.RawQuery = presignQuery.Encode()
	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	creds := aws.Credentials{
		AccessKeyID:     testS3AccessKey,
		SecretAccessKey: testS3SecretKey,
		Source:          "test",
	}
	presignedURL, _, err := signer.PresignHTTP(
		context.Background(),
		creds,
		presignReq,
		s3UnsignedPayload,
		"s3",
		testS3Region,
		oldTime,
	)
	require.NoError(t, err)

	parsedURL, err := url.Parse(presignedURL)
	require.NoError(t, err)
	presignGetReq := newS3TestRequest(http.MethodGet, parsedURL.RequestURI(), nil)
	presignGetReq.Host = "localhost"
	rec := httptest.NewRecorder()
	server.handle(rec, presignGetReq)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "expired")
}

func TestS3Server_RangeReadAcrossMultipleChunks(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-rmc", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// Create a payload larger than one chunk (s3ChunkSize=1MiB), use 2MiB+1.
	payload := strings.Repeat("Z", 2*1024*1024+1)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-rmc/big.bin", strings.NewReader(payload)))
	require.Equal(t, http.StatusOK, rec.Code)

	// Range read spanning chunk boundary.
	rec = httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/bucket-rmc/big.bin", nil)
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", 1024*1024-5, 1024*1024+5))
	server.handle(rec, req)
	require.Equal(t, http.StatusPartialContent, rec.Code)
	require.Equal(t, payload[1024*1024-5:1024*1024+6], rec.Body.String())
}

func TestS3Server_MultipartUploadPartOverwrite(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-overwrite", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-overwrite/obj?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// Upload part 1 twice.
	data1 := strings.Repeat("A", 5*1024*1024)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-overwrite/obj?uploadId=%s&partNumber=1", uploadID),
		strings.NewReader(data1)))
	require.Equal(t, http.StatusOK, rec.Code)

	data2 := strings.Repeat("B", 5*1024*1024)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-overwrite/obj?uploadId=%s&partNumber=1", uploadID),
		strings.NewReader(data2)))
	require.Equal(t, http.StatusOK, rec.Code)
	overwriteETag := strings.Trim(rec.Header().Get("ETag"), `"`)

	// The latest part 1 should be used in complete.
	lastPart := "end"
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-overwrite/obj?uploadId=%s&partNumber=2", uploadID),
		strings.NewReader(lastPart)))
	require.Equal(t, http.StatusOK, rec.Code)
	lastETag := strings.Trim(rec.Header().Get("ETag"), `"`)

	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>"%s"</ETag></Part>
	</CompleteMultipartUpload>`, overwriteETag, lastETag)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		fmt.Sprintf("/bucket-overwrite/obj?uploadId=%s", uploadID),
		strings.NewReader(completeBody)))
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the overwritten data is used.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodGet, "/bucket-overwrite/obj", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, data2+lastPart, rec.Body.String())
}

func TestExtractS3Signature(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "standard SigV4 header",
			input: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abc123def456",
			want:  "abc123def456",
		},
		{
			name:  "signature with extra whitespace",
			input: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature= abc123 ",
			want:  "abc123",
		},
		{
			name:  "different parameter order",
			input: "AWS4-HMAC-SHA256 SignedHeaders=host;x-amz-date, Credential=AKID/20240101/us-east-1/s3/aws4_request, Signature=deadbeef",
			want:  "deadbeef",
		},
		{
			name:  "missing signature",
			input: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, SignedHeaders=host",
			want:  "",
		},
		{
			name:  "empty header",
			input: "",
			want:  "",
		},
		{
			name:  "no space separator",
			input: "AWS4-HMAC-SHA256Credential=foo",
			want:  "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractS3Signature(tc.input)
			if got != tc.want {
				t.Errorf("extractS3Signature(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestS3Server_CompleteMultipartUploadETagMismatch(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-etag-mm", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// CreateMultipartUpload.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-etag-mm/obj?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// UploadPart 1.
	partData := strings.Repeat("X", 5*1024*1024)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-etag-mm/obj?uploadId=%s&partNumber=1", uploadID),
		strings.NewReader(partData)))
	require.Equal(t, http.StatusOK, rec.Code)

	// Complete with wrong ETag.
	completeBody := `<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>"0000000000000000deadbeef00000000"</ETag></Part>
	</CompleteMultipartUpload>`
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		fmt.Sprintf("/bucket-etag-mm/obj?uploadId=%s", uploadID),
		strings.NewReader(completeBody)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "InvalidPart")
	require.Contains(t, rec.Body.String(), "ETag mismatch")
}

func TestS3Server_CompleteMultipartUploadEntityTooSmall(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(nil, "", st, newLocalAdapterCoordinator(st), nil)

	rec := httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut, "/bucket-small", nil))
	require.Equal(t, http.StatusOK, rec.Code)

	// CreateMultipartUpload.
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost, "/bucket-small/obj?uploads=", nil))
	require.Equal(t, http.StatusOK, rec.Code)
	var initResult s3InitiateMultipartUploadResult
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &initResult))
	uploadID := initResult.UploadId

	// UploadPart 1 — only 100 bytes (too small for non-last part).
	part1Data := strings.Repeat("A", 100)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-small/obj?uploadId=%s&partNumber=1", uploadID),
		strings.NewReader(part1Data)))
	require.Equal(t, http.StatusOK, rec.Code)
	part1ETag := strings.Trim(rec.Header().Get("ETag"), `"`)

	// UploadPart 2 — last part, small is OK.
	part2Data := "end"
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPut,
		fmt.Sprintf("/bucket-small/obj?uploadId=%s&partNumber=2", uploadID),
		strings.NewReader(part2Data)))
	require.Equal(t, http.StatusOK, rec.Code)
	part2ETag := strings.Trim(rec.Header().Get("ETag"), `"`)

	// Complete — part 1 is not the last and is < 5 MiB.
	completeBody := fmt.Sprintf(`<CompleteMultipartUpload>
		<Part><PartNumber>1</PartNumber><ETag>"%s"</ETag></Part>
		<Part><PartNumber>2</PartNumber><ETag>"%s"</ETag></Part>
	</CompleteMultipartUpload>`, part1ETag, part2ETag)
	rec = httptest.NewRecorder()
	server.handle(rec, newS3TestRequest(http.MethodPost,
		fmt.Sprintf("/bucket-small/obj?uploadId=%s", uploadID),
		strings.NewReader(completeBody)))
	require.Equal(t, http.StatusBadRequest, rec.Code)
	require.Contains(t, rec.Body.String(), "EntityTooSmall")
}

func TestS3Server_PresignedURLWrongCredentials(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3Region(testS3Region),
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	signingTime := currentS3SigningTime()

	// Build a presigned URL with unknown access key.
	presignReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://localhost/bucket-presign/obj.txt", nil)
	require.NoError(t, err)
	signer := v4.NewSigner(func(opts *v4.SignerOptions) {
		opts.DisableURIPathEscaping = true
	})
	wrongCreds := aws.Credentials{
		AccessKeyID:     "unknown-access-key",
		SecretAccessKey: "wrong-secret",
		Source:          "test",
	}
	presignedURL, _, err := signer.PresignHTTP(
		context.Background(),
		wrongCreds,
		presignReq,
		s3UnsignedPayload,
		"s3",
		testS3Region,
		signingTime,
	)
	require.NoError(t, err)

	parsedURL, err := url.Parse(presignedURL)
	require.NoError(t, err)
	presignGetReq := newS3TestRequest(http.MethodGet, parsedURL.RequestURI(), nil)
	presignGetReq.Host = "localhost"
	rec := httptest.NewRecorder()
	server.handle(rec, presignGetReq)
	require.Equal(t, http.StatusForbidden, rec.Code)
	require.Contains(t, rec.Body.String(), "InvalidAccessKeyId")
}

// --- Public Bucket ACL Tests ---

func TestS3Server_PublicBucket_AnonymousGetObject(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	// Create bucket with public-read ACL (signed).
	sigTime := currentS3SigningTime()
	req := newSignedS3Request(t, "/pub-bucket", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "public-read")
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "create bucket: %s", rec.Body.String())

	// Upload object (signed).
	payload := "public-data"
	req = newSignedS3Request(t, "/pub-bucket/file.txt", payload, sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "put object: %s", rec.Body.String())

	// Anonymous GET (no auth header).
	req = newS3TestRequest(http.MethodGet, "/pub-bucket/file.txt", nil)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "anonymous get: %s", rec.Body.String())
	require.Equal(t, payload, rec.Body.String())
}

func TestS3Server_PublicBucket_AnonymousPutRejected(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	// Create public bucket.
	sigTime := currentS3SigningTime()
	req := newSignedS3Request(t, "/pub-bucket-wr", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "public-read")
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Anonymous PUT must be rejected.
	req = newS3TestRequest(http.MethodPut, "/pub-bucket-wr/file.txt", strings.NewReader("evil"))
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestS3Server_PublicBucket_AnonymousListObjectsV2(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	// Create public bucket.
	req := newSignedS3Request(t, "/pub-list", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "public-read")
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Upload object.
	req = newSignedS3Request(t, "/pub-list/obj.txt", "data", sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Anonymous ListObjectsV2.
	req = newS3TestRequest(http.MethodGet, "/pub-list?list-type=2", nil)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "<Key>obj.txt</Key>")
}

func TestS3Server_PrivateBucket_AnonymousGetRejected(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	// Create private bucket (default ACL).
	req := newSignedS3Request(t, "/priv-bucket", "", sigTime)
	req.Method = http.MethodPut
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Upload object.
	req = newSignedS3Request(t, "/priv-bucket/secret.txt", "secret", sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Anonymous GET rejected.
	req = newS3TestRequest(http.MethodGet, "/priv-bucket/secret.txt", nil)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestS3Server_PutBucketAcl_ChangeToPublicAndBack(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	// Create private bucket.
	req := newSignedS3Request(t, "/acl-bucket", "", sigTime)
	req.Method = http.MethodPut
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Upload object.
	req = newSignedS3Request(t, "/acl-bucket/file.txt", "hello", sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Anonymous GET should fail (private).
	req = newS3TestRequest(http.MethodGet, "/acl-bucket/file.txt", nil)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)

	// PutBucketAcl → public-read.
	req = newSignedS3Request(t, "/acl-bucket?acl", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "public-read")
	req = resignS3Request(t, req, sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, "put acl: %s", rec.Body.String())

	// Anonymous GET should now succeed.
	req = newS3TestRequest(http.MethodGet, "/acl-bucket/file.txt", nil)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "hello", rec.Body.String())

	// PutBucketAcl → private.
	req = newSignedS3Request(t, "/acl-bucket?acl", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "private")
	req = resignS3Request(t, req, sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Anonymous GET should fail again.
	req = newS3TestRequest(http.MethodGet, "/acl-bucket/file.txt", nil)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestS3Server_GetBucketAcl_Private(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	req := newSignedS3Request(t, "/acl-get-priv", "", sigTime)
	req.Method = http.MethodPut
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// GetBucketAcl on private bucket.
	req = newSignedS3Request(t, "/acl-get-priv?acl", "", sigTime)
	req.Method = http.MethodGet
	req = resignS3Request(t, req, sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "FULL_CONTROL")
	require.NotContains(t, body, "AllUsers")
}

func TestS3Server_GetBucketAcl_PublicRead(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	req := newSignedS3Request(t, "/acl-get-pub", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "public-read")
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// GetBucketAcl on public bucket.
	req = newSignedS3Request(t, "/acl-get-pub?acl", "", sigTime)
	req.Method = http.MethodGet
	req = resignS3Request(t, req, sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "FULL_CONTROL")
	require.Contains(t, body, "AllUsers")
	require.Contains(t, body, "READ")
}

func TestS3Server_CreateBucketWithUnsupportedAcl(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	req := newSignedS3Request(t, "/unsupported-acl", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "public-read-write")
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestS3Server_PublicBucket_AnonymousHeadObject(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	req := newSignedS3Request(t, "/pub-head", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "public-read")
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Upload object.
	req = newSignedS3Request(t, "/pub-head/file.txt", "head-test", sigTime)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Anonymous HEAD.
	req = newS3TestRequest(http.MethodHead, "/pub-head/file.txt", nil)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)
	require.Equal(t, "9", rec.Header().Get("Content-Length"))
}

func TestS3Server_PublicBucket_AnonymousGetBucketAclRejected(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	req := newSignedS3Request(t, "/pub-acl-reject", "", sigTime)
	req.Method = http.MethodPut
	req.Header.Set("x-amz-acl", "public-read")
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// Anonymous GetBucketAcl must be rejected even on public bucket.
	req = newS3TestRequest(http.MethodGet, "/pub-acl-reject?acl", nil)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestS3Server_BackwardCompatibility_NoBucketAclFieldIsPrivate(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)

	// Write legacy bucket metadata as JSON directly into the store without the
	// "acl" field, simulating a bucket created before the ACL feature was introduced.
	type legacyBucketMeta struct {
		BucketName   string `json:"bucket_name"`
		Generation   uint64 `json:"generation"`
		CreatedAtHLC uint64 `json:"created_at_hlc"`
		Owner        string `json:"owner,omitempty"`
		Region       string `json:"region,omitempty"`
		// intentionally omits Acl to replicate the pre-ACL schema
	}
	legacyMeta := legacyBucketMeta{
		BucketName:   "legacy-bucket",
		Generation:   1,
		CreatedAtHLC: 1,
	}
	legacyJSON, err := json.Marshal(legacyMeta)
	require.NoError(t, err)
	commitTS := coord.Clock().Next()
	err = st.ApplyMutations(context.Background(), []*store.KVPairMutation{
		{Op: store.OpTypePut, Key: s3keys.BucketMetaKey("legacy-bucket"), Value: legacyJSON},
	}, commitTS-1, commitTS)
	require.NoError(t, err)

	// Create a server WITH credentials; the legacy bucket has no acl field.
	authServer := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	// Anonymous GET should fail on legacy bucket (empty acl field treated as private).
	rec := httptest.NewRecorder()
	req := newS3TestRequest(http.MethodGet, "/legacy-bucket/file.txt", nil)
	authServer.handle(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code)
}

func TestS3Server_PutBucketAcl_RejectsXMLBody(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	server := NewS3Server(
		nil, "", st, newLocalAdapterCoordinator(st), nil,
		WithS3StaticCredentials(map[string]string{testS3AccessKey: testS3SecretKey}),
	)

	sigTime := currentS3SigningTime()
	// Create bucket first.
	req := newSignedS3Request(t, "/xml-acl-bucket", "", sigTime)
	req.Method = http.MethodPut
	req = resignS3Request(t, req, sigTime)
	rec := httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusOK, rec.Code)

	// PutBucketAcl with XML body and explicit ContentLength — must be rejected.
	xmlBody := `<AccessControlPolicy><Owner><ID>owner</ID></Owner></AccessControlPolicy>`
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/xml-acl-bucket?acl", strings.NewReader(xmlBody))
	req.ContentLength = int64(len(xmlBody))
	payloadHash := sha256Hex(xmlBody)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signer := v4.NewSigner(func(opts *v4.SignerOptions) { opts.DisableURIPathEscaping = true })
	err := signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID: testS3AccessKey, SecretAccessKey: testS3SecretKey, Source: "test",
	}, req, payloadHash, "s3", testS3Region, sigTime)
	require.NoError(t, err)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusNotImplemented, rec.Code)

	// PutBucketAcl with chunked body (ContentLength == -1) — must also be rejected.
	req = httptest.NewRequestWithContext(context.Background(), http.MethodPut, "/xml-acl-bucket?acl", strings.NewReader(xmlBody))
	req.ContentLength = -1 // simulate chunked transfer encoding
	payloadHash = sha256Hex(xmlBody)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)
	signer = v4.NewSigner(func(opts *v4.SignerOptions) { opts.DisableURIPathEscaping = true })
	err = signer.SignHTTP(context.Background(), aws.Credentials{
		AccessKeyID: testS3AccessKey, SecretAccessKey: testS3SecretKey, Source: "test",
	}, req, payloadHash, "s3", testS3Region, sigTime)
	require.NoError(t, err)
	rec = httptest.NewRecorder()
	server.handle(rec, req)
	require.Equal(t, http.StatusNotImplemented, rec.Code)
}

func TestIsReadOnlyS3Request(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		method string
		path   string
		want   bool
	}{
		// Allowed: object-level GET/HEAD with no query params.
		{"GetObject", http.MethodGet, "/bucket/key.txt", true},
		{"HeadObject", http.MethodHead, "/bucket/key.txt", true},
		// Allowed: HeadBucket with no query params.
		{"HeadBucket", http.MethodHead, "/bucket", true},
		// Allowed: ListObjectsV2.
		{"ListObjectsV2", http.MethodGet, "/bucket?list-type=2", true},
		// Not allowed: bucket GET without list-type=2 (returns NotImplemented later, not via anonymous path).
		{"GetBucket_NoListType", http.MethodGet, "/bucket", false},
		// Not allowed: object GET with extra query params.
		{"GetObject_WithQuery", http.MethodGet, "/bucket/key.txt?versionId=xyz", false},
		// Allowed: HeadBucket with "location" query param.
		{"HeadBucket_WithLocation", http.MethodHead, "/bucket?location", true},
		// Not allowed: ACL / multipart subresources.
		{"GetAcl", http.MethodGet, "/bucket?acl", false},
		{"ListMultipart", http.MethodGet, "/bucket?uploads", false},
		{"GetUploadPart", http.MethodGet, "/bucket/key.txt?uploadId=u1", false},
		// Not allowed: write methods.
		{"PutObject", http.MethodPut, "/bucket/key.txt", false},
		{"DeleteObject", http.MethodDelete, "/bucket/key.txt", false},
		// Not allowed: root path.
		{"ListBuckets", http.MethodGet, "/", false},
	}

	for _, tc := range cases {

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := newS3TestRequest(tc.method, tc.path, nil)
			require.Equal(t, tc.want, isReadOnlyS3Request(req), "path=%s method=%s", tc.path, tc.method)
		})
	}
}
