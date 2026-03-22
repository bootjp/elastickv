package adapter

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // S3 ETag compatibility requires MD5.
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
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

	groups := map[uint64]*kv.ShardGroup{
		1: {Raft: raft1, Store: store1, Txn: kv.NewLeaderProxy(raft1)},
		2: {Raft: raft2, Store: store2, Txn: kv.NewLeaderProxy(raft2)},
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
