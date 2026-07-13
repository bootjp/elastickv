package adapter

import (
	"context"
	"strconv"
	"sync"
	"testing"

	_ "github.com/Jille/grpc-multi-resolver"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/status"
)

func Test_value_can_be_deleted(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("test-key")
	want := []byte("v")

	_, err := c.RawPut(
		context.Background(),
		&pb.RawPutRequest{Key: key, Value: want},
	)
	assert.NoError(t, err, "Put RPC failed")

	_, err = c.RawPut(context.TODO(), &pb.RawPutRequest{Key: key, Value: want})
	assert.NoError(t, err, "Put RPC failed")
	assert.Nil(t, err)

	resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	assert.NoError(t, err, "Get RPC failed")
	assert.Nil(t, err)
	assert.True(t, resp.Exists)
	assert.Equal(t, want, resp.Value)

	_, err = c.RawDelete(context.TODO(), &pb.RawDeleteRequest{Key: key})
	assert.NoError(t, err, "Delete RPC failed")

	resp, err = c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	assert.NoError(t, err, "Get RPC failed")
	assert.False(t, resp.Exists)
}

func Test_grpc_raw_get_empty_value(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	key := []byte("empty-key")
	empty := []byte{}

	_, err := c.RawPut(context.Background(), &pb.RawPutRequest{Key: key, Value: empty})
	assert.NoError(t, err, "Put RPC failed")

	resp, err := c.RawGet(context.TODO(), &pb.RawGetRequest{Key: key})
	assert.NoError(t, err, "Get RPC failed")
	assert.True(t, resp.Exists)
	assert.Equal(t, 0, len(resp.Value))
}

func Test_grpc_scan(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := transactionalKVClient(t, adders)
	defer shutdown(nodes)

	for i := range 10 {
		key := []byte("test-key-" + strconv.Itoa(i))
		want := []byte(strconv.Itoa(i))
		res, err := c.Put(
			context.Background(),
			&pb.PutRequest{Key: key, Value: want},
		)
		assert.NoError(t, err, "Put RPC failed")
		assert.True(t, res.Success, "Put RPC failed")
		t.Log(res.CommitIndex)
	}

	resp, err := c.Scan(context.TODO(), &pb.ScanRequest{
		StartKey: []byte("test-key"),
		EndKey:   []byte("z" + strconv.Itoa(100)),
		Limit:    10,
	})
	assert.NoError(t, err, "Scan RPC failed")
	assert.Equal(t, 10, len(resp.Kv), "Scan RPC failed")

	for i := range 10 {
		key := []byte("test-key-" + strconv.Itoa(i))
		want := []byte(strconv.Itoa(i))
		assert.Equal(t, key, resp.Kv[i].Key, "Scan RPC failed")
		assert.Equal(t, want, resp.Kv[i].Value, "Scan RPC failed")
	}
}

func TestGRPCServer_RawLatestCommitTS_EmptyKeyReturnsGlobalWatermark(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v"), 77, 0))

	s := NewGRPCServer(st, nil)

	// Empty key should return global LastCommitTS, not an error.
	resp, err := s.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{})
	assert.NoError(t, err)
	assert.Equal(t, uint64(77), resp.GetTs())
	assert.True(t, resp.GetExists())

	// Non-empty key should still work as before.
	resp, err = s.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: []byte("k")})
	assert.NoError(t, err)
	assert.Equal(t, uint64(77), resp.GetTs())
}

func TestGRPCServer_RawScanAt_RejectsOversizedLimit(t *testing.T) {
	t.Parallel()

	s := NewGRPCServer(store.NewMVCCStore(), nil)

	_, err := s.RawScanAt(context.Background(), &pb.RawScanAtRequest{
		Limit: maxGRPCScanLimit + 1,
	})

	assert.Error(t, err)
}

type recordingRawGroupStore struct {
	store.MVCCStore

	getGroupID   uint64
	getGroupKey  []byte
	scanGroupID  uint64
	scanStart    []byte
	scanEnd      []byte
	fallbackGet  bool
	fallbackScan bool
}

func (s *recordingRawGroupStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	s.fallbackGet = true
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func (s *recordingRawGroupStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.fallbackScan = true
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func (s *recordingRawGroupStore) GetGroupAt(ctx context.Context, groupID uint64, key []byte, ts uint64) ([]byte, error) {
	s.getGroupID = groupID
	s.getGroupKey = append([]byte(nil), key...)
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func (s *recordingRawGroupStore) ScanGroupAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.scanGroupID = groupID
	s.scanStart = append([]byte(nil), start...)
	s.scanEnd = append([]byte(nil), end...)
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func TestGRPCServer_RawGet_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawGroupStore{MVCCStore: store.NewMVCCStore()}
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v"), 9, 0))
	s := NewGRPCServer(st, nil)

	resp, err := s.RawGet(ctx, &pb.RawGetRequest{Key: []byte("k"), Ts: 9, GroupId: 42})
	require.NoError(t, err)
	require.True(t, resp.GetExists())
	require.Equal(t, []byte("v"), resp.GetValue())
	require.False(t, st.fallbackGet)
	require.Equal(t, uint64(42), st.getGroupID)
	require.Equal(t, []byte("k"), st.getGroupKey)
}

func TestGRPCServer_RawScanAt_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawGroupStore{MVCCStore: store.NewMVCCStore()}
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("v"), 9, 0))
	s := NewGRPCServer(st, nil)

	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       9,
		GroupId:  42,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetKv(), 1)
	require.False(t, st.fallbackScan)
	require.Equal(t, uint64(42), st.scanGroupID)
	require.Equal(t, []byte("a"), st.scanStart)
	require.Equal(t, []byte("z"), st.scanEnd)
}

type recordingRawReadFenceStore struct {
	store.MVCCStore

	routeVersion             uint64
	getReadRouteVersion      uint64
	latestReadRouteVersion   uint64
	scanReadRouteVersion     uint64
	scanReadRouteStart       []byte
	scanReadRouteEnd         []byte
	callerSuppliedGetSeen    uint64
	callerSuppliedScanSeen   uint64
	callerSuppliedLatestSeen uint64
}

func (s *recordingRawReadFenceStore) ReadRouteVersion() uint64 {
	return s.routeVersion
}

func (s *recordingRawReadFenceStore) GetAtWithReadFence(_ context.Context, _ []byte, _ uint64, _ uint64, readRouteVersion uint64) ([]byte, error) {
	s.getReadRouteVersion = readRouteVersion
	if readRouteVersion == 99 {
		s.callerSuppliedGetSeen = readRouteVersion
	}
	return []byte("v"), nil
}

func (s *recordingRawReadFenceStore) LatestCommitTSWithReadFence(_ context.Context, _ []byte, readRouteVersion uint64) (uint64, bool, error) {
	s.latestReadRouteVersion = readRouteVersion
	if readRouteVersion == 98 {
		s.callerSuppliedLatestSeen = readRouteVersion
	}
	return 10, true, nil
}

func (s *recordingRawReadFenceStore) ScanAtWithReadFence(_ context.Context, start []byte, _ []byte, _ int, _ uint64, _ bool, _ uint64, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	s.scanReadRouteVersion = readRouteVersion
	s.scanReadRouteStart = append([]byte(nil), routeStart...)
	s.scanReadRouteEnd = append([]byte(nil), routeEnd...)
	if readRouteVersion == 97 {
		s.callerSuppliedScanSeen = readRouteVersion
	}
	return []*store.KVPair{{Key: append([]byte(nil), start...), Value: []byte("v")}}, nil
}

func TestGRPCServer_RawReadFenceHelpersStampCurrentRouteVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	_, err := s.RawGet(ctx, &pb.RawGetRequest{Key: []byte("k"), Ts: 10})
	require.NoError(t, err)
	_, err = s.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: []byte("k")})
	require.NoError(t, err)
	_, err = s.RawScanAt(ctx, &pb.RawScanAtRequest{StartKey: []byte("a"), EndKey: []byte("z"), Limit: 10, Ts: 10})
	require.NoError(t, err)

	require.Equal(t, uint64(55), st.getReadRouteVersion)
	require.Equal(t, uint64(55), st.latestReadRouteVersion)
	require.Equal(t, uint64(55), st.scanReadRouteVersion)
}

func TestGRPCServer_RawReadFenceHelpersKeepCallerRouteVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	_, err := s.RawGet(ctx, &pb.RawGetRequest{Key: []byte("k"), Ts: 10, ReadRouteVersion: 99})
	require.NoError(t, err)
	_, err = s.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: []byte("k"), ReadRouteVersion: 98})
	require.NoError(t, err)
	_, err = s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey:         []byte("a"),
		EndKey:           []byte("z"),
		Limit:            10,
		Ts:               10,
		ReadRouteVersion: 97,
		RouteStart:       []byte("m"),
		RouteEnd:         []byte("z"),
	})
	require.NoError(t, err)

	require.Equal(t, uint64(99), st.callerSuppliedGetSeen)
	require.Equal(t, uint64(98), st.callerSuppliedLatestSeen)
	require.Equal(t, uint64(97), st.callerSuppliedScanSeen)
	require.Equal(t, []byte("m"), st.scanReadRouteStart)
	require.Equal(t, []byte("z"), st.scanReadRouteEnd)
}

func TestGRPCServer_RawScanAt_GroupedReverseStaysInvalidArgumentWithReadFenceStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	_, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       10,
		GroupId:  42,
		Reverse:  true,
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Zero(t, st.scanReadRouteVersion)
}

func TestGRPCServer_Scan_RejectsOversizedLimit(t *testing.T) {
	t.Parallel()

	s := NewGRPCServer(store.NewMVCCStore(), nil)

	_, err := s.Scan(context.Background(), &pb.ScanRequest{
		Limit: maxGRPCScanLimit + 1,
	})

	assert.Error(t, err)
}

func Test_consistency_satisfy_write_after_read_for_parallel(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	wg := sync.WaitGroup{}
	const workers = 1000
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			key := []byte("test-key-parallel" + strconv.Itoa(i))
			want := []byte(strconv.Itoa(i))
			_, err := c.RawPut(
				context.Background(),
				&pb.RawPutRequest{Key: key, Value: want},
			)
			if !assert.NoError(t, err, "Put RPC failed") {
				return
			}
			_, err = c.RawPut(context.Background(), &pb.RawPutRequest{Key: key, Value: want})
			if !assert.NoError(t, err, "Put RPC failed") {
				return
			}

			resp, err := c.RawGet(context.Background(), &pb.RawGetRequest{Key: key})
			if !assert.NoError(t, err, "Get RPC failed") {
				return
			}
			assert.Equal(t, want, resp.Value, "consistency check failed")
		}(i)
	}
	wg.Wait()
}

func Test_consistency_satisfy_write_after_read_sequence(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := rawKVClient(t, adders)
	defer shutdown(nodes)

	// Use t.Context() so a test-level cancel (timeout, parent test
	// stopping) propagates into every RPC and the retry loop alike,
	// rather than leaking work via context.Background() once the test
	// goroutine returns.
	ctx := t.Context()
	key := []byte("test-key-sequence")

	// Each RPC is wrapped in retryNotLeader so an in-flight Raft
	// re-election (which can fire mid-loop on a busy CI runner — emit
	// "leader not found" / "etcd raft engine is not leader" — and is
	// purely an availability hiccup, not a consistency violation) does
	// not abort the test. The post-RPC assert.Equal still pins the
	// consistency invariant: once Put eventually succeeds, the
	// subsequent Get must return the same value, otherwise we fail.
	for i := range 9999 {
		want := []byte("sequence" + strconv.Itoa(i))
		err := retryNotLeader(ctx, func() error {
			_, perr := c.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: want})
			return perr
		})
		// Stop at the first non-leader-churn RPC failure instead of
		// continuing: a genuine regression would otherwise cascade
		// into 9998 more iterations, each reporting the same broken
		// invariant, and drown the real cause in test-output noise.
		if !assert.NoError(t, err, "Put RPC failed") {
			break
		}

		err = retryNotLeader(ctx, func() error {
			_, perr := c.RawPut(ctx, &pb.RawPutRequest{Key: key, Value: want})
			return perr
		})
		if !assert.NoError(t, err, "Put RPC failed") {
			break
		}

		var resp *pb.RawGetResponse
		err = retryNotLeader(ctx, func() error {
			var gerr error
			resp, gerr = c.RawGet(ctx, &pb.RawGetRequest{Key: key})
			return gerr
		})
		if !assert.NoError(t, err, "Get RPC failed") {
			break
		}

		// Consistency invariant — the entire reason this test exists.
		// Wrapped RPCs only mask transport-layer flakes; if the
		// cluster ever returns a stale Get result here it is still
		// flagged loudly.
		assert.Equal(t, want, resp.Value, "consistency check failed")
	}
}

func Test_grpc_transaction(t *testing.T) {
	t.Parallel()
	nodes, adders, _ := createNode(t, 3)
	c := transactionalKVClient(t, adders)
	defer shutdown(nodes)

	// See Test_consistency_satisfy_write_after_read_sequence for why
	// we use t.Context() and retryNotLeader together.
	ctx := t.Context()
	key := []byte("test-key-sequence")

	// Same retryNotLeader wrap as Test_consistency_satisfy_write_after_read
	// _sequence: tolerate transient leader churn (purely availability,
	// not consistency) while keeping the Put → Get → Delete → Get
	// invariants strict.
	for i := range 9999 {
		want := []byte("sequence" + strconv.Itoa(i))
		err := retryNotLeader(ctx, func() error {
			_, perr := c.Put(ctx, &pb.PutRequest{Key: key, Value: want})
			return perr
		})
		// See Test_consistency_satisfy_write_after_read_sequence:
		// break on first RPC failure so a single broken invariant
		// does not amplify into thousands of assertion lines.
		if !assert.NoError(t, err, "Put RPC failed") {
			break
		}
		var resp *pb.GetResponse
		err = retryNotLeader(ctx, func() error {
			var gerr error
			resp, gerr = c.Get(ctx, &pb.GetRequest{Key: key})
			return gerr
		})
		if !assert.NoError(t, err, "Get RPC failed") {
			break
		}
		assert.Equal(t, want, resp.Value, "consistency check failed")

		err = retryNotLeader(ctx, func() error {
			_, derr := c.Delete(ctx, &pb.DeleteRequest{Key: key})
			return derr
		})
		if !assert.NoError(t, err, "Delete RPC failed") {
			break
		}

		err = retryNotLeader(ctx, func() error {
			var gerr error
			resp, gerr = c.Get(ctx, &pb.GetRequest{Key: key})
			return gerr
		})
		if !assert.NoError(t, err, "Get RPC failed") {
			break
		}
		assert.Nil(t, resp.Value, "consistency check failed")
	}
}

func rawKVClient(t *testing.T, hosts []string) pb.RawKVClient {
	conn, err := grpc.NewClient(hosts[0],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	assert.NoError(t, err)
	return pb.NewRawKVClient(conn)
}

func transactionalKVClient(t *testing.T, hosts []string) pb.TransactionalKVClient {
	conn, err := grpc.NewClient(hosts[0],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)

	assert.NoError(t, err)
	return pb.NewTransactionalKVClient(conn)
}
