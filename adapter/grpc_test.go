package adapter

import (
	"context"
	"strconv"
	"sync"
	"testing"

	_ "github.com/Jille/grpc-multi-resolver"
	"github.com/bootjp/elastickv/distribution"
	kvstore "github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	_ "google.golang.org/grpc/health"
	"google.golang.org/grpc/status"
	goproto "google.golang.org/protobuf/proto"
)

func TestRawKeyPairsPreservesNilAndEmptyKeys(t *testing.T) {
	t.Parallel()

	pairs := rawKeyPairs([][]byte{nil, {}, []byte("a")})
	require.Len(t, pairs, 3)
	require.Nil(t, pairs[0].Key)
	require.NotNil(t, pairs[1].Key)
	require.Empty(t, pairs[1].Key)
	require.Equal(t, []byte("a"), pairs[2].Key)
}

const (
	grpcSequenceFullIterations  = 9999
	grpcSequenceShortIterations = 256
)

var _ rawGroupCommitTSReader = (*kvstore.ShardStore)(nil)

func grpcSequenceIterations(t testing.TB) int {
	t.Helper()
	if testing.Short() {
		return grpcSequenceShortIterations
	}
	return grpcSequenceFullIterations
}

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
	assert.Zero(t, resp.GetGroupId())
	assert.False(t, resp.GetLeaderFenced())

	// Non-empty key should still work as before.
	resp, err = s.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: []byte("k")})
	assert.NoError(t, err)
	assert.Equal(t, uint64(77), resp.GetTs())
}

func TestGRPCServer_RawLatestCommitTS_ExplicitGroupUsesLeaderFencedReader(t *testing.T) {
	t.Parallel()

	st := &recordingRawGroupStore{
		MVCCStore: store.NewMVCCStore(),
		floorTS:   88,
	}
	s := NewGRPCServer(st, nil)

	resp, err := s.RawLatestCommitTS(context.Background(), &pb.RawLatestCommitTSRequest{GroupId: 7})
	require.NoError(t, err)
	require.Equal(t, uint64(88), resp.GetTs())
	require.True(t, resp.GetExists())
	require.Equal(t, uint64(7), resp.GetGroupId())
	require.True(t, resp.GetLeaderFenced())
	require.Equal(t, uint64(7), st.floorGroupID)
}

func TestGRPCServer_RawLatestCommitTS_ExplicitGroupRequiresAwareStore(t *testing.T) {
	t.Parallel()

	s := NewGRPCServer(store.NewMVCCStore(), nil)
	_, err := s.RawLatestCommitTS(context.Background(), &pb.RawLatestCommitTSRequest{GroupId: 1})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
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
	keyScanGroup bool
	fallbackGet  bool
	fallbackScan bool
	floorGroupID uint64
	floorTS      uint64
	reverseScan  bool
}

func (s *recordingRawGroupStore) GroupCommittedTimestampFloor(_ context.Context, groupID uint64) (uint64, error) {
	s.floorGroupID = groupID
	return s.floorTS, nil
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

func (s *recordingRawGroupStore) ReverseScanGroupAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	s.scanGroupID = groupID
	s.scanStart = append([]byte(nil), start...)
	s.scanEnd = append([]byte(nil), end...)
	s.reverseScan = true
	return s.ReverseScanAt(ctx, start, end, limit, ts)
}

func (s *recordingRawGroupStore) ScanGroupKeysAt(ctx context.Context, groupID uint64, start []byte, end []byte, limit int, ts uint64) ([][]byte, error) {
	s.scanGroupID = groupID
	s.scanStart = append([]byte(nil), start...)
	s.scanEnd = append([]byte(nil), end...)
	s.keyScanGroup = true
	return s.ScanKeysAt(ctx, start, end, limit, ts)
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
	latestGroupID            uint64
	latestGroupReadVersion   uint64
	scanReadRouteVersion     uint64
	scanReadRouteStart       []byte
	scanReadRouteEnd         []byte
	scanReverse              bool
	scanGroupID              uint64
	scanRouteBoundsPresent   bool
	keyScanCalled            bool
	keyScanReadRouteVersion  uint64
	keyScanGroupID           uint64
	callerSuppliedGetSeen    uint64
	callerSuppliedScanSeen   uint64
	callerSuppliedLatestSeen uint64
}

type recordingReadFenceGroupStore struct {
	*recordingRawGroupStore

	readFenceScanCalled     bool
	readFenceReadRouteVer   uint64
	readFenceScanGroupID    uint64
	readFenceScanReverse    bool
	readFenceRouteBoundsSet bool
}

func (s *recordingReadFenceGroupStore) ReadRouteVersion() uint64 {
	return 55
}

func (s *recordingReadFenceGroupStore) ScanAtWithReadFence(
	ctx context.Context,
	start []byte,
	end []byte,
	limit int,
	ts uint64,
	reverse bool,
	groupID uint64,
	readRouteVersion uint64,
	routeStart []byte,
	routeEnd []byte,
) ([]*store.KVPair, error) {
	s.readFenceScanCalled = true
	s.readFenceReadRouteVer = readRouteVersion
	s.readFenceScanGroupID = groupID
	s.readFenceScanReverse = reverse
	s.readFenceRouteBoundsSet = routeStart != nil || routeEnd != nil
	if reverse {
		return s.ReverseScanGroupAt(ctx, groupID, start, end, limit, ts)
	}
	return s.ScanGroupAt(ctx, groupID, start, end, limit, ts)
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

func (s *recordingRawReadFenceStore) LatestCommitTSGroupWithReadFence(_ context.Context, _ []byte, groupID uint64, readRouteVersion uint64) (uint64, bool, error) {
	s.latestGroupID = groupID
	s.latestGroupReadVersion = readRouteVersion
	return 11, true, nil
}

func (s *recordingRawReadFenceStore) ScanAtWithReadFence(_ context.Context, start []byte, _ []byte, _ int, _ uint64, reverse bool, groupID uint64, readRouteVersion uint64, routeStart []byte, routeEnd []byte) ([]*store.KVPair, error) {
	s.scanReadRouteVersion = readRouteVersion
	s.scanReadRouteStart = cloneTestBytes(routeStart)
	s.scanReadRouteEnd = cloneTestBytes(routeEnd)
	s.scanReverse = reverse
	s.scanGroupID = groupID
	s.scanRouteBoundsPresent = routeStart != nil || routeEnd != nil
	if readRouteVersion == 97 {
		s.callerSuppliedScanSeen = readRouteVersion
	}
	return []*store.KVPair{{Key: append([]byte(nil), start...), Value: []byte("v")}}, nil
}

func (s *recordingRawReadFenceStore) ScanKeysAtWithReadFence(_ context.Context, start []byte, _ []byte, _ int, _ uint64, groupID uint64, readRouteVersion uint64) ([][]byte, error) {
	s.keyScanCalled = true
	s.keyScanReadRouteVersion = readRouteVersion
	s.keyScanGroupID = groupID
	return [][]byte{append([]byte(nil), start...)}, nil
}

func cloneTestBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	return append([]byte{}, b...)
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

func TestGRPCServer_RawLatestCommitTS_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	resp, err := s.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{Key: []byte("k"), GroupId: 42, ReadRouteVersion: 98})
	require.NoError(t, err)
	require.True(t, resp.GetExists())
	require.Equal(t, uint64(11), resp.GetTs())
	require.Equal(t, uint64(42), st.latestGroupID)
	require.Equal(t, uint64(98), st.latestGroupReadVersion)
	require.Zero(t, st.latestReadRouteVersion)
}

func TestGRPCServer_RawLatestCommitTS_ExplicitGroupShardStoreVersionProbe(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: nil, GroupID: 42, State: distribution.RouteStateActive},
		},
	}))
	groupStore := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, groupStore.Close()) })
	key := []byte("!dist|migstage|probe|k")
	require.NoError(t, groupStore.PutAt(ctx, key, []byte("v"), 10, 0))
	shards := kvstore.NewShardStore(engine, map[uint64]*kvstore.ShardGroup{
		42: {Store: groupStore},
	})
	server := NewGRPCServer(shards, nil)

	resp, err := server.RawLatestCommitTS(ctx, &pb.RawLatestCommitTSRequest{
		Key:                key,
		GroupId:            42,
		ReadRouteVersion:   1,
		VersionVisibleAtTs: 10,
	})
	require.NoError(t, err)
	require.True(t, resp.GetExists())
	require.Equal(t, uint64(10), resp.GetTs())
	require.True(t, resp.GetVersionVisibleSupported())
	require.True(t, resp.GetVersionVisible())
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
		StartKey:           []byte("a"),
		EndKey:             []byte("z"),
		Limit:              10,
		Ts:                 10,
		ReadRouteVersion:   97,
		RouteStart:         []byte("m"),
		RouteEnd:           []byte("z"),
		RouteBoundsPresent: true,
	})
	require.NoError(t, err)

	require.Equal(t, uint64(99), st.callerSuppliedGetSeen)
	require.Equal(t, uint64(98), st.callerSuppliedLatestSeen)
	require.Equal(t, uint64(97), st.callerSuppliedScanSeen)
	require.Equal(t, []byte("m"), st.scanReadRouteStart)
	require.Equal(t, []byte("z"), st.scanReadRouteEnd)
}

func TestGRPCServer_RawScanAt_ReadFenceVariants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		req               *pb.RawScanAtRequest
		wireRoundTrip     bool
		wantRouteVersion  uint64
		wantBoundsPresent bool
		wantRouteStart    []byte
		wantRouteEnd      []byte
		wantKeysOnly      bool
	}{
		{
			name: "preserves empty full-range bounds across proto",
			req: &pb.RawScanAtRequest{
				StartKey:           []byte("!redis|meta|"),
				EndKey:             []byte("!redis|meta}"),
				Limit:              10,
				Ts:                 10,
				ReadRouteVersion:   97,
				RouteStart:         []byte{},
				RouteEnd:           []byte{},
				RouteBoundsPresent: true,
			},
			wireRoundTrip:     true,
			wantRouteVersion:  97,
			wantBoundsPresent: true,
			wantRouteStart:    []byte{},
			wantRouteEnd:      []byte{},
		},
		{
			name: "ignores bytes when bounds presence is false",
			req: &pb.RawScanAtRequest{
				StartKey:         []byte("!redis|meta|"),
				EndKey:           []byte("!redis|meta}"),
				Limit:            10,
				Ts:               10,
				ReadRouteVersion: 97,
				RouteStart:       []byte("m"),
				RouteEnd:         []byte("z"),
			},
			wantRouteVersion: 97,
		},
		{
			name: "keys-only stamps current version without caller fields",
			req: &pb.RawScanAtRequest{
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Limit:    10,
				Ts:       10,
				KeysOnly: true,
			},
			wantRouteVersion: 55,
			wantKeysOnly:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
			s := NewGRPCServer(st, nil)
			req := tc.req
			if tc.wireRoundTrip {
				wire, err := goproto.Marshal(req)
				require.NoError(t, err)
				decoded := new(pb.RawScanAtRequest)
				require.NoError(t, goproto.Unmarshal(wire, decoded))
				require.True(t, decoded.GetRouteBoundsPresent())
				require.Nil(t, decoded.RouteStart)
				require.Nil(t, decoded.RouteEnd)
				req = decoded
			}

			resp, err := s.RawScanAt(ctx, req)
			require.NoError(t, err)
			require.Len(t, resp.GetKv(), 1)
			if tc.wantKeysOnly {
				require.Empty(t, resp.GetKv()[0].GetValue())
			}
			if tc.wantKeysOnly && !tc.wantBoundsPresent {
				require.True(t, st.keyScanCalled)
				require.Equal(t, tc.wantRouteVersion, st.keyScanReadRouteVersion)
				require.Zero(t, st.scanReadRouteVersion)
			} else {
				require.Equal(t, tc.wantRouteVersion, st.scanReadRouteVersion)
			}
			require.Equal(t, tc.wantBoundsPresent, st.scanRouteBoundsPresent)
			if tc.wantRouteStart == nil {
				require.Nil(t, st.scanReadRouteStart)
			} else {
				require.NotNil(t, st.scanReadRouteStart)
				require.Equal(t, tc.wantRouteStart, st.scanReadRouteStart)
			}
			if tc.wantRouteEnd == nil {
				require.Nil(t, st.scanReadRouteEnd)
			} else {
				require.NotNil(t, st.scanReadRouteEnd)
				require.Equal(t, tc.wantRouteEnd, st.scanReadRouteEnd)
			}
		})
	}
}

func TestGRPCServer_RawScanAt_ValueReadFenceRequiresAwareStore(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		req  *pb.RawScanAtRequest
	}{
		{
			name: "route version",
			req: &pb.RawScanAtRequest{
				StartKey:         []byte("a"),
				EndKey:           []byte("z"),
				Limit:            10,
				Ts:               10,
				ReadRouteVersion: 7,
			},
		},
		{
			name: "route bounds",
			req: &pb.RawScanAtRequest{
				StartKey:           []byte("a"),
				EndKey:             []byte("z"),
				Limit:              10,
				Ts:                 10,
				RouteStart:         []byte("m"),
				RouteEnd:           []byte("z"),
				RouteBoundsPresent: true,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.NewMVCCStore()
			t.Cleanup(func() { _ = st.Close() })
			s := NewGRPCServer(st, nil)
			_, err := s.RawScanAt(context.Background(), tc.req)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}

func TestGRPCServer_RawPointReadsRequireReadFenceAwareStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		read func(*GRPCServer) error
	}{
		{
			name: "get",
			read: func(s *GRPCServer) error {
				_, err := s.RawGet(context.Background(), &pb.RawGetRequest{
					Key:              []byte("k"),
					Ts:               10,
					ReadRouteVersion: 7,
				})
				return err
			},
		},
		{
			name: "latest commit timestamp",
			read: func(s *GRPCServer) error {
				_, err := s.RawLatestCommitTS(context.Background(), &pb.RawLatestCommitTSRequest{
					Key:              []byte("k"),
					ReadRouteVersion: 7,
				})
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := store.NewMVCCStore()
			t.Cleanup(func() { _ = st.Close() })
			err := tc.read(NewGRPCServer(st, nil))
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
		})
	}
}

func TestGRPCServer_RawScanAt_GroupedReverseGoesThroughReadFenceStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	// An unbounded grouped reverse scan used to be rejected here while a store
	// that was also group-aware skipped the fence entirely. Both shapes now
	// reach the fence-aware store with the server-stamped route version.
	_, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       10,
		GroupId:  42,
		Reverse:  true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(55), st.scanReadRouteVersion)
	require.Equal(t, uint64(42), st.scanGroupID)
	require.True(t, st.scanReverse)
}

func TestGRPCServer_RawScanAt_AllowsRouteBoundGroupedReverseWithReadFenceStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	_, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey:           []byte("!redis|meta|"),
		EndKey:             []byte("!redis|meta}"),
		Limit:              10,
		Ts:                 10,
		GroupId:            42,
		Reverse:            true,
		RouteStart:         []byte("m"),
		RouteBoundsPresent: true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(55), st.scanReadRouteVersion)
	require.Equal(t, uint64(42), st.scanGroupID)
	require.True(t, st.scanReverse)
	require.Equal(t, []byte("m"), st.scanReadRouteStart)
	require.NotNil(t, st.scanReadRouteEnd)
	require.Empty(t, st.scanReadRouteEnd)
}

func TestGRPCServer_RawScanAt_AllowsRouteBoundGroupedReverseWithShardStore(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	mvcc := store.NewMVCCStore()
	t.Cleanup(func() { _ = mvcc.Close() })
	st := kvstore.NewShardStore(engine, map[uint64]*kvstore.ShardGroup{
		1: {Store: mvcc},
	})
	s := NewGRPCServer(st, nil)

	left := []byte("!redis|meta|a")
	right := []byte("!redis|meta|z")
	require.NoError(t, mvcc.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, mvcc.PutAt(ctx, right, []byte("right"), 2, 0))

	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey:           []byte("!redis|meta|"),
		EndKey:             []byte("!redis|meta}"),
		Limit:              1,
		Ts:                 2,
		GroupId:            1,
		Reverse:            true,
		RouteStart:         []byte("m"),
		RouteBoundsPresent: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.Kv, 1)
	require.Equal(t, right, resp.Kv[0].Key)
	require.Equal(t, []byte("right"), resp.Kv[0].Value)
}

func TestGRPCServer_RawScanAt_KeysOnlyWithRouteBoundsUsesReadFence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey:           []byte("!redis|meta|"),
		EndKey:             []byte("!redis|meta}"),
		Limit:              10,
		Ts:                 10,
		GroupId:            42,
		KeysOnly:           true,
		RouteStart:         []byte("m"),
		RouteBoundsPresent: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetKv(), 1)
	require.Equal(t, []byte("!redis|meta|"), resp.GetKv()[0].GetKey())
	require.Empty(t, resp.GetKv()[0].GetValue())
	require.Equal(t, uint64(55), st.scanReadRouteVersion)
	require.Equal(t, uint64(42), st.scanGroupID)
	require.Equal(t, []byte("m"), st.scanReadRouteStart)
	require.NotNil(t, st.scanReadRouteEnd)
	require.Empty(t, st.scanReadRouteEnd)
}

func TestGRPCServer_RawScanAt_KeysOnlyUsesReadFenceKeyScanner(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       10,
		GroupId:  42,
		KeysOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetKv(), 1)
	require.Equal(t, []byte("a"), resp.GetKv()[0].GetKey())
	require.Empty(t, resp.GetKv()[0].GetValue())
	require.True(t, st.keyScanCalled)
	require.Equal(t, uint64(55), st.keyScanReadRouteVersion)
	require.Equal(t, uint64(42), st.keyScanGroupID)
	require.Zero(t, st.scanReadRouteVersion)
}

func TestGRPCServer_RawScanAt_UsesExplicitGroupForReverse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawGroupStore{MVCCStore: store.NewMVCCStore()}
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 9, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 10, 0))
	s := NewGRPCServer(st, nil)

	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       10,
		Reverse:  true,
		GroupId:  42,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetKv(), 2)
	require.False(t, st.fallbackScan)
	require.True(t, st.reverseScan)
	require.Equal(t, uint64(42), st.scanGroupID)
	require.Equal(t, []byte("a"), st.scanStart)
	require.Equal(t, []byte("z"), st.scanEnd)
	require.Equal(t, []byte("b"), resp.GetKv()[0].Key)
	require.Equal(t, []byte("a"), resp.GetKv()[1].Key)
}

func TestGRPCServer_RawScanAt_ReadFenceAwareStoreFencesExplicitGroupReverse(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		keysOnly bool
	}{
		{name: "values"},
		{name: "keys-only", keysOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			st := &recordingReadFenceGroupStore{
				recordingRawGroupStore: &recordingRawGroupStore{MVCCStore: store.NewMVCCStore()},
			}
			t.Cleanup(func() { _ = st.Close() })
			require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 9, 0))
			require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 10, 0))
			s := NewGRPCServer(st, nil)

			resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
				StartKey: []byte("a"),
				EndKey:   []byte("z"),
				Limit:    10,
				Ts:       10,
				Reverse:  true,
				GroupId:  42,
				KeysOnly: tc.keysOnly,
			})
			require.NoError(t, err)
			require.Len(t, resp.GetKv(), 2)
			require.Equal(t, []byte("b"), resp.GetKv()[0].GetKey())
			require.Equal(t, []byte("a"), resp.GetKv()[1].GetKey())
			require.True(t, st.reverseScan)
			require.False(t, st.fallbackScan)
			require.True(t, st.readFenceScanCalled)
			require.Equal(t, uint64(55), st.readFenceReadRouteVer)
			require.Equal(t, uint64(42), st.readFenceScanGroupID)
			require.True(t, st.readFenceScanReverse)
			require.False(t, st.readFenceRouteBoundsSet)
			require.Equal(t, uint64(42), st.scanGroupID)
		})
	}
}

func TestGRPCServer_RawScanAt_KeysOnlyUsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawGroupStore{MVCCStore: store.NewMVCCStore()}
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("large-value"), 9, 0))
	s := NewGRPCServer(st, nil)

	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       9,
		GroupId:  42,
		KeysOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetKv(), 1)
	require.Equal(t, []byte("a"), resp.GetKv()[0].GetKey())
	require.Empty(t, resp.GetKv()[0].GetValue())
	require.True(t, st.keyScanGroup)
	require.False(t, st.fallbackScan)
	require.Equal(t, uint64(42), st.scanGroupID)
	require.Equal(t, []byte("a"), st.scanStart)
	require.Equal(t, []byte("z"), st.scanEnd)
}

func TestGRPCServer_RawScanAt_KeysOnlyFallbackOmitsValues(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("large-value"), 9, 0))
	s := NewGRPCServer(st, nil)

	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       9,
		KeysOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetKv(), 1)
	require.Equal(t, []byte("a"), resp.GetKv()[0].GetKey())
	require.Empty(t, resp.GetKv()[0].GetValue())
}

func TestGRPCServer_RawScanAt_KeysOnlyExplicitGroupMergesStagedVisibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  []byte("a"),
				End:                    []byte("z"),
				GroupID:                1,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
		},
	}))
	group := &kvstore.ShardGroup{Store: store.NewMVCCStore()}
	shards := kvstore.NewShardStore(engine, map[uint64]*kvstore.ShardGroup{1: group})
	t.Cleanup(func() { require.NoError(t, shards.Close()) })

	require.NoError(t, group.Store.PutAt(ctx, []byte("b"), []byte("live-b"), 10, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("b")), []byte("staged-b"), 20, 0))
	require.NoError(t, group.Store.PutAt(ctx, distribution.MigrationStagedDataKey(9, []byte("c")), []byte("staged-c"), 30, 0))

	s := NewGRPCServer(shards, nil)
	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       35,
		GroupId:  1,
		KeysOnly: true,
	})
	require.NoError(t, err)
	require.Equal(t, []*pb.RawKVPair{
		{Key: []byte("b")},
		{Key: []byte("c")},
	}, resp.GetKv())
}

func TestGRPCServer_RawScanAt_ReverseKeysOnlyUsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawGroupStore{MVCCStore: store.NewMVCCStore()}
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("large-value-a"), 9, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("large-value-b"), 10, 0))
	s := NewGRPCServer(st, nil)

	resp, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey: []byte("a"),
		EndKey:   []byte("z"),
		Limit:    10,
		Ts:       10,
		Reverse:  true,
		GroupId:  42,
		KeysOnly: true,
	})
	require.NoError(t, err)
	require.Len(t, resp.GetKv(), 2)
	require.Equal(t, []byte("b"), resp.GetKv()[0].GetKey())
	require.Empty(t, resp.GetKv()[0].GetValue())
	require.Equal(t, []byte("a"), resp.GetKv()[1].GetKey())
	require.Empty(t, resp.GetKv()[1].GetValue())
	require.True(t, st.reverseScan)
	require.False(t, st.fallbackScan)
	require.Equal(t, uint64(42), st.scanGroupID)
	require.Equal(t, []byte("a"), st.scanStart)
	require.Equal(t, []byte("z"), st.scanEnd)
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
	for i := range grpcSequenceIterations(t) {
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
	for i := range grpcSequenceIterations(t) {
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

// A grouped reverse key-scan must reach the fence-aware store too. The
// keys-only path had the same legacy shortcut ahead of the fence check.
func TestGRPCServer_RawScanAt_GroupedReverseKeysOnlyStampsReadRouteVersion(t *testing.T) {
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
		KeysOnly: true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(55), st.scanReadRouteVersion)
	require.Equal(t, uint64(42), st.scanGroupID)
	require.True(t, st.scanReverse)
}

// A caller-supplied read_route_version must not be lowered by the server for
// grouped reverse scans either.
func TestGRPCServer_RawScanAt_GroupedReversePreservesCallerReadRouteVersion(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := &recordingRawReadFenceStore{MVCCStore: store.NewMVCCStore(), routeVersion: 55}
	s := NewGRPCServer(st, nil)

	_, err := s.RawScanAt(ctx, &pb.RawScanAtRequest{
		StartKey:         []byte("a"),
		EndKey:           []byte("z"),
		Limit:            10,
		Ts:               10,
		GroupId:          42,
		Reverse:          true,
		ReadRouteVersion: 97,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(97), st.scanReadRouteVersion)
}

type recordingVersionPresenceStore struct {
	store.MVCCStore

	visible   bool
	supported bool
	calls     int
	lastKey   []byte
	lastGroup uint64
	lastTS    uint64
}

func (s *recordingVersionPresenceStore) VersionExistsAtOrBeforeGroupWithReadFence(
	_ context.Context, key []byte, groupID uint64, ts uint64, _ uint64,
) (bool, bool, error) {
	s.calls++
	s.lastKey = append([]byte(nil), key...)
	s.lastGroup = groupID
	s.lastTS = ts
	return s.visible, s.supported, nil
}

type recordingVersionPresenceBatchStore struct {
	store.MVCCStore

	visible              map[string]bool
	supported            bool
	calls                int
	lastKeys             [][]byte
	lastGroup            uint64
	lastTS               uint64
	lastReadRouteVersion uint64
}

func cloneBytes2D(keys [][]byte) [][]byte {
	out := make([][]byte, 0, len(keys))
	for _, key := range keys {
		out = append(out, append([]byte(nil), key...))
	}
	return out
}

func (s *recordingVersionPresenceBatchStore) VersionsExistAtOrBeforeGroupWithReadFence(
	_ context.Context, keys [][]byte, groupID uint64, ts uint64, readRouteVersion uint64,
) ([]bool, bool, error) {
	s.calls++
	s.lastKeys = cloneBytes2D(keys)
	s.lastGroup = groupID
	s.lastTS = ts
	s.lastReadRouteVersion = readRouteVersion
	out := make([]bool, len(keys))
	for i, key := range keys {
		out[i] = s.visible[string(key)]
	}
	return out, s.supported, nil
}

// version_visible_at_ts is optional: only a request that asks gets an answer,
// and a store that cannot answer must not look like "no version exists".
func TestGRPCServer_RawLatestCommitTS_VersionVisibleProbe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		visibleAtTS    uint64
		storeVisible   bool
		storeSupported bool
		wantCalls      int
		wantVisible    bool
		wantSupported  bool
	}{
		{
			name:        "no probe requested",
			visibleAtTS: 0,
			wantCalls:   0,
		},
		{
			name:           "version visible at the read timestamp",
			visibleAtTS:    100,
			storeVisible:   true,
			storeSupported: true,
			wantCalls:      1,
			wantVisible:    true,
			wantSupported:  true,
		},
		{
			name:           "no version at or before the read timestamp",
			visibleAtTS:    100,
			storeVisible:   false,
			storeSupported: true,
			wantCalls:      1,
			wantVisible:    false,
			wantSupported:  true,
		},
		{
			name:           "store cannot answer authoritatively",
			visibleAtTS:    100,
			storeVisible:   false,
			storeSupported: false,
			wantCalls:      1,
			wantVisible:    false,
			wantSupported:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			st := &recordingVersionPresenceStore{
				MVCCStore: store.NewMVCCStore(),
				visible:   tt.storeVisible,
				supported: tt.storeSupported,
			}
			t.Cleanup(func() { _ = st.Close() })
			s := NewGRPCServer(st, nil)

			resp, err := s.RawLatestCommitTS(context.Background(), &pb.RawLatestCommitTSRequest{
				Key:                []byte("k"),
				VersionVisibleAtTs: tt.visibleAtTS,
			})
			require.NoError(t, err)
			require.Equal(t, tt.wantVisible, resp.GetVersionVisible())
			require.Equal(t, tt.wantSupported, resp.GetVersionVisibleSupported())
			require.Equal(t, tt.wantCalls, st.calls)
			if tt.wantCalls == 0 {
				return
			}
			require.Equal(t, []byte("k"), st.lastKey)
			require.Equal(t, tt.visibleAtTS, st.lastTS)
		})
	}
}

func TestGRPCServer_RawLatestCommitTS_BatchVersionVisibleProbe(t *testing.T) {
	t.Parallel()

	st := &recordingVersionPresenceBatchStore{
		MVCCStore: store.NewMVCCStore(),
		visible:   map[string]bool{"a": true, "b": false},
		supported: true,
	}
	t.Cleanup(func() { _ = st.Close() })
	s := NewGRPCServer(st, nil)

	resp, err := s.RawLatestCommitTS(context.Background(), &pb.RawLatestCommitTSRequest{
		KeyBatch:           pb.EncodeRawLatestCommitTSKeyBatch([][]byte{[]byte("a"), []byte("b")}),
		GroupId:            42,
		ReadRouteVersion:   77,
		VersionVisibleAtTs: 100,
	})
	require.NoError(t, err)
	require.Equal(t, []bool{true, false}, resp.GetVersionVisibleResults())
	require.True(t, resp.GetVersionVisibleSupported())
	require.Equal(t, 1, st.calls)
	require.Equal(t, [][]byte{[]byte("a"), []byte("b")}, st.lastKeys)
	require.Equal(t, uint64(42), st.lastGroup)
	require.Equal(t, uint64(100), st.lastTS)
	require.Equal(t, uint64(77), st.lastReadRouteVersion)
}

func TestGRPCServer_RawLatestCommitTS_RejectsOversizedBatchBeforeProbe(t *testing.T) {
	t.Parallel()

	keys := make([][]byte, maxGRPCScanLimit+1)
	for i := range keys {
		keys[i] = []byte("k")
	}
	st := &recordingVersionPresenceBatchStore{
		MVCCStore: store.NewMVCCStore(),
		visible:   map[string]bool{},
		supported: true,
	}
	t.Cleanup(func() { _ = st.Close() })
	s := NewGRPCServer(st, nil)

	_, err := s.RawLatestCommitTS(context.Background(), &pb.RawLatestCommitTSRequest{
		KeyBatch:           pb.EncodeRawLatestCommitTSKeyBatch(keys),
		VersionVisibleAtTs: 100,
	})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Zero(t, st.calls)
}
