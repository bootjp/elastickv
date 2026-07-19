package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type followerProxyEngine struct {
	leader string
}

func (e *followerProxyEngine) Propose(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, ErrLeaderNotFound
}

func (e *followerProxyEngine) ProposeAdmin(context.Context, []byte) (*raftengine.ProposalResult, error) {
	return nil, ErrLeaderNotFound
}

func (e *followerProxyEngine) State() raftengine.State {
	return raftengine.StateFollower
}

func (e *followerProxyEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{Address: e.leader}
}

func (e *followerProxyEngine) VerifyLeader(context.Context) error {
	return ErrLeaderNotFound
}

func (e *followerProxyEngine) LinearizableRead(context.Context) (uint64, error) {
	return 0, ErrLeaderNotFound
}

func (e *followerProxyEngine) Status() raftengine.Status {
	return raftengine.Status{
		State:  raftengine.StateFollower,
		Leader: raftengine.LeaderInfo{Address: e.leader},
	}
}

func (e *followerProxyEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}

func (e *followerProxyEngine) Close() error {
	return nil
}

func TestShardStoreScanAt_IncludesListKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))

	userKey := []byte("x")
	itemKey := store.ListItemKey(userKey, 0)
	require.NoError(t, st.PutAt(ctx, itemKey, []byte("v0"), 3, 0))

	// A full scan should surface internal list keys that may live on any shard.
	kvs, err := st.ScanAt(ctx, []byte(""), nil, 1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, itemKey, kvs[0].Key)
}

func TestShardStoreScanAt_RoutesListItemScansByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	userKey := []byte("x") // routes to group 2
	k0 := store.ListItemKey(userKey, 0)
	k1 := store.ListItemKey(userKey, 1)
	k2 := store.ListItemKey(userKey, 2)
	require.NoError(t, st.PutAt(ctx, k0, []byte("v0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("v1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("v2"), 3, 0))

	end := store.ListItemKey(userKey, 3) // exclusive upper bound
	kvs, err := st.ScanAt(ctx, k0, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 3)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
	require.Equal(t, k2, kvs[2].Key)
}

func TestShardStoreScanAtWithReadFence_RoutesListAuxiliaryScansByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	shardStore := NewShardStore(engine, groups)
	userKey := []byte("x")
	deltaKey := store.ListMetaDeltaKey(userKey, 10, 0)
	claimKey := store.ListClaimKey(userKey, 1)
	require.NoError(t, groups[2].Store.PutAt(ctx, deltaKey, []byte("delta"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, claimKey, []byte("claim"), 11, 0))

	for _, tc := range []struct {
		name   string
		prefix []byte
		key    []byte
	}{
		{name: "delta", prefix: store.ListMetaDeltaScanPrefix(userKey), key: deltaKey},
		{name: "claim", prefix: store.ListClaimScanPrefix(userKey), key: claimKey},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvs, err := shardStore.ScanAtWithReadFence(
				ctx, tc.prefix, prefixScanEnd(tc.prefix), 10, ^uint64(0), false, 0, engine.Version(), nil, nil,
			)
			require.NoError(t, err)
			require.Len(t, kvs, 1)
			require.Equal(t, tc.key, kvs[0].Key)
		})
	}
}

func TestShardStoreScanGroupAt_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1:  {Store: store.NewMVCCStore()},
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := []byte("!sqs|msg|vis|p|orders|partition-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("msg-2"), 7, 0))

	kvs, err := st.ScanGroupAt(ctx, 42, []byte("!sqs|msg|vis|p|"), prefixScanEnd([]byte("!sqs|msg|vis|p|")), 10, 7)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("msg-2"), kvs[0].Value)
}

func TestShardStoreGetGroupAt_UsesExplicitGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1:  {Store: store.NewMVCCStore()},
		42: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := []byte("!sqs|msg|data|p|orders|partition-2|msg-2")
	require.NoError(t, groups[42].Store.PutAt(ctx, key, []byte("payload"), 7, 0))

	val, err := st.GetGroupAt(ctx, 42, key, 7)
	require.NoError(t, err)
	require.Equal(t, []byte("payload"), val)

	_, err = st.GetAt(ctx, key, 7)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}

func TestShardStore_ForwardsReadFenceStamps(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		getResp: &pb.RawGetResponse{
			Exists: true,
			Value:  []byte("remote-v"),
		},
		scanResp: &pb.RawScanAtResponse{},
		latestResp: &pb.RawLatestCommitTSResponse{
			Ts:     42,
			Exists: true,
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 100,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	st := NewShardStore(engine, map[uint64]*ShardGroup{
		1: {
			Store:  store.NewMVCCStore(),
			Engine: &stubFollowerEngine{leaderAddr: addr},
		},
	})
	t.Cleanup(func() { _ = st.Close() })

	ctx := context.Background()
	_, err := st.GetAt(ctx, []byte("k"), 10)
	require.NoError(t, err)
	_, _, err = st.LatestCommitTS(ctx, []byte("k"))
	require.NoError(t, err)
	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 79, []byte("a"), []byte("m"))
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(100), fake.lastGetReq.GetReadRouteVersion())
	require.Equal(t, uint64(100), fake.lastLatestReq.GetReadRouteVersion())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, []byte("a"), fake.lastScanReq.GetRouteStart())
	require.Equal(t, []byte("m"), fake.lastScanReq.GetRouteEnd())
	require.True(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAt(ctx, []byte("a"), []byte("z"), 10, 11)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(0), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	fake.mu.Unlock()

	_, err = st.ScanKeysAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, 0, 82)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(0), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.True(t, fake.lastScanReq.GetKeysOnly())
	fake.mu.Unlock()

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 80, []byte{}, []byte{})
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.True(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 10, 11, false, 0, 81, nil, nil)
	require.NoError(t, err)

	fake.mu.Lock()
	require.Equal(t, uint64(0), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
	require.False(t, fake.lastScanReq.GetRouteBoundsPresent())
	fake.mu.Unlock()

	_, err = st.ScanAt(ctx, []byte(""), nil, 10, 11)
	require.NoError(t, err)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, uint64(1), fake.lastScanReq.GetGroupId())
	require.Equal(t, uint64(100), fake.lastScanReq.GetReadRouteVersion())
}

func TestShardStoreRoutesForScanUsesWideColumnUserKey(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	st := NewShardStore(engine, nil)
	userKey := []byte("z-user")

	for _, tc := range []struct {
		name   string
		prefix []byte
	}{
		{name: "hash fields", prefix: store.HashFieldScanPrefix(userKey)},
		{name: "hash deltas", prefix: store.HashMetaDeltaScanPrefix(userKey)},
		{name: "set members", prefix: store.SetMemberScanPrefix(userKey)},
		{name: "set deltas", prefix: store.SetMetaDeltaScanPrefix(userKey)},
		{name: "zset members", prefix: store.ZSetMemberScanPrefix(userKey)},
		{name: "zset scores", prefix: store.ZSetScoreScanPrefix(userKey)},
		{name: "zset deltas", prefix: store.ZSetMetaDeltaScanPrefix(userKey)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			routes, clamp := st.routesForScan(tc.prefix, prefixScanEnd(tc.prefix))
			require.False(t, clamp)
			require.Len(t, routes, 1)
			require.Equal(t, uint64(2), routes[0].GroupID)
		})
	}
}

func TestShardStoreScanAtRoutesWideColumnPrefixesByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	userKey := []byte("z-user")

	for _, tc := range []struct {
		name   string
		key    []byte
		prefix []byte
	}{
		{name: "hash field", key: store.HashFieldKey(userKey, []byte("field")), prefix: store.HashFieldScanPrefix(userKey)},
		{name: "hash delta", key: store.HashMetaDeltaKey(userKey, 10, 1), prefix: store.HashMetaDeltaScanPrefix(userKey)},
		{name: "set member", key: store.SetMemberKey(userKey, []byte("member")), prefix: store.SetMemberScanPrefix(userKey)},
		{name: "set delta", key: store.SetMetaDeltaKey(userKey, 11, 1), prefix: store.SetMetaDeltaScanPrefix(userKey)},
		{name: "zset member", key: store.ZSetMemberKey(userKey, []byte("member")), prefix: store.ZSetMemberScanPrefix(userKey)},
		{name: "zset score", key: store.ZSetScoreKey(userKey, 1.5, []byte("member")), prefix: store.ZSetScoreScanPrefix(userKey)},
		{name: "zset delta", key: store.ZSetMetaDeltaKey(userKey, 12, 1), prefix: store.ZSetMetaDeltaScanPrefix(userKey)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, st.PutAt(ctx, tc.key, []byte("value"), 20, 0))
			kvs, err := st.ScanAt(ctx, tc.prefix, prefixScanEnd(tc.prefix), 10, 20)
			require.NoError(t, err)
			require.Equal(t, []*store.KVPair{{Key: tc.key, Value: []byte("value")}}, kvs)
			_, err = groups[1].Store.GetAt(ctx, tc.key, 20)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		})
	}
}

func TestShardStoreReadFenceFailsClosedWhileCatalogVersionIsBehind(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groupStore := store.NewMVCCStore()
	require.NoError(t, groupStore.PutAt(context.Background(), []byte("k"), []byte("stale"), 1, 0))
	st := NewShardStore(engine, map[uint64]*ShardGroup{1: {Store: groupStore}})

	tests := []struct {
		name string
		read func(context.Context) error
	}{
		{
			name: "point read",
			read: func(ctx context.Context) error {
				_, err := st.GetAtWithReadFence(ctx, []byte("k"), 1, 0, 2)
				return err
			},
		},
		{
			name: "latest commit timestamp",
			read: func(ctx context.Context) error {
				_, _, err := st.LatestCommitTSWithReadFence(ctx, []byte("k"), 2)
				return err
			},
		},
		{
			name: "range scan",
			read: func(ctx context.Context) error {
				_, err := st.ScanAtWithReadFence(ctx, []byte("a"), []byte("z"), 1, 1, false, 0, 2, nil, nil)
				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			err := tc.read(ctx)
			require.ErrorIs(t, err, ErrReadRouteVersionUnavailable)
		})
	}
}

func TestShardStoreReadFenceWaitsForCatalogAndReroutesPointRead(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	require.NoError(t, groups[1].Store.PutAt(context.Background(), []byte("k"), []byte("old-owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(context.Background(), []byte("k"), []byte("new-owner"), 1, 0))
	st := NewShardStore(engine, groups)

	applyErr := make(chan error, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		applyErr <- engine.ApplySnapshot(distribution.CatalogSnapshot{
			Version: 2,
			Routes: []distribution.RouteDescriptor{
				{RouteID: 1, Start: []byte(""), GroupID: 2, State: distribution.RouteStateActive},
			},
		})
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	value, err := st.GetAtWithReadFence(ctx, []byte("k"), 1, 0, 2)
	require.NoError(t, err)
	require.Equal(t, []byte("new-owner"), value)
	require.NoError(t, <-applyErr)
}

func TestShardStoreScanAtWithReadFence_RoutesUsingSuppliedBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	first := []byte("!redis|meta|x")
	second := []byte("!redis|meta|y")
	require.NoError(t, groups[2].Store.PutAt(ctx, first, []byte("v1"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, second, []byte("v2"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, first, kvs[0].Key)
	require.Equal(t, second, kvs[1].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, true, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, second, kvs[0].Key)
	require.Equal(t, first, kvs[1].Key)
}

func TestShardStoreScanAtWithReadFence_ScansSameGroupSuppliedBoundsAcrossRouteIntervals(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	first := []byte("!redis|meta|a")
	second := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, first, []byte("v1"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, second, []byte("v2"), 2, 0))

	for _, tc := range []struct {
		name       string
		reverse    bool
		routeStart []byte
		routeEnd   []byte
		want       [][]byte
	}{
		{
			name:       "left interval only",
			routeStart: []byte("a"),
			routeEnd:   []byte("z"),
			want:       [][]byte{first},
		},
		{
			name:       "forward across intervals",
			routeStart: []byte("a"),
			want:       [][]byte{first, second},
		},
		{
			name:       "reverse across intervals",
			reverse:    true,
			routeStart: []byte("a"),
			want:       [][]byte{second, first},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 2, tc.reverse, 0, st.ReadRouteVersion(), tc.routeStart, tc.routeEnd)
			require.NoError(t, err)
			require.Len(t, kvs, len(tc.want))
			for i, want := range tc.want {
				require.Equal(t, want, kvs[i].Key)
			}
		})
	}
}

func TestShardStoreScanAtWithReadFence_FiltersWideRedisKeysByUserKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!hs|")
	left := store.HashFieldKey([]byte("alpha"), []byte("f"))
	right := store.HashFieldKey([]byte("zulu"), []byte("f"))
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 0, st.ReadRouteVersion(), []byte{}, []byte("m"))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, left, kvs[0].Key)
}

func TestShardStoreScanAtWithReadFence_FiltersSuppliedBoundsByRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|a")
	right := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, false, 0, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 0, st.ReadRouteVersion(), []byte{}, []byte("m"))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, left, kvs[0].Key)
}

func TestShardStoreScanAtWithReadFence_FiltersByEachRouteBounds(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), []byte("z"), 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|b")
	staleRightOnLeftGroup := []byte("!redis|meta|x")
	right := []byte("!redis|meta|y")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, staleRightOnLeftGroup, []byte("stale"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, right, []byte("right"), 3, 0))

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 10, 3, false, 0, st.ReadRouteVersion(), []byte("a"), []byte("z"))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, left, kvs[0].Key)
	require.Equal(t, right, kvs[1].Key)
}

func TestShardStoreScanAtWithReadFence_AllowsExplicitGroupRouteBoundReverse(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	rawPrefix := []byte("!redis|meta|")
	left := []byte("!redis|meta|a")
	right := []byte("!redis|meta|z")
	require.NoError(t, groups[1].Store.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, right, []byte("right"), 2, 0))

	_, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 1, st.ReadRouteVersion(), nil, nil)
	require.ErrorIs(t, err, store.ErrNotSupported)

	kvs, err := st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), -1, 2, true, 1, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Empty(t, kvs)

	kvs, err = st.ScanAtWithReadFence(ctx, rawPrefix, prefixScanEnd(rawPrefix), 1, 2, true, 1, st.ReadRouteVersion(), []byte("m"), nil)
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, right, kvs[0].Key)
	require.Equal(t, []byte("right"), kvs[0].Value)
}

func TestShardStoreScanAt_IncludesS3ManifestKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "alpha")
	k2 := s3keys.ObjectManifestKey("bucket-a", 1, "zeta")
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("m2"), 2, 0))

	start := s3keys.ObjectManifestPrefixForBucket("bucket-a", 1)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k2, kvs[1].Key)
}

func TestShardStoreScanKeysAt_IncludesKeysAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.DeleteAt(ctx, []byte("b"), 3))
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 4, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 5, 0))

	keys, err := st.ScanKeysAt(ctx, []byte(""), nil, 2, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("x")}, keys)
}

func TestShardStoreScanKeysAt_DeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	keys, err := st.ScanKeysAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, keys)
}

func TestShardStoreScanAt_DeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	kvs, err := st.ScanAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("a"), kvs[0].Key)
	require.Equal(t, []byte("z"), kvs[1].Key)
}

func TestBackupScannerDeduplicatesUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, got)
}

func TestBackupScannerRetainsAllCapturedRangesForUnclampedGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("z"), []byte("vz"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("m"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("z")}, got)
}

func TestBackupScannerRetainsAllCapturedChunkRanges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	home := uint64(11)
	inodeA := uint64(22)
	inodeB := uint64(23)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	routeEnd := prefixScanEnd(routeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, routeEnd, 2)
	engine.UpdateRoute(routeEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	chunkA := fskeys.ChunkKey(home, inodeA, 0)
	chunkB := fskeys.ChunkKey(home, inodeB, 0)
	require.NoError(t, st.PutAt(ctx, chunkA, []byte("a"), 1, 0))
	require.NoError(t, st.PutAt(ctx, chunkB, []byte("b"), 2, 0))

	start := fskeys.ChunkPrefix(home, inodeA)
	end := prefixScanEnd(fskeys.ChunkPrefix(home, inodeB))
	sc := st.NewBackupScanner(start, end, ^uint64(0), 10)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), End: routeA, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 2, Start: routeA, End: routeB, GroupID: 2, State: distribution.RouteStateActive},
			{RouteID: 3, Start: routeB, End: routeEnd, GroupID: 1, State: distribution.RouteStateActive},
			{RouteID: 4, Start: routeEnd, GroupID: 3, State: distribution.RouteStateActive},
		},
	}))

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{chunkA, chunkB}, got)
}

func TestBackupScannerDoesNotUseLiveCatalogForMaterialization(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("x"), []byte("stale-old-owner"), 1, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 10)
	defer sc.Close()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 10,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestShardStoreScanKeysRouteAtLeaderRefillsAfterTxnInternalKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})

	require.NoError(t, g.Store.PutAt(ctx, txnCommitKey([]byte("primary"), 10), []byte("commit"), 1, 0))
	require.NoError(t, g.Store.PutAt(ctx, []byte("a"), []byte("va"), 2, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, []byte(""), nil, 1, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("a")}, keys)
}

func TestShardStoreScanKeysRouteAtLeaderPreservesEmptyKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	g := &ShardGroup{Store: store.NewMVCCStore()}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{1: g})

	require.NoError(t, g.Store.PutAt(ctx, []byte(""), []byte("empty"), 1, 0))
	require.NoError(t, g.Store.PutAt(ctx, []byte("a"), []byte("va"), 2, 0))

	keys, err := st.scanKeysRouteAtLeader(ctx, g, nil, nil, 2, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte(""), []byte("a")}, keys)
}

func TestShardStoreProxyScanKeysAtUsesSelectedGroup(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{{Key: []byte("k"), Value: []byte("v")}},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 42)
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: g})

	keys, err := st.scanKeyRouteAt(ctx, distribution.Route{GroupID: 42}, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("k")}, keys)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, 1, fake.scanCalls)
	require.Equal(t, uint64(42), fake.lastScanGroupID)
	require.True(t, fake.lastScanKeysOnly)
}

func TestShardStoreProxyScanAtUsesSelectedGroup(t *testing.T) {
	t.Parallel()

	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{{Key: []byte("k"), Value: []byte("v")}},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 42)
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(engine, map[uint64]*ShardGroup{42: g})

	kvs, err := st.ScanAt(ctx, []byte(""), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, []byte("k"), kvs[0].Key)

	fake.mu.Lock()
	defer fake.mu.Unlock()
	require.Equal(t, 1, fake.scanCalls)
	require.Equal(t, uint64(42), fake.lastScanGroupID)
}

func TestShardStoreProxyForwardPageAdvancesFromRawPage(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	fake := &fakeRawKVServer{
		scanResp: &pb.RawScanAtResponse{
			Kv: []*pb.RawKVPair{
				{Key: []byte("a"), Value: []byte("va")},
				{Key: internalKey, Value: []byte("commit")},
			},
		},
	}
	addr, stop := startRawKVServer(t, fake)
	t.Cleanup(stop)

	ctx := context.Background()
	g := &ShardGroup{
		Engine: &followerProxyEngine{leader: addr},
		Store:  store.NewMVCCStore(),
	}
	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{42: g})

	page, err := st.scanRouteAtForwardPage(ctx, distribution.Route{GroupID: 42}, g, []byte(""), nil, 2, ^uint64(0), true, 0, nil, nil)
	require.NoError(t, err)
	require.True(t, page.full)
	require.Equal(t, internalKey, page.advanceKey)
	require.Len(t, page.kvs, 1)
	require.Equal(t, []byte("a"), page.kvs[0].Key)
}

func TestKeyOnlyScanHelpersPreserveEmptyKey(t *testing.T) {
	t.Parallel()

	keys := keysFromKVs(kvPairsFromKeys([][]byte{nil, []byte(""), []byte("a")}))
	require.Equal(t, [][]byte{[]byte(""), []byte("a")}, keys)
}

func TestMergeAndTrimScanKeysSortsBeforeTruncating(t *testing.T) {
	t.Parallel()

	keys := mergeAndTrimScanKeys(nil, [][]byte{[]byte("c"), []byte("d"), []byte("b")}, 2)
	require.Equal(t, [][]byte{[]byte("b"), []byte("c")}, keys)
}

func TestBackupScannerPaging(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	var commitTS uint64 = 1
	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("x"), []byte("z")} {
		require.NoError(t, st.PutAt(ctx, key, []byte("v"), commitTS, 0))
		commitTS++
	}

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 2)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("x"), []byte("z")}, got)
}

func TestBackupScannerMaterializesFromCapturedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("old-owner"), 1, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("old-owner"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerMaterializesFromEnumeratedRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("stale-first-route"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("a"), []byte("enumerated-route"), 2, 0))

	sc := &backupScanner{
		store:         st,
		routes:        []distribution.Route{{RouteID: 1, Start: []byte(""), GroupID: 1}, {RouteID: 2, Start: []byte(""), GroupID: 2}},
		clampToRoutes: false,
		cursor:        []byte(""),
		ts:            ^uint64(0),
		pageSize:      1,
	}
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("enumerated-route"), kvp.Value)
}

func TestBackupScannerPreservesFullRoutingAfterListKeyCursor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	listKey := store.ListItemKey([]byte("x"), 0)
	require.NoError(t, st.PutAt(ctx, listKey, []byte("list"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("plain"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	var got [][]byte
	for {
		kvp, ok, err := sc.Next(ctx)
		require.NoError(t, err)
		if !ok {
			break
		}
		got = append(got, kvp.Key)
	}
	require.Equal(t, [][]byte{listKey, []byte("a")}, got)
}

func TestBackupScannerContinuesPastTxnInternalOnlyPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, groups[1].Store.PutAt(ctx, txnCommitKey([]byte("primary"), 10), []byte("commit"), 1, 0))
	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("a"), []byte("visible"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerContinuesPastStaleOffRoutePage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, groups[1].Store.PutAt(ctx, []byte("x"), []byte("stale-old-owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, []byte("z"), []byte("visible-new-owner"), 2, 0))

	sc := st.NewBackupScanner([]byte(""), nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("z"), kvp.Key)
	require.Equal(t, []byte("visible-new-owner"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerEmptyKeyContinuesAfterPage(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, st.PutAt(ctx, []byte(""), []byte("empty"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("later"), 2, 0))

	sc := st.NewBackupScanner(nil, nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte(""), kvp.Key)
	require.Equal(t, []byte("empty"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("later"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerPebblePagingDoesNotRepeatLastKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	pebbleStore, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, pebbleStore.Close()) })

	groups := map[uint64]*ShardGroup{
		1: {Store: pebbleStore},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))

	sc := st.NewBackupScanner(nil, nil, ^uint64(0), 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("b"), kvp.Key)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestBackupScannerPagesPebbleKeysInLogicalOrder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	pebbleStore, err := store.NewPebbleStore(t.TempDir())
	require.NoError(t, err)
	defer pebbleStore.Close()

	groups := map[uint64]*ShardGroup{
		1: {Store: pebbleStore},
	}
	st := NewShardStore(engine, groups)
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a"), []byte("va"), 10, 0))
	require.NoError(t, pebbleStore.PutAt(ctx, []byte("a\x80"), []byte("vax"), 20, 0))

	sc := st.NewBackupScanner(nil, nil, 20, 1)
	defer sc.Close()

	kvp, ok, err := sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a"), kvp.Key)
	require.Equal(t, []byte("va"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, []byte("a\x80"), kvp.Key)
	require.Equal(t, []byte("vax"), kvp.Value)

	kvp, ok, err = sc.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, kvp)
}

func TestShardStoreScanAt_RoutesS3ManifestScansByLogicalObjectKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := s3keys.ObjectManifestKey("bucket-a", 1, "z/object-0")
	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "z/object-1")
	require.NoError(t, st.PutAt(ctx, k0, []byte("m0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 2, 0))

	start := s3keys.ObjectManifestScanStart("bucket-a", 1, "z/")
	end := prefixScanEnd(start)
	kvs, err := st.ScanAt(ctx, start, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreScanAt_RoutesRedisWideColumnPrefixAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("am"), 1)
	engine.UpdateRoute([]byte("am"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("alice"), []byte("field"))
	right := store.HashFieldKey([]byte("amy"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	start := store.HashFieldScanPrefix([]byte("a"))
	end := prefixScanEnd([]byte(store.HashFieldPrefix))
	kvs, err := st.ScanAt(ctx, start, end, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.ElementsMatch(t, [][]byte{left, right}, [][]byte{kvs[0].Key, kvs[1].Key})
}

func TestShardStoreScanAt_RoutesBareRedisWideColumnFamilyAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("anna"), []byte("field"))
	right := store.HashFieldKey([]byte("zoey"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	prefix := []byte(store.HashFieldPrefix)
	kvs, err := st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, [][]byte{left, right}, [][]byte{kvs[0].Key, kvs[1].Key})
}

func TestShardStoreScanAt_RoutesRedisWideColumnCursorAcrossRemainingShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	left := store.HashFieldKey([]byte("anna"), []byte("field"))
	right := store.HashFieldKey([]byte("zoey"), []byte("field"))
	require.NoError(t, st.PutAt(ctx, left, []byte("left"), 1, 0))
	require.NoError(t, st.PutAt(ctx, right, []byte("right"), 2, 0))

	prefix := []byte(store.HashFieldPrefix)
	kvs, err := st.ScanAt(ctx, nextScanCursor(left), prefixScanEnd(prefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, [][]byte{right}, [][]byte{kvs[0].Key})
}

func TestShardStoreScanAt_RoutesExactRedisWideColumnScanToOneShard(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("am"), 1)
	engine.UpdateRoute([]byte("am"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	t.Cleanup(func() {
		_ = groups[1].Store.Close()
		_ = groups[2].Store.Close()
	})
	st := NewShardStore(engine, groups)

	start := store.HashFieldScanPrefix([]byte("alice"))
	routes, clamp, _ := st.routesForScanWithVersion(start, prefixScanEnd(start))
	require.False(t, clamp)
	require.Len(t, routes, 1)
	require.Equal(t, uint64(1), routes[0].GroupID)
}

func TestShardStoreScanAt_RoutesFilesystemChunkScansByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	start := fskeys.ChunkPrefix(home, inode)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreResolveFilesystemHomeSlot(t *testing.T) {
	t.Parallel()

	boundary := fskeys.ChunkRouteKey(100, 0)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), boundary, 1)
	engine.UpdateRoute(boundary, nil, 2)
	st := NewShardStore(engine, map[uint64]*ShardGroup{})

	home, err := st.ResolveFilesystemHomeSlot(1, 77)
	require.NoError(t, err)
	groupID, ok := st.FilesystemGroupForHome(home, 77)
	require.True(t, ok)
	require.EqualValues(t, 1, groupID)
	require.Less(t, home, uint64(100))

	home, err = st.ResolveFilesystemHomeSlot(2, 77)
	require.NoError(t, err)
	groupID, ok = st.FilesystemGroupForHome(home, 77)
	require.True(t, ok)
	require.EqualValues(t, 2, groupID)
	require.GreaterOrEqual(t, home, uint64(100))

	_, err = st.ResolveFilesystemHomeSlot(3, 77)
	require.ErrorIs(t, err, ErrFilesystemPlacementTargetNotFound)
}

func TestShardStoreFilesystemGroupIDsReturnsPhysicalGroupsSorted(t *testing.T) {
	t.Parallel()

	st := NewShardStore(distribution.NewEngine(), map[uint64]*ShardGroup{
		9: {},
		2: {},
		5: {},
	})
	require.Equal(t, []uint64{2, 5, 9}, st.FilesystemGroupIDs())
}

func TestShardStoreScanAt_RoutesFilesystemUsageCountersAcrossRouteGroups(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	usagePrefix := fskeys.UsageRouteAllPrefix()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), usagePrefix, 1)
	engine.UpdateRoute(usagePrefix, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	key := fskeys.UsageRouteKey(fskeys.InodeKey(22))
	staleOnlyKey := fskeys.UsageRouteKey(fskeys.InodeKey(23))
	require.NoError(t, st.PutAt(ctx, key, []byte("usage"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("stale"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleOnlyKey, []byte("stale-only"), 2, 0))

	kvs, err := st.ScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("usage"), kvs[0].Value)

	keys, err := st.ScanKeysAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{key}, keys)

	kvs, err = st.ReverseScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, key, kvs[0].Key)
	require.Equal(t, []byte("usage"), kvs[0].Value)
}

func TestShardStoreScanAt_RefillsAfterStaleFilesystemUsageCounters(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usagePrefix := fskeys.UsageRouteAllPrefix()
	ownerBoundary := fskeys.InodeKey(50)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), ownerBoundary, 1)
	engine.UpdateRoute(ownerBoundary, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	staleOne := fskeys.UsageRouteKey(fskeys.InodeKey(1))
	staleTwo := fskeys.UsageRouteKey(fskeys.InodeKey(2))
	owned := fskeys.UsageRouteKey(fskeys.InodeKey(99))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleOne, []byte("stale-1"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, staleTwo, []byte("stale-2"), 2, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, owned, []byte("owned"), 3, 0))

	kvs, err := st.ScanAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 1, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, owned, kvs[0].Key)
	require.Equal(t, []byte("owned"), kvs[0].Value)

	keys, err := st.ScanKeysAt(ctx, usagePrefix, prefixScanEnd(usagePrefix), 1, ^uint64(0))
	require.NoError(t, err)
	require.Equal(t, [][]byte{owned}, keys)
}

func TestBackupScannerPrefersFilesystemUsageOwnerCopy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	usagePrefix := fskeys.UsageRouteAllPrefix()
	ownerBoundary := fskeys.InodeKey(50)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), ownerBoundary, 1)
	engine.UpdateRoute(ownerBoundary, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	key := fskeys.UsageRouteKey(fskeys.InodeKey(22))
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("owner"), 1, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("stale"), 2, 0))

	scanner := st.NewBackupScanner(usagePrefix, prefixScanEnd(usagePrefix), ^uint64(0), 1)
	defer scanner.Close()

	pair, ok, err := scanner.Next(ctx)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, key, pair.Key)
	require.Equal(t, []byte("owner"), pair.Value)
	pair, ok, err = scanner.Next(ctx)
	require.NoError(t, err)
	require.False(t, ok)
	require.Nil(t, pair)
}

func TestShardStoreScanAt_RoutesFilesystemChunkSubrangeByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ScanAt(ctx, k0, nextScanCursor(k0), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 1)
	require.Equal(t, k0, kvs[0].Key)
}

func TestShardStoreScanAt_RoutesFilesystemChunkCrossFileSubrangeEndRoute(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inodeA := uint64(22)
	inodeB := uint64(23)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	kA := fskeys.ChunkKey(home, inodeA, 7)
	kB0 := fskeys.ChunkKey(home, inodeB, 0)
	kB5 := fskeys.ChunkKey(home, inodeB, 5)
	require.NoError(t, st.PutAt(ctx, kA, []byte("a7"), 1, 0))
	require.NoError(t, st.PutAt(ctx, kB0, []byte("b0"), 2, 0))
	require.NoError(t, st.PutAt(ctx, kB5, []byte("b5"), 3, 0))

	kvs, err := st.ScanAt(ctx, kA, kB5, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, kA, kvs[0].Key)
	require.Equal(t, kB0, kvs[1].Key)
}

func TestShardStoreScanAt_RoutesFilesystemChunkCrossFileCarriedPrefixEnd(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inodeA := uint64(0xfe)
	inodeB := uint64(0xff)
	routeA := fskeys.ChunkRouteKey(home, inodeA)
	routeB := fskeys.ChunkRouteKey(home, inodeB)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeA, 1)
	engine.UpdateRoute(routeA, routeB, 2)
	engine.UpdateRoute(routeB, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	kA := fskeys.ChunkKey(home, inodeA, 7)
	kB := fskeys.ChunkKey(home, inodeB, 0)
	require.NoError(t, st.PutAt(ctx, kA, []byte("a7"), 1, 0))
	require.NoError(t, st.PutAt(ctx, kB, []byte("b0"), 2, 0))

	kvs, err := st.ScanAt(ctx, kA, prefixScanEnd(fskeys.ChunkPrefix(home, inodeB)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, kA, kvs[0].Key)
	require.Equal(t, kB, kvs[1].Key)
}

func TestShardStoreScanAt_UnboundedFilesystemChunkScanIncludesRawKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	inodeKey := fskeys.InodeKey(99)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, inodeKey, []byte("inode"), 3, 0))

	kvs, err := st.ScanAt(ctx, nextScanCursor(k0), nil, 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, inodeKey, kvs[1].Key)
}

func TestShardStoreScanAt_BoundedFilesystemChunkScanIncludesRawKeys(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	inodeKey := fskeys.InodeKey(99)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))
	require.NoError(t, st.PutAt(ctx, inodeKey, []byte("inode"), 3, 0))

	kvs, err := st.ScanAt(ctx, nextScanCursor(k0), prefixScanEnd(fskeys.InodeAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, inodeKey, kvs[1].Key)
}

func TestShardStoreScanAt_UpperBoundedFilesystemChunkScanIncludesChunkRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreScanAt_NilStartFanoutUsesExplicitGroupForProxy(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	recorder := &recordingRawScanKVServer{}
	addr, stop := startRawKVServer(t, recorder)
	t.Cleanup(stop)

	rawRouteEnd := fskeys.ChunkRouteKey(11, 22)
	chunkRouteEnd := prefixScanEnd(rawRouteEnd)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), rawRouteEnd, 1)
	engine.UpdateRoute(rawRouteEnd, chunkRouteEnd, 2)
	engine.UpdateRoute(chunkRouteEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
		2: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
		3: {Engine: followerEngineForTest(addr), Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)
	t.Cleanup(func() { require.NoError(t, st.Close()) })

	_, err := st.ScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)

	groupIDs := recorder.rawScanGroupIDs()
	require.NotContains(t, groupIDs, uint64(0))
	require.Contains(t, groupIDs, uint64(1))
	require.Contains(t, groupIDs, uint64(2))
}

func TestShardStoreReverseScanAt_UpperBoundedFilesystemChunkScanIncludesChunkRoutes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	kvs, err := st.ReverseScanAt(ctx, nil, prefixScanEnd(fskeys.ChunkAllPrefix()), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k0, kvs[1].Key)
}

type recordingRawScanKVServer struct {
	pb.UnimplementedRawKVServer

	mu       sync.Mutex
	groupIDs []uint64
}

func (s *recordingRawScanKVServer) RawScanAt(_ context.Context, req *pb.RawScanAtRequest) (*pb.RawScanAtResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupIDs = append(s.groupIDs, req.GetGroupId())
	return &pb.RawScanAtResponse{}, nil
}

func (s *recordingRawScanKVServer) rawScanGroupIDs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.groupIDs...)
}

type followerLeaderAddrEngine struct {
	*fakeLeaseEngine
	addr string
}

func followerEngineForTest(addr string) *followerLeaderAddrEngine {
	inner := &fakeLeaseEngine{}
	inner.state.Store(raftengine.StateFollower)
	return &followerLeaderAddrEngine{fakeLeaseEngine: inner, addr: addr}
}

func (e *followerLeaderAddrEngine) Leader() raftengine.LeaderInfo {
	return raftengine.LeaderInfo{ID: "n1", Address: e.addr}
}

func TestShardStoreScanAt_DeduplicatesFilesystemChunkRoutesByGroup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	home := uint64(11)
	inode := uint64(22)
	routeStart := fskeys.ChunkRouteKey(home, inode)
	routeEnd := prefixScanEnd(routeStart)
	routeSplit := append(append([]byte(nil), routeStart...), 0x80)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeStart, 1)
	engine.UpdateRoute(routeStart, routeSplit, 2)
	engine.UpdateRoute(routeSplit, routeEnd, 2)
	engine.UpdateRoute(routeEnd, nil, 3)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
		3: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	start := fskeys.ChunkPrefix(home, inode)
	kvs, err := st.ScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k0, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

func TestShardStoreReverseScanAt_RoutesFilesystemChunkScansByChunkRouteKey(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	home := uint64(11)
	inode := uint64(22)
	routeKey := fskeys.ChunkRouteKey(home, inode)
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), routeKey, 1)
	engine.UpdateRoute(routeKey, nil, 2)
	st := NewShardStore(engine, map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	})

	k0 := fskeys.ChunkKey(home, inode, 0)
	k1 := fskeys.ChunkKey(home, inode, 1)
	require.NoError(t, st.PutAt(ctx, k0, []byte("c0"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k1, []byte("c1"), 2, 0))

	routes, clampToRoutes := st.routesForReverseScan(fskeys.ChunkPrefix(home, inode), prefixScanEnd(fskeys.ChunkPrefix(home, inode)))
	require.False(t, clampToRoutes)
	require.Len(t, routes, 1)
	require.Equal(t, uint64(2), routes[0].GroupID)

	kvs, err := st.ReverseScanAt(ctx, fskeys.ChunkPrefix(home, inode), prefixScanEnd(fskeys.ChunkPrefix(home, inode)), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, k1, kvs[0].Key)
	require.Equal(t, k0, kvs[1].Key)
}

// TestShardStoreReverseScanAt_DescendingOrderAcrossShards verifies that
// ReverseScanAt with a nil start (clampToRoutes=false) merges results from all
// shards and returns them in descending key order.
func TestShardStoreReverseScanAt_DescendingOrderAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	// Shard 1 (keys < "m")
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 2, 0))
	// Shard 2 (keys >= "m")
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 3, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 4, 0))

	// nil start → clampToRoutes=false; both shards must be merged in descending order.
	kvs, err := st.ReverseScanAt(ctx, nil, nil, 4, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 4)
	require.Equal(t, []byte("z"), kvs[0].Key)
	require.Equal(t, []byte("x"), kvs[1].Key)
	require.Equal(t, []byte("c"), kvs[2].Key)
	require.Equal(t, []byte("a"), kvs[3].Key)
}

// TestShardStoreReverseScanAt_LimitAcrossShards verifies that the limit is
// correctly applied when results from multiple shards are merged in descending
// order. The top-N keys across all shards must be returned.
func TestShardStoreReverseScanAt_LimitAcrossShards(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	// Shard 1 (keys < "m")
	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 3, 0))
	// Shard 2 (keys >= "m")
	require.NoError(t, st.PutAt(ctx, []byte("x"), []byte("vx"), 4, 0))
	require.NoError(t, st.PutAt(ctx, []byte("y"), []byte("vy"), 5, 0))
	require.NoError(t, st.PutAt(ctx, []byte("z"), []byte("vz"), 6, 0))

	// limit=4: top-4 in descending order are z, y, x, c.
	kvs, err := st.ReverseScanAt(ctx, nil, nil, 4, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 4)
	require.Equal(t, []byte("z"), kvs[0].Key)
	require.Equal(t, []byte("y"), kvs[1].Key)
	require.Equal(t, []byte("x"), kvs[2].Key)
	require.Equal(t, []byte("c"), kvs[3].Key)
}

// TestShardStoreReverseScanAt_SingleShard verifies that ReverseScanAt on a
// single shard returns results in descending key order.
func TestShardStoreReverseScanAt_SingleShard(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	require.NoError(t, st.PutAt(ctx, []byte("a"), []byte("va"), 1, 0))
	require.NoError(t, st.PutAt(ctx, []byte("b"), []byte("vb"), 2, 0))
	require.NoError(t, st.PutAt(ctx, []byte("c"), []byte("vc"), 3, 0))

	kvs, err := st.ReverseScanAt(ctx, nil, nil, 2, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	require.Equal(t, []byte("c"), kvs[0].Key)
	require.Equal(t, []byte("b"), kvs[1].Key)
}

// TestShardStoreReverseScanAt_IncludesS3ManifestKeysDescending mirrors
// TestShardStoreScanAt_IncludesS3ManifestKeysAcrossShards but for
// ReverseScanAt — results must be returned in descending key order.
func TestShardStoreReverseScanAt_IncludesS3ManifestKeysDescending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	groups := map[uint64]*ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	st := NewShardStore(engine, groups)

	k1 := s3keys.ObjectManifestKey("bucket-a", 1, "alpha")
	k2 := s3keys.ObjectManifestKey("bucket-a", 1, "zeta")
	require.NoError(t, st.PutAt(ctx, k1, []byte("m1"), 1, 0))
	require.NoError(t, st.PutAt(ctx, k2, []byte("m2"), 2, 0))

	start := s3keys.ObjectManifestPrefixForBucket("bucket-a", 1)
	kvs, err := st.ReverseScanAt(ctx, start, prefixScanEnd(start), 10, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, kvs, 2)
	// "zeta" > "alpha" → descending order puts k2 first.
	require.Equal(t, k2, kvs[0].Key)
	require.Equal(t, k1, kvs[1].Key)
}

// TestMergeAndTrimReverseScanResults verifies that the helper merges two
// slices, sorts them in descending key order, and trims to the given limit.
func TestMergeAndTrimReverseScanResults(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("m"), Value: []byte("vm")},
	}
	kvs := []*store.KVPair{
		{Key: []byte("y"), Value: []byte("vy")},
		{Key: []byte("a"), Value: []byte("va")},
	}

	result := mergeAndTrimReverseScanResults(out, kvs, 3)
	require.Len(t, result, 3)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("y"), result[1].Key)
	require.Equal(t, []byte("m"), result[2].Key)
}

func TestMergeAndTrimReverseScanResults_EmptyInput(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{{Key: []byte("z"), Value: []byte("vz")}}
	result := mergeAndTrimReverseScanResults(out, nil, 10)
	require.Len(t, result, 1)
	require.Equal(t, []byte("z"), result[0].Key)
}

func TestMergeAndTrimReverseScanResults_WithinLimit(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{{Key: []byte("z"), Value: []byte("vz")}}
	kvs := []*store.KVPair{{Key: []byte("a"), Value: []byte("va")}}

	result := mergeAndTrimReverseScanResults(out, kvs, 10)
	require.Len(t, result, 2)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("a"), result[1].Key)
}

func TestMergeAndTrimReverseScanResults_ExactLimit(t *testing.T) {
	t.Parallel()

	out := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("c"), Value: []byte("vc")},
	}
	kvs := []*store.KVPair{
		{Key: []byte("y"), Value: []byte("vy")},
		{Key: []byte("a"), Value: []byte("va")},
	}

	// limit=2: top-2 in descending order are "z", "y".
	result := mergeAndTrimReverseScanResults(out, kvs, 2)
	require.Len(t, result, 2)
	require.Equal(t, []byte("z"), result[0].Key)
	require.Equal(t, []byte("y"), result[1].Key)
}

func TestScanLockBoundsForKVs_ReverseOrder(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
		{Key: []byte("b"), Value: []byte("vb")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("d"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, nextScanCursor([]byte("c")), lockEnd)
}

func TestScanLockBoundsForKVsDirection_ReverseUsesReturnedWindow(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("z"), Value: []byte("vz")},
		{Key: []byte("y"), Value: []byte("vy")},
	}

	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, []byte("a"), []byte("zz"), 2, true)
	require.Equal(t, []byte("y"), lockStart)
	require.Equal(t, []byte("zz"), lockEnd)
}

func TestScanLockBoundsForKVs_PreservesOriginalStart(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
		{Key: []byte("e"), Value: []byte("ve")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("z"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, nextScanCursor([]byte("e")), lockEnd)
}

func TestScanLockBoundsForKVs_IncompleteScanUsesOriginalRange(t *testing.T) {
	t.Parallel()

	kvs := []*store.KVPair{
		{Key: []byte("c"), Value: []byte("vc")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte("a"), []byte("z"), 2)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}

func TestScanLockBoundsForKVs_EmptyUsesOriginalRange(t *testing.T) {
	t.Parallel()

	lockStart, lockEnd := scanLockBoundsForKVs(nil, []byte("a"), []byte("z"), 10)
	require.Equal(t, []byte("a"), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}

func TestScanLockBoundsForKVs_FullInternalPageUsesRawPageBound(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	kvs := []*store.KVPair{
		{Key: internalKey, Value: []byte("commit")},
	}

	lockStart, lockEnd := scanLockBoundsForKVs(kvs, []byte(""), nil, 1)
	require.Equal(t, []byte(""), lockStart)
	require.Equal(t, nextScanCursor(internalKey), lockEnd)
}

func TestScanLockBoundsForKVs_ReverseInternalOnlyPageUsesOriginalRange(t *testing.T) {
	t.Parallel()

	internalKey := txnCommitKey([]byte("primary"), 10)
	kvs := []*store.KVPair{
		{Key: internalKey, Value: []byte("commit")},
	}

	lockStart, lockEnd := scanLockBoundsForKVsDirection(kvs, []byte(""), []byte("z"), 1, true)
	require.Equal(t, []byte(""), lockStart)
	require.Equal(t, []byte("z"), lockEnd)
}
