package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
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

	page, err := st.scanRouteAtForwardPage(ctx, distribution.Route{GroupID: 42}, g, []byte(""), nil, 2, ^uint64(0))
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
