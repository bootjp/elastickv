package adapter

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

func newRedisStorageMigrationTestServer(t *testing.T) (*RedisServer, store.MVCCStore) {
	t.Helper()
	st := store.NewMVCCStore()
	server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
	return server, st
}

const redisTxnTestStartTS = 10

func newRedisTxnTestContext(server *RedisServer) *txnContext {
	return &txnContext{
		server:          server,
		working:         map[string]*txnValue{},
		replacers:       map[string]*stringReplacement{},
		listStates:      map[string]*listTxnState{},
		hashStates:      map[string]*hashTxnState{},
		zsetStates:      map[string]*zsetTxnState{},
		ttlStates:       map[string]*ttlTxnState{},
		readKeys:        map[string][]byte{},
		deletedKeys:     map[string]struct{}{},
		logicalDeletes:  map[string][]byte{},
		hashDeletes:     map[string][]byte{},
		setDeletes:      map[string][]byte{},
		hashCreates:     map[string]struct{}{},
		streamDeletions: map[string][]byte{},
		startTS:         redisTxnTestStartTS,
	}
}

type verifyHookCoordinator struct {
	*localAdapterCoordinator
	leaseForKey func(context.Context, []byte) (uint64, error)
	groupForKey func([]byte) uint64
	mu          sync.Mutex
	leaseCalls  int
	leaseKeys   [][]byte
}

func newVerifyHookCoordinator(st store.MVCCStore) *verifyHookCoordinator {
	return &verifyHookCoordinator{localAdapterCoordinator: newLocalAdapterCoordinator(st)}
}

func (c *verifyHookCoordinator) LeaseReadForKey(ctx context.Context, key []byte) (uint64, error) {
	c.mu.Lock()
	c.leaseCalls++
	c.leaseKeys = append(c.leaseKeys, bytes.Clone(key))
	c.mu.Unlock()
	if c.leaseForKey != nil {
		return c.leaseForKey(ctx, key)
	}
	return c.localAdapterCoordinator.LeaseReadForKey(ctx, key)
}

func (c *verifyHookCoordinator) EngineGroupIDForKey(key []byte) uint64 {
	if c.groupForKey != nil {
		return c.groupForKey(key)
	}
	return 1
}

type observingLastCommitStore struct {
	store.MVCCStore
	onLastCommitTS func()
}

func (s *observingLastCommitStore) LastCommitTS() uint64 {
	if s.onLastCommitTS != nil {
		s.onLastCommitTS()
	}
	return s.MVCCStore.LastCommitTS()
}

type redisReadFenceRangeStore struct {
	store.MVCCStore
	rangeKeysByStart map[string][][]byte
}

func (s *redisReadFenceRangeStore) ReadFenceGroupKeysForRange(start []byte, _ []byte) [][]byte {
	if s == nil {
		return nil
	}
	return cloneReadKeys(s.rangeKeysByStart[string(start)])
}

type redisReadFenceRouteVersionStore struct {
	store.MVCCStore
	mu                         sync.Mutex
	routeVersion               uint64
	rangeKeysByVersion         map[uint64][][]byte
	lastReadFenceGroupKeys     [][]byte
	readFenceGroupKeyCallCount int
	onLastCommitTS             func()
	onScanAt                   func()
	onReadFenceGroupKeys       func()
}

func (s *redisReadFenceRouteVersionStore) ReadFenceRouteVersion() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.routeVersion
}

func (s *redisReadFenceRouteVersionStore) advanceRouteVersionForTest() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.routeVersion = 2
}

func (s *redisReadFenceRouteVersionStore) ReadFenceGroupKeysForRange(_ []byte, _ []byte) [][]byte {
	if s == nil {
		return nil
	}
	if s.onReadFenceGroupKeys != nil {
		s.onReadFenceGroupKeys()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readFenceGroupKeyCallCount++
	keys := cloneReadKeys(s.rangeKeysByVersion[s.routeVersion])
	s.lastReadFenceGroupKeys = cloneReadKeys(keys)
	return keys
}

func (s *redisReadFenceRouteVersionStore) readFenceGroupKeyCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.readFenceGroupKeyCallCount
}

func (s *redisReadFenceRouteVersionStore) LastCommitTS() uint64 {
	if s.onLastCommitTS != nil {
		s.onLastCommitTS()
	}
	return s.MVCCStore.LastCommitTS()
}

func (s *redisReadFenceRouteVersionStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if s.onScanAt != nil {
		s.onScanAt()
	}
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func seedRedisListAt(t *testing.T, st store.MVCCStore, key []byte, values ...string) {
	t.Helper()
	metaBytes, err := store.MarshalListMeta(store.ListMeta{Len: int64(len(values))})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), store.ListMetaKey(key), metaBytes, redisTxnTestStartTS, 0))
	for i, value := range values {
		require.NoError(t, st.PutAt(context.Background(), listItemKey(key, int64(i)), []byte(value), redisTxnTestStartTS, 0))
	}
}

func elemKeysContain(elems []*kv.Elem[kv.OP], want []byte) bool {
	for _, elem := range elems {
		if elem != nil && string(elem.Key) == string(want) {
			return true
		}
	}
	return false
}

func requireNoPutElemByKey(t *testing.T, elems []*kv.Elem[kv.OP], want []byte) {
	t.Helper()
	for _, elem := range elems {
		if elem != nil && elem.Op == kv.Put && string(elem.Key) == string(want) {
			t.Fatalf("unexpected put elem key %q", string(want))
		}
	}
}

func requireElemByKey(t *testing.T, elems []*kv.Elem[kv.OP], want []byte) *kv.Elem[kv.OP] {
	t.Helper()
	var found *kv.Elem[kv.OP]
	for _, elem := range elems {
		if elem != nil && string(elem.Key) == string(want) {
			found = elem
		}
	}
	if found != nil {
		return found
	}
	t.Fatalf("missing elem key %q", string(want))
	return nil
}

func requireTTLNear(t *testing.T, raw []byte, want time.Time) {
	t.Helper()
	got, err := decodeRedisTTL(raw)
	require.NoError(t, err)
	require.WithinDuration(t, want, got, 3*time.Second)
}

type readKeyRecordingCoordinator struct {
	*localAdapterCoordinator
	lastReadKeys             [][]byte
	lastObservedRouteVersion uint64
	dispatches               int
	prevCommitTS             []uint64
}

func newReadKeyRecordingCoordinator(st store.MVCCStore) *readKeyRecordingCoordinator {
	return &readKeyRecordingCoordinator{localAdapterCoordinator: newLocalAdapterCoordinator(st)}
}

func (c *readKeyRecordingCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches++
	c.lastReadKeys = cloneReadKeys(req.ReadKeys)
	c.lastObservedRouteVersion = req.ObservedRouteVersion
	c.prevCommitTS = append(c.prevCommitTS, req.PrevCommitTS)
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

type composedRetryCoordinator struct {
	*localAdapterCoordinator
	dispatches   int
	prevCommitTS []uint64
	firstErr     error
}

func (c *composedRetryCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches++
	c.prevCommitTS = append(c.prevCommitTS, req.PrevCommitTS)
	if c.dispatches == 1 {
		if c.firstErr != nil {
			return nil, c.firstErr
		}
		return nil, kv.ErrComposed1Violation
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

type routeChangingDispatchCoordinator struct {
	*verifyHookCoordinator
	dispatches     int
	beforeDispatch func(int)
	firstErr       error
}

func (c *routeChangingDispatchCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches++
	if c.beforeDispatch != nil {
		c.beforeDispatch(c.dispatches)
	}
	if c.dispatches == 1 {
		if c.firstErr != nil {
			return nil, c.firstErr
		}
		return nil, store.NewWriteConflictError([]byte("redis-route-version-retry"))
	}
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

func cloneReadKeys(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, key := range in {
		out = append(out, bytes.Clone(key))
	}
	return out
}

func countReadKey(in [][]byte, want []byte) int {
	count := 0
	for _, key := range in {
		if bytes.Equal(key, want) {
			count++
		}
	}
	return count
}

func readFenceRouteVersionGroupForKey(routeA, routeB []byte) func([]byte) uint64 {
	return func(got []byte) uint64 {
		switch {
		case bytes.Equal(got, routeA):
			return 101
		case bytes.Equal(got, routeB):
			return 102
		default:
			return 1
		}
	}
}

func requireReadKeysMatch(t *testing.T, got [][]byte, want [][]byte) {
	t.Helper()
	gotSet := make(map[string]struct{}, len(got))
	for _, key := range got {
		gotSet[string(key)] = struct{}{}
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, key := range want {
		wantSet[string(key)] = struct{}{}
	}
	require.Equal(t, wantSet, gotSet)
}

type redisTxnFenceRoutingCoordinator struct {
	stubAdapterCoordinator
	defaultLeader bool
	localLeader   func([]byte) bool
	raftLeader    func([]byte) string
	groupID       func([]byte) uint64
}

func (c *redisTxnFenceRoutingCoordinator) IsLeader() bool {
	return c.defaultLeader
}

func (c *redisTxnFenceRoutingCoordinator) IsLeaderForKey(key []byte) bool {
	if c.localLeader != nil {
		return c.localLeader(key)
	}
	return true
}

func (c *redisTxnFenceRoutingCoordinator) RaftLeaderForKey(key []byte) string {
	if c.raftLeader != nil {
		return c.raftLeader(key)
	}
	return ""
}

func (c *redisTxnFenceRoutingCoordinator) EngineGroupIDForKey(key []byte) uint64 {
	if c.groupID != nil {
		return c.groupID(key)
	}
	return 1
}

func TestRedisRangeListVerifiesLeaderBeforeSnapshot(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	key := []byte("list:leader-fence-lrange")
	coord := newVerifyHookCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)

	seeded := false
	coord.leaseForKey = func(_ context.Context, got []byte) (uint64, error) {
		if !seeded {
			seedRedisListAt(t, st, key, "v1")
			seeded = true
		}
		return 0, nil
	}

	got, err := server.rangeList(context.Background(), key, []byte("0"), []byte("-1"))
	require.NoError(t, err)
	require.Equal(t, []string{"v1"}, got)
	require.Equal(t, 2, coord.leaseCalls)
}

func TestRedisRangeListFencesEveryStorageReadGroup(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	key := []byte("list:multi-fence-lrange")
	coord := newVerifyHookCoordinator(st)
	coord.groupForKey = func(got []byte) uint64 {
		switch string(got) {
		case string(redisStrKey(key)):
			return 1
		case string(listMetaKey(key)):
			return 2
		case string(store.ListMetaDeltaScanPrefix(key)):
			return 3
		default:
			return 1
		}
	}
	server := NewRedisServer(nil, "", st, coord, nil, nil)

	var seedOnce sync.Once
	coord.leaseForKey = func(_ context.Context, _ []byte) (uint64, error) {
		seedOnce.Do(func() {
			seedRedisListAt(t, st, key, "v1")
		})
		return 0, nil
	}

	got, err := server.rangeList(context.Background(), key, []byte("0"), []byte("-1"))
	require.NoError(t, err)
	require.Equal(t, []string{"v1"}, got)
	require.Equal(t, 6, coord.leaseCalls)
	require.ElementsMatch(t, [][]byte{
		redisStrKey(key),
		listMetaKey(key),
		store.ListMetaDeltaScanPrefix(key),
		redisStrKey(key),
		listMetaKey(key),
		store.ListMetaDeltaScanPrefix(key),
	}, coord.leaseKeys)
}

func TestRedisRangeListRetriesWhenReadFenceRouteVersionChangesDuringScan(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := []byte("list:route-version-retry")
	routeA := []byte("list-route-a")
	routeB := []byte("list-route-b")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 1,
		rangeKeysByVersion: map[uint64][][]byte{
			1: {routeA},
			2: {routeB},
		},
	}
	seedRedisListAt(t, st, key, "v1")

	var scanOnce sync.Once
	st.onScanAt = func() {
		scanOnce.Do(func() {
			require.NoError(t, st.PutAt(ctx, listItemKey(key, 0), []byte("v2"), 20, 0))
			st.advanceRouteVersionForTest()
		})
	}

	coord := newVerifyHookCoordinator(st)
	coord.groupForKey = readFenceRouteVersionGroupForKey(routeA, routeB)
	server := NewRedisServer(nil, "", st, coord, nil, nil)

	got, err := server.rangeList(ctx, key, []byte("0"), []byte("-1"))
	require.NoError(t, err)
	require.Equal(t, []string{"v2"}, got)
	require.GreaterOrEqual(t, countReadKey(coord.leaseKeys, routeA), 2)
	require.GreaterOrEqual(t, countReadKey(coord.leaseKeys, routeB), 2)
}

func TestRedisExecProxyRouteUsesShardLeaderInsteadOfDefaultLeader(t *testing.T) {
	t.Parallel()

	key := []byte("txn:shard-local")
	coord := &redisTxnFenceRoutingCoordinator{
		defaultLeader: false,
		localLeader: func([]byte) bool {
			return true
		},
		raftLeader: func([]byte) string {
			return "raft-local"
		},
	}
	server := &RedisServer{coordinator: coord, leaderRedis: map[string]string{"raft-remote": "redis-remote"}}

	route, err := server.transactionProxyRoute([]redcon.Command{{
		Args: [][]byte{[]byte(cmdGet), key},
	}})
	require.NoError(t, err)
	require.False(t, route.defaultLeader)
	require.Empty(t, route.key)
}

func TestRedisExecProxyRouteTargetsRemoteShardLeader(t *testing.T) {
	t.Parallel()

	key := []byte("txn:shard-remote")
	coord := &redisTxnFenceRoutingCoordinator{
		defaultLeader: true,
		localLeader: func([]byte) bool {
			return false
		},
		raftLeader: func([]byte) string {
			return "raft-remote"
		},
	}
	server := &RedisServer{coordinator: coord, leaderRedis: map[string]string{"raft-remote": "redis-remote"}}

	route, err := server.transactionProxyRoute([]redcon.Command{{
		Args: [][]byte{[]byte(cmdGet), key},
	}})
	require.NoError(t, err)
	require.False(t, route.defaultLeader)
	require.Equal(t, redisStrKey(key), route.key)
}

func TestRedisExecProxyRouteFailsClosedOnSplitShardLeaders(t *testing.T) {
	t.Parallel()

	keyA := []byte("txn:split-a")
	keyB := []byte("txn:split-b")
	coord := &redisTxnFenceRoutingCoordinator{
		defaultLeader: true,
		localLeader: func([]byte) bool {
			return false
		},
		raftLeader: func(key []byte) string {
			if bytes.Contains(key, keyA) {
				return "raft-a"
			}
			return "raft-b"
		},
		groupID: func(key []byte) uint64 {
			if bytes.Contains(key, keyA) {
				return 1
			}
			return 2
		},
	}
	server := &RedisServer{coordinator: coord, leaderRedis: map[string]string{
		"raft-a": "redis-a",
		"raft-b": "redis-b",
	}}

	_, err := server.transactionProxyRoute([]redcon.Command{
		{Args: [][]byte{[]byte(cmdGet), keyA}},
		{Args: [][]byte{[]byte(cmdGet), keyB}},
	})
	require.ErrorIs(t, err, errRedisExecSplitShardLeaders)
}

func TestRedisExecProxyRouteFailsClosedOnMixedLocalAndRemoteShardLeaders(t *testing.T) {
	t.Parallel()

	localKey := []byte("txn:split-local")
	remoteKey := []byte("txn:split-remote")
	coord := &redisTxnFenceRoutingCoordinator{
		defaultLeader: true,
		localLeader: func(key []byte) bool {
			return bytes.Contains(key, localKey)
		},
		raftLeader: func(key []byte) string {
			if bytes.Contains(key, remoteKey) {
				return "raft-remote"
			}
			return ""
		},
		groupID: func(key []byte) uint64 {
			if bytes.Contains(key, localKey) {
				return 1
			}
			return 2
		},
	}
	server := &RedisServer{coordinator: coord}

	_, err := server.transactionProxyRoute([]redcon.Command{
		{Args: [][]byte{[]byte(cmdGet), localKey}},
		{Args: [][]byte{[]byte(cmdGet), remoteKey}},
	})
	require.ErrorIs(t, err, errRedisExecSplitShardLeaders)
}

func TestRedisExecProxyRouteAllowsDistinctRaftEndpointsWithSameRedisTarget(t *testing.T) {
	t.Parallel()

	keyA := []byte("txn:same-redis-a")
	keyB := []byte("txn:same-redis-b")
	coord := &redisTxnFenceRoutingCoordinator{
		defaultLeader: true,
		localLeader: func([]byte) bool {
			return false
		},
		raftLeader: func(key []byte) string {
			if bytes.Contains(key, keyA) {
				return "raft-a"
			}
			return "raft-b"
		},
		groupID: func(key []byte) uint64 {
			if bytes.Contains(key, keyA) {
				return 1
			}
			return 2
		},
	}
	server := &RedisServer{
		coordinator: coord,
		leaderRedis: map[string]string{
			"raft-a": "redis-remote",
			"raft-b": "redis-remote",
		},
	}

	route, err := server.transactionProxyRoute([]redcon.Command{
		{Args: [][]byte{[]byte(cmdGet), keyA}},
		{Args: [][]byte{[]byte(cmdGet), keyB}},
	})
	require.NoError(t, err)
	require.False(t, route.defaultLeader)
	require.Equal(t, redisStrKey(keyA), route.key)
}

func TestRedisExecReadFenceUsesRangeRoutesAndExactHashFields(t *testing.T) {
	t.Parallel()

	key := []byte("txn:hash-fence-ranges")
	field := []byte("field-b")
	hashPrefix := store.HashFieldScanPrefix(key)
	rangeKeyA := []byte("hash-route-a")
	rangeKeyB := []byte("hash-route-b")
	exactFieldKey := store.HashFieldKey(key, field)
	st := &redisReadFenceRangeStore{
		MVCCStore: store.NewMVCCStore(),
		rangeKeysByStart: map[string][][]byte{
			string(hashPrefix): {rangeKeyA, rangeKeyB},
		},
	}
	coord := newVerifyHookCoordinator(st)
	coord.groupForKey = func(got []byte) uint64 {
		switch string(got) {
		case string(rangeKeyA):
			return 101
		case string(rangeKeyB):
			return 102
		case string(exactFieldKey):
			return 103
		default:
			return 1
		}
	}
	server := &RedisServer{store: st, coordinator: coord}

	got := server.queuedCommandReadFenceGroupKeys([]redcon.Command{{
		Args: [][]byte{[]byte(cmdHSet), key, field, []byte("value")},
	}})

	require.Contains(t, got, rangeKeyA)
	require.Contains(t, got, rangeKeyB)
	require.Contains(t, got, exactFieldKey)
}

func TestRedisExecReadFenceUsesOnlyCommandRelevantRanges(t *testing.T) {
	t.Parallel()

	key := []byte("txn:command-specific-fence-ranges")
	otherKey := []byte("txn:command-specific-fence-ranges-other")
	field := []byte("field-a")
	listTypeRoute := []byte("list-type-route")
	listClaimRoute := []byte("list-claim-route")
	hashFieldRoute := []byte("hash-field-route")
	hashDeltaRoute := []byte("hash-delta-route")
	setMemberRoute := []byte("set-member-route")
	setDeltaRoute := []byte("set-delta-route")
	zsetMemberRoute := []byte("zset-member-route")
	zsetScoreRoute := []byte("zset-score-route")
	zsetDeltaRoute := []byte("zset-delta-route")
	streamEntryRoute := []byte("stream-entry-route")
	otherListTypeRoute := []byte("other-list-type-route")
	exactFieldKey := store.HashFieldKey(key, field)
	st := &redisReadFenceRangeStore{
		MVCCStore: store.NewMVCCStore(),
		rangeKeysByStart: map[string][][]byte{
			string(store.ListMetaDeltaScanPrefix(key)):      {listTypeRoute},
			string(store.ListClaimScanPrefix(key)):          {listClaimRoute},
			string(store.HashFieldScanPrefix(key)):          {hashFieldRoute},
			string(store.HashMetaDeltaScanPrefix(key)):      {hashDeltaRoute},
			string(store.SetMemberScanPrefix(key)):          {setMemberRoute},
			string(store.SetMetaDeltaScanPrefix(key)):       {setDeltaRoute},
			string(store.ZSetMemberScanPrefix(key)):         {zsetMemberRoute},
			string(store.ZSetScoreScanPrefix(key)):          {zsetScoreRoute},
			string(store.ZSetMetaDeltaScanPrefix(key)):      {zsetDeltaRoute},
			string(store.StreamEntryScanPrefix(key)):        {streamEntryRoute},
			string(store.ListMetaDeltaScanPrefix(otherKey)): {otherListTypeRoute},
		},
	}
	coord := newVerifyHookCoordinator(st)
	groupIDsByKey := map[string]uint64{
		string(listTypeRoute):      101,
		string(listClaimRoute):     102,
		string(hashFieldRoute):     103,
		string(hashDeltaRoute):     104,
		string(setMemberRoute):     105,
		string(setDeltaRoute):      106,
		string(zsetMemberRoute):    107,
		string(zsetScoreRoute):     108,
		string(zsetDeltaRoute):     109,
		string(streamEntryRoute):   110,
		string(otherListTypeRoute): 111,
		string(exactFieldKey):      112,
	}
	coord.groupForKey = func(got []byte) uint64 {
		if gid, ok := groupIDsByKey[string(got)]; ok {
			return gid
		}
		return 1
	}
	server := &RedisServer{store: st, coordinator: coord}

	getKeys := server.queuedCommandReadFenceGroupKeys([]redcon.Command{{
		Args: [][]byte{[]byte(cmdGet), key},
	}})
	require.ElementsMatch(t, [][]byte{
		redisStrKey(key),
		listTypeRoute,
		hashFieldRoute,
		hashDeltaRoute,
		setMemberRoute,
		setDeltaRoute,
		zsetMemberRoute,
		zsetDeltaRoute,
	}, getKeys)

	existsKeys := server.queuedCommandReadFenceGroupKeys([]redcon.Command{{
		Args: [][]byte{[]byte(cmdExists), key, otherKey},
	}})
	require.ElementsMatch(t, [][]byte{
		redisStrKey(key),
		listTypeRoute,
		hashFieldRoute,
		hashDeltaRoute,
		setMemberRoute,
		setDeltaRoute,
		zsetMemberRoute,
		zsetDeltaRoute,
		otherListTypeRoute,
	}, existsKeys)

	lrangeKeys := server.queuedCommandReadFenceGroupKeys([]redcon.Command{{
		Args: [][]byte{[]byte(cmdLRange), key, []byte("0"), []byte("-1")},
	}})
	require.ElementsMatch(t, [][]byte{
		redisStrKey(key),
		listTypeRoute,
		listClaimRoute,
	}, lrangeKeys)

	hsetKeys := server.queuedCommandReadFenceGroupKeys([]redcon.Command{{
		Args: [][]byte{[]byte(cmdHSet), key, field, []byte("value")},
	}})
	require.ElementsMatch(t, [][]byte{
		redisStrKey(key),
		hashFieldRoute,
		hashDeltaRoute,
		exactFieldKey,
	}, hsetKeys)
}

func TestRedisReadFencedTimestampLeasesBeforeAndAfterSelectingTimestamp(t *testing.T) {
	t.Parallel()

	base := store.NewMVCCStore()
	coord := newVerifyHookCoordinator(base)
	var leaseCallsAtLastCommitTS int
	st := &observingLastCommitStore{
		MVCCStore: base,
		onLastCommitTS: func() {
			coord.mu.Lock()
			defer coord.mu.Unlock()
			leaseCallsAtLastCommitTS = coord.leaseCalls
		},
	}
	server := &RedisServer{store: st, coordinator: coord}

	startTS, readPin, err := server.redisReadFencedTimestamp(
		context.Background(),
		[][]byte{redisStrKey([]byte("txn:fence-order"))},
		server.txnStartTS,
	)
	defer readPin.Release()

	require.NoError(t, err)
	require.Equal(t, uint64(1), startTS)
	coord.mu.Lock()
	leaseCalls := coord.leaseCalls
	coord.mu.Unlock()
	require.Equal(t, 1, leaseCallsAtLastCommitTS)
	require.Equal(t, 2, leaseCalls)
}

func TestRedisExecRetriesWhenReadFenceRouteVersionChanges(t *testing.T) {
	t.Parallel()

	key := []byte("txn:route-version-retry")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 1,
	}

	var bumpOnce sync.Once
	lastCommitCalls := 0
	st.onLastCommitTS = func() {
		lastCommitCalls++
		bumpOnce.Do(func() {
			st.advanceRouteVersionForTest()
		})
	}

	coord := newVerifyHookCoordinator(st)
	server := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}

	results, err := server.runTransactionDirect([]redcon.Command{{
		Args: [][]byte{[]byte(cmdGet), key},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, resultNil, results[0].typ)
	require.Equal(t, 2, lastCommitCalls)
}

func TestRedisExecDispatchCarriesReadFenceRouteVersion(t *testing.T) {
	t.Parallel()

	key := []byte("txn:observed-route-version")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 7,
		rangeKeysByVersion: map[uint64][][]byte{
			7: {[]byte("txn-observed-route")},
		},
	}
	coord := newReadKeyRecordingCoordinator(st)
	server := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}

	results, err := server.runTransactionDirect([]redcon.Command{{
		Args: [][]byte{[]byte(cmdSet), key, []byte("value")},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, uint64(7), coord.lastObservedRouteVersion)
}

func TestRedisExecDispatchLeavesReadFenceRouteVersionZeroUnpinnedUntilCapabilityGate(t *testing.T) {
	t.Parallel()

	key := []byte("txn:observed-route-version-zero")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 0,
		rangeKeysByVersion: map[uint64][][]byte{
			0: {[]byte("txn-observed-route-zero")},
		},
	}
	coord := newReadKeyRecordingCoordinator(st)
	server := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}

	results, err := server.runTransactionDirect([]redcon.Command{{
		Args: [][]byte{[]byte(cmdSet), key, []byte("value")},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, uint64(0), coord.lastObservedRouteVersion)
}

func TestRedisExecDedupFailsClosedWhenReadFenceRouteVersionChangesAfterAmbiguousAttempt(t *testing.T) {
	t.Parallel()

	key := []byte("txn:route-version-dedup")
	routeA := []byte("txn-dedup-route-a")
	routeB := []byte("txn-dedup-route-b")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 1,
		rangeKeysByVersion: map[uint64][][]byte{
			1: {routeA},
			2: {routeB},
		},
	}
	coord := &routeChangingDispatchCoordinator{
		verifyHookCoordinator: newVerifyHookCoordinator(st),
	}
	coord.groupForKey = readFenceRouteVersionGroupForKey(routeA, routeB)
	coord.beforeDispatch = func(n int) {
		if n == 1 {
			st.advanceRouteVersionForTest()
		}
	}
	server := &RedisServer{
		store:            st,
		coordinator:      coord,
		scriptCache:      map[string]string{},
		onePhaseTxnDedup: true,
	}

	results, err := server.runTransactionWithDedup([]redcon.Command{{
		Args: [][]byte{[]byte(cmdSet), key, []byte("value")},
	}})
	require.ErrorIs(t, err, errRedisExecRouteChangedAfterAmbiguousAttempt)
	require.Nil(t, results)
	require.Equal(t, 1, coord.dispatches)
	require.GreaterOrEqual(t, countReadKey(coord.leaseKeys, routeA), 2)
	require.Zero(t, countReadKey(coord.leaseKeys, routeB))
}

func TestRedisExecDedupRebuildsLockedAttemptWhenReadFenceRouteVersionChanges(t *testing.T) {
	t.Parallel()

	key := []byte("txn:route-version-locked-rebuild")
	routeA := []byte("txn-locked-route-a")
	routeB := []byte("txn-locked-route-b")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 1,
		rangeKeysByVersion: map[uint64][][]byte{
			1: {routeA},
			2: {routeB},
		},
	}
	coord := &routeChangingDispatchCoordinator{
		verifyHookCoordinator: newVerifyHookCoordinator(st),
		firstErr:              kv.NewTxnLockedError([]byte("redis-route-lock")),
	}
	coord.groupForKey = readFenceRouteVersionGroupForKey(routeA, routeB)
	coord.beforeDispatch = func(n int) {
		if n == 1 {
			st.advanceRouteVersionForTest()
		}
	}
	server := &RedisServer{
		store:            st,
		coordinator:      coord,
		scriptCache:      map[string]string{},
		onePhaseTxnDedup: true,
	}

	results, err := server.runTransactionWithDedup([]redcon.Command{{
		Args: [][]byte{[]byte(cmdSet), key, []byte("value")},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, "OK", results[0].str)
	require.Equal(t, 2, coord.dispatches)
	require.GreaterOrEqual(t, countReadKey(coord.leaseKeys, routeA), 2)
	require.GreaterOrEqual(t, countReadKey(coord.leaseKeys, routeB), 2)
}

func TestRedisTxnCommitRechecksReadFenceRouteVersionAfterPrepare(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	key := []byte("txn:prepare-route-version-direct")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 1,
	}
	require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("field")), []byte("old"), redisTxnTestStartTS, 0))
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), redisTxnTestStartTS, 0))

	var bumpOnce sync.Once
	st.onScanAt = func() {
		bumpOnce.Do(func() {
			st.advanceRouteVersionForTest()
		})
	}

	coord := newReadKeyRecordingCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	txn := newRedisTxnTestContext(server)
	txn.ctx = ctx
	txn.logicalDeletes[string(key)] = key

	err := txn.commit(redisReadFenceRouteVersion{tracked: true, version: 1})
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.Zero(t, coord.dispatches)
}

func TestRedisExecDedupRetriesWhenReadFenceRouteVersionChangesDuringPrepare(t *testing.T) {
	t.Parallel()

	key := []byte("txn:prepare-route-version-dedup")
	routeA := []byte("txn-prepare-route-a")
	routeB := []byte("txn-prepare-route-b")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 1,
		rangeKeysByVersion: map[uint64][][]byte{
			1: {routeA},
			2: {routeB},
		},
	}
	var bumpOnce sync.Once
	st.onScanAt = func() {
		bumpOnce.Do(func() {
			st.advanceRouteVersionForTest()
		})
	}

	coord := newReadKeyRecordingCoordinator(st)
	server := &RedisServer{
		store:            st,
		coordinator:      coord,
		scriptCache:      map[string]string{},
		onePhaseTxnDedup: true,
	}

	results, err := server.runTransactionWithDedup([]redcon.Command{{
		Args: [][]byte{[]byte(cmdSet), key, []byte("value")},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 1, coord.dispatches)
	require.Equal(t, uint64(2), coord.lastObservedRouteVersion)
}

func TestRedisExecRetriesComposedRouteRejectionByRebuilding(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		dedup bool
	}{
		{name: "direct", dedup: false},
		{name: "dedup", dedup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			key := []byte("txn:composed-retry:" + tc.name)
			st := store.NewMVCCStore()
			coord := &composedRetryCoordinator{
				localAdapterCoordinator: newLocalAdapterCoordinator(st),
			}
			server := &RedisServer{
				store:            st,
				coordinator:      coord,
				scriptCache:      map[string]string{},
				onePhaseTxnDedup: tc.dedup,
			}

			results, err := server.runTransaction([]redcon.Command{{
				Args: [][]byte{[]byte(cmdSet), key, []byte("value")},
			}})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, "OK", results[0].str)
			require.Equal(t, 2, coord.dispatches)
			require.Equal(t, []uint64{0, 0}, coord.prevCommitTS)
		})
	}
}

func TestRedisExecProxyRouteRetriesWhenReadFenceRouteVersionChanges(t *testing.T) {
	t.Parallel()

	key := []byte("txn:route-version-proxy")
	routeA := []byte("txn-proxy-route-a")
	routeB := []byte("txn-proxy-route-b")
	st := &redisReadFenceRouteVersionStore{
		MVCCStore:    store.NewMVCCStore(),
		routeVersion: 1,
		rangeKeysByVersion: map[uint64][][]byte{
			1: {routeA},
			2: {routeB},
		},
	}
	var bumpOnce sync.Once
	st.onReadFenceGroupKeys = func() {
		bumpOnce.Do(func() {
			st.advanceRouteVersionForTest()
		})
	}
	coord := &redisTxnFenceRoutingCoordinator{
		localLeader: func([]byte) bool {
			return false
		},
		raftLeader: func([]byte) string {
			return "raft-remote"
		},
		groupID: readFenceRouteVersionGroupForKey(routeA, routeB),
	}
	server := &RedisServer{
		store:       st,
		coordinator: coord,
		leaderRedis: map[string]string{"raft-remote": "redis-remote"},
	}

	route, err := server.retryTransactionProxyRoute(context.Background(), []redcon.Command{{
		Args: [][]byte{[]byte(cmdLRange), key, []byte("0"), []byte("-1")},
	}})
	require.NoError(t, err)
	require.Equal(t, redisStrKey(key), route.key)
	st.mu.Lock()
	lastReadFenceGroupKeys := cloneReadKeys(st.lastReadFenceGroupKeys)
	st.mu.Unlock()
	require.Equal(t, [][]byte{routeB}, lastReadFenceGroupKeys)
	require.Greater(t, st.readFenceGroupKeyCalls(), len(redisListReadFenceRanges(key)))
}

func TestRedisExecVerifiesLeaderBeforeSnapshot(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		dedup bool
	}{
		{name: "direct", dedup: false},
		{name: "dedup", dedup: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			st := store.NewMVCCStore()
			key := []byte("list:leader-fence-exec:" + tc.name)
			coord := newVerifyHookCoordinator(st)
			server := &RedisServer{
				store:            st,
				coordinator:      coord,
				scriptCache:      map[string]string{},
				onePhaseTxnDedup: tc.dedup,
			}

			seeded := false
			coord.leaseForKey = func(_ context.Context, _ []byte) (uint64, error) {
				if !seeded {
					seedRedisListAt(t, st, key, "v1")
					seeded = true
				}
				return 0, nil
			}

			results, err := server.runTransaction([]redcon.Command{{
				Args: [][]byte{[]byte(cmdLRange), key, []byte("0"), []byte("-1")},
			}})
			require.NoError(t, err)
			require.Len(t, results, 1)
			require.Equal(t, resultArray, results[0].typ)
			require.Equal(t, []string{"v1"}, results[0].arr)
			require.Equal(t, 2, coord.leaseCalls)
		})
	}
}

func TestRedisExecReadFenceUsesRedisStorageKeys(t *testing.T) {
	t.Parallel()

	key := []byte("!sqs|user-visible")
	keys := redisQueuedCommandReadFenceKeys([]redcon.Command{{
		Args: [][]byte{[]byte(cmdGet), key},
	}})
	keySet := make(map[string]struct{}, len(keys))
	for _, got := range keys {
		keySet[string(got)] = struct{}{}
	}

	require.Contains(t, keySet, string(redisStrKey(key)))
	require.NotContains(t, keySet, string(key))
}

// TestRedisTxnValidateReadSet_ConcurrentRPushTriggersConflict verifies that a
// concurrent RPUSH to a list triggers an OCC read-write conflict for a MULTI
// transaction that read the list via LRANGE.  Without the boundary key tracking
// added to loadListState the validateReadSet call would report no conflict,
// allowing a G2-item anti-dependency cycle to commit undetected.
func TestRedisTxnValidateReadSet_ConcurrentRPushTriggersConflict(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("list:concurrent-rpush")

	// Write a list with Head=0, Len=5 at ts=10.
	metaBytes, err := store.MarshalListMeta(store.ListMeta{Len: 5})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), store.ListMetaKey(key), metaBytes, 10, 0))

	// T1: begin a MULTI/EXEC that reads the list (LRANGE) at startTS=10.
	txn := &txnContext{
		server:     server,
		working:    map[string]*txnValue{},
		listStates: map[string]*listTxnState{},
		zsetStates: map[string]*zsetTxnState{},
		ttlStates:  map[string]*ttlTxnState{},
		readKeys:   map[string][]byte{},
		startTS:    10,
	}
	_, err = txn.loadListState(key)
	require.NoError(t, err)

	// T2: a concurrent RPUSH commits a new item at the tail position (seq=5) at ts=11.
	require.NoError(t, st.PutAt(context.Background(), store.ListItemKey(key, 5), []byte("new"), 11, 0))

	// T1's validateReadSet must detect the read-write conflict via the tracked tail key.
	err = txn.validateReadSet(context.Background())
	require.ErrorIs(t, err, store.ErrWriteConflict,
		"LRANGE in MULTI must conflict with a concurrent RPUSH on the same key (G2-item prevention)")
}

// TestRedisTxnValidateReadSet_ConcurrentLPushTriggersConflict verifies that a
// concurrent LPUSH to a list triggers an OCC read-write conflict for a MULTI
// transaction that read the list via LRANGE.
func TestRedisTxnValidateReadSet_ConcurrentLPushTriggersConflict(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("list:concurrent-lpush")

	// Write a list with Head=0, Len=5 at ts=10.
	metaBytes, err := store.MarshalListMeta(store.ListMeta{Len: 5})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), store.ListMetaKey(key), metaBytes, 10, 0))

	// T1: begin a MULTI/EXEC that reads the list at startTS=10.
	txn := &txnContext{
		server:     server,
		working:    map[string]*txnValue{},
		listStates: map[string]*listTxnState{},
		zsetStates: map[string]*zsetTxnState{},
		ttlStates:  map[string]*ttlTxnState{},
		readKeys:   map[string][]byte{},
		startTS:    10,
	}
	_, err = txn.loadListState(key)
	require.NoError(t, err)

	// T2: a concurrent LPUSH commits a new item at head-1 (seq=-1) at ts=11.
	require.NoError(t, st.PutAt(context.Background(), store.ListItemKey(key, -1), []byte("new"), 11, 0))

	// T1's validateReadSet must detect the read-write conflict via the tracked head-adjacent key.
	err = txn.validateReadSet(context.Background())
	require.ErrorIs(t, err, store.ErrWriteConflict,
		"LRANGE in MULTI must conflict with a concurrent LPUSH on the same key (G2-item prevention)")
}

// TestRedisTxnValidateReadSet_ListMetaUpdateNoConflict verifies that updating
// the base list metadata key (e.g. by a DeltaCompactor) does NOT trigger an
// OCC conflict for append operations.  With the Delta pattern, appenders never
// read-modify-write the base meta key, so compaction is invisible to them.
func TestRedisTxnValidateReadSet_ListMetaUpdateNoConflict(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("list:stale")

	metaV1, err := store.MarshalListMeta(store.ListMeta{Len: 1})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), store.ListMetaKey(key), metaV1, 10, 0))

	txn := &txnContext{
		server:     server,
		working:    map[string]*txnValue{},
		listStates: map[string]*listTxnState{},
		zsetStates: map[string]*zsetTxnState{},
		ttlStates:  map[string]*ttlTxnState{},
		readKeys:   map[string][]byte{},
		startTS:    10,
	}

	_, err = txn.loadListState(key)
	require.NoError(t, err)

	// Simulate a DeltaCompactor updating the base meta after our read.
	metaV2, err := store.MarshalListMeta(store.ListMeta{Len: 2})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), store.ListMetaKey(key), metaV2, 11, 0))

	// With the Delta pattern the base meta key is NOT in the OCC read set,
	// so the compaction write must NOT surface as a write conflict.
	err = txn.validateReadSet(context.Background())
	require.NoError(t, err)
}

// TestRedisTxnValidateReadSet_TTLUpdateNoConflict verifies that a concurrent TTL
// update does NOT trigger an OCC conflict for list append operations. TTL is now
// written via IsTxn=false batch flushes and is excluded from the read set, so
// concurrent EXPIRE/SETEX writes are invisible to data transactions.
func TestRedisTxnValidateReadSet_TTLUpdateNoConflict(t *testing.T) {
	t.Parallel()

	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("list:ttl-no-conflict")

	metaBytes, err := store.MarshalListMeta(store.ListMeta{Len: 1})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(context.Background(), store.ListMetaKey(key), metaBytes, 10, 0))

	txn := &txnContext{
		server:     server,
		working:    map[string]*txnValue{},
		listStates: map[string]*listTxnState{},
		zsetStates: map[string]*zsetTxnState{},
		ttlStates:  map[string]*ttlTxnState{},
		readKeys:   map[string][]byte{},
		startTS:    10,
	}

	_, err = txn.loadListState(key)
	require.NoError(t, err)

	// A concurrent EXPIRE updates the TTL key after our read.
	// Because TTL is no longer tracked in the OCC read set, this must NOT
	// surface as a write conflict.
	require.NoError(t, st.PutAt(context.Background(), redisTTLKey(key), []byte("dummy"), 11, 0))

	err = txn.validateReadSet(context.Background())
	require.NoError(t, err)
}

func TestRedisTxnWideHashDeleteConflictsWithConcurrentNewField(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newOCCAdapterCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	key := []byte("hash:wide-delete-conflict")

	require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("old")), []byte("v"), 10, 0))
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), 10, 0))
	coord.Clock().Observe(10)

	txn := newRedisTxnTestContext(server)
	res, err := txn.stageKeyDeletion(key)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.integer)

	added, err := server.applyHashFieldPairs(key, [][]byte{[]byte("new"), []byte("v")})
	require.NoError(t, err)
	require.Equal(t, 1, added)

	err = txn.validateReadSet(ctx)
	require.ErrorIs(t, err, store.ErrWriteConflict,
		"wide hash DEL in MULTI must conflict with concurrent HSET of a new field")
}

func TestRedisTxnWideSetDeleteConflictsWithConcurrentNewMember(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newOCCAdapterCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	key := []byte("set:wide-delete-conflict")

	require.NoError(t, st.PutAt(ctx, store.SetMemberKey(key, []byte("old")), []byte{}, 10, 0))
	require.NoError(t, st.PutAt(ctx, store.SetMetaKey(key), store.MarshalSetMeta(store.SetMeta{Len: 1}), 10, 0))
	coord.Clock().Observe(10)

	txn := newRedisTxnTestContext(server)
	res, err := txn.stageKeyDeletion(key)
	require.NoError(t, err)
	require.Equal(t, int64(1), res.integer)

	conn := &recordingConn{}
	server.mutateExactSetWide(conn, ctx, key, [][]byte{[]byte("new")}, true)
	require.Empty(t, conn.err)
	require.Equal(t, int64(1), conn.int)

	err = txn.validateReadSet(ctx)
	require.ErrorIs(t, err, store.ErrWriteConflict,
		"wide set DEL in MULTI must conflict with concurrent SADD of a new member")
}

func TestRedisTxnWideFenceKeysUseRedisRoutePrefix(t *testing.T) {
	t.Parallel()

	key := []byte("user:key")
	require.Equal(t, []byte("!redis|txn-wide-hash|user:key"), redisTxnWideHashFenceKey(key))
	require.Equal(t, key, redisTxnWideFenceUserKey(redisTxnWideHashFenceKey(key)))
	require.Equal(t, []byte("!redis|txn-wide-set|user:key"), redisTxnWideSetFenceKey(key))
	require.Equal(t, key, redisTxnWideFenceUserKey(redisTxnWideSetFenceKey(key)))
	require.Equal(t, []byte("!redis|txn-wide-list|user:key"), redisTxnWideListFenceKey(key))
	require.Equal(t, key, redisTxnWideFenceUserKey(redisTxnWideListFenceKey(key)))
	require.Equal(t, []byte("!redis|txn-wide-zset|user:key"), redisTxnWideZSetFenceKey(key))
	require.Equal(t, key, redisTxnWideFenceUserKey(redisTxnWideZSetFenceKey(key)))
	require.Len(t, redisTxnWideCollectionFenceKeys(key), 4)
}

func TestRedisTxnMissingKeyCreatorsReadAllWideFences(t *testing.T) {
	t.Parallel()

	server, _ := newRedisStorageMigrationTestServer(t)
	cases := []struct {
		name  string
		apply func(*testing.T, *txnContext, []byte)
	}{
		{
			name: "incr",
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				res, err := txn.applyIncr(redcon.Command{Args: [][]byte{[]byte(cmdIncr), key}})
				require.NoError(t, err)
				require.Equal(t, int64(1), res.integer)
			},
		},
		{
			name: "hset",
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				res, err := txn.applyHSet(redcon.Command{Args: [][]byte{[]byte(cmdHSet), key, []byte("field"), []byte("value")}})
				require.NoError(t, err)
				require.Equal(t, int64(1), res.integer)
			},
		},
		{
			name: "rpush",
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				res, err := txn.applyRPush(redcon.Command{Args: [][]byte{[]byte(cmdRPush), key, []byte("value")}})
				require.NoError(t, err)
				require.Equal(t, int64(1), res.integer)
			},
		},
		{
			name: "zincrby",
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				res, err := txn.applyZIncrBy(redcon.Command{Args: [][]byte{[]byte(cmdZIncrBy), key, []byte("1"), []byte("member")}})
				require.NoError(t, err)
				require.Equal(t, resultBulk, res.typ)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			txn := newRedisTxnTestContext(server)
			key := []byte("missing:" + tc.name)
			tc.apply(t, txn, key)
			requireTxnReadKeysContainWideFences(t, txn, key)
		})
	}
}

func TestRedisStandaloneHSetDedupAvoidsWideHashMaterializationLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	key := []byte("hash:oversized-wide")

	for i := 0; i <= maxWideColumnItems; i++ {
		field := []byte(fmt.Sprintf("field:%06d", i))
		require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, field), []byte("v"), redisTxnTestStartTS, 0))
	}
	coord.Clock().Observe(redisTxnTestStartTS)

	_, err := server.loadHashAt(ctx, key, redisTxnTestStartTS)
	require.ErrorIs(t, err, ErrCollectionTooLarge)

	results, err := server.runTransactionWithDedup([]redcon.Command{{
		Args: [][]byte{[]byte(cmdHSet), key, []byte("new-field"), []byte("new-value")},
	}})
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, resultInt, results[0].typ)
	require.Equal(t, int64(1), results[0].integer)

	raw, err := st.GetAt(ctx, store.HashFieldKey(key, []byte("new-field")), server.readTS())
	require.NoError(t, err)
	require.Equal(t, []byte("new-value"), raw)
}

func TestRedisTxnHSetAfterDelDoesNotReloadDeletedHash(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
	key := []byte("txn:hash-del-recreate")
	field := []byte("field")
	require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, field), []byte("old"), redisTxnTestStartTS, 0))
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), redisTxnTestStartTS, 0))

	txn := newRedisTxnTestContext(server)
	first, err := txn.applyHSet(redcon.Command{Args: [][]byte{[]byte(cmdHSet), key, field, []byte("updated")}})
	require.NoError(t, err)
	require.Equal(t, int64(0), first.integer)

	delRes, err := txn.applyDel(redcon.Command{Args: [][]byte{[]byte(cmdDel), key}})
	require.NoError(t, err)
	require.Equal(t, int64(1), delRes.integer)

	recreated, err := txn.applyHSet(redcon.Command{Args: [][]byte{[]byte(cmdHSet), key, field, []byte("recreated")}})
	require.NoError(t, err)
	require.Equal(t, int64(1), recreated.integer)

	hashState := txn.hashStates[string(key)]
	require.NotNil(t, hashState)
	require.False(t, hashState.deleted)
	require.Empty(t, hashState.origFields)

	elems := txn.buildHashElems(20)
	deltaElem := requireElemByKey(t, elems, store.HashMetaDeltaKey(key, 20, 0))
	delta, err := store.UnmarshalHashMetaDelta(deltaElem.Value)
	require.NoError(t, err)
	require.Equal(t, int64(1), delta.LenDelta)
}

func TestRedisTxnZIncrByAfterDelDoesNotDiffAgainstDeletedZSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("txn:zset-del-recreate")
	member := []byte("member")
	require.NoError(t, st.PutAt(ctx, store.ZSetMemberKey(key, member), store.MarshalZSetScore(10), redisTxnTestStartTS, 0))
	require.NoError(t, st.PutAt(ctx, store.ZSetScoreKey(key, 10, member), []byte{}, redisTxnTestStartTS, 0))
	require.NoError(t, st.PutAt(ctx, store.ZSetMetaKey(key), store.MarshalZSetMeta(store.ZSetMeta{Len: 1}), redisTxnTestStartTS, 0))

	txn := newRedisTxnTestContext(server)
	delRes, err := txn.applyDel(redcon.Command{Args: [][]byte{[]byte(cmdDel), key}})
	require.NoError(t, err)
	require.Equal(t, int64(1), delRes.integer)

	recreated, err := txn.applyZIncrBy(redcon.Command{Args: [][]byte{[]byte(cmdZIncrBy), key, []byte("1"), member}})
	require.NoError(t, err)
	require.Equal(t, resultBulk, recreated.typ)
	require.Equal(t, []byte("1"), recreated.bulk)

	zsetState := txn.zsetStates[string(key)]
	require.NotNil(t, zsetState)
	require.True(t, zsetState.isWide)
	require.Empty(t, zsetState.origMembers)

	elems, err := txn.buildZSetElems(20)
	require.NoError(t, err)
	deltaElem := requireElemByKey(t, elems, store.ZSetMetaDeltaKey(key, 20, 0))
	delta, err := store.UnmarshalZSetMetaDelta(deltaElem.Value)
	require.NoError(t, err)
	require.Equal(t, int64(1), delta.LenDelta)
}

func requireTxnReadKeysContainWideFences(t *testing.T, txn *txnContext, key []byte) {
	t.Helper()
	for _, fenceKey := range redisTxnWideCollectionFenceKeys(key) {
		require.Contains(t, txn.readKeys, string(fenceKey))
	}
}

func TestRedisTxnMissingKeyCreatorsConflictWithConcurrentWideCreator(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		apply      func(*testing.T, *txnContext, []byte)
		concurrent func(*testing.T, context.Context, *RedisServer, []byte)
	}{
		{
			name: "incr_vs_hash",
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				_, err := txn.applyIncr(redcon.Command{Args: [][]byte{[]byte(cmdIncr), key}})
				require.NoError(t, err)
			},
			concurrent: func(t *testing.T, _ context.Context, server *RedisServer, key []byte) {
				t.Helper()
				added, err := server.applyHashFieldPairs(key, [][]byte{[]byte("field"), []byte("value")})
				require.NoError(t, err)
				require.Equal(t, 1, added)
			},
		},
		{
			name: "hset_vs_list",
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				_, err := txn.applyHSet(redcon.Command{Args: [][]byte{[]byte(cmdHSet), key, []byte("field"), []byte("value")}})
				require.NoError(t, err)
			},
			concurrent: func(t *testing.T, ctx context.Context, server *RedisServer, key []byte) {
				t.Helper()
				length, err := server.listRPush(ctx, key, [][]byte{[]byte("value")})
				require.NoError(t, err)
				require.Equal(t, int64(1), length)
			},
		},
		{
			name: "rpush_vs_zset",
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				_, err := txn.applyRPush(redcon.Command{Args: [][]byte{[]byte(cmdRPush), key, []byte("value")}})
				require.NoError(t, err)
			},
			concurrent: func(t *testing.T, ctx context.Context, server *RedisServer, key []byte) {
				t.Helper()
				score, err := server.zincrbyTxn(ctx, key, "member", 1)
				require.NoError(t, err)
				require.Equal(t, float64(1), score)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			st := store.NewMVCCStore()
			coord := newOCCAdapterCoordinator(st)
			server := NewRedisServer(nil, "", st, coord, nil, nil)
			coord.Clock().Observe(redisTxnTestStartTS)
			key := []byte("missing-conflict:" + tc.name)

			txn := newRedisTxnTestContext(server)
			tc.apply(t, txn, key)
			tc.concurrent(t, ctx, server, key)

			err := txn.validateReadSet(ctx)
			require.ErrorIs(t, err, store.ErrWriteConflict)
		})
	}
}

func TestRedisTxnBuildZSetWideElemsWritesFence(t *testing.T) {
	t.Parallel()

	key := []byte("zset:wide-fence")
	elems, lenDelta := buildZSetWideElems(key, &zsetTxnState{
		members:     map[string]float64{"new": 1},
		origMembers: map[string]float64{},
		isWide:      true,
		exists:      true,
		dirty:       true,
	})

	require.Equal(t, int64(1), lenDelta)
	require.True(t, elemKeysContain(elems, redisTxnWideZSetFenceKey(key)),
		"wide zset writers must update the replacement/delete fence")
}

func TestRedisTxnBuildZSetLegacyElemsWritesFence(t *testing.T) {
	t.Parallel()

	key := []byte("zset:legacy-fence")
	txn := &txnContext{
		zsetStates: map[string]*zsetTxnState{
			string(key): {
				members: map[string]float64{"member": 1},
				dirty:   true,
			},
		},
		replacers: map[string]*stringReplacement{},
	}

	elems, err := txn.buildZSetElems(20)
	require.NoError(t, err)
	require.True(t, elemKeysContain(elems, redisTxnWideZSetFenceKey(key)),
		"legacy zset writers must update the replacement/delete fence")
}

func TestRedisTxnMissingIncrWritesWideFences(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, _ := newRedisStorageMigrationTestServer(t)
	txn := newRedisTxnTestContext(server)
	key := []byte("missing-incr:fence")

	res, err := txn.applyIncr(redcon.Command{Args: [][]byte{[]byte(cmdIncr), key}})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.integer)

	elems, err := txn.buildReplacementElems(ctx)
	require.NoError(t, err)
	for _, fenceKey := range redisTxnWideCollectionFenceKeys(key) {
		require.True(t, elemKeysContain(elems, fenceKey))
	}
}

func TestLuaWideFenceReadKeysForPlan(t *testing.T) {
	t.Parallel()

	key := []byte("lua:fence")
	require.Equal(t, redisTxnWideCollectionFenceKeys(key),
		luaWideFenceReadKeysForPlan(key, redisTypeString, redisTypeNone, false))
	require.Equal(t, redisTxnWideCollectionFenceKeys(key),
		luaWideFenceReadKeysForPlan(key, redisTypeList, redisTypeNone, true))
	require.Equal(t, [][]byte{redisTxnWideZSetFenceKey(key)},
		luaWideFenceReadKeysForPlan(key, redisTypeZSet, redisTypeZSet, true))
	require.Nil(t, luaWideFenceReadKeysForPlan(key, redisTypeString, redisTypeString, true))
}

type luaCleanupScanTrackingStore struct {
	store.MVCCStore
	fullScanStarts [][]byte
}

func (s *luaCleanupScanTrackingStore) ScanAt(ctx context.Context, start []byte, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if limit == store.MaxDeltaScanLimit {
		s.fullScanStarts = append(s.fullScanStarts, bytes.Clone(start))
	}
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

func newLuaCommitPlanTestContext(server *RedisServer, startTS uint64) *luaScriptContext {
	return &luaScriptContext{
		server:       server,
		startTS:      startTS,
		touched:      map[string]struct{}{},
		readKeys:     map[string][]byte{},
		deleted:      map[string]bool{},
		everDeleted:  map[string]bool{},
		negativeType: map[string]bool{},
		strings:      map[string]*luaStringState{},
		lists:        map[string]*luaListState{},
		hashes:       map[string]*luaHashState{},
		sets:         map[string]*luaSetState{},
		zsets:        map[string]*luaZSetState{},
		streams:      map[string]*luaStreamState{},
		ttls:         map[string]*luaTTLState{},
	}
}

func TestLuaCommitPlanForAbsentRewriteSkipsFullLogicalCleanupScans(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	base := store.NewMVCCStore()
	tracking := &luaCleanupScanTrackingStore{MVCCStore: base}
	server := NewRedisServer(nil, "", tracking, newLocalAdapterCoordinator(base), nil, nil)
	key := "lua:absent-rewrite"

	scriptCtx := newLuaCommitPlanTestContext(server, 10)
	scriptCtx.strings[key] = &luaStringState{loaded: true, exists: true, dirty: true, value: []byte("v")}
	scriptCtx.ttls[key] = &luaTTLState{loaded: true}

	plan, err := scriptCtx.commitPlanForKey(ctx, key, 11)
	require.NoError(t, err)
	require.Empty(t, tracking.fullScanStarts)
	require.True(t, elemKeysContain(plan.elems, redisStrKey([]byte(key))))
	require.True(t, elemKeysContain(plan.elems, redisTxnWideHashFenceKey([]byte(key))))
}

func TestLuaCommitPlanForExistingListRewriteOnlyScansListCleanup(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	base := store.NewMVCCStore()
	tracking := &luaCleanupScanTrackingStore{MVCCStore: base}
	server := NewRedisServer(nil, "", tracking, newLocalAdapterCoordinator(base), nil, nil)
	key := []byte("lua:list-rewrite")
	keyString := string(key)
	meta, err := store.MarshalListMeta(store.ListMeta{Len: 1})
	require.NoError(t, err)
	require.NoError(t, base.PutAt(ctx, store.ListMetaKey(key), meta, 10, 0))
	require.NoError(t, base.PutAt(ctx, listItemKey(key, 0), []byte("old"), 10, 0))

	scriptCtx := newLuaCommitPlanTestContext(server, 11)
	scriptCtx.lists[keyString] = &luaListState{
		loaded:       true,
		exists:       true,
		dirty:        true,
		materialized: true,
		values:       []string{"new"},
	}
	scriptCtx.ttls[keyString] = &luaTTLState{loaded: true}

	_, err = scriptCtx.commitPlanForKey(ctx, keyString, 12)
	require.NoError(t, err)
	requireScanStartsIncludePrefix(t, tracking.fullScanStarts, append(append([]byte(nil), []byte(store.ListItemPrefix)...), key...))
	requireScanStartsIncludePrefix(t, tracking.fullScanStarts, store.ListMetaDeltaScanPrefix(key))
	requireScanStartsIncludePrefix(t, tracking.fullScanStarts, store.ListClaimScanPrefix(key))
	requireScanStartsExcludePrefix(t, tracking.fullScanStarts, store.HashFieldScanPrefix(key))
	requireScanStartsExcludePrefix(t, tracking.fullScanStarts, store.SetMemberScanPrefix(key))
	requireScanStartsExcludePrefix(t, tracking.fullScanStarts, store.ZSetMemberScanPrefix(key))
	requireScanStartsExcludePrefix(t, tracking.fullScanStarts, store.ZSetScoreScanPrefix(key))
	requireScanStartsExcludePrefix(t, tracking.fullScanStarts, store.StreamEntryScanPrefix(key))
}

func requireScanStartsIncludePrefix(t *testing.T, starts [][]byte, prefix []byte) {
	t.Helper()
	for _, start := range starts {
		if bytes.HasPrefix(start, prefix) {
			return
		}
	}
	t.Fatalf("expected a scan under prefix %q, got %q", prefix, starts)
}

func requireScanStartsExcludePrefix(t *testing.T, starts [][]byte, prefix []byte) {
	t.Helper()
	for _, start := range starts {
		require.Falsef(t, bytes.HasPrefix(start, prefix), "unexpected scan under prefix %q in %q", prefix, starts)
	}
}

func TestRedisTxnSetReplacementConflictsWithConcurrentWideHashWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newOCCAdapterCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	key := []byte("hash:set-replace-conflict")

	require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("old")), []byte("v"), 10, 0))
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), 10, 0))
	coord.Clock().Observe(10)

	txn := newRedisTxnTestContext(server)
	res, err := txn.applySet(redcon.Command{Args: [][]byte{[]byte(cmdSet), key, []byte("string")}})
	require.NoError(t, err)
	require.Equal(t, "OK", res.str)
	_, err = txn.buildReplacementElems(ctx)
	require.NoError(t, err)

	added, err := server.applyHashFieldPairs(key, [][]byte{[]byte("new"), []byte("v")})
	require.NoError(t, err)
	require.Equal(t, 1, added)

	err = txn.validateReadSet(ctx)
	require.ErrorIs(t, err, store.ErrWriteConflict,
		"SET replacement in MULTI must conflict with concurrent HSET of a new field")
}

func TestRedisTxnSetReplacementConflictsWithConcurrentListPush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newOCCAdapterCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	key := []byte("list:set-replace-conflict")

	metaBytes, err := store.MarshalListMeta(store.ListMeta{Len: 1})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, store.ListMetaKey(key), metaBytes, 10, 0))
	require.NoError(t, st.PutAt(ctx, store.ListItemKey(key, 0), []byte("old"), 10, 0))
	coord.Clock().Observe(10)

	txn := newRedisTxnTestContext(server)
	res, err := txn.applySet(redcon.Command{Args: [][]byte{[]byte(cmdSet), key, []byte("string")}})
	require.NoError(t, err)
	require.Equal(t, "OK", res.str)
	_, err = txn.buildReplacementElems(ctx)
	require.NoError(t, err)

	newLen, err := server.listRPush(ctx, key, [][]byte{[]byte("new")})
	require.NoError(t, err)
	require.Equal(t, int64(2), newLen)

	err = txn.validateReadSet(ctx)
	require.ErrorIs(t, err, store.ErrWriteConflict,
		"SET replacement in MULTI must conflict with concurrent RPUSH on the same key")
}

func TestRedisStandaloneMissingCreatorsReadAllWideFences(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("sadd", func(t *testing.T) {
		t.Parallel()
		st := store.NewMVCCStore()
		coord := newReadKeyRecordingCoordinator(st)
		server := NewRedisServer(nil, "", st, coord, nil, nil)
		key := []byte("missing-create:sadd")

		conn := &recordingConn{}
		server.mutateExactSetWide(conn, ctx, key, [][]byte{[]byte("member")}, true)
		require.Empty(t, conn.err)
		require.Equal(t, int64(1), conn.int)
		requireReadKeysMatch(t, coord.lastReadKeys, redisTxnWideCollectionFenceKeys(key))
	})

	t.Run("rpush", func(t *testing.T) {
		t.Parallel()
		st := store.NewMVCCStore()
		coord := newReadKeyRecordingCoordinator(st)
		server := NewRedisServer(nil, "", st, coord, nil, nil)
		key := []byte("missing-create:rpush")

		n, err := server.listRPush(ctx, key, [][]byte{[]byte("value")})
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		requireReadKeysMatch(t, coord.lastReadKeys, redisTxnWideCollectionFenceKeys(key))
	})

	t.Run("zadd", func(t *testing.T) {
		t.Parallel()
		st := store.NewMVCCStore()
		coord := newReadKeyRecordingCoordinator(st)
		server := NewRedisServer(nil, "", st, coord, nil, nil)
		key := []byte("missing-create:zadd")

		n, err := server.zaddTxn(ctx, key, zaddFlags{}, []zaddPair{{score: 1, member: "member"}})
		require.NoError(t, err)
		require.Equal(t, 1, n)
		requireReadKeysMatch(t, coord.lastReadKeys, redisTxnWideCollectionFenceKeys(key))
	})

	t.Run("hincrby", func(t *testing.T) {
		t.Parallel()
		st := store.NewMVCCStore()
		coord := newReadKeyRecordingCoordinator(st)
		server := NewRedisServer(nil, "", st, coord, nil, nil)
		key := []byte("missing-create:hincrby")

		n, err := server.hincrbyTxn(ctx, key, []byte("field"), 1)
		require.NoError(t, err)
		require.Equal(t, int64(1), n)
		requireReadKeysMatch(t, coord.lastReadKeys, redisTxnWideCollectionFenceKeys(key))
	})

	t.Run("hincrby-legacy-migration-expired", func(t *testing.T) {
		t.Parallel()
		st := store.NewMVCCStore()
		coord := newReadKeyRecordingCoordinator(st)
		server := NewRedisServer(nil, "", st, coord, nil, nil)
		key := []byte("missing-create:hincrby-legacy")

		raw, err := marshalHashValue(redisHashValue{"field": "1"})
		require.NoError(t, err)
		require.NoError(t, st.PutAt(ctx, redisHashKey(key), raw, redisTxnTestStartTS, 0))
		require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(time.Now().Add(-time.Hour)), redisTxnTestStartTS, 0))
		coord.Clock().Observe(redisTxnTestStartTS)

		n, err := server.hincrbyTxn(ctx, key, []byte("field"), 2)
		require.NoError(t, err)
		require.NotZero(t, n)
		requireReadKeysMatch(t, coord.lastReadKeys, redisTxnWideCollectionFenceKeys(key))
	})
}

func TestRedisTxnMissingLRangeConflictsWithConcurrentRPush(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, _ := newRedisStorageMigrationTestServer(t)
	key := []byte("missing-lrange:concurrent-rpush")

	txn := newRedisTxnTestContext(server)
	res, err := txn.applyLRange(redcon.Command{Args: [][]byte{[]byte(cmdLRange), key, []byte("0"), []byte("-1")}})
	require.NoError(t, err)
	require.Equal(t, resultArray, res.typ)
	require.Empty(t, res.arr)
	require.Contains(t, txn.readKeys, string(redisTxnWideListFenceKey(key)))

	newLen, err := server.listRPush(ctx, key, [][]byte{[]byte("value")})
	require.NoError(t, err)
	require.Equal(t, int64(1), newLen)

	err = txn.validateReadSet(ctx)
	require.ErrorIs(t, err, store.ErrWriteConflict,
		"missing-key LRANGE in MULTI must conflict with a concurrent RPUSH on the same key")
}

func TestRedisListPushRechecksTypeAtDispatchSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newLocalAdapterCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	key := []byte("list-push:type-recheck")
	raw, err := marshalHashValue(redisHashValue{"field": "value"})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHashKey(key), raw, redisTxnTestStartTS, 0))
	coord.Clock().Observe(redisTxnTestStartTS)

	_, err = server.listRPush(ctx, key, [][]byte{[]byte("value")})
	require.ErrorContains(t, err, wrongTypeMessage)
}

func TestRedisTxnSetThenExpireUpdatesReplacementTTL(t *testing.T) {
	t.Parallel()

	server, _ := newRedisStorageMigrationTestServer(t)
	txn := newRedisTxnTestContext(server)
	key := []byte("set:then-expire")

	setRes, err := txn.applySet(redcon.Command{Args: [][]byte{[]byte(cmdSet), key, []byte("v")}})
	require.NoError(t, err)
	require.Equal(t, "OK", setRes.str)

	wantExpire := time.Now().Add(20 * time.Second)
	expireRes, err := txn.applyExpire(redcon.Command{Args: [][]byte{[]byte(cmdExpire), key, []byte("20")}}, time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1), expireRes.integer)

	elems, err := txn.buildReplacementElems(context.Background())
	require.NoError(t, err)
	strElem := requireElemByKey(t, elems, redisStrKey(key))
	value, inlineTTL, err := decodeRedisStr(strElem.Value)
	require.NoError(t, err)
	require.Equal(t, []byte("v"), value)
	require.NotNil(t, inlineTTL)
	require.WithinDuration(t, wantExpire, *inlineTTL, 3*time.Second)
	requireTTLNear(t, requireElemByKey(t, elems, redisTTLKey(key)).Value, wantExpire)
}

func TestRedisTxnExpireNXUsesStagedReplacementTTL(t *testing.T) {
	t.Parallel()

	server, _ := newRedisStorageMigrationTestServer(t)
	txn := newRedisTxnTestContext(server)
	key := []byte("set:expire-nx")

	setRes, err := txn.applySet(redcon.Command{Args: [][]byte{[]byte(cmdSet), key, []byte("v"), []byte("EX"), []byte("10")}})
	require.NoError(t, err)
	require.Equal(t, "OK", setRes.str)
	initialTTL := cloneTimePtr(txn.replacers[string(key)].ttl)
	require.NotNil(t, initialTTL)

	expireRes, err := txn.applyExpire(redcon.Command{Args: [][]byte{[]byte(cmdExpire), key, []byte("20"), []byte("NX")}}, time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(0), expireRes.integer)
	require.Equal(t, initialTTL, txn.replacers[string(key)].ttl)
}

func TestRedisTxnSetClearsOldTTLBeforeExpireNX(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("set:clear-old-ttl")
	oldExpire := time.Now().Add(time.Hour)
	require.NoError(t, st.PutAt(ctx, redisStrKey(key), encodeRedisStr([]byte("old"), &oldExpire), 10, 0))
	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(oldExpire), 10, 0))

	txn := newRedisTxnTestContext(server)
	setRes, err := txn.applySet(redcon.Command{Args: [][]byte{[]byte(cmdSet), key, []byte("v")}})
	require.NoError(t, err)
	require.Equal(t, "OK", setRes.str)
	require.Nil(t, txn.replacers[string(key)].ttl)

	wantExpire := time.Now().Add(20 * time.Second)
	expireRes, err := txn.applyExpire(redcon.Command{Args: [][]byte{[]byte(cmdExpire), key, []byte("20"), []byte("NX")}}, time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1), expireRes.integer)

	elems, err := txn.buildReplacementElems(ctx)
	require.NoError(t, err)
	requireTTLNear(t, requireElemByKey(t, elems, redisTTLKey(key)).Value, wantExpire)
}

func TestRedisTxnSetAfterCollectionExpireSkipsInlineTTLRebuild(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("set-after-collection-expire")
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), redisTxnTestStartTS, 0))
	require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("old")), []byte("v"), redisTxnTestStartTS, 0))

	txn := newRedisTxnTestContext(server)
	expireRes, err := txn.applyExpire(redcon.Command{Args: [][]byte{[]byte(cmdExpire), key, []byte("20")}}, time.Second)
	require.NoError(t, err)
	require.Equal(t, int64(1), expireRes.integer)
	require.Contains(t, txn.collectionExpireTypes, string(key))

	setRes, err := txn.applySet(redcon.Command{Args: [][]byte{[]byte(cmdSet), key, []byte("replacement")}})
	require.NoError(t, err)
	require.Equal(t, "OK", setRes.str)
	require.NotContains(t, txn.collectionExpireTypes, string(key))

	replacementElems, err := txn.buildReplacementElems(ctx)
	require.NoError(t, err)
	strElem := requireElemByKey(t, replacementElems, redisStrKey(key))
	value, _, err := decodeRedisStr(strElem.Value)
	require.NoError(t, err)
	require.Equal(t, []byte("replacement"), value)

	collectionTTLElems, skipTTLIndex, err := txn.buildCollectionTTLElems(ctx)
	require.NoError(t, err)
	require.Empty(t, collectionTTLElems)
	require.Empty(t, skipTTLIndex)
	requireNoPutElemByKey(t, append(replacementElems, collectionTTLElems...), store.HashMetaKey(key))
}

func TestRedisTxnCollectionCreateExpireKeepsTTLIndex(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cases := []struct {
		name   string
		create func(*testing.T, *txnContext, []byte) error
	}{
		{
			name: "list",
			create: func(t *testing.T, txn *txnContext, key []byte) error {
				t.Helper()
				res, err := txn.applyRPush(redcon.Command{Args: [][]byte{[]byte(cmdRPush), key, []byte("v")}})
				require.NoError(t, err)
				require.Equal(t, int64(1), res.integer)
				return nil
			},
		},
		{
			name: "hash",
			create: func(t *testing.T, txn *txnContext, key []byte) error {
				t.Helper()
				res, err := txn.applyHSet(redcon.Command{Args: [][]byte{[]byte(cmdHSet), key, []byte("f"), []byte("v")}})
				require.NoError(t, err)
				require.Equal(t, int64(1), res.integer)
				return nil
			},
		},
		{
			name: "zset",
			create: func(t *testing.T, txn *txnContext, key []byte) error {
				t.Helper()
				res, err := txn.applyZIncrBy(redcon.Command{Args: [][]byte{[]byte(cmdZIncrBy), key, []byte("1"), []byte("m")}})
				require.NoError(t, err)
				require.Equal(t, resultBulk, res.typ)
				require.Equal(t, []byte("1"), res.bulk)
				return nil
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server, _ := newRedisStorageMigrationTestServer(t)
			txn := newRedisTxnTestContext(server)
			key := []byte("txn:create-expire-ttl-index:" + tc.name)

			require.NoError(t, tc.create(t, txn, key))
			expireRes, err := txn.applyExpire(redcon.Command{Args: [][]byte{[]byte(cmdPExpire), key, []byte("50000")}}, time.Millisecond)
			require.NoError(t, err)
			require.Equal(t, int64(1), expireRes.integer)

			collectionTTLElems, skipTTLIndex, err := txn.buildCollectionTTLElems(ctx)
			require.NoError(t, err)
			require.Empty(t, collectionTTLElems)
			require.Empty(t, skipTTLIndex)
			requireElemByKey(t, txn.buildTTLElems(skipTTLIndex), redisTTLKey(key))
		})
	}
}

func TestRedisTxnCollectionExpireDeltaOnlyTruncatedReturnsError(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cases := []struct {
		name       string
		deltaKey   func([]byte, uint64) []byte
		deltaValue []byte
	}{
		{
			name:       "hash",
			deltaKey:   func(key []byte, ts uint64) []byte { return store.HashMetaDeltaKey(key, ts, 0) },
			deltaValue: store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1}),
		},
		{
			name:       "set",
			deltaKey:   func(key []byte, ts uint64) []byte { return store.SetMetaDeltaKey(key, ts, 0) },
			deltaValue: store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: 1}),
		},
		{
			name:       "zset",
			deltaKey:   func(key []byte, ts uint64) []byte { return store.ZSetMetaDeltaKey(key, ts, 0) },
			deltaValue: store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server, st := newRedisStorageMigrationTestServer(t)
			key := []byte("txn:ttl:delta-only-truncated:" + tc.name)
			for ts := uint64(1); ts <= store.MaxDeltaScanLimit+1; ts++ {
				require.NoError(t, st.PutAt(ctx, tc.deltaKey(key, ts), tc.deltaValue, ts, 0))
			}

			txn := newRedisTxnTestContext(server)
			txn.startTS = store.MaxDeltaScanLimit + 2
			expireRes, err := txn.applyExpire(redcon.Command{Args: [][]byte{[]byte(cmdPExpire), key, []byte("50000")}}, time.Millisecond)
			require.NoError(t, err)
			require.Equal(t, int64(1), expireRes.integer)

			elems, skipTTLIndex, err := txn.buildCollectionTTLElems(ctx)
			require.ErrorIs(t, err, ErrDeltaScanTruncated)
			require.Empty(t, elems)
			require.Empty(t, skipTTLIndex)
		})
	}
}

func TestRedisTxnSetAfterHLLExpireSkipsDirtyHLLAnchor(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("set-after-hll-expire")
	payload, err := encodeRedisHLL(redisSetValue{Members: []string{"a"}}, nil)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisHLLKey(key), payload, redisTxnTestStartTS, 0))

	txn := newRedisTxnTestContext(server)
	expireRes, err := txn.applyExpire(redcon.Command{Args: [][]byte{[]byte(cmdPExpire), key, []byte("1000")}}, time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, int64(1), expireRes.integer)
	require.True(t, elemKeysContain(txn.buildKeyElems(), redisHLLKey(key)))

	setRes, err := txn.applySet(redcon.Command{Args: [][]byte{[]byte(cmdSet), key, []byte("replacement")}})
	require.NoError(t, err)
	require.Equal(t, "OK", setRes.str)
	require.False(t, elemKeysContain(txn.buildKeyElems(), redisHLLKey(key)))
}

func TestRedisTxnStagedStringWinsOverDeletionOnlyCollectionStates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("del-incr-rpush")
	require.NoError(t, st.PutAt(ctx, redisStrKey(key), encodeRedisStr([]byte("0"), nil), 10, 0))

	txn := newRedisTxnTestContext(server)
	delRes, err := txn.applyDel(redcon.Command{Args: [][]byte{[]byte(cmdDel), key}})
	require.NoError(t, err)
	require.Equal(t, int64(1), delRes.integer)

	incrRes, err := txn.applyIncr(redcon.Command{Args: [][]byte{[]byte(cmdIncr), key}})
	require.NoError(t, err)
	require.Equal(t, int64(1), incrRes.integer)

	typ, err := txn.stagedKeyType(key)
	require.NoError(t, err)
	require.Equal(t, redisTypeString, typ)

	pushRes, err := txn.applyRPush(redcon.Command{Args: [][]byte{[]byte(cmdRPush), key, []byte("x")}})
	require.NoError(t, err)
	require.Equal(t, resultError, pushRes.typ)
	require.Error(t, pushRes.err)
}

func TestRedisTxnExistingWideWritersReadReplacementFences(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cases := []struct {
		name  string
		seed  func(store.MVCCStore, []byte)
		apply func(*testing.T, *txnContext, []byte)
		fence func([]byte) []byte
	}{
		{
			name: "hash",
			seed: func(st store.MVCCStore, key []byte) {
				require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, []byte("old")), []byte("v"), 10, 0))
				require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), 10, 0))
			},
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				res, err := txn.applyHSet(redcon.Command{Args: [][]byte{[]byte(cmdHSet), key, []byte("new"), []byte("v")}})
				require.NoError(t, err)
				require.Equal(t, int64(1), res.integer)
			},
			fence: redisTxnWideHashFenceKey,
		},
		{
			name: "list",
			seed: func(st store.MVCCStore, key []byte) {
				meta, err := store.MarshalListMeta(store.ListMeta{Len: 1, Tail: 1})
				require.NoError(t, err)
				require.NoError(t, st.PutAt(ctx, store.ListMetaKey(key), meta, 10, 0))
				require.NoError(t, st.PutAt(ctx, store.ListItemKey(key, 0), []byte("old"), 10, 0))
			},
			apply: func(t *testing.T, txn *txnContext, key []byte) {
				t.Helper()
				res, err := txn.applyRPush(redcon.Command{Args: [][]byte{[]byte(cmdRPush), key, []byte("new")}})
				require.NoError(t, err)
				require.Equal(t, int64(2), res.integer)
			},
			fence: redisTxnWideListFenceKey,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := store.NewMVCCStore()
			server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
			key := []byte("existing-wide-fence:" + tc.name)
			tc.seed(st, key)

			txn := newRedisTxnTestContext(server)
			tc.apply(t, txn, key)
			fenceKey := tc.fence(key)
			require.Contains(t, txn.readKeys, string(fenceKey))

			require.NoError(t, st.PutAt(ctx, fenceKey, []byte{}, 11, 0))
			require.ErrorIs(t, txn.validateReadSet(ctx), store.ErrWriteConflict)
		})
	}
}

func TestRedisTxnWideDeletionElemsWriteFences(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
	hashKey := []byte("delete-fence:hash")
	setKey := []byte("delete-fence:set")
	require.NoError(t, st.PutAt(ctx, store.HashFieldKey(hashKey, []byte("old")), []byte("v"), 10, 0))
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(hashKey), store.MarshalHashMeta(store.HashMeta{Len: 1}), 10, 0))
	require.NoError(t, st.PutAt(ctx, store.SetMemberKey(setKey, []byte("old")), []byte{}, 10, 0))
	require.NoError(t, st.PutAt(ctx, store.SetMetaKey(setKey), store.MarshalSetMeta(store.SetMeta{Len: 1}), 10, 0))

	txn := newRedisTxnTestContext(server)
	txn.hashDeletes[string(hashKey)] = hashKey
	txn.setDeletes[string(setKey)] = setKey

	hashElems, err := txn.buildHashDeletionElems(ctx)
	require.NoError(t, err)
	require.True(t, elemKeysContain(hashElems, redisTxnWideHashFenceKey(hashKey)))

	setElems, err := txn.buildSetDeletionElems(ctx)
	require.NoError(t, err)
	require.True(t, elemKeysContain(setElems, redisTxnWideSetFenceKey(setKey)))
}

func TestRedisTxnListDeletionElemsWriteFence(t *testing.T) {
	t.Parallel()

	key := []byte("delete-fence:list")
	elems := appendListDeletionElems(nil, key, &listTxnState{
		meta:       store.ListMeta{Len: 1, Tail: 1},
		metaExists: true,
		deleted:    true,
	})
	require.True(t, elemKeysContain(elems, redisTxnWideListFenceKey(key)))
}

func TestRedisTxnHashLegacyRewriteWritesFence(t *testing.T) {
	t.Parallel()

	key := []byte("legacy-rewrite:hash")
	elems := buildHashLegacyRewriteElems(key, map[string][]byte{"field": []byte("value")}, 0)
	require.True(t, elemKeysContain(elems, redisTxnWideHashFenceKey(key)))
}

func TestRedisTxnHashLegacyRewritePreservesTTL(t *testing.T) {
	t.Parallel()

	key := []byte("legacy-rewrite:hash-ttl")
	expireAt := redisExpireAtMillis(time.Now().Add(time.Hour))
	elems := buildHashLegacyRewriteElems(key, map[string][]byte{"field": []byte("value")}, expireAt)
	metaElem := requireElemByKey(t, elems, store.HashMetaKey(key))
	meta, err := store.UnmarshalHashMeta(metaElem.Value)
	require.NoError(t, err)
	require.Equal(t, int64(1), meta.Len)
	require.Equal(t, expireAt, meta.ExpireAt)
}

func TestRedisSetLegacyMigrationWritesFenceWithoutLenDelta(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := NewRedisServer(nil, "", st, newLocalAdapterCoordinator(st), nil, nil)
	key := []byte("legacy-migration:set")
	raw, err := marshalSetValue(redisSetValue{Members: []string{"member"}})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisSetKey(key), raw, 10, 0))

	elems, err := server.buildSetLegacyMigrationElems(ctx, key, 10)
	require.NoError(t, err)
	require.True(t, elemKeysContain(elems, redisTxnWideSetFenceKey(key)))
}

func TestRedisTxnExpiredRecreateConflictsWithConcurrentCollectionWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newOCCAdapterCoordinator(st)
	server := NewRedisServer(nil, "", st, coord, nil, nil)
	key := []byte("expired:recreate-conflict")

	require.NoError(t, st.PutAt(ctx, redisTTLKey(key), encodeRedisTTL(time.Now().Add(-time.Hour)), 10, 0))
	coord.Clock().Observe(10)

	txn := newRedisTxnTestContext(server)
	res, err := txn.applyRPush(redcon.Command{Args: [][]byte{[]byte(cmdRPush), key, []byte("list-value")}})
	require.NoError(t, err)
	require.Equal(t, int64(1), res.integer)

	added, err := server.applyHashFieldPairs(key, [][]byte{[]byte("field"), []byte("hash-value")})
	require.NoError(t, err)
	require.Equal(t, 1, added)

	err = txn.validateReadSet(ctx)
	require.ErrorIs(t, err, store.ErrWriteConflict,
		"expired-key recreate in MULTI must conflict with a concurrent collection recreate")
}

// TestRedisTxnMULTIEXECRetriesOnCoordinatorConflict verifies that runTransaction
// retries the full transaction when the coordinator returns ErrWriteConflict,
// matching the retry behaviour of individual write commands.
func TestRedisTxnMULTIEXECRetriesOnCoordinatorConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newRetryOnceCoordinator(st)

	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}

	// Simulate a queued MULTI/EXEC with a single SET command.
	queue := []redcon.Command{
		{Args: [][]byte{[]byte(cmdSet), []byte("txn:key"), []byte("v1")}},
	}

	results, err := srv.runTransaction(queue)
	require.NoError(t, err)
	require.Len(t, results, 1)
	require.Equal(t, 2, coord.dispatches) // first dispatch fails, second succeeds

	rawVal, err := st.GetAt(ctx, redisStrKey([]byte("txn:key")), snapshotTS(coord.clock, st))
	require.NoError(t, err)
	val, _, err := decodeRedisStr(rawVal)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
}

// TestTxnStartTSUsesLastCommitTS verifies that txnStartTS returns
// store.LastCommitTS() even when the HLC has advanced beyond the last applied
// commit, preventing the lost-write anomaly described in the PR.
// If txnStartTS returned clock.Next() instead, a concurrent write that obtained
// commitTS = lastCommitTS could satisfy latestTS ≤ startTS, silently passing
// the FSM conflict check and causing a lost write.
func TestTxnStartTSUsesLastCommitTS(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()

	// Advance the store's LastCommitTS to a known value.
	const appliedTS = uint64(5)
	require.NoError(t, st.PutAt(ctx, []byte("k"), []byte("v"), appliedTS, 0))
	require.Equal(t, appliedTS, st.LastCommitTS())

	// Advance the HLC well past the applied commit timestamp to simulate
	// the window where clock.Next() is ahead of unapplied Raft entries.
	clock := kv.NewHLC()
	clock.Observe(100)
	// Verify the clock is ahead of the store watermark.
	require.Greater(t, clock.Next(), appliedTS)

	coord := newRetryOnceCoordinator(st)
	coord.clock = clock

	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}

	// txnStartTS must return store.LastCommitTS(), not the HLC value.
	startTS := srv.txnStartTS()
	require.Equal(t, appliedTS, startTS,
		"txnStartTS must equal store.LastCommitTS() to prevent lost-write anomaly")
}
