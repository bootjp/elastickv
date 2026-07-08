package adapter

import (
	"bytes"
	"context"
	"fmt"
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
		streamDeletions: map[string][]byte{},
		startTS:         redisTxnTestStartTS,
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
	lastReadKeys [][]byte
}

func newReadKeyRecordingCoordinator(st store.MVCCStore) *readKeyRecordingCoordinator {
	return &readKeyRecordingCoordinator{localAdapterCoordinator: newLocalAdapterCoordinator(st)}
}

func (c *readKeyRecordingCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.lastReadKeys = cloneReadKeys(req.ReadKeys)
	return c.localAdapterCoordinator.Dispatch(ctx, req)
}

func cloneReadKeys(in [][]byte) [][]byte {
	out := make([][]byte, 0, len(in))
	for _, key := range in {
		out = append(out, bytes.Clone(key))
	}
	return out
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
	elems := buildHashLegacyRewriteElems(key, map[string][]byte{"field": []byte("value")})
	require.True(t, elemKeysContain(elems, redisTxnWideHashFenceKey(key)))
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
