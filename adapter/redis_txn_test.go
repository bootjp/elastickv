package adapter

import (
	"context"
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
