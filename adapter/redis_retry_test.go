package adapter

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type retryOnceCoordinator struct {
	store      store.MVCCStore
	clock      *kv.HLC
	dispatches int
}

func newRetryOnceCoordinator(st store.MVCCStore) *retryOnceCoordinator {
	return &retryOnceCoordinator{
		store: st,
		clock: kv.NewHLC(),
	}
}

func (c *retryOnceCoordinator) Dispatch(ctx context.Context, reqs *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.dispatches++
	if c.dispatches == 1 {
		return nil, store.ErrWriteConflict
	}

	ts := c.clock.Next()
	for _, elem := range reqs.Elems {
		var err error
		switch elem.Op {
		case kv.Put:
			err = c.store.PutAt(ctx, elem.Key, elem.Value, ts, 0)
		case kv.Del:
			err = c.store.DeleteAt(ctx, elem.Key, ts)
		case kv.DelPrefix:
			err = c.store.DeletePrefixAt(ctx, elem.Key, nil, ts)
		}
		if err != nil {
			return nil, err
		}
	}

	return &kv.CoordinateResponse{}, nil
}

func (c *retryOnceCoordinator) IsLeader() bool {
	return true
}

func (c *retryOnceCoordinator) VerifyLeader() error {
	return nil
}

func (c *retryOnceCoordinator) RaftLeader() raft.ServerAddress {
	return ""
}

func (c *retryOnceCoordinator) IsLeaderForKey([]byte) bool {
	return true
}

func (c *retryOnceCoordinator) VerifyLeaderForKey([]byte) error {
	return nil
}

func (c *retryOnceCoordinator) RaftLeaderForKey([]byte) raft.ServerAddress {
	return ""
}

func (c *retryOnceCoordinator) Clock() *kv.HLC {
	return c.clock
}

func (c *retryOnceCoordinator) LinearizableRead(_ context.Context) (uint64, error) {
	return 0, nil
}

func (c *retryOnceCoordinator) LeaseRead(ctx context.Context) (uint64, error) {
	return c.LinearizableRead(ctx)
}

func (c *retryOnceCoordinator) LeaseReadForKey(ctx context.Context, _ []byte) (uint64, error) {
	return c.LinearizableRead(ctx)
}

type recordingConn struct {
	ctx  any
	err  string
	bulk []byte
	int  int64
}

func (c *recordingConn) RemoteAddr() string { return "" }
func (c *recordingConn) Close() error       { return nil }
func (c *recordingConn) WriteError(msg string) {
	c.err = msg
}
func (c *recordingConn) WriteString(str string) {
	c.bulk = []byte(str)
}
func (c *recordingConn) WriteBulk(bulk []byte) {
	c.bulk = append([]byte(nil), bulk...)
}
func (c *recordingConn) WriteBulkString(bulk string) {
	c.bulk = []byte(bulk)
}
func (c *recordingConn) WriteInt(num int) {
	c.int = int64(num)
}
func (c *recordingConn) WriteInt64(num int64) {
	c.int = num
}
func (c *recordingConn) WriteUint64(num uint64) {
	if num > math.MaxInt64 {
		c.int = math.MaxInt64
		return
	}
	c.int = int64(num)
}
func (c *recordingConn) WriteArray(count int) {}
func (c *recordingConn) WriteNull() {
	c.bulk = nil
}
func (c *recordingConn) WriteRaw(data []byte) {
	c.bulk = append([]byte(nil), data...)
}
func (c *recordingConn) WriteAny(v any) {
	switch value := v.(type) {
	case string:
		c.WriteBulkString(value)
	case []byte:
		c.WriteBulk(value)
	case int:
		c.WriteInt(value)
	case int64:
		c.WriteInt64(value)
	case uint64:
		c.WriteUint64(value)
	case nil:
		c.WriteNull()
	}
}
func (c *recordingConn) Context() any { return c.ctx }
func (c *recordingConn) SetContext(v any) {
	c.ctx = v
}
func (c *recordingConn) SetReadBuffer(bytes int) {}
func (c *recordingConn) Detach() redcon.DetachedConn {
	return nil
}
func (c *recordingConn) ReadPipeline() []redcon.Command { return nil }
func (c *recordingConn) PeekPipeline() []redcon.Command { return nil }
func (c *recordingConn) NetConn() net.Conn              { return nil }

func TestRedisDelRetriesWriteConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, redisStrKey([]byte("retry:del")), []byte("v1"), 1, 0))

	coord := newRetryOnceCoordinator(st)
	coord.clock.Observe(1)

	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}
	conn := &recordingConn{}

	srv.del(conn, redcon.Command{Args: [][]byte{[]byte(cmdDel), []byte("retry:del")}})

	require.Empty(t, conn.err)
	require.Equal(t, int64(1), conn.int)
	require.Equal(t, 2, coord.dispatches)

	exists, err := st.ExistsAt(ctx, redisStrKey([]byte("retry:del")), snapshotTS(coord.clock, st))
	require.NoError(t, err)
	require.False(t, exists)
}

func TestRedisExecLuaCompatRetriesWriteConflict(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	coord := newRetryOnceCoordinator(st)

	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}
	conn := &recordingConn{}

	srv.execLuaCompat(conn, cmdZAdd, [][]byte{[]byte("retry:z"), []byte("1"), []byte("member-1")})

	require.Empty(t, conn.err)
	require.Equal(t, int64(1), conn.int)
	require.Equal(t, 2, coord.dispatches)

	zset, exists, err := srv.loadZSetAt(ctx, []byte("retry:z"), snapshotTS(coord.clock, st))
	require.NoError(t, err)
	require.True(t, exists)
	require.Len(t, zset.Entries, 1)
	require.Equal(t, "member-1", zset.Entries[0].Member)
	require.Equal(t, 1.0, zset.Entries[0].Score)
}

func TestRedisEvalRetriesWriteConflict(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := newRetryOnceCoordinator(st)

	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}
	conn := &recordingConn{}

	srv.runLuaScript(conn, `redis.call("SET", KEYS[1], ARGV[1]); return redis.call("GET", KEYS[1])`, [][]byte{
		[]byte("1"),
		[]byte("retry:lua"),
		[]byte("v1"),
	})

	require.Empty(t, conn.err)
	require.Equal(t, "v1", string(conn.bulk))
	require.Equal(t, 2, coord.dispatches)

	rawValue, err := srv.readValueAt(redisStrKey([]byte("retry:lua")), snapshotTS(coord.clock, st))
	require.NoError(t, err)
	value, _, err := decodeRedisStr(rawValue)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), value)
}

func TestNormalizeRetryableRedisTxnErrListKey(t *testing.T) {
	t.Parallel()

	internalKey := store.ListItemKey([]byte("retry:list"), 1)
	err := kv.NewTxnLockedError(internalKey)

	normalized := normalizeRetryableRedisTxnErr(err)

	require.ErrorIs(t, normalized, kv.ErrTxnLocked)
	require.ErrorContains(t, normalized, "key: retry:list")
	require.NotContains(t, normalized.Error(), store.ListItemPrefix)
}

func TestNormalizeRetryableRedisTxnErrTxnTTLKey(t *testing.T) {
	t.Parallel()

	internalKey := append([]byte("!txn|cmt|"), redisTTLKey([]byte("retry:ttl"))...)
	internalKey = append(internalKey, make([]byte, 8)...)
	err := store.NewWriteConflictError(internalKey)

	normalized := normalizeRetryableRedisTxnErr(err)

	require.ErrorIs(t, normalized, store.ErrWriteConflict)
	require.ErrorContains(t, normalized, "key: retry:ttl")
	require.NotContains(t, normalized.Error(), "!txn|cmt|")
	require.NotContains(t, normalized.Error(), redisTTLPrefix)
}

func TestNormalizeRetryableRedisTxnErrPreservesTxnLockedDetail(t *testing.T) {
	t.Parallel()

	internalKey := store.ListItemKey([]byte("retry:list"), 2)
	err := errors.WithStack(kv.NewTxnLockedErrorWithDetail(internalKey, "timestamp overflow"))

	normalized := normalizeRetryableRedisTxnErr(err)

	require.ErrorIs(t, normalized, kv.ErrTxnLocked)
	require.ErrorContains(t, normalized, "key: retry:list")
	require.ErrorContains(t, normalized, "timestamp overflow")
	require.NotContains(t, normalized.Error(), store.ListItemPrefix)
}

func TestRetryPolicyForRedisTxnErr(t *testing.T) {
	t.Parallel()

	require.Equal(t, redisWriteConflictRetryPolicy, retryPolicyForRedisTxnErr(store.ErrWriteConflict))
	require.Equal(t, redisTxnLockedRetryPolicy, retryPolicyForRedisTxnErr(kv.ErrTxnLocked))
}

// TestZCard_LegacyBlobZSet verifies that ZCARD inside a Lua script returns the
// correct member count for a ZSet stored in the legacy single-blob format.
// Before the fix, zsetCard fell through to resolveZSetMeta which found no
// wide-column ZSetMetaKey and returned 0 for legacy-blob ZSets.
func TestZCard_LegacyBlobZSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()

	payload, err := marshalZSetValue(redisZSetValue{
		Entries: []redisZSetEntry{
			{Member: "a", Score: 1.0},
			{Member: "b", Score: 2.0},
			{Member: "c", Score: 3.0},
		},
	})
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, redisZSetKey([]byte("legacy:zset")), payload, 1, 0))

	srv := &RedisServer{
		store:       st,
		coordinator: &stubAdapterCoordinator{clock: kv.NewHLC()},
		scriptCache: map[string]string{},
	}
	conn := &recordingConn{}
	srv.runLuaScript(conn, `return redis.call("ZCARD", KEYS[1])`, [][]byte{
		[]byte("1"),
		[]byte("legacy:zset"),
	})

	require.Empty(t, conn.err)
	require.Equal(t, int64(3), conn.int)
}

// TestZAdd_XX_MissingKey_NoPhantom verifies that ZADD with the XX flag on a
// key that does not exist does not create a phantom key visible to subsequent
// EXISTS/TYPE calls within the same script.
func TestZAdd_XX_MissingKey_NoPhantom(t *testing.T) {
	t.Parallel()

	st := store.NewMVCCStore()
	coord := &stubAdapterCoordinator{clock: kv.NewHLC()}
	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}
	conn := &recordingConn{}
	// ZADD XX on a non-existent key must not create the key; EXISTS must return 0.
	srv.runLuaScript(conn, `
		redis.call("ZADD", KEYS[1], "XX", 1, "member")
		return redis.call("EXISTS", KEYS[1])
	`, [][]byte{
		[]byte("1"),
		[]byte("phantom:zset"),
	})

	require.Empty(t, conn.err)
	require.Equal(t, int64(0), conn.int, "ZADD XX on missing key must not create a phantom key")
}

// TestZSetTTLExpiredRecreation verifies that when a ZSet expires via TTL and is
// recreated by Lua ZADD, stale wide-column data (members, score-index, meta, TTL)
// from the expired ZSet is removed. Before the fix, zsetCommitPlan took the delta
// path (preserveExisting=true) because st.exists==false and everDeleted was never
// set, leaving old members and the expired TTL key in storage.
func TestZSetTTLExpiredRecreation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()

	// Write a wide-column ZSet with member "old-member" at ts=1.
	require.NoError(t, st.PutAt(ctx, store.ZSetMemberKey([]byte("expired:zset"), []byte("old-member")), store.MarshalZSetScore(9.0), 1, 0))
	require.NoError(t, st.PutAt(ctx, store.ZSetScoreKey([]byte("expired:zset"), 9.0, []byte("old-member")), []byte{}, 1, 0))
	require.NoError(t, st.PutAt(ctx, store.ZSetMetaKey([]byte("expired:zset")), store.MarshalZSetMeta(store.ZSetMeta{Len: 1}), 1, 0))
	// Set a TTL that is already in the past so the key appears expired.
	require.NoError(t, st.PutAt(ctx, redisTTLKey([]byte("expired:zset")), encodeRedisTTL(time.Unix(0, 0)), 2, 0))

	coord := newRetryOnceCoordinator(st)
	coord.clock.Observe(2)

	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}
	conn := &recordingConn{}
	// Recreate the expired ZSet by adding "new-member".
	srv.runLuaScript(conn, `return redis.call("ZADD", KEYS[1], 1, "new-member")`, [][]byte{
		[]byte("1"),
		[]byte("expired:zset"),
	})

	require.Empty(t, conn.err)
	require.Equal(t, int64(1), conn.int, "ZADD should add new-member")

	// Old member key must be gone.
	oldMemberExists, err := st.ExistsAt(ctx, store.ZSetMemberKey([]byte("expired:zset"), []byte("old-member")), snapshotTS(coord.clock, st))
	require.NoError(t, err)
	require.False(t, oldMemberExists, "old-member from expired ZSet must not survive recreation")

	// Old TTL key must be gone.
	ttlExists, err := st.ExistsAt(ctx, redisTTLKey([]byte("expired:zset")), snapshotTS(coord.clock, st))
	require.NoError(t, err)
	require.False(t, ttlExists, "expired TTL key must not survive ZSet recreation")

	// New member must exist.
	newMemberExists, err := st.ExistsAt(ctx, store.ZSetMemberKey([]byte("expired:zset"), []byte("new-member")), snapshotTS(coord.clock, st))
	require.NoError(t, err)
	require.True(t, newMemberExists, "new-member must be present after recreation")
}

// TestZSetDeltaCommitOnExistingWideColumn verifies that the delta commit path
// (write-only Lua script on a pre-existing wide-column ZSet) correctly adds new
// members without dropping untouched existing ones, and that ZCARD reflects the
// updated count inside the script.
func TestZSetDeltaCommitOnExistingWideColumn(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()

	// Pre-populate wide-column ZSet with member "a" at ts=1.
	require.NoError(t, st.PutAt(ctx, store.ZSetMemberKey([]byte("delta:zset"), []byte("a")), store.MarshalZSetScore(1.0), 1, 0))
	require.NoError(t, st.PutAt(ctx, store.ZSetScoreKey([]byte("delta:zset"), 1.0, []byte("a")), []byte{}, 1, 0))
	require.NoError(t, st.PutAt(ctx, store.ZSetMetaKey([]byte("delta:zset")), store.MarshalZSetMeta(store.ZSetMeta{Len: 1}), 1, 0))

	coord := newRetryOnceCoordinator(st)
	coord.clock.Observe(1)

	srv := &RedisServer{
		store:       st,
		coordinator: coord,
		scriptCache: map[string]string{},
	}
	conn := &recordingConn{}
	// Add "b" and return ZCARD; ZCARD must account for both "a" (existing) and "b" (added).
	srv.runLuaScript(conn, `redis.call("ZADD", KEYS[1], 2, "b"); return redis.call("ZCARD", KEYS[1])`, [][]byte{
		[]byte("1"),
		[]byte("delta:zset"),
	})

	require.Empty(t, conn.err)
	require.Equal(t, int64(2), conn.int, "ZCARD must reflect both existing and newly added member")

	readTS := snapshotTS(coord.clock, st)

	// "a" must still exist after delta commit.
	aExists, err := st.ExistsAt(ctx, store.ZSetMemberKey([]byte("delta:zset"), []byte("a")), readTS)
	require.NoError(t, err)
	require.True(t, aExists, "existing member 'a' must survive delta commit")

	// "b" must have been written.
	bExists, err := st.ExistsAt(ctx, store.ZSetMemberKey([]byte("delta:zset"), []byte("b")), readTS)
	require.NoError(t, err)
	require.True(t, bExists, "new member 'b' must be written by delta commit")
}
