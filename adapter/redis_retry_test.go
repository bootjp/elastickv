package adapter

import (
	"context"
	"math"
	"net"
	"testing"

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

	value, err := srv.readValueAt(redisStrKey([]byte("retry:lua")), snapshotTS(coord.clock, st))
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
