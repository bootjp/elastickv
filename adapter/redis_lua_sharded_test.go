package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type luaLateWriteCoordinator struct {
	*kv.ShardedCoordinator
	beforeDispatch func()
	starts         []uint64
}

func (c *luaLateWriteCoordinator) LeaseReadTime(context.Context) (time.Time, error) {
	return time.UnixMilli(500), nil
}

func (c *luaLateWriteCoordinator) Dispatch(ctx context.Context, op *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.starts = append(c.starts, op.StartTS)
	if before := c.beforeDispatch; before != nil {
		c.beforeDispatch = nil
		before()
	}
	return c.ShardedCoordinator.Dispatch(ctx, op)
}

// Deliver a write with an already allocated timestamp after Lua's final read
// index validation, just before its mutation reaches the participant's Raft log.
func TestLuaRetriesWriteAfterFinalValidation(t *testing.T) {
	t.Parallel()
	routes := distribution.NewEngine()
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1, Routes: []distribution.RouteDescriptor{
			{RouteID: 1, End: []byte("z"), GroupID: 2, State: distribution.RouteStateActive},
			{RouteID: 2, Start: []byte("z"), GroupID: 1, State: distribution.RouteStateActive},
		},
	}))
	groups := map[uint64]*kv.ShardGroup{}
	for i, id := range []string{"hot", "counter"} {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { _ = st.Close() })
		engine, stop := newSingleRaftForS3Test(t, id, kv.NewKvFSMWithHLC(st, kv.NewHLC()))
		t.Cleanup(stop)
		groups[uint64(i+1)] = &kv.ShardGroup{Store: st, Engine: engine, Txn: kv.NewLeaderProxyWithEngine(engine)}
	}
	shards := kv.NewShardStore(routes, groups)
	base := kv.NewShardedCoordinator(routes, groups, 1, kv.NewHLC(), shards)
	put := func(key, value []byte, start, commit uint64) {
		_, err := base.Dispatch(t.Context(), &kv.OperationGroup[kv.OP]{
			IsTxn: true, StartTS: start, CommitTS: commit,
			Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: value}},
		})
		require.NoError(t, err)
	}
	key := redisStrKey([]byte("b"))
	put([]byte("z"), []byte("hot"), 1, 100)
	put(key, encodeRedisStr([]byte("0"), nil), 1, 50)
	coord := &luaLateWriteCoordinator{ShardedCoordinator: base, beforeDispatch: func() {
		put(key, encodeRedisStr([]byte("10"), nil), 50, 51)
	}}
	srv := NewRedisServer(nil, "", shards, coord, nil, nil)
	conn := &recordingConn{}
	srv.eval(conn, redcon.Command{Args: [][]byte{
		[]byte("EVAL"), []byte("return redis.call('INCR',KEYS[1])"), []byte("1"), []byte("b"),
	}})
	require.Empty(t, conn.err)
	require.EqualValues(t, 11, conn.int)
	require.Equal(t, []uint64{50, 51}, coord.starts)
	got, err := groups[2].Store.GetAt(t.Context(), key, groups[2].Store.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, encodeRedisStr([]byte("11"), nil), got)
}
