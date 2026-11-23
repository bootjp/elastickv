package adapter

import (
	"context"
	"strconv"
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

type luaShardedFixture struct {
	groups map[uint64]*kv.ShardGroup
	shards *kv.ShardStore
	base   *kv.ShardedCoordinator
}

func newLuaShardedFixture(t *testing.T) luaShardedFixture {
	t.Helper()
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
	return luaShardedFixture{groups, shards, base}
}

func (f luaShardedFixture) put(t *testing.T, key, value []byte, start, commit uint64) {
	t.Helper()
	_, err := f.base.Dispatch(t.Context(), &kv.OperationGroup[kv.OP]{
		IsTxn: true, StartTS: start, CommitTS: commit,
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: value}},
	})
	require.NoError(t, err)
}

// Deliver a write with an already allocated timestamp after Lua's final read
// index validation, just before its mutation reaches the participant's Raft log.
func TestLuaRetriesWriteAfterFinalValidation(t *testing.T) {
	t.Parallel()
	f := newLuaShardedFixture(t)
	key := redisStrKey([]byte("b"))
	f.put(t, []byte("z"), []byte("hot"), 1, 100)
	f.put(t, key, encodeRedisStr([]byte("0"), nil), 1, 50)
	coord := &luaLateWriteCoordinator{ShardedCoordinator: f.base, beforeDispatch: func() {
		f.put(t, key, encodeRedisStr([]byte("10"), nil), 50, 51)
	}}
	srv := NewRedisServer(nil, "", f.shards, coord, nil, nil)
	conn := &recordingConn{}
	srv.eval(conn, redcon.Command{Args: [][]byte{
		[]byte("EVAL"), []byte("return redis.call('INCR',KEYS[1])"), []byte("1"), []byte("b"),
	}})
	require.Empty(t, conn.err)
	require.EqualValues(t, 11, conn.int)
	require.Equal(t, []uint64{50, 51}, coord.starts)
	got, err := f.groups[2].Store.GetAt(t.Context(), key, f.groups[2].Store.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, encodeRedisStr([]byte("11"), nil), got)
}

func TestLuaCrossShardMutationProgress(t *testing.T) {
	for _, tc := range []struct {
		name   string
		seed   bool
		keys   []string
		script string
		want   int64
		stored []string
	}{
		{"unequal participant watermarks", true, []string{"z", "b"}, "return redis.call('INCR',KEYS[1]) + redis.call('INCR',KEYS[2])", 32, []string{"11", "21"}},
		{"empty participant", false, []string{"b"}, "return redis.call('INCR',KEYS[1])", 1, []string{"1"}},
		{"read only lagging participant", true, []string{"z", "b"}, "local v = redis.call('GET',KEYS[2]); redis.call('SET',KEYS[1],v); return tonumber(v)", 20, []string{"20", "20"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newLuaShardedFixture(t)
			if tc.seed {
				f.put(t, redisStrKey([]byte("z")), encodeRedisStr([]byte("10"), nil), 1, 100)
				f.put(t, redisStrKey([]byte("b")), encodeRedisStr([]byte("20"), nil), 1, 50)
			}
			coord := &luaLateWriteCoordinator{ShardedCoordinator: f.base}
			srv := NewRedisServer(nil, "", f.shards, coord, nil, nil)
			conn := &recordingConn{}
			args := [][]byte{[]byte("EVAL"), []byte(tc.script), []byte(strconv.Itoa(len(tc.keys)))}
			for _, key := range tc.keys {
				args = append(args, []byte(key))
			}
			srv.eval(conn, redcon.Command{Args: args})
			require.Empty(t, conn.err)
			require.Equal(t, tc.want, conn.int)
			for i, key := range tc.keys {
				physical := redisStrKey([]byte(key))
				group := f.groups[f.base.EngineGroupIDForKey(physical)]
				value, err := group.Store.GetAt(t.Context(), physical, group.Store.LastCommitTS())
				require.NoError(t, err)
				require.Equal(t, encodeRedisStr([]byte(tc.stored[i]), nil), value)
			}
			require.Len(t, coord.starts, 1, "the original attempt must not submit user mutations before a fresh read")
		})
	}
}
