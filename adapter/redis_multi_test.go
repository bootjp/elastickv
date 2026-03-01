package adapter

import (
	"context"
	"math"
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedis_MultiExecAtomic(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[1].redisAddress})
	ctx := context.Background()

	// start txn
	res := rdb.Do(ctx, "MULTI")
	require.NoError(t, res.Err())
	require.Equal(t, "OK", res.Val())

	// queue writes
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "mk1", "v1").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "mk2", "v2").Val())

	// commit
	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	vals, ok := execRes.([]interface{})
	require.True(t, ok, "exec result should be []interface{}")
	require.Len(t, vals, 2)
	require.Equal(t, "OK", vals[0])
	require.Equal(t, "OK", vals[1])

	// ensure writes visible
	got1, err := rdb.Get(ctx, "mk1").Result()
	require.NoError(t, err)
	require.Equal(t, "v1", got1)
	got2, err := rdb.Get(ctx, "mk2").Result()
	require.NoError(t, err)
	require.Equal(t, "v2", got2)
}

func TestRedis_RPushLRange(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	pushRes, err := rdb.Do(ctx, "RPUSH", "listk", "a", "b", "c").Result()
	require.NoError(t, err)
	length, ok := pushRes.(int64)
	require.True(t, ok, "RPUSH result should be int64")
	require.Equal(t, int64(3), length)

	rangeRes, err := rdb.Do(ctx, "LRANGE", "listk", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []interface{}{"a", "b", "c"}, rangeRes)

	rangeEmpty, err := rdb.Do(ctx, "LRANGE", "listk", 5, 10).Result()
	require.NoError(t, err)
	require.Equal(t, []interface{}{}, rangeEmpty)
}

func TestRedis_DiscardClearsTxn(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[2].redisAddress})
	ctx := context.Background()

	require.NoError(t, rdb.Set(ctx, "dc-key", "before", 0).Err())

	require.Equal(t, "OK", rdb.Do(ctx, "MULTI").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "dc-key", "after").Val())

	require.Equal(t, "OK", rdb.Do(ctx, "DISCARD").Val())

	// exec without MULTI should error
	exec := rdb.Do(ctx, "EXEC")
	require.Error(t, exec.Err())

	// value should remain unchanged
	got, err := rdb.Get(ctx, "dc-key").Result()
	require.NoError(t, err)
	require.Equal(t, "before", got)
}

func TestRedis_DelList_RemovesLargeListAndInternalKeys(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	args := make([]interface{}, 0, 1302)
	args = append(args, "RPUSH", "list-big-del")
	for i := 0; i < 1300; i++ {
		args = append(args, "v"+strconv.Itoa(i))
	}
	_, err := rdb.Do(ctx, args...).Result()
	require.NoError(t, err)

	delCount, err := rdb.Del(ctx, "list-big-del").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), delCount)

	rangeRes, err := rdb.Do(ctx, "LRANGE", "list-big-del", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []interface{}{}, rangeRes)

	readTS := nodes[0].redisServer.readTS()
	_, err = nodes[0].redisServer.store.GetAt(ctx, store.ListMetaKey([]byte("list-big-del")), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	kvs, err := nodes[0].redisServer.store.ScanAt(
		ctx,
		store.ListItemKey([]byte("list-big-del"), math.MinInt64),
		store.ListItemKey([]byte("list-big-del"), math.MaxInt64),
		1,
		readTS,
	)
	require.NoError(t, err)
	assert.Len(t, kvs, 0)
}

func TestRedis_DelList_EmptyAfterDeleteHasNoResidualInternalKeys(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	ctx := context.Background()

	_, err := rdb.Do(ctx, "RPUSH", "list-empty-del", "a").Result()
	require.NoError(t, err)

	_, err = rdb.Del(ctx, "list-empty-del").Result()
	require.NoError(t, err)

	// Second DEL should be a no-op for list internals.
	_, err = rdb.Del(ctx, "list-empty-del").Result()
	require.NoError(t, err)

	readTS := nodes[0].redisServer.readTS()
	_, err = nodes[0].redisServer.store.GetAt(ctx, store.ListMetaKey([]byte("list-empty-del")), readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)

	kvs, err := nodes[0].redisServer.store.ScanAt(
		ctx,
		store.ListItemKey([]byte("list-empty-del"), math.MinInt64),
		store.ListItemKey([]byte("list-empty-del"), math.MaxInt64),
		1,
		readTS,
	)
	require.NoError(t, err)
	assert.Len(t, kvs, 0)
}

func TestRedis_MultiExec_DelThenRPushRecreatesList(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[1].redisAddress})
	ctx := context.Background()

	_, err := rdb.Do(ctx, "RPUSH", "list-del-rpush", "old1", "old2").Result()
	require.NoError(t, err)

	require.Equal(t, "OK", rdb.Do(ctx, "MULTI").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "DEL", "list-del-rpush").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "RPUSH", "list-del-rpush", "new1", "new2").Val())

	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	vals, ok := execRes.([]interface{})
	require.True(t, ok)
	require.Len(t, vals, 2)
	require.Equal(t, int64(1), vals[0])
	require.Equal(t, int64(2), vals[1])

	rangeRes, err := rdb.Do(ctx, "LRANGE", "list-del-rpush", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []interface{}{"new1", "new2"}, rangeRes)

	readTS := nodes[1].redisServer.readTS()
	metaRaw, err := nodes[1].redisServer.store.GetAt(ctx, store.ListMetaKey([]byte("list-del-rpush")), readTS)
	require.NoError(t, err)
	meta, err := store.UnmarshalListMeta(metaRaw)
	require.NoError(t, err)
	require.Equal(t, int64(2), meta.Len)

	kvs, err := nodes[1].redisServer.store.ScanAt(
		ctx,
		store.ListItemKey([]byte("list-del-rpush"), math.MinInt64),
		store.ListItemKey([]byte("list-del-rpush"), math.MaxInt64),
		10,
		readTS,
	)
	require.NoError(t, err)
	require.Len(t, kvs, 2)

	got := make([]string, 0, len(kvs))
	for _, kvp := range kvs {
		got = append(got, string(kvp.Value))
	}
	require.Equal(t, []string{"new1", "new2"}, got)
}
