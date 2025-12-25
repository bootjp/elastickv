package adapter

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
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
