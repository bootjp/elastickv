package adapter

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestRedis_leader_node_set_get_deleted(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{
		Addr:     nodes[0].redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	key := []byte("test-key")
	want := "v"

	ctx := context.Background()
	res := rdb.Set(ctx, string(key), want, 0)
	assert.NoError(t, res.Err())
	assert.Equal(t, "OK", res.Val())

	res2 := rdb.Get(ctx, string(key))
	assert.NoError(t, res2.Err())
	assert.Equal(t, want, res2.Val())

	res3 := rdb.Del(ctx, string(key))
	assert.NoError(t, res3.Err())
	assert.Equal(t, int64(1), res3.Val())

	res4 := rdb.Get(ctx, string(key))
	assert.Equal(t, redis.Nil, res4.Err())
	assert.Equal(t, "", res4.Val())
}

func TestRedis_follower_redirect_node_set_get_deleted(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{
		Addr:     nodes[1].redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	key := []byte("test-key")
	want := "v"

	ctx := context.Background()
	res := rdb.Set(ctx, string(key), want, 0)
	assert.NoError(t, res.Err())
	assert.Equal(t, "OK", res.Val())

	res2 := rdb.Get(ctx, string(key))
	assert.NoError(t, res2.Err())
	assert.Equal(t, want, res2.Val())

	res3 := rdb.Del(ctx, string(key))
	assert.NoError(t, res3.Err())
	assert.Equal(t, int64(1), res3.Val())

	res4 := rdb.Get(ctx, string(key))
	assert.Equal(t, redis.Nil, res4.Err())
	assert.Equal(t, "", res4.Val())
}

func TestRedis_leader_keys(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{
		Addr:     nodes[0].redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	ctx := context.Background()
	keys := []string{"test-key1", "test-key2", "test-key3"}
	for _, key := range keys {
		want := "v"
		res := rdb.Set(ctx, key, want, 0)
		assert.NoError(t, res.Err())
		assert.Equal(t, "OK", res.Val())
	}

	res := rdb.Keys(ctx, "*")
	assert.NoError(t, res.Err())
	assert.ElementsMatch(t, keys, res.Val())
}
