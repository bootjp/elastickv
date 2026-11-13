package adapter

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedis_EmptyWideCollectionIsAbsentBeforeCompaction(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	for _, tc := range []struct {
		name   string
		create func(string) error
		remove func(string) error
	}{
		{name: "hash", create: func(key string) error { return client.HSet(ctx, key, "field", "value").Err() },
			remove: func(key string) error { return client.HDel(ctx, key, "field").Err() }},
		{name: "set", create: func(key string) error { return client.SAdd(ctx, key, "member").Err() },
			remove: func(key string) error { return client.SRem(ctx, key, "member").Err() }},
		{name: "zset", create: func(key string) error {
			return client.ZAdd(ctx, key, redis.Z{Score: 1, Member: "member"}).Err()
		}, remove: func(key string) error { return client.ZRem(ctx, key, "member").Err() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := "empty-wide:" + tc.name
			require.NoError(t, tc.create(key))
			require.NoError(t, tc.remove(key))
			exists, err := client.Exists(ctx, key).Result()
			require.NoError(t, err)
			require.Zero(t, exists)
			typ, err := client.Type(ctx, key).Result()
			require.NoError(t, err)
			require.Equal(t, "none", typ)
			created, err := client.SetNX(ctx, key, "replacement", 0).Result()
			require.NoError(t, err)
			require.True(t, created)
		})
	}
}

func TestRedis_EmptyWideCollectionRecreateDropsOldTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:ttl-recreate"
	require.NoError(t, client.HSet(ctx, key, "field", "value").Err())
	require.NoError(t, client.Expire(ctx, key, time.Minute).Err())
	require.NoError(t, client.HDel(ctx, key, "field").Err())
	require.NoError(t, client.SAdd(ctx, key, "member").Err())
	ttl, err := client.TTL(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
	require.Equal(t, int64(1), client.SCard(ctx, key).Val())
}

func TestRedis_LuaZSetProbeIgnoresEmptyWideZSet(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:lua-zset"
	require.NoError(t, client.ZAdd(ctx, key, redis.Z{Score: 1, Member: "member"}).Err())
	require.NoError(t, client.ZRem(ctx, key, "member").Err())
	physical, err := nodes[0].redisServer.zsetMetaOrDeltaExistsAt(ctx, []byte(key), math.MaxUint64)
	require.NoError(t, err)
	require.False(t, physical)
	require.NoError(t, client.HSet(ctx, key, "field", "value").Err())
	_, err = client.Eval(ctx, `return redis.call('ZADD', KEYS[1], 2, 'other')`, []string{key}).Result()
	require.ErrorContains(t, err, "WRONGTYPE")
	require.Equal(t, "hash", client.Type(ctx, key).Val())
}
