package adapter

import (
	"context"
	"testing"

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
