package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisCollectionExpireWritesInlineMetaTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	server := nodes[0].redisServer
	ttl := 30 * time.Second
	cases := []struct {
		name     string
		keyBase  string
		create   func(string) error
		metaKey  func([]byte) []byte
		expireAt func([]byte) (uint64, error)
	}{
		{
			name:    "hash",
			keyBase: "ttl:inline:hash",
			create:  func(key string) error { return rdb.HSet(ctx, key, "field", "value").Err() },
			metaKey: store.HashMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "set",
			keyBase: "ttl:inline:set",
			create:  func(key string) error { return rdb.SAdd(ctx, key, "member").Err() },
			metaKey: store.SetMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "zset",
			keyBase: "ttl:inline:zset",
			create: func(key string) error {
				return rdb.ZAdd(ctx, key, redis.Z{Score: 1, Member: "member"}).Err()
			},
			metaKey: store.ZSetMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "list",
			keyBase: "ttl:inline:list",
			create:  func(key string) error { return rdb.RPush(ctx, key, "item").Err() },
			metaKey: store.ListMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalListMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "stream",
			keyBase: "ttl:inline:stream",
			create: func(key string) error {
				return rdb.XAdd(ctx, &redis.XAddArgs{
					Stream: key,
					Values: map[string]any{"field": "value"},
				}).Err()
			},
			metaKey: store.StreamMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalStreamMeta(raw)
				return meta.ExpireAt, err
			},
		},
	}
	modes := []struct {
		name   string
		expire func(string) error
	}{
		{
			name: "direct",
			expire: func(key string) error {
				return rdb.PExpire(ctx, key, ttl).Err()
			},
		},
		{
			name: "multi",
			expire: func(key string) error {
				pipe := rdb.TxPipeline()
				pipe.PExpire(ctx, key, ttl)
				_, err := pipe.Exec(ctx)
				return err
			},
		},
		{
			name: "lua",
			expire: func(key string) error {
				_, err := rdb.Eval(ctx, `return redis.call("pexpire", KEYS[1], ARGV[1])`, []string{key}, int(ttl/time.Millisecond)).Result()
				return err
			},
		},
	}

	for _, tc := range cases {
		for _, mode := range modes {
			t.Run(tc.name+"/"+mode.name, func(t *testing.T) {
				key := tc.keyBase + ":" + mode.name
				require.NoError(t, tc.create(key))
				require.NoError(t, mode.expire(key))

				readTS := server.readTS()
				raw, err := server.store.GetAt(ctx, tc.metaKey([]byte(key)), readTS)
				require.NoError(t, err)
				expireAtMs, err := tc.expireAt(raw)
				require.NoError(t, err)
				require.Greater(t, expireAtMs, redisExpireAtMillis(time.Now().Add(ttl/2)))

				commitTS, err := server.coordinator.Clock().NextFenced()
				require.NoError(t, err)
				require.NoError(t, server.store.DeleteAt(ctx, redisTTLKey([]byte(key)), commitTS))

				got, err := server.ttlAt(ctx, []byte(key), commitTS)
				require.NoError(t, err)
				require.NotNil(t, got, "ttlAt should read inline metadata after the scan index is deleted")
				require.Equal(t, expireAtMs, redisExpireAtMillis(*got))
			})
		}
	}
}
