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
		key      string
		create   func() error
		metaKey  func([]byte) []byte
		expireAt func([]byte) (uint64, error)
	}{
		{
			name:    "hash",
			key:     "ttl:inline:hash",
			create:  func() error { return rdb.HSet(ctx, "ttl:inline:hash", "field", "value").Err() },
			metaKey: store.HashMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalHashMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "set",
			key:     "ttl:inline:set",
			create:  func() error { return rdb.SAdd(ctx, "ttl:inline:set", "member").Err() },
			metaKey: store.SetMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name: "zset",
			key:  "ttl:inline:zset",
			create: func() error {
				return rdb.ZAdd(ctx, "ttl:inline:zset", redis.Z{Score: 1, Member: "member"}).Err()
			},
			metaKey: store.ZSetMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalZSetMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name:    "list",
			key:     "ttl:inline:list",
			create:  func() error { return rdb.RPush(ctx, "ttl:inline:list", "item").Err() },
			metaKey: store.ListMetaKey,
			expireAt: func(raw []byte) (uint64, error) {
				meta, err := store.UnmarshalListMeta(raw)
				return meta.ExpireAt, err
			},
		},
		{
			name: "stream",
			key:  "ttl:inline:stream",
			create: func() error {
				return rdb.XAdd(ctx, &redis.XAddArgs{
					Stream: "ttl:inline:stream",
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

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.NoError(t, tc.create())
			require.NoError(t, rdb.PExpire(ctx, tc.key, ttl).Err())

			readTS := server.readTS()
			raw, err := server.store.GetAt(ctx, tc.metaKey([]byte(tc.key)), readTS)
			require.NoError(t, err)
			expireAtMs, err := tc.expireAt(raw)
			require.NoError(t, err)
			require.Greater(t, expireAtMs, redisExpireAtMillis(time.Now().Add(ttl/2)))

			commitTS, err := server.coordinator.Clock().NextFenced()
			require.NoError(t, err)
			require.NoError(t, server.store.DeleteAt(ctx, redisTTLKey([]byte(tc.key)), commitTS))

			got, err := server.ttlAt(ctx, []byte(tc.key), commitTS)
			require.NoError(t, err)
			require.NotNil(t, got, "ttlAt should read inline metadata after the scan index is deleted")
			require.Equal(t, expireAtMs, redisExpireAtMillis(*got))
		})
	}
}
