package adapter

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
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

func TestRedis_ZSetProbeIgnoresZeroLengthMetadata(t *testing.T) {
	ctx := context.Background()
	server, st := newRedisStorageMigrationTestServer(t)
	key := []byte("empty-wide:metadata-only-zset")
	require.NoError(t, st.PutAt(ctx, store.ZSetMetaKey(key), store.MarshalZSetMeta(store.ZSetMeta{}), 1, 0))

	memberless, err := server.zsetMetaOrDeltaExistsAt(ctx, key, 2)
	require.NoError(t, err)
	require.False(t, memberless)
	typ, _, err := server.rawZSetPhysTypeAt(ctx, key, 2)
	require.NoError(t, err)
	require.Equal(t, redisTypeNone, typ)
}

func TestRedis_LuaRecreateEmptyHashClearsDeltas(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:lua-recreate-hash"
	script := `return redis.call('HSET', KEYS[1], 'field', 'value')`
	require.NoError(t, client.Eval(ctx, script, []string{key}).Err())
	require.NoError(t, client.HDel(ctx, key, "field").Err())
	require.NoError(t, client.Eval(ctx, script, []string{key}).Err())
	length, err := client.HLen(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), length)
}

func TestRedis_LuaRecreateEmptyHashDropsOldTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:lua-recreate-ttl"
	require.NoError(t, client.HSet(ctx, key, "field", "value").Err())
	require.NoError(t, client.Expire(ctx, key, time.Minute).Err())
	require.NoError(t, client.HDel(ctx, key, "field").Err())
	require.NoError(t, client.Eval(ctx, `return redis.call('SADD', KEYS[1], 'member')`, []string{key}).Err())
	ttl, err := client.TTL(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
	require.Equal(t, int64(1), client.SCard(ctx, key).Val())
	pttlKey := key + ":pttl"
	require.NoError(t, client.HSet(ctx, pttlKey, "field", "value").Err())
	require.NoError(t, client.Expire(ctx, pttlKey, time.Minute).Err())
	require.NoError(t, client.HDel(ctx, pttlKey, "field").Err())
	remaining, err := client.Eval(ctx, `redis.call('SADD', KEYS[1], 'member'); return redis.call('PTTL', KEYS[1])`, []string{pttlKey}).Int64()
	require.NoError(t, err)
	require.Equal(t, int64(-1), remaining)

	withTTL := key + ":explicit"
	require.NoError(t, client.HSet(ctx, withTTL, "field", "value").Err())
	require.NoError(t, client.Expire(ctx, withTTL, time.Minute).Err())
	require.NoError(t, client.HDel(ctx, withTTL, "field").Err())
	require.NoError(t, client.Eval(ctx, `redis.call('SADD', KEYS[1], 'member'); return redis.call('PEXPIRE', KEYS[1], 30000)`, []string{withTTL}).Err())
	ttl, err = client.TTL(ctx, withTTL).Result()
	require.NoError(t, err)
	require.Positive(t, ttl)
	keepTTL := key + ":keep-ttl"
	require.NoError(t, client.HSet(ctx, keepTTL, "field", "value").Err())
	require.NoError(t, client.Expire(ctx, keepTTL, time.Minute).Err())
	require.NoError(t, client.HDel(ctx, keepTTL, "field").Err())
	require.NoError(t, client.Eval(ctx, `return redis.call('SET', KEYS[1], 'replacement', 'KEEPTTL')`, []string{keepTTL}).Err())
	ttl, err = client.TTL(ctx, keepTTL).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
	zsetKey := key + ":zset"
	require.NoError(t, client.HSet(ctx, zsetKey, "field", "value").Err())
	require.NoError(t, client.Expire(ctx, zsetKey, time.Minute).Err())
	require.NoError(t, client.HDel(ctx, zsetKey, "field").Err())
	require.NoError(t, client.Eval(ctx, `return redis.call('ZADD', KEYS[1], 1, 'member')`, []string{zsetKey}).Err())
	ttl, err = client.TTL(ctx, zsetKey).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
	require.Equal(t, int64(1), client.ZCard(ctx, zsetKey).Val())
}

func TestRedis_MultiRecreateEmptyHashDropsOldTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:multi-zincrby"
	require.NoError(t, client.HSet(ctx, key, "field", "value").Err())
	require.NoError(t, client.Expire(ctx, key, time.Minute).Err())
	require.NoError(t, client.HDel(ctx, key, "field").Err())
	pipe := client.TxPipeline()
	pipe.ZIncrBy(ctx, key, 1, "member")
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
	ttl, err := client.TTL(ctx, key).Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
	require.Equal(t, int64(1), client.ZCard(ctx, key).Val())
}

func TestRedis_MultiListRecreateEmptyHashDropsOldTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:multi-list-from-hash"
	require.NoError(t, client.HSet(ctx, key, "field", "value").Err())
	require.NoError(t, client.Expire(ctx, key, time.Minute).Err())
	require.NoError(t, client.HDel(ctx, key, "field").Err())
	pipe := client.TxPipeline()
	pipe.RPush(ctx, key, "item")
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"item"}, client.LRange(ctx, key, 0, -1).Val())
	require.Equal(t, time.Duration(-1), client.TTL(ctx, key).Val())
}

func TestRedis_MultiHashRecreateEmptySetDropsOldTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:multi-hash-from-set"
	require.NoError(t, client.SAdd(ctx, key, "member").Err())
	require.NoError(t, client.Expire(ctx, key, time.Minute).Err())
	require.NoError(t, client.SRem(ctx, key, "member").Err())
	pipe := client.TxPipeline()
	pipe.HSet(ctx, key, "field", "value")
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, "value", client.HGet(ctx, key, "field").Val())
	require.Equal(t, time.Duration(-1), client.TTL(ctx, key).Val())
	exists, err := nodes[0].redisServer.store.ExistsAt(ctx, store.SetMetaKey([]byte(key)), math.MaxUint64)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestRedis_MultiListRecreateEmptyHashWithoutTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:multi-list-from-hash-no-ttl"
	require.NoError(t, client.HSet(ctx, key, "field", "value").Err())
	require.NoError(t, client.HDel(ctx, key, "field").Err())
	pipe := client.TxPipeline()
	pipe.RPush(ctx, key, "item")
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"item"}, client.LRange(ctx, key, 0, -1).Val())
	exists, err := nodes[0].redisServer.store.ExistsAt(ctx, store.HashMetaKey([]byte(key)), math.MaxUint64)
	require.NoError(t, err)
	require.False(t, exists)
	delta, err := nodes[0].redisServer.prefixExistsAt(ctx, store.HashMetaDeltaScanPrefix([]byte(key)), math.MaxUint64)
	require.NoError(t, err)
	require.False(t, delta)
}

func TestRedis_MultiListRecreateExpiredZSet(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:multi-list-from-expired-zset"
	require.NoError(t, client.ZAdd(ctx, key, redis.Z{Score: 1, Member: "member"}).Err())
	require.NoError(t, client.PExpire(ctx, key, time.Millisecond).Err())
	time.Sleep(10 * time.Millisecond)
	pipe := client.TxPipeline()
	pipe.RPush(ctx, key, "item")
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"item"}, client.LRange(ctx, key, 0, -1).Val())
	require.Equal(t, "list", client.Type(ctx, key).Val())
}

func TestRedis_MultiExpiredZSetDeleteThenListWithTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = client.Close() }()

	key := "empty-wide:multi-expired-zset-to-list"
	require.NoError(t, client.ZAdd(ctx, key, redis.Z{Score: 1, Member: "member"}).Err())
	require.NoError(t, client.PExpire(ctx, key, time.Millisecond).Err())
	time.Sleep(10 * time.Millisecond)
	pipe := client.TxPipeline()
	pipe.ZIncrBy(ctx, key, 1, "new")
	pipe.Del(ctx, key)
	pipe.RPush(ctx, key, "item")
	pipe.PExpire(ctx, key, 30*time.Second)
	_, err := pipe.Exec(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"item"}, client.LRange(ctx, key, 0, -1).Val())
	require.Equal(t, "list", client.Type(ctx, key).Val())
	require.Positive(t, client.PTTL(ctx, key).Val())
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
