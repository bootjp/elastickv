package adapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

// These tests pin the semantics of the wide-column fast paths for
// HGET, HEXISTS, and SISMEMBER introduced in PR #565. Each command
// now probes the specific field / member key directly and falls back
// to the keyTypeAt + loadHash/SetAt slow path on miss, so we need
// end-to-end coverage for:
//
//   - fast-path hit (wide-column present, TTL alive)
//   - TTL-expired hit (wide-column row physically present but the
//     hash/set has a past EXPIRE -- must return nil / 0)
//   - miss (nonexistent user key -- must return nil / 0)
//   - WRONGTYPE (user key exists but as a string, not the expected
//     collection type)
//
// The tests deliberately speak Redis RESP rather than calling
// hashFieldFastLookup / setMemberFastExists directly, so they also
// cover the handler wiring above the helpers (WriteBulk vs
// WriteBulkString path for HGET, integer protocol for HEXISTS /
// SISMEMBER, etc.).

const collectionFastPathTTL = 80 * time.Millisecond

func TestRedis_HGET_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "h:fast", "field", "value").Err())

	got, err := rdb.HGet(ctx, "h:fast", "field").Result()
	require.NoError(t, err)
	require.Equal(t, "value", got)
}

func TestRedis_HGET_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	_, err := rdb.HGet(ctx, "h:missing", "field").Result()
	require.ErrorIs(t, err, redis.Nil, "HGET on a nonexistent key must return nil")
}

func TestRedis_HGET_UnknownField(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "h:known", "a", "1").Err())
	_, err := rdb.HGet(ctx, "h:known", "b").Result()
	require.ErrorIs(t, err, redis.Nil, "HGET on an existing hash but unknown field must return nil")
}

func TestRedis_HGET_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "h:str", "not-a-hash", 0).Err())
	_, err := rdb.HGet(ctx, "h:str", "field").Result()
	require.Error(t, err, "HGET on a string key must return WRONGTYPE")
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestRedis_HGET_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "h:ttl", "field", "value").Err())
	require.NoError(t, rdb.PExpire(ctx, "h:ttl", collectionFastPathTTL).Err())
	eventuallyExpired(t, collectionFastPathTTL, func() bool {
		_, err := rdb.HGet(ctx, "h:ttl", "field").Result()
		return errors.Is(err, redis.Nil)
	}, "HGET on an expired hash must return nil")
}

func TestRedis_HEXISTS_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "he:fast", "field", "v").Err())

	ok, err := rdb.HExists(ctx, "he:fast", "field").Result()
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRedis_HEXISTS_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	ok, err := rdb.HExists(ctx, "he:missing", "field").Result()
	require.NoError(t, err)
	require.False(t, ok, "HEXISTS on a nonexistent key must return 0")
}

func TestRedis_HEXISTS_UnknownField(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "he:known", "a", "1").Err())
	ok, err := rdb.HExists(ctx, "he:known", "b").Result()
	require.NoError(t, err)
	require.False(t, ok, "HEXISTS on an existing hash but unknown field must return 0")
}

func TestRedis_HEXISTS_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "he:str", "x", 0).Err())
	_, err := rdb.HExists(ctx, "he:str", "field").Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestRedis_HEXISTS_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "he:ttl", "field", "v").Err())
	require.NoError(t, rdb.PExpire(ctx, "he:ttl", collectionFastPathTTL).Err())
	eventuallyExpired(t, collectionFastPathTTL, func() bool {
		ok, err := rdb.HExists(ctx, "he:ttl", "field").Result()
		return err == nil && !ok
	}, "HEXISTS on an expired hash must return 0")
}

func TestRedis_SISMEMBER_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "s:fast", "member").Err())

	ok, err := rdb.SIsMember(ctx, "s:fast", "member").Result()
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRedis_SISMEMBER_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	ok, err := rdb.SIsMember(ctx, "s:missing", "member").Result()
	require.NoError(t, err)
	require.False(t, ok, "SISMEMBER on a nonexistent set must return 0")
}

func TestRedis_SISMEMBER_UnknownMember(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "s:known", "a").Err())
	ok, err := rdb.SIsMember(ctx, "s:known", "b").Result()
	require.NoError(t, err)
	require.False(t, ok, "SISMEMBER on an existing set but unknown member must return 0")
}

func TestRedis_SISMEMBER_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "s:str", "x", 0).Err())
	_, err := rdb.SIsMember(ctx, "s:str", "member").Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestRedis_SISMEMBER_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "s:ttl", "member").Err())
	require.NoError(t, rdb.PExpire(ctx, "s:ttl", collectionFastPathTTL).Err())
	eventuallyExpired(t, collectionFastPathTTL, func() bool {
		ok, err := rdb.SIsMember(ctx, "s:ttl", "member").Result()
		return err == nil && !ok
	}, "SISMEMBER on an expired set must return 0")
}

// TestRedis_FastPathGuardDualEncodingReportsWrongType pins the
// string-priority guard (hasHigherPriorityStringEncoding) on the HGET,
// HEXISTS and SISMEMBER fast paths: a key holding both a string and a
// hash field / set member row must report WRONGTYPE, not the collection
// value. The dual encoding is only reachable under data-corruption
// recovery (SET tombstones the collection rows), so the rows are seeded
// directly in the store.
func TestRedis_FastPathGuardDualEncodingReportsWrongType(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := store.NewMVCCStore()
	server := &RedisServer{store: st, coordinator: newLocalAdapterCoordinator(st)}
	key := []byte("dual:key")
	field := []byte("field")
	member := []byte("member")
	require.NoError(t, st.PutAt(ctx, redisStrKey(key), encodeRedisStr([]byte("stringVal"), nil), 10, 0))
	require.NoError(t, st.PutAt(ctx, store.HashFieldKey(key, field), []byte("hashVal"), 11, 0))
	require.NoError(t, st.PutAt(ctx, store.HashMetaKey(key), store.MarshalHashMeta(store.HashMeta{Len: 1}), 11, 0))
	require.NoError(t, st.PutAt(ctx, store.SetMemberKey(key, member), []byte{}, 12, 0))
	require.NoError(t, st.PutAt(ctx, store.SetMetaKey(key), store.MarshalSetMeta(store.SetMeta{Len: 1}), 12, 0))

	for _, tc := range []struct {
		name    string
		handler func(redcon.Conn, redcon.Command)
		args    [][]byte
	}{
		{name: "hget", handler: server.hget, args: [][]byte{[]byte(cmdHGet), key, field}},
		{name: "hexists", handler: server.hexists, args: [][]byte{[]byte(cmdHExists), key, field}},
		{name: "sismember", handler: server.sismember, args: [][]byte{[]byte(cmdSIsMember), key, member}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &commandRecorder{}
			tc.handler(conn, redcon.Command{Args: tc.args})
			require.Len(t, conn.writes, 1)
			require.Equal(t, "error", conn.writes[0].op, "must not return the collection value: %+v", conn.writes[0])
			require.Contains(t, conn.writes[0].s, "WRONGTYPE")
		})
	}
}
