package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func TestRedis_LuaScriptCacheCompatibility(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	script := redis.NewScript(`
redis.call("SET", KEYS[1], ARGV[1])
return redis.call("GET", KEYS[1])
`)

	_, err := script.EvalSha(ctx, rdb, []string{"lua:cache"}).Result()
	require.ErrorContains(t, err, "NOSCRIPT")

	loaded, err := script.Eval(ctx, rdb, []string{"lua:cache"}, "v1").Result()
	require.NoError(t, err)
	require.Equal(t, "v1", loaded)

	cached, err := script.EvalSha(ctx, rdb, []string{"lua:cache"}, "v2").Result()
	require.NoError(t, err)
	require.Equal(t, "v2", cached)
}

func TestRedis_LuaBullMQLikeHelpers(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	payload, err := msgpack.Marshal([]any{"job-1", 5})
	require.NoError(t, err)

	script := `
local data = cmsgpack.unpack(ARGV[1])
local event = cjson.decode(ARGV[2])
redis.call("HMSET", KEYS[1], "name", data[1], "priority", data[2])
redis.call("LPUSH", KEYS[2], data[1])
redis.call("ZADD", KEYS[3], data[2], data[1])
local eventId = redis.call("XADD", KEYS[4], "MAXLEN", "~", 10, "*", "event", event["event"], "jobId", data[1])
local missing = redis.pcall("NOTACMD")
return {eventId, redis.call("HGET", KEYS[1], "name"), cjson.encode(event), redis.call("ZRANGE", KEYS[3], 0, -1, "WITHSCORES"), missing.err}
`

	result, err := rdb.Eval(ctx, script, []string{
		"bull:test:job",
		"bull:test:wait",
		"bull:test:priority",
		"bull:test:events",
	}, payload, `{"event":"waiting"}`).Result()
	require.NoError(t, err)

	values, ok := result.([]any)
	require.True(t, ok)
	require.Len(t, values, 5)
	require.IsType(t, "", values[0])
	require.Equal(t, "job-1", values[1])
	require.Equal(t, `{"event":"waiting"}`, values[2])
	require.Contains(t, values[4], "unsupported command 'NOTACMD'")

	zvalues, ok := values[3].([]any)
	require.True(t, ok)
	require.Equal(t, []any{"job-1", "5"}, zvalues)

	hash, err := rdb.HGetAll(ctx, "bull:test:job").Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"name": "job-1", "priority": "5"}, hash)

	waiting, err := rdb.LRange(ctx, "bull:test:wait", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"job-1"}, waiting)

	priorities, err := rdb.ZRangeWithScores(ctx, "bull:test:priority", 0, -1).Result()
	require.NoError(t, err)
	require.Len(t, priorities, 1)
	require.Equal(t, "job-1", priorities[0].Member)
	require.Equal(t, 5.0, priorities[0].Score)

	events, err := rdb.XRangeN(ctx, "bull:test:events", "-", "+", 10).Result()
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, map[string]any{"event": "waiting", "jobId": "job-1"}, events[0].Values)
}

func TestRedis_LuaReplyHelpers(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	status, err := rdb.Eval(ctx, `return redis.status_reply("OK")`, nil).Result()
	require.NoError(t, err)
	require.Equal(t, "OK", status)

	_, err = rdb.Eval(ctx, `return redis.error_reply("boom")`, nil).Result()
	require.ErrorContains(t, err, "boom")
}

func TestRedis_LuaRPopLPushBullMQLikeLists(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.RPush(ctx, "bull:test:wait", "job-1", "job-2", "job-3").Err())
	require.NoError(t, rdb.LPush(ctx, "bull:test:active", "job-0").Err())

	result, err := rdb.Eval(ctx, `
local moved = redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
return {moved, redis.call("LLEN", KEYS[1]), redis.call("LLEN", KEYS[2])}
`, []string{"bull:test:wait", "bull:test:active"}).Result()
	require.NoError(t, err)

	values, ok := result.([]any)
	require.True(t, ok)
	require.Equal(t, []any{"job-3", int64(2), int64(2)}, values)

	waiting, err := rdb.LRange(ctx, "bull:test:wait", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"job-1", "job-2"}, waiting)

	active, err := rdb.LRange(ctx, "bull:test:active", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"job-3", "job-0"}, active)
}

func TestRedis_LuaRPopLPushPreservesTTL(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.RPush(ctx, "bull:test:rotate", "job-1", "job-2", "job-3").Err())
	require.NoError(t, rdb.PExpire(ctx, "bull:test:rotate", time.Minute).Err())

	result, err := rdb.Eval(ctx, `
local moved = redis.call("RPOPLPUSH", KEYS[1], KEYS[1])
return {moved, redis.call("LLEN", KEYS[1])}
`, []string{"bull:test:rotate"}).Result()
	require.NoError(t, err)

	values, ok := result.([]any)
	require.True(t, ok)
	require.Equal(t, []any{"job-3", int64(3)}, values)

	rotated, err := rdb.LRange(ctx, "bull:test:rotate", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"job-3", "job-1", "job-2"}, rotated)

	ttl, err := rdb.PTTL(ctx, "bull:test:rotate").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
}

func TestRedis_LuaDelAndRecreateListNoOrphan(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// Pre-populate a list so there are existing storage items.
	require.NoError(t, rdb.RPush(ctx, "mylist", "a", "b", "c").Err())

	// In a single Lua script: DEL the key, then recreate it as a list.
	// After the script, only the new items should be visible; the old
	// [a, b, c] items must not be orphaned/leaked in storage.
	result, err := rdb.Eval(ctx, `
redis.call("DEL", KEYS[1])
redis.call("RPUSH", KEYS[1], "d")
return redis.call("LRANGE", KEYS[1], 0, -1)
`, []string{"mylist"}).Result()
	require.NoError(t, err)

	values, ok := result.([]any)
	require.True(t, ok)
	require.Equal(t, []any{"d"}, values, "list should contain only the newly pushed item")

	// Verify via a plain LRANGE that the old items are gone.
	final, err := rdb.LRange(ctx, "mylist", 0, -1).Result()
	require.NoError(t, err)
	require.Equal(t, []string{"d"}, final)
}
