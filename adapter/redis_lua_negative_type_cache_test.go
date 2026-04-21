package adapter

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// These tests pin the BullMQ-style "poll a missing delayed zset" path
// that the negative-type cache targets. See `perf(redis-lua): cache
// keyTypeAt=None results in Eval scope`.
//
// Wire-level tests cover correctness invariants (WRONGTYPE after
// create-over-negative, nil across repeated misses). A dedicated
// in-process test uses luaScriptContext.keyTypeProbeCount to pin the
// "only ONE storage probe per key per Eval" property without relying
// on brittle production metric assertions.

// TestLua_ZSCORE_MissingThenMissingStillReturnsNil covers the hot-case
// invariant: two ZSCORE calls on a key that is absent at script start
// must both return nil. The negative-type cache short-circuits the
// second call; this test merely ensures the short-circuit produces the
// same reply shape as the slow path.
func TestLua_ZSCORE_MissingThenMissingStillReturnsNil(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// Key is NOT created in pebble. Two ZSCOREs in the same Eval
	// exercise cold-probe then negative-cache-hit paths.
	got, err := rdb.Eval(ctx, `
local a = redis.call("ZSCORE", KEYS[1], "m")
local b = redis.call("ZSCORE", KEYS[1], "m")
if a == false and b == false then return "both-nil" end
return "unexpected"
`, []string{"lua:neg:zscore"}).Result()
	require.NoError(t, err)
	require.Equal(t, "both-nil", got)
}

// TestLua_ZRANGEBYSCORE_MissingReturnsEmptyArray pins the empty-array
// reply when the negative-type cache short-circuits the ZRANGEBYSCORE
// fast-path guard. Matches the slow-path behaviour for a missing key.
func TestLua_ZRANGEBYSCORE_MissingReturnsEmptyArray(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	got, err := rdb.Eval(ctx, `
redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")
local r = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")
return #r
`, []string{"lua:neg:zrange"}).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
}

// TestLua_HGET_MissingThenSetReturnsWrongType pins the critical
// correctness invariant from the task spec: after an EXISTS-style probe
// loads a negative cache entry, a subsequent SET that transitions the
// key to String must cause HGET to return WRONGTYPE -- NOT the stale
// "cached as None" nil reply. The cachedLoadedTypes lookup inside
// cachedType() shadows the negative entry because SET populates
// c.strings[k] with exists=true.
func TestLua_HGET_MissingThenSetReturnsWrongType(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	_, err := rdb.Eval(ctx, `
redis.call("EXISTS", KEYS[1])
redis.call("SET", KEYS[1], "v")
return redis.call("HGET", KEYS[1], "f")
`, []string{"lua:neg:hget-wt"}).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE",
		"negative cache must NOT shadow an in-script SET that changes the logical type")
}

// TestLua_ZSCORE_MissingThenZAddReturnsScore pins the companion
// invariant for zsets: after a probe observes None, a subsequent ZADD
// in the same script must update c.zsets[k].exists, so ZSCORE returns
// the newly-added member's score -- not a cached nil.
func TestLua_ZSCORE_MissingThenZAddReturnsScore(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	got, err := rdb.Eval(ctx, `
redis.call("ZSCORE", KEYS[1], "m")
redis.call("ZADD", KEYS[1], 7, "m")
return redis.call("ZSCORE", KEYS[1], "m")
`, []string{"lua:neg:zscore-recreate"}).Result()
	require.NoError(t, err)
	require.Equal(t, "7", got)
}

// TestLuaNegativeTypeCache_SingleProbePerKey pins the probe-count
// property that motivates the negative cache. Uses the in-process
// luaScriptContext.keyTypeProbeCount counter rather than a brittle
// Prometheus metric scrape: this guarantees that N repeated reads of a
// missing key incur exactly ONE server.keyTypeAt() call.
func TestLuaNegativeTypeCache_SingleProbePerKey(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	sc, err := newLuaScriptContext(ctx, nodes[0].redisServer)
	require.NoError(t, err)
	defer sc.Close()

	// The FIRST keyType call triggers a server probe; the cache must
	// absorb every subsequent call on the same key.
	key := []byte("lua:neg:probe-count")
	for i := 0; i < 5; i++ {
		typ, kerr := sc.keyType(key)
		require.NoError(t, kerr)
		require.Equal(t, redisTypeNone, typ)
	}
	require.Equal(t, 1, sc.keyTypeProbeCount,
		"repeated keyType() on a missing key must issue exactly one server probe")
}
