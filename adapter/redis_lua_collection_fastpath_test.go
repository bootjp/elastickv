package adapter

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// These tests pin the wire-level behaviour of the Lua-context fast
// paths added alongside PR #565's top-level equivalents. Each
// redis.call("HGET" / "HEXISTS" / "SISMEMBER", ...) inside a Lua
// script now takes the direct wide-column lookup path bypassing
// keyTypeAt's ~8-seek type probe, and must preserve identical
// semantics to the legacy hashState / setState flow:
//
//   - hit on a live wide-column key returns the value / 1
//   - in-script writes (HSET / SADD) done earlier in the same Eval
//     must be visible to subsequent HGET / SISMEMBER calls (the
//     fast path checks c.hashes / c.sets first)
//   - missing key / unknown field returns nil / 0 via the slow-path
//     fallback
//   - TTL-expired key is reported as missing
//   - WRONGTYPE propagates as a script error

const luaFastPathTTL = 80 * time.Millisecond

func waitForLuaTTL() {
	time.Sleep(luaFastPathTTL + 50*time.Millisecond)
}

func TestLua_HGET_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "lua:h:fast", "field", "value").Err())

	got, err := rdb.Eval(ctx,
		`return redis.call("HGET", KEYS[1], ARGV[1])`,
		[]string{"lua:h:fast"}, "field",
	).Result()
	require.NoError(t, err)
	require.Equal(t, "value", got)
}

func TestLua_HGET_HonorsInScriptHSet(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// HSET then HGET in the same script. The fast path must NOT bypass
	// the unflushed in-script mutation -- it must honor c.hashes[key]
	// and return the pending value.
	got, err := rdb.Eval(ctx, `
redis.call("HSET", KEYS[1], "field", ARGV[1])
return redis.call("HGET", KEYS[1], "field")
`, []string{"lua:h:rw"}, "scripted-value").Result()
	require.NoError(t, err)
	require.Equal(t, "scripted-value", got)
}

func TestLua_HGET_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	_, err := rdb.Eval(ctx,
		`return redis.call("HGET", KEYS[1], ARGV[1])`,
		[]string{"lua:h:missing"}, "field",
	).Result()
	require.ErrorIs(t, err, redis.Nil, "HGET on nonexistent key from Lua must surface as nil")
}

func TestLua_HGET_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "lua:h:str", "x", 0).Err())
	_, err := rdb.Eval(ctx,
		`return redis.call("HGET", KEYS[1], ARGV[1])`,
		[]string{"lua:h:str"}, "field",
	).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestLua_HGET_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "lua:h:ttl", "field", "v").Err())
	require.NoError(t, rdb.PExpire(ctx, "lua:h:ttl", luaFastPathTTL).Err())
	waitForLuaTTL()

	_, err := rdb.Eval(ctx,
		`return redis.call("HGET", KEYS[1], "field")`,
		[]string{"lua:h:ttl"},
	).Result()
	require.ErrorIs(t, err, redis.Nil, "HGET on an expired hash from Lua must surface as nil")
}

func TestLua_HEXISTS_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "lua:he:fast", "field", "v").Err())

	got, err := rdb.Eval(ctx,
		`return redis.call("HEXISTS", KEYS[1], ARGV[1])`,
		[]string{"lua:he:fast"}, "field",
	).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
}

func TestLua_HEXISTS_HonorsInScriptHSet(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	got, err := rdb.Eval(ctx, `
redis.call("HSET", KEYS[1], "f", "v")
return redis.call("HEXISTS", KEYS[1], "f")
`, []string{"lua:he:rw"}).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
}

func TestLua_HEXISTS_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	got, err := rdb.Eval(ctx,
		`return redis.call("HEXISTS", KEYS[1], ARGV[1])`,
		[]string{"lua:he:missing"}, "field",
	).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
}

func TestLua_HEXISTS_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "lua:he:ttl", "f", "v").Err())
	require.NoError(t, rdb.PExpire(ctx, "lua:he:ttl", luaFastPathTTL).Err())
	waitForLuaTTL()

	got, err := rdb.Eval(ctx,
		`return redis.call("HEXISTS", KEYS[1], "f")`,
		[]string{"lua:he:ttl"},
	).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
}

func TestLua_SISMEMBER_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "lua:s:fast", "member").Err())

	got, err := rdb.Eval(ctx,
		`return redis.call("SISMEMBER", KEYS[1], ARGV[1])`,
		[]string{"lua:s:fast"}, "member",
	).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
}

func TestLua_SISMEMBER_HonorsInScriptSAdd(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	got, err := rdb.Eval(ctx, `
redis.call("SADD", KEYS[1], "m")
return redis.call("SISMEMBER", KEYS[1], "m")
`, []string{"lua:s:rw"}).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
}

func TestLua_SISMEMBER_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	got, err := rdb.Eval(ctx,
		`return redis.call("SISMEMBER", KEYS[1], ARGV[1])`,
		[]string{"lua:s:missing"}, "member",
	).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
}

// --- Regression tests for the cachedType defer-to-slow-path guard ---
//
// These pin down the High-severity findings from the PR #567 review:
// a script that mutates the logical type (DEL / SET / RENAME / HDEL
// of an entire hash) within the same Eval must NOT see the fast path
// read pre-script pebble state and return stale collection data.

func TestLua_HGET_SetThenHGetReturnsWrongType(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// Seed a hash with a field in pebble.
	require.NoError(t, rdb.HSet(ctx, "lua:h:typechange", "f", "hashval").Err())

	// Script: SET string over the same key, then HGET. Pre-script
	// pebble still has the wide-column row, but the in-script type
	// is now String -> HGET MUST return WRONGTYPE, NOT "hashval".
	_, err := rdb.Eval(ctx, `
redis.call("SET", KEYS[1], "stringval")
return redis.call("HGET", KEYS[1], "f")
`, []string{"lua:h:typechange"}).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE",
		"fast path must defer to cachedType when a prior SET changes the logical type")
}

func TestLua_HGET_DelThenHGetReturnsNil(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "lua:h:delthenget", "f", "v").Err())

	_, err := rdb.Eval(ctx, `
redis.call("DEL", KEYS[1])
return redis.call("HGET", KEYS[1], "f")
`, []string{"lua:h:delthenget"}).Result()
	require.ErrorIs(t, err, redis.Nil,
		"fast path must defer to cachedType / deleted flag when a prior DEL tombstones the key")
}

func TestLua_HEXISTS_SetThenHExistsReturnsWrongType(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "lua:he:typechange", "f", "v").Err())
	_, err := rdb.Eval(ctx, `
redis.call("SET", KEYS[1], "x")
return redis.call("HEXISTS", KEYS[1], "f")
`, []string{"lua:he:typechange"}).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestLua_HEXISTS_DelThenHExistsReturnsZero(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "lua:he:delthenexists", "f", "v").Err())
	got, err := rdb.Eval(ctx, `
redis.call("DEL", KEYS[1])
return redis.call("HEXISTS", KEYS[1], "f")
`, []string{"lua:he:delthenexists"}).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
}

func TestLua_SISMEMBER_SetThenSIsMemberReturnsWrongType(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "lua:s:typechange", "m").Err())
	_, err := rdb.Eval(ctx, `
redis.call("SET", KEYS[1], "x")
return redis.call("SISMEMBER", KEYS[1], "m")
`, []string{"lua:s:typechange"}).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestLua_SISMEMBER_DelThenSIsMemberReturnsZero(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.SAdd(ctx, "lua:s:delthenexists", "m").Err())
	got, err := rdb.Eval(ctx, `
redis.call("DEL", KEYS[1])
return redis.call("SISMEMBER", KEYS[1], "m")
`, []string{"lua:s:delthenexists"}).Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), got)
}

// Additional coverage: in-script HSET then HGET on a DIFFERENT field
// must see the cached state (nil for unset field), not fall through
// to the fast path which would miss the unflushed HSET.
func TestLua_HGET_AnotherFieldAfterHSet(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// Seed pebble with field "seeded".
	require.NoError(t, rdb.HSet(ctx, "lua:h:twofield", "seeded", "old").Err())

	// Script writes "newf" then reads "seeded" and "newf" and
	// "unknownf". Must see the unflushed write, the seeded value,
	// and nil for unknown.
	got, err := rdb.Eval(ctx, `
redis.call("HSET", KEYS[1], "newf", "newval")
local a = redis.call("HGET", KEYS[1], "seeded") or "nil"
local b = redis.call("HGET", KEYS[1], "newf") or "nil"
local c = redis.call("HGET", KEYS[1], "unknownf") or "nil"
return a .. "|" .. b .. "|" .. c
`, []string{"lua:h:twofield"}).Result()
	require.NoError(t, err)
	require.Equal(t, "old|newval|nil", got)
}

func TestLua_SISMEMBER_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "lua:s:str", "x", 0).Err())
	_, err := rdb.Eval(ctx,
		`return redis.call("SISMEMBER", KEYS[1], "member")`,
		[]string{"lua:s:str"},
	).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

// TestLua_HGET_RepeatedMissReusesLoadedState pins the optimisation
// that a missing hash loaded once should NOT re-run the wide-column
// probe on every subsequent HGET in the same Eval. The script does
// N HGETs on the same missing key; if the fast-path guard correctly
// honors an already-loaded luaHashState (even with exists=false),
// only the first call reaches Pebble.
func TestLua_HGET_RepeatedMissReusesLoadedState(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// 4 HGETs on the same missing key; the script must return nil for
	// each and complete without divergent results. Correctness check
	// only -- the optimisation itself is a seek-count saving that
	// needs pprof to verify; this test guards against the guard
	// regressing into returning wrong answers.
	got, err := rdb.Eval(ctx, `
local a = redis.call("HGET", KEYS[1], "f") or "nil"
local b = redis.call("HGET", KEYS[1], "f") or "nil"
local c = redis.call("HGET", KEYS[1], "f") or "nil"
local d = redis.call("HGET", KEYS[1], "f") or "nil"
return a .. "|" .. b .. "|" .. c .. "|" .. d
`, []string{"lua:h:repeatedmiss"}).Result()
	require.NoError(t, err)
	require.Equal(t, "nil|nil|nil|nil", got)
}

// --- ZSCORE fast-path (#568) ---

func TestLua_ZSCORE_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.ZAdd(ctx, "lua:z:fast", redis.Z{Score: 42, Member: "m"}).Err())

	got, err := rdb.Eval(ctx,
		`return redis.call("ZSCORE", KEYS[1], ARGV[1])`,
		[]string{"lua:z:fast"}, "m",
	).Result()
	require.NoError(t, err)
	require.Equal(t, "42", got)
}

func TestLua_ZSCORE_HonorsInScriptZAdd(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	got, err := rdb.Eval(ctx, `
redis.call("ZADD", KEYS[1], ARGV[1], ARGV[2])
return redis.call("ZSCORE", KEYS[1], ARGV[2])
`, []string{"lua:z:rw"}, "7", "m").Result()
	require.NoError(t, err)
	require.Equal(t, "7", got)
}

func TestLua_ZSCORE_Miss(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	_, err := rdb.Eval(ctx,
		`return redis.call("ZSCORE", KEYS[1], ARGV[1])`,
		[]string{"lua:z:missing"}, "m",
	).Result()
	require.ErrorIs(t, err, redis.Nil)
}

func TestLua_ZSCORE_UnknownMember(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.ZAdd(ctx, "lua:z:known", redis.Z{Score: 1, Member: "a"}).Err())
	_, err := rdb.Eval(ctx,
		`return redis.call("ZSCORE", KEYS[1], ARGV[1])`,
		[]string{"lua:z:known"}, "b",
	).Result()
	require.ErrorIs(t, err, redis.Nil)
}

func TestLua_ZSCORE_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "lua:z:str", "x", 0).Err())
	_, err := rdb.Eval(ctx,
		`return redis.call("ZSCORE", KEYS[1], "m")`,
		[]string{"lua:z:str"},
	).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestLua_ZSCORE_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.ZAdd(ctx, "lua:z:ttl", redis.Z{Score: 1, Member: "m"}).Err())
	require.NoError(t, rdb.PExpire(ctx, "lua:z:ttl", luaFastPathTTL).Err())
	waitForLuaTTL()

	_, err := rdb.Eval(ctx,
		`return redis.call("ZSCORE", KEYS[1], "m")`,
		[]string{"lua:z:ttl"},
	).Result()
	require.ErrorIs(t, err, redis.Nil)
}

func TestLua_ZSCORE_SetThenZScoreReturnsWrongType(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.ZAdd(ctx, "lua:z:typechange", redis.Z{Score: 7, Member: "m"}).Err())
	_, err := rdb.Eval(ctx, `
redis.call("SET", KEYS[1], "x")
return redis.call("ZSCORE", KEYS[1], "m")
`, []string{"lua:z:typechange"}).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestLua_ZSCORE_DelThenZScoreReturnsNil(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.ZAdd(ctx, "lua:z:delthen", redis.Z{Score: 7, Member: "m"}).Err())
	_, err := rdb.Eval(ctx, `
redis.call("DEL", KEYS[1])
return redis.call("ZSCORE", KEYS[1], "m")
`, []string{"lua:z:delthen"}).Result()
	require.ErrorIs(t, err, redis.Nil)
}

// ZSCORE arity: exactly 2 arguments (command + key + member). Extra
// arguments must be rejected, matching Redis server semantics.
func TestLua_ZSCORE_ArityTooFew(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	_, err := rdb.Eval(ctx,
		`return redis.call("ZSCORE", KEYS[1])`,
		[]string{"lua:z:arityfew"},
	).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong number of arguments")
}

func TestLua_ZSCORE_ArityTooMany(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	_, err := rdb.Eval(ctx,
		`return redis.call("ZSCORE", KEYS[1], "m", "extra")`,
		[]string{"lua:z:aritymany"},
	).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "wrong number of arguments")
}

// --- ZRANGEBYSCORE streaming fast-path (Phase B) ---

// TestLua_ZRANGEBYSCORE_FastPathHit pins the hot-case BullMQ pattern:
// a bounded delay-queue poll returns only the range entries, without
// loading every member.
func TestLua_ZRANGEBYSCORE_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	for i := 1; i <= 10; i++ {
		require.NoError(t, rdb.ZAdd(ctx,
			"lua:zr:fast",
			redis.Z{Score: float64(i), Member: "m" + strconv.Itoa(i)},
		).Err())
	}

	got, err := rdb.Eval(ctx,
		`return redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[2])`,
		[]string{"lua:zr:fast"}, "3", "6",
	).Result()
	require.NoError(t, err)
	require.Equal(t, []any{"m3", "m4", "m5", "m6"}, got)
}

func TestLua_ZRANGEBYSCORE_FastPathHitWithScores(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	for i := 1; i <= 5; i++ {
		require.NoError(t, rdb.ZAdd(ctx,
			"lua:zr:scores",
			redis.Z{Score: float64(i), Member: "m" + strconv.Itoa(i)},
		).Err())
	}

	got, err := rdb.Eval(ctx,
		`return redis.call("ZRANGEBYSCORE", KEYS[1], "2", "4", "WITHSCORES")`,
		[]string{"lua:zr:scores"},
	).Result()
	require.NoError(t, err)
	require.Equal(t, []any{"m2", "2", "m3", "3", "m4", "4"}, got)
}

func TestLua_ZRANGEBYSCORE_LimitOffset(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	for i := 1; i <= 10; i++ {
		require.NoError(t, rdb.ZAdd(ctx,
			"lua:zr:limit",
			redis.Z{Score: float64(i), Member: "m" + strconv.Itoa(i)},
		).Err())
	}

	// LIMIT 2 3: skip first 2 matches, take next 3. Range 1..10 matches
	// m1..m10; offset 2 drops m1 m2, limit 3 picks m3 m4 m5.
	got, err := rdb.Eval(ctx,
		`return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf", "LIMIT", "2", "3")`,
		[]string{"lua:zr:limit"},
	).Result()
	require.NoError(t, err)
	require.Equal(t, []any{"m3", "m4", "m5"}, got)
}

func TestLua_ZRANGEBYSCORE_ExclusiveBounds(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	for i := 1; i <= 5; i++ {
		require.NoError(t, rdb.ZAdd(ctx,
			"lua:zr:excl",
			redis.Z{Score: float64(i), Member: "m" + strconv.Itoa(i)},
		).Err())
	}

	// (2 (4 means strictly between 2 and 4 -> only m3.
	got, err := rdb.Eval(ctx,
		`return redis.call("ZRANGEBYSCORE", KEYS[1], "(2", "(4")`,
		[]string{"lua:zr:excl"},
	).Result()
	require.NoError(t, err)
	require.Equal(t, []any{"m3"}, got)
}

func TestLua_ZRANGEBYSCORE_EmptyRange(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.ZAdd(ctx, "lua:zr:emptyrange", redis.Z{Score: 5, Member: "m"}).Err())

	got, err := rdb.Eval(ctx,
		`return redis.call("ZRANGEBYSCORE", KEYS[1], "10", "20")`,
		[]string{"lua:zr:emptyrange"},
	).Result()
	require.NoError(t, err)
	require.Equal(t, []any{}, got)
}

func TestLua_ZRANGEBYSCORE_MissingKey(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	got, err := rdb.Eval(ctx,
		`return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")`,
		[]string{"lua:zr:missing"},
	).Result()
	require.NoError(t, err)
	require.Equal(t, []any{}, got)
}

func TestLua_ZRANGEBYSCORE_WRONGTYPE(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "lua:zr:str", "x", 0).Err())
	_, err := rdb.Eval(ctx,
		`return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")`,
		[]string{"lua:zr:str"},
	).Result()
	require.Error(t, err)
	require.Contains(t, err.Error(), "WRONGTYPE")
}

func TestLua_ZRANGEBYSCORE_HonorsInScriptZAdd(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// Seed one member in pebble, add another in-script; both must
	// appear in the subsequent ZRANGEBYSCORE inside the SAME Eval.
	require.NoError(t, rdb.ZAdd(ctx, "lua:zr:rw", redis.Z{Score: 1, Member: "seeded"}).Err())

	got, err := rdb.Eval(ctx, `
redis.call("ZADD", KEYS[1], "2", "newm")
return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")
`, []string{"lua:zr:rw"}).Result()
	require.NoError(t, err)
	require.Equal(t, []any{"seeded", "newm"}, got)
}

func TestLua_ZREVRANGEBYSCORE_FastPathHit(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	for i := 1; i <= 5; i++ {
		require.NoError(t, rdb.ZAdd(ctx,
			"lua:zrr:fast",
			redis.Z{Score: float64(i), Member: "m" + strconv.Itoa(i)},
		).Err())
	}

	got, err := rdb.Eval(ctx,
		`return redis.call("ZREVRANGEBYSCORE", KEYS[1], "4", "2")`,
		[]string{"lua:zrr:fast"},
	).Result()
	require.NoError(t, err)
	require.Equal(t, []any{"m4", "m3", "m2"}, got)
}

func TestLua_ZRANGEBYSCORE_TTLExpired(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.ZAdd(ctx, "lua:zr:ttl", redis.Z{Score: 1, Member: "m"}).Err())
	require.NoError(t, rdb.PExpire(ctx, "lua:zr:ttl", luaFastPathTTL).Err())
	waitForLuaTTL()

	got, err := rdb.Eval(ctx,
		`return redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", "+inf")`,
		[]string{"lua:zr:ttl"},
	).Result()
	require.NoError(t, err)
	require.Equal(t, []any{}, got)
}
