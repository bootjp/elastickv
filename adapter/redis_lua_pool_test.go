package adapter

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	lua "github.com/yuin/gopher-lua"
)

// BenchmarkLuaState_NewVsPooled compares the cost of minting a brand
// new *lua.LState per call (matching the pre-pool hot path) against
// pulling one out of the pool and resetting. Use:
//
//	go test -run='^$' -bench=BenchmarkLuaState_NewVsPooled -benchmem ./adapter/
//
// On the author's laptop (darwin/arm64, go1.26) it shows roughly a
// 10x reduction in B/op and allocs/op for the pooled path.
func BenchmarkLuaState_NewVsPooled(b *testing.B) {
	b.Run("new_state_per_call", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			s := newPooledLuaState()
			// Simulate a trivial KEYS/ARGV set + small script.
			s.state.SetGlobal("KEYS", s.state.NewTable())
			s.state.SetGlobal("ARGV", s.state.NewTable())
			if err := s.state.DoString(`return 1 + 1`); err != nil {
				b.Fatal(err)
			}
			s.state.Close()
		}
	})

	b.Run("pooled_state", func(b *testing.B) {
		pool := newLuaStatePool()
		// Prime the pool so the first iteration is a hit.
		pool.put(pool.get(nil))

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			pls := pool.get(nil)
			pls.state.SetGlobal("KEYS", pls.state.NewTable())
			pls.state.SetGlobal("ARGV", pls.state.NewTable())
			if err := pls.state.DoString(`return 1 + 1`); err != nil {
				b.Fatal(err)
			}
			pool.put(pls)
		}
	})
}

// TestLua_VMReuseDoesNotLeakGlobals is the load-bearing safety test
// for the pool. Script A assigns GLOBAL_LEAK = 42 at the Lua level;
// script B then executes on a *lua.LState obtained from the same
// pool and asserts that GLOBAL_LEAK is nil.
//
// It also asserts that script B sees a fresh KEYS / ARGV and that
// the pool did hand back the same underlying *lua.LState (pool hit),
// which is the whole point of the optimisation.
func TestLua_VMReuseDoesNotLeakGlobals(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	// --- Script A: sets a user global -----------------------------
	plsA := pool.get(nil) // nil ctx is fine: scriptA does not call redis.call.
	stateA := plsA.state
	require.NoError(t, stateA.DoString(`GLOBAL_LEAK = 42`))
	// Also add a random table global to stress the reset path on
	// non-scalar user additions.
	require.NoError(t, stateA.DoString(`LEAKY_TABLE = { x = 1, y = 2 }`))
	require.Equal(t, lua.LNumber(42), stateA.GetGlobal("GLOBAL_LEAK"))
	ptrA := stateA
	pool.put(plsA)

	// --- Script B: same pool, no leak -----------------------------
	// sync.Pool is free to allocate a fresh item even immediately
	// after a put under race/GC, so we do not assert pointer
	// identity here. To assert the pool is effective at all, see
	// TestLua_PoolRecordsReuseVsAllocation which uses the hit counter.
	// What we DO assert is the security invariant: whichever state
	// we got, it must not observe the leaked globals from script A.
	_ = ptrA
	plsB := pool.get(nil)
	stateB := plsB.state

	require.Equal(t, lua.LNil, stateB.GetGlobal("GLOBAL_LEAK"),
		"GLOBAL_LEAK leaked from prior script -- security invariant broken")
	require.Equal(t, lua.LNil, stateB.GetGlobal("LEAKY_TABLE"),
		"LEAKY_TABLE leaked from prior script -- security invariant broken")

	// Whitelisted globals must still be intact for script B.
	require.NotEqual(t, lua.LNil, stateB.GetGlobal("redis"),
		"redis module missing after pool reuse")
	require.NotEqual(t, lua.LNil, stateB.GetGlobal("cjson"),
		"cjson module missing after pool reuse")
	require.NotEqual(t, lua.LNil, stateB.GetGlobal("cmsgpack"),
		"cmsgpack module missing after pool reuse")
	require.NotEqual(t, lua.LNil, stateB.GetGlobal("string"),
		"string stdlib missing after pool reuse")

	// Script B can still run normal Lua that depends on the
	// whitelisted base libs.
	require.NoError(t, stateB.DoString(`assert(string.upper("ok") == "OK")`))
	pool.put(plsB)

	// Pool should have registered at least one hit by now.
	require.GreaterOrEqual(t, pool.Hits(), uint64(1), "pool never reported a hit")
}

// TestLua_VMReuseRestoresRebindsWhitelistedGlobals guards against a
// script that overwrites an allowed global (e.g. `redis = nil`). The
// reset must put the original back so the next script isn't affected.
func TestLua_VMReuseRestoresRebindsWhitelistedGlobals(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	plsA := pool.get(nil)
	// Try to sabotage pooled state: wipe redis and hijack string.upper.
	require.NoError(t, plsA.state.DoString(`redis = nil; string = { upper = function() return "pwned" end }`))
	require.Equal(t, lua.LNil, plsA.state.GetGlobal("redis"))
	pool.put(plsA)

	plsB := pool.get(nil)
	defer pool.put(plsB)
	require.NotEqual(t, lua.LNil, plsB.state.GetGlobal("redis"),
		"redis global was not restored after sabotage; security invariant broken")

	// Original string lib must be restored such that string.upper works correctly.
	require.NoError(t, plsB.state.DoString(`assert(string.upper("abc") == "ABC", "string.upper was poisoned")`))
}

// TestLua_PoolSerialAcquireReusesState verifies the pool serves
// existing *lua.LState instances in sequential acquire/release cycles
// -- the knob we care about for the heap-pressure win. sync.Pool is
// free to reclaim under GC pressure, so we cannot assert on the exact
// pointer; instead we count hits vs misses via the test hook.
func TestLua_PoolSerialAcquireReusesState(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	// Prime the pool so the first Get allocates.
	pool.put(pool.get(nil))

	const iters = 50
	for i := 0; i < iters; i++ {
		pls := pool.get(nil)
		pool.put(pls)
	}
	// At least one hit proves the pool is actually handing back an
	// existing VM rather than minting a new one every time.
	require.GreaterOrEqual(t, pool.Hits(), uint64(1),
		"pool never reported a hit; sync.Pool reuse not happening")
}

// TestLua_PoolRecordsReuseVsAllocation pins down the "is the pool
// actually doing anything?" question via the hit counter. After N
// get/put cycles we must see at least one hit; a broken pool (e.g.
// one that never returned to the shared pile) would show zero hits.
func TestLua_PoolRecordsReuseVsAllocation(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	const iters = 200
	for i := 0; i < iters; i++ {
		pool.put(pool.get(nil))
	}
	require.Greater(t, pool.Hits(), uint64(0),
		"pool reported zero hits across %d cycles -- reuse not happening", iters)
}

// TestLua_VMReuseNonStringGlobalKeysAreWiped guards against a leak
// vector missed by the original reset: globals keyed by types other
// than string. Lua permits any non-nil, non-NaN value as a table key,
// so a script doing `_G[42] = "leak"` or `_G[true] = "bad"` bypasses a
// naive string-only snapshot/wipe. The LValue-keyed snapshot + the
// RawSetH-based reset in pool.reset must catch these.
func TestLua_VMReuseNonStringGlobalKeysAreWiped(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	plsA := pool.get(nil)
	// Set non-string-keyed globals directly via _G. This is the
	// attack surface being regression-tested.
	require.NoError(t, plsA.state.DoString(`_G[42] = "leak"; _G[true] = "bad"`))
	// Sanity: script A sees what it set.
	require.NoError(t, plsA.state.DoString(`assert(_G[42] == "leak" and _G[true] == "bad")`))
	pool.put(plsA)

	plsB := pool.get(nil)
	defer pool.put(plsB)
	// If either leaks, DoString errors out via Lua's assert and
	// the test fails with the error message.
	require.NoError(t, plsB.state.DoString(
		`assert(_G[42] == nil and _G[true] == nil, "non-string-keyed global leaked across pool reuse")`))
}

// TestLua_VMReuseDoesNotPoisonStringLib regression-tests the table
// poisoning fix. Script A mutates `string.upper` in place (not via
// rebinding the `string` global), which survives a naive snapshot
// that only restores the top-level `string` reference. The new
// tableSnapshots mechanism must restore the original `string.upper`
// function so script B's string.upper("x") == "X" holds.
func TestLua_VMReuseDoesNotPoisonStringLib(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	plsA := pool.get(nil)
	require.NoError(t, plsA.state.DoString(`
string.upper = function() return "pwned" end
-- Sanity: script A sees its own sabotage.
assert(string.upper("x") == "pwned")
-- Add a rogue field too -- must also be cleaned up.
string.pwn = 1
`))
	pool.put(plsA)

	plsB := pool.get(nil)
	defer pool.put(plsB)
	require.NoError(t, plsB.state.DoString(`
assert(string.upper("x") == "X", "string.upper was poisoned across pool reuse")
assert(string.pwn == nil, "script-added field on string leaked across pool reuse")
-- Same for other whitelisted tables.
assert(type(math.floor) == "function", "math.floor was wiped")
assert(type(table.insert) == "function", "table.insert was wiped")
`))
}

// TestLua_VMReuseDoesNotPoisonRedisModule covers the same poisoning
// class but on the pool-registered `redis` table itself. A script
// that replaces redis.sha1hex with a sabotaged implementation must
// not affect subsequent scripts.
func TestLua_VMReuseDoesNotPoisonRedisModule(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	plsA := pool.get(nil)
	require.NoError(t, plsA.state.DoString(`
redis.sha1hex = function() return "deadbeef" end
assert(redis.sha1hex("x") == "deadbeef")
`))
	pool.put(plsA)

	plsB := pool.get(nil)
	defer pool.put(plsB)
	// Any non-"deadbeef" digest proves the original sha1hex is back.
	require.NoError(t, plsB.state.DoString(`
local got = redis.sha1hex("x")
assert(got ~= "deadbeef", "redis.sha1hex remained poisoned after pool reuse: " .. tostring(got))
assert(#got == 40, "redis.sha1hex returned non-hex value after reset: " .. tostring(got))
`))
}

// TestLua_PoolNilContextProducesErrorNotPanic is the regression test
// for the nil-context nil-pointer deref. Before the fix, calling
// redis.call with a pool entry bound to a nil *luaScriptContext --
// which happens in the bench path via pool.get(nil) -- would panic in
// luaRedisCommand. After the fix it surfaces as a clean Lua error.
func TestLua_PoolNilContextProducesErrorNotPanic(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	pls := pool.get(nil) // explicit nil context
	defer pool.put(pls)

	// redis.call must raise a Lua error rather than panicking in
	// Go; the returned error wraps the Lua error message.
	err := pls.state.DoString(`redis.call("GET", "x")`)
	require.Error(t, err, "redis.call with nil context should return an error")
	require.Contains(t, err.Error(), "redis.call invoked without an active script context")

	// redis.pcall must not panic either; it should push a Lua
	// error table. The DoString itself returns no Go error --
	// pcall is the pcall path -- but the returned value carries
	// the err field.
	require.NoError(t, pls.state.DoString(`
local reply = redis.pcall("GET", "x")
assert(type(reply) == "table", "redis.pcall should return a table even with nil context")
assert(type(reply.err) == "string", "redis.pcall error reply must carry .err")
assert(reply.err:find("redis.pcall invoked without an active script context") ~= nil,
    "redis.pcall error reply text mismatch: " .. tostring(reply.err))
`))
}

// TestRedis_LuaPoolNoGlobalLeakEndToEnd drives the full EVAL path on
// a live RedisServer to make sure the pool integration (not just the
// pool in isolation) holds the security invariant. Script A tries to
// leak GLOBAL_LEAK; script B asserts the leak is gone.
func TestRedis_LuaPoolNoGlobalLeakEndToEnd(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// Script A: set a leaking global.
	_, err := rdb.Eval(ctx, `GLOBAL_LEAK = 42; return 1`, nil).Result()
	require.NoError(t, err)

	// Script B: assert that GLOBAL_LEAK is nil from its point of view.
	// Returning the raw value would conflate nil with Redis' nil-bulk;
	// instead, return a sentinel string and check.
	out, err := rdb.Eval(ctx, `
if GLOBAL_LEAK == nil then
    return "clean"
else
    return "leaked:" .. tostring(GLOBAL_LEAK)
end`, nil).Result()
	require.NoError(t, err)
	require.Equal(t, "clean", out, "pooled *lua.LState leaked a global to a subsequent script")

	// Sanity: the pooled state still supports the standard shared modules.
	out2, err := rdb.Eval(ctx, `return cjson.encode({a = 1})`, nil).Result()
	require.NoError(t, err)
	require.Equal(t, `{"a":1}`, out2)
}
