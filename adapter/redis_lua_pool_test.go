package adapter

import (
	"context"
	"sync"
	"sync/atomic"
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

	// NOTE: we intentionally do NOT assert pool.Hits() >= 1 here.
	// sync.Pool may evict items between Put and Get under GC pressure
	// (same non-determinism acknowledged in the comment above line
	// 80), so the Script-B Get may be a fresh allocation even though
	// Script A's plsA was just Put. CI on GitHub Actions has
	// reproduced this as a flake. Pool effectiveness is covered
	// deterministically by TestLua_PoolRecordsReuseVsAllocation,
	// which asserts hits + misses rather than hits alone.
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
// actually doing anything?" question via the hit/miss counters. The
// test guards against the subtle regression where sync.Pool.New is
// (re-)configured: with a New func set, p.pool.Get() on an empty
// pool would auto-construct and never return nil, so hit/miss
// tracking would be meaningless. Two sub-scenarios are exercised:
//
//  1. Miss branch: a get() on a brand-new pool has nothing to hand
//     out. It must increment the miss counter (fresh allocation) and
//     leave hits at zero. This is deterministic -- sync.Pool's own
//     scheduling cannot turn an empty pool into a non-empty one.
//  2. Hit branch: after many put/get cycles at least one acquire
//     must actually be served from the pool. sync.Pool under -race
//     randomises per-P caching and can drop items, so we cannot
//     assert on a single put/get round-trip; instead we run a loop
//     large enough that the probability of zero reuse is negligible.
//
// If sync.Pool.New were accidentally re-introduced, the miss branch
// (step 1) would fail immediately: Misses would be 0, Hits would be 1.
func TestLua_PoolRecordsReuseVsAllocation(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	// Scenario 1: empty pool -> miss. Deterministic.
	plsA := pool.get(nil)
	require.NotNil(t, plsA, "get on empty pool must allocate a fresh state, not return nil")
	require.Equal(t, uint64(0), pool.Hits(),
		"empty pool must not record a hit on first acquire -- sync.Pool.New likely reintroduced")
	require.Equal(t, uint64(1), pool.Misses(),
		"empty pool must record exactly one miss on first acquire")
	pool.put(plsA)

	// Scenario 2: with the state now available, a loop of get/put
	// cycles must observe at least one genuine reuse. We cannot
	// assert on a single round-trip because sync.Pool under -race
	// may drop the freshly-put item from the local P cache; over
	// many iterations, however, at least one must be served.
	const iters = 500
	for i := 0; i < iters; i++ {
		pool.put(pool.get(nil))
	}
	require.Greater(t, pool.Hits(), uint64(0),
		"pool reported zero hits across %d cycles -- reuse not happening", iters)
	// The total acquires must sum to Hits + Misses = iters + 1 (the
	// initial get outside the loop). This invariant catches a bug
	// where get() forgets to increment either counter on some path.
	require.Equal(t, uint64(iters+1), pool.Hits()+pool.Misses(),
		"hit+miss counters must sum to total acquires; got hits=%d misses=%d",
		pool.Hits(), pool.Misses())
}

// TestLua_VMReuseNonStringGlobalKeysAreWiped guards against a leak
// vector missed by the original reset: globals keyed by types other
// than string. Lua permits any non-nil, non-NaN value as a table key,
// so a script doing `_G[42] = "leak"` or `_G[true] = "bad"` bypasses a
// naive string-only snapshot/wipe. The LValue-keyed snapshot + the
// RawSet-based reset in pool.reset must catch these. RawSet (rather
// than RawSetH) matters because gopher-lua stores integer keys in the
// array part, and only RawSet dispatches to the right storage by key
// type.
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

// TestLua_VMReuseDoesNotPoisonGlobalsMetatable regression-tests the
// metatable-snapshot fix. gopher-lua's base lib exposes setmetatable,
// so a script can install an __index handler on _G and poison every
// subsequent pooled eval's view of undefined globals. We verify that
// after Script A poisons _G's metatable, Script B -- acquired from
// the pool after A's state is released -- reads `_G.undefined` as
// genuine nil rather than the attacker-supplied sentinel.
//
// The test also pokes `_G[nonExisting]` via a local to make sure the
// leak path is _G's __index specifically and not something else (e.g.
// a leftover global called "undefined"). We use a freshly-minted
// symbol name on both sides to avoid interference with any snapshot
// entry the fix itself would restore.
func TestLua_VMReuseDoesNotPoisonGlobalsMetatable(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	plsA := pool.get(nil)
	require.NoError(t, plsA.state.DoString(`
setmetatable(_G, { __index = function() return "leak" end })
-- Sanity: script A sees its own poisoned __index.
assert(_G.some_never_defined_symbol == "leak",
    "setmetatable on _G did not take effect inside script A")
`))
	pool.put(plsA)

	plsB := pool.get(nil)
	defer pool.put(plsB)
	require.NoError(t, plsB.state.DoString(`
-- An undefined global must read as nil, not the attacker sentinel.
local v = _G.some_never_defined_symbol
assert(v == nil,
    "globals metatable leaked across pool reuse: got " .. tostring(v))
-- And installing a fresh metatable on _G must still work (we didn't
-- accidentally lock _G via __metatable or anything similar).
setmetatable(_G, nil)
`))
}

// TestLua_VMReuseDoesNotPoisonStringMetatable covers the same
// metatable-poisoning risk, but applied to the string library table.
// gopher-lua resolves method-style calls on string literals (e.g.
// `("x"):upper()`) via the string builtin metatable, not via the
// string table's metatable -- so this test specifically guards
// against a script that installs an __index on the string table and
// relies on subsequent scripts fetching fields off that table (e.g.
// in code that walks the library dynamically).
func TestLua_VMReuseDoesNotPoisonStringMetatable(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	plsA := pool.get(nil)
	require.NoError(t, plsA.state.DoString(`
setmetatable(string, { __index = function() return "leak" end })
-- Sanity: the poisoned __index fires on absent fields.
assert(string.no_such_function == "leak",
    "setmetatable on string did not take effect inside script A")
`))
	pool.put(plsA)

	plsB := pool.get(nil)
	defer pool.put(plsB)
	require.NoError(t, plsB.state.DoString(`
local v = string.no_such_function
assert(v == nil,
    "string metatable leaked across pool reuse: got " .. tostring(v))
-- string.upper must still be the genuine builtin.
assert(string.upper("x") == "X", "string.upper was damaged by reset")
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

// TestLua_PoolConcurrentContextIsolation is the regression test for
// the HIGH-priority concurrency fix. It asserts that when many
// goroutines concurrently get / bind / lookup / put pooled states,
// each goroutine's redis.call closure observes *its own*
// *luaScriptContext -- never another goroutine's context.
//
// Before the fix, the global luaStateBindings map + sync.RWMutex was
// a single contention point on every redis.call. After the fix, each
// state reads an *LUserData from its own registry, which must never
// point at a different goroutine's context even under heavy
// interleaving. Run with `go test -race -count=5 -run TestLua_Pool`.
func TestLua_PoolConcurrentContextIsolation(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()

	const (
		goroutines    = 64
		lookupsPerScr = 100
	)

	var (
		mismatches atomic.Int64
		wg         sync.WaitGroup
	)
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			// Each goroutine uses a distinct context pointer so that
			// observing a wrong-valued pointer is a detectable bug.
			ownCtx := &luaScriptContext{}
			pls := pool.get(ownCtx)
			// Simulate many redis.call lookups inside one script.
			for i := 0; i < lookupsPerScr; i++ {
				observed, ok := luaLookupContext(pls.state)
				if !ok || observed != ownCtx {
					mismatches.Add(1)
				}
			}
			pool.put(pls)
		}()
	}
	wg.Wait()

	require.EqualValues(t, 0, mismatches.Load(),
		"concurrent goroutines observed a wrong context via luaLookupContext -- state-local binding is broken")
}

// TestLua_PoolContextIsRegistryBacked asserts the binding lives in the
// state's own Lua registry -- the very thing that frees us from the
// global sync.RWMutex. If a refactor ever reintroduces a global map,
// this test pins down the contract.
func TestLua_PoolContextIsRegistryBacked(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()
	ctx := &luaScriptContext{}
	pls := pool.get(ctx)
	defer pool.put(pls)

	ud, ok := pls.state.GetField(pls.state.Get(lua.RegistryIndex), luaCtxRegistryKey).(*lua.LUserData)
	require.True(t, ok, "ctx binding userdata missing from state registry")
	require.Same(t, pls.ctxBinding, ud, "registry userdata differs from pooledLuaState.ctxBinding")
	storedCtx, ok := ud.Value.(*luaScriptContext)
	require.True(t, ok, "registry userdata value is not a *luaScriptContext")
	require.Same(t, ctx, storedCtx,
		"registry userdata value does not point at the bound script context")
}

// TestLua_PoolScratchKeysReused verifies the MEDIUM allocation fix.
// After a reset, pooledLuaState.scratchKeys must retain a non-nil
// backing array (sliced to zero length) so the next reset reuses it
// instead of minting a new one. We also verify the luaScratchKeysMaxCap
// bound kicks in for pathological scripts.
func TestLua_PoolScratchKeysReused(t *testing.T) {
	t.Parallel()

	pls := newPooledLuaState()

	// First reset primes scratchKeys from nil to a real backing array.
	pls.reset()
	require.NotNil(t, pls.scratchKeys,
		"scratchKeys still nil after reset; no reuse buffer was retained")
	require.Equal(t, 0, len(pls.scratchKeys),
		"scratchKeys must be reset to zero length for reuse")
	firstCap := cap(pls.scratchKeys)
	require.Greater(t, firstCap, 0, "scratchKeys capacity must be > 0 after priming")

	// Second reset must reuse the same backing array (cap unchanged
	// or grown, never shrunk).
	pls.reset()
	require.GreaterOrEqual(t, cap(pls.scratchKeys), firstCap,
		"scratchKeys backing array was discarded between resets; no reuse")

	// Force the cap-bound path: manually push scratchKeys past the
	// bound, reset, and assert it is dropped.
	pls.scratchKeys = make([]lua.LValue, 0, luaScratchKeysMaxCap+16)
	pls.reset()
	require.LessOrEqual(t, cap(pls.scratchKeys), luaScratchKeysMaxCap,
		"scratchKeys was not bounded; pathological scripts can pin unbounded memory")
}

// BenchmarkLuaLookupContext_Concurrent measures the cost of the
// redis.call context lookup under high fan-out. This is the bench the
// Gemini reviewer called out: ~50 lookups/script/s across concurrent
// scripts used to hammer a global RWMutex. Now it should be a
// lock-free per-state read.
//
//	go test -run='^$' -bench=BenchmarkLuaLookupContext_Concurrent -benchtime=5s ./adapter/
func BenchmarkLuaLookupContext_Concurrent(b *testing.B) {
	pool := newLuaStatePool()
	// Prime a handful of states so pool.Get is warm.
	for i := 0; i < 8; i++ {
		pool.put(pool.get(&luaScriptContext{}))
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := &luaScriptContext{}
		for pb.Next() {
			pls := pool.get(ctx)
			// Simulate 5 redis.call invocations per script.
			for i := 0; i < 5; i++ {
				if got, ok := luaLookupContext(pls.state); !ok || got != ctx {
					b.Fatalf("wrong ctx observed: got=%p want=%p ok=%v", got, ctx, ok)
				}
			}
			pool.put(pls)
		}
	})
}

// TestLua_VMReuseClearsContext verifies that a pooled *lua.LState
// does NOT retain a reference to a previous request's
// context.Context after it has been returned to the pool via
// pool.put.
//
// runLuaScript binds a per-request context onto the state with
// LState.SetContext (redis_lua.go). If pooledLuaState.reset() fails
// to clear that binding, the pooled VM keeps the prior ctx alive
// until it is either reused or garbage collected -- that retains
// any timers / cancel funcs / attached values referenced by the
// context. The reset() path must call LState.RemoveContext (or
// equivalently SetContext(context.Background())) to prevent this.
//
// We check both conditions:
//  1. After put, LState.Context() must NOT return the original
//     request ctx (identity compare).
//  2. After put, LState.Context() must be nil (RemoveContext's
//     documented post-state).
func TestLua_VMReuseClearsContext(t *testing.T) {
	t.Parallel()

	pool := newLuaStatePool()
	pls := pool.get(nil)

	// Simulate what runLuaScript does: attach a request-scoped ctx
	// to the state. Use WithCancel so we have a distinct, non-Background
	// identity that the state would measurably retain if reset() is
	// broken.
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pls.state.SetContext(reqCtx)

	// Sanity: the binding was actually observed by the state.
	require.Same(t, reqCtx, pls.state.Context(),
		"precondition: SetContext must bind the given ctx identity")

	// Release back to the pool. This is the code path that must
	// clear the ctx retention.
	pool.put(pls)

	// After put, the pooled state must have dropped the ctx reference.
	got := pls.state.Context()
	require.Nil(t, got,
		"pooled LState must not retain a ctx reference after reset/put")
	// Belt-and-braces identity check: even if a future gopher-lua
	// version ever returns a non-nil Background-style ctx here, it
	// must NOT be the original request ctx.
	if got != nil {
		require.NotSame(t, reqCtx, got,
			"pooled LState leaked the original request ctx across put")
	}
}
