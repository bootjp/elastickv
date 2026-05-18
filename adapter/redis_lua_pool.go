package adapter

import (
	"sync/atomic"

	lua "github.com/yuin/gopher-lua"
)

// luaCtxRegistryKey is the fixed registry key under which each pooled
// *lua.LState stores a pre-allocated *lua.LUserData whose .Value holds
// the per-eval *luaScriptContext. Putting the binding in the state's
// own registry (instead of a global map guarded by sync.RWMutex) means
// every redis.call / redis.pcall lookup is O(1), lock-free, and local
// to the state -- no cross-state contention even under high fan-out
// workloads like BullMQ (~50 lookups/s/script).
const luaCtxRegistryKey = "elastickv_ctx"

// luaInitialGlobalsHint is the expected number of string-keyed
// globals present on a freshly initialised pooled state (base lib
// helpers + string/math/table tables + redis/cjson/cmsgpack + the
// nil-ed loader placeholders + unpack). Sizing the snapshot map to
// this up front avoids an internal grow during fill.
const luaInitialGlobalsHint = 64

// luaResetKeySlack accounts for the handful of user-added globals
// (KEYS, ARGV, and any helpers the script itself defined) that the
// reset routine has to walk. Serves only as a capacity hint for a
// scratch slice in resetPooledLuaState.
const luaResetKeySlack = 8

// luaWhitelistedTableHint is a capacity hint for the tableSnapshots
// map -- one entry per nested table value at init (math, string,
// table, redis, cjson, cmsgpack).
const luaWhitelistedTableHint = 8

// DefaultLuaPoolMaxIdle is the default upper bound on idle pooled
// *lua.LState instances retained for reuse. Each pooled state holds
// the base stdlib + redis/cjson/cmsgpack closures + per-state
// snapshot tables (globals / tables / metatables); empirically ~200
// KiB of long-lived heap per state. 64 is sized to comfortably cover
// typical Redis-side EVAL/EVALSHA concurrency (one in-flight script
// per connection up to redcon's default worker pool) without
// retaining a long tail of warm states after a burst subsides.
//
// Operators expecting sustained higher concurrency can raise the cap
// with --redisLuaMaxIdleStates; concurrency that exceeds the cap
// still works correctly — excess get() calls fall through to a fresh
// allocation (miss) and excess put() calls drop the state for the GC
// (drop). The cap therefore controls memory floor, not throughput
// ceiling.
//
// Exported so main.go can use it as the default for the
// --redisLuaMaxIdleStates flag instead of duplicating the literal
// (which trips the mnd lint and creates a drift source if the
// adapter-side default changes).
const DefaultLuaPoolMaxIdle = 64

// luaStatePool pools *lua.LState instances to cut heap/GC pressure on
// high-rate EVAL / EVALSHA workloads (e.g. BullMQ ~10 scripts/s, where
// each fresh state allocs ~34% of in-use heap via newFuncContext,
// newRegistry, newFunctionProto).
//
// Internal storage is a buffered channel of capacity maxIdle. We
// previously used sync.Pool, which GC-clears on every cycle and has
// no operator-tunable capacity; the bounded channel gives a
// predictable memory floor (maxIdle * per-state footprint) and an
// observable "states dropped because the pool was full" counter that
// makes mis-sizing visible. The channel ops are O(1) atomic CAS on
// the buffered chan; under high concurrency the contention is
// comparable to sync.Pool's per-P slabs and well below the cost of
// a single Lua eval. See TestLua_PoolBoundedOverflow for the
// invariants.
//
// Security invariant: no state must leak between scripts. Each
// pooled state is initialised with a fixed set of base globals
// (redis, cjson, cmsgpack, table/string/math + base lib helpers,
// and nil-ed loaders). Three snapshots — captured per-state at
// construction and stored on pooledLuaState — back the reset path:
// globalsSnapshot (the full LValue-keyed _G map, so non-string-keyed
// leaks like `_G[42] = "secret"` are caught), tableSnapshots
// (shallow field sets of the whitelisted nested tables, so
// `string.upper = function() return "pwned" end` cannot poison
// reuse), and metatableSnapshots (the init-time raw metatable of
// _G plus each whitelisted nested table, so a script-installed
// `setmetatable(_G, { __index = function() … end })` does not
// leak across evals).
//
// On release, the reset routine
//
//  1. restores the raw metatable of _G and every whitelisted table
//     (LNil if there was none originally), neutering setmetatable
//     poisoning,
//  2. walks each snapshotted nested table and restores its contents
//     (deletes script-added fields, rebinds original fields),
//  3. walks the current global table and deletes every key — of any
//     type — that is not present in the globals snapshot (removes
//     user-added globals such as KEYS, ARGV, GLOBAL_LEAK, _G[42]),
//     and
//  4. restores every globals-snapshot key to its original value (so
//     a script that did `table = nil` or `redis = evil` cannot
//     poison the next script).
//
// The value stack is also truncated to 0 and the script-context
// binding is cleared so the redis.call/pcall closures cannot be
// invoked against a stale context. Those closures are registered
// ONCE at pool fill time and read the per-eval *luaScriptContext
// out of each state's own Lua registry (see luaCtxRegistryKey /
// pooledLuaState.ctxBinding); this is what keeps redis.call
// lock-free on the hot path, unlike the first iteration which used
// a package-level map guarded by sync.RWMutex.
type luaStatePool struct {
	idle    chan *pooledLuaState
	maxIdle int

	// hits / misses / drops are exposed for tests and metrics.
	hits   atomic.Uint64
	misses atomic.Uint64
	drops  atomic.Uint64
}

// pooledLuaState wraps a *lua.LState plus the immutable snapshot of
// the globals that were present after base initialisation. Everything
// NOT in globalsSnapshot is treated as user-introduced state and
// removed on release.
type pooledLuaState struct {
	state *lua.LState
	// globalsSnapshot is a copy of every entry reachable via the
	// state's global table at init, keyed by LValue (not just string)
	// so scripts cannot smuggle state across evals via non-string
	// keys such as _G[42] = "secret".
	globalsSnapshot map[lua.LValue]lua.LValue
	// tableSnapshots holds the shallow field sets of well-known
	// whitelisted tables (string, math, table, redis, cjson,
	// cmsgpack) captured at init. On reset we restore each to its
	// original contents so a script doing e.g.
	// `string.upper = function() return "pwned" end` cannot poison
	// subsequent pooled reuses.
	//
	// The outer map is keyed by the *LTable pointer of the parent
	// (e.g. the `string` table) so tableSnapshots survives even if a
	// script rebinds the global name (`string = nil`) -- the reset
	// restores the global name first, then restores the table's
	// internal contents from this snapshot.
	tableSnapshots map[*lua.LTable]map[lua.LValue]lua.LValue
	// metatableSnapshots holds the init-time raw metatable of every
	// snapshotted table (the globals table _G plus each entry in
	// tableSnapshots). gopher-lua's base lib exposes setmetatable, so
	// a script can do `setmetatable(_G, { __index = function()
	// return "pwned" end })` -- the next pooled eval reading any
	// undefined global would then fall through the poisoned __index.
	// The same risk applies to the standard-library tables (string,
	// math, ...). We restore each table's metatable on reset; if the
	// original had none, we restore lua.LNil (which strips any
	// metatable installed by the script).
	metatableSnapshots map[*lua.LTable]lua.LValue
	// ctxBinding is a pre-allocated *LUserData stashed in the state's
	// registry under luaCtxRegistryKey. Its .Value holds the active
	// *luaScriptContext for the duration of an eval. Using the state's
	// own registry (instead of a global map + sync.RWMutex) keeps the
	// redis.call / redis.pcall lookup lock-free and local, which is
	// critical for high-concurrency workloads where a single script
	// may issue dozens of redis.call invocations.
	ctxBinding *lua.LUserData
	// scratchKeys is a reusable slice for collecting table keys during
	// reset / resetTableContents. Each reset leaves it sliced to
	// [:0] so subsequent resets reuse the underlying array. If a
	// pathological script inflates it past luaScratchKeysMaxCap we
	// drop the backing array to avoid pinning unbounded memory on
	// pooled states.
	scratchKeys []lua.LValue
}

// luaScratchKeysMaxCap bounds the backing array retained by
// scratchKeys across resets. Beyond this we drop the slice so one
// rogue script does not inflate the pool's per-state footprint
// indefinitely. Chosen to cover typical EVAL globals comfortably
// (base stdlib + redis/cjson/cmsgpack + a handful of user globals).
const luaScratchKeysMaxCap = 1024

// luaLookupContext returns the *luaScriptContext bound to state for
// the current eval, reading it from the state's own registry. Because
// each pooled *lua.LState is used by at most one goroutine at a time,
// this lookup needs no synchronisation -- unlike the previous global
// map guarded by sync.RWMutex, which under BullMQ-style workloads
// (dozens of redis.call invocations per script, thousands of scripts/s)
// became a global RLock contention point.
//
// The registry entry is a pre-allocated *LUserData (see
// pooledLuaState.ctxBinding) whose .Value is mutated by bind/unbind.
// Reading it therefore amortises to a single pointer load + type
// assertion per redis.call.
func luaLookupContext(state *lua.LState) (*luaScriptContext, bool) {
	ud, ok := state.GetField(state.Get(lua.RegistryIndex), luaCtxRegistryKey).(*lua.LUserData)
	if !ok || ud == nil {
		return nil, false
	}
	ctx, ok := ud.Value.(*luaScriptContext)
	if !ok || ctx == nil {
		return nil, false
	}
	return ctx, true
}

// getLuaPool returns the RedisServer's pooled lua state pool,
// creating it on first use. The constructor path (NewRedisServer)
// always pre-populates r.luaPool; this lazy fallback exists so unit
// tests that construct a bare &RedisServer{} literal (common in this
// package) do not NPE the first time EVAL is exercised.
func (r *RedisServer) getLuaPool() *luaStatePool {
	r.luaPoolOnce.Do(func() {
		if r.luaPool == nil {
			r.luaPool = newLuaStatePoolWithMaxIdle(r.luaPoolMaxIdle)
		}
	})
	return r.luaPool
}

// newLuaStatePool returns a bounded pool sized at DefaultLuaPoolMaxIdle.
// Used by test fixtures and any caller that does not thread a
// configured cap through. Production wires the explicit cap via
// newLuaStatePoolWithMaxIdle from NewRedisServer.
func newLuaStatePool() *luaStatePool {
	return newLuaStatePoolWithMaxIdle(DefaultLuaPoolMaxIdle)
}

// newLuaStatePoolWithMaxIdle returns a pool whose idle backing
// channel is sized at maxIdle. Non-positive values clamp to
// DefaultLuaPoolMaxIdle to keep get/put semantics well-defined
// (cap=0 would make every put() drop, which is never what we want
// — callers asking for "no pool" should bypass the pool entirely).
func newLuaStatePoolWithMaxIdle(maxIdle int) *luaStatePool {
	if maxIdle < 1 {
		maxIdle = DefaultLuaPoolMaxIdle
	}
	return &luaStatePool{
		idle:    make(chan *pooledLuaState, maxIdle),
		maxIdle: maxIdle,
	}
}

// newPooledLuaState builds a fresh pooled state: base libs, dangerous
// loaders nil-ed, a per-state ctxBinding userdata stashed in the Lua
// registry, redis/cjson/cmsgpack closures wired to that binding, and a
// snapshot of globals for leak-free reset.
func newPooledLuaState() *pooledLuaState {
	state := lua.NewState(lua.Options{SkipOpenLibs: true})
	openLuaLib(state, lua.BaseLibName, lua.OpenBase)
	openLuaLib(state, lua.TabLibName, lua.OpenTable)
	openLuaLib(state, lua.StringLibName, lua.OpenString)
	openLuaLib(state, lua.MathLibName, lua.OpenMath)

	for _, name := range []string{"dofile", "load", "loadfile", "loadstring", "module", "require"} {
		state.SetGlobal(name, lua.LNil)
	}

	// Pre-allocate the per-state context binding and stash it in the
	// state's registry. redis.call / redis.pcall read this userdata
	// (lock-free, per-state) to find the active *luaScriptContext for
	// the current eval.
	ctxBinding := state.NewUserData()
	state.SetField(state.Get(lua.RegistryIndex), luaCtxRegistryKey, ctxBinding)

	registerPooledRedisModule(state)
	registerCJSONModule(state)
	registerCMsgpackModule(state)

	// Expose table.unpack as the top-level `unpack` just like the
	// non-pooled path in initLuaGlobals does -- keeping the base set
	// identical across paths avoids subtle semantic drift.
	if tableModule, ok := state.GetGlobal("table").(*lua.LTable); ok {
		if unpack := tableModule.RawGetString("unpack"); unpack != lua.LNil {
			state.SetGlobal("unpack", unpack)
		}
	}

	globalsSnapshot, tableSnapshots, metatableSnapshots := snapshotGlobals(state)
	return &pooledLuaState{
		state:              state,
		globalsSnapshot:    globalsSnapshot,
		tableSnapshots:     tableSnapshots,
		metatableSnapshots: metatableSnapshots,
		ctxBinding:         ctxBinding,
	}
}

// snapshotGlobals captures the full set of globals (string AND
// non-string keys) plus shallow snapshots of every nested table value
// reachable from _G, AND the raw metatable of each of those tables
// (plus _G itself). Returning all three lets resetPooledLuaState
// defeat three classes of pool-state leaks:
//
//  1. Non-string-keyed globals. Lua allows any non-nil, non-NaN value
//     as a table key. A malicious script doing `_G[42] = "secret"` or
//     `_G[true] = "bad"` would persist across pool reuse if we only
//     snapshotted string keys. Iterating with ForEach over LValue keys
//     closes this hole.
//
//  2. Table poisoning. Standard-library tables are mutable in
//     gopher-lua, and the snapshot only holds a reference to the
//     table object. A script doing `string.upper = function() return
//     "pwned" end` mutates the shared table in place; merely
//     re-binding the global name `string` to its original LTable
//     value on reset is not enough. We therefore shallow-snapshot
//     every LTable-typed global's contents at init time and restore
//     them on reset. Inner tables are not recursed into -- they are
//     expected to hold leaf values (functions, numbers, strings) in
//     the libraries we install; if that ever changes, extend this.
//
//  3. Metatable poisoning. gopher-lua's base library exposes
//     setmetatable, so a script can do
//     `setmetatable(_G, { __index = function() return "pwned" end })`
//     and the next pooled eval that reads any undefined global (which
//     triggers __index) would observe attacker-controlled behaviour.
//     The same risk applies to every whitelisted table (string, math,
//     ...). Snapshotting each table's raw metatable at init lets
//     reset put the original back; when a table had no metatable,
//     the snapshot holds lua.LNil and reset strips whatever the
//     script installed.
//
// We deliberately skip snapshotting _G's own contents as a "table
// snapshot": _G IS the globals table, so that entry would be
// redundant with the outer globals snapshot. Any other self-reference
// is handled the same way (by *LTable identity). _G's metatable is
// still captured, because the poisoning surface applies to _G too.
//
// We read each table's metatable via the exported LTable.Metatable
// field (not state.GetMetatable) to avoid dispatching through
// __metatable -- we want the raw pointer so SetMetatable can restore
// it verbatim.
func snapshotGlobals(state *lua.LState) (
	map[lua.LValue]lua.LValue,
	map[*lua.LTable]map[lua.LValue]lua.LValue,
	map[*lua.LTable]lua.LValue,
) {
	globals := state.G.Global
	snapshot := make(map[lua.LValue]lua.LValue, luaInitialGlobalsHint)
	tableSnaps := make(map[*lua.LTable]map[lua.LValue]lua.LValue, luaWhitelistedTableHint)
	metaSnaps := make(map[*lua.LTable]lua.LValue, luaWhitelistedTableHint+1)

	// _G itself is a poisoning target (setmetatable(_G, ...)).
	metaSnaps[globals] = rawMetatable(globals)

	globals.ForEach(func(k, v lua.LValue) {
		snapshot[k] = v
		if tbl, ok := v.(*lua.LTable); ok && tbl != globals {
			// Shallow copy the table's contents. Keys may be
			// non-string (e.g. array-like entries).
			inner := make(map[lua.LValue]lua.LValue, tbl.Len()+luaResetKeySlack)
			tbl.ForEach(func(ik, iv lua.LValue) {
				inner[ik] = iv
			})
			tableSnaps[tbl] = inner
			// Capture the raw metatable exactly once per *LTable.
			// A given library table appears in _G under one name, so
			// there is no duplication risk here in practice; even if
			// there were, the value would be identical.
			if _, seen := metaSnaps[tbl]; !seen {
				metaSnaps[tbl] = rawMetatable(tbl)
			}
		}
	})
	return snapshot, tableSnaps, metaSnaps
}

// rawMetatable returns the LTable's raw metatable field, normalising a
// Go nil into lua.LNil so callers can pass the result straight to
// state.SetMetatable (which requires an LValue, not an untyped nil).
// We bypass state.GetMetatable deliberately: that path respects the
// __metatable field and can return something other than the real
// metatable, which would corrupt restore-on-reset if a script set
// __metatable = "blocked".
func rawMetatable(tbl *lua.LTable) lua.LValue {
	if tbl.Metatable == nil {
		return lua.LNil
	}
	return tbl.Metatable
}

// resetPooledLuaState wipes all user-introduced globals and restores
// the whitelisted ones (including the contents of nested tables like
// `string`, `math`, `redis`), then truncates the value stack. It is
// the heart of the security invariant: anything the script did to
// globals must not be observable by the next user.
//
// Ordering matters:
//  1. Restore every snapshotted table's metatable FIRST. A poisoned
//     __index / __newindex would otherwise intercept the subsequent
//     RawSet / ForEach work we do to clean up fields. In practice
//     RawSet bypasses metamethods already, but restoring the
//     metatable first keeps any future code that uses non-raw access
//     safe-by-construction.
//  2. Reset nested whitelisted tables' field sets. Doing this BEFORE
//     restoring the globals' top-level bindings means we mutate the
//     ORIGINAL table objects (the ones snapshot still references by
//     pointer), even if the script rebound `string = nil` at the
//     global level -- the original LTable is still alive and held
//     via our tableSnapshots map key.
//  3. Delete top-level globals not in the snapshot (KEYS, ARGV,
//     GLOBAL_LEAK, _G[42], etc). We iterate ALL key types, not just
//     strings, so non-string-keyed leaks (`_G[42] = "secret"`) do not
//     survive.
//  4. Restore top-level whitelisted globals. This fixes e.g.
//     `redis = nil` by re-binding `redis` to the original module
//     table.
func (p *pooledLuaState) reset() {
	globals := p.state.G.Global

	// (1) Restore the raw metatable of every snapshotted table.
	// This blocks setmetatable(_G, {__index=...}) and
	// setmetatable(string, {...}) from leaking a poisoned fallback
	// into the next eval. SetMetatable with lua.LNil strips any
	// metatable the script installed where there was none originally.
	for tbl, mt := range p.metatableSnapshots {
		p.state.SetMetatable(tbl, mt)
	}

	// (2) Restore inner contents of every snapshotted whitelisted
	// table. This defeats poisoning attacks like
	// `string.upper = function() return "pwned" end`.
	//
	// resetTableContents borrows p.scratchKeys as a working slice.
	// We pass it in and receive the (possibly grown) backing array
	// back so successive calls within the same reset share one
	// allocation.
	scratch := p.scratchKeys[:0]
	for tbl, originalFields := range p.tableSnapshots {
		scratch = resetTableContents(tbl, originalFields, scratch[:0])
	}

	// (3) Collect all current global keys (of any type). Mutating
	// the table inside ForEach is unsafe, so snapshot keys first.
	scratch = scratch[:0]
	globals.ForEach(func(k, _ lua.LValue) {
		scratch = append(scratch, k)
	})

	// Delete any key not in the init-time snapshot: these are
	// user-introduced globals (KEYS, ARGV, GLOBAL_LEAK, _G[42],
	// _G[true], ...).
	//
	// We use RawSet (not RawSetH) because gopher-lua stores integer
	// keys in an internal `array` slice rather than `dict`; RawSetH
	// only touches `dict`, so a call like RawSetH(LNumber(42), LNil)
	// leaves the array entry intact. RawSet dispatches to the right
	// storage by key type.
	for _, k := range scratch {
		if _, keep := p.globalsSnapshot[k]; !keep {
			globals.RawSet(k, lua.LNil)
		}
	}

	// (4) Restore every whitelisted global to its original value.
	// This covers the case where a script rebinds an allowed global
	// (e.g. `redis = something`) -- we simply put the original back.
	for k, v := range p.globalsSnapshot {
		globals.RawSet(k, v)
	}

	// Drop anything the script may have left on the value stack.
	p.state.SetTop(0)

	// Clear any request-scoped context bound to the state via
	// LState.SetContext (done in runLuaScript). Without this, the
	// pooled *lua.LState keeps a reference to the previous request's
	// context.Context -- and transitively anything the context retains
	// (timers, cancel funcs, attached values) -- until the state is
	// reused or garbage-collected. That causes memory retention and
	// delays cancellation propagation for the prior request's chain.
	// RemoveContext is the canonical API for this and is preferred over
	// SetContext(context.Background()) for clearer intent.
	p.state.RemoveContext()

	// Retain scratch for the next reset, but bound the backing array
	// so a pathological script that created thousands of globals does
	// not permanently bloat every pooled state. If we exceeded the
	// cap, drop the slice -- the next reset will reallocate at the
	// modest default size.
	if cap(scratch) > luaScratchKeysMaxCap {
		p.scratchKeys = nil
	} else {
		p.scratchKeys = scratch[:0]
	}
}

// resetTableContents restores tbl's entries so that it exactly
// matches originalFields: extra keys added by the script are deleted,
// and every original key is re-bound to its original value. Inner
// tables are treated as shallow: if a script mutated `string.upper`,
// the original function value (still alive via originalFields) is
// put back; if a script added a new field (`string.pwn = 1`), the
// field is deleted.
//
// scratch is a caller-provided slice used to buffer the current key
// set (we cannot mutate a table while ForEach iterates it). The
// (possibly grown) slice is returned so the caller can keep reusing
// the underlying array across invocations.
func resetTableContents(tbl *lua.LTable, originalFields map[lua.LValue]lua.LValue, scratch []lua.LValue) []lua.LValue {
	currentKeys := scratch[:0]
	tbl.ForEach(func(k, _ lua.LValue) {
		currentKeys = append(currentKeys, k)
	})
	for _, k := range currentKeys {
		if _, keep := originalFields[k]; !keep {
			tbl.RawSet(k, lua.LNil)
		}
	}
	for k, v := range originalFields {
		tbl.RawSet(k, v)
	}
	return currentKeys
}

// get acquires a pooled state and binds the given *luaScriptContext
// so that redis.call / redis.pcall can see it. Binding is a single
// pointer write to the state-local ctxBinding userdata -- no lock,
// no global map.
//
// A non-blocking recv that does not match either case fires the
// default branch (allocation miss). The defensive nil guard
// preserves behaviour if a future refactor ever sends an unexpected
// value through the idle channel.
//
// The ctxBinding assignment and return are centralized after the
// select so every path goes through one binding step. This was a
// gemini r1 review nit: refactor for clarity rather than duplicate
// the binding in each branch.
func (p *luaStatePool) get(ctx *luaScriptContext) *pooledLuaState {
	var pls *pooledLuaState
	select {
	case pls = <-p.idle:
		if pls != nil {
			p.hits.Add(1)
		} else {
			// Intentionally-unreachable defence in depth:
			// put() above already guards `if pls == nil ||
			// pls.state == nil { return }`, so no caller can
			// enqueue a nil. If a future refactor breaks that
			// invariant the nil arrives here as a miss + fresh
			// allocation rather than a runtime panic. Not a
			// branch a reader should try to hit.
			p.misses.Add(1)
			pls = newPooledLuaState()
		}
	default:
		p.misses.Add(1)
		pls = newPooledLuaState()
	}
	pls.ctxBinding.Value = ctx
	return pls
}

// put resets the state and tries to return it to the pool. If the
// state is closed (shouldn't happen on the happy path) or the idle
// channel is full it is dropped so the GC can reclaim it; the drop
// counter makes pool saturation observable so operators can tune
// maxIdle.
//
// Fast-path: when len(p.idle) already equals maxIdle the put is
// guaranteed to overflow, so skip the (non-trivial) pls.reset()
// — restoring the globals / tables / metatables snapshots is
// pointless work for a state that is about to be Close()'d. This is
// a gemini r1 review optimisation; under saturation the EVAL hot
// path spends much less CPU on doomed resets. The check is a racy
// snapshot of len(); concurrent puts can still observe stale
// "len < max" and race into the select below, where the same
// channel-full guard catches them — so the fast-path is a strict
// improvement, not a new correctness requirement.
func (p *luaStatePool) put(pls *pooledLuaState) {
	if pls == nil || pls.state == nil {
		return
	}
	// Clear the binding so a stale *luaScriptContext cannot be
	// observed via a pooled state that is briefly re-acquired by a
	// future get() before the caller writes a fresh context.
	if pls.ctxBinding != nil {
		pls.ctxBinding.Value = nil
	}
	if pls.state.IsClosed() {
		return
	}
	if len(p.idle) >= p.maxIdle {
		p.drops.Add(1)
		pls.state.Close()
		return
	}
	pls.reset()
	select {
	case p.idle <- pls:
	default:
		// Idle channel filled between the fast-path check and the
		// select (concurrent puts winning the race). Drop the state.
		p.drops.Add(1)
		pls.state.Close()
	}
}

// Hits / Misses / Drops are test hooks. They count Get/Put outcomes,
// not allocations proper, but in practice they track allocation
// avoidance well enough for the "is the pool actually being used?"
// test and the "is maxIdle too low for the workload?" diagnostic.
func (p *luaStatePool) Hits() uint64   { return p.hits.Load() }
func (p *luaStatePool) Misses() uint64 { return p.misses.Load() }
func (p *luaStatePool) Drops() uint64  { return p.drops.Load() }

// Idle reports the number of states currently sitting in the pool.
// Useful for metrics gauges and for tests asserting bounded retention.
func (p *luaStatePool) Idle() int { return len(p.idle) }

// MaxIdle reports the configured cap. Exposed for diagnostics so
// /admin can surface "(idle / maxIdle)" to operators.
func (p *luaStatePool) MaxIdle() int { return p.maxIdle }

// registerPooledRedisModule installs redis.call / redis.pcall /
// redis.sha1hex / redis.status_reply / redis.error_reply where the
// call/pcall closures resolve the *luaScriptContext per-invocation
// via luaLookupContext, so a single pre-registered module works for
// every eval the state is reused for.
func registerPooledRedisModule(state *lua.LState) {
	module := state.NewTable()
	state.SetFuncs(module, map[string]lua.LGFunction{
		"call": func(scriptState *lua.LState) int {
			ctx, ok := luaLookupContext(scriptState)
			// Must guard against ctx == nil as well as !ok: the
			// bench path and misuse can luaBindContext(nil), which
			// stores a (nil, true) entry. Dereferencing that in
			// luaRedisCommand would panic.
			if !ok || ctx == nil {
				scriptState.RaiseError("redis.call invoked without an active script context")
				return 0
			}
			return luaRedisCommand(scriptState, ctx, true)
		},
		"pcall": func(scriptState *lua.LState) int {
			ctx, ok := luaLookupContext(scriptState)
			if !ok || ctx == nil {
				scriptState.Push(luaErrorTable(scriptState, "redis.pcall invoked without an active script context"))
				return 1
			}
			return luaRedisCommand(scriptState, ctx, false)
		},
		"sha1hex": func(scriptState *lua.LState) int {
			scriptState.Push(lua.LString(luaScriptSHA(scriptState.CheckString(1))))
			return 1
		},
		"status_reply": func(scriptState *lua.LState) int {
			reply := scriptState.NewTable()
			reply.RawSetString(luaTypeOKKey, lua.LString(scriptState.CheckString(1)))
			scriptState.Push(reply)
			return 1
		},
		"error_reply": func(scriptState *lua.LState) int {
			reply := scriptState.NewTable()
			reply.RawSetString(luaTypeErrKey, lua.LString(scriptState.CheckString(1)))
			scriptState.Push(reply)
			return 1
		},
	})
	state.SetGlobal("redis", module)
}
