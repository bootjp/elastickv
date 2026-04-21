package adapter

import (
	"sync"
	"sync/atomic"

	lua "github.com/yuin/gopher-lua"
)

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

// luaStatePool pools *lua.LState instances to cut heap/GC pressure on
// high-rate EVAL / EVALSHA workloads (e.g. BullMQ ~10 scripts/s, where
// each fresh state allocs ~34% of in-use heap via newFuncContext,
// newRegistry, newFunctionProto).
//
// Security invariant: no state must leak between scripts. Each pooled
// state is initialised with a fixed set of base globals (redis, cjson,
// cmsgpack, table/string/math + base lib helpers, and nil-ed loaders).
// Two snapshots are captured at construction time:
//
//   - globalsSnapshot: the full (*any*-keyed) _G map at init. Using an
//     LValue-keyed map lets the reset path catch non-string-keyed
//     leaks like `_G[42] = "secret"` or `_G[true] = "bad"`, which
//     would otherwise survive a naive string-only wipe.
//   - tableSnapshots: a shallow map from each whitelisted nested
//     table (string, math, table, redis, cjson, cmsgpack) to its
//     init-time field set. This is what blocks table-poisoning
//     attacks such as `string.upper = function() return "pwned" end`
//     -- merely restoring the `string` *reference* on _G would leave
//     the shared table's fields still mutated.
//
// On release, the reset routine
//
//  1. walks each snapshotted nested table and restores its contents
//     (deletes script-added fields, rebinds original fields),
//  2. walks the current global table and deletes every key -- of any
//     type -- that is not present in the globals snapshot (removes
//     user-added globals such as KEYS, ARGV, GLOBAL_LEAK, _G[42]),
//     and
//  3. restores every globals-snapshot key to its original value (so a
//     script that did `table = nil` or `redis = evil` cannot poison
//     the next script).
//
// Additionally the value stack is truncated to 0 and the script
// context binding is cleared so the redis.call/pcall closures cannot
// be invoked against a stale context.
//
// The redis / cjson / cmsgpack closures are registered ONCE at pool
// fill time and read the per-eval *luaScriptContext out of
// luaStateBindings, which is set on acquire and cleared on release.
// Closures that would otherwise capture a fresh context per eval no
// longer need to be re-registered, which is what makes pooling safe
// and cheap.
type luaStatePool struct {
	pool sync.Pool

	// hits / misses are exposed for tests and metrics.
	hits   atomic.Uint64
	misses atomic.Uint64
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
}

// luaStateBindings maps a pooled *lua.LState to the per-eval
// *luaScriptContext. The pre-registered redis.call / redis.pcall
// closures look up the binding on every invocation, which means the
// state does not have to be rewired with fresh closures each time it
// leaves the pool.
//
// Access is guarded by a sync.RWMutex because a single pooled state
// is only ever used by one goroutine at a time, but different pooled
// states are looked up concurrently.
var luaStateBindings = struct {
	sync.RWMutex
	m map[*lua.LState]*luaScriptContext
}{m: map[*lua.LState]*luaScriptContext{}}

func luaBindContext(state *lua.LState, ctx *luaScriptContext) {
	luaStateBindings.Lock()
	luaStateBindings.m[state] = ctx
	luaStateBindings.Unlock()
}

func luaUnbindContext(state *lua.LState) {
	luaStateBindings.Lock()
	delete(luaStateBindings.m, state)
	luaStateBindings.Unlock()
}

func luaLookupContext(state *lua.LState) (*luaScriptContext, bool) {
	luaStateBindings.RLock()
	ctx, ok := luaStateBindings.m[state]
	luaStateBindings.RUnlock()
	return ctx, ok
}

// getLuaPool returns the RedisServer's pooled lua state pool,
// creating it on first use. The constructor path (NewRedisServer)
// always pre-populates r.luaPool; this lazy fallback exists so unit
// tests that construct a bare &RedisServer{} literal (common in this
// package) do not NPE the first time EVAL is exercised.
func (r *RedisServer) getLuaPool() *luaStatePool {
	r.luaPoolOnce.Do(func() {
		if r.luaPool == nil {
			r.luaPool = newLuaStatePool()
		}
	})
	return r.luaPool
}

// newLuaStatePool returns a pool whose New func builds a freshly
// initialised, reusable *lua.LState. The state has all libs opened,
// dangerous loaders nil-ed, and redis/cjson/cmsgpack registered with
// closures that dispatch through luaLookupContext (not a captured
// *luaScriptContext pointer -- that would be wrong for reuse).
func newLuaStatePool() *luaStatePool {
	p := &luaStatePool{}
	p.pool.New = func() any {
		return newPooledLuaState()
	}
	return p
}

// newPooledLuaState builds a fresh pooled state: base libs, dangerous
// loaders nil-ed, redis/cjson/cmsgpack closures wired to the global
// binding table, and a snapshot of globals for leak-free reset.
func newPooledLuaState() *pooledLuaState {
	state := lua.NewState(lua.Options{SkipOpenLibs: true})
	openLuaLib(state, lua.BaseLibName, lua.OpenBase)
	openLuaLib(state, lua.TabLibName, lua.OpenTable)
	openLuaLib(state, lua.StringLibName, lua.OpenString)
	openLuaLib(state, lua.MathLibName, lua.OpenMath)

	for _, name := range []string{"dofile", "load", "loadfile", "loadstring", "module", "require"} {
		state.SetGlobal(name, lua.LNil)
	}

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

	globalsSnapshot, tableSnapshots := snapshotGlobals(state)
	return &pooledLuaState{
		state:           state,
		globalsSnapshot: globalsSnapshot,
		tableSnapshots:  tableSnapshots,
	}
}

// snapshotGlobals captures the full set of globals (string AND
// non-string keys) plus shallow snapshots of every nested table value
// reachable from _G. Returning both lets resetPooledLuaState defeat two
// classes of pool-state leaks:
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
// We deliberately skip snapshotting _G's own contents as a "table
// snapshot": _G IS the globals table, so that entry would be
// redundant with the outer globals snapshot. Any other self-reference
// is handled the same way (by *LTable identity).
func snapshotGlobals(state *lua.LState) (map[lua.LValue]lua.LValue, map[*lua.LTable]map[lua.LValue]lua.LValue) {
	globals := state.G.Global
	snapshot := make(map[lua.LValue]lua.LValue, luaInitialGlobalsHint)
	tableSnaps := make(map[*lua.LTable]map[lua.LValue]lua.LValue, luaWhitelistedTableHint)

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
		}
	})
	return snapshot, tableSnaps
}

// resetPooledLuaState wipes all user-introduced globals and restores
// the whitelisted ones (including the contents of nested tables like
// `string`, `math`, `redis`), then truncates the value stack. It is
// the heart of the security invariant: anything the script did to
// globals must not be observable by the next user.
//
// Ordering matters:
//  1. Reset nested whitelisted tables first. Doing this BEFORE
//     restoring the globals' top-level bindings means we mutate the
//     ORIGINAL table objects (the ones snapshot still references by
//     pointer), even if the script rebound `string = nil` at the
//     global level -- the original LTable is still alive and held
//     via our tableSnapshots map key.
//  2. Delete top-level globals not in the snapshot (KEYS, ARGV,
//     GLOBAL_LEAK, _G[42], etc). We iterate ALL key types, not just
//     strings, so non-string-keyed leaks (`_G[42] = "secret"`) do not
//     survive.
//  3. Restore top-level whitelisted globals. This fixes e.g.
//     `redis = nil` by re-binding `redis` to the original module
//     table.
func (p *pooledLuaState) reset() {
	globals := p.state.G.Global

	// (1) Restore inner contents of every snapshotted whitelisted
	// table. This defeats poisoning attacks like
	// `string.upper = function() return "pwned" end`.
	for tbl, originalFields := range p.tableSnapshots {
		resetTableContents(tbl, originalFields)
	}

	// (2) Collect all current global keys (of any type). Mutating
	// the table inside ForEach is unsafe, so snapshot keys first.
	currentKeys := make([]lua.LValue, 0, len(p.globalsSnapshot)+luaResetKeySlack)
	globals.ForEach(func(k, _ lua.LValue) {
		currentKeys = append(currentKeys, k)
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
	for _, k := range currentKeys {
		if _, keep := p.globalsSnapshot[k]; !keep {
			globals.RawSet(k, lua.LNil)
		}
	}

	// (3) Restore every whitelisted global to its original value.
	// This covers the case where a script rebinds an allowed global
	// (e.g. `redis = something`) -- we simply put the original back.
	for k, v := range p.globalsSnapshot {
		globals.RawSet(k, v)
	}

	// Drop anything the script may have left on the value stack.
	p.state.SetTop(0)
}

// resetTableContents restores tbl's entries so that it exactly
// matches originalFields: extra keys added by the script are deleted,
// and every original key is re-bound to its original value. Inner
// tables are treated as shallow: if a script mutated `string.upper`,
// the original function value (still alive via originalFields) is
// put back; if a script added a new field (`string.pwn = 1`), the
// field is deleted.
func resetTableContents(tbl *lua.LTable, originalFields map[lua.LValue]lua.LValue) {
	// Gather current keys first to avoid mutating during ForEach.
	currentKeys := make([]lua.LValue, 0, len(originalFields)+luaResetKeySlack)
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
}

// get acquires a pooled state and binds the given *luaScriptContext
// so that redis.call / redis.pcall can see it.
func (p *luaStatePool) get(ctx *luaScriptContext) *pooledLuaState {
	pls, ok := p.pool.Get().(*pooledLuaState)
	if !ok || pls == nil {
		// New func never returns nil, but defend against misuse.
		pls = newPooledLuaState()
		p.misses.Add(1)
	} else {
		p.hits.Add(1)
	}
	luaBindContext(pls.state, ctx)
	return pls
}

// put resets the state and returns it to the pool. If the state is
// somehow closed (shouldn't happen on the happy path), it is dropped
// so a dead VM is never handed out again.
func (p *luaStatePool) put(pls *pooledLuaState) {
	if pls == nil || pls.state == nil {
		return
	}
	luaUnbindContext(pls.state)
	if pls.state.IsClosed() {
		return
	}
	pls.reset()
	p.pool.Put(pls)
}

// Hits / Misses are test hooks. They count Get() outcomes, not
// allocations proper, but in practice they track allocation avoidance
// well enough for the "is the pool actually being used?" test.
func (p *luaStatePool) Hits() uint64   { return p.hits.Load() }
func (p *luaStatePool) Misses() uint64 { return p.misses.Load() }

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
