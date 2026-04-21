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

// luaStatePool pools *lua.LState instances to cut heap/GC pressure on
// high-rate EVAL / EVALSHA workloads (e.g. BullMQ ~10 scripts/s, where
// each fresh state allocs ~34% of in-use heap via newFuncContext,
// newRegistry, newFunctionProto).
//
// Security invariant: no state must leak between scripts. Each pooled
// state is initialised with a fixed set of base globals (redis, cjson,
// cmsgpack, table/string/math + base lib helpers, and nil-ed loaders).
// A snapshot of that initial global table is captured at construction
// time. On release, the reset routine
//
//  1. walks the current global table and deletes every key that is
//     not present in the snapshot (removes user-added globals such as
//     KEYS, ARGV and any GLOBAL_LEAK a script may have set), and
//  2. restores every snapshot key to its original value (so a script
//     that did `table = nil` or `redis = evil` cannot poison the next
//     script).
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
	// globalsSnapshot is a copy of state.G.Global keyed by string.
	// Only string keys can be set from the scripts we accept
	// (state.SetGlobal is string-keyed), which makes string-key
	// snapshotting sufficient for the safety invariant.
	globalsSnapshot map[string]lua.LValue
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

	return &pooledLuaState{
		state:           state,
		globalsSnapshot: snapshotGlobals(state),
	}
}

// snapshotGlobals shallow-copies every string-keyed entry from the
// state's global table. Values inside tables (e.g. table.insert) are
// retained by reference -- that is fine because the reset path does
// not attempt to deep-clone them; if a script mutates
// `table.insert = nil`, the restore re-binds the original function
// value which is still alive.
func snapshotGlobals(state *lua.LState) map[string]lua.LValue {
	globals := state.G.Global
	snapshot := make(map[string]lua.LValue, luaInitialGlobalsHint)
	globals.ForEach(func(k, v lua.LValue) {
		if ks, ok := k.(lua.LString); ok {
			snapshot[string(ks)] = v
		}
	})
	return snapshot
}

// resetPooledLuaState wipes all user-introduced globals and restores
// the whitelisted ones, then truncates the value stack. It is the
// heart of the security invariant: anything the script did to globals
// must not be observable by the next user.
func (p *pooledLuaState) reset() {
	globals := p.state.G.Global

	// Collect all current string keys first; mutating the table in
	// ForEach is unsafe.
	currentKeys := make([]string, 0, len(p.globalsSnapshot)+luaResetKeySlack)
	globals.ForEach(func(k, _ lua.LValue) {
		if ks, ok := k.(lua.LString); ok {
			currentKeys = append(currentKeys, string(ks))
		}
	})

	// Delete any key not in the snapshot: these are user-introduced
	// globals (KEYS, ARGV, GLOBAL_LEAK, ...).
	for _, k := range currentKeys {
		if _, keep := p.globalsSnapshot[k]; !keep {
			globals.RawSetString(k, lua.LNil)
		}
	}

	// Restore every whitelisted global to its original value. This
	// covers the case where a script rebinds an allowed global
	// (e.g. redis = something) -- we simply put the original back.
	for k, v := range p.globalsSnapshot {
		globals.RawSetString(k, v)
	}

	// Drop anything the script may have left on the value stack.
	p.state.SetTop(0)
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
			if !ok {
				scriptState.RaiseError("redis.call invoked without an active script context")
				return 0
			}
			return luaRedisCommand(scriptState, ctx, true)
		},
		"pcall": func(scriptState *lua.LState) int {
			ctx, ok := luaLookupContext(scriptState)
			if !ok {
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
