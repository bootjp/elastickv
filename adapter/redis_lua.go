package adapter

import (
	"context"
	"crypto/sha1" // #nosec G505 -- Redis EVALSHA specifies SHA1 script digests.
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
	"github.com/tidwall/redcon"
	"github.com/vmihailenco/msgpack/v5"
	lua "github.com/yuin/gopher-lua"
)

const (
	luaTypeErrKey    = "err"
	luaTypeOKKey     = "ok"
	luaTypeArrayBase = 1
)

type luaReplyKind int

const (
	luaReplyNil luaReplyKind = iota
	luaReplyInt
	luaReplyString
	luaReplyArray
	luaReplyStatus
	luaReplyError
	luaReplyBool
)

type luaReply struct {
	kind    luaReplyKind
	text    string
	integer int64
	array   []luaReply
	boolean bool
}

func luaNilReply() luaReply {
	return luaReply{kind: luaReplyNil}
}

func luaIntReply(v int64) luaReply {
	return luaReply{kind: luaReplyInt, integer: v}
}

func luaStringReply(v string) luaReply {
	return luaReply{kind: luaReplyString, text: v}
}

func luaArrayReply(values ...luaReply) luaReply {
	return luaReply{kind: luaReplyArray, array: values}
}

func luaStatusReply(v string) luaReply {
	return luaReply{kind: luaReplyStatus, text: v}
}

func luaErrorReply(v string) luaReply {
	return luaReply{kind: luaReplyError, text: v}
}

func luaBoolReply(v bool) luaReply {
	return luaReply{kind: luaReplyBool, boolean: v}
}

func (r *RedisServer) eval(conn redcon.Conn, cmd redcon.Command) {
	script := string(cmd.Args[1])
	sha := luaScriptSHA(script)
	r.cacheScript(sha, script)
	r.runLuaScript(conn, script, cmd.Args[2:])
}

func (r *RedisServer) evalsha(conn redcon.Conn, cmd redcon.Command) {
	script, ok := r.lookupScript(strings.ToLower(string(cmd.Args[1])))
	if !ok {
		conn.WriteError("NOSCRIPT No matching script. Please use EVAL.")
		return
	}
	r.runLuaScript(conn, script, cmd.Args[2:])
}

func luaScriptSHA(script string) string {
	// #nosec G401 -- Redis script identity is SHA1 by protocol.
	sum := sha1.Sum([]byte(script))
	return hex.EncodeToString(sum[:])
}

func (r *RedisServer) cacheScript(sha string, script string) {
	r.scriptMu.Lock()
	defer r.scriptMu.Unlock()
	r.scriptCache[sha] = script
}

func (r *RedisServer) lookupScript(sha string) (string, bool) {
	r.scriptMu.RLock()
	defer r.scriptMu.RUnlock()
	script, ok := r.scriptCache[sha]
	return script, ok
}

func (r *RedisServer) runLuaScript(conn redcon.Conn, script string, evalArgs [][]byte) {
	keys, argv, err := parseRedisEvalArgs(evalArgs)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	var reply luaReply
	err = r.retryRedisWrite(ctx, func() error {
		scriptCtx := newLuaScriptContext(r)
		defer scriptCtx.Close()
		state := newRedisLuaState()
		defer state.Close()
		state.SetContext(ctx)
		r.initLuaGlobals(state, scriptCtx, keys, argv)

		chunk, err := state.LoadString(script)
		if err != nil {
			return errors.WithStack(err)
		}
		state.Push(chunk)
		if err := state.PCall(0, 1, nil); err != nil {
			return errors.WithStack(err)
		}
		result := state.Get(-1)
		defer state.Pop(1)

		nextReply, err := luaValueToReply(result)
		if err != nil {
			return err
		}
		if err := scriptCtx.commit(); err != nil {
			return err
		}
		reply = nextReply
		return nil
	})
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	writeLuaReply(conn, reply)
}

func parseRedisEvalArgs(args [][]byte) ([][]byte, [][]byte, error) {
	if len(args) == 0 {
		return nil, nil, errors.New("ERR syntax error")
	}
	numKeys, err := strconv.Atoi(string(args[0]))
	if err != nil || numKeys < 0 || numKeys > len(args)-1 {
		return nil, nil, errors.New("ERR syntax error")
	}

	keys := make([][]byte, numKeys)
	copy(keys, args[1:1+numKeys])
	argv := make([][]byte, len(args)-(1+numKeys))
	copy(argv, args[1+numKeys:])
	return keys, argv, nil
}

func (r *RedisServer) initLuaGlobals(state *lua.LState, ctx *luaScriptContext, keys [][]byte, argv [][]byte) {
	state.SetGlobal("KEYS", makeLuaStringArray(state, keys))
	state.SetGlobal("ARGV", makeLuaStringArray(state, argv))
	registerRedisModule(state, ctx)
	registerCJSONModule(state)
	registerCMsgpackModule(state)

	tableModule := state.GetGlobal("table")
	if tbl, ok := tableModule.(*lua.LTable); ok {
		if unpack := tbl.RawGetString("unpack"); unpack != lua.LNil {
			state.SetGlobal("unpack", unpack)
		}
	}
}

func newRedisLuaState() *lua.LState {
	state := lua.NewState(lua.Options{SkipOpenLibs: true})
	openLuaLib(state, lua.BaseLibName, lua.OpenBase)
	openLuaLib(state, lua.TabLibName, lua.OpenTable)
	openLuaLib(state, lua.StringLibName, lua.OpenString)
	openLuaLib(state, lua.MathLibName, lua.OpenMath)

	for _, name := range []string{"dofile", "load", "loadfile", "loadstring", "module", "require"} {
		state.SetGlobal(name, lua.LNil)
	}
	return state
}

func openLuaLib(state *lua.LState, name string, fn lua.LGFunction) {
	state.Push(state.NewFunction(fn))
	state.Push(lua.LString(name))
	state.Call(1, 0)
}

func registerRedisModule(state *lua.LState, ctx *luaScriptContext) {
	module := state.NewTable()
	state.SetFuncs(module, map[string]lua.LGFunction{
		"call": func(scriptState *lua.LState) int {
			return luaRedisCommand(scriptState, ctx, true)
		},
		"pcall": func(scriptState *lua.LState) int {
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

func luaRedisCommand(state *lua.LState, ctx *luaScriptContext, raise bool) int {
	if state.GetTop() == 0 {
		if raise {
			state.RaiseError("Please specify at least one argument for this redis lib call")
			return 0
		}
		state.Push(luaErrorTable(state, "Please specify at least one argument for this redis lib call"))
		return 1
	}

	command := strings.ToUpper(luaValueToCommandArg(state.Get(1)))
	args := make([]string, 0, state.GetTop()-1)
	for i := 2; i <= state.GetTop(); i++ {
		args = append(args, luaValueToCommandArg(state.Get(i)))
	}

	reply, err := ctx.exec(command, args)
	if err != nil {
		if raise {
			state.RaiseError("%s", err.Error())
			return 0
		}
		state.Push(luaErrorTable(state, err.Error()))
		return 1
	}

	pushLuaReply(state, reply)
	return 1
}

func makeLuaStringArray(state *lua.LState, values [][]byte) *lua.LTable {
	tbl := state.NewTable()
	for i, value := range values {
		tbl.RawSetInt(i+luaTypeArrayBase, lua.LString(string(value)))
	}
	return tbl
}

type luaReplyPusher func(*lua.LState, luaReply)

func pushLuaReply(state *lua.LState, reply luaReply) {
	if push := luaReplyPusherFor(reply.kind); push != nil {
		push(state, reply)
		return
	}

	state.Push(lua.LNil)
}

func luaReplyPusherFor(kind luaReplyKind) luaReplyPusher {
	switch kind {
	case luaReplyNil:
		return pushLuaNilReply
	case luaReplyInt:
		return pushLuaIntReply
	case luaReplyString:
		return pushLuaStringReply
	case luaReplyArray:
		return pushLuaArrayReply
	case luaReplyStatus:
		return pushLuaStatusReply
	case luaReplyError:
		return pushLuaErrorReply
	case luaReplyBool:
		return pushLuaBoolReply
	default:
		return nil
	}
}

func pushLuaNilReply(state *lua.LState, _ luaReply) {
	state.Push(lua.LFalse)
}

func pushLuaIntReply(state *lua.LState, reply luaReply) {
	state.Push(lua.LNumber(reply.integer))
}

func pushLuaStringReply(state *lua.LState, reply luaReply) {
	state.Push(lua.LString(reply.text))
}

func pushLuaStatusReply(state *lua.LState, reply luaReply) {
	tbl := state.NewTable()
	tbl.RawSetString(luaTypeOKKey, lua.LString(reply.text))
	state.Push(tbl)
}

func pushLuaErrorReply(state *lua.LState, reply luaReply) {
	state.Push(luaErrorTable(state, reply.text))
}

func pushLuaBoolReply(state *lua.LState, reply luaReply) {
	if reply.boolean {
		state.Push(lua.LTrue)
		return
	}

	state.Push(lua.LFalse)
}

func pushLuaArrayReply(state *lua.LState, reply luaReply) {
	tbl := state.NewTable()
	for i, item := range reply.array {
		pushLuaReply(state, item)
		tbl.RawSetInt(i+luaTypeArrayBase, state.Get(-1))
		state.Pop(1)
	}
	state.Push(tbl)
}

func luaErrorTable(state *lua.LState, msg string) *lua.LTable {
	tbl := state.NewTable()
	tbl.RawSetString(luaTypeErrKey, lua.LString(msg))
	return tbl
}

func registerCJSONModule(state *lua.LState) {
	module := state.NewTable()
	state.SetFuncs(module, map[string]lua.LGFunction{
		"encode": func(scriptState *lua.LState) int {
			goValue, err := luaToGoValue(scriptState.Get(1))
			if err != nil {
				scriptState.RaiseError("%s", err.Error())
				return 0
			}
			raw, err := json.Marshal(goValue)
			if err != nil {
				scriptState.RaiseError("%s", err.Error())
				return 0
			}
			scriptState.Push(lua.LString(string(raw)))
			return 1
		},
		"decode": func(scriptState *lua.LState) int {
			var decoded any
			if err := json.Unmarshal([]byte(scriptState.CheckString(1)), &decoded); err != nil {
				scriptState.RaiseError("%s", err.Error())
				return 0
			}
			scriptState.Push(goToLuaValue(scriptState, decoded))
			return 1
		},
	})
	state.SetGlobal("cjson", module)
}

func registerCMsgpackModule(state *lua.LState) {
	module := state.NewTable()
	state.SetFuncs(module, map[string]lua.LGFunction{
		"unpack": func(scriptState *lua.LState) int {
			var decoded any
			if err := msgpack.Unmarshal([]byte(scriptState.CheckString(1)), &decoded); err != nil {
				scriptState.RaiseError("%s", err.Error())
				return 0
			}
			scriptState.Push(goToLuaValue(scriptState, normalizeDecodedValue(decoded)))
			return 1
		},
	})
	state.SetGlobal("cmsgpack", module)
}

func normalizeDecodedValue(v any) any {
	switch x := v.(type) {
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = normalizeDecodedValue(x[i])
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(x))
		for key, value := range x {
			out[key] = normalizeDecodedValue(value)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(x))
		for key, value := range x {
			out[normalizeDecodedMapKey(key)] = normalizeDecodedValue(value)
		}
		return out
	default:
		return v
	}
}

func normalizeDecodedMapKey(key any) string {
	switch x := key.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case bool:
		return boolToLuaIntString(x)
	}

	if normalized, ok := normalizeSignedMapKey(key); ok {
		return normalized
	}
	if normalized, ok := normalizeUnsignedMapKey(key); ok {
		return normalized
	}
	if normalized, ok := normalizeFloatMapKey(key); ok {
		return normalized
	}

	return fmt.Sprint(key)
}

func normalizeSignedMapKey(key any) (string, bool) {
	switch x := key.(type) {
	case int:
		return strconv.Itoa(x), true
	case int8:
		return strconv.FormatInt(int64(x), 10), true
	case int16:
		return strconv.FormatInt(int64(x), 10), true
	case int32:
		return strconv.FormatInt(int64(x), 10), true
	case int64:
		return strconv.FormatInt(x, 10), true
	default:
		return "", false
	}
}

func normalizeUnsignedMapKey(key any) (string, bool) {
	switch x := key.(type) {
	case uint:
		return strconv.FormatUint(uint64(x), 10), true
	case uint8:
		return strconv.FormatUint(uint64(x), 10), true
	case uint16:
		return strconv.FormatUint(uint64(x), 10), true
	case uint32:
		return strconv.FormatUint(uint64(x), 10), true
	case uint64:
		return strconv.FormatUint(x, 10), true
	default:
		return "", false
	}
}

func normalizeFloatMapKey(key any) (string, bool) {
	switch x := key.(type) {
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64), true
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64), true
	default:
		return "", false
	}
}

func boolToLuaIntString(value bool) string {
	if value {
		return "1"
	}

	return "0"
}

func goToLuaValue(state *lua.LState, value any) lua.LValue {
	if scalar, ok := goToLuaScalar(value); ok {
		return scalar
	}

	switch x := value.(type) {
	case []any:
		return goSliceToLuaValue(state, x)
	case map[string]any:
		return goMapToLuaValue(state, x)
	default:
		raw, _ := json.Marshal(x)
		return lua.LString(string(raw))
	}
}

func goToLuaScalar(value any) (lua.LValue, bool) {
	switch x := value.(type) {
	case nil:
		return lua.LNil, true
	case bool:
		if x {
			return lua.LTrue, true
		}
		return lua.LFalse, true
	case string:
		return lua.LString(x), true
	case []byte:
		return lua.LString(string(x)), true
	}

	if numeric, ok := goToLuaSignedNumber(value); ok {
		return numeric, true
	}
	if numeric, ok := goToLuaUnsignedNumber(value); ok {
		return numeric, true
	}
	if numeric, ok := goToLuaFloatNumber(value); ok {
		return numeric, true
	}

	return lua.LNil, false
}

func goToLuaSignedNumber(value any) (lua.LValue, bool) {
	switch x := value.(type) {
	case int:
		return lua.LNumber(x), true
	case int8:
		return lua.LNumber(x), true
	case int16:
		return lua.LNumber(x), true
	case int32:
		return lua.LNumber(x), true
	case int64:
		return lua.LNumber(x), true
	default:
		return lua.LNil, false
	}
}

func goToLuaUnsignedNumber(value any) (lua.LValue, bool) {
	switch x := value.(type) {
	case uint:
		return lua.LNumber(x), true
	case uint8:
		return lua.LNumber(x), true
	case uint16:
		return lua.LNumber(x), true
	case uint32:
		return lua.LNumber(x), true
	case uint64:
		return lua.LNumber(x), true
	default:
		return lua.LNil, false
	}
}

func goToLuaFloatNumber(value any) (lua.LValue, bool) {
	switch x := value.(type) {
	case float32:
		return lua.LNumber(x), true
	case float64:
		return lua.LNumber(x), true
	default:
		return lua.LNil, false
	}
}

func goSliceToLuaValue(state *lua.LState, values []any) lua.LValue {
	tbl := state.NewTable()
	for i, item := range values {
		v := goToLuaValue(state, item)
		if v == lua.LNil {
			continue // nil entries are not stored in Lua tables (matching Redis cmsgpack)
		}
		tbl.RawSetInt(i+luaTypeArrayBase, v)
	}
	return tbl
}

func goMapToLuaValue(state *lua.LState, value map[string]any) lua.LValue {
	tbl := state.NewTable()
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		v := goToLuaValue(state, value[key])
		if v == lua.LNil {
			continue
		}
		tbl.RawSetString(key, v)
	}
	return tbl
}

func luaToGoValue(value lua.LValue) (any, error) {
	switch v := value.(type) {
	case lua.LBool:
		return bool(v), nil
	case lua.LNumber:
		n := float64(v)
		if float64(int64(n)) == n {
			return int64(n), nil
		}
		return n, nil
	case lua.LString:
		return string(v), nil
	case *lua.LTable:
		return luaTableToGo(v)
	case *lua.LNilType:
		return nil, nil
	default:
		return nil, errors.WithStack(errors.Newf("unsupported lua value %s", value.Type().String()))
	}
}

func luaTableToGo(tbl *lua.LTable) (any, error) {
	if isLuaArray(tbl) {
		out := make([]any, 0, tbl.Len())
		for i := luaTypeArrayBase; i <= tbl.Len(); i++ {
			value, err := luaToGoValue(tbl.RawGetInt(i))
			if err != nil {
				return nil, err
			}
			out = append(out, value)
		}
		return out, nil
	}

	out := map[string]any{}
	var convErr error
	tbl.ForEach(func(key lua.LValue, value lua.LValue) {
		if convErr != nil {
			return
		}
		goValue, err := luaToGoValue(value)
		if err != nil {
			convErr = err
			return
		}
		out[luaValueToCommandArg(key)] = goValue
	})
	return out, convErr
}

func isLuaArray(tbl *lua.LTable) bool {
	expected := luaTypeArrayBase
	isArray := true
	tbl.ForEach(func(key lua.LValue, _ lua.LValue) {
		if !isArray {
			return
		}
		number, ok := key.(lua.LNumber)
		if !ok || int(number) != expected {
			isArray = false
			return
		}
		expected++
	})
	return isArray
}

func luaValueToCommandArg(value lua.LValue) string {
	switch v := value.(type) {
	case lua.LString:
		return string(v)
	case lua.LNumber:
		n := float64(v)
		if float64(int64(n)) == n {
			return strconv.FormatInt(int64(n), 10)
		}
		return strconv.FormatFloat(n, 'f', -1, 64)
	case lua.LBool:
		if bool(v) {
			return "1"
		}
		return "0"
	default:
		return value.String()
	}
}

func luaValueToReply(value lua.LValue) (luaReply, error) {
	switch v := value.(type) {
	case lua.LString:
		return luaStringReply(string(v)), nil
	case lua.LNumber:
		return luaIntReply(int64(v)), nil
	case lua.LBool:
		return luaBoolReply(bool(v)), nil
	case *lua.LTable:
		return luaTableToReply(v)
	default:
		return luaNilReply(), nil
	}
}

func luaTableToReply(tbl *lua.LTable) (luaReply, error) {
	if reply, ok := luaTableSpecialReply(tbl); ok {
		return reply, nil
	}
	if isLuaArray(tbl) {
		return luaArrayTableToReply(tbl)
	}
	return luaMapTableToReply(tbl)
}

func luaTableSpecialReply(tbl *lua.LTable) (luaReply, bool) {
	if errValue := tbl.RawGetString(luaTypeErrKey); errValue != lua.LNil {
		return luaErrorReply(luaValueToCommandArg(errValue)), true
	}
	if okValue := tbl.RawGetString(luaTypeOKKey); okValue != lua.LNil {
		return luaStatusReply(luaValueToCommandArg(okValue)), true
	}
	return luaReply{}, false
}

func luaArrayTableToReply(tbl *lua.LTable) (luaReply, error) {
	values := make([]luaReply, 0, tbl.Len())
	for i := luaTypeArrayBase; i <= tbl.Len(); i++ {
		reply, err := luaValueToReply(tbl.RawGetInt(i))
		if err != nil {
			return luaReply{}, err
		}
		values = append(values, reply)
	}
	return luaArrayReply(values...), nil
}

func luaMapTableToReply(tbl *lua.LTable) (luaReply, error) {
	keys := make([]string, 0)
	values := map[string]lua.LValue{}
	tbl.ForEach(func(key lua.LValue, value lua.LValue) {
		asString := luaValueToCommandArg(key)
		keys = append(keys, asString)
		values[asString] = value
	})
	sort.Strings(keys)

	flattened := make([]luaReply, 0, len(keys)*redisPairWidth)
	for _, key := range keys {
		reply, err := luaValueToReply(values[key])
		if err != nil {
			return luaReply{}, err
		}
		flattened = append(flattened, luaStringReply(key), reply)
	}
	return luaArrayReply(flattened...), nil
}

type luaReplyWriter func(redcon.Conn, luaReply)

func writeLuaReply(conn redcon.Conn, reply luaReply) {
	if write := luaReplyWriterFor(reply.kind); write != nil {
		write(conn, reply)
		return
	}

	conn.WriteNull()
}

func luaReplyWriterFor(kind luaReplyKind) luaReplyWriter {
	switch kind {
	case luaReplyNil:
		return writeLuaNilReply
	case luaReplyInt:
		return writeLuaIntReply
	case luaReplyString:
		return writeLuaStringReply
	case luaReplyArray:
		return writeLuaArrayReply
	case luaReplyStatus:
		return writeLuaStatusReply
	case luaReplyError:
		return writeLuaErrorReply
	case luaReplyBool:
		return writeLuaBoolReply
	default:
		return nil
	}
}

func writeLuaNilReply(conn redcon.Conn, _ luaReply) {
	conn.WriteNull()
}

func writeLuaIntReply(conn redcon.Conn, reply luaReply) {
	conn.WriteInt64(reply.integer)
}

func writeLuaStringReply(conn redcon.Conn, reply luaReply) {
	conn.WriteBulkString(reply.text)
}

func writeLuaStatusReply(conn redcon.Conn, reply luaReply) {
	conn.WriteString(reply.text)
}

func writeLuaErrorReply(conn redcon.Conn, reply luaReply) {
	conn.WriteError(reply.text)
}

func writeLuaBoolReply(conn redcon.Conn, reply luaReply) {
	if reply.boolean {
		conn.WriteInt(1)
		return
	}

	conn.WriteNull()
}

func writeLuaArrayReply(conn redcon.Conn, reply luaReply) {
	conn.WriteArray(len(reply.array))
	for _, item := range reply.array {
		writeLuaReply(conn, item)
	}
}

func (r *RedisServer) execLuaCompat(conn redcon.Conn, command string, args [][]byte) {
	stringArgs := make([]string, len(args))
	for i, arg := range args {
		stringArgs[i] = string(arg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	var reply luaReply
	err := r.retryRedisWrite(ctx, func() error {
		scriptCtx := newLuaScriptContext(r)
		defer scriptCtx.Close()
		nextReply, err := scriptCtx.exec(command, stringArgs)
		if err != nil {
			return err
		}
		if err := scriptCtx.commit(); err != nil {
			return err
		}
		reply = nextReply
		return nil
	})
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	writeLuaReply(conn, reply)
}

func (r *RedisServer) rename(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdRename, cmd.Args[1:])
}

func (r *RedisServer) llen(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdLLen, cmd.Args[1:])
}

func (r *RedisServer) lrem(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdLRem, cmd.Args[1:])
}

func (r *RedisServer) lpop(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execListPop(conn, cmd, true)
}

func (r *RedisServer) rpop(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execListPop(conn, cmd, false)
}

// execListPop executes LPOP (left=true) or RPOP (left=false) using the Claim
// mechanism for O(1) conflict-free pops. An optional count argument is supported:
// LPOP key [count] / RPOP key [count].
func (r *RedisServer) execListPop(conn redcon.Conn, cmd redcon.Command, left bool) {
	count := 1
	withCount := len(cmd.Args) > 2 //nolint:mnd // args: [CMD, key, count]
	if withCount {
		n, err := strconv.Atoi(string(cmd.Args[2]))
		if err != nil || n < 0 {
			conn.WriteError("ERR value is not an integer or out of range")
			return
		}
		count = n
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	values, err := r.listPopClaim(ctx, cmd.Args[1], count, left)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	if withCount {
		// Count variant: always returns an array (or nil array if key not found).
		if values == nil {
			conn.WriteNull()
			return
		}
		conn.WriteArray(len(values))
		for _, v := range values {
			conn.WriteBulkString(v)
		}
		return
	}
	// Non-count variant: return a single bulk string (or nil).
	if len(values) == 0 {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(values[0])
}

func (r *RedisServer) rpoplpush(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdRPopLPush, cmd.Args[1:])
}

func (r *RedisServer) lpos(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdLPos, cmd.Args[1:])
}

func (r *RedisServer) lset(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdLSet, cmd.Args[1:])
}

// collectionCardinal handles SCARD/ZCARD: checks type, uses the delta-aggregated
// metadata for wide-column collections (O(1)), and falls back to the Lua
// compatibility path for legacy blob-encoded collections.
func (r *RedisServer) collectionCardinal(
	conn redcon.Conn,
	cmd redcon.Command,
	expectedType redisValueType,
	resolveMeta func(context.Context, []byte, uint64) (int64, bool, error),
	legacyCmd string,
) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != expectedType {
		conn.WriteError(wrongTypeMessage)
		return
	}
	count, exists, err := resolveMeta(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if exists {
		conn.WriteInt64(count)
		return
	}
	// Legacy blob fallback.
	r.execLuaCompat(conn, legacyCmd, cmd.Args[1:])
}

func (r *RedisServer) scard(conn redcon.Conn, cmd redcon.Command) {
	r.collectionCardinal(conn, cmd, redisTypeSet, r.resolveSetMeta, cmdSCard)
}

func (r *RedisServer) zcard(conn redcon.Conn, cmd redcon.Command) {
	r.collectionCardinal(conn, cmd, redisTypeZSet, r.resolveZSetMeta, cmdZCard)
}

func (r *RedisServer) zcount(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdZCount, cmd.Args[1:])
}

func (r *RedisServer) zrangebyscore(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdZRangeByScore, cmd.Args[1:])
}

func (r *RedisServer) zrevrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdZRevRange, cmd.Args[1:])
}

func (r *RedisServer) zrevrangebyscore(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdZRevRangeByScore, cmd.Args[1:])
}

func (r *RedisServer) zscore(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdZScore, cmd.Args[1:])
}

func (r *RedisServer) zpopmin(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdZPopMin, cmd.Args[1:])
}

func (r *RedisServer) zremrangebyscore(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.execLuaCompat(conn, cmdZRemRangeByScore, cmd.Args[1:])
}
