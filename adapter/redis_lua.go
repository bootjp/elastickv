package adapter

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
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
		L := newRedisLuaState()
		defer L.Close()
		r.initLuaGlobals(L, scriptCtx, keys, argv)

		chunk, err := L.LoadString(script)
		if err != nil {
			return err
		}
		L.Push(chunk)
		if err := L.PCall(0, 1, nil); err != nil {
			return err
		}
		result := L.Get(-1)
		defer L.Pop(1)

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

func (r *RedisServer) initLuaGlobals(L *lua.LState, ctx *luaScriptContext, keys [][]byte, argv [][]byte) {
	L.SetGlobal("KEYS", makeLuaStringArray(L, keys))
	L.SetGlobal("ARGV", makeLuaStringArray(L, argv))
	registerRedisModule(L, ctx)
	registerCJSONModule(L)
	registerCMsgpackModule(L)

	tableModule := L.GetGlobal("table")
	if tbl, ok := tableModule.(*lua.LTable); ok {
		if unpack := tbl.RawGetString("unpack"); unpack != lua.LNil {
			L.SetGlobal("unpack", unpack)
		}
	}
}

func newRedisLuaState() *lua.LState {
	L := lua.NewState(lua.Options{SkipOpenLibs: true})
	openLuaLib(L, lua.BaseLibName, lua.OpenBase)
	openLuaLib(L, lua.TabLibName, lua.OpenTable)
	openLuaLib(L, lua.StringLibName, lua.OpenString)
	openLuaLib(L, lua.MathLibName, lua.OpenMath)

	for _, name := range []string{"dofile", "load", "loadfile", "loadstring", "module", "require"} {
		L.SetGlobal(name, lua.LNil)
	}
	return L
}

func openLuaLib(L *lua.LState, name string, fn lua.LGFunction) {
	L.Push(L.NewFunction(fn))
	L.Push(lua.LString(name))
	L.Call(1, 0)
}

func registerRedisModule(L *lua.LState, ctx *luaScriptContext) {
	module := L.NewTable()
	L.SetFuncs(module, map[string]lua.LGFunction{
		"call": func(L *lua.LState) int {
			return luaRedisCommand(L, ctx, true)
		},
		"pcall": func(L *lua.LState) int {
			return luaRedisCommand(L, ctx, false)
		},
		"sha1hex": func(L *lua.LState) int {
			L.Push(lua.LString(luaScriptSHA(L.CheckString(1))))
			return 1
		},
		"status_reply": func(L *lua.LState) int {
			reply := L.NewTable()
			reply.RawSetString(luaTypeOKKey, lua.LString(L.CheckString(1)))
			L.Push(reply)
			return 1
		},
		"error_reply": func(L *lua.LState) int {
			reply := L.NewTable()
			reply.RawSetString(luaTypeErrKey, lua.LString(L.CheckString(1)))
			L.Push(reply)
			return 1
		},
	})
	L.SetGlobal("redis", module)
}

func luaRedisCommand(L *lua.LState, ctx *luaScriptContext, raise bool) int {
	if L.GetTop() == 0 {
		if raise {
			L.RaiseError("Please specify at least one argument for this redis lib call")
			return 0
		}
		L.Push(luaErrorTable(L, "Please specify at least one argument for this redis lib call"))
		return 1
	}

	command := strings.ToUpper(luaValueToCommandArg(L.Get(1)))
	args := make([]string, 0, L.GetTop()-1)
	for i := 2; i <= L.GetTop(); i++ {
		args = append(args, luaValueToCommandArg(L.Get(i)))
	}

	reply, err := ctx.exec(command, args)
	if err != nil {
		if raise {
			L.RaiseError("%s", err.Error())
			return 0
		}
		L.Push(luaErrorTable(L, err.Error()))
		return 1
	}

	pushLuaReply(L, reply)
	return 1
}

func makeLuaStringArray(L *lua.LState, values [][]byte) *lua.LTable {
	tbl := L.NewTable()
	for i, value := range values {
		tbl.RawSetInt(i+luaTypeArrayBase, lua.LString(string(value)))
	}
	return tbl
}

func pushLuaReply(L *lua.LState, reply luaReply) {
	switch reply.kind {
	case luaReplyNil:
		L.Push(lua.LFalse)
	case luaReplyInt:
		L.Push(lua.LNumber(reply.integer))
	case luaReplyString:
		L.Push(lua.LString(reply.text))
	case luaReplyStatus:
		tbl := L.NewTable()
		tbl.RawSetString(luaTypeOKKey, lua.LString(reply.text))
		L.Push(tbl)
	case luaReplyError:
		L.Push(luaErrorTable(L, reply.text))
	case luaReplyBool:
		if reply.boolean {
			L.Push(lua.LTrue)
		} else {
			L.Push(lua.LFalse)
		}
	case luaReplyArray:
		tbl := L.NewTable()
		for i, item := range reply.array {
			pushLuaReply(L, item)
			tbl.RawSetInt(i+luaTypeArrayBase, L.Get(-1))
			L.Pop(1)
		}
		L.Push(tbl)
	default:
		L.Push(lua.LNil)
	}
}

func luaErrorTable(L *lua.LState, msg string) *lua.LTable {
	tbl := L.NewTable()
	tbl.RawSetString(luaTypeErrKey, lua.LString(msg))
	return tbl
}

func registerCJSONModule(L *lua.LState) {
	module := L.NewTable()
	L.SetFuncs(module, map[string]lua.LGFunction{
		"encode": func(L *lua.LState) int {
			goValue, err := luaToGoValue(L.Get(1))
			if err != nil {
				L.RaiseError("%s", err.Error())
				return 0
			}
			raw, err := json.Marshal(goValue)
			if err != nil {
				L.RaiseError("%s", err.Error())
				return 0
			}
			L.Push(lua.LString(string(raw)))
			return 1
		},
		"decode": func(L *lua.LState) int {
			var decoded any
			if err := json.Unmarshal([]byte(L.CheckString(1)), &decoded); err != nil {
				L.RaiseError("%s", err.Error())
				return 0
			}
			L.Push(goToLuaValue(L, decoded))
			return 1
		},
	})
	L.SetGlobal("cjson", module)
}

func registerCMsgpackModule(L *lua.LState) {
	module := L.NewTable()
	L.SetFuncs(module, map[string]lua.LGFunction{
		"unpack": func(L *lua.LState) int {
			var decoded any
			if err := msgpack.Unmarshal([]byte(L.CheckString(1)), &decoded); err != nil {
				L.RaiseError("%s", err.Error())
				return 0
			}
			L.Push(goToLuaValue(L, normalizeDecodedValue(decoded)))
			return 1
		},
	})
	L.SetGlobal("cmsgpack", module)
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
	case int:
		return strconv.Itoa(x)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "1"
		}
		return "0"
	default:
		return fmt.Sprint(x)
	}
}

func goToLuaValue(L *lua.LState, value any) lua.LValue {
	switch x := value.(type) {
	case nil:
		return lua.LFalse
	case bool:
		if x {
			return lua.LTrue
		}
		return lua.LFalse
	case string:
		return lua.LString(x)
	case []byte:
		return lua.LString(string(x))
	case int:
		return lua.LNumber(x)
	case int8:
		return lua.LNumber(x)
	case int16:
		return lua.LNumber(x)
	case int32:
		return lua.LNumber(x)
	case int64:
		return lua.LNumber(x)
	case uint:
		return lua.LNumber(x)
	case uint8:
		return lua.LNumber(x)
	case uint16:
		return lua.LNumber(x)
	case uint32:
		return lua.LNumber(x)
	case uint64:
		return lua.LNumber(x)
	case float32:
		return lua.LNumber(x)
	case float64:
		return lua.LNumber(x)
	case []any:
		tbl := L.NewTable()
		for i, item := range x {
			tbl.RawSetInt(i+luaTypeArrayBase, goToLuaValue(L, item))
		}
		return tbl
	case map[string]any:
		tbl := L.NewTable()
		keys := make([]string, 0, len(x))
		for key := range x {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			tbl.RawSetString(key, goToLuaValue(L, x[key]))
		}
		return tbl
	default:
		raw, _ := json.Marshal(x)
		return lua.LString(string(raw))
	}
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
		return nil, errors.Newf("unsupported lua value %s", value.Type().String())
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

func writeLuaValue(conn redcon.Conn, value lua.LValue) {
	switch v := value.(type) {
	case lua.LString:
		conn.WriteBulkString(string(v))
	case lua.LNumber:
		conn.WriteInt64(int64(v))
	case lua.LBool:
		if bool(v) {
			conn.WriteInt(1)
			return
		}
		conn.WriteNull()
	case *lua.LTable:
		if errValue := v.RawGetString(luaTypeErrKey); errValue != lua.LNil {
			conn.WriteError(luaValueToCommandArg(errValue))
			return
		}
		if okValue := v.RawGetString(luaTypeOKKey); okValue != lua.LNil {
			conn.WriteString(luaValueToCommandArg(okValue))
			return
		}
		writeLuaTable(conn, v)
	default:
		conn.WriteNull()
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
		if errValue := v.RawGetString(luaTypeErrKey); errValue != lua.LNil {
			return luaErrorReply(luaValueToCommandArg(errValue)), nil
		}
		if okValue := v.RawGetString(luaTypeOKKey); okValue != lua.LNil {
			return luaStatusReply(luaValueToCommandArg(okValue)), nil
		}
		if isLuaArray(v) {
			values := make([]luaReply, 0, v.Len())
			for i := luaTypeArrayBase; i <= v.Len(); i++ {
				reply, err := luaValueToReply(v.RawGetInt(i))
				if err != nil {
					return luaReply{}, err
				}
				values = append(values, reply)
			}
			return luaArrayReply(values...), nil
		}

		keys := make([]string, 0)
		values := map[string]lua.LValue{}
		v.ForEach(func(key lua.LValue, value lua.LValue) {
			asString := luaValueToCommandArg(key)
			keys = append(keys, asString)
			values[asString] = value
		})
		sort.Strings(keys)

		flattened := make([]luaReply, 0, len(keys)*2)
		for _, key := range keys {
			reply, err := luaValueToReply(values[key])
			if err != nil {
				return luaReply{}, err
			}
			flattened = append(flattened, luaStringReply(key), reply)
		}
		return luaArrayReply(flattened...), nil
	default:
		return luaNilReply(), nil
	}
}

func writeLuaReply(conn redcon.Conn, reply luaReply) {
	switch reply.kind {
	case luaReplyNil:
		conn.WriteNull()
	case luaReplyInt:
		conn.WriteInt64(reply.integer)
	case luaReplyString:
		conn.WriteBulkString(reply.text)
	case luaReplyStatus:
		conn.WriteString(reply.text)
	case luaReplyError:
		conn.WriteError(reply.text)
	case luaReplyBool:
		if reply.boolean {
			conn.WriteInt(1)
			return
		}
		conn.WriteNull()
	case luaReplyArray:
		conn.WriteArray(len(reply.array))
		for _, item := range reply.array {
			writeLuaReply(conn, item)
		}
	default:
		conn.WriteNull()
	}
}

func writeLuaTable(conn redcon.Conn, table *lua.LTable) {
	if isLuaArray(table) {
		length := table.Len()
		conn.WriteArray(length)
		for i := luaTypeArrayBase; i <= length; i++ {
			writeLuaValue(conn, table.RawGetInt(i))
		}
		return
	}

	keys := make([]string, 0)
	values := map[string]lua.LValue{}
	table.ForEach(func(key lua.LValue, value lua.LValue) {
		asString := luaValueToCommandArg(key)
		keys = append(keys, asString)
		values[asString] = value
	})
	sort.Strings(keys)

	conn.WriteArray(len(keys) * 2)
	for _, key := range keys {
		conn.WriteBulkString(key)
		writeLuaValue(conn, values[key])
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
	r.execLuaCompat(conn, cmdRename, cmd.Args[1:])
}

func (r *RedisServer) llen(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdLLen, cmd.Args[1:])
}

func (r *RedisServer) lrem(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdLRem, cmd.Args[1:])
}

func (r *RedisServer) lpop(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdLPop, cmd.Args[1:])
}

func (r *RedisServer) rpop(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdRPop, cmd.Args[1:])
}

func (r *RedisServer) rpoplpush(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdRPopLPush, cmd.Args[1:])
}

func (r *RedisServer) lpos(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdLPos, cmd.Args[1:])
}

func (r *RedisServer) lset(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdLSet, cmd.Args[1:])
}

func (r *RedisServer) scard(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdSCard, cmd.Args[1:])
}

func (r *RedisServer) zcard(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdZCard, cmd.Args[1:])
}

func (r *RedisServer) zcount(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdZCount, cmd.Args[1:])
}

func (r *RedisServer) zrangebyscore(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdZRangeByScore, cmd.Args[1:])
}

func (r *RedisServer) zrevrange(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdZRevRange, cmd.Args[1:])
}

func (r *RedisServer) zrevrangebyscore(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdZRevRangeByScore, cmd.Args[1:])
}

func (r *RedisServer) zscore(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdZScore, cmd.Args[1:])
}

func (r *RedisServer) zpopmin(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdZPopMin, cmd.Args[1:])
}

func (r *RedisServer) zremrangebyscore(conn redcon.Conn, cmd redcon.Command) {
	r.execLuaCompat(conn, cmdZRemRangeByScore, cmd.Args[1:])
}
