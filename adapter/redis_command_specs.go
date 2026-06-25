package adapter

import (
	"fmt"

	"github.com/tidwall/redcon"
)

// redisCommandSpec is the canonical row for a single Redis command. The
// route map (handler dispatch), argsLen (arity validation), and
// redisCommandTable (COMMAND INFO/COUNT/LIST/DOCS metadata) are all
// derived from redisCommandSpecs below — adding a new command requires
// a single row here plus a handler entry in routeHandlers, and the
// three views can never drift the way HELLO did when it was added to
// r.route + argsLen but missed redisCommandTable. buildRouteMap panics
// at server construction if any spec is missing a handler or vice
// versa, so the surviving 2-way sync is enforced at startup.
//
// We keep handler bindings out of the spec slice itself because Go's
// package-level init dependency analysis would otherwise see
// redisCommandSpecs → method body → argsLen → redisCommandSpecs and
// flag the whole graph as a cycle.
type redisCommandSpec struct {
	// Constant is the UPPERCASE wire name (matches the cmd* identifier
	// in redis.go) used as the route / argsLen / redisCommandTable key.
	Constant string
	// Name is the lowercase form Redis emits in COMMAND INFO replies.
	Name     string
	Arity    int
	Flags    []string
	FirstKey int
	LastKey  int
	Step     int
}

// redisCommandSpecs is the single source of truth for every routed
// Redis command's metadata. Rows are alphabetised by Constant so diffs
// stay small when adding a command.
//
//nolint:mnd // magic numbers here are literal Redis metadata (arity, key positions)
var redisCommandSpecs = []redisCommandSpec{
	{Constant: cmdBZPopMin, Name: "bzpopmin", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: -2, Step: 1},
	{Constant: cmdClient, Name: "client", Arity: -2, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdCommand, Name: "command", Arity: -1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdDBSize, Name: "dbsize", Arity: 1, Flags: []string{redisCmdFlagReadonly}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdDel, Name: "del", Arity: -2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: -1, Step: 1},
	{Constant: cmdDiscard, Name: "discard", Arity: 1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdEval, Name: "eval", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdEvalSHA, Name: "evalsha", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdExec, Name: "exec", Arity: 1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdExists, Name: "exists", Arity: -2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: -1, Step: 1},
	{Constant: cmdExpire, Name: "expire", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdFlushAll, Name: "flushall", Arity: 1, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdFlushDB, Name: "flushdb", Arity: 1, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	// FLUSHLEGACY is an elastickv-internal alias; mirror FLUSHDB metadata.
	{Constant: cmdFlushLegacy, Name: "flushlegacy", Arity: 1, Flags: []string{redisCmdFlagWrite}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdGet, Name: "get", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdGetDel, Name: "getdel", Arity: 2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHDel, Name: "hdel", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHExists, Name: "hexists", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHGet, Name: "hget", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHGetAll, Name: "hgetall", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHIncrBy, Name: "hincrby", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHLen, Name: "hlen", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHMGet, Name: "hmget", Arity: -3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHMSet, Name: "hmset", Arity: -4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdHSet, Name: "hset", Arity: -4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	// HELLO is a connection-establishment command (RESP2 negotiation,
	// AUTH probe, SETNAME). Arity is -1 because the protover and option
	// list are all optional. No key arguments.
	{Constant: cmdHello, Name: "hello", Arity: -1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdInfo, Name: "info", Arity: -1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdIncr, Name: "incr", Arity: 2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdKeys, Name: "keys", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdLIndex, Name: "lindex", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdLLen, Name: "llen", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdLPop, Name: "lpop", Arity: 2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdLPos, Name: "lpos", Arity: -3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdLPush, Name: "lpush", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdLRange, Name: "lrange", Arity: 4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdLRem, Name: "lrem", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdLSet, Name: "lset", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdLTrim, Name: "ltrim", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdMulti, Name: "multi", Arity: 1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdPExpire, Name: "pexpire", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdPFAdd, Name: "pfadd", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdPFCount, Name: "pfcount", Arity: -2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: -1, Step: 1},
	{Constant: cmdPing, Name: "ping", Arity: -1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdPTTL, Name: "pttl", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdPublish, Name: "publish", Arity: 3, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdPubSub, Name: "pubsub", Arity: -2, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdQuit, Name: "quit", Arity: 1, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdRename, Name: "rename", Arity: 3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 2, Step: 1},
	{Constant: cmdRPop, Name: "rpop", Arity: 2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdRPopLPush, Name: "rpoplpush", Arity: 3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 2, Step: 1},
	{Constant: cmdRPush, Name: "rpush", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdSAdd, Name: "sadd", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdScan, Name: "scan", Arity: -2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdSCard, Name: "scard", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdSelect, Name: "select", Arity: 2, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdSet, Name: "set", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdSetEx, Name: "setex", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdSetNX, Name: "setnx", Arity: 3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdSIsMember, Name: "sismember", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdSMembers, Name: "smembers", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdSRem, Name: "srem", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdSubscribe, Name: "subscribe", Arity: -2, Flags: []string{redisCmdFlagAdmin}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdTTL, Name: "ttl", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdType, Name: "type", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdXAdd, Name: "xadd", Arity: -5, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdXLen, Name: "xlen", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdXRange, Name: "xrange", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdXRead, Name: "xread", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 0, LastKey: 0, Step: 0},
	{Constant: cmdXRevRange, Name: "xrevrange", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdXTrim, Name: "xtrim", Arity: -4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZAdd, Name: "zadd", Arity: -4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZCard, Name: "zcard", Arity: 2, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZCount, Name: "zcount", Arity: 4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZIncrBy, Name: "zincrby", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZPopMin, Name: "zpopmin", Arity: -2, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZRange, Name: "zrange", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZRangeByScore, Name: "zrangebyscore", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZRem, Name: "zrem", Arity: -3, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZRemRangeByRank, Name: "zremrangebyrank", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZRemRangeByScore, Name: "zremrangebyscore", Arity: 4, Flags: []string{redisCmdFlagWrite}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZRevRange, Name: "zrevrange", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZRevRangeByScore, Name: "zrevrangebyscore", Arity: -4, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
	{Constant: cmdZScore, Name: "zscore", Arity: 3, Flags: []string{redisCmdFlagReadonly}, FirstKey: 1, LastKey: 1, Step: 1},
}

// argsLen is derived once at init from redisCommandSpecs so callers
// (RedisServer.dispatchCommand arity check) keep their map shape.
//
//nolint:gochecknoglobals // package-level derived from redisCommandSpecs by design
var argsLen = func() map[string]int {
	m := make(map[string]int, len(redisCommandSpecs))
	for _, s := range redisCommandSpecs {
		m[s.Constant] = s.Arity
	}
	return m
}()

// redisCommandTable is derived once at init from redisCommandSpecs.
// COMMAND INFO/COUNT/LIST/DOCS/GETKEYS read from this map; deriving it
// from the spec list means a routed-but-untabled drift like the HELLO
// regression cannot recur.
//
//nolint:gochecknoglobals // package-level derived from redisCommandSpecs by design
var redisCommandTable = func() map[string]redisCommandMeta {
	m := make(map[string]redisCommandMeta, len(redisCommandSpecs))
	for _, s := range redisCommandSpecs {
		m[s.Constant] = redisCommandMeta{
			Name:     s.Name,
			Arity:    s.Arity,
			Flags:    s.Flags,
			FirstKey: s.FirstKey,
			LastKey:  s.LastKey,
			Step:     s.Step,
		}
	}
	return m
}()

// buildRouteMap returns the dispatch table for r. The handler binding
// is pulled from a private map built on each call so handler references
// (which transitively read argsLen via RedisServer.dispatchCommand)
// don't pull redisCommandSpecs into a package-init cycle. buildRouteMap
// panics if any spec lacks a handler or vice-versa, surfacing drift at
// server construction rather than at the first request for a missing
// command.
func (r *RedisServer) buildRouteMap() map[string]func(redcon.Conn, redcon.Command) {
	handlers := map[string]func(redcon.Conn, redcon.Command){
		cmdBZPopMin:         r.bzpopmin,
		cmdClient:           r.client,
		cmdCommand:          r.command,
		cmdDBSize:           r.dbsize,
		cmdDel:              r.del,
		cmdDiscard:          r.discard,
		cmdEval:             r.eval,
		cmdEvalSHA:          r.evalsha,
		cmdExec:             r.exec,
		cmdExists:           r.exists,
		cmdExpire:           r.expire,
		cmdFlushAll:         r.flushall,
		cmdFlushDB:          r.flushdb,
		cmdFlushLegacy:      r.flushlegacy,
		cmdGet:              r.get,
		cmdGetDel:           r.getdel,
		cmdHDel:             r.hdel,
		cmdHExists:          r.hexists,
		cmdHGet:             r.hget,
		cmdHGetAll:          r.hgetall,
		cmdHIncrBy:          r.hincrby,
		cmdHLen:             r.hlen,
		cmdHMGet:            r.hmget,
		cmdHMSet:            r.hmset,
		cmdHSet:             r.hset,
		cmdHello:            r.hello,
		cmdInfo:             r.info,
		cmdIncr:             r.incr,
		cmdKeys:             r.keys,
		cmdLIndex:           r.lindex,
		cmdLLen:             r.llen,
		cmdLPop:             r.lpop,
		cmdLPos:             r.lpos,
		cmdLPush:            r.lpush,
		cmdLRange:           r.lrange,
		cmdLRem:             r.lrem,
		cmdLSet:             r.lset,
		cmdLTrim:            r.ltrim,
		cmdMulti:            r.multi,
		cmdPExpire:          r.pexpire,
		cmdPFAdd:            r.pfadd,
		cmdPFCount:          r.pfcount,
		cmdPing:             r.ping,
		cmdPTTL:             r.pttl,
		cmdPublish:          r.publish,
		cmdPubSub:           r.pubsubCmd,
		cmdQuit:             r.quit,
		cmdRename:           r.rename,
		cmdRPop:             r.rpop,
		cmdRPopLPush:        r.rpoplpush,
		cmdRPush:            r.rpush,
		cmdSAdd:             r.sadd,
		cmdScan:             r.scan,
		cmdSCard:            r.scard,
		cmdSelect:           r.selectDB,
		cmdSet:              r.set,
		cmdSetEx:            r.setex,
		cmdSetNX:            r.setnx,
		cmdSIsMember:        r.sismember,
		cmdSMembers:         r.smembers,
		cmdSRem:             r.srem,
		cmdSubscribe:        r.subscribe,
		cmdTTL:              r.ttl,
		cmdType:             r.typeCmd,
		cmdXAdd:             r.xadd,
		cmdXLen:             r.xlen,
		cmdXRange:           r.xrange,
		cmdXRead:            r.xread,
		cmdXRevRange:        r.xrevrange,
		cmdXTrim:            r.xtrim,
		cmdZAdd:             r.zadd,
		cmdZCard:            r.zcard,
		cmdZCount:           r.zcount,
		cmdZIncrBy:          r.zincrby,
		cmdZPopMin:          r.zpopmin,
		cmdZRange:           r.zrange,
		cmdZRangeByScore:    r.zrangebyscore,
		cmdZRem:             r.zrem,
		cmdZRemRangeByRank:  r.zremrangebyrank,
		cmdZRemRangeByScore: r.zremrangebyscore,
		cmdZRevRange:        r.zrevrange,
		cmdZRevRangeByScore: r.zrevrangebyscore,
		cmdZScore:           r.zscore,
	}
	m := make(map[string]func(redcon.Conn, redcon.Command), len(redisCommandSpecs))
	for _, s := range redisCommandSpecs {
		h, ok := handlers[s.Constant]
		if !ok {
			panic(fmt.Sprintf("redis: command %q has a spec but no handler in buildRouteMap; add a row", s.Constant))
		}
		m[s.Constant] = h
	}
	if len(handlers) != len(m) {
		// At least one handler has no matching spec — surface the
		// orphan so the operator knows which row to add.
		for name := range handlers {
			if _, ok := m[name]; !ok {
				panic(fmt.Sprintf("redis: command %q has a handler but no spec in redisCommandSpecs; add a row", name))
			}
		}
	}
	return m
}
