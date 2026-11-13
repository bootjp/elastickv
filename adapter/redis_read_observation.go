package adapter

import (
	"context"
	"strings"

	"github.com/bootjp/elastickv/kv"
)

// redisReadKeys returns user keys whose command may inspect stored state.
// Writes are included because a failed condition can return without a commit.
func redisReadKeys(name string, args [][]byte) [][]byte {
	meta, ok := redisCommandTable[name]
	if !ok || (!hasRedisCommandFlag(meta.Flags, redisCmdFlagReadonly) && !hasRedisCommandFlag(meta.Flags, redisCmdFlagWrite)) {
		return nil
	}
	if name == cmdXRead {
		req, err := parseXReadRequest(args)
		if err != nil {
			return nil
		}
		return req.keys
	}
	return redisFixedReadKeys(meta, args)
}

func redisFixedReadKeys(meta redisCommandMeta, args [][]byte) [][]byte {
	if meta.FirstKey == 0 || meta.Step <= 0 || len(args) <= meta.FirstKey {
		return nil
	}
	last := meta.LastKey
	if last < 0 {
		last += len(args)
	}
	if last < meta.FirstKey || last >= len(args) {
		return nil
	}
	keys := make([][]byte, 0, (last-meta.FirstKey)/meta.Step+1)
	for i := meta.FirstKey; i <= last; i += meta.Step {
		keys = append(keys, args[i])
	}
	return keys
}

func hasRedisCommandFlag(flags []string, flag string) bool {
	for _, candidate := range flags {
		if candidate == flag {
			return true
		}
	}
	return false
}

func (r *RedisServer) observeRedisRead(ctx context.Context, name string, args [][]byte, direct bool) {
	if r == nil || r.coordinator == nil {
		return
	}
	// Standalone GET already records through LeaseReadForKeyThrough. Lua and
	// EXEC use a different read fence, so their GETs need this observation.
	if direct && name == cmdGet {
		return
	}
	if redisReadUnattributable(name, args) {
		kv.MarkUnattributableClientReadThrough(r.coordinator, 0)
		return
	}
	keys := redisReadKeys(name, args)
	// A direct command may have been forwarded. Its first key determines
	// the routing target, so do not invoke telemetry on the ingress node.
	if r.redisReadForwarded(direct, keys) {
		return
	}
	if len(keys) > 1 && !r.redisReadKeysLocal(keys) {
		kv.MarkUnattributableClientReadThrough(r.coordinator, 0)
		return
	}
	for _, key := range keys {
		kv.ObserveReadForKeyThrough(r.coordinator, ctx, redisUserRouteKey(key))
	}
}

func (r *RedisServer) redisReadForwarded(direct bool, keys [][]byte) bool {
	return direct && len(keys) != 0 &&
		!r.coordinator.IsLeaderForKey(redisUserRouteKey(keys[0]))
}

func redisReadUnattributable(name string, args [][]byte) bool {
	if name == cmdDBSize || name == cmdKeys || name == cmdScan ||
		name == cmdFlushAll || name == cmdFlushDB || name == cmdFlushLegacy {
		return true
	}
	if name != cmdXRead {
		return false
	}
	_, err := parseXReadRequest(args)
	return err != nil
}

func (r *RedisServer) redisReadKeysLocal(keys [][]byte) bool {
	for _, key := range keys {
		if !r.coordinator.IsLeaderForKey(redisUserRouteKey(key)) {
			return false
		}
	}
	return true
}

func (r *RedisServer) observeLuaRead(ctx context.Context, name string, args []string) {
	argv := make([][]byte, len(args)+1)
	argv[0] = []byte(name)
	for i, arg := range args {
		argv[i+1] = []byte(arg)
	}
	r.observeRedisRead(ctx, strings.ToUpper(name), argv, false)
}
