package adapter

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
)

func (r *RedisServer) proxyKeys(pattern []byte) ([]string, error) {
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		return nil, ErrLeaderNotFound
	}

	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return nil, errors.WithStack(errors.Newf("ERR leader redis address unknown for %s", leader))
	}

	cli := r.getOrCreateLeaderClient(leaderAddr)

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	keys, err := cli.Keys(ctx, string(pattern)).Result()
	return keys, errors.WithStack(err)
}

// proxyTransactionToLeader forwards a MULTI/EXEC transaction to the leader
// node and writes the EXEC response array back to conn.
//
//nolint:cyclop // inherent complexity of MULTI/EXEC proxy; refactoring would obscure the protocol flow
func (r *RedisServer) proxyTransactionToLeader(conn redcon.Conn, queue []redcon.Command) {
	leaderAddr, ok := r.resolveLeaderRedisAddr(conn)
	if !ok {
		return
	}
	cli := r.getOrCreateLeaderClient(leaderAddr)

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	cmds, err := r.execTxPipeline(ctx, cli, queue)
	if handleProxyTxnError(conn, err) {
		return
	}
	if handleProxyTxnCommandError(conn, cmds) {
		return
	}
	writeProxyCmdsResult(conn, cmds)
}

// resolveLeaderRedisAddr looks up the Redis address of the current Raft leader,
// writes an error reply to conn on failure and returns ("", false).
func (r *RedisServer) resolveLeaderRedisAddr(conn redcon.Conn) (string, bool) {
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		writeRedisError(conn, ErrLeaderNotFound)
		return "", false
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		conn.WriteError(fmt.Sprintf("ERR leader redis address unknown for raft address %s", leader))
		return "", false
	}
	return leaderAddr, true
}

// execTxPipeline sends queue as a single TxPipelined batch and returns the
// per-command result handles together with any pipeline-level error.
func (r *RedisServer) execTxPipeline(ctx context.Context, cli *redis.Client, queue []redcon.Command) ([]*redis.Cmd, error) {
	cmds := make([]*redis.Cmd, 0, len(queue))
	_, err := cli.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, cmd := range queue {
			args := make([]interface{}, len(cmd.Args))
			for i, a := range cmd.Args {
				args[i] = a
			}
			cmds = append(cmds, pipe.Do(ctx, args...))
		}
		return nil
	})
	return cmds, errors.WithStack(err)
}

// handleProxyTxnError writes the appropriate reply for terminal pipeline errors
// and returns true when the caller should return early without writing results.
func handleProxyTxnError(conn redcon.Conn, err error) bool {
	// Transaction aborted (WATCH conflict): Redis protocol requires a Null
	// array reply (*-1\r\n), not a null bulk string or an error.
	// redis.Nil is a per-command nil response and must NOT be treated as an
	// EXEC abort — only redis.TxFailedErr signals that.
	if errors.Is(err, redis.TxFailedErr) {
		conn.WriteArray(-1)
		return true
	}
	// Fatal transport / context error: per-command results are unreliable.
	if err != nil {
		var netErr net.Error
		if isTransientLeaderRedisError(err) ||
			errors.Is(err, context.DeadlineExceeded) ||
			errors.Is(err, context.Canceled) ||
			errors.Is(err, io.EOF) ||
			errors.Is(err, io.ErrUnexpectedEOF) ||
			errors.As(err, &netErr) {
			writeRedisError(conn, err)
			return true
		}
	}
	return false
}

// handleProxyTxnCommandError promotes transient leadership failures returned
// by the proxied EXEC target to a top-level EXEC error. go-redis can surface a
// target's EXEC-level error on the queued command handles; writing those as
// EXEC array elements makes clients treat leadership churn like command data.
func handleProxyTxnCommandError(conn redcon.Conn, cmds []*redis.Cmd) bool {
	for _, cmd := range cmds {
		err := cmd.Err()
		if err == nil || errors.Is(err, redis.Nil) {
			continue
		}
		if isTransientLeaderRedisError(err) {
			writeRedisError(conn, err)
			return true
		}
	}
	return false
}

// writeProxyCmdsResult writes an EXEC-style array reply for the given pipeline
// command handles. For any other non-nil per-command errors, each cmd carries
// its own result, which is the correct Redis EXEC semantics.
func writeProxyCmdsResult(conn redcon.Conn, cmds []*redis.Cmd) {
	conn.WriteArray(len(cmds))
	for _, cmd := range cmds {
		writeGoRedisResult(conn, cmd)
	}
}

func (r *RedisServer) proxyLRange(key []byte, startRaw, endRaw []byte) ([]string, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
	if leader == "" {
		return nil, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return nil, errors.WithStack(errors.Newf("ERR leader redis address unknown for %s", leader))
	}

	cli := r.getOrCreateLeaderClient(leaderAddr)

	start, err := parseInt(startRaw)
	if err != nil {
		return nil, err
	}
	end, err := parseInt(endRaw)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	res, err := cli.LRange(ctx, string(key), int64(start), int64(end)).Result()
	return res, errors.WithStack(err)
}

func (r *RedisServer) proxyRPush(key []byte, values [][]byte) (int64, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
	if leader == "" {
		return 0, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return 0, errors.WithStack(errors.Newf("ERR leader redis address unknown for %s", leader))
	}

	cli := r.getOrCreateLeaderClient(leaderAddr)

	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, string(v))
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	res, err := cli.RPush(ctx, string(key), args...).Result()
	return res, errors.WithStack(err)
}

func (r *RedisServer) proxyLPush(key []byte, values [][]byte) (int64, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
	if leader == "" {
		return 0, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return 0, errors.WithStack(errors.Newf("ERR leader redis address unknown for %s", leader))
	}

	cli := r.getOrCreateLeaderClient(leaderAddr)

	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, string(v))
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	res, err := cli.LPush(ctx, string(key), args...).Result()
	return res, errors.WithStack(err)
}

// getOrCreateLeaderClient returns a cached go-redis client for the given address,
// creating one if it doesn't exist.
func (r *RedisServer) getOrCreateLeaderClient(addr string) *redis.Client {
	r.leaderClientsMu.RLock()
	cli, ok := r.leaderClients[addr]
	r.leaderClientsMu.RUnlock()
	if ok {
		return cli
	}

	r.leaderClientsMu.Lock()
	defer r.leaderClientsMu.Unlock()
	// Double-check after acquiring write lock.
	if cli, ok = r.leaderClients[addr]; ok {
		return cli
	}
	cli = redis.NewClient(&redis.Options{Addr: addr})
	r.leaderClients[addr] = cli
	return cli
}

// leaderClientForKey returns a cached go-redis client connected to the leader
// for the given key.
func (r *RedisServer) leaderClientForKey(key []byte) (*redis.Client, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
	if leader == "" {
		return nil, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return nil, errors.WithStack(errors.Newf("ERR leader redis address unknown for %s", leader))
	}
	return r.getOrCreateLeaderClient(leaderAddr), nil
}

// proxyToLeader forwards a Redis command to the leader and writes the
// response to conn. Returns true if the command was proxied (caller should
// return immediately), false if this node is the leader.
func (r *RedisServer) proxyToLeader(conn redcon.Conn, cmd redcon.Command, key []byte) bool {
	if r.coordinator.IsLeaderForKey(key) {
		return false
	}
	cli, err := r.leaderClientForKey(key)
	if err != nil {
		writeRedisError(conn, err)
		return true
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	args := make([]interface{}, len(cmd.Args))
	for i, a := range cmd.Args {
		args[i] = a
	}
	writeGoRedisResult(conn, cli.Do(ctx, args...))
	return true
}

func writeGoRedisResult(conn redcon.Conn, cmd *redis.Cmd) {
	val, err := cmd.Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			conn.WriteNull()
		} else {
			writeRedisError(conn, err)
		}
		return
	}
	writeGoRedisValue(conn, val)
}

func writeGoRedisValue(conn redcon.Conn, val interface{}) {
	switch v := val.(type) {
	case string:
		conn.WriteBulkString(v)
	case []byte:
		conn.WriteBulk(v)
	case int64:
		conn.WriteInt64(v)
	case bool:
		conn.WriteInt(boolToInt(v))
	case float64:
		conn.WriteBulkString(strconv.FormatFloat(v, 'f', -1, 64))
	case []interface{}:
		writeGoRedisArray(conn, v)
	case nil:
		conn.WriteNull()
	default:
		conn.WriteBulkString(fmt.Sprint(v))
	}
}

func writeGoRedisArray(conn redcon.Conn, arr []interface{}) {
	conn.WriteArray(len(arr))
	for _, item := range arr {
		writeGoRedisValue(conn, item)
	}
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func parseInt(b []byte) (int, error) {
	i, err := strconv.Atoi(string(b))
	return i, errors.WithStack(err)
}

// tryLeaderGet proxies a GET to the current Raft leader, returning the value and
// whether the proxy succeeded.
func (r *RedisServer) tryLeaderGetAt(key []byte, ts uint64) ([]byte, error) {
	addr := r.coordinator.RaftLeaderForKey(key)
	if addr == "" {
		return nil, ErrLeaderNotFound
	}

	conn, err := r.relayConnCache.ConnFor(addr)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisRelayPublishTimeout)
	defer cancel()

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: ts})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Compatibility with older nodes that don't set RawGetResponse.exists:
	// treat any non-nil payload as found even when exists=false.
	if !resp.GetExists() && resp.GetValue() == nil {
		return nil, errors.WithStack(store.ErrKeyNotFound)
	}
	return resp.Value, nil
}

func (r *RedisServer) readValueAt(ctx context.Context, key []byte, readTS uint64) ([]byte, error) {
	ttlKey := key
	nonStringInternal := false
	if userKey := extractRedisInternalUserKey(key); userKey != nil {
		ttlKey = userKey
		// Non-string internal keys (!redis|hash|, !redis|set|, …) can never
		// carry an embedded-TTL payload, so we can skip the !redis|str| probe
		// that ttlAt would otherwise make.
		nonStringInternal = !bytes.HasPrefix(key, []byte(redisStrPrefix))
	}
	expired, err := r.hasExpired(ctx, ttlKey, readTS, nonStringInternal)
	if err != nil {
		return nil, err
	}
	if expired {
		return nil, errors.WithStack(store.ErrKeyNotFound)
	}

	if r.coordinator.IsLeaderForKey(key) {
		// PR #749 follow-up: caller-supplied ctx (with
		// redisDispatchTimeout from the dispatch handler) replaces
		// r.handlerContext() so VerifyLeaderForKey honours the
		// per-command deadline. Same shape as keys() / FLUSHDB.
		if err := r.coordinator.VerifyLeaderForKey(ctx, key); err != nil {
			return nil, errors.WithStack(err)
		}
		v, err := r.store.GetAt(ctx, key, readTS)
		return v, errors.WithStack(err)
	}
	return r.tryLeaderGetAt(key, readTS)
}
