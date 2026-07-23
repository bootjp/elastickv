package adapter

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	cockerrors "github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

// SETEX key seconds value — equivalent to SET key value EX seconds
func (r *RedisServer) setex(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	seconds, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil || seconds <= 0 {
		conn.WriteError("ERR invalid expire time in 'setex' command")
		return
	}
	ttl := time.Now().Add(time.Duration(seconds) * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.saveString(ctx, cmd.Args[1], cmd.Args[3], &ttl); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteString("OK")
}

// GETDEL key — get the value and delete the key atomically
func (r *RedisServer) getdel(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var v []byte
	err := r.retryRedisWrite(ctx, func() error {
		readTS, err := r.beginTxnStartTS(ctx, "redis getdel: begin read timestamp")
		if err != nil {
			return cockerrors.WithStack(err)
		}
		raw, exists, err := r.getdelValueAt(ctx, key, readTS)
		if err != nil {
			return err
		}
		if !exists {
			v = nil
			return nil
		}
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		if err := r.dispatchElems(ctx, true, readTS, elems); err != nil {
			return err
		}
		v = raw
		return nil
	})
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if v == nil {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(v)
}

func (r *RedisServer) getdelValueAt(ctx context.Context, key []byte, readTS uint64) ([]byte, bool, error) {
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		return nil, false, err
	}
	if typ == redisTypeNone {
		return nil, false, nil
	}
	if typ != redisTypeString {
		return nil, false, wrongTypeError()
	}
	raw, _, err := r.readRedisStringAt(key, readTS)
	if err != nil {
		// Key may have expired or been deleted between type check and read.
		return nil, false, nil //nolint:nilerr // treat not-found/expired as nil value
	}
	return raw, true, nil
}

// SETNX key value — set if not exists, returns 1 on success, 0 on failure
func (r *RedisServer) setnx(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	opts := redisSetOptions{missingCond: true}
	result, err := r.executeSet(ctx, cmd.Args[1], cmd.Args[2], opts)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if result.wroteNull {
		conn.WriteInt(0)
		return
	}
	conn.WriteInt(1)
}

func (r *RedisServer) ttl(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.writeTTL(conn, cmd.Args[1], false)
}

func (r *RedisServer) pttl(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.writeTTL(conn, cmd.Args[1], true)
}

func (r *RedisServer) writeTTL(conn redcon.Conn, key []byte, milliseconds bool) {
	readTS := r.readTS()
	exists, err := r.logicalExistsAt(context.Background(), key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if !exists {
		conn.WriteInt64(-2)
		return
	}
	ttl, err := r.ttlAt(context.Background(), key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	ms := ttlMilliseconds(ttl)
	if ms == -1 {
		conn.WriteInt64(-1)
		return
	}
	if !milliseconds && ms >= 0 {
		ms /= 1000
	}
	conn.WriteInt64(ms)
}

func (r *RedisServer) expire(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.setExpire(conn, cmd, time.Second)
}

func (r *RedisServer) pexpire(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.setExpire(conn, cmd, time.Millisecond)
}

func parseExpireNXOnly(args [][]byte) (bool, error) {
	nxOnly := false
	for _, arg := range args {
		if !strings.EqualFold(string(arg), "NX") {
			return false, errors.New("ERR syntax error")
		}
		nxOnly = true
	}
	return nxOnly, nil
}

func hasActiveTTL(ttl *time.Time, now time.Time) bool {
	return ttl != nil && ttl.After(now)
}

func parseExpireTTL(raw []byte) (int64, error) {
	ttl, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse expire ttl: %w", err)
	}
	return ttl, nil
}

func (r *RedisServer) prepareExpire(ctx context.Context, key []byte, nxOnly bool) (uint64, bool, error) {
	readTS, err := r.beginTxnStartTS(ctx, "redis expire: begin read timestamp")
	if err != nil {
		return 0, false, cockerrors.WithStack(err)
	}
	exists, err := r.logicalExistsAt(ctx, key, readTS)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return readTS, false, nil
	}

	if !nxOnly {
		return readTS, true, nil
	}

	currentTTL, err := r.ttlAt(ctx, key, readTS)
	if err != nil {
		return 0, false, err
	}
	return readTS, !hasActiveTTL(currentTTL, time.Now()), nil
}

func (r *RedisServer) setExpire(conn redcon.Conn, cmd redcon.Command, unit time.Duration) {
	ttl, err := parseExpireTTL(cmd.Args[2])
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	nxOnly, err := parseExpireNXOnly(cmd.Args[3:])
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	// Pin expireAt once before the retry loop so successive attempts all write
	// the same wall-clock deadline (OCC retries must not push expiry forward).
	var expireAt time.Time
	if ttl > 0 {
		if ttl > math.MaxInt64/int64(unit) {
			conn.WriteError("ERR invalid expire time in command")
			return
		}
		expireAt = time.Now().Add(time.Duration(ttl) * unit)
	}

	var result int
	if err := r.retryRedisWrite(ctx, func() error {
		var retErr error
		result, retErr = r.doSetExpire(ctx, cmd.Args[1], ttl, expireAt, nxOnly)
		return retErr
	}); err != nil {
		writeRedisError(conn, err)
		return
	}
	conn.WriteInt(result)
}

// doSetExpire is the inner body of setExpire's retryRedisWrite loop.
// All reads (existence, type, value) use the same readTS snapshot so they form
// a consistent view. The subsequent dispatchElems calls use IsTxn=true with
// StartTS=readTS, which causes coordinator.Dispatch to reject the write with
// ErrWriteConflict if any touched key was modified after readTS. retryRedisWrite
// then re-invokes doSetExpire with a fresh readTS, providing OCC safety without
// an explicit mutex. Leadership is verified by coordinator.Dispatch itself.
func (r *RedisServer) doSetExpire(ctx context.Context, key []byte, ttl int64, expireAt time.Time, nxOnly bool) (int, error) {
	readTS, eligible, err := r.prepareExpire(ctx, key, nxOnly)
	if err != nil {
		return 0, err
	}
	if !eligible {
		return 0, nil
	}
	if ttl <= 0 {
		return r.expireDeleteKey(ctx, key, readTS)
	}
	typ, err := r.rawKeyTypeAt(ctx, key, readTS)
	if err != nil {
		return 0, err
	}
	applied, err := r.dispatchExpireForType(ctx, key, readTS, typ, expireAt)
	if err != nil || !applied {
		return 0, err
	}
	return 1, nil
}

func (r *RedisServer) dispatchExpireForType(
	ctx context.Context,
	key []byte,
	readTS uint64,
	typ redisValueType,
	expireAt time.Time,
) (bool, error) {
	if typ == redisTypeString {
		// rawKeyTypeAt also reports HLL as redisTypeString; HLL payloads live
		// under !redis|hll|<key>, so distinguish them from plain strings
		// before rewriting the TTL-bearing anchor.
		plain, err := r.isPlainRedisString(ctx, key, readTS)
		if err != nil {
			return false, err
		}
		if plain {
			return r.dispatchStringExpire(ctx, key, readTS, expireAt)
		}
		return r.dispatchHLLExpire(ctx, key, readTS, expireAt)
	}
	if isNonStringCollectionType(typ) {
		return r.dispatchCollectionExpire(ctx, key, readTS, typ, expireAt)
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(expireAt)}}
	return true, r.dispatchElems(ctx, true, readTS, elems)
}

// isPlainRedisString distinguishes a plain Redis string (stored under
// !redis|str|<key> or, for legacy data, the bare key) from a HyperLogLog
// (stored under !redis|hll|<key>), both of which rawKeyTypeAt reports as
// redisTypeString.
func (r *RedisServer) isPlainRedisString(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	exists, err := r.store.ExistsAt(ctx, redisStrKey(key), readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	if exists {
		return true, nil
	}
	// Fall back to the bare legacy layout.
	legacy, err := r.store.ExistsAt(ctx, key, readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	return legacy, nil
}

func (r *RedisServer) expireDeleteKey(ctx context.Context, key []byte, readTS uint64) (int, error) {
	elems, existed, err := r.deleteLogicalKeyElems(ctx, key, readTS)
	if err != nil {
		return 0, err
	}
	if err := r.dispatchElems(ctx, true, readTS, elems); err != nil {
		return 0, err
	}
	if existed {
		return 1, nil
	}
	return 0, nil
}

// dispatchStringExpire performs a read-modify-write on the string anchor key:
// it reads the current value at readTS, re-encodes it with the new expiry, and
// writes both the updated value and the !redis|ttl| scan index in a single Raft
// entry (IsTxn=true, StartTS=readTS). The coordinator rejects the write with
// ErrWriteConflict if any key was modified after readTS, so stale-data safety is
// guaranteed by OCC — no explicit mutex is required.
func (r *RedisServer) dispatchStringExpire(ctx context.Context, key []byte, readTS uint64, expireAt time.Time) (bool, error) {
	userValue, _, readErr := r.readRedisStringAt(key, readTS)
	if readErr != nil {
		if cockerrors.Is(readErr, store.ErrKeyNotFound) {
			// Raced with a delete/expiry between prepareExpire and this read;
			// do not resurrect the key with an empty anchor.
			return false, nil
		}
		return false, cockerrors.WithStack(readErr)
	}
	encoded := encodeRedisStr(userValue, &expireAt)
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisStrKey(key), Value: encoded},
		{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(expireAt)},
	}
	return true, r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) dispatchHLLExpire(ctx context.Context, key []byte, readTS uint64, expireAt time.Time) (bool, error) {
	raw, err := r.store.GetAt(ctx, redisHLLKey(key), readTS)
	if err != nil {
		if cockerrors.Is(err, store.ErrKeyNotFound) {
			return false, nil
		}
		return false, cockerrors.WithStack(err)
	}
	value, _, _, err := decodeRedisHLL(raw)
	if err != nil {
		return false, err
	}
	encoded, err := encodeRedisHLL(value, &expireAt)
	if err != nil {
		return false, err
	}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisHLLKey(key), Value: encoded},
		{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(expireAt)},
	}
	return true, r.dispatchElems(ctx, true, readTS, elems)
}
