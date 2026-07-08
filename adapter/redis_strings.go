package adapter

import (
	"bytes"
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

type redisSetOptions struct {
	existsCond  bool
	missingCond bool
	returnOld   bool
	ttl         *time.Time
}

type redisSetState struct {
	rawTyp   redisValueType // TTL-unaware type, for internal-key cleanup
	typ      redisValueType // TTL-aware type, for NX/XX/GET semantics
	oldValue []byte
}

type redisSetExecution struct {
	state        redisSetState
	wroteNull    bool
	wroteOldBulk bool
}

func parseRedisSetOptions(args [][]byte, now time.Time) (redisSetOptions, error) {
	opts := redisSetOptions{}
	for i := 0; i < len(args); i++ {
		opt := strings.ToUpper(string(args[i]))
		switch opt {
		case "EX", "PX":
			ttl, nextIndex, err := parseRedisSetTTL(args, i, opt, now)
			if err != nil {
				return redisSetOptions{}, err
			}
			opts.ttl = ttl
			i = nextIndex
		case "NX":
			opts.missingCond = true
		case "XX":
			opts.existsCond = true
		case "GET":
			opts.returnOld = true
		default:
			return redisSetOptions{}, errors.New("ERR syntax error")
		}
	}
	if opts.existsCond && opts.missingCond {
		return redisSetOptions{}, errors.New("ERR syntax error")
	}
	return opts, nil
}

func parseRedisSetTTL(args [][]byte, index int, opt string, now time.Time) (*time.Time, int, error) {
	if index+1 >= len(args) {
		return nil, index, errors.New("ERR syntax error")
	}
	n, err := strconv.ParseInt(string(args[index+1]), 10, 64)
	if err != nil {
		// Match Redis behavior: invalid numeric TTL value should not expose
		// internal parsing errors, but return a stable protocol error.
		return nil, index, errors.New("ERR value is not an integer or out of range")
	}
	if n <= 0 {
		return nil, index, errors.New("ERR invalid expire time in 'set' command")
	}

	unit := time.Millisecond
	if opt == "EX" {
		unit = time.Second
	}
	if n > math.MaxInt64/int64(unit) {
		return nil, index, errors.New("ERR invalid expire time in 'set' command")
	}

	expireAt := now.Add(time.Duration(n) * unit)
	return &expireAt, index + 1, nil
}

func (o redisSetOptions) isFastPath() bool {
	return !o.returnOld && !o.existsCond && !o.missingCond
}

func (o redisSetOptions) allows(exists bool) bool {
	if o.existsCond && !exists {
		return false
	}
	if o.missingCond && exists {
		return false
	}
	return true
}

func (r *RedisServer) loadRedisSetState(ctx context.Context, key []byte, readTS uint64, returnOld bool) (redisSetState, error) {
	// Probe type ONCE (rawKeyTypeAt issues up to ~17 pebble seeks),
	// then derive both the raw and TTL-filtered views from it. The
	// previous implementation called rawKeyTypeAt + keyTypeAt, which
	// called rawKeyTypeAt again inside -- doubling every SET to ~34
	// seeks for purely redundant work.
	rawTyp, err := r.rawKeyTypeAt(ctx, key, readTS)
	if err != nil {
		return redisSetState{}, err
	}
	// typ (TTL-aware) drives NX/XX/GET Redis semantics: expired keys are "gone".
	typ, err := r.applyTTLFilter(ctx, key, readTS, rawTyp)
	if err != nil {
		return redisSetState{}, err
	}

	state := redisSetState{rawTyp: rawTyp, typ: typ}
	if !returnOld || typ != redisTypeString {
		return state, nil
	}

	oldValue, _, err := r.readRedisStringAt(key, readTS)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return redisSetState{}, err
	}
	state.oldValue = oldValue
	return state, nil
}

func (r *RedisServer) replaceWithStringTxn(ctx context.Context, key, value []byte, ttl *time.Time, typ redisValueType, readTS uint64) error {
	var elems []*kv.Elem[kv.OP]
	if isNonStringCollectionType(typ) {
		delElems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		elems = append(elems, delElems...)
	}
	// Embed TTL in the string value; write !redis|ttl| as a secondary scan index.
	encoded := encodeRedisStr(bytes.Clone(value), ttl)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisStrKey(key), Value: encoded})
	if ttl != nil {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(*ttl)})
	} else {
		// Clear any prior scan index so a persistent string is not later expired.
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(key)})
	}
	return r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) executeSet(ctx context.Context, key, value []byte, opts redisSetOptions) (redisSetExecution, error) {
	var result redisSetExecution
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		state, err := r.loadRedisSetState(ctx, key, readTS, opts.returnOld)
		if err != nil {
			return err
		}

		exists := state.typ != redisTypeNone
		if !opts.allows(exists) {
			result = redisSetExecution{wroteNull: true}
			return nil
		}
		if opts.returnOld && exists && state.typ != redisTypeString {
			return wrongTypeError()
		}
		// Use rawTyp for cleanup so expired-but-lingering internal keys are deleted.
		if err := r.replaceWithStringTxn(ctx, key, value, opts.ttl, state.rawTyp, readTS); err != nil {
			return err
		}
		result = redisSetExecution{state: state, wroteOldBulk: opts.returnOld}
		return nil
	})
	return result, err
}

// trySetFastPath attempts the fast-path for SET (no NX/XX/GET flags) when the
// key is a string or absent. Returns true if the fast-path handled the command.
// When the key holds a non-string type, returns false so the caller can fall
// through to executeSet which cleans up internal keys before overwriting.
func (r *RedisServer) trySetFastPath(conn redcon.Conn, ctx context.Context, key, value []byte, ttl *time.Time) bool {
	// Only use the fast path when we are the leader for this key so the local
	// type check is authoritative. On followers, stale MVCC state could miss a
	// non-string type, leaving orphaned internal keys after overwrite.
	if !r.coordinator.IsLeaderForKey(key) {
		return false
	}
	readTS := r.readTS()
	// Use rawKeyTypeAt (TTL-unaware) so that expired keys whose internal data
	// still exists are detected and routed through the full cleanup path.
	typ, err := r.rawKeyTypeAt(context.Background(), key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return true
	}
	if isNonStringCollectionType(typ) {
		return false
	}
	if err := r.saveString(ctx, key, value, ttl); err != nil {
		writeRedisError(conn, err)
		return true
	}
	conn.WriteString("OK")
	return true
}

func (r *RedisServer) set(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	if r.runStandaloneDedup(conn, cmd) {
		return
	}
	r.setLegacy(conn, cmd)
}

func (r *RedisServer) runStandaloneDedup(conn redcon.Conn, cmd redcon.Command) bool {
	if !r.onePhaseTxnDedup {
		return false
	}
	results, err := r.runTransactionWithDedup([]redcon.Command{cmd})
	if err != nil {
		writeRedisError(conn, err)
		return true
	}
	writeRedisStandaloneResult(conn, results)
	return true
}

// setLegacy is the pre-dedup standalone SET path. Extracted from set() so
// the gate-on routing through runTransactionWithDedup keeps set() under the
// cyclop budget (the gate-off branch's parse + fast-path + executeSet
// shape carries its own decision points). Behaviour is byte-identical to
// the pre-PR set() body.
func (r *RedisServer) setLegacy(conn redcon.Conn, cmd redcon.Command) {
	opts, err := parseRedisSetOptions(cmd.Args[3:], time.Now())
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	if opts.isFastPath() && r.trySetFastPath(conn, ctx, cmd.Args[1], cmd.Args[2], opts.ttl) {
		return
	}

	result, err := r.executeSet(ctx, cmd.Args[1], cmd.Args[2], opts)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if result.wroteNull {
		conn.WriteNull()
		return
	}
	if result.wroteOldBulk {
		if result.state.oldValue == nil {
			conn.WriteNull()
			return
		}
		conn.WriteBulk(result.state.oldValue)
		return
	}
	conn.WriteString("OK")
}

// writeRedisStandaloneResult translates a single-element results array from
// runTransactionWithDedup into a redcon response, mirroring the shape a
// standalone handler would write directly. Used by SET / future standalone
// commands routed through the dedup loop. Differs from writeResults in NOT
// wrapping the response in conn.WriteArray — the standalone protocol returns
// the bare element.
//
// Empty or multi-element input is degenerate for standalone callers; we
// default to nil so a misuse never leaks a malformed reply to the wire.
//
// Array-element constraint: the resultArray arm writes each element via
// WriteBulkString, which is correct for flat arrays of strings (the
// shape applySet / future SET-pattern callers produce). It does NOT
// recurse into nested arrays. A future caller whose applyXxx emits
// resultArray with non-string elements (e.g. HGETALL-like nested
// responses) must either pre-flatten its result or extend this switch
// with a recursive arm; reusing it as-is would silently mangle the
// wire reply.
func writeRedisStandaloneResult(conn redcon.Conn, results []redisResult) {
	if len(results) != 1 {
		conn.WriteNull()
		return
	}
	res := results[0]
	switch res.typ {
	case resultNil:
		conn.WriteNull()
	case resultError:
		writeRedisError(conn, res.err)
	case resultBulk:
		conn.WriteBulk(res.bulk)
	case resultString:
		conn.WriteString(res.str)
	case resultArray:
		conn.WriteArray(len(res.arr))
		for _, s := range res.arr {
			conn.WriteBulkString(s)
		}
	case resultInt:
		conn.WriteInt64(res.integer)
	default:
		conn.WriteNull()
	}
}

func (r *RedisServer) get(conn redcon.Conn, cmd redcon.Command) {
	key := cmd.Args[1]
	if r.proxyToLeader(conn, cmd, key) {
		return
	}

	// Single bounded context for the slow paths in this handler,
	// derived from the server's base context so Close() cancels any
	// in-flight handler instead of leaving it running on a detached
	// context.Background(). Only LeaseReadForKey and keyTypeAt accept
	// a context; readRedisStringAt is a local-store read that does
	// not take one. The shared deadline bounds the only branches
	// that can actually block on quorum / I/O.
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	if _, err := kv.LeaseReadForKeyThrough(r.coordinator, ctx, key); err != nil {
		writeRedisError(conn, err)
		return
	}
	readTS := r.readTS()

	// Fast path: attempt the string read directly instead of probing
	// every possible Redis encoding first. rawKeyTypeAt issues up to
	// ~17 pebble seeks (list meta + list delta + 3×wide-column probes
	// each doing 3 seeks + hash/set/zset/stream/HLL/str/bare); that
	// overhead dominated every GET on a hot cluster (see
	// docs/design/2026_04_20_implemented_lease_read.md). A live string key resolves in 1-2
	// seeks here, and we only fall back to keyTypeAt when the string
	// path returns ErrKeyNotFound (meaning either missing, expired,
	// or a non-string type is present under this user-key).
	//
	// Use the snapshot variant: LeaseReadForKeyThrough above already
	// established the ReadIndex fence, so a per-call VerifyLeaderForKey
	// (inside leaderAwareGetAt) would duplicate the quorum work.
	v, _, err := r.readRedisStringAtSnapshot(key, readTS)
	if err == nil {
		conn.WriteBulk(v)
		return
	}
	if !errors.Is(err, store.ErrKeyNotFound) {
		writeRedisError(conn, err)
		return
	}

	// Slow path: disambiguate "missing / expired" from WRONGTYPE.
	// keyTypeAt applies the TTL filter, so an expired string reports
	// as redisTypeNone here and we return nil -- matching the
	// pre-optimisation behaviour.
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		writeRedisError(conn, err)
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	// If keyTypeAt disagrees with the fast path and classifies the key
	// as a live string (e.g. a rare TTL-filter discrepancy between
	// decodePrefixedStringWith/readBareLegacyStringWith and
	// hasExpiredTTLAt), match the pre-optimisation behaviour and
	// return nil rather than WRONGTYPE.
	if typ == redisTypeString {
		conn.WriteNull()
		return
	}
	conn.WriteError(wrongTypeMessage)
}

// leaderEmbeddedTTLExpired looks at !redis|str|<key> on the leader and, if the
// payload is in new format, returns the embedded-TTL expiry verdict. The bool
// indicates whether the caller should use this verdict (true) or fall through
// to the legacy !redis|ttl| index (false).
func (r *RedisServer) leaderEmbeddedTTLExpired(key []byte) (bool, bool) {
	raw, err := r.tryLeaderGetAt(redisStrKey(key), 0)
	if err != nil || !isNewRedisStrFormat(raw) {
		return false, false
	}
	_, expireAt, decErr := decodeRedisStr(raw)
	if decErr != nil {
		// Malformed new-format payload: treat as expired rather than silently alive.
		return true, true
	}
	if expireAt == nil {
		return false, true
	}
	return !expireAt.After(time.Now()), true
}

// isLeaderKeyExpired checks whether the key has an expired TTL on the leader.
func (r *RedisServer) isLeaderKeyExpired(key []byte) bool {
	// For string keys with new encoding: check embedded TTL.
	if expired, ok := r.leaderEmbeddedTTLExpired(key); ok {
		return expired
	}
	raw, err := r.tryLeaderGetAt(redisTTLKey(key), 0)
	if err != nil {
		return false
	}
	ttl, err := decodeRedisTTL(raw)
	if err != nil {
		return false
	}
	return !ttl.After(time.Now())
}

// tryLeaderNonStringExists checks whether the key exists as a non-string type
// (hash, set, zset, stream, HLL, or list) on the leader. Returns false if the
// key has an expired TTL.
func (r *RedisServer) tryLeaderNonStringExists(key []byte) bool {
	// Check TTL first: if expired, the key is logically gone.
	if raw, err := r.tryLeaderGetAt(redisTTLKey(key), 0); err == nil {
		if ttl, decErr := decodeRedisTTL(raw); decErr == nil && !ttl.After(time.Now()) {
			return false
		}
	}
	for _, internalKey := range [][]byte{
		redisHashKey(key),
		redisSetKey(key),
		redisHLLKey(key),
		redisZSetKey(key),
		redisStreamKey(key),
	} {
		if _, err := r.tryLeaderGetAt(internalKey, 0); err == nil {
			return true
		}
	}
	if _, err := r.tryLeaderGetAt(listMetaKey(key), 0); err == nil {
		return true
	}
	return false
}

// tryLeaderLogicalExists checks whether the key exists as any type on the leader.
func (r *RedisServer) tryLeaderLogicalExists(key []byte) bool {
	// Prefer asking the leader's Redis command path directly: it evaluates
	// existence with ttlAt() semantics (including the in-memory TTL buffer).
	// If this path is unavailable we fall back to raw-KV probing, which is
	// best-effort and may lag unflushed buffer-only TTL updates.
	if cli, err := r.leaderClientForKey(key); err == nil {
		ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
		defer cancel()
		if count, existsErr := cli.Exists(ctx, string(key)).Result(); existsErr == nil {
			return count > 0
		}
	}

	// Fallback to raw KV probing if Redis command proxying is unavailable.
	if r.isLeaderKeyExpired(key) {
		return false
	}
	// String type (raw user key).
	if _, err := r.tryLeaderGetAt(key, 0); err == nil {
		return true
	}
	return r.tryLeaderNonStringExists(key)
}

func (r *RedisServer) del(conn redcon.Conn, cmd redcon.Command) {
	// DEL discovers internal keys via local MVCC state. On followers this state
	// may lag, producing incomplete deletes. Check per-key leadership and proxy
	// non-local keys to the correct leader for accurate internal-key discovery.
	localKeys := make([][]byte, 0, len(cmd.Args)-1)
	proxyKeys := make([][]byte, 0)
	for _, key := range cmd.Args[1:] {
		if r.coordinator.IsLeaderForKey(key) {
			localKeys = append(localKeys, key)
		} else {
			proxyKeys = append(proxyKeys, key)
		}
	}

	var removed int64

	// Proxy non-local keys to the appropriate leader.
	if len(proxyKeys) > 0 {
		proxied, err := r.proxyDel(proxyKeys)
		if err != nil {
			writeRedisError(conn, err)
			return
		}
		removed += proxied
	}

	// Delete local keys directly.
	if len(localKeys) > 0 {
		localRemoved, err := r.delLocal(localKeys)
		if err != nil {
			writeRedisError(conn, err)
			return
		}
		removed += int64(localRemoved)
	}

	conn.WriteInt64(removed)
}

func (r *RedisServer) delLocal(keys [][]byte) (int, error) {
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	var removed int
	err := r.retryRedisWrite(ctx, func() error {
		elems := []*kv.Elem[kv.OP]{}
		nextRemoved := 0
		readTS := r.readTS()
		for _, key := range keys {
			keyElems, existed, err := r.deleteLogicalKeyElems(ctx, key, readTS)
			if err != nil {
				return err
			}
			if existed {
				nextRemoved++
			}
			elems = append(elems, keyElems...)
		}
		if err := r.dispatchElems(ctx, true, readTS, elems); err != nil {
			return err
		}
		removed = nextRemoved
		return nil
	})
	return removed, err
}

func (r *RedisServer) exists(conn redcon.Conn, cmd redcon.Command) {
	readTS := r.readTS()
	// Derive ctx from the server's base context so work in this handler
	// that honors context deadlines is bounded and cancels on shutdown.
	// Local Pebble reads (store.GetAt / ExistsAt / ScanAt) currently
	// ignore the context parameter, so cancellation does not interrupt
	// an in-flight local probe. The negative-result follower fallback
	// currently calls tryLeaderLogicalExists(), which manages its own
	// timeout/context rather than using this ctx.
	ctx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()
	count := 0
	for _, key := range cmd.Args[1:] {
		ok, err := r.existsAtFast(ctx, key, readTS)
		if err != nil {
			writeRedisError(conn, err)
			return
		}
		if ok {
			count++
		} else if !r.coordinator.IsLeaderForKey(key) {
			// Local MVCC may be stale on a follower; proxy to the leader.
			if r.tryLeaderLogicalExists(key) {
				count++
			}
		}
	}
	conn.WriteInt(count)
}

// existsAtFast is a string-first fast path for EXISTS-style liveness
// checks. Strings dominate real workloads, and a live string key
// resolves here in 1-2 seeks against redisStrKey (with TTL filtering
// applied inline) versus the ~17 seeks of a full logicalExistsAt
// probe. When the redisStrKey probe misses we fall back to the full
// type-probe.
//
// The probe goes directly to the local store. EXISTS tolerates stale-
// positive reads on followers by design -- the pre-optimisation flow
// (logicalExistsAt → keyTypeAt → local store.ExistsAt) never proxied
// to the leader for the probe itself; proxying is reserved for the
// negative-result fallback (tryLeaderLogicalExists in the caller).
// Routing through readRedisStringAt here would instead issue a Raft
// round-trip per key on every follower, regressing EXISTS latency on
// workloads that were previously all-local.
func (r *RedisServer) existsAtFast(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	raw, err := r.store.GetAt(ctx, redisStrKey(key), readTS)
	if err == nil {
		alive, decErr := r.stringPayloadIsLive(ctx, key, raw, readTS)
		if decErr != nil {
			return false, errors.WithStack(decErr)
		}
		if alive {
			return true, nil
		}
		// Expired: fall through so other encodings still get their
		// chance. Undecodable payloads are already propagated as an
		// error by stringPayloadIsLive above -- they're a corruption
		// signal, not a "try something else" case.
	} else if !errors.Is(err, store.ErrKeyNotFound) {
		return false, errors.WithStack(err)
	}
	return r.logicalExistsAt(ctx, key, readTS)
}

// stringPayloadIsLive reports whether a redisStrKey payload is still
// TTL-alive. New-format payloads carry their expiry inline; legacy-
// format payloads need the !redis|ttl| index consulted for the TTL.
// Both paths use the LOCAL store, matching existsAtFast's no-proxy
// contract.
func (r *RedisServer) stringPayloadIsLive(ctx context.Context, key, raw []byte, readTS uint64) (bool, error) {
	if isNewRedisStrFormat(raw) {
		_, expireAt, err := decodeRedisStr(raw)
		if err != nil {
			return false, err
		}
		return expireAt == nil || expireAt.After(time.Now()), nil
	}
	ttl, err := r.legacyIndexTTLAt(ctx, key, readTS)
	if err != nil {
		return false, err
	}
	return ttl == nil || ttl.After(time.Now()), nil
}
