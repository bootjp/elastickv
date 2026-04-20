package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	cockerrors "github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

const (
	redisPairWidth       = 2
	redisTripletWidth    = 3
	pubsubPatternArgMin  = 3
	pubsubFirstChannel   = 2
	redisBusyPollBackoff = 10 * time.Millisecond
	redisKeywordCount    = "COUNT"

	// setWideColOverhead is the number of extra elements reserved in a set
	// wide-column mutation slice beyond the per-member elements: one for the
	// metadata key and one for the legacy-blob deletion tombstone.
	setWideColOverhead = 2

	// wideColumnBulkScanThreshold is the minimum batch size at which a full
	// prefix scan is used to check field/member existence instead of one
	// ExistsAt per field. Below this threshold the per-key lookups are cheaper
	// because they avoid scanning an arbitrarily large collection.
	wideColumnBulkScanThreshold = 16
)

type xreadRequest struct {
	block    time.Duration
	count    int
	keys     [][]byte
	afterIDs []string
}

type xreadOptions struct {
	block        time.Duration
	count        int
	streamsIndex int
}

type xreadResult struct {
	key     []byte
	entries []redisStreamEntry
}

type xaddRequest struct {
	maxLen int
	id     string
	fields []string
}

type zrangeOptions struct {
	withScores bool
	reverse    bool
}

type bzpopminResult struct {
	key   []byte
	entry redisZSetEntry
}

func (r *RedisServer) info(conn redcon.Conn, _ redcon.Command) {
	role := "slave"
	if r.coordinator != nil && r.coordinator.IsLeader() {
		role = "master"
	}

	leaderRedis := r.raftLeaderRedisAddr()

	conn.WriteBulkString(strings.Join([]string{
		"# Server",
		"redis_version:7.2.0",
		"loading:0",
		"role:" + role,
		"",
		"# Replication",
		"role:" + role,
		"raft_leader_redis:" + leaderRedis,
		"",
	}, "\r\n"))
}

// raftLeaderRedisAddr returns the Redis-protocol address of the current Raft
// leader as known by this node. When this node is itself the leader the
// server's own listen address is returned. An empty string is returned when
// the leader is not yet known or when the leader's Redis address is not
// configured in the leaderRedis map.
func (r *RedisServer) raftLeaderRedisAddr() string {
	if r.coordinator == nil {
		return ""
	}
	if r.coordinator.IsLeader() {
		return r.redisAddr
	}
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		return ""
	}
	return r.leaderRedis[leader]
}

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
		conn.WriteError(err.Error())
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
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), key, readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			v = nil
			return nil
		}
		if typ != redisTypeString {
			return wrongTypeError()
		}
		raw, _, err := r.readRedisStringAt(key, readTS)
		if err != nil {
			// Key may have expired or been deleted between type check and read.
			v = nil
			return nil //nolint:nilerr // treat not-found/expired as nil value
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
		conn.WriteError(err.Error())
		return
	}
	if v == nil {
		conn.WriteNull()
		return
	}
	conn.WriteBulk(v)
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
		conn.WriteError(err.Error())
		return
	}
	if result.wroteNull {
		conn.WriteInt(0)
		return
	}
	conn.WriteInt(1)
}

func (r *RedisServer) client(conn redcon.Conn, cmd redcon.Command) {
	sub := strings.ToUpper(string(cmd.Args[1]))
	switch sub {
	case "SETINFO", "SETNAME":
		conn.WriteString("OK")
	case "GETNAME":
		conn.WriteNull()
	case "INFO":
		conn.WriteBulkString("id=1 addr=" + conn.RemoteAddr())
	default:
		conn.WriteError("ERR unsupported CLIENT subcommand '" + sub + "'")
	}
}

func (r *RedisServer) selectDB(conn redcon.Conn, cmd redcon.Command) {
	if _, err := strconv.Atoi(string(cmd.Args[1])); err != nil {
		conn.WriteError("ERR invalid DB index")
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) quit(conn redcon.Conn, _ redcon.Command) {
	conn.WriteString("OK")
	_ = conn.Close()
}

func (r *RedisServer) typeCmd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	typ, err := r.keyType(context.Background(), cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString(string(typ))
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
		conn.WriteError(err.Error())
		return
	}
	if !exists {
		conn.WriteInt64(-2)
		return
	}
	ttl, err := r.ttlAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
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

func (r *RedisServer) prepareExpire(key []byte, nxOnly bool) (uint64, bool, error) {
	readTS := r.readTS()
	exists, err := r.logicalExistsAt(context.Background(), key, readTS)
	if err != nil {
		return 0, false, err
	}
	if !exists {
		return readTS, false, nil
	}

	if !nxOnly {
		return readTS, true, nil
	}

	currentTTL, err := r.ttlAt(context.Background(), key, readTS)
	if err != nil {
		return 0, false, err
	}
	return readTS, !hasActiveTTL(currentTTL, time.Now()), nil
}

func (r *RedisServer) setExpire(conn redcon.Conn, cmd redcon.Command, unit time.Duration) {
	ttl, err := parseExpireTTL(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	nxOnly, err := parseExpireNXOnly(cmd.Args[3:])
	if err != nil {
		conn.WriteError(err.Error())
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
		conn.WriteError(err.Error())
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
	readTS, eligible, err := r.prepareExpire(key, nxOnly)
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
	if typ == redisTypeString {
		// rawKeyTypeAt also reports HLL as redisTypeString; HLL payloads live
		// under !redis|hll|<key> and don't carry an inline TTL, so fall back
		// to the legacy scan-index path for them.
		plain, err := r.isPlainRedisString(ctx, key, readTS)
		if err != nil {
			return 0, err
		}
		if plain {
			applied, err := r.dispatchStringExpire(ctx, key, readTS, expireAt)
			if err != nil || !applied {
				return 0, err
			}
			return 1, nil
		}
	}
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisTTLKey(key), Value: encodeRedisTTL(expireAt)}}
	return 1, r.dispatchElems(ctx, true, readTS, elems)
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

func parseScanArgs(args [][]byte) (int, []byte, int, error) {
	cursor, err := strconv.Atoi(string(args[1]))
	if err != nil || cursor < 0 {
		return 0, nil, 0, errors.New("ERR invalid cursor")
	}

	pattern := []byte("*")
	count := 10
	for i := redisPairWidth; i < len(args); i += redisPairWidth {
		if i+1 >= len(args) {
			return 0, nil, 0, errors.New("ERR syntax error")
		}
		switch strings.ToUpper(string(args[i])) {
		case "MATCH":
			pattern = args[i+1]
		case redisKeywordCount:
			count, err = strconv.Atoi(string(args[i+1]))
			if err != nil || count <= 0 {
				return 0, nil, 0, errors.New("ERR syntax error")
			}
		default:
			return 0, nil, 0, errors.New("ERR syntax error")
		}
	}
	return cursor, pattern, count, nil
}

func writeScanReply(conn redcon.Conn, next int, keys [][]byte) {
	conn.WriteArray(redisPairWidth)
	conn.WriteBulkString(strconv.Itoa(next))
	conn.WriteArray(len(keys))
	for _, key := range keys {
		conn.WriteBulk(key)
	}
}

func (r *RedisServer) scan(conn redcon.Conn, cmd redcon.Command) {
	cursor, pattern, count, err := parseScanArgs(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	keys, err := r.visibleKeys(pattern)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if cursor >= len(keys) {
		writeScanReply(conn, 0, nil)
		return
	}

	end := minRedisInt(cursor+count, len(keys))
	next := 0
	if end < len(keys) {
		next = end
	}

	writeScanReply(conn, next, keys[cursor:end])
}

func (r *RedisServer) publish(conn redcon.Conn, cmd redcon.Command) {
	count := r.publishCluster(context.Background(), cmd.Args[1], cmd.Args[2])
	if r.traceCommands {
		log.Printf("redis trace publish remote=%s channel=%q subscribers=%d", conn.RemoteAddr(), string(cmd.Args[1]), count)
	}
	conn.WriteInt64(count)
}

func (r *RedisServer) subscribe(conn redcon.Conn, cmd redcon.Command) {
	for _, channel := range cmd.Args[1:] {
		r.pubsub.Subscribe(conn, string(channel))
	}
}

func (r *RedisServer) dbsize(conn redcon.Conn, _ redcon.Command) {
	if !r.coordinator.IsLeader() {
		size, err := r.proxyDBSize()
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(size)
		return
	}
	if err := r.coordinator.VerifyLeader(); err != nil {
		conn.WriteError(err.Error())
		return
	}

	keys, err := r.visibleKeys([]byte("*"))
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(len(keys))
}

func (r *RedisServer) flushdb(conn redcon.Conn, _ redcon.Command) {
	r.flushDatabase(conn, false)
}

func (r *RedisServer) flushall(conn redcon.Conn, _ redcon.Command) {
	r.flushDatabase(conn, true)
}

// deleteLegacyKeys scans the full keyspace and deletes keys that do not belong
// to any known internal prefix. Returns the number of user-visible legacy keys
// deleted. TTL keys are intentionally NOT deleted because the !redis|ttl|
// namespace is shared across all Redis types — deleting them could strip
// expiration from already-migrated or newly-created keys.
func (r *RedisServer) deleteLegacyKeys(ctx context.Context, readTS uint64) (int, error) {
	const batchSize = 1000
	var totalDeleted int
	cursor := make([]byte, 0, batchSize)
	for {
		kvs, err := r.store.ScanAt(ctx, cursor, nil, batchSize, readTS)
		if err != nil {
			return totalDeleted, fmt.Errorf("scan: %w", err)
		}
		if len(kvs) == 0 {
			break
		}

		elems := make([]*kv.Elem[kv.OP], 0, len(kvs))
		legacyCount := 0
		for _, pair := range kvs {
			if !isKnownInternalKey(pair.Key) {
				legacyCount++
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: pair.Key})
			}
		}

		if len(elems) > 0 {
			if err := r.dispatchElems(ctx, false, readTS, elems); err != nil {
				return totalDeleted, err
			}
			totalDeleted += legacyCount
		}

		// Advance cursor past the last key in this batch.
		lastKey := kvs[len(kvs)-1].Key
		cursor = make([]byte, len(lastKey)+1)
		copy(cursor, lastKey)

		// Yield briefly between batches to avoid saturating the Raft log.
		time.Sleep(time.Millisecond)
	}
	return totalDeleted, nil
}

// flushlegacy deletes old unprefixed Redis string keys that were written before
// the !redis|str| prefix migration. It scans all keys and deletes those that
// do not match any known internal prefix. This is a one-time migration operation.
func (r *RedisServer) flushlegacy(conn redcon.Conn, _ redcon.Command) {
	if !r.coordinator.IsLeader() {
		n, err := r.proxyFlushLegacy()
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(n)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisFlushLegacyTimeout)
	defer cancel()

	totalDeleted, err := r.deleteLegacyKeys(ctx, r.readTS())
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(totalDeleted)
}

func (r *RedisServer) flushDatabase(conn redcon.Conn, all bool) {
	if !r.coordinator.IsLeader() {
		if err := r.proxyFlushDatabase(all); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteString("OK")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if err := r.retryRedisWrite(ctx, func() error {
		if err := r.coordinator.VerifyLeader(); err != nil {
			return fmt.Errorf("verify leader: %w", err)
		}

		// Delete only Redis-related keys. Each DEL_PREFIX operation must be
		// dispatched separately because the FSM processes only one DEL_PREFIX
		// per request (the first mutation).
		//
		// Namespaces covered:
		//   "!redis|" — str, legacy hash/set/zset/hll/stream, ttl
		//   "!lst|"   — list meta + items
		//   "!zs|"    — zset wide-column
		//   "!hs|"    — hash wide-column meta/field/delta
		//   "!st|"    — set wide-column meta/member/delta
		//
		// Legacy bare keys are NOT deleted here to avoid a full keyspace
		// scan. Run FLUSHLEGACY first to clean up legacy data.
		//
		// All prefixes are attempted even if one dispatch fails so that we
		// delete as many namespaces as possible before reporting errors.
		var combined error
		for _, prefix := range [][]byte{
			[]byte("!redis|"),
			[]byte("!lst|"),
			[]byte("!zs|"),
			[]byte("!hs|"),
			[]byte("!st|"),
		} {
			if _, err := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
				Elems: []*kv.Elem[kv.OP]{
					{Op: kv.DelPrefix, Key: prefix},
				},
			}); err != nil {
				combined = cockerrors.CombineErrors(combined, fmt.Errorf("dispatch del_prefix %q: %w", prefix, err))
			}
		}
		return cockerrors.WithStack(combined)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteString("OK")
}

func (r *RedisServer) pubsubCmd(conn redcon.Conn, cmd redcon.Command) {
	switch strings.ToUpper(string(cmd.Args[1])) {
	case "CHANNELS":
		r.writePubSubChannels(conn, cmd.Args)
	case "NUMSUB":
		r.writePubSubNumSub(conn, cmd.Args)
	case "NUMPAT":
		conn.WriteInt(0)
	default:
		conn.WriteError("ERR unsupported PUBSUB subcommand '" + string(cmd.Args[1]) + "'")
	}
}

func (r *RedisServer) writePubSubChannels(conn redcon.Conn, args [][]byte) {
	pattern := []byte("*")
	if len(args) >= pubsubPatternArgMin {
		pattern = args[pubsubFirstChannel]
	}

	counts := r.pubsubChannelCounts()
	channels := make([]string, 0, len(counts))
	for channel, count := range counts {
		if count <= 0 || !matchesAsteriskPattern(pattern, []byte(channel)) {
			continue
		}
		channels = append(channels, channel)
	}

	sort.Strings(channels)
	conn.WriteArray(len(channels))
	for _, channel := range channels {
		conn.WriteBulkString(channel)
	}
}

func (r *RedisServer) writePubSubNumSub(conn redcon.Conn, args [][]byte) {
	channels := args[pubsubFirstChannel:]
	snapshot := r.pubsubChannelCounts()

	conn.WriteArray(len(channels) * redisPairWidth)
	for _, channel := range channels {
		conn.WriteBulk(channel)
		conn.WriteInt(snapshot[string(channel)])
	}
}

func (r *RedisServer) pubsubChannelCounts() map[string]int {
	return r.pubsub.ChannelCounts()
}

func (r *RedisServer) sadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.mutateExactSet(conn, setKind, cmd.Args[1], cmd.Args[2:], true)
}

func (r *RedisServer) srem(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.mutateExactSet(conn, setKind, cmd.Args[1], cmd.Args[2:], false)
}

func (r *RedisServer) validateExactSetKind(kind string, key []byte, readTS uint64) error {
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return err
	}

	switch kind {
	case setKind:
		return r.validateExactSetType(typ, key, readTS)
	case hllKind:
		return r.validateExactHLLType(typ, key, readTS)
	default:
		return errors.New("ERR unsupported exact set kind")
	}
}

func (r *RedisServer) hllExistsAt(key []byte, readTS uint64) (bool, error) {
	exists, err := r.store.ExistsAt(context.Background(), redisHLLKey(key), readTS)
	if err != nil {
		return false, fmt.Errorf("exists hll: %w", err)
	}
	return exists, nil
}

func (r *RedisServer) validateExactSetType(typ redisValueType, key []byte, readTS uint64) error {
	if typ == redisTypeSet {
		return nil
	}
	if typ != redisTypeNone {
		return wrongTypeError()
	}

	hllExists, err := r.hllExistsAt(key, readTS)
	if err != nil {
		return err
	}
	if hllExists {
		return wrongTypeError()
	}
	return nil
}

func (r *RedisServer) validateExactHLLType(typ redisValueType, key []byte, readTS uint64) error {
	if typ == redisTypeNone {
		return nil
	}

	hllExists, err := r.hllExistsAt(key, readTS)
	if err != nil {
		return err
	}
	if !hllExists {
		return wrongTypeError()
	}
	return nil
}

func exactSetMembers(value redisSetValue) map[string]struct{} {
	members := make(map[string]struct{}, len(value.Members))
	for _, member := range value.Members {
		members[member] = struct{}{}
	}
	return members
}

func applyExactSetMutation(existing map[string]struct{}, members [][]byte, add bool) int {
	changed := 0
	for _, member := range members {
		memberKey := string(member)
		_, ok := existing[memberKey]
		if add {
			if ok {
				continue
			}
			existing[memberKey] = struct{}{}
			changed++
			continue
		}
		if ok {
			delete(existing, memberKey)
			changed++
		}
	}
	return changed
}

func sortedExactSetMembers(existing map[string]struct{}) []string {
	out := make([]string, 0, len(existing))
	for member := range existing {
		out = append(out, member)
	}
	sort.Strings(out)
	return out
}

func (r *RedisServer) persistExactSetMembersTxn(ctx context.Context, kind string, key []byte, readTS uint64, members map[string]struct{}) error {
	if kind != setKind {
		// HLL and other non-set kinds keep using the legacy blob format.
		if len(members) == 0 {
			elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
			if err != nil {
				return err
			}
			return r.dispatchElems(ctx, true, readTS, elems)
		}
		payload, err := marshalSetValue(redisSetValue{Members: sortedExactSetMembers(members)})
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisExactSetStorageKey(kind, key), Value: payload},
		})
	}
	// Wide-column set: full rewrite (used when the whole state is available).
	if len(members) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(members)+setWideColOverhead)
	for member := range members {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMemberKey(key, []byte(member)),
			Value: []byte{},
		})
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.SetMetaKey(key),
		Value: store.MarshalSetMeta(store.SetMeta{Len: int64(len(members))}),
	})
	// Remove legacy blob if present.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisSetKey(key)})
	return r.dispatchElems(ctx, true, readTS, elems)
}

// applySetMemberMutation emits a Put or Del for one set member and returns the
// change count (1) and the signed length delta (+1 or -1), or (0, 0) if no change.
func applySetMemberMutation(elems []*kv.Elem[kv.OP], memberKey []byte, exists, add bool) ([]*kv.Elem[kv.OP], int, int64) {
	if add && !exists {
		return append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: []byte{}}), 1, 1
	}
	if !add && exists {
		return append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: memberKey}), 1, -1
	}
	return elems, 0, 0
}

// mutateExactSetLegacy handles SADD/SREM for non-set kinds (e.g. HLL) via the legacy blob path.
func (r *RedisServer) mutateExactSetLegacy(conn redcon.Conn, ctx context.Context, kind string, key []byte, members [][]byte, add bool) {
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(kind, key, readTS); err != nil {
			return err
		}
		value, err := r.loadSetAt(context.Background(), kind, key, readTS)
		if err != nil {
			return err
		}
		existing := exactSetMembers(value)
		changed = applyExactSetMutation(existing, members, add)
		if changed == 0 {
			return nil
		}
		return r.persistExactSetMembersTxn(ctx, kind, key, readTS, existing)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(changed)
}

// mutateExactSetWide handles SADD/SREM for the wide-column set path.
func (r *RedisServer) mutateExactSetWide(conn redcon.Conn, ctx context.Context, key []byte, members [][]byte, add bool) {
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(setKind, key, readTS); err != nil {
			return err
		}

		commitTS := r.coordinator.Clock().Next()

		migrationElems, migErr := r.buildSetLegacyMigrationElems(ctx, key, readTS)
		if migErr != nil {
			return migErr
		}
		elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(members)+setWideColOverhead)
		elems = append(elems, migrationElems...)

		// Extract legacy member names from migration ops so that applySetMemberMutations
		// can treat them as already-existing (they are not yet visible at readTS).
		legacyMemberBase := buildLegacySetMemberBase(migrationElems, key)

		var lenDelta int64
		var mutErr error
		elems, changed, lenDelta, mutErr = r.applySetMemberMutations(ctx, key, members, add, readTS, elems, legacyMemberBase)
		if mutErr != nil {
			return mutErr
		}

		if changed == 0 && len(migrationElems) == 0 {
			return nil
		}

		if lenDelta != 0 {
			deltaVal := store.MarshalSetMetaDelta(store.SetMetaDelta{LenDelta: lenDelta})
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.SetMetaDeltaKey(key, commitTS, 0),
				Value: deltaVal,
			})
		}

		if len(elems) == 0 {
			return nil
		}

		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(changed)
}

// scanSetMemberExistsMap does a paginated prefix scan of all member keys for
// the given set and returns a map from member name to struct{}{}.
// Using a single prefix scan eliminates the per-member ExistsAt round-trip.
func (r *RedisServer) scanSetMemberExistsMap(ctx context.Context, key []byte, readTS uint64) (map[string]struct{}, error) {
	return r.scanKeyExistsMap(ctx, store.SetMemberScanPrefix(key), readTS,
		func(k []byte) []byte { return store.ExtractSetMemberName(k, key) })
}

// scanHashFieldExistsMap does a paginated prefix scan of all field keys for
// the given hash and returns a map from field name to struct{}{}.
// Using a single prefix scan eliminates per-field ExistsAt round-trips.
func (r *RedisServer) scanHashFieldExistsMap(ctx context.Context, key []byte, readTS uint64) (map[string]struct{}, error) {
	return r.scanKeyExistsMap(ctx, store.HashFieldScanPrefix(key), readTS,
		func(k []byte) []byte { return store.ExtractHashFieldName(k, key) })
}

// mergeZSetBulkScores performs a single prefix scan of ZSet member keys and
// merges the store scores into inTxnView when pairCount >= wideColumnBulkScanThreshold.
// This avoids O(pairCount) individual GetAt round-trips inside applyZAddPair.
// Members already in inTxnView (migration elems or earlier pairs) take precedence.
// Returns inTxnView unchanged when the batch is below the threshold.
func (r *RedisServer) mergeZSetBulkScores(ctx context.Context, key []byte, readTS uint64, pairCount int, inTxnView map[string]float64) (map[string]float64, error) {
	if pairCount < wideColumnBulkScanThreshold {
		return inTxnView, nil
	}
	bulkScores, err := r.scanZSetMemberScoreMap(ctx, key, readTS)
	if err != nil {
		return nil, err
	}
	if inTxnView == nil {
		return bulkScores, nil
	}
	for m, s := range bulkScores {
		if _, alreadySeen := inTxnView[m]; !alreadySeen {
			inTxnView[m] = s
		}
	}
	return inTxnView, nil
}

// scanZSetMemberScoreMap does a paginated prefix scan of all member keys for
// the given ZSet and returns a map from member name to its current score.
// Using a single prefix scan eliminates O(N) GetAt round-trips in ZADD for
// large batches (>= wideColumnBulkScanThreshold pairs).
func (r *RedisServer) scanZSetMemberScoreMap(ctx context.Context, key []byte, readTS uint64) (map[string]float64, error) {
	scanPrefix := store.ZSetMemberScanPrefix(key)
	scanEnd := store.PrefixScanEnd(scanPrefix)
	scores := make(map[string]float64)
	cursor := scanPrefix
	for {
		scanKVs, err := r.store.ScanAt(ctx, cursor, scanEnd, store.MaxDeltaScanLimit, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
		for _, pair := range scanKVs {
			m := store.ExtractZSetMemberName(pair.Key, key)
			if m == nil {
				continue
			}
			if s, decodeErr := store.UnmarshalZSetScore(pair.Value); decodeErr == nil {
				scores[string(m)] = s
			}
		}
		if len(scanKVs) < store.MaxDeltaScanLimit {
			break
		}
		lastKey := scanKVs[len(scanKVs)-1].Key
		next := make([]byte, len(lastKey)+1)
		copy(next, lastKey)
		cursor = next
	}
	return scores, nil
}

// scanKeyExistsMap paginates through all keys under scanPrefix, extracts a
// name from each key using extractName, and builds a set of existing names.
// It is used by scanSetMemberExistsMap and scanHashFieldExistsMap to eliminate
// per-key ExistsAt round-trips during SADD/SREM/HDEL operations.
func (r *RedisServer) scanKeyExistsMap(ctx context.Context, scanPrefix []byte, readTS uint64, extractName func([]byte) []byte) (map[string]struct{}, error) {
	scanEnd := store.PrefixScanEnd(scanPrefix)
	existsMap := make(map[string]struct{})
	cursor := scanPrefix
	for {
		scanKVs, err := r.store.ScanAt(ctx, cursor, scanEnd, store.MaxDeltaScanLimit, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
		for _, pair := range scanKVs {
			if name := extractName(pair.Key); name != nil {
				existsMap[string(name)] = struct{}{}
			}
		}
		if len(scanKVs) < store.MaxDeltaScanLimit {
			break
		}
		lastKey := scanKVs[len(scanKVs)-1].Key
		next := make([]byte, len(lastKey)+1)
		copy(next, lastKey)
		cursor = next
	}
	return existsMap, nil
}

// initSetExistsMap builds the initial existence map for a set mutation batch.
// For large batches or when legacy members are present it does a bulk prefix
// scan; otherwise it returns an empty (non-nil) map for per-member ExistsAt
// fallback. Legacy members from migration elems are merged in so that members
// already in-flight in the same transaction are treated as existing.
func (r *RedisServer) initSetExistsMap(ctx context.Context, key []byte, members [][]byte, readTS uint64, legacyBase map[string]struct{}) (map[string]struct{}, error) {
	existsMap := make(map[string]struct{})
	if len(members) >= wideColumnBulkScanThreshold || len(legacyBase) > 0 {
		var err error
		existsMap, err = r.scanSetMemberExistsMap(ctx, key, readTS)
		if err != nil {
			return nil, cockerrors.WithStack(err)
		}
	}
	for m := range legacyBase {
		existsMap[m] = struct{}{}
	}
	return existsMap, nil
}

// lookupSetMemberExists reports whether memberStr is present, updating
// existsMap as a cache. For small clean batches (no bulk scan, no legacy
// migration) it falls back to an ExistsAt store read; otherwise it relies
// solely on the pre-built map.
func (r *RedisServer) lookupSetMemberExists(ctx context.Context, memberStr string, memberKey []byte, readTS uint64, existsMap map[string]struct{}, isSmallClean bool) (bool, error) {
	if _, ok := existsMap[memberStr]; ok {
		return true, nil
	}
	if !isSmallClean {
		return false, nil
	}
	exists, err := r.store.ExistsAt(ctx, memberKey, readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	if exists {
		existsMap[memberStr] = struct{}{}
	}
	return exists, nil
}

// applySetMemberMutations resolves existence for each member using either a
// pre-built bulk scan (for large batches) or individual ExistsAt calls (for
// small batches), then applies the mutation to elems.
// The bulk scan threshold is wideColumnBulkScanThreshold.
// legacyBase contains members from a legacy blob being migrated in the same
// transaction; they are not visible at readTS and must be treated as existing.
func (r *RedisServer) applySetMemberMutations(ctx context.Context, key []byte, members [][]byte, add bool, readTS uint64, elems []*kv.Elem[kv.OP], legacyBase map[string]struct{}) ([]*kv.Elem[kv.OP], int, int64, error) {
	existsMap, err := r.initSetExistsMap(ctx, key, members, readTS, legacyBase)
	if err != nil {
		return nil, 0, 0, err
	}
	isSmallClean := len(members) < wideColumnBulkScanThreshold && len(legacyBase) == 0
	changed := 0
	lenDelta := int64(0)
	for _, member := range members {
		memberStr := string(member)
		memberKey := store.SetMemberKey(key, member)
		exists, lookupErr := r.lookupSetMemberExists(ctx, memberStr, memberKey, readTS, existsMap, isSmallClean)
		if lookupErr != nil {
			return nil, 0, 0, lookupErr
		}
		var cnt int
		var d int64
		elems, cnt, d = applySetMemberMutation(elems, memberKey, exists, add)
		changed += cnt
		lenDelta += d
		// Update existsMap to reflect this mutation so that subsequent
		// duplicate members in this call observe the correct in-txn state.
		if add {
			existsMap[memberStr] = struct{}{}
		} else {
			delete(existsMap, memberStr)
		}
	}
	return elems, changed, lenDelta, nil
}

func (r *RedisServer) mutateExactSet(conn redcon.Conn, kind string, key []byte, members [][]byte, add bool) {
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	if kind != setKind {
		r.mutateExactSetLegacy(conn, ctx, kind, key, members, add)
		return
	}
	r.mutateExactSetWide(conn, ctx, key, members, add)
}

func (r *RedisServer) sismember(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	member := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	hit, alive, err := r.setMemberFastExists(ctx, key, member, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if hit {
		if alive {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return
	}
	r.sismemberSlow(conn, ctx, key, member, readTS)
}

func (r *RedisServer) setMemberFastExists(ctx context.Context, key, member []byte, readTS uint64) (hit, alive bool, err error) {
	// String-priority guard; see hashFieldFastLookup for rationale.
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return false, false, hErr
	} else if higher {
		return false, false, nil
	}
	exists, err := r.store.ExistsAt(ctx, store.SetMemberKey(key, member), readTS)
	if err != nil {
		return false, false, cockerrors.WithStack(err)
	}
	if !exists {
		return false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return false, false, cockerrors.WithStack(expErr)
	}
	return true, !expired, nil
}

func (r *RedisServer) sismemberSlow(conn redcon.Conn, ctx context.Context, key, member []byte, readTS uint64) {
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeSet {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadSetAt(ctx, setKind, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if slices.Contains(value.Members, string(member)) {
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) smembers(conn redcon.Conn, cmd redcon.Command) {
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
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadSetAt(context.Background(), setKind, cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(value.Members))
	for _, member := range value.Members {
		conn.WriteBulkString(member)
	}
}

func (r *RedisServer) pfadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var changed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		if err := r.validateExactSetKind(hllKind, cmd.Args[1], readTS); err != nil {
			return err
		}

		value, err := r.loadSetAt(context.Background(), hllKind, cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		existing := exactSetMembers(value)
		changed = applyExactSetMutation(existing, cmd.Args[2:], true)
		if changed == 0 {
			return nil
		}

		return r.persistExactSetMembersTxn(ctx, hllKind, cmd.Args[1], readTS, existing)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	if changed == 0 {
		conn.WriteInt(0)
	} else {
		conn.WriteInt(1)
	}
}

func (r *RedisServer) pfcount(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	union := map[string]struct{}{}
	for _, key := range cmd.Args[1:] {
		typ, err := r.keyTypeAt(context.Background(), key, readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if typ != redisTypeNone {
			hllExists, err := r.store.ExistsAt(context.Background(), redisHLLKey(key), readTS)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if !hllExists {
				conn.WriteError(wrongTypeMessage)
				return
			}
		}
		value, err := r.loadSetAt(context.Background(), hllKind, key, readTS)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		for _, member := range value.Members {
			union[member] = struct{}{}
		}
	}
	conn.WriteInt(len(union))
}

func (r *RedisServer) hset(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	added, err := r.applyHashFieldPairs(cmd.Args[1], cmd.Args[2:])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(added)
}

func (r *RedisServer) hmset(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	if _, err := r.applyHashFieldPairs(cmd.Args[1], cmd.Args[2:]); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

// buildHashLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|hash| blob to wide-column !hs|fld| keys.  Returns nil if no legacy
// blob exists.  The base meta key is also written with the migrated count so
// that resolveHashMeta works correctly after migration.
func (r *RedisServer) buildHashLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisHashKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalHashValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value)+setWideColOverhead)
	for field, val := range value {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(key, []byte(field)),
			Value: []byte(val),
		})
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisHashKey(key)})
	// Write a base meta so that resolveHashMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaKey(key),
		Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(value))}),
	})
	return elems, nil
}

// buildSetLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|set| blob to wide-column !st|mem| keys.  Returns nil if no legacy
// blob exists.
func (r *RedisServer) buildSetLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisSetKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalSetValue(raw)
	if err != nil {
		return nil, err
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Members)+setWideColOverhead)
	for _, member := range value.Members {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMemberKey(key, []byte(member)),
			Value: []byte{},
		})
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisSetKey(key)})
	// Write a base meta so that resolveSetMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.SetMetaKey(key),
		Value: store.MarshalSetMeta(store.SetMeta{Len: int64(len(value.Members))}),
	})
	return elems, nil
}

// buildZSetLegacyMigrationElems returns ops that atomically migrate a legacy
// !redis|zset| blob to wide-column !zs|mem| + !zs|scr| keys. Returns nil if no legacy
// blob exists.  The base meta key is also written with the migrated count so
// that resolveZSetMeta works correctly after migration.
func (r *RedisServer) buildZSetLegacyMigrationElems(ctx context.Context, key []byte, readTS uint64) ([]*kv.Elem[kv.OP], error) {
	raw, err := r.store.GetAt(ctx, redisZSetKey(key), readTS)
	if cockerrors.Is(err, store.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, cockerrors.WithStack(err)
	}
	value, err := unmarshalZSetValue(raw)
	if err != nil {
		return nil, err
	}
	// Each entry → member key + score index key; plus legacy blob deletion + base meta.
	elems := make([]*kv.Elem[kv.OP], 0, len(value.Entries)*2+setWideColOverhead) //nolint:mnd // 2 ops per entry (member + score index)
	for _, entry := range value.Entries {
		elems = append(elems,
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetMemberKey(key, []byte(entry.Member)),
				Value: store.MarshalZSetScore(entry.Score),
			},
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetScoreKey(key, entry.Score, []byte(entry.Member)),
				Value: []byte{},
			},
		)
	}
	// Delete the legacy blob.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisZSetKey(key)})
	// Write a base meta so that resolveZSetMeta starts from an accurate count.
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.ZSetMetaKey(key),
		Value: store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(value.Entries))}),
	})
	return elems, nil
}

// addLegacyHashFieldsToMap adds field names from migration Put elems (fields
// being migrated in the current transaction, not yet visible at readTS) into
// existsMap so that buildHashFieldElems does not count them as new fields.
func addLegacyHashFieldsToMap(migrationElems []*kv.Elem[kv.OP], key []byte, existsMap map[string]struct{}) {
	for _, elem := range migrationElems {
		if elem.Op == kv.Put {
			if f := store.ExtractHashFieldName(elem.Key, key); f != nil {
				existsMap[string(f)] = struct{}{}
			}
		}
	}
}

// buildLegacySetMemberBase extracts member names from migration Put elems
// (members being migrated in the current transaction, invisible at readTS)
// and returns them as a set. Returns nil when no migration is happening.
func buildLegacySetMemberBase(migrationElems []*kv.Elem[kv.OP], key []byte) map[string]struct{} {
	var base map[string]struct{}
	for _, elem := range migrationElems {
		if elem.Op == kv.Put {
			if m := store.ExtractSetMemberName(elem.Key, key); m != nil {
				if base == nil {
					base = make(map[string]struct{})
				}
				base[string(m)] = struct{}{}
			}
		}
	}
	return base
}

// buildHashFieldElems iterates over field-value pairs in args, checks each
// field against existsMap to determine if it is new, appends Put operations
// to elems, and returns the updated elems and new-field count.
// existsMap is built by scanHashFieldExistsMap before this call so that
// existence checks are a single bulk scan rather than N ExistsAt round-trips.
func (r *RedisServer) buildHashFieldElems(key []byte, args [][]byte, existsMap map[string]struct{}, elems []*kv.Elem[kv.OP]) ([]*kv.Elem[kv.OP], int) {
	newFields := 0
	for i := 0; i < len(args); i += redisPairWidth {
		field := args[i]
		value := args[i+1]
		fieldStr := string(field)
		fieldKey := store.HashFieldKey(key, field)
		if _, exists := existsMap[fieldStr]; !exists {
			newFields++
			// Mark as seen so duplicate field names in one HSET call are not
			// counted as additional new fields (Redis deduplication semantics).
			existsMap[fieldStr] = struct{}{}
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: value})
	}
	return elems, newFields
}

func (r *RedisServer) applyHashFieldPairs(key []byte, args [][]byte) (int, error) {
	if len(args) == 0 || len(args)%redisPairWidth != 0 {
		return 0, errors.New("ERR wrong number of arguments for hash command")
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var added int
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), key, readTS)
		if err != nil {
			return err
		}
		if typ != redisTypeNone && typ != redisTypeHash {
			return wrongTypeError()
		}

		commitTS := r.coordinator.Clock().Next()

		// Atomically migrate any legacy blob on first wide-column write.
		// Fetch migration elems before allocating the main elems slice so that
		// the initial capacity accounts for both migration and field Put ops,
		// avoiding a reallocation when a legacy blob is present.
		migrationElems, err := r.buildHashLegacyMigrationElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(args)/redisPairWidth+setWideColOverhead)
		elems = append(elems, migrationElems...)

		// Bulk-scan existing fields once so buildHashFieldElems can check
		// existence via a map lookup instead of per-field ExistsAt.
		existsMap, err := r.scanHashFieldExistsMap(ctx, key, readTS)
		if err != nil {
			return err
		}
		// Fields from the legacy blob are being migrated in this same transaction,
		// so they are not yet visible at readTS. Add them to existsMap so that
		// buildHashFieldElems does not count already-existing fields as new.
		addLegacyHashFieldsToMap(migrationElems, key, existsMap)

		var newFields int
		elems, newFields = r.buildHashFieldElems(key, args, existsMap, elems)
		added = newFields

		// Emit a single delta key for all newly-added fields.
		if newFields != 0 {
			deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: int64(newFields)})
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.HashMetaDeltaKey(key, commitTS, 0),
				Value: deltaVal,
			})
		}

		if len(elems) == 0 {
			return nil
		}

		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	})
	return added, err
}

func (r *RedisServer) hget(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	field := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	// Fast path: look the wide-column field up directly. Live
	// wide-column hashes resolve here in 1 seek + TTL probe versus
	// the ~17 seeks rawKeyTypeAt issues through keyTypeAt. Legacy-
	// blob hashes miss the wide-column key and fall through.
	raw, hit, alive, err := r.hashFieldFastLookup(ctx, key, field, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if hit {
		if !alive {
			conn.WriteNull()
			return
		}
		// WriteBulk sends the payload directly from the []byte backing
		// store; WriteBulkString(string(raw)) would force a []byte →
		// string copy on every fast-path hit.
		conn.WriteBulk(raw)
		return
	}
	r.hgetSlow(conn, ctx, key, field, readTS)
}

// hashFieldFastLookup probes the wide-column field entry directly and
// reports whether it is present and TTL-alive. Returns hit=false when
// the wide-column key is absent, or when the narrow string-encoding
// guard in hasHigherPriorityStringEncoding fires, so the caller
// falls through to hgetSlow.
//
// Priority-alignment scope: this fast path does NOT fully mirror
// rawKeyTypeAt / keyTypeAt's priority checks. The guard only probes
// redisStrKey (the common SET-over-previous-hash corruption case);
// rarer dual-encoding corruption involving HLL, legacy bare keys, or
// list meta / delta entries is NOT caught here and will surface the
// wide-column hash answer instead of the WRONGTYPE / nil response
// keyTypeAt would produce. In normal operation at most one encoding
// exists per user key, so the guard is a guaranteed miss and the
// priority-alignment gap is invisible; pre-existing writers already
// clean up the old encoding before switching types. A full check
// would cost ~3-5 extra seeks per fast-path hit, which would negate
// most of the gain over the ~17-seek keyTypeAt slow path.
func (r *RedisServer) hashFieldFastLookup(ctx context.Context, key, field []byte, readTS uint64) (raw []byte, hit, alive bool, err error) {
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return nil, false, false, hErr
	} else if higher {
		return nil, false, false, nil
	}
	raw, err = r.store.GetAt(ctx, store.HashFieldKey(key, field), readTS)
	if err != nil {
		if cockerrors.Is(err, store.ErrKeyNotFound) {
			return nil, false, false, nil
		}
		return nil, false, false, cockerrors.WithStack(err)
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return nil, false, false, cockerrors.WithStack(expErr)
	}
	return raw, true, !expired, nil
}

// hasHigherPriorityStringEncoding returns true iff a string-encoded
// entry exists for key. Matches the "string wins" tiebreaker in
// rawKeyTypeAt (see adapter/redis_compat_helpers.go), so a corrupt
// dual-encoded key where both a string and a collection row are
// present will take the slow path and return WRONGTYPE / nil from
// keyTypeAt rather than the collection-specific fast-path answer.
func (r *RedisServer) hasHigherPriorityStringEncoding(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	exists, err := r.store.ExistsAt(ctx, redisStrKey(key), readTS)
	if err != nil {
		return false, cockerrors.WithStack(err)
	}
	return exists, nil
}

// hgetSlow falls back to the type-probing path when hashFieldFastLookup
// misses. Handles legacy-blob hashes and nil / WRONGTYPE disambiguation.
func (r *RedisServer) hgetSlow(conn redcon.Conn, ctx context.Context, key, field []byte, readTS uint64) {
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadHashAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fieldValue, ok := value[string(field)]
	if !ok {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(fieldValue)
}

func (r *RedisServer) hmget(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fields := cmd.Args[redisPairWidth:]
	if typ == redisTypeNone {
		conn.WriteArray(len(fields))
		for range cmd.Args[2:] {
			conn.WriteNull()
		}
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(fields))
	for _, field := range fields {
		fieldValue, ok := value[string(field)]
		if !ok {
			conn.WriteNull()
			continue
		}
		conn.WriteBulkString(fieldValue)
	}
}

func (r *RedisServer) hdel(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		removed, err = r.hdelTxn(ctx, cmd.Args[1], cmd.Args[2:])
		return err
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

// hdelWideColumn deletes the given fields from the wide-column hash and emits a negative delta.
func (r *RedisServer) hdelWideColumn(ctx context.Context, key []byte, fields [][]byte, readTS uint64) (int, error) {
	delElems, removed, err := r.resolveHashFieldDelElems(ctx, key, fields, readTS)
	if err != nil {
		return 0, err
	}
	if removed == 0 {
		return 0, nil
	}
	commitTS := r.coordinator.Clock().Next()
	elems := delElems
	deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: int64(-removed)})
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaDeltaKey(key, commitTS, 0),
		Value: deltaVal,
	})
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return removed, cockerrors.WithStack(dispatchErr)
}

// resolveHashFieldDelElems checks which fields exist using either a bulk scan
// (for large batches) or individual ExistsAt calls (for small batches), then
// returns Del elems for every field that exists and the count of deletions.
func (r *RedisServer) resolveHashFieldDelElems(ctx context.Context, key []byte, fields [][]byte, readTS uint64) ([]*kv.Elem[kv.OP], int, error) {
	var existsMap map[string]struct{}
	if len(fields) >= wideColumnBulkScanThreshold {
		var err error
		existsMap, err = r.scanHashFieldExistsMap(ctx, key, readTS)
		if err != nil {
			return nil, 0, err
		}
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(fields)+1)
	removed := 0
	for _, field := range fields {
		fieldKey := store.HashFieldKey(key, field)
		var exists bool
		if existsMap != nil {
			_, exists = existsMap[string(field)]
		} else {
			var err error
			exists, err = r.store.ExistsAt(ctx, fieldKey, readTS)
			if err != nil {
				return nil, 0, cockerrors.WithStack(err)
			}
		}
		if exists {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: fieldKey})
			removed++
		}
	}
	return elems, removed, nil
}

func (r *RedisServer) hdelTxn(ctx context.Context, key []byte, fields [][]byte) (int, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	if typ == redisTypeNone {
		return 0, nil
	}
	if typ != redisTypeHash {
		return 0, wrongTypeError()
	}

	// Wide-column path: check if any !hs|fld| keys exist for this key.
	hashFieldPrefix := store.HashFieldScanPrefix(key)
	hashFieldEnd := store.PrefixScanEnd(hashFieldPrefix)
	wideKVs, err := r.store.ScanAt(context.Background(), hashFieldPrefix, hashFieldEnd, 1, readTS)
	if err != nil {
		return 0, cockerrors.WithStack(err)
	}
	if len(wideKVs) > 0 {
		return r.hdelWideColumn(ctx, key, fields, readTS)
	}

	// Legacy blob path.
	value, err := r.loadHashAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	removed := removeHashFields(value, fields)
	if removed == 0 {
		return 0, nil
	}
	return removed, r.persistHashTxn(ctx, key, readTS, value)
}

func removeHashFields(value redisHashValue, fields [][]byte) int {
	removed := 0
	for _, field := range fields {
		if _, ok := value[string(field)]; ok {
			delete(value, string(field))
			removed++
		}
	}
	return removed
}

func (r *RedisServer) persistHashTxn(ctx context.Context, key []byte, readTS uint64, value redisHashValue) error {
	if len(value) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	// Wide-column rewrite: write per-field keys and a new base meta.
	// deleteLogicalKeyElems (called by the caller when needed) clears old keys.
	elems := make([]*kv.Elem[kv.OP], 0, len(value)+1)
	for field, val := range value {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(key, []byte(field)),
			Value: []byte(val),
		})
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaKey(key),
		Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(value))}),
	})
	// Also remove the legacy blob if it was present.
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisHashKey(key)})
	return r.dispatchElems(ctx, true, readTS, elems)
}

func (r *RedisServer) hexists(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	key := cmd.Args[1]
	field := cmd.Args[2]
	readTS := r.readTS()
	ctx := context.Background()

	// Fast path: direct wide-column field existence check. ExistsAt
	// is cheaper than GetAt since we don't need the value payload.
	hit, alive, err := r.hashFieldFastExists(ctx, key, field, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if hit {
		if alive {
			conn.WriteInt(1)
		} else {
			conn.WriteInt(0)
		}
		return
	}
	r.hexistsSlow(conn, ctx, key, field, readTS)
}

func (r *RedisServer) hashFieldFastExists(ctx context.Context, key, field []byte, readTS uint64) (hit, alive bool, err error) {
	// String-priority guard; see hashFieldFastLookup for rationale.
	if higher, hErr := r.hasHigherPriorityStringEncoding(ctx, key, readTS); hErr != nil {
		return false, false, hErr
	} else if higher {
		return false, false, nil
	}
	exists, err := r.store.ExistsAt(ctx, store.HashFieldKey(key, field), readTS)
	if err != nil {
		return false, false, cockerrors.WithStack(err)
	}
	if !exists {
		return false, false, nil
	}
	expired, expErr := r.hasExpired(ctx, key, readTS, true)
	if expErr != nil {
		return false, false, cockerrors.WithStack(expErr)
	}
	return true, !expired, nil
}

func (r *RedisServer) hexistsSlow(conn redcon.Conn, ctx context.Context, key, field []byte, readTS uint64) {
	typ, err := r.keyTypeAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteInt(0)
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}
	value, err := r.loadHashAt(ctx, key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if _, ok := value[string(field)]; ok {
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) hlen(conn redcon.Conn, cmd redcon.Command) {
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
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	// Wide-column path: use delta-aggregated metadata for O(1) count.
	count, exists, err := r.resolveHashMeta(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if exists {
		conn.WriteInt64(count)
		return
	}
	// Legacy blob fallback: load all fields and count.
	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(len(value))
}

func (r *RedisServer) hincrby(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	increment, err := strconv.ParseInt(string(cmd.Args[3]), 10, 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var current int64
	if err := r.retryRedisWrite(ctx, func() error {
		var txnErr error
		current, txnErr = r.hincrbyTxn(ctx, cmd.Args[1], cmd.Args[2], increment)
		return txnErr
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(current)
}

// readHashFieldInt reads the current integer value of a hash field from wide-column or legacy storage.
// Returns (current, isNewField, legacyHashValue, error). legacyHashValue is non-nil only when
// the value came from a legacy JSON blob that needs to be migrated on the next write.
func (r *RedisServer) readHashFieldInt(ctx context.Context, key, field []byte, readTS uint64) (int64, bool, redisHashValue, error) {
	fieldKey := store.HashFieldKey(key, field)
	raw, readErr := r.store.GetAt(ctx, fieldKey, readTS)
	if readErr != nil && !cockerrors.Is(readErr, store.ErrKeyNotFound) {
		return 0, true, nil, cockerrors.WithStack(readErr)
	}
	if readErr == nil {
		current, parseErr := strconv.ParseInt(string(raw), 10, 64)
		if parseErr != nil {
			return 0, false, nil, errors.New("ERR hash value is not an integer")
		}
		return current, false, nil, nil
	}
	// Not in wide-column – check legacy blob.
	legacyValue, legacyErr := r.loadHashAt(ctx, key, readTS)
	if legacyErr != nil {
		return 0, true, nil, legacyErr
	}
	if rawLegacy, ok := legacyValue[string(field)]; ok {
		current, parseErr := strconv.ParseInt(rawLegacy, 10, 64)
		if parseErr != nil {
			return 0, false, nil, errors.New("ERR hash value is not an integer")
		}
		return current, false, legacyValue, nil
	}
	return 0, true, legacyValue, nil
}

// hincrbyWithMigration handles the HINCRBY case where a legacy JSON blob must be migrated
// atomically with the increment operation.
func (r *RedisServer) hincrbyWithMigration(ctx context.Context, key, fieldKey []byte, readTS, commitTS uint64, current int64, isNewField bool, increment int64) (int64, error) {
	migrationElems, migErr := r.buildHashLegacyMigrationElems(ctx, key, readTS)
	if migErr != nil {
		return 0, migErr
	}
	current += increment
	newVal := strconv.FormatInt(current, 10)
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+setWideColOverhead)
	elems = append(elems, migrationElems...)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: []byte(newVal)})
	if isNewField {
		deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return current, cockerrors.WithStack(dispatchErr)
}

func (r *RedisServer) hincrbyTxn(ctx context.Context, key, field []byte, increment int64) (int64, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	if typ != redisTypeNone && typ != redisTypeHash {
		return 0, wrongTypeError()
	}

	commitTS := r.coordinator.Clock().Next()
	fieldKey := store.HashFieldKey(key, field)

	current, isNewField, legacyValue, err := r.readHashFieldInt(ctx, key, field, readTS)
	if err != nil {
		return 0, err
	}

	// If a legacy blob exists, migrate it atomically with the increment.
	if len(legacyValue) > 0 {
		return r.hincrbyWithMigration(ctx, key, fieldKey, readTS, commitTS, current, isNewField, increment)
	}

	current += increment
	newVal := strconv.FormatInt(current, 10)
	elems := make([]*kv.Elem[kv.OP], 0, setWideColOverhead)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: []byte(newVal)})
	if isNewField {
		deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: 1})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return current, cockerrors.WithStack(dispatchErr)
}

func (r *RedisServer) incr(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var current int64
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ != redisTypeNone && typ != redisTypeString {
			return wrongTypeError()
		}

		current = 0
		var existingTTL *time.Time
		if typ == redisTypeString {
			raw, ttl, err := r.readRedisStringAt(cmd.Args[1], readTS)
			if err != nil {
				return err
			}
			existingTTL = ttl
			current, err = strconv.ParseInt(string(raw), 10, 64)
			if err != nil {
				return fmt.Errorf("ERR value is not an integer or out of range")
			}
		}
		current++

		// INCR preserves any existing TTL (Redis semantics).
		encoded := encodeRedisStr([]byte(strconv.FormatInt(current, 10)), existingTTL)
		elems := []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisStrKey(cmd.Args[1]), Value: encoded},
		}
		if existingTTL != nil {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(cmd.Args[1]), Value: encodeRedisTTL(*existingTTL)})
		} else {
			// Defensively clear any stale/legacy scan index entry so the sweeper
			// cannot later expire a now-persistent key.
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(cmd.Args[1])})
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(current)
}

func (r *RedisServer) hgetall(conn redcon.Conn, cmd redcon.Command) {
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
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fields := make([]string, 0, len(value))
	for field := range value {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	conn.WriteArray(len(fields) * redisPairWidth)
	for _, field := range fields {
		conn.WriteBulkString(field)
		conn.WriteBulkString(value[field])
	}
}

type zaddFlags struct {
	nx bool // only add new elements
	xx bool // only update existing elements
	gt bool // only update when new score > current score
	lt bool // only update when new score < current score
}

func parseZAddFlags(args [][]byte) (zaddFlags, int, error) {
	var flags zaddFlags
	i := 2
	for i < len(args) {
		if !flags.applyFlag(strings.ToUpper(string(args[i]))) {
			break
		}
		i++
	}
	if err := flags.validate(); err != nil {
		return zaddFlags{}, 0, err
	}
	return flags, i, nil
}

func (f *zaddFlags) applyFlag(name string) bool {
	switch name {
	case "NX":
		f.nx = true
	case "XX":
		f.xx = true
	case "GT":
		f.gt = true
	case "LT":
		f.lt = true
	default:
		return false
	}
	return true
}

func (f zaddFlags) allows(exists bool, oldScore, newScore float64) bool {
	if (f.nx && exists) || (f.xx && !exists) {
		return false
	}
	return !exists || f.scoreAllowed(oldScore, newScore)
}

func (f zaddFlags) scoreAllowed(oldScore, newScore float64) bool {
	if f.gt && newScore <= oldScore {
		return false
	}
	if f.lt && newScore >= oldScore {
		return false
	}
	return true
}

func (f zaddFlags) validate() error {
	if f.nx && f.xx {
		return fmt.Errorf("ERR XX and NX options at the same time are not compatible")
	}
	if f.nx && (f.gt || f.lt) {
		return fmt.Errorf("ERR GT, LT, and NX options at the same time are not compatible")
	}
	return nil
}

type zaddPair struct {
	score  float64
	member string
}

func parseZAddPairs(remaining [][]byte) ([]zaddPair, error) {
	pairs := make([]zaddPair, 0, len(remaining)/redisPairWidth)
	for i := 0; i < len(remaining); i += redisPairWidth {
		score, err := strconv.ParseFloat(string(remaining[i]), 64)
		if err != nil {
			return nil, fmt.Errorf("parse zadd score: %w", err)
		}
		pairs = append(pairs, zaddPair{score: score, member: string(remaining[i+1])})
	}
	return pairs, nil
}

func (r *RedisServer) zadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	flags, pairStart, err := parseZAddFlags(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	remaining := cmd.Args[pairStart:]
	if len(remaining) == 0 || len(remaining)%redisPairWidth != 0 {
		conn.WriteError("ERR syntax error")
		return
	}
	pairs, err := parseZAddPairs(remaining)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var added int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		added, err = r.zaddTxn(ctx, cmd.Args[1], flags, pairs)
		return err
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(added)
}

// buildZSetMigrationView extracts member→score from ZSet migration Put elems
// so that applyZAddPair can see migrated members without a store round-trip.
// Returns a map from member name to score; absent members were not migrated.
func buildZSetMigrationView(migrationElems []*kv.Elem[kv.OP], key []byte) map[string]float64 {
	view := make(map[string]float64)
	for _, elem := range migrationElems {
		if elem.Op != kv.Put {
			continue
		}
		m := store.ExtractZSetMemberName(elem.Key, key)
		if m == nil {
			continue
		}
		score, err := store.UnmarshalZSetScore(elem.Value)
		if err == nil {
			view[string(m)] = score
		}
	}
	return view
}

// resolveZSetMemberScore returns the current score and existence for a ZSet
// member. It checks inTxnView first (covers migration elems and earlier pairs
// in the same ZADD call), then falls back to a store GetAt.
func (r *RedisServer) resolveZSetMemberScore(ctx context.Context, memberKey []byte, member string, readTS uint64, inTxnView map[string]float64) (score float64, exists bool, err error) {
	if s, ok := inTxnView[member]; ok {
		return s, true, nil
	}
	raw, getErr := r.store.GetAt(ctx, memberKey, readTS)
	if getErr == nil {
		s, unmarshalErr := store.UnmarshalZSetScore(raw)
		if unmarshalErr != nil {
			return 0, false, cockerrors.WithStack(unmarshalErr)
		}
		return s, true, nil
	}
	if !cockerrors.Is(getErr, store.ErrKeyNotFound) {
		return 0, false, cockerrors.WithStack(getErr)
	}
	return 0, false, nil
}

// applyZAddPair processes one ZADD pair against the wide-column store: reads the
// existing member score (if any), checks the ZADD flags, emits del-old-score /
// put-member / put-score-index ops, and returns the updated elems, the add count
// (0 or 1), and the length delta (0 or +1).
// inTxnView provides an in-transaction view of member→score for members written
// in the same transaction (migration or earlier pairs); checked before GetAt so
// migrated and duplicate members are handled correctly.
func (r *RedisServer) applyZAddPair(ctx context.Context, key []byte, p zaddPair, flags zaddFlags, readTS uint64, elems []*kv.Elem[kv.OP], inTxnView map[string]float64) ([]*kv.Elem[kv.OP], int, int64, error) {
	memberKey := store.ZSetMemberKey(key, []byte(p.member))
	oldScore, memberExists, err := r.resolveZSetMemberScore(ctx, memberKey, p.member, readTS, inTxnView)
	if err != nil {
		return nil, 0, 0, err
	}
	if !flags.allows(memberExists, oldScore, p.score) {
		return elems, 0, 0, nil
	}
	if memberExists {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(p.member))})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: store.MarshalZSetScore(p.score)},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, p.score, []byte(p.member)), Value: []byte{}},
	)
	// Update inTxnView so subsequent pairs (duplicates) see this write.
	inTxnView[p.member] = p.score
	if memberExists {
		return elems, 0, 0, nil
	}
	return elems, 1, 1, nil
}

func (r *RedisServer) zaddTxn(ctx context.Context, key []byte, flags zaddFlags, pairs []zaddPair) (int, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		return 0, wrongTypeError()
	}

	commitTS := r.coordinator.Clock().Next()

	migrationElems, err := r.buildZSetLegacyMigrationElems(ctx, key, readTS)
	if err != nil {
		return 0, err
	}
	// Capacity: each pair may produce 3 ops (del old score + put member + put score index),
	// plus migration elems and a delta key.
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+len(pairs)*3+setWideColOverhead) //nolint:mnd // 3 ops per pair
	elems = append(elems, migrationElems...)

	// Seed the in-transaction view from migration elems so that migrated
	// members are not incorrectly counted as new by applyZAddPair.
	inTxnView := buildZSetMigrationView(migrationElems, key)

	// For large batches, mergeZSetBulkScores performs one prefix scan that
	// eliminates O(N) GetAt calls inside applyZAddPair; it is a no-op for
	// batches below wideColumnBulkScanThreshold.
	inTxnView, err = r.mergeZSetBulkScores(ctx, key, readTS, len(pairs), inTxnView)
	if err != nil {
		return 0, err
	}

	added := 0
	lenDelta := int64(0)
	for _, p := range pairs {
		var c int
		var d int64
		elems, c, d, err = r.applyZAddPair(ctx, key, p, flags, readTS, elems, inTxnView)
		if err != nil {
			return 0, err
		}
		added += c
		lenDelta += d
	}

	if len(elems) == 0 {
		return 0, nil
	}

	if lenDelta != 0 {
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: lenDelta})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}

	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return added, cockerrors.WithStack(dispatchErr)
}

// zincrbyTxn performs one attempt of ZINCRBY in wide-column format.
// Returns the new score after applying increment.
func (r *RedisServer) zincrbyTxn(ctx context.Context, key []byte, member string, increment float64) (float64, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		return 0, wrongTypeError()
	}

	memberKey := store.ZSetMemberKey(key, []byte(member))
	commitTS := r.coordinator.Clock().Next()

	migrationElems, migErr := r.buildZSetLegacyMigrationElems(ctx, key, readTS)
	if migErr != nil {
		return 0, migErr
	}

	// Check in-txn migration view before falling back to the store
	// (migrated keys are not yet visible at readTS).
	inTxnView := buildZSetMigrationView(migrationElems, key)
	oldScore, memberExists, err := r.resolveZSetMemberScore(ctx, memberKey, member, readTS, inTxnView)
	if err != nil {
		return 0, err
	}

	newScore := oldScore + increment
	if math.IsNaN(newScore) {
		return 0, errors.New("ERR resulting score is not a number (NaN)")
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(migrationElems)+3) //nolint:mnd // del old score + put member + put score index
	elems = append(elems, migrationElems...)
	if memberExists {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(member))})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{Op: kv.Put, Key: memberKey, Value: store.MarshalZSetScore(newScore)},
		&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, newScore, []byte(member)), Value: []byte{}},
	)
	if !memberExists {
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: 1})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
			Value: deltaVal,
		})
	}
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  normalizeStartTS(readTS),
		CommitTS: commitTS,
		Elems:    elems,
	})
	return newScore, cockerrors.WithStack(dispatchErr)
}

func (r *RedisServer) zincrby(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	increment, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var newScore float64
	if err := r.retryRedisWrite(ctx, func() error {
		var txnErr error
		newScore, txnErr = r.zincrbyTxn(ctx, cmd.Args[1], string(cmd.Args[3]), increment)
		return txnErr
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteBulkString(formatRedisFloat(newScore))
}

func parseZRangeOptions(args [][]byte) (zrangeOptions, error) {
	opts := zrangeOptions{}
	for _, arg := range args {
		switch strings.ToUpper(string(arg)) {
		case "WITHSCORES":
			opts.withScores = true
		case "REV":
			opts.reverse = true
		default:
			return zrangeOptions{}, errors.New("ERR syntax error")
		}
	}
	return opts, nil
}

func reverseZSetEntries(entries []redisZSetEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}

func writeZRangeReply(conn redcon.Conn, entries []redisZSetEntry, withScores bool) {
	if withScores {
		conn.WriteArray(len(entries) * redisPairWidth)
		for _, entry := range entries {
			conn.WriteBulkString(entry.Member)
			conn.WriteBulkString(formatRedisFloat(entry.Score))
		}
		return
	}

	conn.WriteArray(len(entries))
	for _, entry := range entries {
		conn.WriteBulkString(entry.Member)
	}
}

func removeZSetMembers(members map[string]float64, rawMembers [][]byte) int {
	removed := 0
	for _, member := range rawMembers {
		memberKey := string(member)
		if _, ok := members[memberKey]; ok {
			delete(members, memberKey)
			removed++
		}
	}
	return removed
}

func (r *RedisServer) persistZSetEntriesTxn(ctx context.Context, key []byte, readTS uint64, entries []redisZSetEntry) error {
	if len(entries) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	payload, err := marshalZSetValue(redisZSetValue{Entries: entries})
	if err != nil {
		return err
	}
	return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisZSetKey(key), Value: payload},
	})
}

func (r *RedisServer) zrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	opts, err := parseZRangeOptions(cmd.Args[4:])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	r.zrangeRead(conn, cmd.Args[1], start, stop, opts)
}

func (r *RedisServer) zrangeRead(conn redcon.Conn, key []byte, start, stop int, opts zrangeOptions) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeZSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, _, err := r.loadZSetAt(context.Background(), key, readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	entries := append([]redisZSetEntry(nil), value.Entries...)
	if opts.reverse {
		reverseZSetEntries(entries)
	}
	s, e := normalizeRankRange(start, stop, len(entries))
	if e < s {
		conn.WriteArray(0)
		return
	}
	writeZRangeReply(conn, entries[s:e+1], opts.withScores)
}

func (r *RedisServer) zrem(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			removed = 0
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		members := zsetEntriesToMap(value.Entries)
		removed = removeZSetMembers(members, cmd.Args[2:])
		if removed == 0 {
			return nil
		}
		return r.persistZSetEntriesTxn(ctx, cmd.Args[1], readTS, zsetMapToEntries(members))
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) zremrangebyrank(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			removed = 0
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		s, e := normalizeRankRange(start, stop, len(value.Entries))
		if e < s {
			removed = 0
			return nil
		}
		remaining := append([]redisZSetEntry{}, value.Entries[:s]...)
		remaining = append(remaining, value.Entries[e+1:]...)
		removed = e - s + 1
		return r.persistZSetEntriesTxn(ctx, cmd.Args[1], readTS, remaining)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) tryBZPopMin(key []byte) (*bzpopminResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var result *bzpopminResult
	err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), key, readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			result = nil
			return nil
		}
		if typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(context.Background(), key, readTS)
		if err != nil {
			return err
		}
		if len(value.Entries) == 0 {
			result = nil
			return nil
		}
		popped := value.Entries[0]
		remaining := append([]redisZSetEntry(nil), value.Entries[1:]...)

		// Detect wide-column storage.
		memberPrefix := store.ZSetMemberScanPrefix(key)
		memberEnd := store.PrefixScanEnd(memberPrefix)
		probeKVs, probeErr := r.store.ScanAt(ctx, memberPrefix, memberEnd, 1, readTS)
		if probeErr != nil {
			return cockerrors.WithStack(probeErr)
		}
		isWide := len(probeKVs) > 0

		if err := r.persistBZPopMinResult(ctx, key, readTS, popped, remaining, isWide); err != nil {
			return err
		}
		result = &bzpopminResult{key: key, entry: popped}
		return nil
	})
	return result, err
}

func (r *RedisServer) persistBZPopMinResult(ctx context.Context, key []byte, readTS uint64, popped redisZSetEntry, remaining []redisZSetEntry, isWide bool) error {
	if len(remaining) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
	if isWide {
		// Wide-column: delete the popped member key + score index, emit delta -1.
		commitTS := r.coordinator.Clock().Next()
		deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: -1})
		elems := []*kv.Elem[kv.OP]{
			{Op: kv.Del, Key: store.ZSetMemberKey(key, []byte(popped.Member))},
			{Op: kv.Del, Key: store.ZSetScoreKey(key, popped.Score, []byte(popped.Member))},
			{Op: kv.Put, Key: store.ZSetMetaDeltaKey(key, commitTS, 0), Value: deltaVal},
		}
		_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  normalizeStartTS(readTS),
			CommitTS: commitTS,
			Elems:    elems,
		})
		return cockerrors.WithStack(dispatchErr)
	}
	// Legacy blob: write back all remaining entries.
	payload, err := marshalZSetValue(redisZSetValue{Entries: remaining})
	if err != nil {
		return err
	}
	return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisZSetKey(key), Value: payload},
	})
}

func (r *RedisServer) bzpopmin(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	timeoutSeconds, err := strconv.ParseFloat(string(cmd.Args[len(cmd.Args)-1]), 64)
	if err != nil || timeoutSeconds < 0 {
		conn.WriteError("ERR timeout is not a float or out of range")
		return
	}

	// timeout=0 means infinite wait in Redis; cap at redisDispatchTimeout to prevent goroutine leak.
	if timeoutSeconds == 0 {
		timeoutSeconds = redisDispatchTimeout.Seconds()
	}
	deadline := time.Now().Add(time.Duration(timeoutSeconds * float64(time.Second)))

	for {
		for _, key := range cmd.Args[1 : len(cmd.Args)-1] {
			result, err := r.tryBZPopMin(key)
			if err != nil {
				conn.WriteError(err.Error())
				return
			}
			if result == nil {
				continue
			}

			conn.WriteArray(redisTripletWidth)
			conn.WriteBulk(result.key)
			conn.WriteBulkString(result.entry.Member)
			conn.WriteBulkString(formatRedisFloat(result.entry.Score))
			return
		}

		if !time.Now().Before(deadline) {
			conn.WriteNull()
			return
		}
		time.Sleep(redisBusyPollBackoff)
	}
}

func (r *RedisServer) lpush(conn redcon.Conn, cmd redcon.Command) {
	r.listPushCmd(conn, cmd, r.listLPush, r.proxyLPush)
}

func (r *RedisServer) ltrim(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	start, err := parseInt(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	stop, err := parseInt(cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	if err := r.retryRedisWrite(ctx, func() error {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			return nil
		}
		if typ != redisTypeList {
			return wrongTypeError()
		}
		current, err := r.listValuesAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		s, e := normalizeRankRange(start, stop, len(current))
		trimmed := []string{}
		if e >= s {
			trimmed = append(trimmed, current[s:e+1]...)
		}
		return r.rewriteListTxn(ctx, cmd.Args[1], readTS, trimmed)
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteString("OK")
}

func (r *RedisServer) lindex(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	index, err := parseInt(cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteNull()
		return
	}
	if typ != redisTypeList {
		conn.WriteError(wrongTypeMessage)
		return
	}
	values, err := r.listValuesAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	idx := normalizeIndex(index, len(values))
	if idx < 0 {
		conn.WriteNull()
		return
	}
	conn.WriteBulkString(values[idx])
}

func parseXAddMaxLen(args [][]byte) (int, int, error) {
	argIndex := redisPairWidth
	if len(args) < 5 || !strings.EqualFold(string(args[argIndex]), "MAXLEN") {
		return 0, argIndex, nil
	}

	argIndex++
	if argIndex < len(args) && string(args[argIndex]) == "~" {
		argIndex++
	}
	if argIndex >= len(args) {
		return 0, 0, errors.New("ERR syntax error")
	}

	maxLen, err := strconv.Atoi(string(args[argIndex]))
	if err != nil || maxLen < 0 {
		return 0, 0, errors.New("ERR syntax error")
	}
	return maxLen, argIndex + 1, nil
}

func parseXAddFields(args [][]byte, argIndex int) ([]string, error) {
	if argIndex >= len(args) {
		return nil, errors.New("ERR syntax error")
	}
	if (len(args)-argIndex)%redisPairWidth != 0 {
		return nil, errors.New("ERR wrong number of arguments for 'XADD' command")
	}

	fields := make([]string, 0, len(args)-argIndex)
	for _, arg := range args[argIndex:] {
		fields = append(fields, string(arg))
	}
	return fields, nil
}

func parseXAddRequest(args [][]byte) (xaddRequest, error) {
	maxLen, argIndex, err := parseXAddMaxLen(args)
	if err != nil {
		return xaddRequest{}, err
	}
	if argIndex >= len(args) {
		return xaddRequest{}, errors.New("ERR syntax error")
	}
	fields, err := parseXAddFields(args, argIndex+1)
	if err != nil {
		return xaddRequest{}, err
	}
	return xaddRequest{maxLen: maxLen, id: string(args[argIndex]), fields: fields}, nil
}

func nextXAddID(stream redisStreamValue, requested string) (string, error) {
	if requested != "*" {
		requestedID, requestedValid := tryParseRedisStreamID(requested)
		if len(stream.Entries) > 0 && compareParsedRedisStreamID(
			requested,
			requestedID,
			requestedValid,
			stream.Entries[len(stream.Entries)-1].ID,
			stream.Entries[len(stream.Entries)-1].parsedID,
			stream.Entries[len(stream.Entries)-1].parsedIDValid,
		) <= 0 {
			return "", errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
		}
		return requested, nil
	}

	nextID := strconv.FormatInt(time.Now().UnixMilli(), 10) + "-0"
	nextParsedID, nextParsedValid := tryParseRedisStreamID(nextID)
	if len(stream.Entries) == 0 || compareParsedRedisStreamID(
		nextID,
		nextParsedID,
		nextParsedValid,
		stream.Entries[len(stream.Entries)-1].ID,
		stream.Entries[len(stream.Entries)-1].parsedID,
		stream.Entries[len(stream.Entries)-1].parsedIDValid,
	) > 0 {
		return nextID, nil
	}

	last := stream.Entries[len(stream.Entries)-1].parsedID
	return strconv.FormatUint(last.ms, 10) + "-" + strconv.FormatUint(last.seq+1, 10), nil
}

func (r *RedisServer) xadd(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	req, err := parseXAddRequest(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var id string
	if err := r.retryRedisWrite(ctx, func() error {
		id, err = r.xaddTxn(ctx, cmd.Args[1], req)
		return err
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteBulkString(id)
}

func (r *RedisServer) xaddTxn(ctx context.Context, key []byte, req xaddRequest) (string, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return "", err
	}
	if typ != redisTypeNone && typ != redisTypeStream {
		return "", wrongTypeError()
	}

	stream, err := r.loadStreamAt(context.Background(), key, readTS)
	if err != nil {
		return "", err
	}

	id, err := nextXAddID(stream, req.id)
	if err != nil {
		return "", err
	}

	stream.Entries = append(stream.Entries, newRedisStreamEntry(id, req.fields))
	if req.maxLen > 0 && len(stream.Entries) > req.maxLen {
		stream.Entries = append([]redisStreamEntry(nil), stream.Entries[len(stream.Entries)-req.maxLen:]...)
	}

	payload, err := marshalStreamValue(stream)
	if err != nil {
		return "", err
	}
	return id, r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisStreamKey(key), Value: payload},
	})
}

func parseXTrimMaxLen(args [][]byte) (int, error) {
	if !strings.EqualFold(string(args[2]), "MAXLEN") {
		return 0, errors.New("ERR syntax error")
	}

	argIndex := 3
	if argIndex < len(args) && (string(args[argIndex]) == "~" || string(args[argIndex]) == "=") {
		argIndex++
	}
	if argIndex != len(args)-1 {
		return 0, errors.New("ERR syntax error")
	}

	maxLen, err := strconv.Atoi(string(args[argIndex]))
	if err != nil || maxLen < 0 {
		return 0, errors.New("ERR syntax error")
	}
	return maxLen, nil
}

func (r *RedisServer) xtrim(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	maxLen, err := parseXTrimMaxLen(cmd.Args)
	if err != nil {
		conn.WriteError("ERR syntax error")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	var removed int
	if err := r.retryRedisWrite(ctx, func() error {
		var err error
		removed, err = r.xtrimTxn(ctx, cmd.Args[1], maxLen)
		return err
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(removed)
}

func (r *RedisServer) xtrimTxn(ctx context.Context, key []byte, maxLen int) (int, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	if typ == redisTypeNone {
		return 0, nil
	}
	if typ != redisTypeStream {
		return 0, wrongTypeError()
	}

	stream, err := r.loadStreamAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	if len(stream.Entries) <= maxLen {
		return 0, nil
	}

	removed := len(stream.Entries) - maxLen
	stream.Entries = append([]redisStreamEntry(nil), stream.Entries[removed:]...)

	if len(stream.Entries) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return 0, err
		}
		return removed, r.dispatchElems(ctx, true, readTS, elems)
	}

	payload, err := marshalStreamValue(redisStreamValue{Entries: stream.Entries})
	if err != nil {
		return 0, err
	}
	return removed, r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisStreamKey(key), Value: payload},
	})
}

func (r *RedisServer) xrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.rangeStream(conn, cmd, false)
}

func (r *RedisServer) xrevrange(conn redcon.Conn, cmd redcon.Command) {
	if r.proxyToLeader(conn, cmd, cmd.Args[1]) {
		return
	}
	r.rangeStream(conn, cmd, true)
}

func parseXReadCountArg(args [][]byte, index int) (int, error) {
	if index+1 >= len(args) {
		return 0, errors.New("ERR syntax error")
	}
	count, err := strconv.Atoi(string(args[index+1]))
	if err != nil || count <= 0 {
		return 0, errors.New("ERR syntax error")
	}
	return count, nil
}

func parseXReadBlockArg(args [][]byte, index int) (time.Duration, error) {
	if index+1 >= len(args) {
		return 0, errors.New("ERR syntax error")
	}
	ms, err := strconv.Atoi(string(args[index+1]))
	if err != nil || ms < 0 {
		return 0, errors.New("ERR syntax error")
	}
	return time.Duration(ms) * time.Millisecond, nil
}

func parseXReadOptions(args [][]byte) (xreadOptions, error) {
	opts := xreadOptions{count: -1, streamsIndex: -1}
	for i := 1; i < len(args); {
		next, done, err := parseXReadOption(&opts, args, i)
		if err != nil {
			return xreadOptions{}, err
		}
		if done {
			return opts, nil
		}
		i = next
	}
	return opts, nil
}

func parseXReadOption(opts *xreadOptions, args [][]byte, i int) (int, bool, error) {
	switch strings.ToUpper(string(args[i])) {
	case redisKeywordCount:
		count, err := parseXReadCountArg(args, i)
		if err != nil {
			return 0, false, err
		}
		opts.count = count
		return i + redisPairWidth, false, nil
	case "BLOCK":
		block, err := parseXReadBlockArg(args, i)
		if err != nil {
			return 0, false, err
		}
		opts.block = block
		return i + redisPairWidth, false, nil
	case "STREAMS":
		opts.streamsIndex = i + 1
		return len(args), true, nil
	default:
		return 0, false, errors.New("ERR syntax error")
	}
}

func splitXReadStreams(args [][]byte, streamsIndex int) ([][]byte, []string, error) {
	if streamsIndex < 0 || streamsIndex >= len(args) {
		return nil, nil, errors.New("ERR syntax error")
	}
	remaining := len(args) - streamsIndex
	if remaining%redisPairWidth != 0 {
		return nil, nil, errors.New("ERR syntax error")
	}

	streamCount := remaining / redisPairWidth
	keys := make([][]byte, streamCount)
	afterIDs := make([]string, streamCount)
	for i := range streamCount {
		keys[i] = args[streamsIndex+i]
		afterIDs[i] = string(args[streamsIndex+streamCount+i])
	}
	return keys, afterIDs, nil
}

func parseXReadRequest(args [][]byte) (xreadRequest, error) {
	opts, err := parseXReadOptions(args)
	if err != nil {
		return xreadRequest{}, err
	}
	keys, afterIDs, err := splitXReadStreams(args, opts.streamsIndex)
	if err != nil {
		return xreadRequest{}, err
	}
	return xreadRequest{block: opts.block, count: opts.count, keys: keys, afterIDs: afterIDs}, nil
}

func (r *RedisServer) resolveXReadAfterIDs(req *xreadRequest) error {
	for i, afterID := range req.afterIDs {
		if afterID != "$" {
			continue
		}

		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), req.keys[i], readTS)
		if err != nil {
			return err
		}
		if typ == redisTypeNone {
			req.afterIDs[i] = "0-0"
			continue
		}
		if typ != redisTypeStream {
			return wrongTypeError()
		}

		stream, err := r.loadStreamAt(context.Background(), req.keys[i], readTS)
		if err != nil {
			return err
		}
		if len(stream.Entries) == 0 {
			req.afterIDs[i] = "0-0"
			continue
		}
		req.afterIDs[i] = stream.Entries[len(stream.Entries)-1].ID
	}
	return nil
}

func selectXReadEntries(entries []redisStreamEntry, afterID string, count int) []redisStreamEntry {
	afterParsedID, afterParsedValid := tryParseRedisStreamID(afterID)
	start := sort.Search(len(entries), func(i int) bool {
		return entries[i].compareID(afterID, afterParsedID, afterParsedValid) > 0
	})
	if start >= len(entries) {
		return nil
	}
	end := len(entries)
	if count > 0 && start+count < end {
		end = start + count
	}
	return entries[start:end]
}

func (r *RedisServer) xreadOnce(req xreadRequest) ([]xreadResult, error) {
	results := make([]xreadResult, 0, len(req.keys))
	for i, key := range req.keys {
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), key, readTS)
		if err != nil {
			return nil, err
		}
		if typ == redisTypeNone {
			continue
		}
		if typ != redisTypeStream {
			return nil, wrongTypeError()
		}

		stream, err := r.loadStreamAt(context.Background(), key, readTS)
		if err != nil {
			return nil, err
		}
		selected := selectXReadEntries(stream.Entries, req.afterIDs[i], req.count)
		if len(selected) > 0 {
			results = append(results, xreadResult{key: key, entries: selected})
		}
	}
	return results, nil
}

func writeStreamEntry(conn redcon.Conn, entry redisStreamEntry) {
	conn.WriteArray(redisPairWidth)
	conn.WriteBulkString(entry.ID)
	conn.WriteArray(len(entry.Fields))
	for _, field := range entry.Fields {
		conn.WriteBulkString(field)
	}
}

func writeStreamEntries(conn redcon.Conn, entries []redisStreamEntry) {
	conn.WriteArray(len(entries))
	for _, entry := range entries {
		writeStreamEntry(conn, entry)
	}
}

func writeXReadResults(conn redcon.Conn, results []xreadResult) {
	conn.WriteArray(len(results))
	for _, result := range results {
		conn.WriteArray(redisPairWidth)
		conn.WriteBulk(result.key)
		writeStreamEntries(conn, result.entries)
	}
}

func (r *RedisServer) xread(conn redcon.Conn, cmd redcon.Command) {
	req, err := parseXReadRequest(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if err := r.resolveXReadAfterIDs(&req); err != nil {
		conn.WriteError(err.Error())
		return
	}

	blockDuration := req.block
	// block=0 means infinite wait in Redis; cap at redisDispatchTimeout to prevent goroutine leak.
	if blockDuration == 0 {
		blockDuration = redisDispatchTimeout
	}
	deadline := time.Now().Add(blockDuration)

	for {
		results, err := r.xreadOnce(req)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if len(results) > 0 {
			writeXReadResults(conn, results)
			return
		}

		if !time.Now().Before(deadline) {
			conn.WriteNull()
			return
		}
		time.Sleep(redisBusyPollBackoff)
	}
}

func (r *RedisServer) xlen(conn redcon.Conn, cmd redcon.Command) {
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
	if typ != redisTypeStream {
		conn.WriteError(wrongTypeMessage)
		return
	}
	stream, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(len(stream.Entries))
}

func parseRangeStreamCount(args [][]byte) (int, error) {
	count := -1
	for i := 4; i < len(args); i += redisPairWidth {
		if i+1 >= len(args) || !strings.EqualFold(string(args[i]), redisKeywordCount) {
			return 0, errors.New("ERR syntax error")
		}
		nextCount, err := strconv.Atoi(string(args[i+1]))
		if err != nil || nextCount < 0 {
			return 0, errors.New("ERR syntax error")
		}
		count = nextCount
	}
	return count, nil
}

func streamEntryMatchesRange(entryID, startRaw, endRaw string, reverse bool) bool {
	if reverse {
		return streamWithinUpper(entryID, startRaw) && streamWithinLower(entryID, endRaw)
	}
	return streamWithinLower(entryID, startRaw) && streamWithinUpper(entryID, endRaw)
}

func selectForwardStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, count int) []redisStreamEntry {
	selected := make([]redisStreamEntry, 0, len(entries))
	for _, entry := range entries {
		if !streamEntryMatchesRange(entry.ID, startRaw, endRaw, false) {
			continue
		}
		selected = append(selected, entry)
		if count >= 0 && len(selected) >= count {
			break
		}
	}
	return selected
}

func selectReverseStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, count int) []redisStreamEntry {
	selected := make([]redisStreamEntry, 0, len(entries))
	for i := len(entries) - 1; i >= 0; i-- {
		if !streamEntryMatchesRange(entries[i].ID, startRaw, endRaw, true) {
			continue
		}
		selected = append(selected, entries[i])
		if count >= 0 && len(selected) >= count {
			break
		}
	}
	return selected
}

func selectStreamRangeEntries(entries []redisStreamEntry, startRaw, endRaw string, reverse bool, count int) []redisStreamEntry {
	if reverse {
		return selectReverseStreamRangeEntries(entries, startRaw, endRaw, count)
	}
	return selectForwardStreamRangeEntries(entries, startRaw, endRaw, count)
}

func (r *RedisServer) rangeStream(conn redcon.Conn, cmd redcon.Command, reverse bool) {
	count, err := parseRangeStreamCount(cmd.Args)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if typ == redisTypeNone {
		conn.WriteArray(0)
		return
	}
	if typ != redisTypeStream {
		conn.WriteError(wrongTypeMessage)
		return
	}

	stream, err := r.loadStreamAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	selected := selectStreamRangeEntries(stream.Entries, string(cmd.Args[2]), string(cmd.Args[3]), reverse, count)
	writeStreamEntries(conn, selected)
}

func streamWithinLower(entryID, raw string) bool {
	if raw == "-" {
		return true
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	cmp := compareRedisStreamID(entryID, raw)
	if exclusive {
		return cmp > 0
	}
	return cmp >= 0
}

func streamWithinUpper(entryID, raw string) bool {
	if raw == "+" {
		return true
	}
	exclusive := strings.HasPrefix(raw, "(")
	if exclusive {
		raw = raw[1:]
	}
	cmp := compareRedisStreamID(entryID, raw)
	if exclusive {
		return cmp < 0
	}
	return cmp <= 0
}
