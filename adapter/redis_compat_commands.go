package adapter

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	conn.WriteBulkString(strings.Join([]string{
		"# Server",
		"redis_version:7.2.0",
		"loading:0",
		"role:master",
		"",
	}, "\r\n"))
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
		raw, err := r.readRedisStringAt(key, readTS)
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

	var result int
	if err := r.retryRedisWrite(ctx, func() error {
		readTS, eligible, err := r.prepareExpire(cmd.Args[1], nxOnly)
		if err != nil {
			return err
		}
		if !eligible {
			result = 0
			return nil
		}
		if ttl <= 0 {
			elems, existed, err := r.deleteLogicalKeyElems(ctx, cmd.Args[1], readTS)
			if err != nil {
				return err
			}
			if err := r.dispatchElems(ctx, true, readTS, elems); err != nil {
				return err
			}
			if existed {
				result = 1
			} else {
				result = 0
			}
			return nil
		}
		expireAt := time.Now().Add(time.Duration(ttl) * unit)
		result = 1
		return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisTTLKey(cmd.Args[1]), Value: encodeRedisTTL(expireAt)},
		})
	}); err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(result)
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
				return fmt.Errorf("dispatch del_prefix %q: %w", prefix, err)
			}
		}
		return nil
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

		startTS := ensureValidStartTS(readTS, r.coordinator.Clock())
		commitTS := r.coordinator.Clock().Next()
		elems := make([]*kv.Elem[kv.OP], 0, len(members)+setWideColOverhead)

		migrationElems, migErr := r.buildSetLegacyMigrationElems(ctx, key, readTS)
		if migErr != nil {
			return migErr
		}
		elems = append(elems, migrationElems...)

		changed = 0
		lenDelta := int64(0)
		for _, member := range members {
			memberKey := store.SetMemberKey(key, member)
			exists, existsErr := r.store.ExistsAt(ctx, memberKey, readTS)
			if existsErr != nil {
				return cockerrors.WithStack(existsErr)
			}
			var c int
			var d int64
			elems, c, d = applySetMemberMutation(elems, memberKey, exists, add)
			changed += c
			lenDelta += d
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
			StartTS:  startTS,
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
	if typ != redisTypeSet {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadSetAt(context.Background(), setKind, cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if slices.Contains(value.Members, string(cmd.Args[2])) {
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

// buildHashFieldElems iterates over field-value pairs in args, records whether each field is
// new vs. existing, appends Put operations to elems, and returns the updated elems and new-field count.
func (r *RedisServer) buildHashFieldElems(ctx context.Context, key []byte, args [][]byte, readTS uint64, elems []*kv.Elem[kv.OP]) ([]*kv.Elem[kv.OP], int, error) {
	newFields := 0
	for i := 0; i < len(args); i += redisPairWidth {
		field := args[i]
		value := args[i+1]
		fieldKey := store.HashFieldKey(key, field)
		exists, existsErr := r.store.ExistsAt(ctx, fieldKey, readTS)
		if existsErr != nil {
			return nil, 0, cockerrors.WithStack(existsErr)
		}
		if !exists {
			newFields++
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: fieldKey, Value: value})
	}
	return elems, newFields, nil
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

		startTS := ensureValidStartTS(readTS, r.coordinator.Clock())
		commitTS := r.coordinator.Clock().Next()
		elems := make([]*kv.Elem[kv.OP], 0, len(args)/redisPairWidth+setWideColOverhead)

		// Atomically migrate any legacy blob on first wide-column write.
		migrationElems, err := r.buildHashLegacyMigrationElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		elems = append(elems, migrationElems...)

		var newFields int
		elems, newFields, err = r.buildHashFieldElems(ctx, key, args, readTS, elems)
		if err != nil {
			return err
		}
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
			StartTS:  startTS,
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
	if typ != redisTypeHash {
		conn.WriteError(wrongTypeMessage)
		return
	}

	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	fieldValue, ok := value[string(cmd.Args[2])]
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
	startTS := ensureValidStartTS(readTS, r.coordinator.Clock())
	commitTS := r.coordinator.Clock().Next()
	elems := make([]*kv.Elem[kv.OP], 0, len(fields)+1)
	removed := 0
	for _, field := range fields {
		fieldKey := store.HashFieldKey(key, field)
		exists, existsErr := r.store.ExistsAt(ctx, fieldKey, readTS)
		if existsErr != nil {
			return 0, cockerrors.WithStack(existsErr)
		}
		if exists {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: fieldKey})
			removed++
		}
	}
	if removed == 0 {
		return 0, nil
	}
	deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: int64(-removed)})
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaDeltaKey(key, commitTS, 0),
		Value: deltaVal,
	})
	_, dispatchErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems:    elems,
	})
	return removed, cockerrors.WithStack(dispatchErr)
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

	value, err := r.loadHashAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if _, ok := value[string(cmd.Args[2])]; ok {
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
func (r *RedisServer) hincrbyWithMigration(ctx context.Context, key, fieldKey []byte, readTS, startTS, commitTS uint64, current int64, isNewField bool, increment int64) (int64, error) {
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
		StartTS:  startTS,
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

	startTS := ensureValidStartTS(readTS, r.coordinator.Clock())
	commitTS := r.coordinator.Clock().Next()
	fieldKey := store.HashFieldKey(key, field)

	current, isNewField, legacyValue, err := r.readHashFieldInt(ctx, key, field, readTS)
	if err != nil {
		return 0, err
	}

	// If a legacy blob exists, migrate it atomically with the increment.
	if len(legacyValue) > 0 {
		return r.hincrbyWithMigration(ctx, key, fieldKey, readTS, startTS, commitTS, current, isNewField, increment)
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
		StartTS:  startTS,
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
		if typ == redisTypeString {
			raw, err := r.readRedisStringAt(cmd.Args[1], readTS)
			if err != nil {
				return err
			}
			current, err = strconv.ParseInt(string(raw), 10, 64)
			if err != nil {
				return fmt.Errorf("ERR value is not an integer or out of range")
			}
		}
		current++

		return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisStrKey(cmd.Args[1]), Value: []byte(strconv.FormatInt(current, 10))},
			{Op: kv.Del, Key: redisTTLKey(cmd.Args[1])},
		})
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

func (r *RedisServer) zaddTxn(ctx context.Context, key []byte, flags zaddFlags, pairs []zaddPair) (int, error) {
	readTS := r.readTS()
	typ, err := r.keyTypeAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		return 0, wrongTypeError()
	}
	value, _, err := r.loadZSetAt(context.Background(), key, readTS)
	if err != nil {
		return 0, err
	}
	members := zsetEntriesToMap(value.Entries)
	added := 0
	for _, p := range pairs {
		old, exists := members[p.member]
		if !flags.allows(exists, old, p.score) {
			continue
		}
		if !exists {
			added++
		}
		members[p.member] = p.score
	}
	value.Entries = zsetMapToEntries(members)
	payload, err := marshalZSetValue(value)
	if err != nil {
		return 0, err
	}
	return added, r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: redisZSetKey(key), Value: payload},
	})
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
		readTS := r.readTS()
		typ, err := r.keyTypeAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		if typ != redisTypeNone && typ != redisTypeZSet {
			return wrongTypeError()
		}
		value, _, err := r.loadZSetAt(context.Background(), cmd.Args[1], readTS)
		if err != nil {
			return err
		}
		members := zsetEntriesToMap(value.Entries)
		member := string(cmd.Args[3])
		members[member] += increment
		newScore = members[member]
		value.Entries = zsetMapToEntries(members)
		payload, err := marshalZSetValue(value)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: redisZSetKey(cmd.Args[1]), Value: payload},
		})
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
		if err := r.persistBZPopMinResult(ctx, key, readTS, remaining); err != nil {
			return err
		}
		result = &bzpopminResult{key: key, entry: popped}
		return nil
	})
	return result, err
}

func (r *RedisServer) persistBZPopMinResult(ctx context.Context, key []byte, readTS uint64, remaining []redisZSetEntry) error {
	if len(remaining) == 0 {
		elems, _, err := r.deleteLogicalKeyElems(ctx, key, readTS)
		if err != nil {
			return err
		}
		return r.dispatchElems(ctx, true, readTS, elems)
	}
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
