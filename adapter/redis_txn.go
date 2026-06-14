package adapter

import (
	"bytes"
	"context"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

var redisTxnKeyPrefix = []byte("!txn|")

type txnCommandHandler func(*txnContext, redcon.Command) (redisResult, error)

var txnApplyHandlers = map[string]txnCommandHandler{
	cmdSet:     (*txnContext).applySet,
	cmdDel:     (*txnContext).applyDel,
	cmdGet:     (*txnContext).applyGet,
	cmdExists:  (*txnContext).applyExists,
	cmdRPush:   (*txnContext).applyRPush,
	cmdLRange:  (*txnContext).applyLRange,
	cmdZIncrBy: (*txnContext).applyZIncrBy,
	cmdExpire:  (*txnContext).applyExpireSeconds,
	cmdPExpire: (*txnContext).applyExpireMilliseconds,
}

// MULTI/EXEC/DISCARD handling
func (r *RedisServer) multi(conn redcon.Conn, _ redcon.Command) {
	state := getConnState(conn)
	if state.inTxn {
		conn.WriteError("ERR MULTI calls can not be nested")
		return
	}
	state.inTxn = true
	state.queue = nil
	conn.WriteString("OK")
}

func (r *RedisServer) discard(conn redcon.Conn, _ redcon.Command) {
	state := getConnState(conn)
	if !state.inTxn {
		conn.WriteError("ERR DISCARD without MULTI")
		return
	}
	state.inTxn = false
	state.queue = nil
	conn.WriteString("OK")
}

func (r *RedisServer) exec(conn redcon.Conn, _ redcon.Command) {
	state := getConnState(conn)
	if !state.inTxn {
		conn.WriteError("ERR EXEC without MULTI")
		return
	}

	queue := state.queue
	state.inTxn = false
	state.queue = nil

	// Always execute MULTI/EXEC on the leader so that reads and writes within
	// the transaction see consistent, up-to-date data. Serving transactions
	// on followers risks reading stale MVCC state and producing write cycles.
	if !r.coordinator.IsLeader() {
		r.proxyTransactionToLeader(conn, queue)
		return
	}

	results, err := r.runTransaction(queue)
	if err != nil {
		writeRedisError(conn, err)
		return
	}

	r.writeResults(conn, results)
}

type txnValue struct {
	raw     []byte
	ttl     *time.Time
	deleted bool
	dirty   bool
	loaded  bool
}

type txnContext struct {
	server *RedisServer
	// ctx is the per-EXEC dispatch context (redisDispatchTimeout-bounded
	// at the call site in runTransaction). Plumbed through so reads
	// inside the EXEC such as load() → readValueAt() respect the
	// caller's deadline rather than falling back to handlerContext +
	// the verifyLeaderEngineCtx safety net.
	ctx        context.Context //nolint:containedctx // EXEC is a long-lived value type that wraps a single client command, ctx must travel with it.
	working    map[string]*txnValue
	listStates map[string]*listTxnState
	zsetStates map[string]*zsetTxnState
	ttlStates  map[string]*ttlTxnState
	readKeys   map[string][]byte
	// streamDeletions tracks user keys whose stream wide-column layout must
	// be tombstoned on commit: the !stream|meta|<key> record plus every
	// !stream|entry|<key><ID> row. stageKeyDeletion seeds this (MULTI/EXEC
	// DEL / EXPIRE 0) so migrated streams are properly removed rather than
	// leaking entry keys past the DEL's apparent success.
	streamDeletions map[string][]byte
	startTS         uint64
}

type listTxnState struct {
	meta           store.ListMeta
	metaExists     bool
	appends        [][]byte
	deleted        bool
	purge          bool
	purgeMeta      store.ListMeta
	existingDeltas [][]byte // delta key bytes present at load time; deleted on purge/delete
}

type zsetTxnState struct {
	members     map[string]float64 // current (potentially modified) state
	origMembers map[string]float64 // original state at load time (for wide-column diff)
	isWide      bool               // true if loaded from wide-column !zs|mem| storage
	exists      bool
	dirty       bool
}

type ttlTxnState struct {
	value *time.Time
	dirty bool
}

func stageListDelete(st *listTxnState) {
	if st == nil {
		return
	}
	if st.metaExists {
		st.purge = true
		st.purgeMeta = st.meta
	}
	st.deleted = true
	st.appends = nil
}

func (t *txnContext) trackReadKey(key []byte) {
	if len(key) == 0 {
		return
	}
	k := string(key)
	if _, ok := t.readKeys[k]; ok {
		return
	}
	t.readKeys[k] = bytes.Clone(key)
}

func (t *txnContext) trackTypeReadKeys(key []byte) {
	for _, readKey := range [][]byte{
		listMetaKey(key),
		redisHashKey(key),
		redisSetKey(key),
		redisZSetKey(key),
		redisStreamKey(key),      // legacy single-blob stream key
		store.StreamMetaKey(key), // post-migration wide-column stream meta
		redisHLLKey(key),
		redisStrKey(key),
		key, // legacy bare key for fallback reads
	} {
		t.trackReadKey(readKey)
	}
}

func (t *txnContext) ctxOrBackground() context.Context {
	if t.ctx != nil {
		return t.ctx
	}
	return context.Background()
}

func (t *txnContext) load(key []byte) (*txnValue, error) {
	// If the key is already an internal key (e.g., !redis|hash|...,
	// !lst|..., !txn|..., !ddb|..., !s3|..., !dist|...), use it as-is.
	// Otherwise, it's a bare user key for a string value — prefix it.
	storageKey := key
	if !isKnownInternalKey(key) {
		storageKey = redisStrKey(key)
	}
	k := string(storageKey)
	if tv, ok := t.working[k]; ok {
		return tv, nil
	}
	t.trackReadKey(storageKey)
	if !isKnownInternalKey(key) {
		// Track the bare key too for conflict detection on legacy fallback reads.
		t.trackReadKey(key)
	}
	tv := &txnValue{}
	var val []byte
	if !isKnownInternalKey(key) {
		// For bare user string keys, use the fallback-aware reader.
		var (
			err error
			ttl *time.Time
		)
		val, ttl, err = t.server.readRedisStringAt(key, t.startTS)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			return nil, errors.WithStack(err)
		}
		tv.ttl = ttl
	} else {
		var err error
		val, err = t.server.readValueAt(t.ctxOrBackground(), storageKey, t.startTS)
		if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
			return nil, errors.WithStack(err)
		}
	}
	tv.raw = val
	tv.loaded = true
	t.working[k] = tv
	return tv, nil
}

func (t *txnContext) loadListState(key []byte) (*listTxnState, error) {
	k := string(key)
	if st, ok := t.listStates[k]; ok {
		return st, nil
	}
	ctx := t.ctxOrBackground()
	meta, exists, err := t.server.resolveListMeta(ctx, key, t.startTS)
	if err != nil {
		return nil, err
	}

	// Capture existing delta keys so they can be deleted if the list is later
	// purged or deleted within this transaction. Scan one extra item to detect
	// truncation: if >MaxDeltaScanLimit deltas exist the transaction cannot
	// safely enumerate all of them for deletion, so we return ErrDeltaScanTruncated
	// and let the caller retry after the background compactor has caught up.
	deltaPrefix := store.ListMetaDeltaScanPrefix(key)
	deltaEnd := store.PrefixScanEnd(deltaPrefix)
	deltaKVs, err := t.server.store.ScanAt(ctx, deltaPrefix, deltaEnd, store.MaxDeltaScanLimit+1, t.startTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if len(deltaKVs) > store.MaxDeltaScanLimit {
		return nil, ErrDeltaScanTruncated
	}
	existingDeltas := make([][]byte, 0, len(deltaKVs))
	for _, kv := range deltaKVs {
		existingDeltas = append(existingDeltas, kv.Key)
	}

	st := &listTxnState{
		meta:           meta,
		metaExists:     exists,
		appends:        [][]byte{},
		existingDeltas: existingDeltas,
	}
	t.listStates[k] = st

	// Track the list-item key at the current tail (and the position before the
	// head) so that concurrent RPUSH/LPUSH operations—which write to exactly
	// these positions—trigger a read-write conflict and force a retry.
	// Without this, a MULTI transaction that reads a list via LRANGE can commit
	// with a stale snapshot while a concurrent RPUSH commits a new item,
	// forming an anti-dependency (G2-item) cycle.
	// The base meta key (listMetaKey) is intentionally NOT tracked here: the
	// Delta scheme allows the DeltaCompactor to rewrite it without conflicting
	// with ongoing push/read transactions (see TestRedisTxnValidateReadSet_ListMetaUpdateNoConflict).
	t.trackReadKey(listItemKey(key, meta.Head+meta.Len)) // next RPUSH target
	if meta.Head > math.MinInt64 {
		t.trackReadKey(listItemKey(key, meta.Head-1)) // next LPUSH target
	}

	return st, nil
}

func (t *txnContext) listLength(st *listTxnState) int64 {
	return st.meta.Len + int64(len(st.appends))
}

func (t *txnContext) loadZSetState(key []byte) (*zsetTxnState, error) {
	k := string(key)
	if st, ok := t.zsetStates[k]; ok {
		return st, nil
	}
	t.trackReadKey(redisZSetKey(key))
	// Check TTL: treat expired keys as non-existent.
	ttlSt, err := t.loadTTLState(key)
	if err != nil {
		return nil, err
	}
	if ttlSt.value != nil && !ttlSt.value.After(time.Now()) {
		st := &zsetTxnState{
			members:     map[string]float64{},
			origMembers: map[string]float64{},
			exists:      false,
		}
		t.zsetStates[k] = st
		return st, nil
	}

	// Detect wide-column storage by probing the !zs|mem| prefix.
	ctx := t.ctxOrBackground()
	memberPrefix := store.ZSetMemberScanPrefix(key)
	memberEnd := store.PrefixScanEnd(memberPrefix)
	probeKVs, probeErr := t.server.store.ScanAt(ctx, memberPrefix, memberEnd, 1, t.startTS)
	if probeErr != nil {
		return nil, errors.WithStack(probeErr)
	}
	isWide := len(probeKVs) > 0

	value, exists, err := t.server.loadZSetAt(ctx, key, t.startTS)
	if err != nil {
		return nil, err
	}
	members := zsetEntriesToMap(value.Entries)
	// Snapshot the original members for wide-column diff at commit time.
	origMembers := make(map[string]float64, len(members))
	for m, s := range members {
		origMembers[m] = s
	}
	st := &zsetTxnState{
		members:     members,
		origMembers: origMembers,
		isWide:      isWide,
		exists:      exists,
	}
	t.zsetStates[k] = st
	return st, nil
}

func (t *txnContext) loadTTLState(key []byte) (*ttlTxnState, error) {
	k := string(key)
	if st, ok := t.ttlStates[k]; ok {
		return st, nil
	}
	value, err := t.server.ttlAt(t.ctxOrBackground(), key, t.startTS)
	if err != nil {
		return nil, err
	}
	st := &ttlTxnState{value: value}
	t.ttlStates[k] = st
	return st, nil
}

func (t *txnContext) stagedKeyType(key []byte) (redisValueType, error) {
	k := string(key)
	if typ, ok := t.stagedZSetType(k); ok {
		return typ, nil
	}
	if typ, ok := t.stagedListType(k); ok {
		return typ, nil
	}
	if typ, ok := t.stagedStringType(k); ok {
		return typ, nil
	}
	t.trackTypeReadKeys(key)
	return t.server.keyTypeAt(t.ctxOrBackground(), key, t.startTS)
}

func (t *txnContext) stagedZSetType(key string) (redisValueType, bool) {
	st, ok := t.zsetStates[key]
	if !ok || (!st.dirty && !st.exists) {
		return redisTypeNone, false
	}
	if len(st.members) == 0 {
		return redisTypeNone, true
	}
	return redisTypeZSet, true
}

func (t *txnContext) stagedListType(key string) (redisValueType, bool) {
	st, ok := t.listStates[key]
	if !ok {
		return redisTypeNone, false
	}
	if st.deleted {
		return redisTypeNone, true
	}
	if st.metaExists || len(st.appends) > 0 {
		return redisTypeList, true
	}
	return redisTypeNone, false
}

func (t *txnContext) stagedStringType(key string) (redisValueType, bool) {
	tv, ok := t.working[string(redisStrKey([]byte(key)))]
	if !ok {
		return redisTypeNone, false
	}
	if tv.deleted || tv.raw == nil {
		return redisTypeNone, true
	}
	return redisTypeString, true
}

func (t *txnContext) apply(cmd redcon.Command) (redisResult, error) {
	handler, ok := txnApplyHandlers[strings.ToUpper(string(cmd.Args[0]))]
	if !ok {
		return redisResult{}, errors.WithStack(errors.Newf("ERR unsupported command '%s'", cmd.Args[0]))
	}
	return handler(t, cmd)
}

func (t *txnContext) applyExpireSeconds(cmd redcon.Command) (redisResult, error) {
	return t.applyExpire(cmd, time.Second)
}

func (t *txnContext) applyExpireMilliseconds(cmd redcon.Command) (redisResult, error) {
	return t.applyExpire(cmd, time.Millisecond)
}

func (t *txnContext) applySet(cmd redcon.Command) (redisResult, error) {
	if isList, err := t.server.isListKeyAt(t.ctxOrBackground(), cmd.Args[1], t.startTS); err != nil {
		return redisResult{}, err
	} else if isList {
		return redisResult{typ: resultError, err: errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")}, nil
	}

	opts, err := parseRedisSetOptions(cmd.Args[3:], time.Now())
	if err != nil {
		return redisResult{}, err
	}

	// NX/XX: skip the write if the key-existence condition is not met.
	blocked, res, err := t.applySetCondition(cmd.Args[1], opts)
	if err != nil {
		return redisResult{}, err
	}
	if blocked {
		return res, nil
	}

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}

	var oldValue []byte
	if opts.returnOld && !tv.deleted {
		oldValue = tv.raw
	}

	tv.raw = cmd.Args[2]
	tv.deleted = false
	tv.dirty = true

	// Always update TTL state: EX/PX sets a new expiry; a plain SET clears it
	// (opts.ttl == nil → nil stored → PERSIST semantics, matching Redis behaviour).
	if err := t.applySetTTL(cmd.Args[1], opts.ttl); err != nil {
		return redisResult{}, err
	}

	return applySetResult(opts, oldValue), nil
}

// applySetCondition checks NX/XX conditions.  Returns (blocked, result, err).
// blocked=true means the condition prevented the write; callers should return result.
// Returns (false, _, nil) immediately when no condition is set.
func (t *txnContext) applySetCondition(key []byte, opts redisSetOptions) (bool, redisResult, error) {
	if !opts.existsCond && !opts.missingCond {
		return false, redisResult{}, nil
	}
	typ, err := t.stagedKeyType(key)
	if err != nil {
		return false, redisResult{}, err
	}
	exists := typ != redisTypeNone
	if (opts.missingCond && exists) || (opts.existsCond && !exists) {
		return true, redisResult{typ: resultNil}, nil
	}
	return false, redisResult{}, nil
}

// applySetTTL stores the expiry in ttlStates so flushTTLToBuffer sends it to
// the TTLBuffer after a successful commit.
func (t *txnContext) applySetTTL(key []byte, expireAt *time.Time) error {
	ttlSt, err := t.loadTTLState(key)
	if err != nil {
		return err
	}
	ttlSt.value = expireAt
	ttlSt.dirty = true
	return nil
}

// applySetResult returns the appropriate redisResult for a completed SET.
func applySetResult(opts redisSetOptions, oldValue []byte) redisResult {
	if !opts.returnOld {
		return redisResult{typ: resultString, str: "OK"}
	}
	if oldValue == nil {
		return redisResult{typ: resultNil}
	}
	return redisResult{typ: resultBulk, bulk: oldValue}
}

func (t *txnContext) applyDel(cmd redcon.Command) (redisResult, error) {
	var deleted int64
	for _, key := range cmd.Args[1:] {
		typ, err := t.stagedKeyType(key)
		if err != nil {
			return redisResult{}, err
		}
		if typ == redisTypeNone {
			continue
		}
		if _, err := t.stageKeyDeletion(key); err != nil {
			return redisResult{}, err
		}
		deleted++
	}
	return redisResult{typ: resultInt, integer: deleted}, nil
}

func (t *txnContext) applyGet(cmd redcon.Command) (redisResult, error) {
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if isNonStringCollectionType(typ) {
		return redisResult{typ: resultError, err: wrongTypeError()}, nil
	}

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if tv.deleted || tv.raw == nil {
		return redisResult{typ: resultNil}, nil
	}
	return redisResult{typ: resultBulk, bulk: tv.raw}, nil
}

func (t *txnContext) applyExists(cmd redcon.Command) (redisResult, error) {
	var count int64
	for _, key := range cmd.Args[1:] {
		typ, err := t.stagedKeyType(key)
		if err != nil {
			return redisResult{}, err
		}
		if typ != redisTypeNone {
			count++
		}
	}
	return redisResult{typ: resultInt, integer: count}, nil
}

func (t *txnContext) applyRPush(cmd redcon.Command) (redisResult, error) {
	st, err := t.loadListState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if st.deleted {
		if st.metaExists {
			st.purge = true
			st.purgeMeta = st.meta
		}
		// DEL followed by RPUSH in the same transaction recreates the list.
		st.deleted = false
		st.metaExists = false
		st.meta = store.ListMeta{}
		st.appends = nil
	}

	for _, v := range cmd.Args[2:] {
		st.appends = append(st.appends, bytes.Clone(v))
	}

	return redisResult{typ: resultInt, integer: t.listLength(st)}, nil
}

func (t *txnContext) applyLRange(cmd redcon.Command) (redisResult, error) {
	st, err := t.loadListState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}

	s, e, err := parseRangeBounds(cmd.Args[2], cmd.Args[3], int(t.listLength(st)))
	if err != nil {
		return redisResult{}, err
	}
	if e < s {
		return redisResult{typ: resultArray, arr: []string{}}, nil
	}

	out, err := t.listRangeValues(cmd.Args[1], st, s, e)
	if err != nil {
		return redisResult{}, err
	}

	return redisResult{typ: resultArray, arr: out}, nil
}

func (t *txnContext) applyZIncrBy(cmd redcon.Command) (redisResult, error) {
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if typ != redisTypeNone && typ != redisTypeZSet {
		return redisResult{typ: resultError, err: wrongTypeError()}, nil
	}

	inc, err := strconv.ParseFloat(string(cmd.Args[2]), 64)
	if err != nil {
		return redisResult{}, errors.WithStack(err)
	}
	st, err := t.loadZSetState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	member := string(cmd.Args[3])
	st.members[member] += inc
	st.dirty = true
	return redisResult{typ: resultBulk, bulk: []byte(formatRedisFloat(st.members[member]))}, nil
}

func (t *txnContext) applyExpire(cmd redcon.Command, unit time.Duration) (redisResult, error) {
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if typ == redisTypeNone {
		return redisResult{typ: resultInt, integer: 0}, nil
	}

	ttl, err := strconv.ParseInt(string(cmd.Args[2]), 10, 64)
	if err != nil {
		return redisResult{}, errors.WithStack(err)
	}
	nxOnly, err := parseExpireNXOnly(cmd.Args[3:])
	if err != nil {
		return redisResult{}, err
	}

	state, err := t.loadTTLState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if nxOnly && hasActiveTTL(state.value, time.Now()) {
		return redisResult{typ: resultInt, integer: 0}, nil
	}

	if ttl <= 0 {
		return t.stageKeyDeletion(cmd.Args[1])
	}
	return t.applyPositiveExpire(cmd.Args[1], ttl, unit, typ, state)
}

func (t *txnContext) applyPositiveExpire(key []byte, ttl int64, unit time.Duration, typ redisValueType, state *ttlTxnState) (redisResult, error) {
	if ttl > math.MaxInt64/int64(unit) {
		return redisResult{}, errors.New("ERR invalid expire time in command")
	}
	expireAt := time.Now().Add(time.Duration(ttl) * unit)
	state.value = &expireAt
	state.dirty = true
	if typ == redisTypeString {
		plain, err := t.server.isPlainRedisString(t.ctxOrBackground(), key, t.startTS)
		if err != nil {
			return redisResult{}, err
		}
		if plain {
			return t.markStringDirty(key)
		}
		// HLL is reported as redisTypeString but stores its payload under
		// !redis|hll|<key>; keep TTL in the legacy scan index via buildTTLElems.
	}
	return redisResult{typ: resultInt, integer: 1}, nil
}

// markStringDirty loads the string value into the working set so that
// buildKeyElems will re-encode it with the updated embedded TTL.
func (t *txnContext) markStringDirty(key []byte) (redisResult, error) {
	tv, err := t.load(key)
	if err != nil {
		return redisResult{}, err
	}
	tv.dirty = true
	return redisResult{typ: resultInt, integer: 1}, nil
}

func (t *txnContext) stageKeyDeletion(key []byte) (redisResult, error) {
	// Mark the list for deletion.
	st, err := t.loadListState(key)
	if err != nil {
		return redisResult{}, err
	}
	stageListDelete(st)
	// Mark the string/main value for deletion.
	tv, err := t.load(key)
	if err != nil {
		return redisResult{}, err
	}
	tv.deleted = true
	tv.dirty = true
	// Mark TTL for deletion.
	ttlState, err := t.loadTTLState(key)
	if err != nil {
		return redisResult{}, err
	}
	ttlState.value = nil
	ttlState.dirty = true
	// Mark zset for deletion. Use empty map (not nil) so that subsequent
	// writes (e.g. ZINCRBY) in the same transaction can safely insert.
	zs, err := t.loadZSetState(key)
	if err != nil {
		return redisResult{}, err
	}
	zs.members = map[string]float64{}
	zs.exists = false
	zs.dirty = true
	// Mark hash, set, stream (legacy blob), and HLL internal keys for deletion.
	for _, internalKey := range [][]byte{
		redisHashKey(key),
		redisSetKey(key),
		redisStreamKey(key),
		redisHLLKey(key),
	} {
		iv, err := t.load(internalKey)
		if err != nil {
			return redisResult{}, err
		}
		iv.deleted = true
		iv.dirty = true
	}
	// Stage the wide-column stream cleanup: the !stream|meta| record and
	// every !stream|entry| row must also be tombstoned when the user deletes
	// a migrated stream via MULTI/EXEC DEL or EXPIRE 0. Without this step
	// the command would report success but leave rows behind, and a later
	// XLEN / XREAD would "resurrect" the stream. commit() expands this
	// entry into concrete Del elems by scanning the entry-key prefix.
	// The map is lazy-initialised so test fixtures that build a minimal
	// txnContext literal without this field still work.
	if t.streamDeletions == nil {
		t.streamDeletions = map[string][]byte{}
	}
	t.streamDeletions[string(key)] = bytes.Clone(key)
	t.trackReadKey(store.StreamMetaKey(key))
	// Mark legacy bare string key for deletion. We bypass load() here
	// because load() auto-prefixes bare keys to !redis|str|.
	// Track the bare key in the read set for conflict detection.
	t.trackReadKey(key)
	bareK := string(key)
	if _, ok := t.working[bareK]; !ok {
		t.working[bareK] = &txnValue{}
	}
	t.working[bareK].deleted = true
	t.working[bareK].dirty = true
	return redisResult{typ: resultInt, integer: 1}, nil
}

func parseRangeBounds(startRaw, endRaw []byte, total int) (int, int, error) {
	start, err := parseInt(startRaw)
	if err != nil {
		return 0, 0, err
	}
	end, err := parseInt(endRaw)
	if err != nil {
		return 0, 0, err
	}
	s, e := clampRange(start, end, total)
	return s, e, nil
}

func (t *txnContext) listRangeValues(key []byte, st *listTxnState, s, e int) ([]string, error) {
	persistedLen := int(st.meta.Len)
	ctx := t.ctxOrBackground()

	switch {
	case e < persistedLen:
		return t.server.fetchListRange(ctx, key, st.meta, int64(s), int64(e), t.startTS)
	case s >= persistedLen:
		return appendValues(st.appends, s-persistedLen, e-persistedLen), nil
	default:
		head, err := t.server.fetchListRange(ctx, key, st.meta, int64(s), int64(persistedLen-1), t.startTS)
		if err != nil {
			return nil, err
		}
		tail := appendValues(st.appends, 0, e-persistedLen)
		return append(head, tail...), nil
	}
}

func appendValues(buf [][]byte, start, end int) []string {
	out := make([]string, 0, end-start+1)
	for i := start; i <= end; i++ {
		out = append(out, string(buf[i]))
	}
	return out
}

func (t *txnContext) validateReadSet(ctx context.Context) error {
	for _, key := range t.readKeys {
		latestTS, exists, err := t.server.store.LatestCommitTS(ctx, key)
		if err != nil {
			return errors.WithStack(err)
		}
		if exists && latestTS > t.startTS {
			return errors.WithStack(store.NewWriteConflictError(key))
		}
	}
	return nil
}

// preparedTxnDispatch is the fully-assembled write set + read set + commit
// timestamp for a MULTI/EXEC transaction, ready to be passed to
// coordinator.Dispatch. Split out from commit() so the option-2 dedup
// path (runTransactionWithDedup) can intercept between prepare and
// dispatch — it needs to capture (elems, commitTS, readKeys) for a
// possible retry under PrevCommitTS without otherwise duplicating the
// commit-building logic. The owned ctx is the redisDispatchTimeout-
// bounded context the caller must run Dispatch under and Cancel after.
type preparedTxnDispatch struct {
	elems    []*kv.Elem[kv.OP]
	commitTS uint64
	readKeys [][]byte
	ctx      context.Context
	cancel   context.CancelFunc
}

// prepareDispatch builds everything Dispatch needs (elems, commitTS,
// readKeys, ctx) without actually calling Dispatch. Callers must always
// invoke `cancel()` on the returned prepared value once the dispatch
// attempt finishes (commit() does this via defer; the dedup path does it
// per retry iteration). When the transaction has no writes this returns
// a prepared value with empty `elems` and a no-op cancel — callers can
// check len(prepared.elems)==0 and skip the dispatch.
func (t *txnContext) prepareDispatch() (preparedTxnDispatch, error) {
	elems := t.buildKeyElems()

	// Pre-allocate commitTS so Delta keys can embed it in their bytes before
	// the coordinator assigns it during Dispatch.
	commitTS, err := t.server.coordinator.Clock().NextFenced()
	if err != nil {
		return preparedTxnDispatch{cancel: func() {}}, errors.Wrap(err, "redis txn commit: allocate commitTS")
	}
	listElems := t.buildListElems(commitTS)
	zsetElems, err := t.buildZSetElems(commitTS)
	if err != nil {
		return preparedTxnDispatch{cancel: func() {}}, err
	}
	// TTL elements: string keys have TTL embedded in value (buildKeyElems handles that),
	// non-string keys get a !redis|ttl| element written in the same transaction.
	ttlElems := t.buildTTLElems()

	// Derive a single redisDispatchTimeout-bounded context covering both
	// the stream-deletion scans (paginated ScanAt/ExistsAt over
	// StreamEntryScanPrefix) and the final Dispatch. The parent is the
	// txnContext's own ctx (the caller's dispatchCtx), not the server-
	// lifetime handlerContext, so an outer cancellation (client
	// disconnect, retryRedisWrite timeout) interrupts the prepare+dispatch
	// promptly instead of waiting the full redisDispatchTimeout. Symmetric
	// with the reuseCtx threading in runTransactionWithDedup. The nil-guard
	// falls back to handlerContext for callers that construct a txnContext
	// without setting ctx (test fixtures).
	parentCtx := t.ctx
	if parentCtx == nil {
		parentCtx = t.server.handlerContext()
	}
	ctx, cancel := context.WithTimeout(parentCtx, redisDispatchTimeout)

	streamElems, err := t.buildStreamDeletionElems(ctx)
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, err
	}

	elems = append(elems, listElems...)
	elems = append(elems, zsetElems...)
	elems = append(elems, ttlElems...)
	elems = append(elems, streamElems...)

	readKeys := make([][]byte, 0, len(t.readKeys))
	for _, k := range t.readKeys {
		readKeys = append(readKeys, k)
	}
	return preparedTxnDispatch{
		elems:    elems,
		commitTS: commitTS,
		readKeys: readKeys,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (t *txnContext) commit() error {
	prepared, err := t.prepareDispatch()
	if err != nil {
		return err
	}
	defer prepared.cancel()
	if len(prepared.elems) == 0 {
		return nil
	}
	group := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		Elems:    prepared.elems,
		StartTS:  t.startTS,
		CommitTS: prepared.commitTS,
		ReadKeys: prepared.readKeys,
	}
	if _, err := t.server.coordinator.Dispatch(prepared.ctx, group); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// stringValueAndTTLElem returns the encoded string value and an optional
// !redis|ttl| scan-index mutation for a string write. Dirty EXPIRE/PERSIST
// state takes priority; otherwise the TTL loaded with the value is preserved
// so commands like INCR or SETBIT inside MULTI/EXEC don't clear it. A dirty
// PERSIST emits a Del so the sweeper cannot later expire a persistent key.
func (t *txnContext) stringValueAndTTLElem(userKey []byte, tv *txnValue) ([]byte, *kv.Elem[kv.OP]) {
	ttl := tv.ttl
	ttlSt := t.ttlStates[string(userKey)]
	if ttlSt != nil && ttlSt.dirty {
		ttl = ttlSt.value
	}
	value := encodeRedisStr(tv.raw, ttl)
	if ttl != nil {
		return value, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(userKey), Value: encodeRedisTTL(*ttl)}
	}
	// ttl is nil: emit Del when there was a prior TTL (loaded or dirty-cleared)
	// so the sweeper cannot later expire a now-persistent key or hit a stale index.
	if tv.ttl != nil || (ttlSt != nil && ttlSt.dirty) {
		return value, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(userKey)}
	}
	return value, nil
}

func (t *txnContext) buildKeyElems() []*kv.Elem[kv.OP] {
	keys := make([]string, 0, len(t.working))
	for k := range t.working {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var elems []*kv.Elem[kv.OP]
	for _, k := range keys {
		tv := t.working[k]
		if !tv.dirty {
			continue
		}
		storageKey := []byte(k)
		if tv.deleted {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: storageKey})
			// Deleting a string anchor must also drop any stale !redis|ttl|
			// scan-index entry; buildTTLElems skips strings because it assumes
			// the inline-TTL path owns them.
			if bytes.HasPrefix(storageKey, []byte(redisStrPrefix)) {
				userKey := storageKey[len(redisStrPrefix):]
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(userKey)})
			}
			continue
		}
		value := tv.raw
		if bytes.HasPrefix(storageKey, []byte(redisStrPrefix)) {
			userKey := storageKey[len(redisStrPrefix):]
			var extra *kv.Elem[kv.OP]
			value, extra = t.stringValueAndTTLElem(userKey, tv)
			if extra != nil {
				elems = append(elems, extra)
			}
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: storageKey, Value: value})
	}
	return elems
}

func listDeleteMeta(st *listTxnState) (store.ListMeta, bool) {
	switch {
	case st.metaExists:
		return st.meta, true
	case st.purge:
		return st.purgeMeta, true
	default:
		return store.ListMeta{}, false
	}
}

func appendListDeleteOps(elems []*kv.Elem[kv.OP], userKey []byte, meta store.ListMeta) []*kv.Elem[kv.OP] {
	for seq := meta.Head; seq < meta.Tail; seq++ {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listItemKey(userKey, seq)})
	}
	return append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(userKey)})
}

func (t *txnContext) buildListElems(commitTS uint64) []*kv.Elem[kv.OP] {
	listKeys := make([]string, 0, len(t.listStates))
	for k := range t.listStates {
		listKeys = append(listKeys, k)
	}
	sort.Strings(listKeys)

	var elems []*kv.Elem[kv.OP]
	var seqInTxn uint32
	for _, k := range listKeys {
		st := t.listStates[k]
		userKey := []byte(k)

		if st.deleted {
			if meta, ok := listDeleteMeta(st); ok {
				elems = appendListDeleteOps(elems, userKey, meta)
			}
			// Delete existing delta keys so they don't survive the logical delete.
			for _, dk := range st.existingDeltas {
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: dk})
			}
			continue
		}
		if len(st.appends) == 0 {
			continue
		}
		if st.purge {
			elems = appendListDeleteOps(elems, userKey, st.purgeMeta)
			// Delete existing delta keys so they don't accumulate after DEL+RPUSH.
			for _, dk := range st.existingDeltas {
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: dk})
			}
		}

		startSeq := st.meta.Head + st.meta.Len
		for i, v := range st.appends {
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   listItemKey(userKey, startSeq+int64(i)),
				Value: v,
			})
		}

		// Emit a Delta key instead of updating the base metadata key.
		// Each list key in this transaction gets a unique seqInTxn.
		n := int64(len(st.appends))
		deltaVal := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: n})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ListMetaDeltaKey(userKey, commitTS, seqInTxn),
			Value: deltaVal,
		})
		seqInTxn++
	}
	return elems
}

func (t *txnContext) buildZSetElems(commitTS uint64) ([]*kv.Elem[kv.OP], error) {
	keys := make([]string, 0, len(t.zsetStates))
	for k := range t.zsetStates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	elems := make([]*kv.Elem[kv.OP], 0, len(keys))
	seqInTxn := uint32(0)
	for _, k := range keys {
		st := t.zsetStates[k]
		if !st.dirty {
			continue
		}
		key := []byte(k)
		if st.isWide {
			wideElems, lenDelta := buildZSetWideElems(key, st)
			elems = append(elems, wideElems...)
			if lenDelta != 0 {
				deltaVal := store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: lenDelta})
				elems = append(elems, &kv.Elem[kv.OP]{
					Op:    kv.Put,
					Key:   store.ZSetMetaDeltaKey(key, commitTS, seqInTxn),
					Value: deltaVal,
				})
				seqInTxn++
			}
			continue
		}
		// Legacy blob path.
		if len(st.members) == 0 {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisZSetKey(key)})
			continue
		}
		payload, err := marshalZSetValue(redisZSetValue{Entries: zsetMapToEntries(st.members)})
		if err != nil {
			return nil, err
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisZSetKey(key), Value: payload})
	}
	return elems, nil
}

// buildZSetWideElems computes the minimal set of ops to transition from st.origMembers to
// st.members in wide-column format. Returns the ops and the net length delta.
func buildZSetWideElems(key []byte, st *zsetTxnState) ([]*kv.Elem[kv.OP], int64) {
	elems := make([]*kv.Elem[kv.OP], 0, len(st.members)+len(st.origMembers))
	var lenDelta int64

	// Deletions: members removed or score changed (old score index must be removed).
	for member, oldScore := range st.origMembers {
		newScore, inNew := st.members[member]
		if !inNew {
			// Fully removed.
			elems = append(elems,
				&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetMemberKey(key, []byte(member))},
				&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(member))},
			)
			lenDelta--
		} else if newScore != oldScore {
			// Score updated: delete old score index.
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey(key, oldScore, []byte(member))})
		}
	}

	// Insertions / updates.
	for member, newScore := range st.members {
		_, wasOrig := st.origMembers[member]
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetMemberKey(key, []byte(member)), Value: store.MarshalZSetScore(newScore)},
			&kv.Elem[kv.OP]{Op: kv.Put, Key: store.ZSetScoreKey(key, newScore, []byte(member)), Value: []byte{}},
		)
		if !wasOrig {
			lenDelta++
		}
	}
	return elems, lenDelta
}

// buildStreamDeletionElems expands every user key queued in streamDeletions
// into the Del operations that actually tombstone a migrated stream:
// !stream|meta|<key> and every !stream|entry|<key><ID> row. Called from
// commit() so that MULTI/EXEC DEL / EXPIRE 0 on a migrated stream leaves
// the store in a consistent state instead of only dropping the legacy blob.
// Each scan runs at t.startTS so the delete honours the transaction's
// snapshot view.
//
// ctx is the redisDispatchTimeout-bounded context derived in commit(); it
// caps the paginated ExistsAt + scanAllDeltaElems inside
// deleteStreamWideColumnElems so a pathological staged-stream count cannot
// hold the EXEC handler open past the per-request budget.
func (t *txnContext) buildStreamDeletionElems(ctx context.Context) ([]*kv.Elem[kv.OP], error) {
	if len(t.streamDeletions) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(t.streamDeletions))
	for k := range t.streamDeletions {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var elems []*kv.Elem[kv.OP]
	for _, k := range keys {
		userKey := t.streamDeletions[k]
		streamElems, err := t.server.deleteStreamWideColumnElems(ctx, userKey, t.startTS)
		if err != nil {
			return nil, err
		}
		elems = append(elems, streamElems...)
	}
	return elems, nil
}

// buildTTLElems returns !redis|ttl| Raft elements for non-string keys with dirty TTL state.
// String keys have TTL embedded in the value; they are handled by buildKeyElems.
func (t *txnContext) buildTTLElems() []*kv.Elem[kv.OP] {
	var elems []*kv.Elem[kv.OP]
	for k, st := range t.ttlStates {
		if !st.dirty {
			continue
		}
		// String keys encode TTL inside the value in buildKeyElems; skip them here.
		if _, isString := t.working[string(redisStrKey([]byte(k)))]; isString {
			continue
		}
		if st.value == nil {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey([]byte(k))})
		} else {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey([]byte(k)), Value: encodeRedisTTL(*st.value)})
		}
	}
	return elems
}

func (r *RedisServer) runTransaction(queue []redcon.Command) ([]redisResult, error) {
	if r.onePhaseTxnDedup {
		return r.runTransactionWithDedup(queue)
	}

	dispatchCtx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	var results []redisResult
	err := r.retryRedisWrite(dispatchCtx, func() error {
		startTS := r.txnStartTS()
		readPin := r.pinReadTS(startTS)
		defer readPin.Release()

		txn := &txnContext{
			server:          r,
			ctx:             dispatchCtx,
			working:         map[string]*txnValue{},
			listStates:      map[string]*listTxnState{},
			zsetStates:      map[string]*zsetTxnState{},
			ttlStates:       map[string]*ttlTxnState{},
			readKeys:        map[string][]byte{},
			streamDeletions: map[string][]byte{},
			startTS:         startTS,
		}

		nextResults := make([]redisResult, 0, len(queue))
		for _, cmd := range queue {
			res, err := txn.apply(cmd)
			if err != nil {
				return err
			}
			nextResults = append(nextResults, res)
		}

		if err := txn.validateReadSet(dispatchCtx); err != nil {
			return err
		}
		if err := txn.commit(); err != nil {
			return err
		}
		results = nextResults
		return nil
	})
	if err != nil {
		return nil, err
	}

	return results, nil
}

// reusableExecTxn captures a dispatched MULTI/EXEC transaction so a
// subsequent retry can reuse its exact write set under a fresh
// commit_ts (carrying prev_commit_ts) and probe whether the prior
// attempt already landed. This is the EXEC analogue of
// reusableListPush (M3 R1 result reconstruction for MULTI/EXEC).
//
// `results` is computed once from attempt 1's startTS snapshot and is
// invariant across reuse for the same reason RPUSH/LPUSH's `length`
// is: the write set is fixed, so apply-vs-no-op is invisible to the
// client. Reads in the EXEC body returned values from attempt 1's
// snapshot — those values were what the client would have observed if
// attempt 1 hadn't returned an ambiguous error, so caching them is
// the right semantics for a confirmed-or-deduped commit. A
// genuine cross-txn conflict is caught by OCC on readKeys at the FSM
// apply (WriteConflict → drop pending → recompute), so the cached
// results are only returned when reuse actually represents the
// outcome of attempt 1's intent.
type reusableExecTxn struct {
	elems    []*kv.Elem[kv.OP]
	startTS  uint64
	commitTS uint64
	readKeys [][]byte
	results  []redisResult
}

// dispatchExecReuse runs one iteration of the option-2 reuse path for
// MULTI/EXEC: dispatches the captured write set under a fresh
// commit_ts (carrying pending.commitTS as PrevCommitTS so the FSM
// probes whether the prior attempt landed) and returns the cached
// client-visible results on success. The drop return signals the
// caller to clear pending — set on a genuine WriteConflict from
// another txn (after the self-conflict probe rules out our own apply)
// so the next iteration rebuilds the txn from a fresh read snapshot.
//
// Mirrors dispatchListPushReuse; the only difference is the result
// payload (cached []redisResult vs computed list length) and the lack
// of a meta re-read fallback — for EXEC there is no post-apply "what
// is the current length" question; the client-visible result IS the
// cached results array.
func (r *RedisServer) dispatchExecReuse(ctx context.Context, pending *reusableExecTxn) (results []redisResult, drop bool, err error) {
	// gemini PR-A HIGH: persistence-grade commit_ts allocation must honor the
	// HLC-4 physical-ceiling fence (see kv/hlc.go NextFenced + the TLA proof
	// at tla/hlc/MCHLC_gap.cfg). Clock().Next() bypasses the ceiling and
	// could issue a timestamp that collides with a subsequent leader's
	// window after renewal — the very class of bug option-2 is meant to
	// rule out.
	commitTS, allocErr := r.coordinator.Clock().NextFenced()
	if allocErr != nil {
		return nil, false, errors.Wrap(allocErr, "redis exec reuse: allocate commitTS")
	}
	_, dispErr := r.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{
		IsTxn:        true,
		StartTS:      pending.startTS,
		CommitTS:     commitTS,
		PrevCommitTS: pending.commitTS,
		ReadKeys:     pending.readKeys,
		Elems:        pending.elems,
	})
	if dispErr == nil {
		return pending.results, false, nil
	}
	if errors.Is(dispErr, store.ErrWriteConflict) {
		// Self-inflicted-conflict guard (mirrors dispatchListPushReuse):
		// the apply might have landed at this fresh commitTS but bubbled
		// up as WriteConflict due to leadership churn. Probe whether our
		// reused write set actually landed; if yes, return the cached
		// results unchanged (they describe the EXEC body's outcome
		// against attempt 1's snapshot, which is the outcome whether
		// the bytes hit MVCC at attempt-1's commitTS or at this fresh
		// commitTS — the OCC fence on readKeys guarantees no
		// intervening cross-txn write slipped past).
		if probeKey := firstWriteKey(pending.elems); len(probeKey) > 0 {
			landed, perr := r.store.CommittedVersionAt(ctx, probeKey, commitTS)
			if perr == nil && landed {
				pending.commitTS = commitTS
				return pending.results, false, nil
			}
		}
		// Our attempt did not land at commitTS and a key collides with
		// another txn — genuine conflict. Drop pending so the next
		// iteration rebuilds from a fresh snapshot.
		return nil, true, errors.WithStack(dispErr)
	}
	// Still ambiguous (lock / other retryable): the reuse may itself
	// have landed, so the next retry must probe THIS commit_ts. Only
	// advance pending.commitTS if retryRedisWrite will actually loop
	// (non-retryable errors escape to the client; pending is then
	// discarded with the goroutine).
	if isRetryableRedisTxnErr(dispErr) {
		pending.commitTS = commitTS
	}
	return nil, false, errors.WithStack(dispErr)
}

// runTransactionWithDedup is the option-2 retry loop for MULTI/EXEC.
// The first attempt builds the txn write set + cached results from
// the user's startTS snapshot; any retryable failure makes the next
// iteration REUSE that write set under a fresh commit_ts with
// prev_commit_ts set, so the FSM no-ops if the prior attempt already
// landed. A WriteConflict on a reuse attempt (after the self-conflict
// probe rules out our own apply) means another txn touched a read or
// write key, and we drop pending → rebuild from a fresh snapshot.
//
// Mirrors listPushCoreWithDedup at the EXEC granularity.
func (r *RedisServer) runTransactionWithDedup(queue []redcon.Command) ([]redisResult, error) {
	dispatchCtx, cancel := context.WithTimeout(r.handlerContext(), redisDispatchTimeout)
	defer cancel()

	var results []redisResult
	var pending *reusableExecTxn
	err := r.retryRedisWrite(dispatchCtx, func() error {
		if pending != nil {
			// gemini PR-A MEDIUM: derive the per-attempt reuse ctx from the
			// caller's `dispatchCtx` (not `r.handlerContext()`) so a cancelled
			// caller stops the reuse promptly. Per-attempt `redisDispatchTimeout`
			// still caps the dispatch the same way `commit()` does for the
			// first attempt; what changes is that an outer cancellation can
			// now interrupt mid-attempt instead of being ignored until the
			// fresh 10 s budget elapses. The earlier "fresh ctx from
			// handlerContext" pattern (noted in design doc §M3) was strictly
			// more conservative but wasted resources on a disconnected
			// client.
			reuseCtx, reuseCancel := context.WithTimeout(dispatchCtx, redisDispatchTimeout)
			defer reuseCancel()
			res, drop, dispErr := r.dispatchExecReuse(reuseCtx, pending)
			if drop {
				pending = nil
			}
			if dispErr != nil {
				return dispErr
			}
			results = res
			return nil
		}
		res, next, ferr := r.firstExecAttempt(dispatchCtx, queue)
		if ferr != nil {
			if next != nil {
				pending = next
			}
			return ferr
		}
		results = res
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// firstExecAttempt runs the initial (no-reuse) EXEC attempt: builds the
// txn snapshot, applies each command to capture the client-visible
// results, validates the read set, and dispatches. On success returns
// the results. On a retryable dispatch failure it returns a
// reusableExecTxn capturing what the retry loop needs to dispatch via
// PrevCommitTS on the next iteration; non-retryable failures return a
// nil reuse state (mirrors listPushCoreWithDedup's gating). Extracted
// from runTransactionWithDedup to keep that loop under the cyclop
// budget; the dedup rationale lives there.
func (r *RedisServer) firstExecAttempt(dispatchCtx context.Context, queue []redcon.Command) ([]redisResult, *reusableExecTxn, error) {
	startTS := r.txnStartTS()
	readPin := r.pinReadTS(startTS)
	defer readPin.Release()

	txn := &txnContext{
		server:          r,
		ctx:             dispatchCtx,
		working:         map[string]*txnValue{},
		listStates:      map[string]*listTxnState{},
		zsetStates:      map[string]*zsetTxnState{},
		ttlStates:       map[string]*ttlTxnState{},
		readKeys:        map[string][]byte{},
		streamDeletions: map[string][]byte{},
		startTS:         startTS,
	}

	nextResults := make([]redisResult, 0, len(queue))
	for _, cmd := range queue {
		res, err := txn.apply(cmd)
		if err != nil {
			return nil, nil, err
		}
		nextResults = append(nextResults, res)
	}

	if err := txn.validateReadSet(dispatchCtx); err != nil {
		return nil, nil, err
	}

	prepared, err := txn.prepareDispatch()
	if err != nil {
		return nil, nil, err
	}
	defer prepared.cancel()
	if len(prepared.elems) == 0 {
		// Read-only EXEC: nothing to dispatch, no dedup window.
		return nextResults, nil, nil
	}

	group := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		Elems:    prepared.elems,
		StartTS:  txn.startTS,
		CommitTS: prepared.commitTS,
		ReadKeys: prepared.readKeys,
	}
	if _, dispErr := r.coordinator.Dispatch(prepared.ctx, group); dispErr != nil {
		// Only remember the attempt for reuse if retryRedisWrite will
		// actually loop. Mirrors listPushCoreWithDedup's gating
		// rationale — errors that escape the loop (transient-leader,
		// context deadline, FSM apply error) leave pending pointing at
		// state wasted with the goroutine; ambiguous errors that
		// escape to the client are out of scope for this loop.
		if isRetryableRedisTxnErr(dispErr) {
			return nil, &reusableExecTxn{
				elems:    prepared.elems,
				startTS:  txn.startTS,
				commitTS: prepared.commitTS,
				readKeys: prepared.readKeys,
				results:  nextResults,
			}, errors.WithStack(dispErr)
		}
		return nil, nil, errors.WithStack(dispErr)
	}
	return nextResults, nil, nil
}

func (r *RedisServer) txnStartTS() uint64 {
	// store.LastCommitTS() is the authoritative safe-snapshot watermark: it is
	// updated atomically only AFTER the corresponding Pebble batch commit, so
	// every version with commitTS ≤ store.LastCommitTS() is guaranteed visible
	// in the store when we read.
	//
	// We must NOT return clock.Next() here.  clock.Next() can be AHEAD of
	// store.LastCommitTS() because concurrent dispatchTxn calls advance the HLC
	// before their Raft entry is applied.  If startTS = clock.Next() = T, a
	// concurrent transaction that already called clock.Next() to obtain
	// commitTS = T-1 and is still in the Raft pipeline will satisfy
	//   latestTS(key) = T-1  ≤  T = startTS
	// causing the FSM conflict check (latestTS > startTS) to silently pass even
	// though we read stale data.  This allows two concurrent RPUSHes to pick the
	// same sequence number, with the second overwriting the first — a lost write.
	//
	// Using store.LastCommitTS() directly closes this gap: any concurrent commit
	// at > maxTS triggers a WriteConflict and a retry via retryRedisWrite.
	//
	// The Observe call still advances the HLC so that dispatchTxn's clock.Next()
	// produces a commitTS strictly greater than maxTS (leader-election safety).
	//
	// When maxTS is 0 (empty store) we return 1 so the coordinator treats this
	// as a valid startTS and does not override it with clock.Next() — which
	// could be ahead of unapplied Raft entries and reintroduce the anomaly.
	var maxTS uint64
	if r.store != nil {
		maxTS = r.store.LastCommitTS()
	}
	if r.coordinator != nil && r.coordinator.Clock() != nil && maxTS > 0 {
		r.coordinator.Clock().Observe(maxTS)
	}
	if maxTS == 0 {
		return 1
	}
	return maxTS
}

func (r *RedisServer) writeResults(conn redcon.Conn, results []redisResult) {
	conn.WriteArray(len(results))
	for _, res := range results {
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
}
