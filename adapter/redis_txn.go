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
var redisTxnWideHashFencePrefix = []byte("!redis|txn-wide-hash|")
var redisTxnWideSetFencePrefix = []byte("!redis|txn-wide-set|")
var redisTxnWideListFencePrefix = []byte("!redis|txn-wide-list|")
var redisTxnWideZSetFencePrefix = []byte("!redis|txn-wide-zset|")

type txnCommandHandler func(*txnContext, redcon.Command) (redisResult, error)

var txnApplyHandlers = map[string]txnCommandHandler{
	cmdSet:     (*txnContext).applySet,
	cmdDel:     (*txnContext).applyDel,
	cmdGet:     (*txnContext).applyGet,
	cmdExists:  (*txnContext).applyExists,
	cmdIncr:    (*txnContext).applyIncr,
	cmdHSet:    (*txnContext).applyHSet,
	cmdHMSet:   (*txnContext).applyHSet,
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

type stringReplacement struct {
	key   []byte
	value []byte
	ttl   *time.Time
}

type txnContext struct {
	server *RedisServer
	// ctx is the per-EXEC dispatch context (redisDispatchTimeout-bounded
	// at the call site in runTransaction). Plumbed through so reads
	// inside the EXEC such as load() → readValueAt() respect the
	// caller's deadline rather than falling back to handlerContext +
	// the verifyLeaderEngineCtx safety net.
	ctx            context.Context //nolint:containedctx // EXEC is a long-lived value type that wraps a single client command, ctx must travel with it.
	working        map[string]*txnValue
	replacers      map[string]*stringReplacement
	listStates     map[string]*listTxnState
	hashStates     map[string]*hashTxnState
	zsetStates     map[string]*zsetTxnState
	ttlStates      map[string]*ttlTxnState
	readKeys       map[string][]byte
	deletedKeys    map[string]struct{}
	logicalDeletes map[string][]byte
	hashDeletes    map[string][]byte
	setDeletes     map[string][]byte
	hashCreates    map[string]struct{}
	// collectionExpireTypes remembers non-string keys whose TTL changed in this
	// EXEC so commit can update the inline metadata anchor with the scan index.
	collectionExpireTypes map[string]redisValueType
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

type hashTxnState struct {
	fields         map[string][]byte
	origFields     map[string][]byte
	dirtyFields    map[string]struct{}
	legacy         bool
	legacyExpireAt uint64
	deleted        bool
	dirty          bool
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

func redisTxnWideHashFenceKey(userKey []byte) []byte {
	return redisTxnWideFenceKey(redisTxnWideHashFencePrefix, userKey)
}

func redisTxnWideSetFenceKey(userKey []byte) []byte {
	return redisTxnWideFenceKey(redisTxnWideSetFencePrefix, userKey)
}

func redisTxnWideListFenceKey(userKey []byte) []byte {
	return redisTxnWideFenceKey(redisTxnWideListFencePrefix, userKey)
}

func redisTxnWideZSetFenceKey(userKey []byte) []byte {
	return redisTxnWideFenceKey(redisTxnWideZSetFencePrefix, userKey)
}

func redisTxnWideFenceUserKey(key []byte) []byte {
	for _, prefix := range [][]byte{
		redisTxnWideHashFencePrefix,
		redisTxnWideSetFencePrefix,
		redisTxnWideListFencePrefix,
		redisTxnWideZSetFencePrefix,
	} {
		if bytes.HasPrefix(key, prefix) {
			return key[len(prefix):]
		}
	}
	return nil
}

func redisTxnWideFenceKey(prefix, userKey []byte) []byte {
	buf := make([]byte, 0, len(prefix)+len(userKey))
	buf = append(buf, prefix...)
	buf = append(buf, userKey...)
	return buf
}

func redisTxnWideHashFenceElem(userKey []byte) *kv.Elem[kv.OP] {
	return redisTxnWideFenceElem(redisTxnWideHashFenceKey(userKey))
}

func redisTxnWideSetFenceElem(userKey []byte) *kv.Elem[kv.OP] {
	return redisTxnWideFenceElem(redisTxnWideSetFenceKey(userKey))
}

func redisTxnWideListFenceElem(userKey []byte) *kv.Elem[kv.OP] {
	return redisTxnWideFenceElem(redisTxnWideListFenceKey(userKey))
}

func redisTxnWideZSetFenceElem(userKey []byte) *kv.Elem[kv.OP] {
	return redisTxnWideFenceElem(redisTxnWideZSetFenceKey(userKey))
}

func redisTxnWideCollectionFenceKeys(userKey []byte) [][]byte {
	return [][]byte{
		redisTxnWideHashFenceKey(userKey),
		redisTxnWideSetFenceKey(userKey),
		redisTxnWideListFenceKey(userKey),
		redisTxnWideZSetFenceKey(userKey),
	}
}

func redisTxnWideCollectionFenceElems(userKey []byte) []*kv.Elem[kv.OP] {
	keys := redisTxnWideCollectionFenceKeys(userKey)
	elems := make([]*kv.Elem[kv.OP], 0, len(keys))
	for _, key := range keys {
		elems = append(elems, redisTxnWideFenceElem(key))
	}
	return elems
}

func redisTxnWideCreateReadKeys(userKey []byte, typ redisValueType, fenceKey func([]byte) []byte) [][]byte {
	if typ == redisTypeNone {
		return redisTxnWideCollectionFenceKeys(userKey)
	}
	return [][]byte{fenceKey(userKey)}
}

func redisTxnWideFenceElem(key []byte) *kv.Elem[kv.OP] {
	return &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: []byte{}}
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
	t.trackReadKey(redisTxnWideListFenceKey(key))
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

func (t *txnContext) loadHashStateForFields(key []byte, fields [][]byte) (*hashTxnState, error) {
	k := string(key)
	if t.hashStates == nil {
		t.hashStates = map[string]*hashTxnState{}
	}
	if st, ok := t.hashStates[k]; ok {
		if st.deleted {
			return reviveDeletedHashState(st), nil
		}
		return st, t.loadHashFieldsIntoState(key, fields, st)
	}
	if _, deleted := t.deletedKeys[k]; deleted {
		return t.storeEmptyHashState(k), nil
	}
	if st, err := t.loadExpiredHashAsEmpty(key, k); err != nil || st != nil {
		return st, err
	}
	if len(fields) == 0 {
		return t.loadExistingHashState(key, k)
	}
	return t.loadExistingHashFieldState(key, k, fields)
}

func reviveDeletedHashState(st *hashTxnState) *hashTxnState {
	if st.deleted {
		st.fields = map[string][]byte{}
		st.origFields = map[string][]byte{}
		st.dirtyFields = map[string]struct{}{}
		st.legacy = false
		st.legacyExpireAt = 0
		st.deleted = false
		st.dirty = true
	}
	return st
}

func (t *txnContext) storeEmptyHashState(key string) *hashTxnState {
	st := &hashTxnState{
		fields:      map[string][]byte{},
		origFields:  map[string][]byte{},
		dirtyFields: map[string]struct{}{},
		dirty:       true,
	}
	t.hashStates[key] = st
	return st
}

func (t *txnContext) loadExpiredHashAsEmpty(key []byte, keyString string) (*hashTxnState, error) {
	expired, err := t.stageExpiredKeyCleanupForRecreate(key)
	if err != nil || !expired {
		return nil, err
	}
	return t.storeEmptyHashState(keyString), nil
}

func (t *txnContext) loadExistingHashState(key []byte, keyString string) (*hashTxnState, error) {
	ctx := t.ctxOrBackground()
	t.trackReadKey(redisTxnWideHashFenceKey(key))
	value, err := t.server.loadHashAt(ctx, key, t.startTS)
	if err != nil {
		return nil, err
	}
	fields := make(map[string][]byte, len(value))
	origFields := make(map[string][]byte, len(value))
	for field, val := range value {
		raw := []byte(val)
		fields[field] = raw
		origFields[field] = bytes.Clone(raw)
	}
	legacy, err := t.server.store.ExistsAt(ctx, redisHashKey(key), t.startTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var legacyExpireAt uint64
	if legacy {
		legacyExpireAt, err = legacyTTLMillisAt(ctx, t.server.store, key, t.startTS)
		if err != nil {
			return nil, err
		}
	}
	st := &hashTxnState{
		fields:         fields,
		origFields:     origFields,
		dirtyFields:    map[string]struct{}{},
		legacy:         legacy,
		legacyExpireAt: legacyExpireAt,
	}
	t.hashStates[keyString] = st
	return st, nil
}

func (t *txnContext) loadExistingHashFieldState(key []byte, keyString string, fields [][]byte) (*hashTxnState, error) {
	ctx := t.ctxOrBackground()
	t.trackReadKey(redisTxnWideHashFenceKey(key))
	wide, err := t.server.prefixExistsAt(ctx, store.HashFieldScanPrefix(key), t.startTS)
	if err != nil {
		return nil, err
	}
	if !wide {
		legacy, err := t.server.store.ExistsAt(ctx, redisHashKey(key), t.startTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if legacy {
			return t.loadExistingHashState(key, keyString)
		}
	}
	st := &hashTxnState{
		fields:      map[string][]byte{},
		origFields:  map[string][]byte{},
		dirtyFields: map[string]struct{}{},
	}
	t.hashStates[keyString] = st
	if err := t.loadHashFieldsIntoState(key, fields, st); err != nil {
		return nil, err
	}
	return st, nil
}

func (t *txnContext) loadHashFieldsIntoState(key []byte, fields [][]byte, st *hashTxnState) error {
	if st.legacy {
		return nil
	}
	ctx := t.ctxOrBackground()
	for _, field := range fields {
		fieldName := string(field)
		if _, loaded := st.origFields[fieldName]; loaded {
			continue
		}
		if _, staged := st.fields[fieldName]; staged {
			continue
		}
		raw, err := t.server.store.GetAt(ctx, store.HashFieldKey(key, field), t.startTS)
		if err != nil {
			if errors.Is(err, store.ErrKeyNotFound) {
				continue
			}
			return errors.WithStack(err)
		}
		st.fields[fieldName] = bytes.Clone(raw)
		st.origFields[fieldName] = bytes.Clone(raw)
	}
	return nil
}

func (t *txnContext) listLength(st *listTxnState) int64 {
	return st.meta.Len + int64(len(st.appends))
}

func (t *txnContext) loadZSetState(key []byte) (*zsetTxnState, error) {
	k := string(key)
	if st, ok := t.zsetStates[k]; ok {
		return st, nil
	}
	t.trackReadKey(redisTxnWideZSetFenceKey(key))
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
	ctx := t.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	value, err := t.server.ttlAt(ctx, key, t.startTS)
	if err != nil {
		return nil, err
	}
	st := &ttlTxnState{value: value}
	t.ttlStates[k] = st
	return st, nil
}

func (t *txnContext) stagedKeyType(key []byte) (redisValueType, error) {
	k := string(key)
	if _, ok := t.replacers[k]; ok {
		return redisTypeString, nil
	}
	if typ, ok := t.stagedPositiveKeyType(k); ok {
		return typ, nil
	}
	if t.hasStagedTypeDeletion(k) {
		return redisTypeNone, nil
	}
	t.trackTypeReadKeys(key)
	return t.server.keyTypeAt(t.ctxOrBackground(), key, t.startTS)
}

func (t *txnContext) stagedPositiveKeyType(key string) (redisValueType, bool) {
	if t.hasPositiveHashState(key) {
		return redisTypeHash, true
	}
	if t.hasPositiveZSetState(key) {
		return redisTypeZSet, true
	}
	if t.hasPositiveListState(key) {
		return redisTypeList, true
	}
	if t.hasPositiveStringState(key) {
		return redisTypeString, true
	}
	return redisTypeNone, false
}

func (t *txnContext) hasStagedTypeDeletion(key string) bool {
	return t.hasDeletedHashState(key) ||
		t.hasDeletedZSetState(key) ||
		t.hasDeletedListState(key) ||
		t.hasDeletedStringState(key) ||
		t.isLogicallyDeleted(key)
}

func (t *txnContext) hasPositiveHashState(key string) bool {
	st, ok := t.hashStates[key]
	return ok && !st.deleted && len(st.fields) > 0
}

func (t *txnContext) hasPositiveZSetState(key string) bool {
	st, ok := t.zsetStates[key]
	return ok && len(st.members) > 0
}

func (t *txnContext) hasPositiveListState(key string) bool {
	st, ok := t.listStates[key]
	return ok && !st.deleted && (st.metaExists || len(st.appends) > 0)
}

func (t *txnContext) hasPositiveStringState(key string) bool {
	tv, ok := t.working[string(redisStrKey([]byte(key)))]
	return ok && !tv.deleted && tv.raw != nil
}

func (t *txnContext) hasDeletedHashState(key string) bool {
	st, ok := t.hashStates[key]
	return ok && (st.deleted || len(st.fields) == 0)
}

func (t *txnContext) hasDeletedZSetState(key string) bool {
	st, ok := t.zsetStates[key]
	return ok && (st.dirty || st.exists) && len(st.members) == 0
}

func (t *txnContext) hasDeletedListState(key string) bool {
	st, ok := t.listStates[key]
	return ok && st.deleted
}

func (t *txnContext) hasDeletedStringState(key string) bool {
	tv, ok := t.working[string(redisStrKey([]byte(key)))]
	return ok && (tv.deleted || tv.raw == nil)
}

func (t *txnContext) isLogicallyDeleted(key string) bool {
	_, ok := t.deletedKeys[key]
	return ok
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
	opts, err := parseRedisSetOptions(cmd.Args[3:], time.Now())
	if err != nil {
		return redisResult{}, err
	}
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}

	// NX/XX: skip the write if the key-existence condition is not met.
	exists := typ != redisTypeNone
	if !opts.allows(exists) {
		return redisResult{typ: resultNil}, nil
	}
	if opts.returnOld && exists && typ != redisTypeString {
		return redisResult{typ: resultError, err: wrongTypeError()}, nil
	}

	oldValue, err := t.oldStringValueForSet(cmd.Args[1], opts.returnOld, typ)
	if err != nil {
		return redisResult{}, err
	}
	t.stageStringReplacement(cmd.Args[1], cmd.Args[2], opts.ttl)
	return applySetResult(opts, oldValue), nil
}

func (t *txnContext) oldStringValueForSet(key []byte, returnOld bool, typ redisValueType) ([]byte, error) {
	if !returnOld || typ != redisTypeString {
		return nil, nil
	}
	raw, _, ok, err := t.currentStringValue(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return raw, nil
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

func cloneTimePtr(in *time.Time) *time.Time {
	if in == nil {
		return nil
	}
	out := *in
	return &out
}

func (t *txnContext) stageStringReplacement(key, value []byte, ttl *time.Time) {
	if t.replacers == nil {
		t.replacers = map[string]*stringReplacement{}
	}
	k := string(key)
	t.replacers[k] = &stringReplacement{
		key:   bytes.Clone(key),
		value: bytes.Clone(value),
		ttl:   cloneTimePtr(ttl),
	}
	delete(t.deletedKeys, k)
	delete(t.collectionExpireTypes, k)
}

func (t *txnContext) updateStringReplacementTTL(key []byte, ttl *time.Time) bool {
	repl, ok := t.replacers[string(key)]
	if !ok {
		return false
	}
	repl.ttl = cloneTimePtr(ttl)
	return true
}

func (t *txnContext) currentStringValue(key []byte) ([]byte, *time.Time, bool, error) {
	if repl, ok := t.replacers[string(key)]; ok {
		return bytes.Clone(repl.value), cloneTimePtr(repl.ttl), true, nil
	}
	tv, err := t.load(key)
	if err != nil {
		return nil, nil, false, err
	}
	if tv.deleted || tv.raw == nil {
		return nil, nil, false, nil
	}
	return bytes.Clone(tv.raw), cloneTimePtr(tv.ttl), true, nil
}

func (t *txnContext) stageStringValue(key, value []byte, ttl *time.Time) error {
	if repl, ok := t.replacers[string(key)]; ok {
		repl.value = bytes.Clone(value)
		repl.ttl = cloneTimePtr(ttl)
		return nil
	}
	tv, err := t.load(key)
	if err != nil {
		return err
	}
	tv.raw = bytes.Clone(value)
	tv.ttl = cloneTimePtr(ttl)
	tv.deleted = false
	tv.dirty = true
	delete(t.deletedKeys, string(key))
	return nil
}

func (t *txnContext) stageExpiredKeyCleanupForRecreate(key []byte) (bool, error) {
	ttlSt, err := t.loadTTLState(key)
	if err != nil {
		return false, err
	}
	if ttlSt.value == nil || hasActiveTTL(ttlSt.value, time.Now()) {
		return false, nil
	}
	ttlSt.value = nil
	ttlSt.dirty = true
	if t.logicalDeletes == nil {
		t.logicalDeletes = map[string][]byte{}
	}
	t.logicalDeletes[string(key)] = bytes.Clone(key)
	t.trackWideCollectionFenceReads(key)
	return true, nil
}

func (t *txnContext) trackMissingKeyCreatorFenceReads(key []byte, typ redisValueType) {
	if typ == redisTypeNone {
		t.trackWideCollectionFenceReads(key)
	}
}

func (t *txnContext) applyIncr(cmd redcon.Command) (redisResult, error) {
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if typ != redisTypeNone && typ != redisTypeString {
		return redisResult{typ: resultError, err: wrongTypeError()}, nil
	}
	t.trackMissingKeyCreatorFenceReads(cmd.Args[1], typ)

	current, ttl, res, handled, err := t.incrBaseValue(cmd.Args[1], typ)
	if err != nil || handled {
		return res, err
	}
	if current == math.MaxInt64 {
		return incrOverflowResult(), nil
	}
	current++
	next := []byte(strconv.FormatInt(current, 10))
	if typ == redisTypeNone {
		if _, err := t.stageExpiredKeyCleanupForRecreate(cmd.Args[1]); err != nil {
			return redisResult{}, err
		}
		t.stageStringReplacement(cmd.Args[1], next, ttl)
		return redisResult{typ: resultInt, integer: current}, nil
	}
	if err := t.stageStringValue(cmd.Args[1], next, ttl); err != nil {
		return redisResult{}, err
	}
	return redisResult{typ: resultInt, integer: current}, nil
}

func (t *txnContext) incrBaseValue(key []byte, typ redisValueType) (int64, *time.Time, redisResult, bool, error) {
	if typ != redisTypeString {
		return 0, nil, redisResult{}, false, nil
	}
	if res, handled, err := t.rejectNonPlainIncrString(key); err != nil || handled {
		return 0, nil, res, handled, err
	}
	raw, ttl, ok, err := t.currentStringValue(key)
	if err != nil {
		return 0, nil, redisResult{}, false, err
	}
	if !ok {
		return 0, ttl, redisResult{}, false, nil
	}
	current, ok := parseRedisIncrValue(raw)
	if !ok {
		return 0, nil, integerValueErrorResult(), true, nil
	}
	return current, ttl, redisResult{}, false, nil
}

func (t *txnContext) rejectNonPlainIncrString(key []byte) (redisResult, bool, error) {
	if t.hasReplacement(key) || t.hasDirtyStagedString(key) {
		return redisResult{}, false, nil
	}
	plain, err := t.server.isPlainRedisString(t.ctxOrBackground(), key, t.startTS)
	if err != nil {
		return redisResult{}, false, err
	}
	if plain {
		return redisResult{}, false, nil
	}
	return integerValueErrorResult(), true, nil
}

func (t *txnContext) hasDirtyStagedString(key []byte) bool {
	tv, ok := t.working[string(redisStrKey(key))]
	return ok && tv.dirty && !tv.deleted && tv.raw != nil
}

func parseRedisIncrValue(raw []byte) (int64, bool) {
	current, err := strconv.ParseInt(string(raw), 10, 64)
	return current, err == nil
}

func integerValueErrorResult() redisResult {
	return redisResult{typ: resultError, err: errors.New("ERR value is not an integer or out of range")}
}

func incrOverflowResult() redisResult {
	return redisResult{typ: resultError, err: errors.New("ERR increment or decrement would overflow")}
}

func (t *txnContext) applyHSet(cmd redcon.Command) (redisResult, error) {
	if len(cmd.Args[2:]) == 0 || len(cmd.Args[2:])%redisPairWidth != 0 {
		return redisResult{typ: resultError, err: errors.New("ERR wrong number of arguments for hash command")}, nil
	}
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if typ != redisTypeNone && typ != redisTypeHash {
		return redisResult{typ: resultError, err: wrongTypeError()}, nil
	}
	t.markHashCreateIfMissing(cmd.Args[1], typ)
	t.trackMissingKeyCreatorFenceReads(cmd.Args[1], typ)
	fields := hashCommandFields(cmd.Args[2:])
	st, err := t.loadHashStateForFields(cmd.Args[1], fields)
	if err != nil {
		return redisResult{}, err
	}
	added := int64(0)
	for i := 2; i < len(cmd.Args); i += redisPairWidth {
		field := string(cmd.Args[i])
		if _, exists := st.fields[field]; !exists {
			added++
		}
		st.fields[field] = bytes.Clone(cmd.Args[i+1])
		st.dirtyFields[field] = struct{}{}
	}
	st.deleted = false
	st.dirty = true
	delete(t.deletedKeys, string(cmd.Args[1]))
	if strings.EqualFold(string(cmd.Args[0]), cmdHMSet) {
		return redisResult{typ: resultString, str: "OK"}, nil
	}
	return redisResult{typ: resultInt, integer: added}, nil
}

func hashCommandFields(args [][]byte) [][]byte {
	fields := make([][]byte, 0, len(args)/redisPairWidth)
	for i := 0; i < len(args); i += redisPairWidth {
		fields = append(fields, args[i])
	}
	return fields
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

	raw, _, ok, err := t.currentStringValue(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if !ok {
		return redisResult{typ: resultNil}, nil
	}
	return redisResult{typ: resultBulk, bulk: raw}, nil
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
	if res, handled, err := t.prepareListWrite(cmd.Args[1]); err != nil || handled {
		return res, err
	}
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

func (t *txnContext) prepareListWrite(key []byte) (redisResult, bool, error) {
	typ, err := t.stagedKeyType(key)
	if err != nil {
		return redisResult{}, false, err
	}
	if typ != redisTypeNone && typ != redisTypeList {
		return redisResult{typ: resultError, err: wrongTypeError()}, true, nil
	}
	if typ != redisTypeNone {
		return redisResult{}, false, nil
	}
	t.trackMissingKeyCreatorFenceReads(key, typ)
	expired, err := t.stageExpiredKeyCleanupForRecreate(key)
	if err != nil {
		return redisResult{}, false, err
	}
	if expired {
		t.listStates[string(key)] = &listTxnState{appends: [][]byte{}}
	}
	return redisResult{}, false, nil
}

func (t *txnContext) applyLRange(cmd redcon.Command) (redisResult, error) {
	typ, err := t.stagedKeyType(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if typ == redisTypeNone {
		t.trackReadKey(redisTxnWideListFenceKey(cmd.Args[1]))
		return redisResult{typ: resultArray, arr: []string{}}, nil
	}
	if typ != redisTypeList {
		return redisResult{typ: resultError, err: wrongTypeError()}, nil
	}
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
	t.trackMissingKeyCreatorFenceReads(cmd.Args[1], typ)

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
	if nxOnly && hasActiveTTL(t.effectiveTTLForExpire(cmd.Args[1], state), time.Now()) {
		return redisResult{typ: resultInt, integer: 0}, nil
	}

	if ttl <= 0 {
		return t.stageKeyDeletion(cmd.Args[1])
	}
	return t.applyPositiveExpire(cmd.Args[1], ttl, unit, typ, state)
}

func (t *txnContext) effectiveTTLForExpire(key []byte, state *ttlTxnState) *time.Time {
	if repl, ok := t.replacers[string(key)]; ok {
		return cloneTimePtr(repl.ttl)
	}
	tv, ok := t.working[string(redisStrKey(key))]
	if ok && !tv.deleted && tv.raw != nil {
		if state != nil && state.dirty {
			return cloneTimePtr(state.value)
		}
		return cloneTimePtr(tv.ttl)
	}
	if state == nil {
		return nil
	}
	return cloneTimePtr(state.value)
}

func (t *txnContext) applyPositiveExpire(key []byte, ttl int64, unit time.Duration, typ redisValueType, state *ttlTxnState) (redisResult, error) {
	if ttl > math.MaxInt64/int64(unit) {
		return redisResult{}, errors.New("ERR invalid expire time in command")
	}
	expireAt := time.Now().Add(time.Duration(ttl) * unit)
	state.value = &expireAt
	state.dirty = true
	if isNonStringCollectionType(typ) {
		t.trackCollectionExpireType(key, typ)
		return redisResult{typ: resultInt, integer: 1}, nil
	}
	if typ == redisTypeString {
		if t.updateStringReplacementTTL(key, &expireAt) {
			return redisResult{typ: resultInt, integer: 1}, nil
		}
		plain, err := t.server.isPlainRedisString(t.ctxOrBackground(), key, t.startTS)
		if err != nil {
			return redisResult{}, err
		}
		if plain {
			return t.markStringDirty(key)
		}
		if err := t.markHLLDirty(key, state.value); err != nil {
			return redisResult{}, err
		}
	}
	return redisResult{typ: resultInt, integer: 1}, nil
}

func (t *txnContext) trackCollectionExpireType(key []byte, typ redisValueType) {
	if t.collectionExpireTypes == nil {
		t.collectionExpireTypes = map[string]redisValueType{}
	}
	t.collectionExpireTypes[string(key)] = typ
}

func (t *txnContext) markHashCreateIfMissing(key []byte, typ redisValueType) {
	if typ == redisTypeNone {
		t.markHashCreate(key)
	}
}

func (t *txnContext) markHashCreate(key []byte) {
	if t.hashCreates == nil {
		t.hashCreates = map[string]struct{}{}
	}
	t.hashCreates[string(key)] = struct{}{}
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

func (t *txnContext) markHLLDirty(key []byte, ttl *time.Time) error {
	hllValue, err := t.load(redisHLLKey(key))
	if err != nil {
		return err
	}
	value, _, _, err := decodeRedisHLL(hllValue.raw)
	if err != nil {
		return err
	}
	encoded, err := encodeRedisHLL(value, ttl)
	if err != nil {
		return err
	}
	hllValue.raw = encoded
	hllValue.dirty = true
	return nil
}

func (t *txnContext) stageKeyDeletion(key []byte) (redisResult, error) {
	k := string(key)
	t.markLogicalDeletion(key, k)
	if err := t.stageCollectionStateDeletion(key); err != nil {
		return redisResult{}, err
	}
	if err := t.stageLegacyInternalDeletion(key); err != nil {
		return redisResult{}, err
	}
	t.stageStreamWideDeletion(key, k)
	t.stageBareStringDeletion(key, k)
	return redisResult{typ: resultInt, integer: 1}, nil
}

func (t *txnContext) markLogicalDeletion(key []byte, k string) {
	if t.deletedKeys == nil {
		t.deletedKeys = map[string]struct{}{}
	}
	t.deletedKeys[k] = struct{}{}
	delete(t.replacers, k)
	if t.hashDeletes == nil {
		t.hashDeletes = map[string][]byte{}
	}
	if t.setDeletes == nil {
		t.setDeletes = map[string][]byte{}
	}
	t.hashDeletes[k] = bytes.Clone(key)
	t.setDeletes[k] = bytes.Clone(key)
	delete(t.hashCreates, k)
	t.trackWideCollectionFenceReads(key)
	if st, ok := t.hashStates[k]; ok {
		st.deleted = true
		st.dirty = false
	}
}

func (t *txnContext) trackWideCollectionFenceReads(key []byte) {
	for _, fenceKey := range redisTxnWideCollectionFenceKeys(key) {
		t.trackReadKey(fenceKey)
	}
}

func (t *txnContext) stageCollectionStateDeletion(key []byte) error {
	// Mark the list for deletion.
	st, err := t.loadListState(key)
	if err != nil {
		return err
	}
	stageListDelete(st)
	// Mark the string/main value for deletion.
	tv, err := t.load(key)
	if err != nil {
		return err
	}
	tv.deleted = true
	tv.dirty = true
	// Mark TTL for deletion.
	ttlState, err := t.loadTTLState(key)
	if err != nil {
		return err
	}
	ttlState.value = nil
	ttlState.dirty = true
	// Mark zset for deletion. Use empty map (not nil) so that subsequent
	// writes (e.g. ZINCRBY) in the same transaction can safely insert.
	zs, err := t.loadZSetState(key)
	if err != nil {
		return err
	}
	zs.members = map[string]float64{}
	zs.origMembers = map[string]float64{}
	zs.exists = false
	zs.dirty = true
	return nil
}

func (t *txnContext) stageLegacyInternalDeletion(key []byte) error {
	// Mark hash, set, stream (legacy blob), and HLL internal keys for deletion.
	for _, internalKey := range [][]byte{
		redisHashKey(key),
		redisSetKey(key),
		redisStreamKey(key),
		redisHLLKey(key),
	} {
		iv, err := t.load(internalKey)
		if err != nil {
			return err
		}
		iv.deleted = true
		iv.dirty = true
	}
	return nil
}

func (t *txnContext) stageStreamWideDeletion(key []byte, k string) {
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
	t.streamDeletions[k] = bytes.Clone(key)
	t.trackReadKey(store.StreamMetaKey(key))
}

func (t *txnContext) stageBareStringDeletion(key []byte, k string) {
	// Mark legacy bare string key for deletion. We bypass load() here
	// because load() auto-prefixes bare keys to !redis|str|.
	// Track the bare key in the read set for conflict detection.
	t.trackReadKey(key)
	if _, ok := t.working[k]; !ok {
		t.working[k] = &txnValue{}
	}
	t.working[k].deleted = true
	t.working[k].dirty = true
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
	ctx, cancel := t.dispatchContext()

	replacementElems, err := t.buildReplacementElems(ctx)
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, err
	}
	logicalDeleteElems, err := t.buildLogicalDeletionElems(ctx)
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, err
	}
	hashDeleteElems, err := t.buildHashDeletionElems(ctx)
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, err
	}
	setDeleteElems, err := t.buildSetDeletionElems(ctx)
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, err
	}
	keyElems := t.buildKeyElems()
	elems := make([]*kv.Elem[kv.OP], 0, len(replacementElems)+len(logicalDeleteElems)+len(hashDeleteElems)+len(setDeleteElems)+len(keyElems))
	elems = append(elems, replacementElems...)
	elems = append(elems, logicalDeleteElems...)
	elems = append(elems, hashDeleteElems...)
	elems = append(elems, setDeleteElems...)
	elems = append(elems, keyElems...)

	// Pre-allocate commitTS so Delta keys can embed it in their bytes before
	// the coordinator assigns it during Dispatch. The allocation is bounded by
	// ctx and must floor above the transaction's read snapshot.
	commitTS, err := t.server.nextCommitTSAfter(ctx, t.startTS, "redis txn commit: allocate commitTS")
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, errors.WithStack(err)
	}
	listElems := t.buildListElems(commitTS)
	zsetElems, err := t.buildZSetElems(commitTS)
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, err
	}
	hashElems := t.buildHashElems(commitTS)
	collectionTTLElems, skipTTLIndex, err := t.buildCollectionTTLElems(ctx)
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, err
	}
	// TTL elements: string/HLL keys have TTL embedded in value, and collection
	// keys also keep !redis|ttl| as the secondary scan index.
	ttlElems := t.buildTTLElems(skipTTLIndex)

	// Continue using the same redisDispatchTimeout-bounded context for
	// the stream-deletion scans (paginated ScanAt/ExistsAt over
	// StreamEntryScanPrefix) and the final Dispatch.
	streamElems, err := t.buildStreamDeletionElems(ctx)
	if err != nil {
		cancel()
		return preparedTxnDispatch{cancel: func() {}}, err
	}

	elems = append(elems, listElems...)
	elems = append(elems, zsetElems...)
	elems = append(elems, hashElems...)
	elems = append(elems, collectionTTLElems...)
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

func (t *txnContext) dispatchContext() (context.Context, context.CancelFunc) {
	// Derive a single redisDispatchTimeout-bounded context covering both
	// the commitTS allocation, the delta-key stream-deletion scans, and
	// the final Dispatch. The parent is the txnContext's own ctx (the
	// caller's dispatchCtx), not the server-lifetime handlerContext, so
	// an outer cancellation interrupts the prepare+dispatch promptly.
	parentCtx := t.ctx
	if parentCtx == nil {
		parentCtx = t.server.handlerContext()
	}
	return context.WithTimeout(parentCtx, redisDispatchTimeout)
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

func (t *txnContext) hasReplacement(key []byte) bool {
	_, ok := t.replacers[string(key)]
	return ok
}

func (t *txnContext) buildReplacementElems(ctx context.Context) ([]*kv.Elem[kv.OP], error) {
	if len(t.replacers) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(t.replacers))
	for k := range t.replacers {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var elems []*kv.Elem[kv.OP]
	for _, k := range keys {
		repl := t.replacers[k]
		t.trackWideCollectionFenceReads(repl.key)
		deleteElems, _, err := t.server.deleteLogicalKeyElems(ctx, repl.key, t.startTS)
		if err != nil {
			return nil, err
		}
		elems = append(elems, deleteElems...)
		elems = append(elems, redisTxnWideCollectionFenceElems(repl.key)...)
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   redisStrKey(repl.key),
			Value: encodeRedisStr(repl.value, repl.ttl),
		})
		if repl.ttl == nil {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(repl.key)})
			continue
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey(repl.key), Value: encodeRedisTTL(*repl.ttl)})
	}
	return elems, nil
}

func (t *txnContext) buildLogicalDeletionElems(ctx context.Context) ([]*kv.Elem[kv.OP], error) {
	if len(t.logicalDeletes) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(t.logicalDeletes))
	for k := range t.logicalDeletes {
		if _, ok := t.replacers[k]; ok {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var elems []*kv.Elem[kv.OP]
	for _, k := range keys {
		next, _, err := t.server.deleteLogicalKeyElems(ctx, t.logicalDeletes[k], t.startTS)
		if err != nil {
			return nil, err
		}
		elems = append(elems, next...)
		elems = append(elems, redisTxnWideCollectionFenceElems(t.logicalDeletes[k])...)
	}
	return elems, nil
}

func (t *txnContext) buildHashDeletionElems(ctx context.Context) ([]*kv.Elem[kv.OP], error) {
	return t.buildWideDeletionElems(ctx, t.hashDeletes,
		store.HashFieldScanPrefix, store.HashMetaKey, store.HashMetaDeltaScanPrefix, redisTxnWideHashFenceKey)
}

func (t *txnContext) buildSetDeletionElems(ctx context.Context) ([]*kv.Elem[kv.OP], error) {
	return t.buildWideDeletionElems(ctx, t.setDeletes,
		store.SetMemberScanPrefix, store.SetMetaKey, store.SetMetaDeltaScanPrefix, redisTxnWideSetFenceKey)
}

func (t *txnContext) buildWideDeletionElems(
	ctx context.Context,
	deletes map[string][]byte,
	fieldPrefix func([]byte) []byte,
	metaKey func([]byte) []byte,
	deltaPrefix func([]byte) []byte,
	fenceKey func([]byte) []byte,
) ([]*kv.Elem[kv.OP], error) {
	if len(deletes) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(deletes))
	for k := range deletes {
		if _, ok := t.replacers[k]; ok {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var elems []*kv.Elem[kv.OP]
	for _, k := range keys {
		key := deletes[k]
		t.trackReadKey(fenceKey(key))
		next, err := t.server.deleteWideColumnElems(ctx, t.startTS,
			fieldPrefix(key), metaKey(key), deltaPrefix(key))
		if err != nil {
			return nil, err
		}
		elems = append(elems, next...)
		elems = append(elems, redisTxnWideFenceElem(fenceKey(key)))
	}
	return elems, nil
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
		if t.skipWorkingKeyForReplacement(storageKey) {
			continue
		}
		elems = t.appendWorkingKeyElems(elems, storageKey, tv)
	}
	return elems
}

func (t *txnContext) skipWorkingKeyForReplacement(storageKey []byte) bool {
	if bytes.HasPrefix(storageKey, []byte(redisStrPrefix)) {
		userKey := storageKey[len(redisStrPrefix):]
		return t.hasReplacement(userKey)
	}
	if bytes.HasPrefix(storageKey, []byte(redisHLLPrefix)) {
		userKey := storageKey[len(redisHLLPrefix):]
		return t.hasReplacement(userKey)
	}
	return t.hasReplacement(storageKey)
}

func (t *txnContext) appendWorkingKeyElems(elems []*kv.Elem[kv.OP], storageKey []byte, tv *txnValue) []*kv.Elem[kv.OP] {
	if tv.deleted {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: storageKey})
		// Deleting a string anchor must also drop any stale !redis|ttl|
		// scan-index entry; buildTTLElems skips strings because it assumes
		// the inline-TTL path owns them.
		if bytes.HasPrefix(storageKey, []byte(redisStrPrefix)) {
			userKey := storageKey[len(redisStrPrefix):]
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(userKey)})
		}
		if bytes.HasPrefix(storageKey, []byte(redisHLLPrefix)) {
			userKey := storageKey[len(redisHLLPrefix):]
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey(userKey)})
		}
		return elems
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
	return append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: storageKey, Value: value})
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
		var emitted bool
		elems, emitted = t.appendListStateElems(elems, []byte(k), t.listStates[k], commitTS, seqInTxn)
		if emitted {
			seqInTxn++
		}
	}
	return elems
}

func (t *txnContext) appendListStateElems(elems []*kv.Elem[kv.OP], userKey []byte, st *listTxnState, commitTS uint64, seqInTxn uint32) ([]*kv.Elem[kv.OP], bool) {
	if t.hasReplacement(userKey) {
		return elems, false
	}
	if st.deleted {
		return appendListDeletionElems(elems, userKey, st), false
	}
	if len(st.appends) == 0 {
		return elems, false
	}
	if st.purge {
		elems = appendListDeletionElems(elems, userKey, st)
	}
	elems = appendListAppendElems(elems, userKey, st)
	if inline, ok := t.listInlineTTLCreateElems(string(userKey), userKey, st); ok {
		return append(elems, inline...), false
	}
	return appendListDeltaElem(elems, userKey, st, commitTS, seqInTxn), true
}

func appendListDeletionElems(elems []*kv.Elem[kv.OP], userKey []byte, st *listTxnState) []*kv.Elem[kv.OP] {
	if meta, ok := listDeleteMeta(st); ok {
		elems = appendListDeleteOps(elems, userKey, meta)
	}
	// Delete existing delta keys so they do not survive logical delete/purge.
	for _, dk := range st.existingDeltas {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: dk})
	}
	elems = append(elems, redisTxnWideListFenceElem(userKey))
	return elems
}

func appendListAppendElems(elems []*kv.Elem[kv.OP], userKey []byte, st *listTxnState) []*kv.Elem[kv.OP] {
	startSeq := st.meta.Head + st.meta.Len
	for i, v := range st.appends {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   listItemKey(userKey, startSeq+int64(i)),
			Value: v,
		})
	}
	return elems
}

func (t *txnContext) listInlineTTLCreateElems(key string, userKey []byte, st *listTxnState) ([]*kv.Elem[kv.OP], bool) {
	ttlMs, ok := t.collectionTTLMillis(key)
	if !ok || st.metaExists || (len(st.existingDeltas) != 0 && !st.purge) {
		return nil, false
	}
	meta := store.ListMeta{Head: st.meta.Head, Len: st.meta.Len + int64(len(st.appends)), ExpireAt: ttlMs}
	metaBytes, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, false
	}
	return []*kv.Elem[kv.OP]{
		redisTxnWideListFenceElem(userKey),
		{
			Op:    kv.Put,
			Key:   store.ListMetaKey(userKey),
			Value: metaBytes,
		},
	}, true
}

func appendListDeltaElem(elems []*kv.Elem[kv.OP], userKey []byte, st *listTxnState, commitTS uint64, seqInTxn uint32) []*kv.Elem[kv.OP] {
	deltaVal := store.MarshalListMetaDelta(store.ListMetaDelta{HeadDelta: 0, LenDelta: int64(len(st.appends))})
	return append(elems,
		redisTxnWideListFenceElem(userKey),
		&kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ListMetaDeltaKey(userKey, commitTS, seqInTxn),
			Value: deltaVal,
		},
	)
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
		if _, ok := t.replacers[k]; ok {
			continue
		}
		if !st.dirty {
			continue
		}
		key := []byte(k)
		inline, ok := t.zsetInlineTTLCreateElems(k, key, st)
		if ok {
			elems = append(elems, inline...)
			continue
		}
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
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Put, Key: redisZSetKey(key), Value: payload},
			redisTxnWideZSetFenceElem(key),
		)
	}
	return elems, nil
}

func (t *txnContext) zsetInlineTTLCreateElems(key string, userKey []byte, st *zsetTxnState) ([]*kv.Elem[kv.OP], bool) {
	ttlMs, ok := t.collectionTTLMillis(key)
	if !ok || st.exists || st.isWide || len(st.origMembers) != 0 || len(st.members) == 0 {
		return nil, false
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(st.members)*zsetOpsPerEntry+setWideColOverhead)
	for _, entry := range zsetMapToEntries(st.members) {
		elems = append(elems,
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetMemberKey(userKey, []byte(entry.Member)),
				Value: store.MarshalZSetScore(entry.Score),
			},
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetScoreKey(userKey, entry.Score, []byte(entry.Member)),
				Value: []byte{},
			},
		)
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ZSetMetaKey(userKey),
			Value: store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(st.members)), ExpireAt: ttlMs}),
		},
		redisTxnWideZSetFenceElem(userKey),
	)
	return elems, true
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
	if len(elems) != 0 {
		elems = append(elems, redisTxnWideZSetFenceElem(key))
	}
	return elems, lenDelta
}

func (t *txnContext) buildHashElems(commitTS uint64) []*kv.Elem[kv.OP] {
	keys := make([]string, 0, len(t.hashStates))
	for k := range t.hashStates {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var elems []*kv.Elem[kv.OP]
	var seqInTxn uint32
	for _, k := range keys {
		var emitted bool
		elems, emitted = t.appendHashStateElems(elems, []byte(k), t.hashStates[k], commitTS, seqInTxn)
		if emitted {
			seqInTxn++
		}
	}
	return elems
}

func (t *txnContext) appendHashStateElems(elems []*kv.Elem[kv.OP], key []byte, st *hashTxnState, commitTS uint64, seqInTxn uint32) ([]*kv.Elem[kv.OP], bool) {
	if !st.dirty || st.deleted || t.hasReplacement(key) {
		return elems, false
	}
	if st.legacy {
		return append(elems, buildHashLegacyRewriteElems(key, st.fields, t.hashLegacyRewriteExpireAt(string(key), st))...), false
	}
	if inline, ok := t.hashInlineTTLCreateElems(string(key), key, st); ok {
		return append(elems, inline...), false
	}
	next, newFields := appendHashChangedFieldElems(elems, key, st)
	if newFields == 0 {
		return next, false
	}
	return appendHashDeltaElem(next, key, newFields, commitTS, seqInTxn), true
}

func appendHashChangedFieldElems(elems []*kv.Elem[kv.OP], key []byte, st *hashTxnState) ([]*kv.Elem[kv.OP], int64) {
	var newFields int64
	for _, field := range sortedHashDirtyFieldNames(st.dirtyFields) {
		value := st.fields[field]
		_, existed := st.origFields[field]
		if !existed {
			newFields++
		}
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(key, []byte(field)),
			Value: bytes.Clone(value),
		})
	}
	return elems, newFields
}

func appendHashDeltaElem(elems []*kv.Elem[kv.OP], key []byte, newFields int64, commitTS uint64, seqInTxn uint32) []*kv.Elem[kv.OP] {
	deltaVal := store.MarshalHashMetaDelta(store.HashMetaDelta{LenDelta: newFields})
	return append(elems,
		redisTxnWideHashFenceElem(key),
		&kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashMetaDeltaKey(key, commitTS, seqInTxn),
			Value: deltaVal,
		},
	)
}

const hashLegacyRewriteOverhead = 2

func (t *txnContext) hashLegacyRewriteExpireAt(key string, st *hashTxnState) uint64 {
	if ttlMs, ok := t.collectionTTLMillis(key); ok {
		return ttlMs
	}
	ttlState := t.ttlStates[key]
	if ttlState != nil && ttlState.dirty && ttlState.value == nil {
		return 0
	}
	return st.legacyExpireAt
}

func (t *txnContext) hashInlineTTLCreateElems(key string, userKey []byte, st *hashTxnState) ([]*kv.Elem[kv.OP], bool) {
	ttlMs, ok := t.collectionTTLMillis(key)
	if !ok || !t.isHashCreate(key) || len(st.fields) == 0 {
		return nil, false
	}
	return buildHashFullRewriteElems(userKey, st.fields, ttlMs), true
}

func (t *txnContext) isHashCreate(key string) bool {
	_, ok := t.hashCreates[key]
	return ok
}

func buildHashLegacyRewriteElems(key []byte, fields map[string][]byte, expireAt uint64) []*kv.Elem[kv.OP] {
	elems := buildHashFullRewriteElems(key, fields, expireAt)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisHashKey(key)})
	return elems
}

func buildHashFullRewriteElems(key []byte, fields map[string][]byte, expireAt uint64) []*kv.Elem[kv.OP] {
	fieldNames := sortedHashFieldNames(fields)
	elems := make([]*kv.Elem[kv.OP], 0, len(fieldNames)+hashLegacyRewriteOverhead)
	for _, field := range fieldNames {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey(key, []byte(field)),
			Value: bytes.Clone(fields[field]),
		})
	}
	elems = append(elems,
		&kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashMetaKey(key),
			Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(fieldNames)), ExpireAt: expireAt}),
		},
		redisTxnWideHashFenceElem(key),
	)
	return elems
}

func sortedHashFieldNames(fields map[string][]byte) []string {
	names := make([]string, 0, len(fields))
	for field := range fields {
		names = append(names, field)
	}
	sort.Strings(names)
	return names
}

func sortedHashDirtyFieldNames(fields map[string]struct{}) []string {
	names := make([]string, 0, len(fields))
	for field := range fields {
		names = append(names, field)
	}
	sort.Strings(names)
	return names
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
func (t *txnContext) buildTTLElems(skip map[string]struct{}) []*kv.Elem[kv.OP] {
	var elems []*kv.Elem[kv.OP]
	for k, st := range t.ttlStates {
		if !st.dirty {
			continue
		}
		if _, blocked := skip[k]; blocked {
			continue
		}
		if _, ok := t.replacers[k]; ok {
			// A staged string replacement owns both the inline TTL and the
			// scan-index TTL. applyPositiveExpire updates the replacement TTL
			// directly so EXPIRE after SET is emitted by buildReplacementElems.
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

func (t *txnContext) collectionTTLMillis(key string) (uint64, bool) {
	st := t.ttlStates[key]
	if st == nil || !st.dirty || st.value == nil {
		return 0, false
	}
	return redisExpireAtMillis(*st.value), true
}

func (t *txnContext) buildCollectionTTLElems(ctx context.Context) ([]*kv.Elem[kv.OP], map[string]struct{}, error) {
	if len(t.collectionExpireTypes) == 0 {
		return nil, nil, nil
	}
	keys := make([]string, 0, len(t.collectionExpireTypes))
	for key := range t.collectionExpireTypes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	var elems []*kv.Elem[kv.OP]
	var skipTTLIndex map[string]struct{}
	for _, key := range keys {
		built, skipIndex, err := t.collectionTTLElemsForKey(ctx, key)
		if err != nil {
			return nil, nil, err
		}
		if !skipIndex {
			elems = append(elems, built...)
			continue
		}
		if skipTTLIndex == nil {
			skipTTLIndex = map[string]struct{}{}
		}
		skipTTLIndex[key] = struct{}{}
	}
	return elems, skipTTLIndex, nil
}

func (t *txnContext) collectionTTLElemsForKey(ctx context.Context, key string) ([]*kv.Elem[kv.OP], bool, error) {
	st := t.ttlStates[key]
	if st == nil || !st.dirty || st.value == nil {
		return nil, false, nil
	}
	typ := t.collectionExpireTypes[key]
	if t.collectionTTLRebuildWouldUseStaleBase(key, typ) {
		return nil, false, nil
	}
	built, ok, err := t.server.collectionExpireElems(
		ctx,
		[]byte(key),
		t.startTS,
		typ,
		redisExpireAtMillis(*st.value),
	)
	if err != nil || ok {
		return built, false, err
	}
	if t.hasStagedInlineTTLCollectionCreate(key, typ) {
		return nil, false, nil
	}
	return nil, true, nil
}

func (t *txnContext) hasStagedInlineTTLCollectionCreate(key string, typ redisValueType) bool {
	switch typ {
	case redisTypeList:
		return t.hasStagedInlineTTLListCreate(key)
	case redisTypeHash:
		return t.hasStagedInlineTTLHashCreate(key)
	case redisTypeZSet:
		return t.hasStagedInlineTTLZSetCreate(key)
	case redisTypeNone, redisTypeString, redisTypeSet, redisTypeStream:
		return false
	}
	return false
}

func (t *txnContext) hasStagedInlineTTLListCreate(key string) bool {
	st, ok := t.listStates[key]
	return ok && !st.deleted && len(st.appends) > 0 && !st.metaExists && (len(st.existingDeltas) == 0 || st.purge)
}

func (t *txnContext) hasStagedInlineTTLHashCreate(key string) bool {
	st, ok := t.hashStates[key]
	return ok && st.dirty && !st.deleted && t.isHashCreate(key) && len(st.fields) > 0
}

func (t *txnContext) hasStagedInlineTTLZSetCreate(key string) bool {
	st, ok := t.zsetStates[key]
	return ok && st.dirty && !st.exists && !st.isWide && len(st.origMembers) == 0 && len(st.members) > 0
}

func (t *txnContext) collectionTTLRebuildWouldUseStaleBase(key string, typ redisValueType) bool {
	if !t.hasStagedLogicalDelete(key) {
		return false
	}
	switch typ {
	case redisTypeList:
		return t.listTTLRebuildWouldUseStaleBase(key)
	case redisTypeHash:
		return t.hashTTLRebuildWouldUseStaleBase(key)
	case redisTypeZSet:
		return t.zsetTTLRebuildWouldUseStaleBase(key)
	case redisTypeSet, redisTypeStream, redisTypeNone, redisTypeString:
		return false
	}
	return false
}

func (t *txnContext) hasStagedLogicalDelete(key string) bool {
	if _, ok := t.deletedKeys[key]; ok {
		return true
	}
	if _, ok := t.logicalDeletes[key]; ok {
		return true
	}
	if _, ok := t.hashDeletes[key]; ok {
		return true
	}
	if _, ok := t.setDeletes[key]; ok {
		return true
	}
	if _, ok := t.streamDeletions[key]; ok {
		return true
	}
	return false
}

func (t *txnContext) listTTLRebuildWouldUseStaleBase(key string) bool {
	st, ok := t.listStates[key]
	return ok && !st.deleted && len(st.appends) > 0
}

func (t *txnContext) hashTTLRebuildWouldUseStaleBase(key string) bool {
	st, ok := t.hashStates[key]
	return ok && st.dirty && !st.deleted && len(st.fields) > 0
}

func (t *txnContext) zsetTTLRebuildWouldUseStaleBase(key string) bool {
	st, ok := t.zsetStates[key]
	return ok && st.dirty && len(st.members) > 0
}

func (r *RedisServer) runTransaction(queue []redcon.Command) ([]redisResult, error) {
	if transactionHasHeavyCommand(queue) {
		var results []redisResult
		var err error
		if ok := r.runWithHeavyCommandSlot(func() {
			results, err = r.runTransactionDirect(queue)
		}); !ok {
			return nil, errRedisHeavyCommandPoolFull
		}
		return results, err
	}
	return r.runTransactionDirect(queue)
}

func (r *RedisServer) runTransactionDirect(queue []redcon.Command) ([]redisResult, error) {
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
			server:                r,
			ctx:                   dispatchCtx,
			working:               map[string]*txnValue{},
			replacers:             map[string]*stringReplacement{},
			listStates:            map[string]*listTxnState{},
			hashStates:            map[string]*hashTxnState{},
			zsetStates:            map[string]*zsetTxnState{},
			ttlStates:             map[string]*ttlTxnState{},
			readKeys:              map[string][]byte{},
			deletedKeys:           map[string]struct{}{},
			logicalDeletes:        map[string][]byte{},
			hashDeletes:           map[string][]byte{},
			setDeletes:            map[string][]byte{},
			hashCreates:           map[string]struct{}{},
			collectionExpireTypes: map[string]redisValueType{},
			streamDeletions:       map[string][]byte{},
			startTS:               startTS,
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
	commitTS, allocErr := r.nextCommitTSAfter(ctx, pending.startTS, "redis exec reuse: allocate commitTS")
	if allocErr != nil {
		return nil, false, errors.WithStack(allocErr)
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
	// This path owns an exact reusable write set plus PrevCommitTS, so it can
	// safely opt in to retrying an otherwise-ambiguous forwarded conflict.
	// Normalize before the typed conflict branch; the generic retry loop keeps
	// raw wire write conflicts fail-closed for callers without this protection.
	dispErr = normalizeRetryableRedisTxnErr(dispErr)
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
		server:                r,
		ctx:                   dispatchCtx,
		working:               map[string]*txnValue{},
		replacers:             map[string]*stringReplacement{},
		listStates:            map[string]*listTxnState{},
		hashStates:            map[string]*hashTxnState{},
		zsetStates:            map[string]*zsetTxnState{},
		ttlStates:             map[string]*ttlTxnState{},
		readKeys:              map[string][]byte{},
		deletedKeys:           map[string]struct{}{},
		logicalDeletes:        map[string][]byte{},
		hashDeletes:           map[string][]byte{},
		setDeletes:            map[string][]byte{},
		hashCreates:           map[string]struct{}{},
		collectionExpireTypes: map[string]redisValueType{},
		streamDeletions:       map[string][]byte{},
		startTS:               startTS,
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
		// Preserve the exact attempt for a forwarded conflict only after
		// restoring its typed form. runTransactionWithDedup can then reuse this
		// write set instead of replaying the EXEC body from a new snapshot.
		dispErr = normalizeRetryableRedisTxnErr(dispErr)
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
