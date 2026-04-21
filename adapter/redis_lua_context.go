package adapter

import (
	"context"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type luaScriptContext struct {
	server  *RedisServer
	startTS uint64
	readPin *kv.ActiveTimestampToken

	// ctx is the request-scoped context captured at newLuaScriptContext
	// time. New cmd* handlers should propagate it into store operations
	// so the Eval's deadline and cancellation reach storage.
	//
	// Existing handlers in this file still use context.Background() at
	// their store call sites -- a historical pattern from before we had
	// a ctx available. Migrating those is tracked as a follow-up PR;
	// see PR #570's review for the gemini note on scope.
	ctx context.Context

	redisCallDuration time.Duration
	redisCallCount    int

	touched     map[string]struct{}
	deleted     map[string]bool
	everDeleted map[string]bool

	strings map[string]*luaStringState
	lists   map[string]*luaListState
	hashes  map[string]*luaHashState
	sets    map[string]*luaSetState
	zsets   map[string]*luaZSetState
	streams map[string]*luaStreamState
	ttls    map[string]*luaTTLState
}

type luaStringState struct {
	loaded bool
	exists bool
	dirty  bool
	value  []byte
}

type luaListState struct {
	loaded       bool
	exists       bool
	dirty        bool
	materialized bool
	meta         store.ListMeta
	leftTrim     int64
	rightTrim    int64
	leftValues   []string
	rightValues  []string
	values       []string
}

type luaHashState struct {
	loaded bool
	exists bool
	dirty  bool
	value  redisHashValue
}

type luaSetState struct {
	loaded  bool
	exists  bool
	dirty   bool
	members map[string]struct{}
}

type luaZSetState struct {
	loaded         bool
	exists         bool
	dirty          bool
	membersLoaded  bool // all members loaded into st.members
	legacyBlobBase bool // existing data is in legacy blob format
	// physicallyExistsAtStart is true when the key had physical ZSet data in
	// storage at script-start time, even if logically absent due to TTL expiry.
	// It is used by zsetCommitPlan to force a full commit (and thus cleanup of
	// stale wide-column rows) when recreating a TTL-expired ZSet.
	physicallyExistsAtStart bool
	members                 map[string]float64  // full member map (only when membersLoaded=true)
	added                   map[string]float64  // members added/updated in this script (delta)
	storageScores           map[string]*float64 // original scores from Pebble (nil ptr = absent)
	removed                 map[string]struct{} // members explicitly removed in this script
	lenDelta                int64               // net cardinality change (for delta commit)

	// resolvedBaseCard caches the result of resolveZSetMeta (storage-level cardinality)
	// so ZCARD does not re-scan delta keys on every call within the same script.
	resolvedBaseCard       int64
	resolvedBaseCardCached bool
}

type luaStreamState struct {
	loaded bool
	exists bool
	dirty  bool
	value  redisStreamValue
}

type luaTTLState struct {
	loaded bool
	dirty  bool
	value  *time.Time
}

func (s *luaListState) currentLen() int64 {
	if !s.exists {
		return 0
	}
	if s.materialized {
		return int64(len(s.values))
	}
	return int64(len(s.leftValues)) + s.remainingOriginalLen() + int64(len(s.rightValues))
}

func (s *luaListState) remainingOriginalLen() int64 {
	remaining := s.meta.Len - s.leftTrim - s.rightTrim
	if remaining < 0 {
		return 0
	}
	return remaining
}

type zScoreBound struct {
	kind      int
	score     float64
	inclusive bool
}

const (
	zBoundNegInf = -1
	zBoundValue  = 0
	zBoundPosInf = 1

	zsetWithScoresArgIndex = 3
	xAddMinArgs            = 3
	xTrimMinArgs           = 4

	// Element counts for ZSet commit slice pre-allocation.
	zsetElemsPerMember  = 2 // PUT member key + PUT score index key
	zsetMetaBaseElems   = 1 // PUT or DEL ZSetMetaKey
	zsetElemsPerAdded   = 3 // optional DEL stale score key + PUT member key + PUT score key
	zsetElemsPerRemoved = 2 // DEL member key + DEL score key
)

type luaCommandHandler func(*luaScriptContext, []string) (luaReply, error)
type luaRenameHandler func(*luaScriptContext, []byte, []byte) error

var luaCommandHandlers = map[string]luaCommandHandler{
	cmdDel:              (*luaScriptContext).cmdDel,
	cmdExists:           (*luaScriptContext).cmdExists,
	cmdGet:              (*luaScriptContext).cmdGet,
	cmdSet:              (*luaScriptContext).cmdSet,
	cmdIncr:             (*luaScriptContext).cmdIncr,
	cmdPTTL:             (*luaScriptContext).cmdPTTL,
	cmdPExpire:          (*luaScriptContext).cmdPExpire,
	cmdType:             (*luaScriptContext).cmdType,
	cmdRename:           (*luaScriptContext).cmdRename,
	cmdHGet:             (*luaScriptContext).cmdHGet,
	cmdHSet:             (*luaScriptContext).cmdHSet,
	cmdHMGet:            (*luaScriptContext).cmdHMGet,
	cmdHMSet:            (*luaScriptContext).cmdHMSet,
	cmdHGetAll:          (*luaScriptContext).cmdHGetAll,
	cmdHIncrBy:          (*luaScriptContext).cmdHIncrBy,
	cmdHDel:             (*luaScriptContext).cmdHDel,
	cmdHExists:          (*luaScriptContext).cmdHExists,
	cmdHLen:             (*luaScriptContext).cmdHLen,
	cmdLPush:            (*luaScriptContext).cmdLPush,
	cmdRPush:            (*luaScriptContext).cmdRPush,
	cmdLLen:             (*luaScriptContext).cmdLLen,
	cmdLIndex:           (*luaScriptContext).cmdLIndex,
	cmdLRange:           (*luaScriptContext).cmdLRange,
	cmdLRem:             (*luaScriptContext).cmdLRem,
	cmdLTrim:            (*luaScriptContext).cmdLTrim,
	cmdLPop:             (*luaScriptContext).cmdLPop,
	cmdRPop:             (*luaScriptContext).cmdRPop,
	cmdRPopLPush:        (*luaScriptContext).cmdRPopLPush,
	cmdLPos:             (*luaScriptContext).cmdLPos,
	cmdLSet:             (*luaScriptContext).cmdLSet,
	cmdSAdd:             (*luaScriptContext).cmdSAdd,
	cmdSCard:            (*luaScriptContext).cmdSCard,
	cmdSMembers:         (*luaScriptContext).cmdSMembers,
	cmdSRem:             (*luaScriptContext).cmdSRem,
	cmdSIsMember:        (*luaScriptContext).cmdSIsMember,
	cmdZAdd:             (*luaScriptContext).cmdZAdd,
	cmdZCard:            (*luaScriptContext).cmdZCard,
	cmdZCount:           (*luaScriptContext).cmdZCount,
	cmdZRange:           (*luaScriptContext).cmdZRange,
	cmdZRangeByScore:    (*luaScriptContext).cmdZRangeByScoreAsc,
	cmdZRevRange:        (*luaScriptContext).cmdZRevRange,
	cmdZRevRangeByScore: (*luaScriptContext).cmdZRevRangeByScore,
	cmdZScore:           (*luaScriptContext).cmdZScore,
	cmdZRem:             (*luaScriptContext).cmdZRem,
	cmdZPopMin:          (*luaScriptContext).cmdZPopMin,
	cmdZRemRangeByRank:  (*luaScriptContext).cmdZRemRangeByRank,
	cmdZRemRangeByScore: (*luaScriptContext).cmdZRemRangeByScore,
	cmdXAdd:             (*luaScriptContext).cmdXAdd,
	cmdXTrim:            (*luaScriptContext).cmdXTrim,
	cmdSetEx:            (*luaScriptContext).cmdSetEx,
	cmdGetDel:           (*luaScriptContext).cmdGetDel,
	cmdSetNX:            (*luaScriptContext).cmdSetNX,
}

var luaRenameHandlers = map[redisValueType]luaRenameHandler{
	redisTypeString: (*luaScriptContext).renameStringValue,
	redisTypeList:   (*luaScriptContext).renameListValue,
	redisTypeHash:   (*luaScriptContext).renameHashValue,
	redisTypeSet:    (*luaScriptContext).renameSetValue,
	redisTypeZSet:   (*luaScriptContext).renameZSetValue,
	redisTypeStream: (*luaScriptContext).renameStreamValue,
}

func newLuaScriptContext(ctx context.Context, server *RedisServer) (*luaScriptContext, error) {
	// LeaseRead confirms leadership at most once per LeaseDuration window;
	// inside the window it returns immediately without a Raft round-trip.
	// All subsequent reads within the script use snapshotGetAt at startTS,
	// so leadership is verified at most once per script and amortised across
	// scripts via the lease.
	if _, err := kv.LeaseReadThrough(server.coordinator, ctx); err != nil {
		return nil, errors.WithStack(err)
	}
	startTS := server.readTS()
	return &luaScriptContext{
		server:      server,
		startTS:     startTS,
		readPin:     server.pinReadTS(startTS),
		ctx:         ctx,
		touched:     map[string]struct{}{},
		deleted:     map[string]bool{},
		everDeleted: map[string]bool{},
		strings:     map[string]*luaStringState{},
		lists:       map[string]*luaListState{},
		hashes:      map[string]*luaHashState{},
		sets:        map[string]*luaSetState{},
		zsets:       map[string]*luaZSetState{},
		streams:     map[string]*luaStreamState{},
		ttls:        map[string]*luaTTLState{},
	}, nil
}

func (c *luaScriptContext) Close() {
	if c == nil || c.readPin == nil {
		return
	}
	c.readPin.Release()
}

func (c *luaScriptContext) exec(command string, args []string) (luaReply, error) {
	execStart := time.Now()
	c.redisCallCount++
	var reply luaReply
	var err error
	if handler, ok := luaCommandHandlers[command]; ok {
		reply, err = handler(c, args)
	} else {
		err = errors.WithStack(errors.Newf("ERR unsupported command '%s'", command))
	}
	c.redisCallDuration += time.Since(execStart)
	return reply, err
}

func (c *luaScriptContext) markTouched(key []byte) {
	c.touched[string(key)] = struct{}{}
}

func (c *luaScriptContext) ttlState(key []byte) *luaTTLState {
	k := string(key)
	if st, ok := c.ttls[k]; ok {
		return st
	}
	st := &luaTTLState{}
	c.ttls[k] = st
	return st
}

func (c *luaScriptContext) loadTTL(key []byte) (*time.Time, error) {
	st := c.ttlState(key)
	if st.loaded {
		return st.value, nil
	}
	value, err := c.server.ttlAt(context.Background(), key, c.startTS)
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.value = value
	return value, nil
}

func (c *luaScriptContext) clearTTL(key []byte) {
	st := c.ttlState(key)
	st.loaded = true
	st.dirty = true
	st.value = nil
}

func (c *luaScriptContext) setTTLValue(key []byte, value *time.Time) {
	st := c.ttlState(key)
	st.loaded = true
	st.dirty = true
	st.value = value
}

func (c *luaScriptContext) deleteLogical(key []byte) {
	k := string(key)
	c.markTouched(key)
	c.deleted[k] = true
	c.everDeleted[k] = true
	c.clearTTL(key)

	if st, ok := c.strings[k]; ok {
		st.loaded = true
		st.exists = false
		st.dirty = true
		st.value = nil
	}
	if st, ok := c.lists[k]; ok {
		st.loaded = true
		st.exists = false
		st.dirty = true
		st.materialized = false
		st.meta = store.ListMeta{}
		st.leftTrim = 0
		st.rightTrim = 0
		st.leftValues = nil
		st.rightValues = nil
		st.values = nil
	}
	if st, ok := c.hashes[k]; ok {
		st.loaded = true
		st.exists = false
		st.dirty = true
		st.value = redisHashValue{}
	}
	if st, ok := c.sets[k]; ok {
		st.loaded = true
		st.exists = false
		st.dirty = true
		st.members = map[string]struct{}{}
	}
	if st, ok := c.zsets[k]; ok {
		st.loaded = true
		st.exists = false
		st.dirty = true
		st.membersLoaded = true
		st.members = map[string]float64{}
		st.added = map[string]float64{}
		st.removed = map[string]struct{}{}
		st.storageScores = map[string]*float64{}
		st.lenDelta = 0
	}
	if st, ok := c.streams[k]; ok {
		st.loaded = true
		st.exists = false
		st.dirty = true
		st.value = redisStreamValue{}
	}
}

func (c *luaScriptContext) cachedType(key []byte) (redisValueType, bool) {
	k := string(key)
	for _, candidate := range c.cachedLoadedTypes(k) {
		if candidate.exists {
			return candidate.typ, true
		}
	}
	if c.deleted[k] {
		return redisTypeNone, true
	}
	return redisTypeNone, false
}

func (c *luaScriptContext) cachedLoadedTypes(key string) []cachedLuaType {
	return []cachedLuaType{
		{typ: redisTypeString, exists: hasLoadedStringValue(c.strings[key])},
		{typ: redisTypeList, exists: hasLoadedListValue(c.lists[key])},
		{typ: redisTypeHash, exists: hasLoadedHashValue(c.hashes[key])},
		{typ: redisTypeSet, exists: hasLoadedSetValue(c.sets[key])},
		{typ: redisTypeZSet, exists: hasLoadedZSetValue(c.zsets[key])},
		{typ: redisTypeStream, exists: hasLoadedStreamValue(c.streams[key])},
	}
}

type cachedLuaType struct {
	typ    redisValueType
	exists bool
}

func hasLoadedStringValue(st *luaStringState) bool {
	return st != nil && st.loaded && st.exists
}

func hasLoadedListValue(st *luaListState) bool {
	return st != nil && st.loaded && st.exists
}

func hasLoadedHashValue(st *luaHashState) bool {
	return st != nil && st.loaded && st.exists
}

func hasLoadedSetValue(st *luaSetState) bool {
	return st != nil && st.loaded && st.exists
}

func hasLoadedZSetValue(st *luaZSetState) bool {
	return st != nil && st.loaded && st.exists
}

func hasLoadedStreamValue(st *luaStreamState) bool {
	return st != nil && st.loaded && st.exists
}

func (c *luaScriptContext) keyType(key []byte) (redisValueType, error) {
	if typ, ok := c.cachedType(key); ok {
		if typ != redisTypeNone {
			return typ, c.ensureKeyNotExpired(key)
		}
		return typ, nil
	}

	typ, err := c.server.keyTypeAt(context.Background(), key, c.startTS)
	if err != nil {
		return redisTypeNone, err
	}
	return typ, nil
}

func (c *luaScriptContext) ensureKeyNotExpired(key []byte) error {
	ttl, err := c.loadTTL(key)
	if err != nil {
		return err
	}
	if ttl != nil && !ttl.After(time.Now()) {
		return errors.WithStack(store.ErrKeyNotFound)
	}
	return nil
}

func (c *luaScriptContext) logicalExists(key []byte) (bool, error) {
	typ, err := c.keyType(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return typ != redisTypeNone, nil
}

func (c *luaScriptContext) stringState(key []byte) (*luaStringState, error) {
	k := string(key)
	if st, ok := c.strings[k]; ok {
		return st, nil
	}
	st := &luaStringState{}
	c.strings[k] = st

	typ, err := c.keyType(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.loaded = true
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		st.loaded = true
		return st, nil
	}
	if typ != redisTypeString {
		return nil, wrongTypeError()
	}

	value, _, err := c.server.readRedisStringAtSnapshot(key, c.startTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.loaded = true
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.exists = true
	st.value = append([]byte(nil), value...)
	return st, nil
}

func (c *luaScriptContext) listState(key []byte) (*luaListState, error) {
	k := string(key)
	if st, ok := c.lists[k]; ok {
		return st, nil
	}
	st := &luaListState{}
	c.lists[k] = st

	typ, err := c.keyType(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.loaded = true
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		st.loaded = true
		return st, nil
	}
	if typ != redisTypeList {
		return nil, wrongTypeError()
	}

	meta, exists, err := c.server.resolveListMeta(context.Background(), key, c.startTS)
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.exists = exists
	st.meta = meta
	return st, nil
}

func (c *luaScriptContext) materializeList(key []byte, st *luaListState) error {
	if st.materialized {
		return nil
	}
	if !st.exists {
		st.materialized = true
		st.values = nil
		return nil
	}

	length := st.currentLen()
	maxInt64 := int64(math.MaxInt)
	if length > maxInt64 {
		length = maxInt64
	}
	values := make([]string, 0, int(length))
	values = append(values, st.leftValues...)
	if remaining := st.remainingOriginalLen(); remaining > 0 {
		start := st.leftTrim
		end := st.meta.Len - st.rightTrim - 1
		fetched, err := c.server.fetchListRange(context.Background(), key, st.meta, start, end, c.startTS)
		if err != nil {
			return err
		}
		values = append(values, fetched...)
	}
	values = append(values, st.rightValues...)

	st.materialized = true
	st.leftTrim = 0
	st.rightTrim = 0
	st.leftValues = nil
	st.rightValues = nil
	st.values = values
	return nil
}

func (c *luaScriptContext) materializedListForRead(key string) (*luaListState, bool, error) {
	st, err := c.listState([]byte(key))
	if err != nil {
		return nil, false, err
	}
	if !st.exists || st.currentLen() == 0 {
		return st, false, nil
	}
	if err := c.materializeList([]byte(key), st); err != nil {
		return nil, false, err
	}
	return st, len(st.values) > 0, nil
}

func (c *luaScriptContext) hashState(key []byte) (*luaHashState, error) {
	k := string(key)
	if st, ok := c.hashes[k]; ok {
		return st, nil
	}
	st := &luaHashState{}
	c.hashes[k] = st

	typ, err := c.keyType(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.loaded = true
		st.value = redisHashValue{}
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		st.loaded = true
		st.value = redisHashValue{}
		return st, nil
	}
	if typ != redisTypeHash {
		return nil, wrongTypeError()
	}

	value, err := c.server.loadHashAt(context.Background(), key, c.startTS)
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.exists = true
	st.value = maps.Clone(value)
	return st, nil
}

func (c *luaScriptContext) setState(key []byte) (*luaSetState, error) {
	k := string(key)
	if st, ok := c.sets[k]; ok {
		return st, nil
	}
	st := &luaSetState{}
	c.sets[k] = st

	typ, err := c.keyType(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.loaded = true
		st.members = map[string]struct{}{}
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		st.loaded = true
		st.members = map[string]struct{}{}
		return st, nil
	}
	if typ != redisTypeSet {
		return nil, wrongTypeError()
	}

	value, err := c.server.loadSetAt(context.Background(), "set", key, c.startTS)
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.exists = true
	st.members = map[string]struct{}{}
	for _, member := range value.Members {
		st.members[member] = struct{}{}
	}
	return st, nil
}

func (c *luaScriptContext) zsetState(key []byte) (*luaZSetState, error) {
	k := string(key)
	if st, ok := c.zsets[k]; ok {
		return st, nil
	}
	st := &luaZSetState{
		added:         map[string]float64{},
		removed:       map[string]struct{}{},
		storageScores: map[string]*float64{},
	}
	c.zsets[k] = st

	typ, err := c.keyType(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.loaded = true
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		// Check whether physical ZSet data exists despite the key being logically
		// absent (TTL-expired). If so, mark physicallyExistsAtStart so that
		// zsetCommitPlan can force a full commit to clean up stale storage rows.
		rawTyp, rawErr := c.server.rawKeyTypeAt(context.Background(), key, c.startTS)
		if rawErr != nil {
			return nil, rawErr
		}
		st.physicallyExistsAtStart = rawTyp == redisTypeZSet
		st.loaded = true
		return st, nil
	}
	if typ != redisTypeZSet {
		return nil, wrongTypeError()
	}

	// Probe for wide-column format with a single seek instead of a full scan.
	prefix := store.ZSetMemberScanPrefix(key)
	kvs, err := c.server.store.ScanAt(context.Background(), prefix, store.PrefixScanEnd(prefix), 1, c.startTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	st.loaded = true
	st.exists = true
	if len(kvs) > 0 {
		return st, nil
	}
	// No !zs|mem| rows does not imply legacy-blob: a wide-column ZSet that had
	// all members deleted leaves only meta/delta keys behind. Probe the legacy
	// blob key directly to distinguish these cases.
	blobExists, err := c.server.store.ExistsAt(context.Background(), redisZSetKey(key), c.startTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	st.legacyBlobBase = blobExists
	return st, nil
}

// ensureZSetLoaded loads all ZSet members from storage if not already loaded,
// merging any in-script delta (added/removed) into st.members.
func (c *luaScriptContext) ensureZSetLoaded(st *luaZSetState, key []byte) error {
	if st.membersLoaded {
		return nil
	}
	members := map[string]float64{}
	if st.exists {
		value, _, err := c.server.loadZSetAt(context.Background(), key, c.startTS)
		if err != nil {
			return err
		}
		members = zsetEntriesToMap(value.Entries)
	}
	for member := range st.removed {
		delete(members, member)
	}
	for member, score := range st.added {
		members[member] = score
	}
	st.members = members
	st.membersLoaded = true
	return nil
}

// memberScore returns the current score of a ZSet member, preferring in-script
// state over Pebble. For wide-column ZSets it performs an O(log N) point lookup
// instead of loading the entire collection.
func (c *luaScriptContext) memberScore(st *luaZSetState, key []byte, member string) (float64, bool, error) {
	if st.membersLoaded {
		score, ok := st.members[member]
		return score, ok, nil
	}
	if score, ok, handled := zsetDeltaScore(st, member); handled {
		return score, ok, nil
	}
	return c.memberScoreFromPebble(st, key, member)
}

// zsetDeltaScore checks the in-script delta state (removed/added/storageScores cache)
// for a member. Returns (score, exists, handled); handled=false means the caller
// must fall through to a Pebble point lookup.
func zsetDeltaScore(st *luaZSetState, member string) (float64, bool, bool) {
	if _, removed := st.removed[member]; removed {
		return 0, false, true
	}
	if score, ok := st.added[member]; ok {
		return score, true, true
	}
	if cached, ok := st.storageScores[member]; ok {
		if cached == nil {
			return 0, false, true
		}
		return *cached, true, true
	}
	return 0, false, false
}

// memberScoreFromPebble resolves a member score from Pebble storage.
// For legacy-blob ZSets it triggers a full load; for wide-column it does an
// O(log N) point lookup and caches the result in st.storageScores.
func (c *luaScriptContext) memberScoreFromPebble(st *luaZSetState, key []byte, member string) (float64, bool, error) {
	if !st.exists {
		st.storageScores[member] = nil
		return 0, false, nil
	}
	if st.legacyBlobBase {
		if err := c.ensureZSetLoaded(st, key); err != nil {
			return 0, false, err
		}
		score, ok := st.members[member]
		return score, ok, nil
	}
	raw, err := c.server.store.GetAt(context.Background(), store.ZSetMemberKey(key, []byte(member)), c.startTS)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.storageScores[member] = nil
		return 0, false, nil
	}
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	score, err := store.UnmarshalZSetScore(raw)
	if err != nil {
		return 0, false, errors.WithStack(err)
	}
	st.storageScores[member] = &score
	return score, true, nil
}

// zsetCard returns the ZSet cardinality. When members are fully loaded it uses
// len(st.members); otherwise it reads the base metadata from storage and adds
// the in-script lenDelta, which is O(log N). The storage-level result is cached
// in st so repeated ZCARD calls within the same script do not re-scan delta keys.
// For legacy-blob ZSets there is no wide-column ZSetMetaKey, so resolveZSetMeta
// would return 0; ensureZSetLoaded is used instead to get the real count.
func (c *luaScriptContext) zsetCard(st *luaZSetState, key []byte) (int64, error) {
	if st.membersLoaded {
		return int64(len(st.members)), nil
	}
	if !st.exists {
		return 0, nil
	}
	if st.legacyBlobBase {
		if err := c.ensureZSetLoaded(st, key); err != nil {
			return 0, err
		}
		return int64(len(st.members)), nil
	}
	if !st.resolvedBaseCardCached {
		baseLen, _, err := c.server.resolveZSetMeta(context.Background(), key, c.startTS)
		if err != nil {
			return 0, err
		}
		st.resolvedBaseCard = baseLen
		st.resolvedBaseCardCached = true
	}
	return st.resolvedBaseCard + st.lenDelta, nil
}

// zsetStateForRead returns a fully-loaded ZSet state for commands that need the
// complete member set (ZRANGE, ZCOUNT, ZPOPMIN, etc.). Returns (st, false, nil)
// when the key does not exist; callers must check the bool before using st.
func (c *luaScriptContext) zsetStateForRead(key []byte) (*luaZSetState, bool, error) {
	st, err := c.zsetState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if !st.exists {
		return st, false, nil
	}
	if err := c.ensureZSetLoaded(st, key); err != nil {
		return nil, false, err
	}
	return st, true, nil
}

func (c *luaScriptContext) streamState(key []byte) (*luaStreamState, error) {
	k := string(key)
	if st, ok := c.streams[k]; ok {
		return st, nil
	}
	st := &luaStreamState{}
	c.streams[k] = st

	typ, err := c.keyType(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.loaded = true
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		st.loaded = true
		return st, nil
	}
	if typ != redisTypeStream {
		return nil, wrongTypeError()
	}

	value, err := c.server.loadStreamAt(context.Background(), key, c.startTS)
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.exists = true
	st.value = cloneStreamValue(value)
	return st, nil
}

func (c *luaScriptContext) markStringValue(key []byte, value []byte) {
	st := c.ttlState(key)
	_ = st
	s := c.strings[string(key)]
	if s == nil {
		s = &luaStringState{}
		c.strings[string(key)] = s
	}
	s.loaded = true
	s.exists = true
	s.dirty = true
	s.value = append([]byte(nil), value...)
	c.markTouched(key)
	c.deleted[string(key)] = false
}

func (c *luaScriptContext) markListValue(key []byte, values []string) error {
	st, err := c.listState(key)
	if err != nil {
		return err
	}
	st.loaded = true
	st.exists = true
	st.dirty = true
	st.materialized = true
	st.meta = store.ListMeta{}
	st.leftTrim = 0
	st.rightTrim = 0
	st.leftValues = nil
	st.rightValues = nil
	st.values = append([]string(nil), values...)
	c.markTouched(key)
	c.deleted[string(key)] = false
	return nil
}

func (c *luaScriptContext) markHashValue(key []byte, value redisHashValue) error {
	st, err := c.hashState(key)
	if err != nil {
		return err
	}
	st.loaded = true
	st.exists = true
	st.dirty = true
	st.value = maps.Clone(value)
	c.markTouched(key)
	c.deleted[string(key)] = false
	return nil
}

func (c *luaScriptContext) markSetValue(key []byte, members map[string]struct{}) error {
	st, err := c.setState(key)
	if err != nil {
		return err
	}
	st.loaded = true
	st.exists = true
	st.dirty = true
	st.members = cloneSetMembers(members)
	c.markTouched(key)
	c.deleted[string(key)] = false
	return nil
}

func (c *luaScriptContext) markZSetValue(key []byte, members map[string]float64) error {
	st, err := c.zsetState(key)
	if err != nil {
		return err
	}
	st.loaded = true
	st.exists = true
	st.dirty = true
	st.membersLoaded = true
	st.members = maps.Clone(members)
	st.added = map[string]float64{}
	st.removed = map[string]struct{}{}
	st.storageScores = map[string]*float64{}
	st.lenDelta = 0
	c.markTouched(key)
	c.deleted[string(key)] = false
	return nil
}

func (c *luaScriptContext) markStreamValue(key []byte, value redisStreamValue) error {
	st, err := c.streamState(key)
	if err != nil {
		return err
	}
	st.loaded = true
	st.exists = true
	st.dirty = true
	st.value = cloneStreamValue(value)
	c.markTouched(key)
	c.deleted[string(key)] = false
	return nil
}

func (c *luaScriptContext) cmdDel(args []string) (luaReply, error) {
	deleted := 0
	for _, arg := range args {
		exists, err := c.logicalExists([]byte(arg))
		if err != nil {
			return luaReply{}, err
		}
		if exists {
			deleted++
			c.deleteLogical([]byte(arg))
		}
	}
	return luaIntReply(int64(deleted)), nil
}

func (c *luaScriptContext) cmdExists(args []string) (luaReply, error) {
	count := 0
	for _, arg := range args {
		exists, err := c.logicalExists([]byte(arg))
		if err != nil {
			return luaReply{}, err
		}
		if exists {
			count++
		}
	}
	return luaIntReply(int64(count)), nil
}

func (c *luaScriptContext) cmdGet(args []string) (luaReply, error) {
	st, err := c.stringState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaNilReply(), nil
	}
	return luaStringReply(string(st.value)), nil
}

type luaSetOptions struct {
	expiresAt *time.Time
	nx        bool
	xx        bool
	keepTTL   bool
}

func (c *luaScriptContext) cmdSet(args []string) (luaReply, error) {
	key := []byte(args[0])
	value := []byte(args[1])

	exists, prevTTL, err := c.loadLuaSetContext(key)
	if err != nil {
		return luaReply{}, err
	}

	options, err := parseLuaSetOptions(args[2:])
	if err != nil {
		return luaReply{}, err
	}

	if err := validateLuaSetOptions(options); err != nil {
		return luaReply{}, err
	}
	if shouldSkipLuaSet(options, exists) {
		return luaNilReply(), nil
	}

	c.deleteLogical(key)
	c.markStringValue(key, value)
	c.applyLuaSetTTL(key, prevTTL, options)
	return luaStatusReply("OK"), nil
}

// SETEX key seconds value
func (c *luaScriptContext) cmdSetEx(args []string) (luaReply, error) {
	key := []byte(args[0])
	seconds, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil || seconds <= 0 {
		return luaReply{}, errors.New("ERR invalid expire time in 'setex' command")
	}
	value := []byte(args[2])
	ttl := time.Now().Add(time.Duration(seconds) * time.Second)

	c.deleteLogical(key)
	c.markStringValue(key, value)
	c.setTTLValue(key, &ttl)
	return luaStatusReply("OK"), nil
}

// GETDEL key
func (c *luaScriptContext) cmdGetDel(args []string) (luaReply, error) {
	key := []byte(args[0])
	st, err := c.stringState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaNilReply(), nil
	}
	val := string(st.value)
	c.deleteLogical(key)
	return luaStringReply(val), nil
}

// SETNX key value — returns 1 if set, 0 if already exists
func (c *luaScriptContext) cmdSetNX(args []string) (luaReply, error) {
	key := []byte(args[0])
	value := []byte(args[1])

	exists, _, err := c.loadLuaSetContext(key)
	if err != nil {
		return luaReply{}, err
	}
	if exists {
		return luaIntReply(0), nil
	}
	c.deleteLogical(key)
	c.markStringValue(key, value)
	c.clearTTL(key)
	return luaIntReply(1), nil
}

func (c *luaScriptContext) loadLuaSetContext(key []byte) (bool, *time.Time, error) {
	exists, err := c.logicalExists(key)
	if err != nil {
		return false, nil, err
	}

	prevTTL, err := c.loadTTL(key)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return false, nil, err
	}
	return exists, prevTTL, nil
}

func validateLuaSetOptions(options luaSetOptions) error {
	if options.nx && options.xx {
		return errors.New("ERR syntax error")
	}
	return nil
}

func shouldSkipLuaSet(options luaSetOptions, exists bool) bool {
	return options.nx && exists || options.xx && !exists
}

func (c *luaScriptContext) applyLuaSetTTL(key []byte, prevTTL *time.Time, options luaSetOptions) {
	switch {
	case options.expiresAt != nil:
		c.setTTLValue(key, options.expiresAt)
	case options.keepTTL:
		c.setTTLValue(key, prevTTL)
	default:
		c.clearTTL(key)
	}
}

func parseLuaSetOptions(args []string) (luaSetOptions, error) {
	options := luaSetOptions{}
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "NX":
			options.nx = true
		case "XX":
			options.xx = true
		case "KEEPTTL":
			options.keepTTL = true
		case "PX", "EX":
			expiresAt, err := parseLuaSetExpiry(args[i], args, i)
			if err != nil {
				return luaSetOptions{}, err
			}
			options.expiresAt = expiresAt
			i++
		default:
			return luaSetOptions{}, errors.New("ERR syntax error")
		}
	}
	return options, nil
}

func parseLuaSetExpiry(unit string, args []string, index int) (*time.Time, error) {
	if index+1 >= len(args) {
		return nil, errors.New("ERR syntax error")
	}

	durationValue, err := strconv.ParseInt(args[index+1], 10, 64)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	durationUnit := time.Millisecond
	if strings.EqualFold(unit, "EX") {
		durationUnit = time.Second
	}

	expiresAt := time.Now().Add(time.Duration(durationValue) * durationUnit)
	return &expiresAt, nil
}

func (c *luaScriptContext) cmdIncr(args []string) (luaReply, error) {
	key := []byte(args[0])
	st, err := c.stringState(key)
	if err != nil {
		return luaReply{}, err
	}
	value := int64(0)
	if st.exists {
		value, err = strconv.ParseInt(string(st.value), 10, 64)
		if err != nil {
			return luaReply{}, errors.New("ERR value is not an integer or out of range")
		}
	}
	value++
	c.markStringValue(key, []byte(strconv.FormatInt(value, 10)))
	return luaIntReply(value), nil
}

func (c *luaScriptContext) cmdPTTL(args []string) (luaReply, error) {
	key := []byte(args[0])
	exists, err := c.logicalExists(key)
	if err != nil {
		return luaReply{}, err
	}
	if !exists {
		return luaIntReply(-2), nil
	}
	ttl, err := c.loadTTL(key)
	if err != nil {
		return luaReply{}, err
	}
	return luaIntReply(ttlMilliseconds(ttl)), nil
}

func (c *luaScriptContext) cmdPExpire(args []string) (luaReply, error) {
	key := []byte(args[0])
	exists, err := c.logicalExists(key)
	if err != nil {
		return luaReply{}, err
	}
	if !exists {
		return luaIntReply(0), nil
	}
	ms, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	if ms <= 0 {
		c.deleteLogical(key)
		return luaIntReply(1), nil
	}
	expireAt := time.Now().Add(time.Duration(ms) * time.Millisecond)
	c.markTouched(key)
	c.setTTLValue(key, &expireAt)
	return luaIntReply(1), nil
}

func (c *luaScriptContext) cmdType(args []string) (luaReply, error) {
	typ, err := c.keyType([]byte(args[0]))
	if errors.Is(err, store.ErrKeyNotFound) {
		return luaStatusReply(string(redisTypeNone)), nil
	}
	if err != nil {
		return luaReply{}, err
	}
	return luaStatusReply(string(typ)), nil
}

func (c *luaScriptContext) cmdRename(args []string) (luaReply, error) {
	src := []byte(args[0])
	dst := []byte(args[1])
	typ, err := c.keyType(src)
	if errors.Is(err, store.ErrKeyNotFound) || typ == redisTypeNone {
		return luaReply{}, errors.New("ERR no such key")
	}
	if err != nil {
		return luaReply{}, err
	}
	ttl, err := c.loadTTL(src)
	if err != nil {
		return luaReply{}, err
	}

	handler, ok := luaRenameHandlers[typ]
	if !ok {
		return luaReply{}, errors.New("ERR unsupported type for RENAME")
	}
	if err := handler(c, src, dst); err != nil {
		return luaReply{}, err
	}

	c.deleteLogical(src)
	if ttl != nil {
		expire := *ttl
		c.setTTLValue(dst, &expire)
	} else {
		c.clearTTL(dst)
	}
	return luaStatusReply("OK"), nil
}

func (c *luaScriptContext) renameStringValue(src, dst []byte) error {
	st, err := c.stringState(src)
	if err != nil {
		return err
	}
	c.deleteLogical(dst)
	c.markStringValue(dst, st.value)
	return nil
}

func (c *luaScriptContext) renameListValue(src, dst []byte) error {
	st, err := c.listState(src)
	if err != nil {
		return err
	}
	if err := c.materializeList(src, st); err != nil {
		return err
	}
	c.deleteLogical(dst)
	return c.markListValue(dst, st.values)
}

func (c *luaScriptContext) renameHashValue(src, dst []byte) error {
	st, err := c.hashState(src)
	if err != nil {
		return err
	}
	c.deleteLogical(dst)
	return c.markHashValue(dst, st.value)
}

func (c *luaScriptContext) renameSetValue(src, dst []byte) error {
	st, err := c.setState(src)
	if err != nil {
		return err
	}
	c.deleteLogical(dst)
	return c.markSetValue(dst, st.members)
}

func (c *luaScriptContext) renameZSetValue(src, dst []byte) error {
	st, err := c.zsetState(src)
	if err != nil {
		return err
	}
	if err := c.ensureZSetLoaded(st, src); err != nil {
		return err
	}
	c.deleteLogical(dst)
	return c.markZSetValue(dst, st.members)
}

func (c *luaScriptContext) renameStreamValue(src, dst []byte) error {
	st, err := c.streamState(src)
	if err != nil {
		return err
	}
	c.deleteLogical(dst)
	return c.markStreamValue(dst, st.value)
}

func (c *luaScriptContext) cmdHGet(args []string) (luaReply, error) {
	key := []byte(args[0])
	field := args[1]
	// Fast path is only safe when there is NO authoritative
	// script-local answer. Two separate signals to check:
	//
	//   (a) cachedType() covers loaded state for OTHER types (string /
	//       list / ...) AND the explicit c.deleted entry from a prior
	//       DEL / RENAME. Bypassing would leak pre-script pebble state.
	//   (b) c.hashes[key] already loaded (even with exists=false for
	//       a confirmed miss) means a prior command already paid the
	//       type-probe tax. Re-running the fast path would just do
	//       another wide-column probe for the same negative answer.
	//       Reuse the loaded state via the slow path, which returns
	//       immediately when c.hashes[key] is present.
	if luaHashAlreadyLoaded(c, key) {
		return hgetFromSlowPath(c, key, field)
	}
	if _, cached := c.cachedType(key); cached {
		return hgetFromSlowPath(c, key, field)
	}
	// Fast path: direct wide-column field lookup, bypassing the ~8-seek
	// keyTypeAt probe inside hashState. This is the same pattern that
	// the top-level HGET handler uses (see adapter/redis_compat_commands.go),
	// reused verbatim here so BullMQ-style scripts that touch many
	// hash keys stop paying the type-probe tax per redis.call.
	raw, hit, alive, err := c.server.hashFieldFastLookup(context.Background(), key, []byte(field), c.startTS)
	if err != nil {
		return luaReply{}, err
	}
	if hit {
		if !alive {
			return luaNilReply(), nil
		}
		return luaStringReply(string(raw)), nil
	}
	// Miss: fall back to the full hashState path so legacy-blob hashes
	// and nil / WRONGTYPE disambiguation behave exactly as before.
	return hgetFromSlowPath(c, key, field)
}

// luaHashAlreadyLoaded reports whether a prior command in this Eval
// already populated c.hashes[key] and resolved its exists/loaded
// flags. A loaded state -- even one representing a known miss --
// makes the fast path redundant: the slow path returns immediately
// by reading the cached luaHashState, skipping a wide-column probe.
func luaHashAlreadyLoaded(c *luaScriptContext, key []byte) bool {
	st, ok := c.hashes[string(key)]
	return ok && st != nil && st.loaded
}

// luaSetAlreadyLoaded mirrors luaHashAlreadyLoaded for c.sets.
func luaSetAlreadyLoaded(c *luaScriptContext, key []byte) bool {
	st, ok := c.sets[string(key)]
	return ok && st != nil && st.loaded
}

// luaZSetAlreadyLoaded mirrors luaHashAlreadyLoaded for c.zsets.
func luaZSetAlreadyLoaded(c *luaScriptContext, key []byte) bool {
	st, ok := c.zsets[string(key)]
	return ok && st != nil && st.loaded
}

// hgetFromSlowPath runs the legacy hashState-based HGET, preserving
// the script-local cache and WRONGTYPE / nil behaviour unchanged.
func hgetFromSlowPath(c *luaScriptContext, key []byte, field string) (luaReply, error) {
	st, err := c.hashState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	return hgetFromHashState(st, field), nil
}

// hgetFromHashState reads field out of a loaded luaHashState in the
// same way the legacy cmdHGet did.
func hgetFromHashState(st *luaHashState, field string) luaReply {
	if !st.exists {
		return luaNilReply()
	}
	value, ok := st.value[field]
	if !ok {
		return luaNilReply()
	}
	return luaStringReply(value)
}

func (c *luaScriptContext) cmdHSet(args []string) (luaReply, error) {
	if len(args)%2 != 1 {
		return luaReply{}, errors.New("ERR wrong number of arguments for 'HSET' command")
	}
	st, err := c.hashState([]byte(args[0]))
	if err != nil {
		return luaReply{}, err
	}
	if !st.exists {
		st.exists = true
		st.value = redisHashValue{}
	}
	added := 0
	for i := 1; i < len(args); i += 2 {
		if _, ok := st.value[args[i]]; !ok {
			added++
		}
		st.value[args[i]] = args[i+1]
	}
	st.loaded = true
	st.dirty = true
	c.markTouched([]byte(args[0]))
	c.deleted[args[0]] = false
	return luaIntReply(int64(added)), nil
}

func (c *luaScriptContext) cmdHMGet(args []string) (luaReply, error) {
	st, err := c.hashState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			out := make([]luaReply, 0, len(args)-1)
			for range args[1:] {
				out = append(out, luaNilReply())
			}
			return luaArrayReply(out...), nil
		}
		return luaReply{}, err
	}
	out := make([]luaReply, 0, len(args)-1)
	for _, field := range args[1:] {
		if !st.exists {
			out = append(out, luaNilReply())
			continue
		}
		value, ok := st.value[field]
		if !ok {
			out = append(out, luaNilReply())
			continue
		}
		out = append(out, luaStringReply(value))
	}
	return luaArrayReply(out...), nil
}

func (c *luaScriptContext) cmdHMSet(args []string) (luaReply, error) {
	if len(args)%2 != 1 {
		return luaReply{}, errors.New("ERR wrong number of arguments for 'HMSET' command")
	}
	if _, err := c.cmdHSet(args); err != nil {
		return luaReply{}, err
	}
	return luaStatusReply("OK"), nil
}

func (c *luaScriptContext) cmdHGetAll(args []string) (luaReply, error) {
	st, err := c.hashState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaArrayReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaArrayReply(), nil
	}
	fields := make([]string, 0, len(st.value))
	for field := range st.value {
		fields = append(fields, field)
	}
	sort.Strings(fields)
	out := make([]luaReply, 0, len(fields)*redisPairWidth)
	for _, field := range fields {
		out = append(out, luaStringReply(field), luaStringReply(st.value[field]))
	}
	return luaArrayReply(out...), nil
}

func (c *luaScriptContext) cmdHIncrBy(args []string) (luaReply, error) {
	st, err := c.hashState([]byte(args[0]))
	if err != nil {
		return luaReply{}, err
	}
	if !st.exists {
		st.exists = true
		st.value = redisHashValue{}
	}
	increment, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	current := int64(0)
	if raw, ok := st.value[args[1]]; ok {
		current, err = strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return luaReply{}, errors.New("ERR hash value is not an integer")
		}
	}
	current += increment
	st.value[args[1]] = strconv.FormatInt(current, 10)
	st.loaded = true
	st.dirty = true
	c.markTouched([]byte(args[0]))
	c.deleted[args[0]] = false
	return luaIntReply(current), nil
}

func (c *luaScriptContext) cmdHDel(args []string) (luaReply, error) {
	st, err := c.hashState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	removed := 0
	for _, field := range args[1:] {
		if _, ok := st.value[field]; ok {
			delete(st.value, field)
			removed++
		}
	}
	if removed == 0 {
		return luaIntReply(0), nil
	}
	st.loaded = true
	st.dirty = true
	if len(st.value) == 0 {
		st.exists = false
		c.clearTTL([]byte(args[0]))
		c.deleted[args[0]] = true
	}
	c.markTouched([]byte(args[0]))
	return luaIntReply(int64(removed)), nil
}

func (c *luaScriptContext) cmdHExists(args []string) (luaReply, error) {
	key := []byte(args[0])
	field := args[1]
	// See cmdHGet: defer to the slow path whenever the key already has
	// an authoritative answer locally, so prior DEL / RENAME / SET /
	// HSET within this Eval is honored, AND so a confirmed-missing
	// hash loaded earlier in the script does not re-run the
	// wide-column probe on every subsequent HEXISTS.
	if luaHashAlreadyLoaded(c, key) {
		return hexistsFromSlowPath(c, key, field)
	}
	if _, cached := c.cachedType(key); cached {
		return hexistsFromSlowPath(c, key, field)
	}
	hit, alive, err := c.server.hashFieldFastExists(context.Background(), key, []byte(field), c.startTS)
	if err != nil {
		return luaReply{}, err
	}
	if hit {
		if alive {
			return luaIntReply(1), nil
		}
		return luaIntReply(0), nil
	}
	return hexistsFromSlowPath(c, key, field)
}

func hexistsFromSlowPath(c *luaScriptContext, key []byte, field string) (luaReply, error) {
	st, err := c.hashState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	return hexistsFromHashState(st, field), nil
}

// hexistsFromHashState reports whether field is present in a loaded
// luaHashState, matching the legacy cmdHExists semantics.
func hexistsFromHashState(st *luaHashState, field string) luaReply {
	if !st.exists {
		return luaIntReply(0)
	}
	if _, ok := st.value[field]; ok {
		return luaIntReply(1)
	}
	return luaIntReply(0)
}

func (c *luaScriptContext) cmdHLen(args []string) (luaReply, error) {
	st, err := c.hashState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	return luaIntReply(int64(len(st.value))), nil
}

func (c *luaScriptContext) cmdLPush(args []string) (luaReply, error) {
	return c.pushList(args, true)
}

func (c *luaScriptContext) cmdRPush(args []string) (luaReply, error) {
	return c.pushList(args, false)
}

func (c *luaScriptContext) pushList(args []string, left bool) (luaReply, error) {
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		return luaReply{}, err
	}
	if !st.exists {
		st.exists = true
		st.materialized = false
		st.meta = store.ListMeta{}
		st.leftTrim = 0
		st.rightTrim = 0
		st.leftValues = nil
		st.rightValues = nil
		st.values = nil
	}
	switch {
	case st.materialized:
		for _, value := range args[1:] {
			if left {
				st.values = append([]string{value}, st.values...)
			} else {
				st.values = append(st.values, value)
			}
		}
	case left:
		prepended := make([]string, 0, len(args)-1+len(st.leftValues))
		for i := len(args) - 1; i >= 1; i-- {
			prepended = append(prepended, args[i])
		}
		st.leftValues = append(prepended, st.leftValues...)
	default:
		st.rightValues = append(st.rightValues, args[1:]...)
	}
	st.loaded = true
	st.dirty = true
	c.markTouched([]byte(args[0]))
	c.deleted[args[0]] = false
	return luaIntReply(st.currentLen()), nil
}

func (c *luaScriptContext) cmdLLen(args []string) (luaReply, error) {
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	return luaIntReply(st.currentLen()), nil
}

func (c *luaScriptContext) cmdLIndex(args []string) (luaReply, error) {
	st, ok, err := c.materializedListForRead(args[0])
	if err != nil {
		return luaReply{}, err
	}
	if !ok {
		return luaNilReply(), nil
	}
	index, err := strconv.Atoi(args[1])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	if index < 0 {
		index += len(st.values)
	}
	if index < 0 || index >= len(st.values) {
		return luaNilReply(), nil
	}
	return luaStringReply(st.values[index]), nil
}

func (c *luaScriptContext) cmdLRange(args []string) (luaReply, error) {
	st, ok, err := c.materializedListForRead(args[0])
	if err != nil {
		return luaReply{}, err
	}
	if !ok {
		return luaArrayReply(), nil
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	s, e := clampRange(start, stop, len(st.values))
	if e < s {
		return luaArrayReply(), nil
	}
	out := make([]luaReply, 0, e-s+1)
	for _, value := range st.values[s : e+1] {
		out = append(out, luaStringReply(value))
	}
	return luaArrayReply(out...), nil
}

func (c *luaScriptContext) cmdLRem(args []string) (luaReply, error) {
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists || st.currentLen() == 0 {
		return luaIntReply(0), nil
	}
	if err := c.materializeList([]byte(args[0]), st); err != nil {
		return luaReply{}, err
	}
	if len(st.values) == 0 {
		return luaIntReply(0), nil
	}
	count, err := strconv.Atoi(args[1])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	target := args[2]
	values, removed := removeListValues(st.values, target, count)
	if removed == 0 {
		return luaIntReply(0), nil
	}
	st.values = values
	st.dirty = true
	if len(values) == 0 {
		st.exists = false
		c.deleted[args[0]] = true
		c.clearTTL([]byte(args[0]))
	}
	c.markTouched([]byte(args[0]))
	return luaIntReply(int64(removed)), nil
}

func removeListValues(values []string, target string, count int) ([]string, int) {
	switch {
	case count == 0:
		return removeAllListValues(values, target)
	case count > 0:
		return removeLeadingListValues(values, target, count)
	default:
		return removeTrailingListValues(values, target, -count)
	}
}

func removeAllListValues(values []string, target string) ([]string, int) {
	filtered := append([]string(nil), values...)
	removed := 0
	out := filtered[:0]
	for _, value := range filtered {
		if value == target {
			removed++
			continue
		}
		out = append(out, value)
	}
	return out, removed
}

func removeLeadingListValues(values []string, target string, count int) ([]string, int) {
	filtered := make([]string, 0, len(values))
	removed := 0
	for _, value := range values {
		if value == target && removed < count {
			removed++
			continue
		}
		filtered = append(filtered, value)
	}
	return filtered, removed
}

func removeTrailingListValues(values []string, target string, count int) ([]string, int) {
	filtered := make([]string, 0, len(values))
	removed := 0
	for i := len(values) - 1; i >= 0; i-- {
		value := values[i]
		if value == target && removed < count {
			removed++
			continue
		}
		filtered = append(filtered, value)
	}
	reverseStrings(filtered)
	return filtered, removed
}

func (c *luaScriptContext) cmdLTrim(args []string) (luaReply, error) {
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaStatusReply("OK"), nil
		}
		return luaReply{}, err
	}
	if !st.exists || st.currentLen() == 0 {
		return luaStatusReply("OK"), nil
	}
	if err := c.materializeList([]byte(args[0]), st); err != nil {
		return luaReply{}, err
	}
	if len(st.values) == 0 {
		return luaStatusReply("OK"), nil
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	s, e := clampRange(start, stop, len(st.values))
	if e < s {
		st.values = nil
		st.exists = false
		st.dirty = true
		c.clearTTL([]byte(args[0]))
		c.deleted[args[0]] = true
		c.markTouched([]byte(args[0]))
		return luaStatusReply("OK"), nil
	}
	st.values = append([]string(nil), st.values[s:e+1]...)
	st.dirty = true
	c.markTouched([]byte(args[0]))
	return luaStatusReply("OK"), nil
}

func (c *luaScriptContext) cmdLPop(args []string) (luaReply, error) {
	return c.popList(args[0], true)
}

func (c *luaScriptContext) cmdRPop(args []string) (luaReply, error) {
	return c.popList(args[0], false)
}

func (c *luaScriptContext) popList(key string, left bool) (luaReply, error) {
	st, err := c.listState([]byte(key))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists || st.currentLen() == 0 {
		return luaNilReply(), nil
	}
	var value string
	if st.materialized {
		if left {
			value = st.values[0]
			st.values = st.values[1:]
		} else {
			value = st.values[len(st.values)-1]
			st.values = st.values[:len(st.values)-1]
		}
	} else {
		value, err = c.popLazyListValue([]byte(key), st, left)
		if err != nil {
			return luaReply{}, err
		}
	}
	st.dirty = true
	if st.currentLen() == 0 {
		st.exists = false
		st.materialized = false
		st.meta = store.ListMeta{}
		st.leftTrim = 0
		st.rightTrim = 0
		st.leftValues = nil
		st.rightValues = nil
		st.values = nil
		c.deleted[key] = true
		c.clearTTL([]byte(key))
	}
	c.markTouched([]byte(key))
	return luaStringReply(value), nil
}

func (c *luaScriptContext) popLazyListValue(key []byte, st *luaListState, left bool) (string, error) {
	if left {
		return c.popLazyListLeft(key, st)
	}
	return c.popLazyListRight(key, st)
}

func (c *luaScriptContext) popLazyListLeft(key []byte, st *luaListState) (string, error) {
	if len(st.leftValues) > 0 {
		value := st.leftValues[0]
		st.leftValues = st.leftValues[1:]
		return value, nil
	}
	if remaining := st.remainingOriginalLen(); remaining > 0 {
		values, err := c.server.fetchListRange(context.Background(), key, st.meta, st.leftTrim, st.leftTrim, c.startTS)
		if err != nil {
			return "", err
		}
		if len(values) == 0 {
			return "", errors.WithStack(store.ErrKeyNotFound)
		}
		st.leftTrim++
		return values[0], nil
	}
	value := st.rightValues[0]
	st.rightValues = st.rightValues[1:]
	return value, nil
}

func (c *luaScriptContext) popLazyListRight(key []byte, st *luaListState) (string, error) {
	if len(st.rightValues) > 0 {
		last := len(st.rightValues) - 1
		value := st.rightValues[last]
		st.rightValues = st.rightValues[:last]
		return value, nil
	}
	if remaining := st.remainingOriginalLen(); remaining > 0 {
		index := st.meta.Len - st.rightTrim - 1
		values, err := c.server.fetchListRange(context.Background(), key, st.meta, index, index, c.startTS)
		if err != nil {
			return "", err
		}
		if len(values) == 0 {
			return "", errors.WithStack(store.ErrKeyNotFound)
		}
		st.rightTrim++
		return values[0], nil
	}
	last := len(st.leftValues) - 1
	value := st.leftValues[last]
	st.leftValues = st.leftValues[:last]
	return value, nil
}

func (c *luaScriptContext) cmdRPopLPush(args []string) (luaReply, error) {
	value, err := c.popList(args[0], false)
	if err != nil || value.kind == luaReplyNil {
		return value, err
	}
	if _, err := c.pushList([]string{args[1], value.text}, true); err != nil {
		return luaReply{}, err
	}
	return luaStringReply(value.text), nil
}

func (c *luaScriptContext) cmdLPos(args []string) (luaReply, error) {
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaNilReply(), nil
	}
	if err := c.materializeList([]byte(args[0]), st); err != nil {
		return luaReply{}, err
	}
	for i, value := range st.values {
		if value == args[1] {
			return luaIntReply(int64(i)), nil
		}
	}
	return luaNilReply(), nil
}

func (c *luaScriptContext) cmdLSet(args []string) (luaReply, error) {
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		return luaReply{}, err
	}
	if err := c.materializeList([]byte(args[0]), st); err != nil {
		return luaReply{}, err
	}
	if !st.exists || len(st.values) == 0 {
		return luaReply{}, errors.New("ERR no such key")
	}
	index, err := strconv.Atoi(args[1])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	if index < 0 {
		index += len(st.values)
	}
	if index < 0 || index >= len(st.values) {
		return luaReply{}, errors.New("ERR index out of range")
	}
	st.values[index] = args[2]
	st.dirty = true
	c.markTouched([]byte(args[0]))
	return luaStatusReply("OK"), nil
}

func (c *luaScriptContext) cmdSAdd(args []string) (luaReply, error) {
	st, err := c.setState([]byte(args[0]))
	if err != nil {
		return luaReply{}, err
	}
	if !st.exists {
		st.exists = true
		st.members = map[string]struct{}{}
	}
	added := 0
	for _, member := range args[1:] {
		if _, ok := st.members[member]; ok {
			continue
		}
		st.members[member] = struct{}{}
		added++
	}
	if added > 0 {
		st.loaded = true
		st.dirty = true
		c.markTouched([]byte(args[0]))
		c.deleted[args[0]] = false
	}
	return luaIntReply(int64(added)), nil
}

func (c *luaScriptContext) cmdSCard(args []string) (luaReply, error) {
	st, err := c.setState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	return luaIntReply(int64(len(st.members))), nil
}

func (c *luaScriptContext) cmdSMembers(args []string) (luaReply, error) {
	st, err := c.setState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaArrayReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaArrayReply(), nil
	}
	members := sortedSetMembers(st.members)
	out := make([]luaReply, 0, len(members))
	for _, member := range members {
		out = append(out, luaStringReply(member))
	}
	return luaArrayReply(out...), nil
}

func (c *luaScriptContext) cmdSRem(args []string) (luaReply, error) {
	st, err := c.setState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	removed := removeMembers(st.members, args[1:])
	finalizeSetLikeRemoval(c, args[0], removed, len(st.members) == 0, func() {
		st.loaded = true
		st.dirty = true
		st.exists = len(st.members) > 0
	})
	return luaIntReply(int64(removed)), nil
}

func removeMembers[V any](members map[string]V, keys []string) int {
	removed := 0
	for _, key := range keys {
		if _, ok := members[key]; !ok {
			continue
		}
		delete(members, key)
		removed++
	}
	return removed
}

func finalizeSetLikeRemoval(c *luaScriptContext, key string, removed int, empty bool, markDirty func()) {
	if removed == 0 {
		return
	}

	markDirty()
	if empty {
		c.deleted[key] = true
		c.clearTTL([]byte(key))
	}
	c.markTouched([]byte(key))
}

func (c *luaScriptContext) cmdSIsMember(args []string) (luaReply, error) {
	key := []byte(args[0])
	member := args[1]
	// See cmdHGet: defer to the slow path whenever the set has an
	// authoritative script-local answer (loaded state incl. miss, or
	// cachedType reporting a different-type / deleted key).
	if luaSetAlreadyLoaded(c, key) {
		return sismemberFromSlowPath(c, key, member)
	}
	if _, cached := c.cachedType(key); cached {
		return sismemberFromSlowPath(c, key, member)
	}
	hit, alive, err := c.server.setMemberFastExists(context.Background(), key, []byte(member), c.startTS)
	if err != nil {
		return luaReply{}, err
	}
	if hit {
		if alive {
			return luaIntReply(1), nil
		}
		return luaIntReply(0), nil
	}
	return sismemberFromSlowPath(c, key, member)
}

func sismemberFromSlowPath(c *luaScriptContext, key []byte, member string) (luaReply, error) {
	st, err := c.setState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	return sismemberFromSetState(st, member), nil
}

// sismemberFromSetState returns the Redis integer reply for SISMEMBER
// against a loaded luaSetState, matching the legacy cmdSIsMember
// semantics.
func sismemberFromSetState(st *luaSetState, member string) luaReply {
	if !st.exists {
		return luaIntReply(0)
	}
	if _, ok := st.members[member]; ok {
		return luaIntReply(1)
	}
	return luaIntReply(0)
}

func parseLuaZAddArgs(args []string) (zaddFlags, []string, error) {
	var flags zaddFlags
	i := 1
	for i < len(args) {
		if !flags.applyFlag(strings.ToUpper(args[i])) {
			break
		}
		i++
	}
	if err := flags.validate(); err != nil {
		return zaddFlags{}, nil, err
	}
	remaining := args[i:]
	if len(remaining)%2 != 0 || len(remaining) == 0 {
		return zaddFlags{}, nil, errors.New("ERR syntax error")
	}
	return flags, remaining, nil
}

func (c *luaScriptContext) cmdZAdd(args []string) (luaReply, error) {
	flags, pairs, err := parseLuaZAddArgs(args)
	if err != nil {
		return luaReply{}, err
	}
	key := []byte(args[0])
	st, err := c.zsetState(key)
	if err != nil {
		return luaReply{}, err
	}
	added := 0
	changed := false
	for j := 0; j < len(pairs); j += 2 {
		score, err := strconv.ParseFloat(pairs[j], 64)
		if err != nil {
			return luaReply{}, errors.WithStack(err)
		}
		isNew, mutated, err := c.applyZAddMember(st, key, flags, pairs[j+1], score)
		if err != nil {
			return luaReply{}, err
		}
		if isNew {
			added++
		}
		if mutated {
			changed = true
		}
	}
	if changed {
		if !st.exists {
			st.exists = true
		}
		st.loaded = true
		st.dirty = true
		c.markTouched(key)
		c.deleted[args[0]] = false
	}
	return luaIntReply(int64(added)), nil
}

// applyZAddMember applies a single ZADD member/score pair, updating the delta or
// the full members map depending on load state. Returns (isNew, mutated, err):
// isNew is true when the member did not previously exist; mutated is true when
// any write was performed (false when flags like NX/XX prevented the operation).
func (c *luaScriptContext) applyZAddMember(st *luaZSetState, key []byte, flags zaddFlags, member string, score float64) (bool, bool, error) {
	oldScore, exists, err := c.memberScore(st, key, member)
	if err != nil {
		return false, false, err
	}
	if !flags.allows(exists, oldScore, score) {
		return false, false, nil
	}
	isNew := !exists
	if isNew && !st.membersLoaded {
		st.lenDelta++
	}
	if st.membersLoaded {
		if st.members == nil {
			st.members = map[string]float64{}
		}
		st.members[member] = score
	} else {
		st.added[member] = score
	}
	return isNew, true, nil
}

func (c *luaScriptContext) cmdZCard(args []string) (luaReply, error) {
	key := []byte(args[0])
	st, err := c.zsetState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	n, err := c.zsetCard(st, key)
	if err != nil {
		return luaReply{}, err
	}
	return luaIntReply(n), nil
}

func (c *luaScriptContext) cmdZCount(args []string) (luaReply, error) {
	key := []byte(args[0])
	st, err := c.zsetState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	if err := c.ensureZSetLoaded(st, key); err != nil {
		return luaReply{}, err
	}
	minBound, err := parseZScoreBound(args[1])
	if err != nil {
		return luaReply{}, err
	}
	maxBound, err := parseZScoreBound(args[2])
	if err != nil {
		return luaReply{}, err
	}
	count := 0
	for _, entry := range zsetMapToEntries(st.members) {
		if scoreInRange(entry.Score, minBound, maxBound) {
			count++
		}
	}
	return luaIntReply(int64(count)), nil
}

func (c *luaScriptContext) cmdZRange(args []string) (luaReply, error) {
	return c.rangeByRank(args, false)
}

func (c *luaScriptContext) cmdZRevRange(args []string) (luaReply, error) {
	return c.rangeByRank(args, true)
}

func (c *luaScriptContext) cmdZRangeByScoreAsc(args []string) (luaReply, error) {
	return c.cmdZRangeByScore(args, false)
}

func (c *luaScriptContext) cmdZRevRangeByScore(args []string) (luaReply, error) {
	return c.cmdZRangeByScore(args, true)
}

func (c *luaScriptContext) rangeByRank(args []string, reverse bool) (luaReply, error) {
	key := []byte(args[0])
	st, err := c.zsetState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaArrayReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaArrayReply(), nil
	}
	if err := c.ensureZSetLoaded(st, key); err != nil {
		return luaReply{}, err
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	withScores := len(args) > zsetWithScoresArgIndex && strings.EqualFold(args[zsetWithScoresArgIndex], "WITHSCORES")
	entries := zsetMapToEntries(st.members)
	if reverse {
		reverseEntries(entries)
	}
	s, e := normalizeRankRange(start, stop, len(entries))
	if e < s {
		return luaArrayReply(), nil
	}
	return zsetRangeReply(entries[s:e+1], withScores), nil
}

func (c *luaScriptContext) cmdZRangeByScore(args []string, reverse bool) (luaReply, error) {
	key := []byte(args[0])
	options, err := parseZRangeByScoreOptions(args, reverse)
	if err != nil {
		return luaReply{}, err
	}
	// Defensive bound on LIMIT offset: negative offsets produce no
	// well-defined Redis behaviour, so treat them as syntax error.
	if options.offset < 0 {
		return luaReply{}, errors.New("ERR value is out of range, must be positive")
	}
	// Redis treats ANY negative LIMIT count as "no limit" (return all
	// elements from offset). parseZRangeByScoreTail's default is -1;
	// an explicit user-supplied negative value is coerced here so the
	// downstream fast path (zsetFastScanLimit / decodeZSetScoreRange)
	// sees a single "unbounded" sentinel.
	if options.limit < 0 {
		options.limit = -1
	}
	// Fast path eligibility: no script-local mutation / deletion /
	// type-change on this key. Mirrors the cmdZScore / cmdHGet guards
	// so in-script ZADD / ZREM / DEL / SET behave exactly as before.
	if luaZSetAlreadyLoaded(c, key) {
		return c.cmdZRangeByScoreSlow(key, options, reverse)
	}
	if _, cached := c.cachedType(key); cached {
		return c.cmdZRangeByScoreSlow(key, options, reverse)
	}
	entries, hit, fastErr := c.zrangeByScoreFastPath(key, options, reverse)
	if fastErr != nil {
		return luaReply{}, fastErr
	}
	if !hit {
		return c.cmdZRangeByScoreSlow(key, options, reverse)
	}
	if len(entries) == 0 {
		return luaArrayReply(), nil
	}
	return zsetRangeReply(entries, options.withScores), nil
}

// zrangeByScoreFastPath translates the caller's min/max bounds into a
// bounded score-index scan through zsetRangeByScoreFast and returns
// the decoded entries. hit=false means the server-side helper wants
// the slow path (legacy-blob zset, string-encoding corruption, or
// empty-result-but-zset-is-missing so WRONGTYPE disambiguation is
// needed).
//
// Threads c.ctx into the server-side helper so the Eval's deadline
// and cancellation reach the store layer. New cmd* handlers should
// follow this pattern; historical Background() call sites in this
// file are migrated incrementally.
func (c *luaScriptContext) zrangeByScoreFastPath(
	key []byte, options luaZRangeByScoreOptions, reverse bool,
) ([]redisZSetEntry, bool, error) {
	startKey, endKey := zsetScoreScanBounds(key, options.minBound, options.maxBound)
	filter := func(score float64) bool {
		return scoreInRange(score, options.minBound, options.maxBound)
	}
	return c.server.zsetRangeByScoreFast(
		c.scriptCtx(), key, startKey, endKey, reverse,
		options.offset, options.limit, filter, c.startTS,
	)
}

// scriptCtx returns the request-scoped context captured at script
// construction, or context.Background() if the caller bypassed the
// normal entry point (unit tests constructing a luaScriptContext
// directly). New code should always have a real ctx, so this is a
// defensive fallback.
func (c *luaScriptContext) scriptCtx() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// cmdZRangeByScoreSlow is the legacy full-load path, preserved for
// fall-through cases (in-script mutations, legacy-blob encoding,
// ambiguous empty results).
func (c *luaScriptContext) cmdZRangeByScoreSlow(key []byte, options luaZRangeByScoreOptions, reverse bool) (luaReply, error) {
	st, err := c.zsetState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaArrayReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaArrayReply(), nil
	}
	if err := c.ensureZSetLoaded(st, key); err != nil {
		return luaReply{}, err
	}
	entries := zsetMapToEntries(st.members)
	if reverse {
		reverseEntries(entries)
	}
	selected := filterZRangeByScore(entries, options.minBound, options.maxBound)
	selected = applyZRangeLimit(selected, options.offset, options.limit)
	if len(selected) == 0 {
		return luaArrayReply(), nil
	}
	return zsetRangeReply(selected, options.withScores), nil
}

// zsetScoreScanBounds maps zScoreBound pairs to a [startKey, endKey)
// byte-range on the score index that EXCLUDES entries outside the
// caller's bound, not just approximates. Placing an exclusive edge
// into the scan bound (rather than post-filtering after the scan)
// matters for `ZRANGEBYSCORE (value +inf LIMIT 0 N`: if there are
// many members AT score=value, a post-filter approach would consume
// the whole offset+limit budget on those filtered-out rows and miss
// the real matches at score > value.
//
// The score index is lex-sorted by (userKey, sortableScore, member).
// Conventions:
//
//	minBound = -Inf              -> startKey = ZSetScoreScanPrefix(key)
//	minBound = value, inclusive  -> startKey = ZSetScoreRangeScanPrefix(key, score)
//	minBound = value, exclusive  -> startKey = PrefixScanEnd(ZSetScoreRangeScanPrefix(key, score))
//	maxBound = +Inf              -> endKey   = PrefixScanEnd(ZSetScoreScanPrefix(key))
//	maxBound = value, inclusive  -> endKey   = PrefixScanEnd(ZSetScoreRangeScanPrefix(key, score))
//	maxBound = value, exclusive  -> endKey   = ZSetScoreRangeScanPrefix(key, score)
//
// The same [start, end) endpoints drive both forward and reverse
// scans; ReverseScanAt iterates from endKey downward.
func zsetScoreScanBounds(key []byte, minBound, maxBound zScoreBound) (startKey, endKey []byte) {
	full := store.ZSetScoreScanPrefix(key)
	fullEnd := store.PrefixScanEnd(full)
	switch minBound.kind {
	case zBoundNegInf:
		startKey = full
	case zBoundValue:
		scorePrefix := store.ZSetScoreRangeScanPrefix(key, minBound.score)
		if minBound.inclusive {
			startKey = scorePrefix
		} else {
			startKey = store.PrefixScanEnd(scorePrefix)
		}
	case zBoundPosInf:
		// +Inf as minBound is a concrete "lower bound = +Inf" — only
		// members with score = +Inf qualify. Use the +Inf score prefix
		// as the scan start so those members are not silently dropped.
		// Redis accepts ZADD key +inf m and ZRANGEBYSCORE key +inf +inf
		// must return m.
		startKey = store.ZSetScoreRangeScanPrefix(key, math.Inf(+1))
	}
	switch maxBound.kind {
	case zBoundNegInf:
		// -Inf as maxBound is a concrete "upper bound = -Inf" — only
		// members with score = -Inf qualify. Bound the scan to the end
		// of the -Inf score slot. Mirrors the +Inf-as-minBound case.
		endKey = store.PrefixScanEnd(store.ZSetScoreRangeScanPrefix(key, math.Inf(-1)))
	case zBoundValue:
		scorePrefix := store.ZSetScoreRangeScanPrefix(key, maxBound.score)
		if maxBound.inclusive {
			endKey = store.PrefixScanEnd(scorePrefix)
		} else {
			endKey = scorePrefix
		}
	case zBoundPosInf:
		endKey = fullEnd
	}
	return startKey, endKey
}

type luaZRangeByScoreOptions struct {
	minBound   zScoreBound
	maxBound   zScoreBound
	withScores bool
	offset     int
	limit      int
}

func parseZRangeByScoreOptions(args []string, reverse bool) (luaZRangeByScoreOptions, error) {
	minRaw := args[1]
	maxRaw := args[2]
	if reverse {
		minRaw, maxRaw = maxRaw, minRaw
	}

	minBound, err := parseZScoreBound(minRaw)
	if err != nil {
		return luaZRangeByScoreOptions{}, err
	}
	maxBound, err := parseZScoreBound(maxRaw)
	if err != nil {
		return luaZRangeByScoreOptions{}, err
	}

	withScores, offset, limit, err := parseZRangeByScoreTail(args[zsetWithScoresArgIndex:])
	if err != nil {
		return luaZRangeByScoreOptions{}, err
	}

	return luaZRangeByScoreOptions{
		minBound:   minBound,
		maxBound:   maxBound,
		withScores: withScores,
		offset:     offset,
		limit:      limit,
	}, nil
}

func parseZRangeByScoreTail(args []string) (bool, int, int, error) {
	withScores := false
	offset := 0
	limit := -1
	for i := 0; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return false, 0, 0, errors.New("ERR syntax error")
			}

			nextOffset, err := strconv.Atoi(args[i+1])
			if err != nil {
				return false, 0, 0, errors.WithStack(err)
			}
			nextLimit, err := strconv.Atoi(args[i+2])
			if err != nil {
				return false, 0, 0, errors.WithStack(err)
			}
			offset = nextOffset
			limit = nextLimit
			i += 2
		default:
			return false, 0, 0, errors.New("ERR syntax error")
		}
	}
	return withScores, offset, limit, nil
}

func filterZRangeByScore(entries []redisZSetEntry, minBound, maxBound zScoreBound) []redisZSetEntry {
	selected := make([]redisZSetEntry, 0, len(entries))
	for _, entry := range entries {
		if scoreInRange(entry.Score, minBound, maxBound) {
			selected = append(selected, entry)
		}
	}
	return selected
}

func applyZRangeLimit(entries []redisZSetEntry, offset, limit int) []redisZSetEntry {
	if offset >= len(entries) {
		return nil
	}

	trimmed := entries[offset:]
	if limit >= 0 && limit < len(trimmed) {
		return trimmed[:limit]
	}
	return trimmed
}

// zscoreArgCount pins the expected argument count for ZSCORE at the
// Lua-context dispatch boundary. Using a named constant satisfies
// the linter without a //nolint:mnd on the arity check and documents
// that the command requires EXACTLY two arguments (any extras must
// be rejected, matching Redis server semantics).
const zscoreArgCount = 2

func (c *luaScriptContext) cmdZScore(args []string) (luaReply, error) {
	if len(args) != zscoreArgCount {
		return luaReply{}, errors.New("ERR wrong number of arguments for 'zscore' command")
	}
	key := []byte(args[0])
	member := args[1]
	// See cmdHGet: defer to the slow path whenever the zset has an
	// authoritative script-local answer. Two separate signals:
	//
	//   (a) luaZSetAlreadyLoaded: a prior cmdZScore / cmdZRange etc.
	//       fully resolved the zset, even to a confirmed miss
	//       (loaded=true, exists=false). Re-running the fast path
	//       would do a redundant wide-column probe.
	//   (b) cachedType: a prior ZADD / ZREM in this Eval OR a prior
	//       DEL / type-change via SET. Without this guard the fast
	//       path would leak pre-script pebble state.
	if luaZSetAlreadyLoaded(c, key) {
		return zscoreFromSlowPath(c, key, member)
	}
	if _, cached := c.cachedType(key); cached {
		return zscoreFromSlowPath(c, key, member)
	}
	score, hit, alive, err := c.server.zsetMemberFastScore(context.Background(), key, []byte(member), c.startTS)
	if err != nil {
		return luaReply{}, err
	}
	if hit {
		if !alive {
			return luaNilReply(), nil
		}
		return luaStringReply(formatRedisFloat(score)), nil
	}
	return zscoreFromSlowPath(c, key, member)
}

// zscoreFromSlowPath runs the legacy zsetState-based ZSCORE,
// preserving the script-local cache and WRONGTYPE / nil behaviour
// unchanged. Handles legacy-blob zsets via ensureZSetLoaded inside
// memberScore.
func zscoreFromSlowPath(c *luaScriptContext, key []byte, member string) (luaReply, error) {
	st, err := c.zsetState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	return c.zscoreFromZSetState(st, key, member)
}

// zscoreFromZSetState returns the Redis reply for ZSCORE against a
// loaded luaZSetState, matching the legacy cmdZScore semantics.
func (c *luaScriptContext) zscoreFromZSetState(st *luaZSetState, key []byte, member string) (luaReply, error) {
	if !st.exists {
		return luaNilReply(), nil
	}
	score, ok, err := c.memberScore(st, key, member)
	if err != nil {
		return luaReply{}, err
	}
	if !ok {
		return luaNilReply(), nil
	}
	return luaStringReply(formatRedisFloat(score)), nil
}

func (c *luaScriptContext) cmdZRem(args []string) (luaReply, error) {
	key := []byte(args[0])
	st, err := c.zsetState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	if err := c.ensureZSetLoaded(st, key); err != nil {
		return luaReply{}, err
	}
	removed := removeMembers(st.members, args[1:])
	finalizeSetLikeRemoval(c, args[0], removed, len(st.members) == 0, func() {
		st.loaded = true
		st.dirty = true
		st.exists = len(st.members) > 0
	})
	return luaIntReply(int64(removed)), nil
}

func (c *luaScriptContext) cmdZPopMin(args []string) (luaReply, error) {
	key := []byte(args[0])
	st, exists, err := c.zsetStateForRead(key)
	if err != nil {
		return luaArrayReply(), err
	}
	if !exists {
		return luaArrayReply(), nil
	}
	count := 1
	if len(args) > 1 {
		count, err = strconv.Atoi(args[1])
		if err != nil {
			return luaReply{}, errors.WithStack(err)
		}
	}
	entries := zsetMapToEntries(st.members)
	if count > len(entries) {
		count = len(entries)
	}
	if count == 0 {
		return luaArrayReply(), nil
	}
	out := make([]luaReply, 0, count*redisPairWidth)
	for _, entry := range entries[:count] {
		delete(st.members, entry.Member)
		out = append(out, luaStringReply(entry.Member), luaStringReply(formatRedisFloat(entry.Score)))
	}
	st.loaded = true
	st.dirty = true
	if len(st.members) == 0 {
		st.exists = false
		c.deleted[args[0]] = true
		c.clearTTL([]byte(args[0]))
	}
	c.markTouched([]byte(args[0]))
	return luaArrayReply(out...), nil
}

func (c *luaScriptContext) cmdZRemRangeByRank(args []string) (luaReply, error) {
	key := []byte(args[0])
	st, err := c.zsetState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	if err := c.ensureZSetLoaded(st, key); err != nil {
		return luaReply{}, err
	}
	start, err := strconv.Atoi(args[1])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	stop, err := strconv.Atoi(args[2])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	entries := zsetMapToEntries(st.members)
	s, e := normalizeRankRange(start, stop, len(entries))
	if e < s {
		return luaIntReply(0), nil
	}
	removed := 0
	for _, entry := range entries[s : e+1] {
		delete(st.members, entry.Member)
		removed++
	}
	st.loaded = true
	st.dirty = true
	if len(st.members) == 0 {
		st.exists = false
		c.deleted[args[0]] = true
		c.clearTTL([]byte(args[0]))
	}
	c.markTouched([]byte(args[0]))
	return luaIntReply(int64(removed)), nil
}

func (c *luaScriptContext) cmdZRemRangeByScore(args []string) (luaReply, error) {
	key := []byte(args[0])
	st, exists, err := c.zsetStateForRead(key)
	if err != nil {
		return luaIntReply(0), err
	}
	if !exists {
		return luaIntReply(0), nil
	}
	minBound, err := parseZScoreBound(args[1])
	if err != nil {
		return luaReply{}, err
	}
	maxBound, err := parseZScoreBound(args[2])
	if err != nil {
		return luaReply{}, err
	}
	removed := 0
	for _, entry := range zsetMapToEntries(st.members) {
		if scoreInRange(entry.Score, minBound, maxBound) {
			delete(st.members, entry.Member)
			removed++
		}
	}
	if removed > 0 {
		st.loaded = true
		st.dirty = true
		if len(st.members) == 0 {
			st.exists = false
			c.deleted[args[0]] = true
			c.clearTTL([]byte(args[0]))
		}
		c.markTouched([]byte(args[0]))
	}
	return luaIntReply(int64(removed)), nil
}

type luaXAddArgs struct {
	key    []byte
	maxLen int
	id     string
	fields []string
}

func (c *luaScriptContext) cmdXAdd(args []string) (luaReply, error) {
	if len(args) < xAddMinArgs {
		return luaReply{}, errors.New("ERR wrong number of arguments for 'XADD' command")
	}
	parsed, err := parseLuaXAddArgs(args)
	if err != nil {
		return luaReply{}, err
	}
	st, err := c.streamState(parsed.key)
	if err != nil {
		return luaReply{}, err
	}
	if !st.exists {
		st.exists = true
	}

	id := parsed.id
	if id == "*" {
		id = nextLuaStreamID(st.value)
	} else if !isNextStreamID(st.value, id) {
		return luaReply{}, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	st.value.Entries = append(st.value.Entries, newRedisStreamEntry(id, append([]string(nil), parsed.fields...)))
	st.loaded = true
	st.dirty = true
	if parsed.maxLen >= 0 && len(st.value.Entries) > parsed.maxLen {
		st.value.Entries = append([]redisStreamEntry(nil), st.value.Entries[len(st.value.Entries)-parsed.maxLen:]...)
	}
	c.markTouched(parsed.key)
	c.deleted[string(parsed.key)] = false
	return luaStringReply(id), nil
}

func parseLuaXAddArgs(args []string) (luaXAddArgs, error) {
	parsed := luaXAddArgs{
		key:    []byte(args[0]),
		maxLen: -1,
	}

	argIndex, maxLen, err := parseLuaXAddMaxLen(args)
	if err != nil {
		return luaXAddArgs{}, err
	}
	parsed.maxLen = maxLen
	if argIndex >= len(args) {
		return luaXAddArgs{}, errors.New("ERR syntax error")
	}

	parsed.id = args[argIndex]
	argIndex++
	if (len(args)-argIndex)%2 != 0 {
		return luaXAddArgs{}, errors.New("ERR wrong number of arguments for 'XADD' command")
	}
	parsed.fields = append([]string(nil), args[argIndex:]...)
	return parsed, nil
}

func parseLuaXAddMaxLen(args []string) (int, int, error) {
	argIndex := 1
	maxLen := -1
	if !strings.EqualFold(args[argIndex], "MAXLEN") {
		return argIndex, maxLen, nil
	}

	argIndex++
	if argIndex < len(args) && (args[argIndex] == "~" || args[argIndex] == "=") {
		argIndex++
	}
	if argIndex >= len(args) {
		return 0, 0, errors.New("ERR syntax error")
	}

	maxLen, err := strconv.Atoi(args[argIndex])
	if err != nil {
		return 0, 0, errors.WithStack(err)
	}
	return argIndex + 1, maxLen, nil
}

func isNextStreamID(value redisStreamValue, id string) bool {
	if len(value.Entries) == 0 {
		return true
	}
	return compareRedisStreamID(id, value.Entries[len(value.Entries)-1].ID) > 0
}

func (c *luaScriptContext) cmdXTrim(args []string) (luaReply, error) {
	if len(args) < xTrimMinArgs || !strings.EqualFold(args[1], "MAXLEN") {
		return luaReply{}, errors.New("ERR syntax error")
	}
	key, maxLen, err := parseLuaXTrimArgs(args)
	if err != nil {
		return luaReply{}, err
	}
	st, err := c.streamState(key)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
	}
	if maxLen >= len(st.value.Entries) {
		return luaIntReply(0), nil
	}
	removed := len(st.value.Entries) - maxLen
	if maxLen <= 0 {
		st.value.Entries = nil
		st.exists = false
		c.deleted[string(key)] = true
		c.clearTTL(key)
	} else {
		st.value.Entries = append([]redisStreamEntry(nil), st.value.Entries[removed:]...)
	}
	st.loaded = true
	st.dirty = true
	c.markTouched(key)
	return luaIntReply(int64(removed)), nil
}

func parseLuaXTrimArgs(args []string) ([]byte, int, error) {
	argIndex := 2
	if args[argIndex] == "~" || args[argIndex] == "=" {
		argIndex++
	}

	maxLen, err := strconv.Atoi(args[argIndex])
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	return []byte(args[0]), maxLen, nil
}

type luaCommitPlan struct {
	preserveExisting bool
	elems            []*kv.Elem[kv.OP]
}

// luaKeyPlan carries the data elements and TTL metadata for a single key commit.
type luaKeyPlan struct {
	elems            []*kv.Elem[kv.OP]
	finalType        redisValueType
	preserveExisting bool
}

func (c *luaScriptContext) commit() error {
	keys := make([]string, 0, len(c.touched))
	for key := range c.touched {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Pre-allocate a commitTS so Delta key bytes can embed it before dispatch.
	commitTS := c.server.coordinator.Clock().Next()

	elems := make([]*kv.Elem[kv.OP], 0, len(keys)*redisPairWidth)
	ctx := context.Background()
	for _, key := range keys {
		plan, err := c.commitPlanForKey(ctx, key, commitTS)
		if err != nil {
			return err
		}
		elems = append(elems, plan.elems...)
		// For non-string keys with dirty TTL: include !redis|ttl| in the same txn.
		// String keys already have TTL embedded in the value via stringCommitElems.
		if isNonStringCollectionType(plan.finalType) {
			ttlElems, err := c.nonStringTTLElems(key, plan.preserveExisting)
			if err != nil {
				return err
			}
			elems = append(elems, ttlElems...)
		}
	}

	if len(elems) == 0 {
		return nil
	}

	dispatchCtx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	startTS := c.startTS
	if startTS == ^uint64(0) {
		startTS = 0
	}
	if _, err := c.server.coordinator.Dispatch(dispatchCtx, &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems:    elems,
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// nonStringTTLElems returns !redis|ttl| elements for a non-string key if the TTL
// state is dirty (or the key was fully rewritten, requiring TTL to be included).
func (c *luaScriptContext) nonStringTTLElems(key string, preserveExisting bool) ([]*kv.Elem[kv.OP], error) {
	st := c.ttls[key]
	if preserveExisting && (st == nil || !st.dirty) {
		return nil, nil
	}
	ttl, err := c.finalTTL([]byte(key))
	if err != nil {
		return nil, err
	}
	if ttl == nil {
		return []*kv.Elem[kv.OP]{{Op: kv.Del, Key: redisTTLKey([]byte(key))}}, nil
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisTTLKey([]byte(key)), Value: encodeRedisTTL(*ttl)}}, nil
}

// commitPlanForKey builds the data Raft elements for a single key.
func (c *luaScriptContext) commitPlanForKey(ctx context.Context, key string, commitTS uint64) (luaKeyPlan, error) {
	finalType, err := c.finalType([]byte(key))
	if err != nil {
		return luaKeyPlan{}, err
	}

	valuePlan, err := c.valueCommitPlan(key, finalType, commitTS)
	if err != nil {
		return luaKeyPlan{}, err
	}

	var deleteElems []*kv.Elem[kv.OP]
	if !valuePlan.preserveExisting {
		deleteElems, _, err = c.server.deleteLogicalKeyElems(ctx, []byte(key), c.startTS)
		if err != nil {
			return luaKeyPlan{}, err
		}
	}

	dataElems := make([]*kv.Elem[kv.OP], 0, len(deleteElems)+len(valuePlan.elems))
	dataElems = append(dataElems, deleteElems...)
	dataElems = append(dataElems, valuePlan.elems...)
	return luaKeyPlan{
		elems:            dataElems,
		finalType:        finalType,
		preserveExisting: valuePlan.preserveExisting,
	}, nil
}

func (c *luaScriptContext) valueCommitPlan(key string, finalType redisValueType, commitTS uint64) (luaCommitPlan, error) {
	switch finalType {
	case redisTypeNone:
		return luaCommitPlan{}, nil
	case redisTypeString:
		elems, err := c.stringCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	case redisTypeList:
		return c.listCommitPlan(key, commitTS)
	case redisTypeHash:
		elems, err := c.hashCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	case redisTypeSet:
		elems, err := c.setCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	case redisTypeZSet:
		return c.zsetCommitPlan(key, commitTS)
	case redisTypeStream:
		elems, err := c.streamCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	default:
		return luaCommitPlan{}, errors.New("ERR unsupported final redis type")
	}
}

func (c *luaScriptContext) stringCommitElems(key string) ([]*kv.Elem[kv.OP], error) {
	st, err := c.stringState([]byte(key))
	if err != nil {
		return nil, err
	}
	ttl, err := c.finalTTL([]byte(key))
	if err != nil {
		return nil, err
	}
	encoded := encodeRedisStr(append([]byte(nil), st.value...), ttl)
	elems := []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisStrKey([]byte(key)), Value: encoded}}
	// Write !redis|ttl| scan index alongside the encoded value; delete it when
	// the script makes the string persistent, so the sweeper cannot later expire it.
	if ttl != nil {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey([]byte(key)), Value: encodeRedisTTL(*ttl)})
	} else {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisTTLKey([]byte(key))})
	}
	return elems, nil
}

func (c *luaScriptContext) listCommitPlan(key string, commitTS uint64) (luaCommitPlan, error) {
	st := c.lists[key]
	if st == nil || !st.dirty {
		return luaCommitPlan{preserveExisting: true}, nil
	}
	if st.materialized {
		elems, err := c.listCommitElems(key, commitTS)
		return luaCommitPlan{elems: elems}, err
	}
	// If the key was deleted earlier in this script and later recreated as a
	// list, we must perform a full rewrite (preserveExisting=false) so that
	// deleteLogicalKeyElems is called and any orphaned storage items from the
	// previous incarnation of the key are cleaned up before writing the delta.
	if c.everDeleted[key] {
		elems, err := c.listCommitElems(key, commitTS)
		return luaCommitPlan{elems: elems}, err
	}
	elems, err := c.listDeltaCommitElems(key, st, commitTS)
	return luaCommitPlan{preserveExisting: true, elems: elems}, err
}

func (c *luaScriptContext) listCommitElems(key string, commitTS uint64) ([]*kv.Elem[kv.OP], error) {
	st, err := c.listState([]byte(key))
	if err != nil {
		return nil, err
	}
	if err := c.materializeList([]byte(key), st); err != nil {
		return nil, err
	}
	values := make([][]byte, 0, len(st.values))
	for _, value := range st.values {
		values = append(values, []byte(value))
	}

	listElems, _, err := c.server.buildRPushOps(store.ListMeta{}, []byte(key), values, commitTS, 0)
	if err != nil {
		return nil, err
	}
	return listElems, nil
}

func (c *luaScriptContext) listDeltaCommitElems(key string, st *luaListState, commitTS uint64) ([]*kv.Elem[kv.OP], error) {
	if !st.exists || st.currentLen() == 0 {
		return nil, nil
	}

	newHead, rightStart, err := validateListDeltaRanges(st)
	if err != nil {
		return nil, err
	}

	elems := make([]*kv.Elem[kv.OP], 0, int(st.leftTrim+st.rightTrim)+len(st.leftValues)+len(st.rightValues)+setWideColOverhead)
	elems = appendListTrimDeletes(elems, []byte(key), st)
	elems = appendListPuts(elems, []byte(key), st.leftValues, newHead)
	elems = appendListPuts(elems, []byte(key), st.rightValues, rightStart)

	// Emit Delta keys for any appended values and trims instead of writing base meta.
	// Trims are counted separately as negative deltas.
	var seqInTxn uint32
	if len(st.rightValues) > 0 {
		rightDelta := store.MarshalListMetaDelta(store.ListMetaDelta{
			HeadDelta: 0,
			LenDelta:  int64(len(st.rightValues)) - st.rightTrim,
		})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ListMetaDeltaKey([]byte(key), commitTS, seqInTxn),
			Value: rightDelta,
		})
		seqInTxn++
	}
	if len(st.leftValues) > 0 || st.leftTrim > 0 {
		leftDelta := store.MarshalListMetaDelta(store.ListMetaDelta{
			HeadDelta: -int64(len(st.leftValues)) + st.leftTrim,
			LenDelta:  int64(len(st.leftValues)) - st.leftTrim,
		})
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.ListMetaDeltaKey([]byte(key), commitTS, seqInTxn),
			Value: leftDelta,
		})
	}
	return elems, nil
}

func validateListDeltaRanges(st *luaListState) (int64, int64, error) {
	remainingHead := st.meta.Head + st.leftTrim
	if remainingHead < math.MinInt64+int64(len(st.leftValues)) {
		return 0, 0, errors.WithStack(errors.New("LPUSH would underflow list Head sequence number"))
	}
	newHead := remainingHead - int64(len(st.leftValues))
	rightStart := st.meta.Tail - st.rightTrim
	if len(st.rightValues) > 0 && rightStart > math.MaxInt64-int64(len(st.rightValues)) {
		return 0, 0, errors.WithStack(errors.New("RPUSH would overflow list Tail sequence number"))
	}
	return newHead, rightStart, nil
}

func appendListTrimDeletes(elems []*kv.Elem[kv.OP], key []byte, st *luaListState) []*kv.Elem[kv.OP] {
	for seq := st.meta.Head; seq < st.meta.Head+st.leftTrim; seq++ {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listItemKey(key, seq)})
	}
	for seq := st.meta.Tail - st.rightTrim; seq < st.meta.Tail; seq++ {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listItemKey(key, seq)})
	}
	return elems
}

func appendListPuts(elems []*kv.Elem[kv.OP], key []byte, values []string, startSeq int64) []*kv.Elem[kv.OP] {
	for i, value := range values {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   listItemKey(key, startSeq+int64(i)),
			Value: []byte(value),
		})
	}
	return elems
}

func (c *luaScriptContext) hashCommitElems(key string) ([]*kv.Elem[kv.OP], error) {
	st, err := c.hashState([]byte(key))
	if err != nil {
		return nil, err
	}
	if len(st.value) == 0 {
		// Deletion is handled by deleteLogicalKeyElems called before this.
		return nil, nil
	}
	// Wide-column: write per-field keys and a base meta key with the final count.
	// deleteLogicalKeyElems (called by the Lua commit flow) clears any old keys.
	elems := make([]*kv.Elem[kv.OP], 0, len(st.value)+1)
	for field, val := range st.value {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.HashFieldKey([]byte(key), []byte(field)),
			Value: []byte(val),
		})
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.HashMetaKey([]byte(key)),
		Value: store.MarshalHashMeta(store.HashMeta{Len: int64(len(st.value))}),
	})
	return elems, nil
}

func (c *luaScriptContext) setCommitElems(key string) ([]*kv.Elem[kv.OP], error) {
	st, err := c.setState([]byte(key))
	if err != nil {
		return nil, err
	}
	if len(st.members) == 0 {
		// Deletion is handled by deleteLogicalKeyElems called before this.
		return nil, nil
	}
	// Wide-column: write per-member keys and a base meta key with the final count.
	elems := make([]*kv.Elem[kv.OP], 0, len(st.members)+1)
	for member := range st.members {
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   store.SetMemberKey([]byte(key), []byte(member)),
			Value: []byte{},
		})
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.SetMetaKey([]byte(key)),
		Value: store.MarshalSetMeta(store.SetMeta{Len: int64(len(st.members))}),
	})
	return elems, nil
}

// zsetCommitPlan decides between a full wide-column rewrite and an incremental
// delta commit. Full rewrite is used when the key was ever deleted, when members
// are fully loaded (triggered by any read command), or when the base data was in
// legacy blob format (one-time migration). Delta commit is used for write-only
// scripts (typically ZADD without prior reads) on an already-wide-column ZSet.
func (c *luaScriptContext) zsetCommitPlan(key string, commitTS uint64) (luaCommitPlan, error) {
	st := c.zsets[key]
	if st == nil || !st.dirty {
		return luaCommitPlan{preserveExisting: true}, nil
	}
	// physicallyExistsAtStart is true when the key had physical ZSet data in
	// storage at script start but was TTL-expired (logically absent). Force a
	// full commit so deleteLogicalKeyElems removes stale wide-column rows
	// (members, score-index, meta, TTL) that were left by the expired ZSet.
	if st.physicallyExistsAtStart || c.everDeleted[key] || st.membersLoaded {
		return c.zsetFullCommitWithMerge(key, st), nil
	}
	if st.legacyBlobBase {
		// One-time migration: load legacy blob, write wide-column, let deleteLogicalKeyElems clean up.
		if err := c.ensureZSetLoaded(st, []byte(key)); err != nil {
			return luaCommitPlan{}, err
		}
		return luaCommitPlan{elems: c.zsetFullCommitElems(key)}, nil
	}
	// Delta path: write only changed members + score index + metadata delta.
	return luaCommitPlan{preserveExisting: true, elems: c.zsetDeltaCommitElems(key, st, commitTS)}, nil
}

// zsetFullCommitWithMerge returns a full wide-column commit plan for key. When
// the key was deleted during the script but delta members were added afterwards
// (membersLoaded=false), st.added is merged into st.members so that
// zsetFullCommitElems does not silently drop the newly added entries.
func (c *luaScriptContext) zsetFullCommitWithMerge(key string, st *luaZSetState) luaCommitPlan {
	if !st.membersLoaded && len(st.added) > 0 {
		if st.members == nil {
			st.members = make(map[string]float64, len(st.added))
		}
		for m, s := range st.added {
			st.members[m] = s
		}
		st.added = map[string]float64{}
	}
	return luaCommitPlan{elems: c.zsetFullCommitElems(key)} // preserveExisting=false → deleteLogicalKeyElems called
}

// zsetFullCommitElems writes all members in wide-column format (member keys,
// score index keys, and a base meta key). deleteLogicalKeyElems is responsible
// for cleaning up any previous storage before these ops are applied.
func (c *luaScriptContext) zsetFullCommitElems(key string) []*kv.Elem[kv.OP] {
	st := c.zsets[key]
	if st == nil || len(st.members) == 0 {
		return nil
	}
	elems := make([]*kv.Elem[kv.OP], 0, len(st.members)*zsetElemsPerMember+zsetMetaBaseElems)
	for member, score := range st.members {
		elems = append(elems,
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetMemberKey([]byte(key), []byte(member)),
				Value: store.MarshalZSetScore(score),
			},
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetScoreKey([]byte(key), score, []byte(member)),
				Value: []byte{},
			},
		)
	}
	elems = append(elems, &kv.Elem[kv.OP]{
		Op:    kv.Put,
		Key:   store.ZSetMetaKey([]byte(key)),
		Value: store.MarshalZSetMeta(store.ZSetMeta{Len: int64(len(st.members))}),
	})
	return elems
}

// zsetDeltaCommitElems writes only the members that changed (added/updated/removed)
// together with their score index entries and a cardinality delta key.
// preserveExisting=true so deleteLogicalKeyElems is NOT called.
func (c *luaScriptContext) zsetDeltaCommitElems(key string, st *luaZSetState, commitTS uint64) []*kv.Elem[kv.OP] {
	if len(st.added) == 0 && len(st.removed) == 0 {
		return nil
	}
	// Each added member: optionally DEL old score key + PUT member key + PUT score key.
	// Each removed member: DEL member key + DEL score key.
	// Plus one optional ZSetMetaDeltaKey.
	elems := make([]*kv.Elem[kv.OP], 0, len(st.added)*zsetElemsPerAdded+len(st.removed)*zsetElemsPerRemoved+zsetMetaBaseElems)
	for member, score := range st.added {
		if storageScore, ok := st.storageScores[member]; ok && storageScore != nil {
			// Remove stale score index from storage.
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:  kv.Del,
				Key: store.ZSetScoreKey([]byte(key), *storageScore, []byte(member)),
			})
		}
		elems = append(elems,
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetMemberKey([]byte(key), []byte(member)),
				Value: store.MarshalZSetScore(score),
			},
			&kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   store.ZSetScoreKey([]byte(key), score, []byte(member)),
				Value: []byte{},
			},
		)
	}
	for member := range st.removed {
		storageScore, ok := st.storageScores[member]
		if !ok || storageScore == nil {
			continue
		}
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetMemberKey([]byte(key), []byte(member))},
			&kv.Elem[kv.OP]{Op: kv.Del, Key: store.ZSetScoreKey([]byte(key), *storageScore, []byte(member))},
		)
	}
	elems = append(elems, c.zsetDeltaMetaElems([]byte(key), st, commitTS)...)
	return elems
}

// zsetDeltaMetaElems returns the metadata element(s) for a delta commit.
// It tries to fold existing delta keys inline (compaction); on error or when
// below the threshold it falls back to writing a single new ZSetMetaDeltaKey.
func (c *luaScriptContext) zsetDeltaMetaElems(key []byte, st *luaZSetState, commitTS uint64) []*kv.Elem[kv.OP] {
	compactElems, compacted, err := c.server.zsetInlineMetaCompactionElems(
		context.Background(), key, c.startTS, st.lenDelta)
	if err == nil && compacted {
		return compactElems
	}
	if st.lenDelta == 0 {
		return nil
	}
	return []*kv.Elem[kv.OP]{{
		Op:    kv.Put,
		Key:   store.ZSetMetaDeltaKey(key, commitTS, 0),
		Value: store.MarshalZSetMetaDelta(store.ZSetMetaDelta{LenDelta: st.lenDelta}),
	}}
}

func (c *luaScriptContext) streamCommitElems(key string) ([]*kv.Elem[kv.OP], error) {
	st, err := c.streamState([]byte(key))
	if err != nil {
		return nil, err
	}
	payload, err := marshalStreamValue(st.value)
	if err != nil {
		return nil, err
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisStreamKey([]byte(key)), Value: payload}}, nil
}

func (c *luaScriptContext) finalType(key []byte) (redisValueType, error) {
	if typ, ok := c.cachedType(key); ok {
		return typ, nil
	}
	return c.server.keyTypeAt(context.Background(), key, c.startTS)
}

func (c *luaScriptContext) finalTTL(key []byte) (*time.Time, error) {
	st := c.ttls[string(key)]
	if st != nil && st.loaded {
		return st.value, nil
	}
	ttl, err := c.server.ttlAt(context.Background(), key, c.startTS)
	if err != nil {
		return nil, err
	}
	// Treat an already-expired TTL as absent so a recreated key does not
	// inherit the old expired timestamp (which would leave a stale TTL key).
	if ttl != nil && !ttl.After(time.Now()) {
		return nil, nil
	}
	return ttl, nil
}

func reverseStrings(values []string) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}

func reverseEntries(entries []redisZSetEntry) {
	for i, j := 0, len(entries)-1; i < j; i, j = i+1, j-1 {
		entries[i], entries[j] = entries[j], entries[i]
	}
}

func sortedSetMembers(members map[string]struct{}) []string {
	out := make([]string, 0, len(members))
	for member := range members {
		out = append(out, member)
	}
	sort.Strings(out)
	return out
}

func zsetRangeReply(entries []redisZSetEntry, withScores bool) luaReply {
	// WITHSCORES doubles the output width (member + score per entry);
	// size the slice accordingly to avoid an internal grow on large
	// reply arrays. Bounded by maxWideScanLimit at the fast-path
	// layer, so the allocation cannot be unbounded.
	capacity := len(entries)
	if withScores {
		capacity *= 2
	}
	out := make([]luaReply, 0, capacity)
	for _, entry := range entries {
		out = append(out, luaStringReply(entry.Member))
		if withScores {
			out = append(out, luaStringReply(formatRedisFloat(entry.Score)))
		}
	}
	return luaArrayReply(out...)
}

func parseZScoreBound(raw string) (zScoreBound, error) {
	switch strings.ToLower(raw) {
	case "-inf":
		return zScoreBound{kind: zBoundNegInf, inclusive: true}, nil
	case "+inf", "inf":
		return zScoreBound{kind: zBoundPosInf, inclusive: true}, nil
	}
	inclusive := true
	if strings.HasPrefix(raw, "(") {
		inclusive = false
		raw = raw[1:]
	}
	score, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return zScoreBound{}, errors.WithStack(err)
	}
	return zScoreBound{kind: zBoundValue, score: score, inclusive: inclusive}, nil
}

func scoreInRange(score float64, minBound zScoreBound, maxBound zScoreBound) bool {
	if minBound.kind == zBoundValue {
		if minBound.inclusive {
			if score < minBound.score {
				return false
			}
		} else if score <= minBound.score {
			return false
		}
	}
	if maxBound.kind == zBoundValue {
		if maxBound.inclusive {
			if score > maxBound.score {
				return false
			}
		} else if score >= maxBound.score {
			return false
		}
	}
	return true
}

func cloneSetMembers(in map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(in))
	for key := range in {
		out[key] = struct{}{}
	}
	return out
}

func cloneStreamValue(in redisStreamValue) redisStreamValue {
	out := redisStreamValue{Entries: make([]redisStreamEntry, 0, len(in.Entries))}
	for _, entry := range in.Entries {
		out.Entries = append(out.Entries, redisStreamEntry{
			ID:            entry.ID,
			Fields:        append([]string(nil), entry.Fields...),
			parsedID:      entry.parsedID,
			parsedIDValid: entry.parsedIDValid,
		})
	}
	return out
}

func nextLuaStreamID(stream redisStreamValue) string {
	nowID := strconv.FormatInt(time.Now().UnixMilli(), 10) + "-0"
	if len(stream.Entries) == 0 {
		return nowID
	}
	lastEntry := stream.Entries[len(stream.Entries)-1]
	lastID := lastEntry.ID
	if compareRedisStreamID(nowID, lastID) > 0 {
		return nowID
	}
	if !lastEntry.parsedIDValid {
		return nowID
	}
	last := lastEntry.parsedID
	return strconv.FormatUint(last.ms, 10) + "-" + strconv.FormatUint(last.seq+1, 10)
}
