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
	loaded      bool
	exists      bool
	dirty       bool
	fromLegacy  bool
	members     map[string]float64
	origMembers map[string]float64 // snapshot at load time for diff-based commit
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

func newLuaScriptContext(server *RedisServer) *luaScriptContext {
	startTS := server.readTS()
	return &luaScriptContext{
		server:      server,
		startTS:     startTS,
		readPin:     server.pinReadTS(startTS),
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
	}
}

func (c *luaScriptContext) Close() {
	if c == nil || c.readPin == nil {
		return
	}
	c.readPin.Release()
}

func (c *luaScriptContext) exec(command string, args []string) (luaReply, error) {
	if handler, ok := luaCommandHandlers[command]; ok {
		return handler(c, args)
	}

	return luaReply{}, errors.WithStack(errors.Newf("ERR unsupported command '%s'", command))
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
		st.members = map[string]float64{}
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

	value, err := c.server.readRedisStringAt(key, c.startTS)
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

	meta, exists, err := c.server.loadListMetaAt(context.Background(), key, c.startTS)
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
	st := &luaZSetState{}
	c.zsets[k] = st

	typ, err := c.keyType(key)
	if errors.Is(err, store.ErrKeyNotFound) {
		st.loaded = true
		st.members = map[string]float64{}
		st.origMembers = map[string]float64{}
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		st.loaded = true
		st.members = map[string]float64{}
		st.origMembers = map[string]float64{}
		return st, nil
	}
	if typ != redisTypeZSet {
		return nil, wrongTypeError()
	}

	load, err := c.server.loadZSetMembersMap(context.Background(), key, c.startTS)
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.exists = load.exists
	st.fromLegacy = load.fromLegacy
	st.members = load.members
	// Snapshot for diff-based commit.
	st.origMembers = make(map[string]float64, len(load.members))
	for k, v := range load.members {
		st.origMembers[k] = v
	}
	return st, nil
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
	st.members = maps.Clone(members)
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
	st, err := c.hashState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaNilReply(), nil
	}
	value, ok := st.value[args[1]]
	if !ok {
		return luaNilReply(), nil
	}
	return luaStringReply(value), nil
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
	_, ok := st.value[args[1]]
	if ok {
		return luaIntReply(1), nil
	}
	return luaIntReply(0), nil
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
	if _, ok := st.members[args[1]]; ok {
		return luaIntReply(1), nil
	}
	return luaIntReply(0), nil
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
	st, err := c.zsetState([]byte(args[0]))
	if err != nil {
		return luaReply{}, err
	}
	if !st.exists {
		st.exists = true
		st.members = map[string]float64{}
	}
	added := 0
	for j := 0; j < len(pairs); j += 2 {
		score, err := strconv.ParseFloat(pairs[j], 64)
		if err != nil {
			return luaReply{}, errors.WithStack(err)
		}
		member := pairs[j+1]
		oldScore, exists := st.members[member]
		if !flags.allows(exists, oldScore, score) {
			continue
		}
		if !exists {
			added++
		}
		st.members[member] = score
	}
	st.loaded = true
	st.dirty = true
	c.markTouched([]byte(args[0]))
	c.deleted[args[0]] = false
	return luaIntReply(int64(added)), nil
}

func (c *luaScriptContext) cmdZCard(args []string) (luaReply, error) {
	st, err := c.zsetState([]byte(args[0]))
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

func (c *luaScriptContext) cmdZCount(args []string) (luaReply, error) {
	st, err := c.zsetState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
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
	st, err := c.zsetState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaArrayReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
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
	st, err := c.zsetState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaArrayReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaArrayReply(), nil
	}

	options, err := parseZRangeByScoreOptions(args, reverse)
	if err != nil {
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

func (c *luaScriptContext) cmdZScore(args []string) (luaReply, error) {
	st, err := c.zsetState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaNilReply(), nil
	}
	score, ok := st.members[args[1]]
	if !ok {
		return luaNilReply(), nil
	}
	return luaStringReply(formatRedisFloat(score)), nil
}

func (c *luaScriptContext) cmdZRem(args []string) (luaReply, error) {
	st, err := c.zsetState([]byte(args[0]))
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

func (c *luaScriptContext) cmdZPopMin(args []string) (luaReply, error) {
	st, err := c.zsetState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaArrayReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
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
	st, err := c.zsetState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
		return luaIntReply(0), nil
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
	st, err := c.zsetState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaIntReply(0), nil
		}
		return luaReply{}, err
	}
	if !st.exists {
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

func (c *luaScriptContext) commit() error {
	keys := make([]string, 0, len(c.touched))
	for key := range c.touched {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	elems := make([]*kv.Elem[kv.OP], 0, len(keys)*redisPairWidth)
	ctx := context.Background()
	for _, key := range keys {
		keyElems, err := c.commitElemsForKey(ctx, key)
		if err != nil {
			return err
		}
		elems = append(elems, keyElems...)
	}

	if len(elems) == 0 {
		return nil
	}
	dispatchCtx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	return c.server.dispatchElems(dispatchCtx, true, c.startTS, elems)
}

func (c *luaScriptContext) commitElemsForKey(ctx context.Context, key string) ([]*kv.Elem[kv.OP], error) {
	finalType, err := c.finalType([]byte(key))
	if err != nil {
		return nil, err
	}

	valuePlan, err := c.valueCommitPlan(key, finalType)
	if err != nil {
		return nil, err
	}

	var deleteElems []*kv.Elem[kv.OP]
	if !valuePlan.preserveExisting {
		deleteElems, _, err = c.server.deleteLogicalKeyElems(ctx, []byte(key), c.startTS)
		if err != nil {
			return nil, err
		}
	}
	ttlElems, err := c.ttlCommitElems(key, finalType, valuePlan.preserveExisting)
	if err != nil {
		return nil, err
	}

	elems := make([]*kv.Elem[kv.OP], 0, len(deleteElems)+len(valuePlan.elems)+len(ttlElems))
	elems = append(elems, deleteElems...)
	elems = append(elems, valuePlan.elems...)
	elems = append(elems, ttlElems...)
	return elems, nil
}

func (c *luaScriptContext) valueCommitPlan(key string, finalType redisValueType) (luaCommitPlan, error) {
	switch finalType {
	case redisTypeNone:
		return luaCommitPlan{}, nil
	case redisTypeString:
		elems, err := c.stringCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	case redisTypeList:
		return c.listCommitPlan(key)
	case redisTypeHash:
		elems, err := c.hashCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	case redisTypeSet:
		elems, err := c.setCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	case redisTypeZSet:
		elems, err := c.zsetCommitElems(key)
		return luaCommitPlan{elems: elems}, err
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
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisStrKey([]byte(key)), Value: append([]byte(nil), st.value...)}}, nil
}

func (c *luaScriptContext) listCommitPlan(key string) (luaCommitPlan, error) {
	st := c.lists[key]
	if st == nil || !st.dirty {
		return luaCommitPlan{preserveExisting: true}, nil
	}
	if st.materialized {
		elems, err := c.listCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	}
	// If the key was deleted earlier in this script and later recreated as a
	// list, we must perform a full rewrite (preserveExisting=false) so that
	// deleteLogicalKeyElems is called and any orphaned storage items from the
	// previous incarnation of the key are cleaned up before writing the delta.
	if c.everDeleted[key] {
		elems, err := c.listCommitElems(key)
		return luaCommitPlan{elems: elems}, err
	}
	elems, err := c.listDeltaCommitElems(key, st)
	return luaCommitPlan{preserveExisting: true, elems: elems}, err
}

func (c *luaScriptContext) listCommitElems(key string) ([]*kv.Elem[kv.OP], error) {
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

	listElems, _, err := c.server.buildRPushOps(store.ListMeta{}, []byte(key), values)
	if err != nil {
		return nil, err
	}
	return listElems, nil
}

func (c *luaScriptContext) listDeltaCommitElems(key string, st *luaListState) ([]*kv.Elem[kv.OP], error) {
	if !st.exists || st.currentLen() == 0 {
		return nil, nil
	}

	newHead, rightStart, err := validateListDeltaRanges(st)
	if err != nil {
		return nil, err
	}
	newMeta := store.ListMeta{
		Head: newHead,
		Len:  st.currentLen(),
		Tail: newHead + st.currentLen(),
	}
	metaBytes, err := store.MarshalListMeta(newMeta)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	elems := make([]*kv.Elem[kv.OP], 0, int(st.leftTrim+st.rightTrim)+len(st.leftValues)+len(st.rightValues)+1)
	elems = appendListTrimDeletes(elems, []byte(key), st)
	elems = appendListPuts(elems, []byte(key), st.leftValues, newHead)
	elems = appendListPuts(elems, []byte(key), st.rightValues, rightStart)
	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listMetaKey([]byte(key)), Value: metaBytes})
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
	payload, err := marshalHashValue(st.value)
	if err != nil {
		return nil, err
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisHashKey([]byte(key)), Value: payload}}, nil
}

func (c *luaScriptContext) setCommitElems(key string) ([]*kv.Elem[kv.OP], error) {
	st, err := c.setState([]byte(key))
	if err != nil {
		return nil, err
	}
	payload, err := marshalSetValue(redisSetValue{Members: sortedSetMembers(st.members)})
	if err != nil {
		return nil, err
	}
	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisSetKey([]byte(key)), Value: payload}}, nil
}

func (c *luaScriptContext) zsetCommitElems(key string) ([]*kv.Elem[kv.OP], error) {
	st, err := c.zsetState([]byte(key))
	if err != nil {
		return nil, err
	}
	keyBytes := []byte(key)
	if st.fromLegacy {
		// Legacy blob → wide-column migration: full write + delete legacy blob.
		elems, err := buildZSetWriteElems(keyBytes, st.members)
		if err != nil {
			return nil, err
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: redisZSetKey(keyBytes)})
		return elems, nil
	}
	if len(st.origMembers) == 0 && len(st.members) > 0 {
		// Brand-new ZSet: full write.
		return buildZSetWriteElems(keyBytes, st.members)
	}
	return buildZSetDiffElems(keyBytes, st.origMembers, st.members)
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

func (c *luaScriptContext) ttlCommitElems(key string, finalType redisValueType, preserveExisting bool) ([]*kv.Elem[kv.OP], error) {
	if finalType == redisTypeNone {
		return nil, nil
	}
	if preserveExisting {
		if st := c.ttls[key]; st == nil || !st.dirty {
			return nil, nil
		}
	}

	ttl, err := c.finalTTL([]byte(key))
	if err != nil {
		return nil, err
	}
	if ttl == nil {
		if preserveExisting {
			return []*kv.Elem[kv.OP]{{Op: kv.Del, Key: redisTTLKey([]byte(key))}}, nil
		}
		return nil, nil
	}

	return []*kv.Elem[kv.OP]{{Op: kv.Put, Key: redisTTLKey([]byte(key)), Value: encodeRedisTTL(*ttl)}}, nil
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
	return c.server.ttlAt(context.Background(), key, c.startTS)
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
	out := make([]luaReply, 0, len(entries))
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
