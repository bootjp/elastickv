package adapter

import (
	"context"
	"maps"
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

	touched map[string]struct{}
	deleted map[string]bool

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
	loaded bool
	exists bool
	dirty  bool
	values []string
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
	loaded  bool
	exists  bool
	dirty   bool
	members map[string]float64
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

type zScoreBound struct {
	kind      int
	score     float64
	inclusive bool
}

const (
	zBoundNegInf = -1
	zBoundValue  = 0
	zBoundPosInf = 1
)

func newLuaScriptContext(server *RedisServer) *luaScriptContext {
	return &luaScriptContext{
		server:  server,
		startTS: server.readTS(),
		touched: map[string]struct{}{},
		deleted: map[string]bool{},
		strings: map[string]*luaStringState{},
		lists:   map[string]*luaListState{},
		hashes:  map[string]*luaHashState{},
		sets:    map[string]*luaSetState{},
		zsets:   map[string]*luaZSetState{},
		streams: map[string]*luaStreamState{},
		ttls:    map[string]*luaTTLState{},
	}
}

func (c *luaScriptContext) exec(command string, args []string) (luaReply, error) {
	switch command {
	case cmdDel:
		return c.cmdDel(args)
	case cmdExists:
		return c.cmdExists(args)
	case cmdGet:
		return c.cmdGet(args)
	case cmdSet:
		return c.cmdSet(args)
	case cmdIncr:
		return c.cmdIncr(args)
	case cmdPTTL:
		return c.cmdPTTL(args)
	case cmdPExpire:
		return c.cmdPExpire(args)
	case cmdType:
		return c.cmdType(args)
	case cmdRename:
		return c.cmdRename(args)
	case cmdHGet:
		return c.cmdHGet(args)
	case cmdHSet:
		return c.cmdHSet(args)
	case cmdHMGet:
		return c.cmdHMGet(args)
	case cmdHMSet:
		return c.cmdHMSet(args)
	case cmdHGetAll:
		return c.cmdHGetAll(args)
	case cmdHIncrBy:
		return c.cmdHIncrBy(args)
	case cmdHDel:
		return c.cmdHDel(args)
	case cmdHExists:
		return c.cmdHExists(args)
	case cmdHLen:
		return c.cmdHLen(args)
	case cmdLPush:
		return c.cmdLPush(args)
	case cmdRPush:
		return c.cmdRPush(args)
	case cmdLLen:
		return c.cmdLLen(args)
	case cmdLIndex:
		return c.cmdLIndex(args)
	case cmdLRange:
		return c.cmdLRange(args)
	case cmdLRem:
		return c.cmdLRem(args)
	case cmdLTrim:
		return c.cmdLTrim(args)
	case cmdLPop:
		return c.cmdLPop(args)
	case cmdRPop:
		return c.cmdRPop(args)
	case cmdRPopLPush:
		return c.cmdRPopLPush(args)
	case cmdLPos:
		return c.cmdLPos(args)
	case cmdLSet:
		return c.cmdLSet(args)
	case cmdSAdd:
		return c.cmdSAdd(args)
	case cmdSCard:
		return c.cmdSCard(args)
	case cmdSMembers:
		return c.cmdSMembers(args)
	case cmdSRem:
		return c.cmdSRem(args)
	case cmdSIsMember:
		return c.cmdSIsMember(args)
	case cmdZAdd:
		return c.cmdZAdd(args)
	case cmdZCard:
		return c.cmdZCard(args)
	case cmdZCount:
		return c.cmdZCount(args)
	case cmdZRange:
		return c.cmdZRange(args)
	case cmdZRangeByScore:
		return c.cmdZRangeByScore(args, false)
	case cmdZRevRange:
		return c.cmdZRevRange(args)
	case cmdZRevRangeByScore:
		return c.cmdZRangeByScore(args, true)
	case cmdZScore:
		return c.cmdZScore(args)
	case cmdZRem:
		return c.cmdZRem(args)
	case cmdZPopMin:
		return c.cmdZPopMin(args)
	case cmdZRemRangeByRank:
		return c.cmdZRemRangeByRank(args)
	case cmdZRemRangeByScore:
		return c.cmdZRemRangeByScore(args)
	case cmdXAdd:
		return c.cmdXAdd(args)
	case cmdXTrim:
		return c.cmdXTrim(args)
	default:
		return luaReply{}, errors.Newf("ERR unsupported command '%s'", command)
	}
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

func (c *luaScriptContext) keyType(key []byte) (redisValueType, error) {
	k := string(key)
	if st, ok := c.strings[k]; ok && st.loaded && st.exists {
		return redisTypeString, c.ensureKeyNotExpired(key)
	}
	if st, ok := c.lists[k]; ok && st.loaded && st.exists {
		return redisTypeList, c.ensureKeyNotExpired(key)
	}
	if st, ok := c.hashes[k]; ok && st.loaded && st.exists {
		return redisTypeHash, c.ensureKeyNotExpired(key)
	}
	if st, ok := c.sets[k]; ok && st.loaded && st.exists {
		return redisTypeSet, c.ensureKeyNotExpired(key)
	}
	if st, ok := c.zsets[k]; ok && st.loaded && st.exists {
		return redisTypeZSet, c.ensureKeyNotExpired(key)
	}
	if st, ok := c.streams[k]; ok && st.loaded && st.exists {
		return redisTypeStream, c.ensureKeyNotExpired(key)
	}
	if c.deleted[k] {
		return redisTypeNone, nil
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

	value, err := c.server.readValueAt(key, c.startTS)
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

	values, err := c.server.listValuesAt(context.Background(), key, c.startTS)
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.exists = true
	st.values = append([]string(nil), values...)
	return st, nil
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

	value, _, err := c.server.loadHashAt(context.Background(), key, c.startTS)
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

	value, _, err := c.server.loadSetAt(context.Background(), "set", key, c.startTS)
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
		return st, nil
	}
	if err != nil {
		return nil, err
	}
	if typ == redisTypeNone {
		st.loaded = true
		st.members = map[string]float64{}
		return st, nil
	}
	if typ != redisTypeZSet {
		return nil, wrongTypeError()
	}

	value, _, err := c.server.loadZSetAt(context.Background(), key, c.startTS)
	if err != nil {
		return nil, err
	}
	st.loaded = true
	st.exists = true
	st.members = zsetEntriesToMap(value.Entries)
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

	value, _, err := c.server.loadStreamAt(context.Background(), key, c.startTS)
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

func (c *luaScriptContext) cmdSet(args []string) (luaReply, error) {
	key := []byte(args[0])
	value := []byte(args[1])

	exists, err := c.logicalExists(key)
	if err != nil {
		return luaReply{}, err
	}
	prevTTL, err := c.loadTTL(key)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return luaReply{}, err
	}

	var (
		expiresAt *time.Time
		nx        bool
		xx        bool
		keepTTL   bool
	)
	for i := 2; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "KEEPTTL":
			keepTTL = true
		case "PX", "EX":
			if i+1 >= len(args) {
				return luaReply{}, errors.New("ERR syntax error")
			}
			n, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				return luaReply{}, errors.WithStack(err)
			}
			unit := time.Millisecond
			if strings.EqualFold(args[i], "EX") {
				unit = time.Second
			}
			exp := time.Now().Add(time.Duration(n) * unit)
			expiresAt = &exp
			i++
		default:
			return luaReply{}, errors.New("ERR syntax error")
		}
	}
	if nx && xx {
		return luaReply{}, errors.New("ERR syntax error")
	}
	if nx && exists {
		return luaNilReply(), nil
	}
	if xx && !exists {
		return luaNilReply(), nil
	}

	c.deleteLogical(key)
	c.markStringValue(key, value)
	switch {
	case expiresAt != nil:
		c.setTTLValue(key, expiresAt)
	case keepTTL:
		c.setTTLValue(key, prevTTL)
	default:
		c.clearTTL(key)
	}
	return luaStatusReply("OK"), nil
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

	switch typ {
	case redisTypeString:
		st, err := c.stringState(src)
		if err != nil {
			return luaReply{}, err
		}
		c.deleteLogical(dst)
		c.markStringValue(dst, st.value)
	case redisTypeList:
		st, err := c.listState(src)
		if err != nil {
			return luaReply{}, err
		}
		c.deleteLogical(dst)
		dstState, err := c.listState(dst)
		if err != nil {
			return luaReply{}, err
		}
		dstState.loaded = true
		dstState.exists = true
		dstState.dirty = true
		dstState.values = append([]string(nil), st.values...)
		c.markTouched(dst)
		c.deleted[string(dst)] = false
	case redisTypeHash:
		st, err := c.hashState(src)
		if err != nil {
			return luaReply{}, err
		}
		c.deleteLogical(dst)
		dstState, err := c.hashState(dst)
		if err != nil {
			return luaReply{}, err
		}
		dstState.loaded = true
		dstState.exists = true
		dstState.dirty = true
		dstState.value = maps.Clone(st.value)
		c.markTouched(dst)
		c.deleted[string(dst)] = false
	case redisTypeSet:
		st, err := c.setState(src)
		if err != nil {
			return luaReply{}, err
		}
		c.deleteLogical(dst)
		dstState, err := c.setState(dst)
		if err != nil {
			return luaReply{}, err
		}
		dstState.loaded = true
		dstState.exists = true
		dstState.dirty = true
		dstState.members = cloneSetMembers(st.members)
		c.markTouched(dst)
		c.deleted[string(dst)] = false
	case redisTypeZSet:
		st, err := c.zsetState(src)
		if err != nil {
			return luaReply{}, err
		}
		c.deleteLogical(dst)
		dstState, err := c.zsetState(dst)
		if err != nil {
			return luaReply{}, err
		}
		dstState.loaded = true
		dstState.exists = true
		dstState.dirty = true
		dstState.members = maps.Clone(st.members)
		c.markTouched(dst)
		c.deleted[string(dst)] = false
	case redisTypeStream:
		st, err := c.streamState(src)
		if err != nil {
			return luaReply{}, err
		}
		c.deleteLogical(dst)
		dstState, err := c.streamState(dst)
		if err != nil {
			return luaReply{}, err
		}
		dstState.loaded = true
		dstState.exists = true
		dstState.dirty = true
		dstState.value = cloneStreamValue(st.value)
		c.markTouched(dst)
		c.deleted[string(dst)] = false
	default:
		return luaReply{}, errors.New("ERR unsupported type for RENAME")
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
	out := make([]luaReply, 0, len(fields)*2)
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
		st.values = []string{}
	}
	for _, value := range args[1:] {
		if left {
			st.values = append([]string{value}, st.values...)
		} else {
			st.values = append(st.values, value)
		}
	}
	st.loaded = true
	st.dirty = true
	c.markTouched([]byte(args[0]))
	c.deleted[args[0]] = false
	return luaIntReply(int64(len(st.values))), nil
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
	return luaIntReply(int64(len(st.values))), nil
}

func (c *luaScriptContext) cmdLIndex(args []string) (luaReply, error) {
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaNilReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists || len(st.values) == 0 {
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
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaArrayReply(), nil
		}
		return luaReply{}, err
	}
	if !st.exists || len(st.values) == 0 {
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
	if !st.exists || len(st.values) == 0 {
		return luaIntReply(0), nil
	}
	count, err := strconv.Atoi(args[1])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
	}
	target := args[2]
	removed := 0
	values := append([]string(nil), st.values...)
	switch {
	case count == 0:
		filtered := values[:0]
		for _, value := range values {
			if value == target {
				removed++
				continue
			}
			filtered = append(filtered, value)
		}
		values = filtered
	case count > 0:
		filtered := make([]string, 0, len(values))
		for _, value := range values {
			if value == target && removed < count {
				removed++
				continue
			}
			filtered = append(filtered, value)
		}
		values = filtered
	default:
		filtered := make([]string, 0, len(values))
		for i := len(values) - 1; i >= 0; i-- {
			value := values[i]
			if value == target && removed < -count {
				removed++
				continue
			}
			filtered = append(filtered, value)
		}
		reverseStrings(filtered)
		values = filtered
	}
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

func (c *luaScriptContext) cmdLTrim(args []string) (luaReply, error) {
	st, err := c.listState([]byte(args[0]))
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return luaStatusReply("OK"), nil
		}
		return luaReply{}, err
	}
	if !st.exists || len(st.values) == 0 {
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
	if !st.exists || len(st.values) == 0 {
		return luaNilReply(), nil
	}
	var value string
	if left {
		value = st.values[0]
		st.values = st.values[1:]
	} else {
		value = st.values[len(st.values)-1]
		st.values = st.values[:len(st.values)-1]
	}
	st.dirty = true
	if len(st.values) == 0 {
		st.exists = false
		c.deleted[key] = true
		c.clearTTL([]byte(key))
	}
	c.markTouched([]byte(key))
	return luaStringReply(value), nil
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
	removed := 0
	for _, member := range args[1:] {
		if _, ok := st.members[member]; !ok {
			continue
		}
		delete(st.members, member)
		removed++
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

func (c *luaScriptContext) cmdZAdd(args []string) (luaReply, error) {
	if (len(args)-1)%2 != 0 {
		return luaReply{}, errors.New("ERR syntax error")
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
	for i := 1; i < len(args); i += 2 {
		score, err := strconv.ParseFloat(args[i], 64)
		if err != nil {
			return luaReply{}, errors.WithStack(err)
		}
		if _, ok := st.members[args[i+1]]; !ok {
			added++
		}
		st.members[args[i+1]] = score
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
	withScores := len(args) > 3 && strings.EqualFold(args[3], "WITHSCORES")
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

	minRaw := args[1]
	maxRaw := args[2]
	if reverse {
		maxRaw = args[1]
		minRaw = args[2]
	}
	minBound, err := parseZScoreBound(minRaw)
	if err != nil {
		return luaReply{}, err
	}
	maxBound, err := parseZScoreBound(maxRaw)
	if err != nil {
		return luaReply{}, err
	}

	withScores := false
	offset := 0
	limit := -1
	for i := 3; i < len(args); i++ {
		switch strings.ToUpper(args[i]) {
		case "WITHSCORES":
			withScores = true
		case "LIMIT":
			if i+2 >= len(args) {
				return luaReply{}, errors.New("ERR syntax error")
			}
			offset, err = strconv.Atoi(args[i+1])
			if err != nil {
				return luaReply{}, errors.WithStack(err)
			}
			limit, err = strconv.Atoi(args[i+2])
			if err != nil {
				return luaReply{}, errors.WithStack(err)
			}
			i += 2
		default:
			return luaReply{}, errors.New("ERR syntax error")
		}
	}

	entries := zsetMapToEntries(st.members)
	if reverse {
		reverseEntries(entries)
	}
	selected := make([]redisZSetEntry, 0, len(entries))
	for _, entry := range entries {
		if scoreInRange(entry.Score, minBound, maxBound) {
			selected = append(selected, entry)
		}
	}
	if offset > len(selected) {
		return luaArrayReply(), nil
	}
	selected = selected[offset:]
	if limit >= 0 && limit < len(selected) {
		selected = selected[:limit]
	}
	return zsetRangeReply(selected, withScores), nil
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
	removed := 0
	for _, member := range args[1:] {
		if _, ok := st.members[member]; !ok {
			continue
		}
		delete(st.members, member)
		removed++
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
	out := make([]luaReply, 0, count*2)
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

func (c *luaScriptContext) cmdXAdd(args []string) (luaReply, error) {
	if len(args) < 3 {
		return luaReply{}, errors.New("ERR wrong number of arguments for 'XADD' command")
	}
	key := []byte(args[0])
	st, err := c.streamState(key)
	if err != nil {
		return luaReply{}, err
	}
	if !st.exists {
		st.exists = true
	}
	maxLen := -1
	argIndex := 1
	if strings.EqualFold(args[argIndex], "MAXLEN") {
		argIndex++
		if argIndex < len(args) && (args[argIndex] == "~" || args[argIndex] == "=") {
			argIndex++
		}
		if argIndex >= len(args) {
			return luaReply{}, errors.New("ERR syntax error")
		}
		maxLen, err = strconv.Atoi(args[argIndex])
		if err != nil {
			return luaReply{}, errors.WithStack(err)
		}
		argIndex++
	}
	if argIndex >= len(args) {
		return luaReply{}, errors.New("ERR syntax error")
	}
	id := args[argIndex]
	argIndex++
	if (len(args)-argIndex)%2 != 0 {
		return luaReply{}, errors.New("ERR wrong number of arguments for 'XADD' command")
	}
	if id == "*" {
		id = nextLuaStreamID(st.value)
	} else if len(st.value.Entries) > 0 && compareRedisStreamID(id, st.value.Entries[len(st.value.Entries)-1].ID) <= 0 {
		return luaReply{}, errors.New("ERR The ID specified in XADD is equal or smaller than the target stream top item")
	}
	fields := append([]string(nil), args[argIndex:]...)
	st.value.Entries = append(st.value.Entries, redisStreamEntry{ID: id, Fields: fields})
	st.loaded = true
	st.dirty = true
	if maxLen >= 0 && len(st.value.Entries) > maxLen {
		st.value.Entries = append([]redisStreamEntry(nil), st.value.Entries[len(st.value.Entries)-maxLen:]...)
	}
	c.markTouched(key)
	c.deleted[string(key)] = false
	return luaStringReply(id), nil
}

func (c *luaScriptContext) cmdXTrim(args []string) (luaReply, error) {
	if len(args) < 4 || !strings.EqualFold(args[1], "MAXLEN") {
		return luaReply{}, errors.New("ERR syntax error")
	}
	key := []byte(args[0])
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
	argIndex := 2
	if args[argIndex] == "~" || args[argIndex] == "=" {
		argIndex++
	}
	maxLen, err := strconv.Atoi(args[argIndex])
	if err != nil {
		return luaReply{}, errors.WithStack(err)
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

func (c *luaScriptContext) commit() error {
	keys := make([]string, 0, len(c.touched))
	for key := range c.touched {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	elems := make([]*kv.Elem[kv.OP], 0, len(keys)*2)
	ctx := context.Background()
	for _, key := range keys {
		deleteElems, _, err := c.server.deleteLogicalKeyElems(ctx, []byte(key), c.startTS)
		if err != nil {
			return err
		}
		elems = append(elems, deleteElems...)

		finalType, err := c.finalType([]byte(key))
		if err != nil {
			return err
		}
		switch finalType {
		case redisTypeNone:
		case redisTypeString:
			st := c.strings[key]
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: []byte(key), Value: append([]byte(nil), st.value...)})
		case redisTypeList:
			st := c.lists[key]
			values := make([][]byte, 0, len(st.values))
			for _, value := range st.values {
				values = append(values, []byte(value))
			}
			listElems, _, err := c.server.buildRPushOps(store.ListMeta{}, []byte(key), values)
			if err != nil {
				return err
			}
			elems = append(elems, listElems...)
		case redisTypeHash:
			st := c.hashes[key]
			payload, err := marshalHashValue(st.value)
			if err != nil {
				return err
			}
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisHashKey([]byte(key)), Value: payload})
		case redisTypeSet:
			st := c.sets[key]
			payload, err := marshalSetValue(redisSetValue{Members: sortedSetMembers(st.members)})
			if err != nil {
				return err
			}
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisSetKey([]byte(key)), Value: payload})
		case redisTypeZSet:
			st := c.zsets[key]
			payload, err := marshalZSetValue(redisZSetValue{Entries: zsetMapToEntries(st.members)})
			if err != nil {
				return err
			}
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisZSetKey([]byte(key)), Value: payload})
		case redisTypeStream:
			st := c.streams[key]
			payload, err := marshalStreamValue(st.value)
			if err != nil {
				return err
			}
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisStreamKey([]byte(key)), Value: payload})
		}

		if finalType != redisTypeNone {
			ttl, err := c.finalTTL([]byte(key))
			if err != nil {
				return err
			}
			if ttl != nil {
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: redisTTLKey([]byte(key)), Value: encodeRedisTTL(*ttl)})
			}
		}
	}

	if len(elems) == 0 {
		return nil
	}
	dispatchCtx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()
	return c.server.dispatchElems(dispatchCtx, true, elems)
}

func (c *luaScriptContext) finalType(key []byte) (redisValueType, error) {
	k := string(key)
	if st, ok := c.strings[k]; ok && st.loaded && st.exists {
		return redisTypeString, nil
	}
	if st, ok := c.lists[k]; ok && st.loaded && st.exists {
		return redisTypeList, nil
	}
	if st, ok := c.hashes[k]; ok && st.loaded && st.exists {
		return redisTypeHash, nil
	}
	if st, ok := c.sets[k]; ok && st.loaded && st.exists {
		return redisTypeSet, nil
	}
	if st, ok := c.zsets[k]; ok && st.loaded && st.exists {
		return redisTypeZSet, nil
	}
	if st, ok := c.streams[k]; ok && st.loaded && st.exists {
		return redisTypeStream, nil
	}
	if c.deleted[k] {
		return redisTypeNone, nil
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
			ID:     entry.ID,
			Fields: append([]string(nil), entry.Fields...),
		})
	}
	return out
}

func nextLuaStreamID(stream redisStreamValue) string {
	nowID := strconv.FormatInt(time.Now().UnixMilli(), 10) + "-0"
	if len(stream.Entries) == 0 {
		return nowID
	}
	lastID := stream.Entries[len(stream.Entries)-1].ID
	if compareRedisStreamID(nowID, lastID) > 0 {
		return nowID
	}
	last, err := parseRedisStreamID(lastID)
	if err != nil {
		return nowID
	}
	return strconv.FormatUint(last.ms, 10) + "-" + strconv.FormatUint(last.seq+1, 10)
}
