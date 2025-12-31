package adapter

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//nolint:mnd
var argsLen = map[string]int{
	"GET":     2,
	"SET":     3,
	"DEL":     2,
	"EXISTS":  2,
	"PING":    1,
	"KEYS":    2,
	"MULTI":   1,
	"EXEC":    1,
	"DISCARD": 1,
	"LRANGE":  4,
	"RPUSH":   -3, // negative means minimum number of args
}

type RedisServer struct {
	listen          net.Listener
	store           store.MVCCStore
	coordinator     kv.Coordinator
	redisTranscoder *redisTranscoder
	// TODO manage membership from raft log
	leaderRedis map[raft.ServerAddress]string

	route map[string]func(conn redcon.Conn, cmd redcon.Command)
}

type connState struct {
	inTxn bool
	queue []redcon.Command
}

type resultType int

const (
	resultNil resultType = iota
	resultError
	resultBulk
	resultString
	resultArray
	resultInt
)

type redisResult struct {
	typ     resultType
	bulk    []byte
	str     string
	arr     []string
	integer int64
	err     error
}

func NewRedisServer(listen net.Listener, store store.MVCCStore, coordinate *kv.Coordinate, leaderRedis map[raft.ServerAddress]string) *RedisServer {
	r := &RedisServer{
		listen:          listen,
		store:           store,
		coordinator:     coordinate,
		redisTranscoder: newRedisTranscoder(),
		leaderRedis:     leaderRedis,
	}

	r.route = map[string]func(conn redcon.Conn, cmd redcon.Command){
		"PING":    r.ping,
		"SET":     r.set,
		"GET":     r.get,
		"DEL":     r.del,
		"EXISTS":  r.exists,
		"KEYS":    r.keys,
		"MULTI":   r.multi,
		"EXEC":    r.exec,
		"DISCARD": r.discard,
		"RPUSH":   r.rpush,
		"LRANGE":  r.lrange,
	}

	return r
}

func getConnState(conn redcon.Conn) *connState {
	if ctx := conn.Context(); ctx != nil {
		if st, ok := ctx.(*connState); ok {
			return st
		}
	}
	st := &connState{}
	conn.SetContext(st)
	return st
}

func (r *RedisServer) readTS() uint64 {
	return snapshotTS(r.coordinator.Clock(), r.store)
}

func (r *RedisServer) Run() error {
	err := redcon.Serve(r.listen,
		func(conn redcon.Conn, cmd redcon.Command) {
			state := getConnState(conn)
			f, ok := r.route[strings.ToUpper(string(cmd.Args[0]))]
			if !ok {
				conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
				return
			}

			if err := r.validateCmd(cmd); err != nil {
				conn.WriteError(err.Error())
				return
			}

			name := strings.ToUpper(string(cmd.Args[0]))
			if state.inTxn && name != "EXEC" && name != "DISCARD" && name != "MULTI" {
				state.queue = append(state.queue, cmd)
				conn.WriteString("QUEUED")
				return
			}

			f(conn, cmd)
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		})

	return errors.WithStack(err)
}

func (r *RedisServer) Stop() {
	_ = r.listen.Close()
}

func (r *RedisServer) validateCmd(cmd redcon.Command) error {
	name := strings.ToUpper(string(cmd.Args[0]))
	expected, ok := argsLen[name]
	if !ok {
		return nil
	}

	switch {
	case expected > 0 && len(cmd.Args) != expected:
		//nolint:wrapcheck
		return errors.WithStack(errors.Newf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0])))
	case expected < 0 && len(cmd.Args) < -expected:
		return errors.WithStack(errors.Newf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0])))
	}
	return nil
}

func (r *RedisServer) ping(conn redcon.Conn, _ redcon.Command) {
	conn.WriteString("PONG")
}

func (r *RedisServer) set(conn redcon.Conn, cmd redcon.Command) {
	// Prevent overwriting list keys with string values without cleanup.
	isList, err := r.isListKey(context.Background(), cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	if isList {
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	res, err := r.redisTranscoder.SetToRequest(cmd.Args[1], cmd.Args[2])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	_, err = r.coordinator.Dispatch(res)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteString("OK")
}

func (r *RedisServer) get(conn redcon.Conn, cmd redcon.Command) {
	if ok, err := r.isListKey(context.Background(), cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
		return
	} else if ok {
		conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
		return
	}

	readTS := r.readTS()
	v, err := r.readValueAt(cmd.Args[1], readTS)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrKeyNotFound):
			conn.WriteNull()
		default:
			conn.WriteError(err.Error())
		}
		return
	}

	conn.WriteBulk(v)
}

func (r *RedisServer) del(conn redcon.Conn, cmd redcon.Command) {
	if ok, err := r.isListKey(context.Background(), cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
		return
	} else if ok {
		if err := r.deleteList(context.Background(), cmd.Args[1]); err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(1)
		return
	}

	res, err := r.redisTranscoder.DeleteToRequest(cmd.Args[1])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	_, err = r.coordinator.Dispatch(res)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteInt(1)
}

func (r *RedisServer) exists(conn redcon.Conn, cmd redcon.Command) {
	if !r.coordinator.IsLeader() {
		res, err := r.proxyExists(cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteInt(res)
		return
	}

	if err := r.coordinator.VerifyLeader(); err != nil {
		conn.WriteError(err.Error())
		return
	}

	if ok, err := r.isListKey(context.Background(), cmd.Args[1]); err != nil {
		conn.WriteError(err.Error())
		return
	} else if ok {
		conn.WriteInt(1)
		return
	}

	readTS := r.readTS()
	ok, err := r.store.ExistsAt(context.Background(), cmd.Args[1], readTS)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	if ok {
		conn.WriteInt(1)
		return
	}
	conn.WriteInt(0)
}

func (r *RedisServer) keys(conn redcon.Conn, cmd redcon.Command) {
	pattern := cmd.Args[1]

	if r.coordinator.IsLeader() {
		if err := r.coordinator.VerifyLeader(); err != nil {
			conn.WriteError(err.Error())
			return
		}
		keys, err := r.localKeys(pattern)
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		conn.WriteArray(len(keys))
		for _, k := range keys {
			conn.WriteBulk(k)
		}
		return
	}

	keys, err := r.proxyKeys(pattern)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(len(keys))
	for _, k := range keys {
		conn.WriteBulkString(k)
	}
}

func (r *RedisServer) localKeys(pattern []byte) ([][]byte, error) {
	if !bytes.Contains(pattern, []byte("*")) {
		return r.localKeysExact(pattern)
	}
	return r.localKeysPattern(pattern)
}

func (r *RedisServer) localKeysExact(pattern []byte) ([][]byte, error) {
	readTS := r.readTS()
	res, err := r.store.ExistsAt(context.Background(), pattern, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if res {
		return [][]byte{bytes.Clone(pattern)}, nil
	}
	return [][]byte{}, nil
}

func (r *RedisServer) localKeysPattern(pattern []byte) ([][]byte, error) {
	start := r.patternStart(pattern)

	readTS := r.readTS()
	keys, err := r.store.ScanAt(context.Background(), start, nil, math.MaxInt, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	keyset := r.collectUserKeys(keys)

	out := make([][]byte, 0, len(keyset))
	for _, v := range keyset {
		out = append(out, v)
	}
	return out, nil
}

func (r *RedisServer) patternStart(pattern []byte) []byte {
	if bytes.Equal(pattern, []byte("*")) {
		return nil
	}
	return bytes.ReplaceAll(pattern, []byte("*"), nil)
}

func (r *RedisServer) collectUserKeys(kvs []*store.KVPair) map[string][]byte {
	keyset := map[string][]byte{}
	for _, kvPair := range kvs {
		if store.IsListMetaKey(kvPair.Key) || store.IsListItemKey(kvPair.Key) {
			if userKey := store.ExtractListUserKey(kvPair.Key); userKey != nil {
				keyset[string(userKey)] = userKey
			}
			continue
		}
		keyset[string(kvPair.Key)] = kvPair.Key
	}
	return keyset
}

func (r *RedisServer) proxyKeys(pattern []byte) ([]string, error) {
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		return nil, ErrLeaderNotFound
	}

	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return nil, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}

	cli := redis.NewClient(&redis.Options{
		Addr: leaderAddr,
	})
	defer func() {
		_ = cli.Close()
	}()

	keys, err := cli.Keys(context.Background(), string(pattern)).Result()
	return keys, errors.WithStack(err)
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

	if !r.coordinator.IsLeader() {
		if err := r.proxyExec(conn, state.queue); err != nil {
			conn.WriteError(err.Error())
		}
		state.inTxn = false
		state.queue = nil
		return
	}

	results, err := r.runTransaction(state.queue)
	state.inTxn = false
	state.queue = nil
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	r.writeResults(conn, results)
}

type txnValue struct {
	raw     []byte
	deleted bool
	dirty   bool
	loaded  bool
}

type txnContext struct {
	server     *RedisServer
	working    map[string]*txnValue
	listStates map[string]*listTxnState
	startTS    uint64
}

type listTxnState struct {
	meta       store.ListMeta
	metaExists bool
	appends    [][]byte
	deleted    bool
}

func (t *txnContext) load(key []byte) (*txnValue, error) {
	k := string(key)
	if tv, ok := t.working[k]; ok {
		return tv, nil
	}
	tv := &txnValue{}
	val, err := t.server.readValueAt(key, t.startTS)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.WithStack(err)
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

	meta, exists, err := t.server.loadListMetaAt(context.Background(), key, t.startTS)
	if err != nil {
		return nil, err
	}

	st := &listTxnState{
		meta:       meta,
		metaExists: exists,
		appends:    [][]byte{},
	}
	t.listStates[k] = st
	return st, nil
}

func (t *txnContext) listLength(st *listTxnState) int64 {
	return st.meta.Len + int64(len(st.appends))
}

func (t *txnContext) apply(cmd redcon.Command) (redisResult, error) {
	switch strings.ToUpper(string(cmd.Args[0])) {
	case "SET":
		return t.applySet(cmd)
	case "DEL":
		return t.applyDel(cmd)
	case "GET":
		return t.applyGet(cmd)
	case "EXISTS":
		return t.applyExists(cmd)
	case "RPUSH":
		return t.applyRPush(cmd)
	case "LRANGE":
		return t.applyLRange(cmd)
	default:
		return redisResult{}, errors.WithStack(errors.Newf("ERR unsupported command '%s'", cmd.Args[0]))
	}
}

func (t *txnContext) applySet(cmd redcon.Command) (redisResult, error) {
	if isList, err := t.server.isListKeyAt(context.Background(), cmd.Args[1], t.startTS); err != nil {
		return redisResult{}, err
	} else if isList {
		return redisResult{typ: resultError, err: errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")}, nil
	}

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	tv.raw = cmd.Args[2]
	tv.deleted = false
	tv.dirty = true
	return redisResult{typ: resultString, str: "OK"}, nil
}

func (t *txnContext) applyDel(cmd redcon.Command) (redisResult, error) {
	// handle list delete separately
	if isList, err := t.server.isListKeyAt(context.Background(), cmd.Args[1], t.startTS); err != nil {
		return redisResult{}, err
	} else if isList {
		st, err := t.loadListState(cmd.Args[1])
		if err != nil {
			return redisResult{}, err
		}
		st.deleted = true
		st.appends = nil
		return redisResult{typ: resultInt, integer: 1}, nil
	}

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	tv.deleted = true
	tv.dirty = true
	return redisResult{typ: resultInt, integer: 1}, nil
}

func (t *txnContext) applyGet(cmd redcon.Command) (redisResult, error) {
	if isList, err := t.server.isListKeyAt(context.Background(), cmd.Args[1], t.startTS); err != nil {
		return redisResult{}, err
	} else if isList {
		return redisResult{typ: resultError, err: errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")}, nil
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
	if isList, err := t.server.isListKeyAt(context.Background(), cmd.Args[1], t.startTS); err != nil {
		return redisResult{}, err
	} else if isList {
		return redisResult{typ: resultInt, integer: 1}, nil
	}

	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if tv.deleted || tv.raw == nil {
		return redisResult{typ: resultInt, integer: 0}, nil
	}
	return redisResult{typ: resultInt, integer: 1}, nil
}

func (t *txnContext) applyRPush(cmd redcon.Command) (redisResult, error) {
	st, err := t.loadListState(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
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

	switch {
	case e < persistedLen:
		return t.server.fetchListRange(context.Background(), key, st.meta, int64(s), int64(e), t.startTS)
	case s >= persistedLen:
		return appendValues(st.appends, s-persistedLen, e-persistedLen), nil
	default:
		head, err := t.server.fetchListRange(context.Background(), key, st.meta, int64(s), int64(persistedLen-1), t.startTS)
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

func (t *txnContext) commit() error {
	elems := t.buildKeyElems()

	listElems, err := t.buildListElems()
	if err != nil {
		return err
	}

	elems = append(elems, listElems...)
	if len(elems) == 0 {
		return nil
	}

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: elems, StartTS: t.startTS}
	if _, err := t.server.coordinator.Dispatch(group); err != nil {
		return errors.WithStack(err)
	}
	return nil
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
		key := []byte(k)
		if tv.deleted {
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: key})
			continue
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: tv.raw})
	}
	return elems
}

func (t *txnContext) buildListElems() ([]*kv.Elem[kv.OP], error) {
	listKeys := make([]string, 0, len(t.listStates))
	for k := range t.listStates {
		listKeys = append(listKeys, k)
	}
	sort.Strings(listKeys)

	var elems []*kv.Elem[kv.OP]
	for _, k := range listKeys {
		st := t.listStates[k]
		userKey := []byte(k)

		if st.deleted {
			// delete all persisted list items
			for seq := st.meta.Head; seq < st.meta.Tail; seq++ {
				elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listItemKey(userKey, seq)})
			}
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(userKey)})
			continue
		}
		if len(st.appends) == 0 {
			continue
		}

		startSeq := st.meta.Head + st.meta.Len
		for i, v := range st.appends {
			elems = append(elems, &kv.Elem[kv.OP]{
				Op:    kv.Put,
				Key:   listItemKey(userKey, startSeq+int64(i)),
				Value: v,
			})
		}

		st.meta.Len += int64(len(st.appends))
		st.meta.Tail = st.meta.Head + st.meta.Len
		metaBytes, err := store.MarshalListMeta(st.meta)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listMetaKey(userKey), Value: metaBytes})
	}
	return elems, nil
}

func (r *RedisServer) runTransaction(queue []redcon.Command) ([]redisResult, error) {
	if err := r.coordinator.VerifyLeader(); err != nil {
		return nil, errors.WithStack(err)
	}

	startTS := r.coordinator.Clock().Next()
	if last := r.store.LastCommitTS(); last > startTS {
		startTS = last
	}

	ctx := &txnContext{
		server:     r,
		working:    map[string]*txnValue{},
		listStates: map[string]*listTxnState{},
		startTS:    startTS,
	}

	results := make([]redisResult, 0, len(queue))
	for _, cmd := range queue {
		res, err := ctx.apply(cmd)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}

	if err := ctx.commit(); err != nil {
		return nil, err
	}

	return results, nil
}

func (r *RedisServer) proxyExec(conn redcon.Conn, queue []redcon.Command) error {
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		return ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}

	cli := redis.NewClient(&redis.Options{Addr: leaderAddr})
	defer func() { _ = cli.Close() }()

	ctx := context.Background()
	cmds := make([]redis.Cmder, len(queue))
	names := make([]string, len(queue))
	_, err := cli.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, c := range queue {
			name := strings.ToUpper(string(c.Args[0]))
			names[i] = name
			args := make([]string, 0, len(c.Args)-1)
			for _, a := range c.Args[1:] {
				args = append(args, string(a))
			}
			cmd := newProxyCmd(name, args, ctx)
			_ = pipe.Process(ctx, cmd)
			cmds[i] = cmd
		}
		return nil
	})
	if err != nil {
		return errors.WithStack(err)
	}

	results := make([]redisResult, 0, len(cmds))
	for i, cmd := range cmds {
		res, err := buildProxyResult(names[i], cmd)
		if err != nil {
			results = append(results, redisResult{typ: resultError, err: err})
			continue
		}
		results = append(results, res)
	}

	r.writeResults(conn, results)
	return nil
}

func (r *RedisServer) writeResults(conn redcon.Conn, results []redisResult) {
	conn.WriteArray(len(results))
	for _, res := range results {
		switch res.typ {
		case resultNil:
			conn.WriteNull()
		case resultError:
			conn.WriteError(res.err.Error())
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

// --- list helpers ----------------------------------------------------
func buildProxyResult(_ string, cmd redis.Cmder) (redisResult, error) {
	switch c := cmd.(type) {
	case *redis.StatusCmd:
		s, err := c.Result()
		return redisResult{typ: resultString, str: s}, errors.WithStack(err)
	case *redis.IntCmd:
		i, err := c.Result()
		return redisResult{typ: resultInt, integer: i}, errors.WithStack(err)
	case *redis.StringCmd:
		b, err := c.Bytes()
		if errors.Is(err, redis.Nil) {
			return redisResult{typ: resultNil}, nil
		}
		return redisResult{typ: resultBulk, bulk: b}, errors.WithStack(err)
	case *redis.StringSliceCmd:
		arr, err := c.Result()
		return redisResult{typ: resultArray, arr: arr}, errors.WithStack(err)
	case *redis.Cmd:
		v, err := c.Result()
		return redisResult{typ: resultString, str: fmt.Sprint(v)}, errors.WithStack(err)
	default:
		return redisResult{typ: resultError, err: errors.Newf("unsupported command result type %T", cmd)}, nil
	}
}

func newProxyCmd(name string, args []string, ctx context.Context) redis.Cmder {
	argv := make([]any, 0, len(args)+1)
	argv = append(argv, name)
	for _, a := range args {
		argv = append(argv, a)
	}

	switch name {
	case "SET":
		return redis.NewStatusCmd(ctx, argv...)
	case "DEL", "EXISTS", "RPUSH":
		return redis.NewIntCmd(ctx, argv...)
	case "GET":
		return redis.NewStringCmd(ctx, argv...)
	case "LRANGE":
		return redis.NewStringSliceCmd(ctx, argv...)
	default:
		return redis.NewCmd(ctx, argv...)
	}
}

func listMetaKey(userKey []byte) []byte {
	return store.ListMetaKey(userKey)
}

func listItemKey(userKey []byte, seq int64) []byte {
	return store.ListItemKey(userKey, seq)
}

func clampRange(start, end, length int) (int, int) {
	if start < 0 {
		start = length + start
	}
	if end < 0 {
		end = length + end
	}
	if start < 0 {
		start = 0
	}
	if end >= length {
		end = length - 1
	}
	if end < start {
		return 0, -1
	}
	return start, end
}

func (r *RedisServer) loadListMeta(ctx context.Context, key []byte) (store.ListMeta, bool, error) {
	return r.loadListMetaAt(ctx, key, r.readTS())
}

func (r *RedisServer) loadListMetaAt(ctx context.Context, key []byte, readTS uint64) (store.ListMeta, bool, error) {
	val, err := r.store.GetAt(ctx, store.ListMetaKey(key), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return store.ListMeta{}, false, nil
		}
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	meta, err := store.UnmarshalListMeta(val)
	if err != nil {
		return store.ListMeta{}, false, errors.WithStack(err)
	}
	return meta, true, nil
}

func (r *RedisServer) isListKey(ctx context.Context, key []byte) (bool, error) {
	_, exists, err := r.loadListMetaAt(ctx, key, r.readTS())
	return exists, err
}

func (r *RedisServer) isListKeyAt(ctx context.Context, key []byte, readTS uint64) (bool, error) {
	_, exists, err := r.loadListMetaAt(ctx, key, readTS)
	return exists, err
}

func (r *RedisServer) buildRPushOps(meta store.ListMeta, key []byte, values [][]byte) ([]*kv.Elem[kv.OP], store.ListMeta, error) {
	if len(values) == 0 {
		return nil, meta, nil
	}

	elems := make([]*kv.Elem[kv.OP], 0, len(values)+1)
	seq := meta.Head + meta.Len
	for _, v := range values {
		vCopy := bytes.Clone(v)
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listItemKey(key, seq), Value: vCopy})
		seq++
	}

	meta.Len += int64(len(values))
	meta.Tail = meta.Head + meta.Len

	b, err := store.MarshalListMeta(meta)
	if err != nil {
		return nil, meta, errors.WithStack(err)
	}

	elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: listMetaKey(key), Value: b})
	return elems, meta, nil
}

func (r *RedisServer) listRPush(ctx context.Context, key []byte, values [][]byte) (int64, error) {
	meta, _, err := r.loadListMeta(ctx, key)
	if err != nil {
		return 0, err
	}

	ops, newMeta, err := r.buildRPushOps(meta, key, values)
	if err != nil {
		return 0, err
	}
	if len(ops) == 0 {
		return newMeta.Len, nil
	}

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: ops}
	if _, err := r.coordinator.Dispatch(group); err != nil {
		return 0, errors.WithStack(err)
	}
	return newMeta.Len, nil
}

func (r *RedisServer) deleteList(ctx context.Context, key []byte) error {
	meta, exists, err := r.loadListMeta(ctx, key)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	start := listItemKey(key, math.MinInt64)
	end := listItemKey(key, math.MaxInt64)

	readTS := r.readTS()
	kvs, err := r.store.ScanAt(ctx, start, end, math.MaxInt, readTS)
	if err != nil {
		return errors.WithStack(err)
	}

	ops := make([]*kv.Elem[kv.OP], 0, len(kvs)+1)
	for _, kvp := range kvs {
		ops = append(ops, &kv.Elem[kv.OP]{Op: kv.Del, Key: kvp.Key})
	}
	// delete meta last
	ops = append(ops, &kv.Elem[kv.OP]{Op: kv.Del, Key: listMetaKey(key)})

	// ensure meta bounds consistent even if scan missed (in case of empty list)
	_ = meta

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: ops}
	_, err = r.coordinator.Dispatch(group)
	return errors.WithStack(err)
}

func (r *RedisServer) fetchListRange(ctx context.Context, key []byte, meta store.ListMeta, startIdx, endIdx int64, readTS uint64) ([]string, error) {
	if endIdx < startIdx {
		return []string{}, nil
	}

	startSeq := meta.Head + startIdx
	endSeq := meta.Head + endIdx

	startKey := listItemKey(key, startSeq)
	endKey := listItemKey(key, endSeq+1) // exclusive

	kvs, err := r.store.ScanAt(ctx, startKey, endKey, int(endIdx-startIdx+1), readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	out := make([]string, 0, len(kvs))
	for _, kvp := range kvs {
		out = append(out, string(kvp.Value))
	}
	return out, nil
}

func (r *RedisServer) rangeList(key []byte, startRaw, endRaw []byte) ([]string, error) {
	readTS := r.readTS()
	if !r.coordinator.IsLeader() {
		return r.proxyLRange(key, startRaw, endRaw)
	}

	if err := r.coordinator.VerifyLeader(); err != nil {
		return nil, errors.WithStack(err)
	}

	meta, exists, err := r.loadListMetaAt(context.Background(), key, readTS)
	if err != nil {
		return nil, err
	}
	if !exists || meta.Len == 0 {
		return []string{}, nil
	}

	start, err := strconv.Atoi(string(startRaw))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	end, err := strconv.Atoi(string(endRaw))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	s, e := clampRange(start, end, int(meta.Len))
	if e < s {
		return []string{}, nil
	}

	return r.fetchListRange(context.Background(), key, meta, int64(s), int64(e), readTS)
}

func (r *RedisServer) proxyLRange(key []byte, startRaw, endRaw []byte) ([]string, error) {
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		return nil, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return nil, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}

	cli := redis.NewClient(&redis.Options{Addr: leaderAddr})
	defer func() { _ = cli.Close() }()

	start, err := parseInt(startRaw)
	if err != nil {
		return nil, err
	}
	end, err := parseInt(endRaw)
	if err != nil {
		return nil, err
	}

	res, err := cli.LRange(context.Background(), string(key), int64(start), int64(end)).Result()
	return res, errors.WithStack(err)
}

func (r *RedisServer) proxyRPush(key []byte, values [][]byte) (int64, error) {
	leader := r.coordinator.RaftLeader()
	if leader == "" {
		return 0, ErrLeaderNotFound
	}
	leaderAddr, ok := r.leaderRedis[leader]
	if !ok || leaderAddr == "" {
		return 0, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
	}

	cli := redis.NewClient(&redis.Options{Addr: leaderAddr})
	defer func() { _ = cli.Close() }()

	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, string(v))
	}

	res, err := cli.RPush(context.Background(), string(key), args...).Result()
	return res, errors.WithStack(err)
}

func parseInt(b []byte) (int, error) {
	i, err := strconv.Atoi(string(b))
	return i, errors.WithStack(err)
}

// tryLeaderGet proxies a GET to the current Raft leader, returning the value and
// whether the proxy succeeded.
func (r *RedisServer) tryLeaderGetAt(key []byte, ts uint64) ([]byte, error) {
	addr := r.coordinator.RaftLeader()
	if addr == "" {
		return nil, ErrLeaderNotFound
	}

	conn, err := grpc.NewClient(string(addr),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.WaitForReady(true)),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer conn.Close()

	cli := pb.NewRawKVClient(conn)
	resp, err := cli.RawGet(context.Background(), &pb.RawGetRequest{Key: key, Ts: ts})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resp.Value, nil
}

func (r *RedisServer) readValueAt(key []byte, readTS uint64) ([]byte, error) {
	if r.coordinator.IsLeader() {
		if err := r.coordinator.VerifyLeader(); err != nil {
			return nil, errors.WithStack(err)
		}
		v, err := r.store.GetAt(context.Background(), key, readTS)
		return v, errors.WithStack(err)
	}
	return r.tryLeaderGetAt(key, readTS)
}

func (r *RedisServer) rpush(conn redcon.Conn, cmd redcon.Command) {
	ctx := context.Background()

	var length int64
	var err error
	if r.coordinator.IsLeader() {
		length, err = r.listRPush(ctx, cmd.Args[1], cmd.Args[2:])
	} else {
		length, err = r.proxyRPush(cmd.Args[1], cmd.Args[2:])
	}

	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt64(length)
}

func (r *RedisServer) lrange(conn redcon.Conn, cmd redcon.Command) {
	items, err := r.rangeList(cmd.Args[1], cmd.Args[2], cmd.Args[3])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteArray(len(items))
	for _, it := range items {
		conn.WriteBulkString(it)
	}
}
