package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
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
	store           store.ScanStore
	coordinator     kv.Coordinator
	redisTranscoder *redisTranscoder

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

func NewRedisServer(listen net.Listener, store store.ScanStore, coordinate *kv.Coordinate) *RedisServer {
	r := &RedisServer{
		listen:          listen,
		store:           store,
		coordinator:     coordinate,
		redisTranscoder: newRedisTranscoder(),
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
	if r.coordinator.IsLeader() {
		v, err := r.store.Get(context.Background(), cmd.Args[1])
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
		return
	}

	v, err := r.tryLeaderGet(cmd.Args[1])
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
	ok, err := r.store.Exists(context.Background(), cmd.Args[1])
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

	// If an asterisk (*) is not included, the match will be exact,
	// so check if the key exists.
	if !bytes.Contains(cmd.Args[1], []byte("*")) {
		res, err := r.store.Exists(context.Background(), cmd.Args[1])
		if err != nil {
			conn.WriteError(err.Error())
			return
		}
		if res {
			conn.WriteArray(1)
			conn.WriteBulk(cmd.Args[1])
			return
		}
		conn.WriteArray(0)
		return
	}

	var start []byte
	switch {
	case bytes.Equal(cmd.Args[1], []byte("*")):
		start = nil
	default:
		start = bytes.ReplaceAll(cmd.Args[1], []byte("*"), nil)
	}

	keys, err := r.store.Scan(context.Background(), start, nil, math.MaxInt)
	if err != nil {
		conn.WriteError(err.Error())
		return
	}

	conn.WriteArray(len(keys))
	for _, kvPair := range keys {
		conn.WriteBulk(kvPair.Key)
	}
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
	list    []string
	isList  bool
	deleted bool
	dirty   bool
	loaded  bool
}

type txnContext struct {
	server  *RedisServer
	working map[string]*txnValue
}

func (t *txnContext) load(key []byte) (*txnValue, error) {
	k := string(key)
	if tv, ok := t.working[k]; ok {
		return tv, nil
	}
	tv := &txnValue{}
	val, err := t.server.getValue(key)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.WithStack(err)
	}
	tv.raw = val
	tv.loaded = true
	t.working[k] = tv
	return tv, nil
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
	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	tv.raw = cmd.Args[2]
	tv.isList = false
	tv.deleted = false
	tv.dirty = true
	return redisResult{typ: resultString, str: "OK"}, nil
}

func (t *txnContext) applyDel(cmd redcon.Command) (redisResult, error) {
	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	tv.deleted = true
	tv.dirty = true
	return redisResult{typ: resultInt, integer: 1}, nil
}

func (t *txnContext) applyGet(cmd redcon.Command) (redisResult, error) {
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
	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if tv.deleted || tv.raw == nil {
		return redisResult{typ: resultInt, integer: 0}, nil
	}
	return redisResult{typ: resultInt, integer: 1}, nil
}

func (t *txnContext) ensureList(tv *txnValue) error {
	if tv.isList {
		return nil
	}
	list, err := decodeList(tv.raw)
	if err != nil {
		return err
	}
	tv.list = list
	tv.isList = true
	return nil
}

func (t *txnContext) applyRPush(cmd redcon.Command) (redisResult, error) {
	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if err := t.ensureList(tv); err != nil {
		return redisResult{}, err
	}
	for _, v := range cmd.Args[2:] {
		tv.list = append(tv.list, string(v))
	}
	tv.dirty = true
	tv.deleted = false
	return redisResult{typ: resultInt, integer: int64(len(tv.list))}, nil
}

func (t *txnContext) applyLRange(cmd redcon.Command) (redisResult, error) {
	tv, err := t.load(cmd.Args[1])
	if err != nil {
		return redisResult{}, err
	}
	if err := t.ensureList(tv); err != nil {
		return redisResult{}, err
	}
	start, err := strconv.Atoi(string(cmd.Args[2]))
	if err != nil {
		return redisResult{}, errors.WithStack(err)
	}
	end, err := strconv.Atoi(string(cmd.Args[3]))
	if err != nil {
		return redisResult{}, errors.WithStack(err)
	}
	s, e := clampRange(start, end, len(tv.list))
	if e < s {
		return redisResult{typ: resultArray, arr: []string{}}, nil
	}
	return redisResult{typ: resultArray, arr: tv.list[s : e+1]}, nil
}

func (t *txnContext) commit() error {
	if len(t.working) == 0 {
		return nil
	}

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
		var val []byte
		if tv.isList {
			enc, err := encodeList(tv.list)
			if err != nil {
				return err
			}
			val = enc
		} else {
			val = tv.raw
		}
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Put, Key: key, Value: val})
	}

	if len(elems) == 0 {
		return nil
	}

	group := &kv.OperationGroup[kv.OP]{IsTxn: true, Elems: elems}
	if _, err := t.server.coordinator.Dispatch(group); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *RedisServer) runTransaction(queue []redcon.Command) ([]redisResult, error) {
	ctx := &txnContext{
		server:  r,
		working: map[string]*txnValue{},
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

// list helpers
func decodeList(b []byte) ([]string, error) {
	if b == nil {
		return []string{}, nil
	}
	var out []string
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

func encodeList(list []string) ([]byte, error) {
	b, err := json.Marshal(list)
	return b, errors.WithStack(err)
}

func (r *RedisServer) pushList(key []byte, values [][]byte) (int, error) {
	current, err := r.getValue(key)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return 0, errors.WithStack(err)
	}

	list, err := decodeList(current)
	if err != nil {
		return 0, err
	}
	for _, v := range values {
		list = append(list, string(v))
	}
	enc, err := encodeList(list)
	if err != nil {
		return 0, err
	}

	req, err := r.redisTranscoder.SetToRequest(key, enc)
	if err != nil {
		return 0, err
	}
	if _, err := r.coordinator.Dispatch(req); err != nil {
		return 0, errors.WithStack(err)
	}
	return len(list), nil
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

func (r *RedisServer) rangeList(key []byte, startRaw, endRaw []byte) ([]string, error) {
	val, err := r.getValue(key)
	if err != nil && !errors.Is(err, store.ErrKeyNotFound) {
		return nil, errors.WithStack(err)
	}
	list, err := decodeList(val)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	start, err := strconv.Atoi(string(startRaw))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	end, err := strconv.Atoi(string(endRaw))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	s, e := clampRange(start, end, len(list))
	if e < s {
		return []string{}, nil
	}
	return list[s : e+1], nil
}

// tryLeaderGet proxies a GET to the current Raft leader, returning the value and
// whether the proxy succeeded.
func (r *RedisServer) tryLeaderGet(key []byte) ([]byte, error) {
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
	resp, err := cli.RawGet(context.Background(), &pb.RawGetRequest{Key: key})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return resp.Value, nil
}

func (r *RedisServer) getValue(key []byte) ([]byte, error) {
	if r.coordinator.IsLeader() {
		v, err := r.store.Get(context.Background(), key)
		return v, errors.WithStack(err)
	}
	return r.tryLeaderGet(key)
}

func (r *RedisServer) rpush(conn redcon.Conn, cmd redcon.Command) {
	length, err := r.pushList(cmd.Args[1], cmd.Args[2:])
	if err != nil {
		conn.WriteError(err.Error())
		return
	}
	conn.WriteInt(length)
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
