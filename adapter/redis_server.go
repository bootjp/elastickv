package adapter

import (
	"bytes"
	"context"
	"math"
	"net"
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/hashicorp/raft"
	"github.com/tidwall/redcon"
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
