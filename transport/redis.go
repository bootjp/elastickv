package transport

import (
	"context"
	"net"
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"github.com/tidwall/redcon"
)

//nolint:gomnd
var argsLen = map[string]int{
	"GET":    2,
	"SET":    3,
	"DEL":    2,
	"EXISTS": 2,
	"PING":   1,
}

const (
	indexKey   = 1
	indexValue = 2
)

type RedisServer struct {
	listen          net.Listener
	store           kv.Store
	coordinator     kv.Coordinator
	redisTranscoder *redisTranscoder

	route map[string]func(conn redcon.Conn, cmd redcon.Command)
}

func NewRedisServer(listen net.Listener, store kv.Store, coordinate *kv.Coordinate) *RedisServer {
	r := &RedisServer{
		listen:          listen,
		store:           store,
		coordinator:     coordinate,
		redisTranscoder: newRedisTranscoder(),
	}

	r.route = map[string]func(conn redcon.Conn, cmd redcon.Command){
		"PING":   r.ping,
		"SET":    r.set,
		"GET":    r.get,
		"DEL":    r.del,
		"EXISTS": r.Exists,
	}

	return r
}
func (r *RedisServer) Run() error {
	err := redcon.Serve(r.listen, func(conn redcon.Conn, cmd redcon.Command) {
		if err := r.validateCmd(cmd); err != nil {
			conn.WriteError(err.Error())
			return
		}
		if err := r.validateKey(cmd.Args[indexKey]); err != nil {
			conn.WriteError(err.Error())
			return
		}

		if err := r.validateValue(cmd.Args[indexValue]); err != nil {
			conn.WriteError(err.Error())
			return
		}

		f, ok := r.route[strings.ToUpper(string(cmd.Args[0]))]
		if ok {
			f(conn, cmd)
			return
		}

		conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
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
	if len(cmd.Args) != argsLen[strings.ToUpper(string(cmd.Args[0]))] {
		return errors.New("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
	}
	return nil
}

func (r *RedisServer) validateKey(key []byte) error {
	if len(key) == 0 {
		return errors.New("ERR wrong number of arguments for '" + string(key) + "' command")
	}
	return nil
}

func (r *RedisServer) validateValue(value []byte) error {
	if len(value) == 0 {
		return errors.New("ERR wrong number of arguments for '" + string(value) + "' command")
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
	v, err := r.store.Get(context.Background(), cmd.Args[1])
	if err != nil {
		conn.WriteNull()
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

func (r *RedisServer) Exists(conn redcon.Conn, cmd redcon.Command) {
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
