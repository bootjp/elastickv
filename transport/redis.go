package transport

import (
	"context"
	"log"
	"strings"

	"github.com/bootjp/elastickv/kv"
	"github.com/tidwall/redcon"
)

type RedisServer struct {
	store           kv.Store
	coordinator     kv.Coordinator
	redisTranscoder *redisTranscoder

	route map[string]func(conn redcon.Conn, cmd redcon.Command)
}

const (
	getCmdArgsLen = 2
	setCmdArgsLen = 3
	delCmdArgsLen = 2
)

func NewRedisServer(store kv.Store, coordinate *kv.Coordinate) *RedisServer {
	r := &RedisServer{
		store:           store,
		coordinator:     coordinate,
		redisTranscoder: newRedisTranscoder(),
	}

	r.route = map[string]func(conn redcon.Conn, cmd redcon.Command){
		"PING": r.ping,
		"SET":  r.set,
		"GET":  r.get,
		"DEL":  r.del,
	}

	return r
}

func (r *RedisServer) Run(addr string) {
	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
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
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

func (r *RedisServer) ping(conn redcon.Conn, _ redcon.Command) {
	conn.WriteString("PONG")
}

func (r *RedisServer) set(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != setCmdArgsLen {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
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
	if len(cmd.Args) != getCmdArgsLen {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	v, err := r.store.Get(context.Background(), cmd.Args[1])
	if err != nil {
		conn.WriteNull()
		return
	}

	conn.WriteBulk(v)
}

func (r *RedisServer) del(conn redcon.Conn, cmd redcon.Command) {
	if len(cmd.Args) != delCmdArgsLen {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
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
