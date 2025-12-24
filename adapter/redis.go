package adapter

import (
	"bytes"
	"context"
	"math"
	"net"
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
	"GET":    2,
	"SET":    3,
	"DEL":    2,
	"EXISTS": 2,
	"PING":   1,
	"KEYS":   2,
}

type RedisServer struct {
	listen          net.Listener
	store           store.ScanStore
	coordinator     kv.Coordinator
	redisTranscoder *redisTranscoder

	route map[string]func(conn redcon.Conn, cmd redcon.Command)
}

func NewRedisServer(listen net.Listener, store store.ScanStore, coordinate *kv.Coordinate) *RedisServer {
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
		"EXISTS": r.exists,
		"KEYS":   r.keys,
	}

	return r
}
func (r *RedisServer) Run() error {
	err := redcon.Serve(r.listen,
		func(conn redcon.Conn, cmd redcon.Command) {
			f, ok := r.route[strings.ToUpper(string(cmd.Args[0]))]
			if !ok {
				conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
				return
			}

			if err := r.validateCmd(cmd); err != nil {
				conn.WriteError(err.Error())
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
	if len(cmd.Args) != argsLen[strings.ToUpper(string(cmd.Args[0]))] {
		//nolint:wrapcheck
		return errors.Newf("ERR wrong number of arguments for '%s' command", string(cmd.Args[0]))
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
