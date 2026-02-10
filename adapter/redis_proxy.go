package adapter

import (
	"context"
	"fmt"
	"strings"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
	"github.com/tidwall/redcon"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (r *RedisServer) proxyExists(key []byte) (int, error) {
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

	res, err := cli.Exists(context.Background(), string(key)).Result()
	return int(res), errors.WithStack(err)
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
