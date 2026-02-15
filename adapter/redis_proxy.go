package adapter

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
)

func (r *RedisServer) proxyExists(key []byte) (int, error) {
	leader := r.coordinator.RaftLeaderForKey(key)
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
