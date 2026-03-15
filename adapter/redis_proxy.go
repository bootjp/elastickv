package adapter

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/redis/go-redis/v9"
)

func (r *RedisServer) proxyDBSize() (int, error) {
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

	res, err := cli.DBSize(context.Background()).Result()
	return int(res), errors.WithStack(err)
}

func (r *RedisServer) proxyFlushDatabase(all bool) error {
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

	var err error
	if all {
		err = cli.FlushAll(context.Background()).Err()
	} else {
		err = cli.FlushDB(context.Background()).Err()
	}
	return errors.WithStack(err)
}
