package adapter

import (
	"context"

	"github.com/cockroachdb/errors"
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

	cli := r.getOrCreateLeaderClient(leaderAddr)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	res, err := cli.DBSize(ctx).Result()
	return int(res), errors.WithStack(err)
}

// proxyDel routes DEL keys to the correct leader per key (via RaftLeaderForKey)
// so that internal-key discovery always runs on the key's leader, even in
// sharded/multi-raft mode.
func (r *RedisServer) proxyDel(keys [][]byte) (int64, error) {
	// Group keys by leader Redis address.
	byAddr := make(map[string][]string)
	for _, k := range keys {
		leader := r.coordinator.RaftLeaderForKey(k)
		if leader == "" {
			return 0, ErrLeaderNotFound
		}
		addr, ok := r.leaderRedis[leader]
		if !ok || addr == "" {
			return 0, errors.WithStack(errors.Newf("leader redis address unknown for %s", leader))
		}
		byAddr[addr] = append(byAddr[addr], string(k))
	}

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	var total int64
	for addr, strKeys := range byAddr {
		cli := r.getOrCreateLeaderClient(addr)
		res, err := cli.Del(ctx, strKeys...).Result()
		if err != nil {
			return total, errors.WithStack(err)
		}
		total += res
	}
	return total, nil
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

	cli := r.getOrCreateLeaderClient(leaderAddr)

	ctx, cancel := context.WithTimeout(context.Background(), redisDispatchTimeout)
	defer cancel()

	var err error
	if all {
		err = cli.FlushAll(ctx).Err()
	} else {
		err = cli.FlushDB(ctx).Err()
	}
	return errors.WithStack(err)
}
