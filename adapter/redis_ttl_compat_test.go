package adapter

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// ────────────────────────────────────────────────────────────────
// EXPIRE / PEXPIRE — TTL is read immediately from the buffer
// ────────────────────────────────────────────────────────────────

// EXPIRE must set a TTL that is immediately visible via the TTL command,
// even before the background flush has run.
func TestRedis_EXPIRE_ImmediatelyVisibleViaTTL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "expire:key", "value", 0).Err())
	require.NoError(t, rdb.Expire(ctx, "expire:key", 100*time.Second).Err())

	ttl, err := rdb.TTL(ctx, "expire:key").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0), "TTL must be > 0 immediately after EXPIRE")
	require.LessOrEqual(t, ttl, 100*time.Second)
}

// PEXPIRE must set a millisecond-precision TTL immediately visible via PTTL.
func TestRedis_PEXPIRE_ImmediatelyVisibleViaPTTL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "pexpire:key", "v", 0).Err())
	require.NoError(t, rdb.Do(ctx, "PEXPIRE", "pexpire:key", "50000").Err())

	pttl, err := rdb.PTTL(ctx, "pexpire:key").Result()
	require.NoError(t, err)
	require.Greater(t, pttl, time.Duration(0))
	require.LessOrEqual(t, pttl, 50*time.Second)
}

// SET EX stores a TTL that must be visible immediately.
func TestRedis_SET_EX_ImmediatelyVisibleViaTTL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Do(ctx, "SET", "setex:key", "v", "EX", "90").Err())

	ttl, err := rdb.TTL(ctx, "setex:key").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, 90*time.Second)
}

// ────────────────────────────────────────────────────────────────
// INCR — must preserve any existing TTL (Redis semantics)
// ────────────────────────────────────────────────────────────────

func TestRedis_INCR_PreservesTTL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// Store an integer with an expiry.
	require.NoError(t, rdb.Do(ctx, "SET", "incr:key", "10", "EX", "100").Err())
	ttlBefore, err := rdb.TTL(ctx, "incr:key").Result()
	require.NoError(t, err)
	require.Greater(t, ttlBefore, time.Duration(0), "key must have a TTL before INCR")

	val, err := rdb.Do(ctx, "INCR", "incr:key").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(11), val)

	// INCR must preserve the TTL — Redis documents INCR as not modifying the TTL.
	ttlAfter, err := rdb.TTL(ctx, "incr:key").Result()
	require.NoError(t, err)
	require.Greater(t, ttlAfter, time.Duration(0),
		"INCR must preserve the existing TTL")
	require.LessOrEqual(t, ttlAfter, ttlBefore,
		"INCR must not extend/reset the existing TTL")
}

// INCR on a key without TTL must keep TTL as -1.
func TestRedis_INCR_NoTTL_StaysNegativeOne(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "incr:notl", "5", 0).Err())
	_, err := rdb.Do(ctx, "INCR", "incr:notl").Int64()
	require.NoError(t, err)

	ttl, err := rdb.TTL(ctx, "incr:notl").Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
}

// ────────────────────────────────────────────────────────────────
// MULTI/EXEC with EXPIRE
// ────────────────────────────────────────────────────────────────

// EXPIRE inside MULTI/EXEC must set a TTL that is visible after EXEC.
func TestRedis_MultiExec_ExpireSetsVisibleTTL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Set(ctx, "multi:key", "v", 0).Err())

	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "EXPIRE", "multi:key", "60").Val())
	_, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)

	ttl, err := rdb.TTL(ctx, "multi:key").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, 60*time.Second)
}

// SET with EX inside MULTI/EXEC must store both value and TTL.
func TestRedis_MultiExec_SetEXStoresTTL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "multi:setex", "hello", "EX", "30").Val())
	_, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)

	val, err := rdb.Get(ctx, "multi:setex").Result()
	require.NoError(t, err)
	require.Equal(t, "hello", val)

	ttl, err := rdb.TTL(ctx, "multi:setex").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
}

func TestRedis_MultiExec_SetThenExpireStoresReplacementTTL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "multi:set-expire", "hello").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "EXPIRE", "multi:set-expire", "30").Val())
	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	require.Equal(t, []any{"OK", int64(1)}, execRes)

	val, err := rdb.Get(ctx, "multi:set-expire").Result()
	require.NoError(t, err)
	require.Equal(t, "hello", val)

	ttl, err := rdb.TTL(ctx, "multi:set-expire").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, 30*time.Second)
}

func TestRedis_MultiExec_ExpireNXUsesStagedSetTTL(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Do(ctx, "SET", "multi:nx-keeps-setex", "old", "EX", "100").Err())
	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "multi:nx-keeps-setex", "new", "EX", "10").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "EXPIRE", "multi:nx-keeps-setex", "20", "NX").Val())
	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	require.Equal(t, []any{"OK", int64(0)}, execRes)
	ttl, err := rdb.TTL(ctx, "multi:nx-keeps-setex").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, 10*time.Second)

	require.NoError(t, rdb.Do(ctx, "SET", "multi:nx-after-persistent-set", "old", "EX", "100").Err())
	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "SET", "multi:nx-after-persistent-set", "new").Val())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "EXPIRE", "multi:nx-after-persistent-set", "20", "NX").Val())
	execRes, err = rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	require.Equal(t, []any{"OK", int64(1)}, execRes)
	ttl, err = rdb.TTL(ctx, "multi:nx-after-persistent-set").Result()
	require.NoError(t, err)
	require.Greater(t, ttl, time.Duration(0))
	require.LessOrEqual(t, ttl, 20*time.Second)
}

func TestRedis_MultiExec_HSetRecreatesExpiredHash(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.HSet(ctx, "multi:expired-hash", "stale", "old").Err())
	require.NoError(t, rdb.PExpire(ctx, "multi:expired-hash", time.Millisecond).Err())
	require.Eventually(t, func() bool {
		exists, err := rdb.Exists(ctx, "multi:expired-hash").Result()
		return err == nil && exists == 0
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "HSET", "multi:expired-hash", "fresh", "new").Val())
	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	require.Equal(t, []any{int64(1)}, execRes)

	got, err := rdb.HGetAll(ctx, "multi:expired-hash").Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"fresh": "new"}, got)

	ttl, err := rdb.TTL(ctx, "multi:expired-hash").Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
}

func TestRedis_MultiExec_HSetRecreatesExpiredListAsHash(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.RPush(ctx, "multi:expired-list-hset", "stale").Err())
	require.NoError(t, rdb.PExpire(ctx, "multi:expired-list-hset", time.Millisecond).Err())
	require.Eventually(t, func() bool {
		exists, err := rdb.Exists(ctx, "multi:expired-list-hset").Result()
		return err == nil && exists == 0
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "HSET", "multi:expired-list-hset", "fresh", "new").Val())
	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	require.Equal(t, []any{int64(1)}, execRes)

	got, err := rdb.HGetAll(ctx, "multi:expired-list-hset").Result()
	require.NoError(t, err)
	require.Equal(t, map[string]string{"fresh": "new"}, got)
	require.ErrorContains(t, rdb.LRange(ctx, "multi:expired-list-hset", 0, -1).Err(), "WRONGTYPE")
}

func TestRedis_SAddRejectsExpiredHLLPayload(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Do(ctx, "PFADD", "set:expired-hll", "a").Err())
	require.NoError(t, rdb.PExpire(ctx, "set:expired-hll", time.Millisecond).Err())
	require.Eventually(t, func() bool {
		exists, err := rdb.Exists(ctx, "set:expired-hll").Result()
		return err == nil && exists == 0
	}, time.Second, 10*time.Millisecond)

	err := rdb.SAdd(ctx, "set:expired-hll", "member").Err()
	require.ErrorContains(t, err, "WRONGTYPE")
	count, err := rdb.Do(ctx, "PFCOUNT", "set:expired-hll").Int64()
	require.NoError(t, err)
	require.Equal(t, int64(1), count)
}

func TestRedis_MultiExec_IncrRecreatesExpiredStringWithoutTTLIndex(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	require.NoError(t, rdb.Do(ctx, "SET", "multi:expired-incr", "0", "PX", "1").Err())
	require.Eventually(t, func() bool {
		exists, err := rdb.Exists(ctx, "multi:expired-incr").Result()
		return err == nil && exists == 0
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, rdb.Do(ctx, "MULTI").Err())
	require.Equal(t, "QUEUED", rdb.Do(ctx, "INCR", "multi:expired-incr").Val())
	execRes, err := rdb.Do(ctx, "EXEC").Result()
	require.NoError(t, err)
	require.Equal(t, []any{int64(1)}, execRes)
	require.Equal(t, "1", rdb.Get(ctx, "multi:expired-incr").Val())

	ttl, err := rdb.TTL(ctx, "multi:expired-incr").Result()
	require.NoError(t, err)
	require.Equal(t, time.Duration(-1), ttl)
	require.Eventually(t, func() bool {
		readTS := nodes[0].redisServer.readTS()
		_, err := nodes[0].redisServer.store.GetAt(ctx, redisTTLKey([]byte("multi:expired-incr")), readTS)
		return errors.Is(err, store.ErrKeyNotFound)
	}, time.Second, 10*time.Millisecond)
}

// ────────────────────────────────────────────────────────────────
// Key expiration — expired keys must become invisible
// ────────────────────────────────────────────────────────────────

func TestRedis_ExpiredKey_BecomesInvisible(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	// initialTTL must comfortably outlast the SET → first-GET
	// round-trip. SET on a 3-node Raft cluster must reach quorum
	// before returning OK, which can take 100–250ms under -race on
	// CI runners. The original 200ms TTL raced that window: by the
	// time the "visible before expiry" GET fired, the wall-clock
	// TTL had already burned and the GET returned redis.Nil. 2s
	// is comfortably past the worst observed SET-ack latency.
	const initialTTL = 2 * time.Second
	require.NoError(t, rdb.Do(ctx, "SET", "expiry:short", "v", "PX",
		initialTTL.Milliseconds()).Err())

	got, err := rdb.Get(ctx, "expiry:short").Result()
	require.NoError(t, err)
	require.Equal(t, "v", got, "key must be visible before expiry")

	// Deadline derives from initialTTL so a future TTL adjustment
	// keeps the assertion window valid: ttl + 3s gives headroom
	// for the 25ms poll cadence plus Raft-replicated DEL latency
	// on expiry.
	require.Eventually(t, func() bool {
		_, e := rdb.Get(ctx, "expiry:short").Result()
		return errors.Is(e, redis.Nil)
	}, initialTTL+3*time.Second, 25*time.Millisecond, "key must be gone after expiry")
}
