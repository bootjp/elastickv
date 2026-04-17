package adapter

import (
	"context"
	"errors"
	"testing"
	"time"

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

	// Set a key with a very short TTL.
	require.NoError(t, rdb.Do(ctx, "SET", "expiry:short", "v", "PX", "200").Err())

	got, err := rdb.Get(ctx, "expiry:short").Result()
	require.NoError(t, err)
	require.Equal(t, "v", got, "key must be visible before expiry")

	require.Eventually(t, func() bool {
		_, e := rdb.Get(ctx, "expiry:short").Result()
		return errors.Is(e, redis.Nil)
	}, time.Second, 25*time.Millisecond, "key must be gone after expiry")
}
