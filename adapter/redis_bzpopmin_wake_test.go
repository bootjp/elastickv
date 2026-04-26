package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestRedis_BZPopMinWakesOnZAdd verifies the event-driven wake path:
// an in-process ZADD on the leader's redis adapter must wake a
// BZPOPMIN waiter on the same node so the reader returns the new
// entry before its BLOCK deadline. The wake comes through
// keyWaiterRegistry's signal channel — the prior 10 ms time.Sleep
// busy-poll loop would have exhibited the same end-to-end behaviour,
// so this is an end-to-end sanity test rather than a wall-clock
// latency gate (the latency gate is impractical under -race +
// parallel CI load, where tryBZPopMin's Pebble seek alone can
// exceed any tight budget).
//
// Both client connections target the same node so they share the
// same keyWaiterRegistry — the signal path is intentionally
// in-process only (Lua and follower-side applies fall through to
// the fallback timer; see bzpopminWaitLoop).
func TestRedis_BZPopMinWakesOnZAdd(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdbReader := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdbReader.Close() }()
	rdbWriter := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdbWriter.Close() }()
	ctx := context.Background()

	type popResult struct {
		zwk *redis.ZWithKey
		err error
	}
	resultCh := make(chan popResult, 1)
	go func() {
		zwk, err := rdbReader.BZPopMin(ctx, 5*time.Second, "zset-wake").Result()
		resultCh <- popResult{zwk: zwk, err: err}
	}()

	// Give the reader a moment to enter bzpopminWaitLoop and register
	// a waiter on keyWaiterRegistry before ZADD. If ZADD landed first
	// the entry would already be visible by the time the reader runs
	// tryBZPopMin, so the registration race is benign — but waiting
	// also gates out a different source of flake where the goroutine
	// has not yet dialed redis.
	time.Sleep(50 * time.Millisecond)

	_, err := rdbWriter.ZAdd(ctx, "zset-wake",
		redis.Z{Score: 1, Member: "first"},
	).Result()
	require.NoError(t, err)

	select {
	case res := <-resultCh:
		require.NoError(t, res.err)
		require.NotNil(t, res.zwk)
		require.Equal(t, "zset-wake", res.zwk.Key)
		require.Equal(t, "first", res.zwk.Member)
		require.InDelta(t, 1.0, res.zwk.Score, 1e-9)
	case <-time.After(6 * time.Second):
		t.Fatal("BZPOPMIN did not return after ZADD signal")
	}
}

// TestRedis_BZPopMinWakesOnZIncrBy verifies the same wake path but
// driven by ZINCRBY rather than ZADD. ZINCRBY on a missing member
// is functionally equivalent to ZADD for the BZPOPMIN consumer:
// the score appears under a new member, which makes the zset
// non-empty and BZPOPMIN should return it.
func TestRedis_BZPopMinWakesOnZIncrBy(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdbReader := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdbReader.Close() }()
	rdbWriter := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdbWriter.Close() }()
	ctx := context.Background()

	type popResult struct {
		zwk *redis.ZWithKey
		err error
	}
	resultCh := make(chan popResult, 1)
	go func() {
		zwk, err := rdbReader.BZPopMin(ctx, 5*time.Second, "zset-incr-wake").Result()
		resultCh <- popResult{zwk: zwk, err: err}
	}()

	time.Sleep(50 * time.Millisecond)

	_, err := rdbWriter.ZIncrBy(ctx, "zset-incr-wake", 7.5, "alpha").Result()
	require.NoError(t, err)

	select {
	case res := <-resultCh:
		require.NoError(t, res.err)
		require.NotNil(t, res.zwk)
		require.Equal(t, "zset-incr-wake", res.zwk.Key)
		require.Equal(t, "alpha", res.zwk.Member)
		require.InDelta(t, 7.5, res.zwk.Score, 1e-9)
	case <-time.After(6 * time.Second):
		t.Fatal("BZPOPMIN did not return after ZINCRBY signal")
	}
}

// TestRedis_BZPopMinTimesOutOnEmptyKey locks down the BLOCK-timeout
// contract: when no ZADD arrives within the BLOCK window, BZPOPMIN
// returns redis.Nil rather than a protocol error. This guards a
// regression in the wait-loop refactor where the new
// waitForBlockedCommandUpdate timer or context-cancel branch could otherwise
// leak a -ERR reply.
// TestRedis_BZPopMinRejectsInitialWrongType locks down the
// first-iteration full-check invariant: when BZPOPMIN is issued
// against a key that already holds a wrongType encoding (e.g. a
// string), the very first tryBZPopMin must surface WRONGTYPE. The
// signal-driven fast path must never run before the initial full
// check has confirmed the type.
func TestRedis_BZPopMinRejectsInitialWrongType(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	// Pre-write a string at the BZPOPMIN target key.
	require.NoError(t, rdb.Set(ctx, "bzpop-wrongtype", "I am a string", 0).Err())

	// BZPOPMIN with a 1 s budget — full check on the first iteration
	// must catch the wrongType immediately, well under the BLOCK
	// timeout.
	zwk, err := rdb.BZPopMin(ctx, time.Second, "bzpop-wrongtype").Result()
	require.Error(t, err, "BZPOPMIN on a string key must return WRONGTYPE")
	require.Contains(t, err.Error(), "WRONGTYPE")
	require.Nil(t, zwk)
}

// TestRedis_BZPopMinDetectsMidBlockWrongType locks down the
// fallback-timer-driven full check: when BZPOPMIN is blocked on a
// non-existent key and a wrongType encoding (e.g. SET) is written
// to that key during the BLOCK window, the next fallback-timer
// wake must run the full check and surface WRONGTYPE within
// ~redisBlockWaitFallback (100 ms). A pure signal-driven path
// would miss this because SET / HSET / etc. do not fire
// zsetWaiters.Signal.
//
// The 5 s BLOCK budget gives plenty of slack for CI scheduler
// jitter; the assertion is "WRONGTYPE before BLOCK timeout", not a
// tight latency gate.
func TestRedis_BZPopMinDetectsMidBlockWrongType(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdbReader := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdbReader.Close() }()
	rdbWriter := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdbWriter.Close() }()
	ctx := context.Background()

	type popResult struct {
		zwk *redis.ZWithKey
		err error
	}
	resultCh := make(chan popResult, 1)
	go func() {
		zwk, err := rdbReader.BZPopMin(ctx, 5*time.Second, "bzpop-mid-wrongtype").Result()
		resultCh <- popResult{zwk: zwk, err: err}
	}()

	// Let the reader enter the wait loop and exhaust its first
	// (full) iteration on a missing key. Then SET a string at the
	// same key. The next fallback-timer wake (~100 ms after the
	// previous one) must run the full check and surface WRONGTYPE.
	time.Sleep(50 * time.Millisecond)
	require.NoError(t, rdbWriter.Set(ctx, "bzpop-mid-wrongtype", "I am a string", 0).Err())

	select {
	case res := <-resultCh:
		require.Error(t, res.err, "BZPOPMIN must return WRONGTYPE after mid-block SET, got zwk=%v", res.zwk)
		require.Contains(t, res.err.Error(), "WRONGTYPE")
	case <-time.After(6 * time.Second):
		t.Fatal("BZPOPMIN did not return WRONGTYPE within the BLOCK window after a mid-block SET")
	}
}

func TestRedis_BZPopMinTimesOutOnEmptyKey(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()
	ctx := context.Background()

	// 250 ms BLOCK on a key that never receives a write. The fallback
	// timer (100 ms) fires twice, then the deadline branch writes
	// nil. Total budget: redisDispatchTimeout caps each iter; we
	// expect ~250 ms total wall time and a redis.Nil reply.
	zwk, err := rdb.BZPopMin(ctx, 250*time.Millisecond, "zset-empty").Result()
	require.ErrorIs(t, err, redis.Nil, "BLOCK timeout must return redis.Nil, got zwk=%v err=%v", zwk, err)
}
