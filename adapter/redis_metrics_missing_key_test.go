package adapter

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/bootjp/elastickv/monitoring"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// TestRedisMetrics_MissingKeyNotCountedAsError pins the observability contract
// described in PR #594 and its follow-up: commands whose Redis protocol
// semantics report a missing key via a null/0/"none" reply (GET, HGET,
// LPOP/RPOP, LLEN, LINDEX, HLEN, HEXISTS, HGETALL, HMGET, SCARD, SMEMBERS,
// SISMEMBER, ZCARD, ZSCORE, ZRANGE, XLEN, XRANGE, EXISTS, TYPE, TTL, PTTL)
// must be counted with outcome="success" on the
// elastickv_redis_requests_total counter and must NOT increment
// elastickv_redis_errors_total.
//
// Conversely, commands whose Redis protocol semantics signal a missing key
// with a real ERR reply (RENAME, LSET) must be counted with outcome="error"
// and must increment the errors counter.
//
// All three nodes in the test cluster receive the same observer so that
// proxyToLeader-induced routing to a different node is captured too.
func TestRedisMetrics_MissingKeyNotCountedAsError(t *testing.T) {
	t.Parallel()

	registry := monitoring.NewRegistry("n1", "127.0.0.1:0")
	observer := registry.RedisObserver()

	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)
	for _, n := range nodes {
		n.redisServer.requestObserver = observer
	}

	ctx := context.Background()
	rdb := redis.NewClient(&redis.Options{Addr: nodes[0].redisAddress})
	defer func() { _ = rdb.Close() }()

	t.Run("missing-key commands report outcome=success", func(t *testing.T) {
		// Warm the client so that any connection-setup traffic
		// go-redis issues (HELLO, CLIENT ID, CLIENT SETINFO, ...)
		// lands BEFORE the baseline snapshot below. elastickv answers
		// some of those with an ERR reply (HELLO is unimplemented,
		// CLIENT ID is unsupported), and those errors are unrelated
		// to the missing-key semantics being exercised here.
		require.NoError(t, rdb.Ping(ctx).Err())

		errorsBefore := countErrorMetrics(t, registry)

		// GET – returns nil
		_, err := rdb.Get(ctx, "mk:get").Result()
		require.ErrorIs(t, err, redis.Nil)

		// HGET – returns nil
		_, err = rdb.HGet(ctx, "mk:hget", "f").Result()
		require.ErrorIs(t, err, redis.Nil)

		// HMGET – returns array of nils (success)
		out, err := rdb.HMGet(ctx, "mk:hmget", "f1", "f2").Result()
		require.NoError(t, err)
		require.Equal(t, []interface{}{nil, nil}, out)

		// HLEN – returns 0
		n, err := rdb.HLen(ctx, "mk:hlen").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		// HEXISTS – returns 0
		ok, err := rdb.HExists(ctx, "mk:hexists", "f").Result()
		require.NoError(t, err)
		require.False(t, ok)

		// HGETALL – returns empty map
		m, err := rdb.HGetAll(ctx, "mk:hgetall").Result()
		require.NoError(t, err)
		require.Empty(t, m)

		// LLEN – returns 0
		n, err = rdb.LLen(ctx, "mk:llen").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		// LINDEX – returns nil
		_, err = rdb.LIndex(ctx, "mk:lindex", 0).Result()
		require.ErrorIs(t, err, redis.Nil)

		// LRANGE – returns empty array (success)
		vs, err := rdb.LRange(ctx, "mk:lrange", 0, -1).Result()
		require.NoError(t, err)
		require.Empty(t, vs)

		// LPOP – returns nil
		_, err = rdb.LPop(ctx, "mk:lpop").Result()
		require.ErrorIs(t, err, redis.Nil)

		// RPOP – returns nil
		_, err = rdb.RPop(ctx, "mk:rpop").Result()
		require.ErrorIs(t, err, redis.Nil)

		// SCARD – returns 0
		n, err = rdb.SCard(ctx, "mk:scard").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		// SMEMBERS – returns empty array
		mems, err := rdb.SMembers(ctx, "mk:smembers").Result()
		require.NoError(t, err)
		require.Empty(t, mems)

		// SISMEMBER – returns 0
		ok, err = rdb.SIsMember(ctx, "mk:sismember", "x").Result()
		require.NoError(t, err)
		require.False(t, ok)

		// ZCARD – returns 0
		n, err = rdb.ZCard(ctx, "mk:zcard").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		// ZSCORE – returns nil
		_, err = rdb.ZScore(ctx, "mk:zscore", "m").Result()
		require.ErrorIs(t, err, redis.Nil)

		// ZRANGE – returns empty array
		zs, err := rdb.ZRange(ctx, "mk:zrange", 0, -1).Result()
		require.NoError(t, err)
		require.Empty(t, zs)

		// XLEN – returns 0
		n, err = rdb.XLen(ctx, "mk:xlen").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		// XRANGE – returns empty array
		xs, err := rdb.XRange(ctx, "mk:xrange", "-", "+").Result()
		require.NoError(t, err)
		require.Empty(t, xs)

		// EXISTS – returns 0
		n, err = rdb.Exists(ctx, "mk:exists").Result()
		require.NoError(t, err)
		require.Equal(t, int64(0), n)

		// TYPE – returns "none"
		ts, err := rdb.Type(ctx, "mk:type").Result()
		require.NoError(t, err)
		require.Equal(t, "none", ts)

		// TTL – returns -2 (raw, key missing). go-redis surfaces this as
		// time.Duration(-2) without scaling, so compare against the
		// raw value rather than -2*time.Second.
		d, err := rdb.TTL(ctx, "mk:ttl").Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-2), d)

		// PTTL – returns -2 (raw, key missing).
		d, err = rdb.PTTL(ctx, "mk:pttl").Result()
		require.NoError(t, err)
		require.Equal(t, time.Duration(-2), d)

		// None of the missing-key commands above must count as errors.
		// Compare against the pre-traffic baseline so unrelated
		// connection-setup errors (CLIENT ID / HELLO / ...) don't
		// bleed into this assertion.
		require.Equal(t, errorsBefore, countErrorMetrics(t, registry),
			"missing-key commands must not increment elastickv_redis_errors_total; details=%s",
			dumpErrorMetrics(t, registry))

		// Sanity check: the success counter for GET went up by the
		// expected amount (1). If this assertion fires, the handler
		// is silently not producing the success-outcome sample even
		// though it avoided WriteError — a regression in the metrics
		// plumbing itself.
		require.GreaterOrEqual(t, requestCountForOutcome(t, registry, "GET", "success"), 1.0,
			"GET on missing key must count towards outcome=success")
	})

	t.Run("missing-key RENAME reports outcome=error", func(t *testing.T) {
		errorsBefore := countErrorMetrics(t, registry)

		_, err := rdb.Rename(ctx, "mk:rename:missing", "mk:rename:dst").Result()
		require.Error(t, err, "RENAME on a missing key must return an error")
		require.Contains(t, err.Error(), "no such key")

		require.Equal(t, errorsBefore+1, countErrorMetrics(t, registry),
			"RENAME on missing key must increment elastickv_redis_errors_total")
	})

	t.Run("missing-key LSET reports outcome=error", func(t *testing.T) {
		errorsBefore := countErrorMetrics(t, registry)

		_, err := rdb.LSet(ctx, "mk:lset:missing", 0, "v").Result()
		require.Error(t, err, "LSET on a missing key must return an error")
		require.Contains(t, err.Error(), "no such key")

		require.Equal(t, errorsBefore+1, countErrorMetrics(t, registry),
			"LSET on missing key must increment elastickv_redis_errors_total")
	})
}

// dumpErrorMetrics renders the per-command error-counter breakdown so that
// a failing "no errors" assertion can point at exactly which command
// regressed. Only non-zero samples are emitted, keeping the diagnostic
// compact when a single command fails among many.
func dumpErrorMetrics(t *testing.T, registry *monitoring.Registry) string {
	t.Helper()
	mfs, err := registry.Gatherer().Gather()
	require.NoError(t, err)
	out := ""
	for _, mf := range mfs {
		if mf.GetName() != "elastickv_redis_errors_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if m.GetCounter().GetValue() == 0 {
				continue
			}
			labels := ""
			for _, lp := range m.GetLabel() {
				labels += lp.GetName() + "=" + lp.GetValue() + ","
			}
			out += labels + "value=" + strconv.FormatFloat(m.GetCounter().GetValue(), 'f', -1, 64) + "\n"
		}
	}
	return out
}

// requestCountForOutcome returns the sum of
// elastickv_redis_requests_total samples whose command label matches the
// argument and whose outcome label matches the argument. Used to sanity-check
// that handlers we believe emit success still do, so a regression that
// silently drops the success sample doesn't pass the "no-errors" assertion.
func requestCountForOutcome(t *testing.T, registry *monitoring.Registry, command, outcome string) float64 {
	t.Helper()
	mfs, err := registry.Gatherer().Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != "elastickv_redis_requests_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var cmd, out string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "command":
					cmd = lp.GetValue()
				case "outcome":
					out = lp.GetValue()
				}
			}
			if cmd == command && out == outcome {
				total += m.GetCounter().GetValue()
			}
		}
	}
	return total
}

// countErrorMetrics returns the sum of all samples for
// elastickv_redis_errors_total. It lets a subtest assert that previously-
// observed errors are the only ones that count.
func countErrorMetrics(t *testing.T, registry *monitoring.Registry) float64 {
	t.Helper()
	got, err := testutil.GatherAndCount(registry.Gatherer(), "elastickv_redis_errors_total")
	require.NoError(t, err)
	if got == 0 {
		return 0
	}
	// GatherAndCount returns the number of distinct label combinations.
	// We want the sum of the counter values; iterate manually.
	mfs, err := registry.Gatherer().Gather()
	require.NoError(t, err)
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != "elastickv_redis_errors_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	return total
}
