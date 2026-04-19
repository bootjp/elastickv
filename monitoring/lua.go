package monitoring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// LuaScriptReport summarises one Lua script invocation (EVAL / EVALSHA),
// covering all retryRedisWrite attempts.
type LuaScriptReport struct {
	// LuaExecDuration is the cumulative time spent inside state.PCall across
	// all retry attempts. This includes RedisCallDuration.
	LuaExecDuration time.Duration
	// RedisCallDuration is the cumulative time spent inside redis.call() /
	// redis.pcall() handlers (scriptCtx.exec) — Raft reads and in-memory
	// state updates. Pure VM time = LuaExecDuration - RedisCallDuration.
	RedisCallDuration time.Duration
	// RedisCallCount is the total number of redis.call()/pcall() invocations
	// across all retry attempts. High counts explain high RedisCallDuration.
	RedisCallCount int
	// CommitDuration is the cumulative time spent in scriptCtx.commit()
	// (coordinator.Dispatch → Raft consensus + storage write).
	CommitDuration time.Duration
	// ConflictRetries is the number of extra attempts caused by write
	// conflicts or locked transactions (0 = succeeded on the first try).
	ConflictRetries int
	// IsError is true when the final outcome was an error.
	IsError bool
}

// LuaScriptObserver is implemented by anything that wants to record Lua
// script execution metrics.
type LuaScriptObserver interface {
	ObserveLuaScript(report LuaScriptReport)
}

// LuaMetrics holds Prometheus metrics that break down where time goes inside
// a Lua script execution so that Lua VM slowness, Raft latency, and write
// conflict retries can be distinguished in dashboards.
type LuaMetrics struct {
	luaExecDuration   *prometheus.HistogramVec
	redisCallDuration *prometheus.HistogramVec
	redisCallCount    *prometheus.HistogramVec
	commitDuration    *prometheus.HistogramVec
	conflictRetries   prometheus.Histogram
}

var luaDurationBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10}
var luaRetryBuckets = []float64{0, 1, 2, 3, 5, 10, 20, 50}
var luaCallCountBuckets = []float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000}

func newLuaMetrics(registerer prometheus.Registerer) *LuaMetrics {
	m := &LuaMetrics{
		luaExecDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_lua_exec_duration_seconds",
				Help:    "Cumulative time inside state.PCall per invocation (includes redis.call() time). Use with redis_call_duration to isolate pure VM time.",
				Buckets: luaDurationBuckets,
			},
			[]string{"outcome"},
		),
		redisCallDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_lua_redis_call_duration_seconds",
				Help:    "Cumulative time spent in redis.call()/pcall() handlers (Raft reads + in-memory state) per invocation. Pure VM time = exec_duration - redis_call_duration.",
				Buckets: luaDurationBuckets,
			},
			[]string{"outcome"},
		),
		redisCallCount: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_lua_redis_call_count",
				Help:    "Number of redis.call()/pcall() invocations per Lua script execution, summed across retries. High counts combined with high redis_call_duration indicate many individual Raft reads.",
				Buckets: luaCallCountBuckets,
			},
			[]string{"outcome"},
		),
		commitDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_lua_commit_duration_seconds",
				Help:    "Cumulative time spent in coordinator.Dispatch (Raft + storage write) per script invocation, summed across retries.",
				Buckets: luaDurationBuckets,
			},
			[]string{"outcome"},
		),
		conflictRetries: prometheus.NewHistogram(
			prometheus.HistogramOpts{
				Name:    "elastickv_lua_conflict_retries",
				Help:    "Number of write-conflict or lock retries per Lua script invocation (0 = first-try success).",
				Buckets: luaRetryBuckets,
			},
		),
	}

	registerer.MustRegister(
		m.luaExecDuration,
		m.redisCallDuration,
		m.redisCallCount,
		m.commitDuration,
		m.conflictRetries,
	)

	return m
}

// ObserveLuaScript records a completed script invocation.
func (m *LuaMetrics) ObserveLuaScript(report LuaScriptReport) {
	if m == nil {
		return
	}
	outcome := redisOutcomeSuccess
	if report.IsError {
		outcome = redisOutcomeError
	}
	m.luaExecDuration.WithLabelValues(outcome).Observe(report.LuaExecDuration.Seconds())
	m.redisCallDuration.WithLabelValues(outcome).Observe(report.RedisCallDuration.Seconds())
	m.redisCallCount.WithLabelValues(outcome).Observe(float64(report.RedisCallCount))
	m.commitDuration.WithLabelValues(outcome).Observe(report.CommitDuration.Seconds())
	m.conflictRetries.Observe(float64(report.ConflictRetries))
}
