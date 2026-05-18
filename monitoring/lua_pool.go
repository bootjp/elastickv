package monitoring

import (
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
)

// LuaPoolSource exposes the bounded *lua.LState pool counters that
// the Redis adapter maintains. The interface is a narrow projection
// of adapter.luaStatePool so the monitoring package does not depend
// on the adapter package (avoiding an import cycle): adapter wires
// `*luaStatePool` as a LuaPoolSource at NewRedisServer time.
//
// All four accessors are read at Prometheus scrape time via
// prometheus.NewCounterFunc / NewGaugeFunc, so the lua hot path
// incurs zero observability overhead — the counters live on the
// pool as atomic.Uint64 either way; the metrics collector merely
// reads them when /metrics is hit.
type LuaPoolSource interface {
	// Hits is the total number of get() calls served from the idle
	// pool (counts pool reuses).
	Hits() uint64
	// Misses is the total number of get() calls that fell through
	// to a fresh *lua.LState allocation (counts pool starvation).
	Misses() uint64
	// Drops is the total number of put() calls rejected because
	// the idle pool was full (counts pool saturation — i.e.
	// MaxIdle could be raised to retain more states).
	Drops() uint64
	// Idle is the current number of *lua.LState instances sitting
	// in the pool ready for reuse.
	Idle() int
	// MaxIdle is the configured upper bound on Idle. Exported so
	// dashboards can plot Idle/MaxIdle as a saturation ratio.
	MaxIdle() int
}

// RegisterLuaPool wires `src` into the Prometheus registerer as a
// set of CounterFunc / GaugeFunc metrics:
//
//   - elastickv_lua_pool_hits_total
//   - elastickv_lua_pool_misses_total
//   - elastickv_lua_pool_drops_total
//   - elastickv_lua_pool_idle
//   - elastickv_lua_pool_max_idle
//
// Returns nil if src or registerer is nil; returns the first
// registration error otherwise. The caller (typically
// adapter.RedisServer.RegisterLuaPoolMetrics) should log-and-
// continue on error rather than refusing to start: a missing pool
// metric is observability degradation, not a correctness issue.
//
// The CounterFunc / GaugeFunc closures capture `src` directly, so
// they read live values at scrape time. There is no polling
// goroutine and no snapshot staleness.
func RegisterLuaPool(registerer prometheus.Registerer, src LuaPoolSource) error {
	if registerer == nil || src == nil {
		return nil
	}
	collectors := []prometheus.Collector{
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "elastickv_lua_pool_hits_total",
			Help: "Total number of Redis Lua VM pool get() calls served from the idle pool (reuses). Reset on process restart.",
		}, func() float64 { return float64(src.Hits()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "elastickv_lua_pool_misses_total",
			Help: "Total number of Redis Lua VM pool get() calls that fell through to a fresh allocation (pool empty). Reset on process restart.",
		}, func() float64 { return float64(src.Misses()) }),
		prometheus.NewCounterFunc(prometheus.CounterOpts{
			Name: "elastickv_lua_pool_drops_total",
			Help: "Total number of Redis Lua VM pool put() calls rejected because the idle pool was at MaxIdle capacity. Drops > 0 means --redisLuaMaxIdleStates is undersized for the workload. Reset on process restart.",
		}, func() float64 { return float64(src.Drops()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "elastickv_lua_pool_idle",
			Help: "Current number of *lua.LState instances sitting in the Redis Lua VM pool ready for reuse. Plot against elastickv_lua_pool_max_idle to see saturation.",
		}, func() float64 { return float64(src.Idle()) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "elastickv_lua_pool_max_idle",
			Help: "Configured upper bound on Redis Lua VM pool idle retention (--redisLuaMaxIdleStates).",
		}, func() float64 { return float64(src.MaxIdle()) }),
	}
	for _, c := range collectors {
		if err := registerer.Register(c); err != nil {
			// Idempotent on AlreadyRegistered so callers that retry
			// (test harnesses with shared registries, hypothetical
			// hot-reload paths) don't trip a spurious slog.Warn in
			// main.go. Any other error — invalid metric name,
			// inconsistent labels, registry conflict — surfaces.
			var already prometheus.AlreadyRegisteredError
			if errors.As(err, &already) {
				continue
			}
			return errors.Wrap(err, "register lua pool collector")
		}
	}
	return nil
}
