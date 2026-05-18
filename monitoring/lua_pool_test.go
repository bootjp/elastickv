package monitoring

import (
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakeLuaPool is a minimal in-memory LuaPoolSource for tests so the
// monitoring package can verify wiring without pulling in the
// adapter package. The four counter accessors read directly from
// atomic.Uint64 values, mirroring what adapter.luaStatePool does
// under the hood.
type fakeLuaPool struct {
	hits, misses, drops atomic.Uint64
	idle                atomic.Int64
	maxIdle             int
}

func (f *fakeLuaPool) Hits() uint64   { return f.hits.Load() }
func (f *fakeLuaPool) Misses() uint64 { return f.misses.Load() }
func (f *fakeLuaPool) Drops() uint64  { return f.drops.Load() }
func (f *fakeLuaPool) Idle() int      { return int(f.idle.Load()) }
func (f *fakeLuaPool) MaxIdle() int   { return f.maxIdle }

func TestRegisterLuaPool_ExposesEachAccessorAsAMetric(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	src := &fakeLuaPool{maxIdle: 64}
	require.NoError(t, RegisterLuaPool(reg, src))

	// Seed each counter with a distinct value so a wiring swap
	// (e.g. hits → misses) would be caught.
	src.hits.Store(7)
	src.misses.Store(11)
	src.drops.Store(3)
	src.idle.Store(5)

	expect := strings.TrimLeft(`
# HELP elastickv_lua_pool_drops_total Total number of Redis Lua VM pool put() calls rejected because the idle pool was at MaxIdle capacity. Drops > 0 means --redisLuaMaxIdleStates is undersized for the workload. Reset on process restart.
# TYPE elastickv_lua_pool_drops_total counter
elastickv_lua_pool_drops_total 3
# HELP elastickv_lua_pool_hits_total Total number of Redis Lua VM pool get() calls served from the idle pool (reuses). Reset on process restart.
# TYPE elastickv_lua_pool_hits_total counter
elastickv_lua_pool_hits_total 7
# HELP elastickv_lua_pool_idle Current number of *lua.LState instances sitting in the Redis Lua VM pool ready for reuse. Plot against elastickv_lua_pool_max_idle to see saturation.
# TYPE elastickv_lua_pool_idle gauge
elastickv_lua_pool_idle 5
# HELP elastickv_lua_pool_max_idle Configured upper bound on Redis Lua VM pool idle retention (--redisLuaMaxIdleStates).
# TYPE elastickv_lua_pool_max_idle gauge
elastickv_lua_pool_max_idle 64
# HELP elastickv_lua_pool_misses_total Total number of Redis Lua VM pool get() calls that fell through to a fresh allocation (pool empty). Reset on process restart.
# TYPE elastickv_lua_pool_misses_total counter
elastickv_lua_pool_misses_total 11
`, "\n")
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expect),
		"elastickv_lua_pool_hits_total",
		"elastickv_lua_pool_misses_total",
		"elastickv_lua_pool_drops_total",
		"elastickv_lua_pool_idle",
		"elastickv_lua_pool_max_idle"))
}

// TestRegisterLuaPool_ReflectsLiveUpdatesAtScrapeTime locks in the
// "read at scrape time" contract — there is no polling goroutine and
// no snapshot, so a counter that increments AFTER registration must
// surface on the very next gather without any flush/wait step. This
// test would catch a regression that introduced internal caching
// behind RegisterLuaPool.
func TestRegisterLuaPool_ReflectsLiveUpdatesAtScrapeTime(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	src := &fakeLuaPool{maxIdle: 64}
	require.NoError(t, RegisterLuaPool(reg, src))

	// First gather: all zero.
	require.Equal(t, float64(0),
		testutil.ToFloat64(srcCollectorByName(t, reg, "elastickv_lua_pool_drops_total")))

	// Update underlying counter, gather again, expect the new value.
	src.drops.Add(42)
	require.Equal(t, float64(42),
		testutil.ToFloat64(srcCollectorByName(t, reg, "elastickv_lua_pool_drops_total")))
}

// TestRegisterLuaPool_NilSafe ensures that callers passing a nil
// source or registerer get a no-op without panicking. This keeps
// the adapter side free to call RegisterLuaPool unconditionally
// (e.g. in tests that wire a bare &RedisServer{} literal).
func TestRegisterLuaPool_NilSafe(t *testing.T) {
	t.Parallel()
	require.NoError(t, RegisterLuaPool(nil, &fakeLuaPool{}))
	require.NoError(t, RegisterLuaPool(prometheus.NewRegistry(), nil))
	require.NoError(t, RegisterLuaPool(nil, nil))
}

// srcCollectorByName is a tiny helper that fetches a registered
// collector by metric name so testutil.ToFloat64 can read its
// scrape value. It hides the gather + scan boilerplate from each
// test case.
func srcCollectorByName(t *testing.T, reg *prometheus.Registry, name string) prometheus.Collector {
	t.Helper()
	mfs, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() == name {
			// We need a Collector to hand to testutil.ToFloat64;
			// re-wrap via a constant collector that emits the same
			// sample.
			require.Len(t, mf.Metric, 1, "metric %q must have exactly one sample for this test", name)
			m := mf.Metric[0]
			var value float64
			switch {
			case m.Counter != nil:
				value = m.Counter.GetValue()
			case m.Gauge != nil:
				value = m.Gauge.GetValue()
			default:
				t.Fatalf("metric %q is neither counter nor gauge", name)
			}
			return &staticCollector{name: name, value: value}
		}
	}
	t.Fatalf("metric %q not registered", name)
	return nil
}

type staticCollector struct {
	name  string
	value float64
}

func (s *staticCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- prometheus.NewDesc(s.name, "", nil, nil)
}
func (s *staticCollector) Collect(ch chan<- prometheus.Metric) {
	ch <- prometheus.MustNewConstMetric(
		prometheus.NewDesc(s.name, "", nil, nil),
		prometheus.GaugeValue,
		s.value,
	)
}
