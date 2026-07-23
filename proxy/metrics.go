package proxy

import "github.com/prometheus/client_golang/prometheus"

// ProxyMetrics holds all Prometheus metrics for the dual-write proxy.
type ProxyMetrics struct {
	CommandTotal    *prometheus.CounterVec
	CommandDuration *prometheus.HistogramVec

	PrimaryWriteErrors           prometheus.Counter
	SecondaryWriteErrors         prometheus.Counter
	SecondaryWriteErrorsByReason *prometheus.CounterVec
	PrimaryReadErrors            prometheus.Counter
	ShadowReadErrors             prometheus.Counter
	Divergences                  *prometheus.CounterVec
	MigrationGaps                *prometheus.CounterVec

	ActiveConnections prometheus.Gauge

	AsyncDrops         prometheus.Counter
	AsyncBackpressure  prometheus.Counter
	AsyncDropsByQueue  *prometheus.CounterVec
	AsyncQueueDepth    *prometheus.GaugeVec
	AsyncQueueCapacity *prometheus.GaugeVec
	AsyncQueueDelay    *prometheus.HistogramVec
	AsyncWorkersActive *prometheus.GaugeVec

	BackendPoolLimit        *prometheus.GaugeVec
	BackendPoolConnections  *prometheus.GaugeVec
	BackendPoolPending      *prometheus.GaugeVec
	BackendPoolEvents       *prometheus.GaugeVec
	BackendPoolWaitDuration *prometheus.GaugeVec

	PubSubShadowDivergences *prometheus.CounterVec
	PubSubShadowErrors      prometheus.Counter
}

// NewProxyMetrics creates and registers all proxy metrics.
func NewProxyMetrics(reg prometheus.Registerer) *ProxyMetrics {
	m := &ProxyMetrics{
		CommandTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "command_total",
			Help:      "Total commands processed by the proxy.",
		}, []string{"command", "backend", "status"}),

		CommandDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "proxy",
			Name:      "command_duration_seconds",
			Help:      "Latency of commands forwarded to backends.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"command", "backend"}),

		PrimaryWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "primary_write_errors_total",
			Help:      "Total write errors from the primary backend.",
		}),
		SecondaryWriteErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "secondary_write_errors_total",
			Help:      "Total write errors from the secondary backend.",
		}),
		SecondaryWriteErrorsByReason: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "secondary_write_errors_by_reason_total",
			Help:      "secondary write failures broken out by redis command and error classification (busy / write_conflict / retry_limit / not_leader / deadline_exceeded / txn_already_finalized / txn_locked / other)",
		}, []string{"cmd", "reason"}),
		PrimaryReadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "primary_read_errors_total",
			Help:      "Total read errors from the primary backend.",
		}),
		ShadowReadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "shadow_read_errors_total",
			Help:      "Total errors from shadow reads.",
		}),
		Divergences: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "divergences_total",
			Help:      "Total data mismatches detected by shadow reads.",
		}, []string{"command", "kind"}),
		MigrationGaps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "migration_gap_total",
			Help:      "Expected divergences due to missing data on secondary (pre-migration).",
		}, []string{"command"}),

		AsyncDrops: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "async_drops_total",
			Help:      "Total best-effort async operations dropped due to semaphore backpressure.",
		}),
		AsyncBackpressure: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "async_backpressure_total",
			Help:      "Total strict secondary writes that waited for an async semaphore slot instead of being dropped.",
		}),
		AsyncDropsByQueue: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "async_drops_by_queue_total",
			Help:      "Async operations dropped by queue and reason.",
		}, []string{"queue", "reason"}),
		AsyncQueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "async_queue_depth",
			Help:      "Current number of async operations waiting for a worker.",
		}, []string{"queue"}),
		AsyncQueueCapacity: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "async_queue_capacity",
			Help:      "Configured async queue capacity.",
		}, []string{"queue"}),
		AsyncQueueDelay: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "proxy",
			Name:      "async_queue_delay_seconds",
			Help:      "Time an async operation waits before acquiring its worker budget.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
		}, []string{"queue"}),
		AsyncWorkersActive: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "async_workers_active",
			Help:      "Current async worker count by queue.",
		}, []string{"queue"}),

		BackendPoolLimit: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "backend_pool_limit",
			Help:      "Configured go-redis connection pool size for the active backend pool.",
		}, []string{"backend"}),
		BackendPoolConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "backend_pool_connections",
			Help:      "Current go-redis connections by backend and state.",
		}, []string{"backend", "state"}),
		BackendPoolPending: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "backend_pool_pending_requests",
			Help:      "Current requests waiting for a go-redis connection.",
		}, []string{"backend"}),
		BackendPoolEvents: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "backend_pool_events",
			Help:      "Current active-pool event counters; values may reset after a leader switch.",
		}, []string{"backend", "event"}),
		BackendPoolWaitDuration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "backend_pool_wait_duration_seconds",
			Help:      "Cumulative connection wait duration for the current active pool; may reset after a leader switch.",
		}, []string{"backend"}),

		ActiveConnections: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "proxy",
			Name:      "active_connections",
			Help:      "Current number of active client connections.",
		}),

		PubSubShadowDivergences: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "pubsub_shadow_divergences_total",
			Help:      "Total pub/sub message mismatches detected by shadow subscribe.",
		}, []string{"kind"}),
		PubSubShadowErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "proxy",
			Name:      "pubsub_shadow_errors_total",
			Help:      "Total errors from shadow pub/sub operations.",
		}),
	}

	reg.MustRegister(
		m.CommandTotal,
		m.CommandDuration,
		m.PrimaryWriteErrors,
		m.SecondaryWriteErrors,
		m.SecondaryWriteErrorsByReason,
		m.PrimaryReadErrors,
		m.ShadowReadErrors,
		m.Divergences,
		m.MigrationGaps,
		m.AsyncDrops,
		m.AsyncBackpressure,
		m.AsyncDropsByQueue,
		m.AsyncQueueDepth,
		m.AsyncQueueCapacity,
		m.AsyncQueueDelay,
		m.AsyncWorkersActive,
		m.BackendPoolLimit,
		m.BackendPoolConnections,
		m.BackendPoolPending,
		m.BackendPoolEvents,
		m.BackendPoolWaitDuration,
		m.ActiveConnections,
		m.PubSubShadowDivergences,
		m.PubSubShadowErrors,
	)

	return m
}

func (m *ProxyMetrics) observeBackendPool(backend Backend) {
	provider, ok := backend.(backendPoolStatsProvider)
	if !ok {
		return
	}
	stats := provider.PoolStats()
	name := backend.Name()
	m.BackendPoolLimit.WithLabelValues(name).Set(float64(stats.Limit))
	m.BackendPoolConnections.WithLabelValues(name, "total").Set(float64(stats.TotalConns))
	m.BackendPoolConnections.WithLabelValues(name, "idle").Set(float64(stats.IdleConns))
	m.BackendPoolPending.WithLabelValues(name).Set(float64(stats.PendingRequests))
	m.BackendPoolEvents.WithLabelValues(name, "hits").Set(float64(stats.Hits))
	m.BackendPoolEvents.WithLabelValues(name, "misses").Set(float64(stats.Misses))
	m.BackendPoolEvents.WithLabelValues(name, "waits").Set(float64(stats.WaitCount))
	m.BackendPoolEvents.WithLabelValues(name, "timeouts").Set(float64(stats.Timeouts))
	m.BackendPoolEvents.WithLabelValues(name, "unusable").Set(float64(stats.Unusable))
	m.BackendPoolEvents.WithLabelValues(name, "stale").Set(float64(stats.StaleConns))
	m.BackendPoolWaitDuration.WithLabelValues(name).Set(stats.WaitDuration.Seconds())
}
