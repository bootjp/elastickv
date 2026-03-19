package proxy

import "github.com/prometheus/client_golang/prometheus"

// ProxyMetrics holds all Prometheus metrics for the dual-write proxy.
type ProxyMetrics struct {
	CommandTotal    *prometheus.CounterVec
	CommandDuration *prometheus.HistogramVec

	PrimaryWriteErrors   prometheus.Counter
	SecondaryWriteErrors prometheus.Counter
	PrimaryReadErrors    prometheus.Counter
	ShadowReadErrors     prometheus.Counter
	Divergences          *prometheus.CounterVec
	MigrationGaps        *prometheus.CounterVec

	ActiveConnections prometheus.Gauge

	AsyncDrops prometheus.Counter

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
			Help:      "Total async operations dropped due to semaphore backpressure.",
		}),

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
		m.PrimaryReadErrors,
		m.ShadowReadErrors,
		m.Divergences,
		m.MigrationGaps,
		m.AsyncDrops,
		m.ActiveConnections,
		m.PubSubShadowDivergences,
		m.PubSubShadowErrors,
	)

	return m
}
