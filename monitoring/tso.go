package monitoring

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var tsoRequestDurationBuckets = []float64{
	0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005,
	0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5,
}

const (
	tsoDivergenceBucketFactor = 16
	tsoDivergenceBucketCount  = 12
)

type TSOMetrics struct {
	requestDuration  *prometheus.HistogramVec
	shadowComparison *prometheus.CounterVec
	shadowDivergence *prometheus.HistogramVec
	mode             *prometheus.GaugeVec
	reloads          *prometheus.CounterVec
	durableState     *prometheus.GaugeVec
}

func newTSOMetrics(registerer prometheus.Registerer) *TSOMetrics {
	m := &TSOMetrics{
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_tso_request_duration_seconds",
				Help:    "End-to-end duration of dedicated TSO reserve and validation attempts by local or remote leader path and outcome.",
				Buckets: tsoRequestDurationBuckets,
			},
			[]string{"operation", "path", "outcome"},
		),
		shadowComparison: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_tso_shadow_comparisons_total",
				Help: "Shadow migration comparisons by bounded result: legacy_ahead, legacy_overlap, cutover_active, or error.",
			},
			[]string{"result"},
		),
		shadowDivergence: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "elastickv_tso_shadow_divergence_timestamps",
				Help:    "Absolute timestamp distance between a shadow legacy candidate and the preceding durable TSO allocation floor.",
				Buckets: prometheus.ExponentialBuckets(1, tsoDivergenceBucketFactor, tsoDivergenceBucketCount),
			},
			[]string{"result"},
		),
		mode: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_tso_mode",
				Help: "Current process-local TSO mode as a one-hot gauge.",
			},
			[]string{"mode"},
		),
		reloads: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_tso_mode_reload_total",
				Help: "Runtime TSO mode reload outcomes.",
			},
			[]string{"result"},
		),
		durableState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_tso_durable_state",
				Help: "Consensus-owned one-way TSO marker state for cutover and phase_d.",
			},
			[]string{"marker"},
		),
	}
	if registerer != nil {
		registerer.MustRegister(
			m.requestDuration,
			m.shadowComparison,
			m.shadowDivergence,
			m.mode,
			m.reloads,
			m.durableState,
		)
	}
	return m
}

type TSOObserver struct {
	metrics *TSOMetrics
}

func newTSOObserver(metrics *TSOMetrics) *TSOObserver {
	return &TSOObserver{metrics: metrics}
}

func (o *TSOObserver) ObserveTSORequest(operation, path, outcome string, duration time.Duration) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.requestDuration.WithLabelValues(operation, path, outcome).Observe(duration.Seconds())
}

func (o *TSOObserver) ObserveTSOShadowComparison(result string, divergence uint64) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.shadowComparison.WithLabelValues(result).Inc()
	if result == "legacy_ahead" || result == "legacy_overlap" {
		o.metrics.shadowDivergence.WithLabelValues(result).Observe(float64(divergence))
	}
}

func (o *TSOObserver) ObserveTSOMode(mode string) {
	if o == nil || o.metrics == nil {
		return
	}
	for _, candidate := range []string{"legacy", "shadow", "cutover", "phase-d"} {
		value := 0.0
		if candidate == mode {
			value = 1
		}
		o.metrics.mode.WithLabelValues(candidate).Set(value)
	}
}

func (o *TSOObserver) ObserveTSOModeReload(result string) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.reloads.WithLabelValues(result).Inc()
}

func (o *TSOObserver) ObserveTSODurableState(cutoverActive, phaseDActive bool) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.durableState.WithLabelValues("cutover").Set(boolMetricValue(cutoverActive))
	o.metrics.durableState.WithLabelValues("phase_d").Set(boolMetricValue(phaseDActive))
}

func boolMetricValue(value bool) float64 {
	if value {
		return 1
	}
	return 0
}
