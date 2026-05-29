package monitoring

import (
	"context"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// hlcMsPerSecond converts the HLC's millisecond-granular ceiling and
// wall samples into the seconds-granular gauge values Prometheus
// convention prefers.  Named so the divisions in observeOnce read
// as unit conversions rather than magic numbers.
const hlcMsPerSecond = 1000.0

// HLCSource is the surface monitoring needs from the kv-layer HLC.
// Defining it here (rather than importing *kv.HLC directly) keeps the
// monitoring package decoupled from the kv module and lets a test or
// shim supply its own snapshot.
//
// PhysicalCeiling returns the Raft-agreed physical ceiling in Unix
// milliseconds (zero before bootstrap).  NextFencedRejections is a
// cumulative counter incremented inside HLC.NextFenced() whenever it
// returns ErrCeilingExpired; the observer reads the difference between
// successive ticks so Prometheus sees a counter, not a gauge.
type HLCSource interface {
	PhysicalCeiling() int64
	NextFencedRejections() uint64
}

// HLCMetrics exposes the HLC-4 (i) bounded-skew assumption and the
// HLC-4 (iii) fence-trip rate as Prometheus series so operators can
// alert before the safety property is exercised in earnest.
//
// The two gauges together let operators see *why* a NextFenced
// rejection happened (was the ceiling stale? was the wall clock
// ahead?), and the counter captures the load-bearing fact:
// NextFenced refused to issue a persistence ts.
type HLCMetrics struct {
	// physicalCeilingSeconds is the Raft-agreed physical ceiling,
	// expressed as a Unix timestamp in seconds.  A leader's
	// ceiling should sit ~hlcPhysicalWindowMs ms (3 s default)
	// ahead of wall_now; a stagnant value means the lease
	// renewer has stopped landing entries.
	physicalCeilingSeconds prometheus.Gauge
	// wallSkewSeconds is wall_now minus physicalCeiling, in
	// seconds.  Negative means the ceiling is in the future
	// (healthy, with the leader's renewer keeping up).  Zero or
	// positive means the ceiling has expired locally —
	// NextFenced is at-or-past the rejection boundary.  Alert
	// on wallSkewSeconds > -0.5 (half of hlcPhysicalWindowMs)
	// to catch a renewal stall before the fence trips.
	wallSkewSeconds prometheus.Gauge
	// nextFencedRejectionsTotal is the cumulative number of
	// NextFenced() calls that returned ErrCeilingExpired.  A
	// non-zero rate is the HLC-4 (iii) fence actually firing —
	// every increment means at least one persistence-grade
	// allocation was refused.
	nextFencedRejectionsTotal prometheus.Counter
}

func newHLCMetrics(registerer prometheus.Registerer) *HLCMetrics {
	m := &HLCMetrics{
		physicalCeilingSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "elastickv_hlc_physical_ceiling_seconds",
			Help: "Raft-agreed HLC physical ceiling, expressed as a Unix timestamp in seconds. Zero before bootstrap.",
		}),
		wallSkewSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "elastickv_hlc_wall_skew_seconds",
			Help: "wall_now - physicalCeiling, in seconds. Negative is healthy (ceiling is in the future); zero or positive means NextFenced is at-or-past the rejection boundary. Alert on > -0.5 to catch renewal stalls before the HLC-4 (iii) fence trips.",
		}),
		nextFencedRejectionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "elastickv_hlc_next_fenced_rejections_total",
			Help: "Cumulative number of HLC.NextFenced() calls that returned ErrCeilingExpired (HLC-4 (iii) fence fired).",
		}),
	}
	if registerer != nil {
		registerer.MustRegister(m.physicalCeilingSeconds)
		registerer.MustRegister(m.wallSkewSeconds)
		registerer.MustRegister(m.nextFencedRejectionsTotal)
	}
	return m
}

// HLCObserver polls an HLCSource on a fixed interval and updates the
// matching HLCMetrics.  Created via Registry.HLCObserver(); started
// by main.go alongside the other observers once the HLC is wired.
type HLCObserver struct {
	metrics *HLCMetrics
	mu      sync.Mutex
	// lastRejections is the previous-tick value of
	// HLCSource.NextFencedRejections(); we report the delta as a
	// counter increment so Prometheus sees monotonic rate
	// arithmetic even though the source already snapshots a
	// cumulative number.
	lastRejections uint64
}

func newHLCObserver(metrics *HLCMetrics) *HLCObserver {
	return &HLCObserver{metrics: metrics}
}

// Start polls the source on every tick until ctx is cancelled.  An
// interval of zero falls back to defaultObserveInterval (5s) to match
// the Raft observer.  Safe to call with a nil receiver / nil source —
// both shapes silently no-op so test fixtures that omit the registry
// do not need conditional wiring.
func (o *HLCObserver) Start(ctx context.Context, source HLCSource, interval time.Duration) {
	if o == nil || source == nil {
		return
	}
	if interval <= 0 {
		interval = defaultObserveInterval
	}
	o.observeOnce(source)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.observeOnce(source)
			}
		}
	}()
}

// ObserveOnce captures a single snapshot.  Exposed for tests.
func (o *HLCObserver) ObserveOnce(source HLCSource) {
	if o == nil {
		return
	}
	o.observeOnce(source)
}

func (o *HLCObserver) observeOnce(source HLCSource) {
	if o == nil || o.metrics == nil || source == nil {
		return
	}
	ceilingMs := source.PhysicalCeiling()
	nowMs := time.Now().UnixMilli()

	o.metrics.physicalCeilingSeconds.Set(float64(ceilingMs) / hlcMsPerSecond)
	o.metrics.wallSkewSeconds.Set(float64(nowMs-ceilingMs) / hlcMsPerSecond)

	o.mu.Lock()
	defer o.mu.Unlock()
	rejections := source.NextFencedRejections()
	if rejections > o.lastRejections {
		delta := rejections - o.lastRejections
		// Counter increments only — float64 conversion is safe
		// because rejections is bounded by the lifetime of the
		// process and a single tick's delta is small in practice.
		o.metrics.nextFencedRejectionsTotal.Add(float64(delta))
		o.lastRejections = rejections
	}
}
