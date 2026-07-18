package autosplit

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Observer receives bounded-cardinality scheduler outcomes. Implementations
// must not attach route IDs or key bytes as metric labels.
type Observer interface {
	ObserveCandidatesPromoted(count int)
	ObserveSplitScheduled()
	ObserveSplitFailed(reason string)
	ObserveSkipped(reason SkipReason)
	ObserveIsolationDeclined(reason IsolationDeclineReason)
	ObserveCompoundPartial()
	ObserveState(enabled bool, trackedRoutes, cooldownActive int, evalDuration time.Duration)
}

type prometheusObserver struct {
	candidatesPromoted prometheus.Counter
	splitsScheduled    prometheus.Counter
	splitsFailed       *prometheus.CounterVec
	skipped            *prometheus.CounterVec
	isolationDeclined  *prometheus.CounterVec
	compoundPartial    prometheus.Counter
	enabled            prometheus.Gauge
	trackedRoutes      prometheus.Gauge
	cooldownActive     prometheus.Gauge
	evalDuration       prometheus.Gauge
}

// NewPrometheusObserver registers the standalone auto-split metric families.
func NewPrometheusObserver(registerer prometheus.Registerer) Observer {
	if registerer == nil {
		return nil
	}
	o := &prometheusObserver{
		candidatesPromoted: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "autosplit_candidates_promoted_total",
			Help: "Hot routes promoted after consecutive committed windows.",
		}),
		splitsScheduled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "autosplit_splits_scheduled_total",
			Help: "Automatic SplitRange RPCs issued.",
		}),
		splitsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "autosplit_splits_failed_total",
			Help: "Automatic SplitRange RPC failures by bounded reason.",
		}, []string{"reason"}),
		skipped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "autosplit_skipped_total",
			Help: "Automatic split decisions skipped before an RPC by bounded reason.",
		}, []string{"reason"}),
		isolationDeclined: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "autosplit_isolation_declined_total",
			Help: "Top-K isolation attempts declined by bounded reason.",
		}, []string{"reason"}),
		compoundPartial: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "autosplit_compound_partial_total",
			Help: "Compound isolations whose first split committed but second split did not.",
		}),
		enabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "autosplit_enabled",
			Help: "Whether automatic splitting is enabled on this node.",
		}),
		trackedRoutes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "autosplit_tracked_routes",
			Help: "Routes in the leader-local detector state map.",
		}),
		cooldownActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "autosplit_cooldown_active",
			Help: "Tracked routes currently in split cooldown.",
		}),
		evalDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "autosplit_eval_duration_seconds",
			Help: "Wall time of the latest automatic split evaluation cycle.",
		}),
	}
	registerer.MustRegister(
		o.candidatesPromoted,
		o.splitsScheduled,
		o.splitsFailed,
		o.skipped,
		o.isolationDeclined,
		o.compoundPartial,
		o.enabled,
		o.trackedRoutes,
		o.cooldownActive,
		o.evalDuration,
	)
	return o
}

func (o *prometheusObserver) ObserveCandidatesPromoted(count int) {
	if o != nil && count > 0 {
		o.candidatesPromoted.Add(float64(count))
	}
}

func (o *prometheusObserver) ObserveSplitScheduled() {
	if o != nil {
		o.splitsScheduled.Inc()
	}
}

func (o *prometheusObserver) ObserveSplitFailed(reason string) {
	if o != nil {
		o.splitsFailed.WithLabelValues(normalizeSplitFailureReason(reason)).Inc()
	}
}

func (o *prometheusObserver) ObserveSkipped(reason SkipReason) {
	if o == nil || !metricSkipReason(reason) {
		return
	}
	o.skipped.WithLabelValues(string(reason)).Inc()
}

func (o *prometheusObserver) ObserveIsolationDeclined(reason IsolationDeclineReason) {
	if o == nil || reason == "" {
		return
	}
	o.isolationDeclined.WithLabelValues(string(reason)).Inc()
}

func (o *prometheusObserver) ObserveCompoundPartial() {
	if o != nil {
		o.compoundPartial.Inc()
	}
}

func (o *prometheusObserver) ObserveState(
	enabled bool,
	trackedRoutes int,
	cooldownActive int,
	evalDuration time.Duration,
) {
	if o == nil {
		return
	}
	if enabled {
		o.enabled.Set(1)
	} else {
		o.enabled.Set(0)
	}
	o.trackedRoutes.Set(float64(trackedRoutes))
	o.cooldownActive.Set(float64(cooldownActive))
	o.evalDuration.Set(evalDuration.Seconds())
}

func metricSkipReason(reason SkipReason) bool {
	switch reason {
	case SkipReasonNoSplitKey,
		SkipReasonRouteCap,
		SkipReasonBudgetExhausted,
		SkipReasonNonActiveState,
		SkipReasonAggregateRow,
		SkipReasonUnsplittableHotKey:
		return true
	case SkipReasonCooldown,
		SkipReasonInvalidWindow,
		SkipReasonLeadershipFence:
		return false
	default:
		return false
	}
}

func normalizeSplitFailureReason(reason string) string {
	switch reason {
	case "cas_conflict", "target_unavailable":
		return reason
	default:
		return "rpc_error"
	}
}
