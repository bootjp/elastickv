package monitoring

import "github.com/prometheus/client_golang/prometheus"

// ColdStartMetrics exposes the cold-start snapshot-restore skip
// gate's three outcomes as Prometheus series. The skip gate fires
// at most once per process lifetime (on cold start) so these are
// low-cardinality, low-rate signals — counters rather than
// histograms. Operators alert when the skip rate falls below a
// soak threshold (design target: ≥ 90% in steady state) or when
// fallback_reason rises unexpectedly.
//
// See docs/design/2026_06_02_idempotent_snapshot_restore.md §9.
type ColdStartMetrics struct {
	// restoreTotal is the per-outcome counter for the three skip-
	// gate outcomes. Labels:
	//   outcome=skipped — the gate fired (live store fresh enough)
	//   outcome=executed — gate did NOT fire (FSM genuinely stale)
	//   outcome=fallback — strictly-additive fallback (FSM doesn't
	//     opt in, meta key missing, or LastAppliedIndex errored)
	// fallback_reason is empty for skipped/executed and one of
	// not_reader / missing_meta / read_err for fallback.
	restoreTotal *prometheus.CounterVec
	// appliedIndexGap is the absolute distance between the
	// snapshot pointer and the FSM's reported applied index at the
	// moment of the decision. Emitted with the same outcome label
	// so dashboards can plot "how far ahead were we when we
	// skipped" alongside "how far behind were we when we
	// restored".
	appliedIndexGap *prometheus.GaugeVec
}

func newColdStartMetrics(registerer prometheus.Registerer) *ColdStartMetrics {
	m := &ColdStartMetrics{
		restoreTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "elastickv_fsm_cold_start_restore_total",
			Help: "Cumulative count of cold-start snapshot-restore skip-gate outcomes. outcome=skipped means the gate fired (LastAppliedIndex >= snapshot.Index); outcome=executed means the FSM was genuinely stale; outcome=fallback means the strictly-additive fallback path ran (FSM did not opt in, meta key missing, or read error). fallback_reason is empty for skipped/executed.",
		}, []string{"outcome", "fallback_reason"}),
		appliedIndexGap: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "elastickv_fsm_cold_start_applied_index_gap",
			Help: "Absolute distance between the snapshot pointer and the FSM's reported applied index at cold start, in Raft log entries. outcome=skipped reports how far ahead the live store was; outcome=executed reports how far behind it was.",
		}, []string{"outcome"}),
	}
	if registerer != nil {
		registerer.MustRegister(m.restoreTotal)
		registerer.MustRegister(m.appliedIndexGap)
	}
	return m
}

// ColdStartObserver is the monitoring-side implementation of
// raftengine.ColdStartObserver. Holds nothing but the metrics
// handle; safe to share across goroutines (Prometheus collectors
// are concurrency-safe).
type ColdStartObserver struct {
	metrics *ColdStartMetrics
}

func newColdStartObserver(metrics *ColdStartMetrics) *ColdStartObserver {
	return &ColdStartObserver{metrics: metrics}
}

// RestoreSkipped records a successful skip. gap = have - snapIndex
// (positive: how far ahead the live store was).
func (o *ColdStartObserver) RestoreSkipped(snapIndex, have uint64) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.restoreTotal.WithLabelValues("skipped", "").Inc()
	o.metrics.appliedIndexGap.WithLabelValues("skipped").Set(float64(have - snapIndex))
}

// RestoreExecuted records a full restore that ran because the FSM
// was stale. gap = snapIndex - have (positive: how far behind).
func (o *ColdStartObserver) RestoreExecuted(snapIndex, have uint64) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.restoreTotal.WithLabelValues("executed", "").Inc()
	o.metrics.appliedIndexGap.WithLabelValues("executed").Set(float64(snapIndex - have))
}

// RestoreFallback records the strictly-additive fallback path.
// reason is a stable enum the engine supplies (not_reader /
// missing_meta / read_err). No gap is reported — the store could
// not authoritatively name a value.
func (o *ColdStartObserver) RestoreFallback(snapIndex uint64, reason string) {
	_ = snapIndex
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.restoreTotal.WithLabelValues("fallback", reason).Inc()
}
