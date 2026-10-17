package monitoring

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Outcomes of a Redis TTL-inline migration pass.
const (
	// RedisTTLInlinePassClean is a pass that read its whole prefix while its
	// group stayed in this node's plans and found every anchor already in the
	// inline format.
	RedisTTLInlinePassClean = "clean"
	// RedisTTLInlinePassPartial is a pass that rewrote, skipped and failed
	// nothing but did not read its whole prefix: it resumed a cursor left
	// before its group dropped out of this node's plans.
	RedisTTLInlinePassPartial = "partial"
	// RedisTTLInlinePassNotClean is a pass that rewrote, skipped or failed at
	// least one anchor.
	RedisTTLInlinePassNotClean = "not_clean"
)

// RedisTTLInlineMigrationObserver records the passes the Redis TTL-inline
// migrator completes. A pass covers one anchor type (typeName, such as
// "ttl-inline-string") in one data group; group 0 is the whole-prefix scan the
// migrator falls back to when the coordinator does not report its groups.
// outcome is one of the RedisTTLInlinePass constants.
type RedisTTLInlineMigrationObserver interface {
	ObserveRedisTTLInlineMigrationPass(typeName string, groupID uint64, outcome string)
}

// RedisTTLInlineMigrationMetrics exports the TTL-inline migrator's passes.
// Labels are the anchor type, data group and outcome only: eight types times
// the data groups a node leads times three outcomes.
type RedisTTLInlineMigrationMetrics struct {
	passes   *prometheus.CounterVec
	lastPass *prometheus.GaugeVec
}

func newRedisTTLInlineMigrationMetrics(registerer prometheus.Registerer) *RedisTTLInlineMigrationMetrics {
	m := &RedisTTLInlineMigrationMetrics{
		passes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_redis_ttl_inline_migration_passes_total",
				Help: "Completed Redis TTL-inline migration passes by anchor type, data group, and outcome (clean, partial, not_clean).",
			},
			[]string{"type", "group", "outcome"},
		),
		lastPass: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_redis_ttl_inline_migration_last_pass_timestamp_seconds",
				Help: "Unix time at which this node last completed a Redis TTL-inline migration pass, by anchor type, data group, and outcome (clean, partial, not_clean).",
			},
			[]string{"type", "group", "outcome"},
		),
	}
	if registerer != nil {
		registerer.MustRegister(m.passes, m.lastPass)
	}
	return m
}

// ObserveRedisTTLInlineMigrationPass counts a completed pass and stamps the
// type, group and outcome's last pass time.
func (m *RedisTTLInlineMigrationMetrics) ObserveRedisTTLInlineMigrationPass(typeName string, groupID uint64, outcome string) {
	if m == nil {
		return
	}
	group := strconv.FormatUint(groupID, 10)
	m.passes.WithLabelValues(typeName, group, outcome).Inc()
	m.lastPass.WithLabelValues(typeName, group, outcome).SetToCurrentTime()
}
