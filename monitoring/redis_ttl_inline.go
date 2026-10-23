package monitoring

import (
	"strconv"
	"time"

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
// outcome is one of the RedisTTLInlinePass constants; startedAt is no later
// than the read timestamp of the pass's first page was acquired.
type RedisTTLInlineMigrationObserver interface {
	ObserveRedisTTLInlineMigrationPass(typeName string, groupID uint64, outcome string, startedAt time.Time)
}

// RedisTTLInlineMigrationMetrics exports the TTL-inline migrator's passes.
// Labels are the anchor type, data group and outcome only: eight types times
// the data groups a node leads times three outcomes.
type RedisTTLInlineMigrationMetrics struct {
	passes        *prometheus.CounterVec
	lastPassStart *prometheus.GaugeVec
}

func newRedisTTLInlineMigrationMetrics(registerer prometheus.Registerer) *RedisTTLInlineMigrationMetrics {
	m := &RedisTTLInlineMigrationMetrics{
		passes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_redis_ttl_inline_migration_passes_total",
				Help: "Completed Redis TTL-inline migration passes by anchor type, data group, and outcome (clean, partial, not_clean).",
			},
			[]string{"type", labelGroup, labelOutcome},
		),
		lastPassStart: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_redis_ttl_inline_migration_last_pass_start_timestamp_seconds",
				Help: "Unix time at which the latest Redis TTL-inline migration pass this node completed began, by anchor type, data group, and outcome (clean, partial, not_clean).",
			},
			[]string{"type", labelGroup, labelOutcome},
		),
	}
	if registerer != nil {
		registerer.MustRegister(m.passes, m.lastPassStart)
	}
	return m
}

// ObserveRedisTTLInlineMigrationPass counts a completed pass and records when
// it began. The start, not the end, is what a gate compares against: a pass
// that ended after an event but began before it read part of its prefix
// before the event.
func (m *RedisTTLInlineMigrationMetrics) ObserveRedisTTLInlineMigrationPass(typeName string, groupID uint64, outcome string, startedAt time.Time) {
	if m == nil {
		return
	}
	group := strconv.FormatUint(groupID, 10)
	m.passes.WithLabelValues(typeName, group, outcome).Inc()
	m.lastPassStart.WithLabelValues(typeName, group, outcome).Set(float64(startedAt.UnixNano()) / float64(time.Second))
}
