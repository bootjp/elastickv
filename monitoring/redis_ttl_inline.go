package monitoring

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	redisTTLInlinePassClean    = "clean"
	redisTTLInlinePassNotClean = "not_clean"
)

// RedisTTLInlineMigrationObserver records the passes the Redis TTL-inline
// migrator completes. A pass covers one anchor type (typeName, such as
// "ttl-inline-string") in one data group; group 0 is the whole-prefix scan the
// migrator falls back to when the coordinator does not report its groups.
type RedisTTLInlineMigrationObserver interface {
	ObserveRedisTTLInlineMigrationPass(typeName string, groupID uint64, clean bool)
}

// RedisTTLInlineMigrationMetrics exports the TTL-inline migrator's passes.
// Labels are the anchor type and data group only: eight types times the data
// groups a node leads.
type RedisTTLInlineMigrationMetrics struct {
	passes        *prometheus.CounterVec
	lastCleanPass *prometheus.GaugeVec
}

func newRedisTTLInlineMigrationMetrics(registerer prometheus.Registerer) *RedisTTLInlineMigrationMetrics {
	m := &RedisTTLInlineMigrationMetrics{
		passes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_redis_ttl_inline_migration_passes_total",
				Help: "Completed Redis TTL-inline migration passes by anchor type, data group, and outcome. A clean pass read the whole prefix and found every anchor already in the inline format.",
			},
			[]string{"type", "group", "outcome"},
		),
		lastCleanPass: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_redis_ttl_inline_migration_last_clean_pass_timestamp_seconds",
				Help: "Unix time at which this node last completed a clean Redis TTL-inline migration pass, by anchor type and data group.",
			},
			[]string{"type", "group"},
		),
	}
	if registerer != nil {
		registerer.MustRegister(m.passes, m.lastCleanPass)
	}
	return m
}

// ObserveRedisTTLInlineMigrationPass counts a completed pass and, when it was
// clean, stamps the type and group's last clean pass time.
func (m *RedisTTLInlineMigrationMetrics) ObserveRedisTTLInlineMigrationPass(typeName string, groupID uint64, clean bool) {
	if m == nil {
		return
	}
	group := strconv.FormatUint(groupID, 10)
	outcome := redisTTLInlinePassNotClean
	if clean {
		outcome = redisTTLInlinePassClean
		m.lastCleanPass.WithLabelValues(typeName, group).SetToCurrentTime()
	}
	m.passes.WithLabelValues(typeName, group, outcome).Inc()
}
