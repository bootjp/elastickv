package adapter

import (
	"context"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const legacyMetadataMetricsScrapeTimeout = 2 * time.Second

type legacyMetadataMetricsSource interface {
	LegacyMetadataPassGroupIDs() []uint64
	LegacyMetadataPassSource
}

// LegacyMetadataPassCollector exposes only complete, currently valid
// four-plan group sets. Missing series are unknown, never a zero-backlog proof.
type LegacyMetadataPassCollector struct {
	source     legacyMetadataMetricsSource
	complete   *prometheus.Desc
	rows       *prometheus.Desc
	catalog    *prometheus.Desc
	term       *prometheus.Desc
	started    *prometheus.Desc
	completion *prometheus.Desc
}

func NewLegacyMetadataPassCollector(source legacyMetadataMetricsSource) *LegacyMetadataPassCollector {
	labels := []string{"group_id", "plan"}
	return &LegacyMetadataPassCollector{
		source: source,
		complete: prometheus.NewDesc("elastickv_redis_legacy_metadata_pass_complete",
			"One means this plan belongs to a complete current four-plan diagnostic set, not an M4 gate verdict.", labels, nil),
		rows: prometheus.NewDesc("elastickv_redis_legacy_metadata_pass_rows",
			"Physical legacy base-metadata rows in a complete current pass, by owner and reason.",
			[]string{"group_id", "plan", "owner_group_id", "reason"}, nil),
		catalog: prometheus.NewDesc("elastickv_redis_legacy_metadata_pass_catalog_version",
			"Catalog version shared by a complete current four-plan diagnostic set.", labels, nil),
		term: prometheus.NewDesc("elastickv_redis_legacy_metadata_pass_leader_term",
			"Local leader term shared by a complete current four-plan diagnostic set.", labels, nil),
		started: prometheus.NewDesc("elastickv_redis_legacy_metadata_pass_start_timestamp_seconds",
			"Start time of a complete current diagnostic pass.", labels, nil),
		completion: prometheus.NewDesc("elastickv_redis_legacy_metadata_pass_completion_timestamp_seconds",
			"Completion epoch of a complete current diagnostic pass.", labels, nil),
	}
}

func (m *LegacyMetadataPassCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.complete
	ch <- m.rows
	ch <- m.catalog
	ch <- m.term
	ch <- m.started
	ch <- m.completion
}

func (m *LegacyMetadataPassCollector) Collect(ch chan<- prometheus.Metric) {
	if m == nil || m.source == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), legacyMetadataMetricsScrapeTimeout)
	defer cancel()
	for _, groupID := range m.source.LegacyMetadataPassGroupIDs() {
		passes, err := m.source.CompleteLegacyMetadataGroupPasses(ctx, groupID)
		if err != nil || validateLegacyMetadataRPCPasses(groupID, passes) != nil {
			continue
		}
		for _, pass := range passes {
			m.collectPass(ch, pass)
		}
	}
}

func (m *LegacyMetadataPassCollector) collectPass(ch chan<- prometheus.Metric, pass LegacyMetadataPassSnapshot) {
	group := strconv.FormatUint(pass.GroupID, 10)
	labels := []string{group, pass.Plan}
	ch <- prometheus.MustNewConstMetric(m.complete, prometheus.GaugeValue, 1, labels...)
	ch <- prometheus.MustNewConstMetric(m.catalog, prometheus.GaugeValue, float64(pass.CatalogVersion), labels...)
	ch <- prometheus.MustNewConstMetric(m.term, prometheus.GaugeValue, float64(pass.LeaderTerm), labels...)
	ch <- prometheus.MustNewConstMetric(m.started, prometheus.GaugeValue, float64(pass.StartedAt.UnixNano())/float64(time.Second), labels...)
	ch <- prometheus.MustNewConstMetric(m.completion, prometheus.GaugeValue, float64(pass.CompletionEpoch.UnixNano())/float64(time.Second), labels...)
	for owner, counts := range pass.Counts {
		ownerLabel := strconv.FormatUint(owner, 10)
		for _, reason := range []struct {
			name  string
			count uint64
		}{
			{"served", counts.Served},
			{"owner_has_version", counts.OwnerHasVersion},
			{"owner_led_elsewhere", counts.OwnerLedElsewhere},
			{"staged_visibility", counts.StagedVisibility},
		} {
			ch <- prometheus.MustNewConstMetric(m.rows, prometheus.GaugeValue, float64(reason.count), group, pass.Plan, ownerLabel, reason.name)
		}
	}
}
