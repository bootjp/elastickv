package monitoring

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
)

var raftStates = []string{
	"follower",
	"candidate",
	"leader",
	"shutdown",
	"unknown",
}

var loggedLastContactParseValues sync.Map

const (
	defaultObserveInterval  = 5 * time.Second
	lastContactUnknownValue = -1
)

// RaftRuntime describes a raft group observed by the metrics exporter.
type RaftRuntime struct {
	GroupID uint64
	Raft    *raft.Raft
}

type RaftMetrics struct {
	localState     *prometheus.GaugeVec
	leaderIdentity *prometheus.GaugeVec
	memberPresent  *prometheus.GaugeVec
	memberIsLeader *prometheus.GaugeVec
	memberCount    *prometheus.GaugeVec
	commitIndex    *prometheus.GaugeVec
	appliedIndex   *prometheus.GaugeVec
	lastContact    *prometheus.GaugeVec
}

func newRaftMetrics(registerer prometheus.Registerer) *RaftMetrics {
	m := &RaftMetrics{
		localState: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_local_state",
				Help: "Current local raft state for each group.",
			},
			[]string{"group", "state"},
		),
		leaderIdentity: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_leader_identity",
				Help: "Current leader identity for each raft group, as observed by this node.",
			},
			[]string{"group", "leader_id", "leader_address"},
		),
		memberPresent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_member_present",
				Help: "Current raft configuration members known to this node.",
			},
			[]string{"group", "member_id", "member_address", "suffrage"},
		),
		memberIsLeader: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_member_is_leader",
				Help: "Whether a raft configuration member is the current leader, as observed by this node.",
			},
			[]string{"group", "member_id", "member_address"},
		),
		memberCount: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_members",
				Help: "Number of raft configuration members currently known for each group.",
			},
			[]string{"group"},
		),
		commitIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_commit_index",
				Help: "Latest raft commit index for each group.",
			},
			[]string{"group"},
		),
		appliedIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_applied_index",
				Help: "Latest raft applied index for each group.",
			},
			[]string{"group"},
		),
		lastContact: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_last_contact_seconds",
				Help: "Time since the last observed leader contact for each group.",
			},
			[]string{"group"},
		),
	}

	registerer.MustRegister(
		m.localState,
		m.leaderIdentity,
		m.memberPresent,
		m.memberIsLeader,
		m.memberCount,
		m.commitIndex,
		m.appliedIndex,
		m.lastContact,
	)

	return m
}

type RaftObserver struct {
	metrics *RaftMetrics

	mu            sync.Mutex
	leaderLabels  map[string]prometheus.Labels
	memberLabels  map[string]map[string]prometheus.Labels
	memberLeaders map[string]map[string]prometheus.Labels
}

func newRaftObserver(metrics *RaftMetrics) *RaftObserver {
	return &RaftObserver{
		metrics:       metrics,
		leaderLabels:  map[string]prometheus.Labels{},
		memberLabels:  map[string]map[string]prometheus.Labels{},
		memberLeaders: map[string]map[string]prometheus.Labels{},
	}
}

// Start polls raft state and configuration on a fixed interval until ctx is canceled.
func (o *RaftObserver) Start(ctx context.Context, runtimes []RaftRuntime, interval time.Duration) {
	if o == nil {
		return
	}
	if interval <= 0 {
		interval = defaultObserveInterval
	}
	o.ObserveOnce(runtimes)
	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.ObserveOnce(runtimes)
			}
		}
	}()
}

// ObserveOnce captures the latest raft state for all configured runtimes.
func (o *RaftObserver) ObserveOnce(runtimes []RaftRuntime) {
	if o == nil {
		return
	}
	for _, runtime := range runtimes {
		o.observeRuntime(runtime)
	}
}

func (o *RaftObserver) observeRuntime(runtime RaftRuntime) {
	if o == nil || o.metrics == nil || runtime.Raft == nil {
		return
	}

	group := strconv.FormatUint(runtime.GroupID, 10)
	stats := runtime.Raft.Stats()
	state := normalizeRaftState(stats["state"])
	for _, candidate := range raftStates {
		value := 0.0
		if candidate == state {
			value = 1
		}
		o.metrics.localState.WithLabelValues(group, candidate).Set(value)
	}

	o.metrics.commitIndex.WithLabelValues(group).Set(float64(parseUintMetric(stats["commit_index"])))
	o.metrics.appliedIndex.WithLabelValues(group).Set(float64(parseUintMetric(stats["applied_index"])))
	o.metrics.lastContact.WithLabelValues(group).Set(parseLastContactSeconds(stats["last_contact"]))

	leaderAddr, leaderID := runtime.Raft.LeaderWithID()
	o.setLeaderMetric(group, string(leaderID), string(leaderAddr))

	future := runtime.Raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return
	}
	o.metrics.memberCount.WithLabelValues(group).Set(float64(len(future.Configuration().Servers)))
	o.setMembers(group, string(leaderID), future.Configuration().Servers)
}

func (o *RaftObserver) setLeaderMetric(group string, leaderID string, leaderAddress string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if previous, ok := o.leaderLabels[group]; ok {
		o.metrics.leaderIdentity.Delete(previous)
		delete(o.leaderLabels, group)
	}
	if leaderID == "" && leaderAddress == "" {
		return
	}
	labels := prometheus.Labels{
		"group":          group,
		"leader_id":      leaderID,
		"leader_address": leaderAddress,
	}
	o.metrics.leaderIdentity.With(labels).Set(1)
	o.leaderLabels[group] = labels
}

func (o *RaftObserver) setMembers(group string, leaderID string, servers []raft.Server) {
	o.mu.Lock()
	defer o.mu.Unlock()

	previousMembers := o.memberLabels[group]
	for _, labels := range previousMembers {
		o.metrics.memberPresent.Delete(labels)
	}
	previousLeaders := o.memberLeaders[group]
	for _, labels := range previousLeaders {
		o.metrics.memberIsLeader.Delete(labels)
	}

	nextMembers := make(map[string]prometheus.Labels, len(servers))
	nextLeaders := make(map[string]prometheus.Labels, len(servers))
	for _, server := range servers {
		memberID := string(server.ID)
		memberAddress := string(server.Address)
		memberKey := fmt.Sprintf("%s|%s", memberID, memberAddress)

		memberLabels := prometheus.Labels{
			"group":          group,
			"member_id":      memberID,
			"member_address": memberAddress,
			"suffrage":       normalizeSuffrage(server.Suffrage),
		}
		o.metrics.memberPresent.With(memberLabels).Set(1)
		nextMembers[memberKey] = memberLabels

		leaderLabels := prometheus.Labels{
			"group":          group,
			"member_id":      memberID,
			"member_address": memberAddress,
		}
		if memberID != "" && memberID == leaderID {
			o.metrics.memberIsLeader.With(leaderLabels).Set(1)
		} else {
			o.metrics.memberIsLeader.With(leaderLabels).Set(0)
		}
		nextLeaders[memberKey] = leaderLabels
	}

	o.memberLabels[group] = nextMembers
	o.memberLeaders[group] = nextLeaders
}

func normalizeRaftState(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "follower":
		return "follower"
	case "candidate":
		return "candidate"
	case "leader":
		return "leader"
	case "shutdown":
		return "shutdown"
	default:
		return "unknown"
	}
}

func normalizeSuffrage(s raft.ServerSuffrage) string {
	switch s {
	case raft.Voter:
		return "voter"
	case raft.Nonvoter:
		return "nonvoter"
	case raft.Staging:
		return "staging"
	default:
		return "unknown"
	}
}

func parseUintMetric(raw string) uint64 {
	v, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func parseLastContactSeconds(raw string) float64 {
	raw = strings.TrimSpace(raw)
	switch raw {
	case "", "never":
		return lastContactUnknownValue
	case "0":
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		if _, loaded := loggedLastContactParseValues.LoadOrStore(raw, struct{}{}); !loaded {
			slog.Warn("failed to parse raft last_contact metric", "raw", raw, "err", err)
		}
		return lastContactUnknownValue
	}
	return d.Seconds()
}
