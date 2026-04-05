package monitoring

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/prometheus/client_golang/prometheus"
)

var raftStates = []string{
	"follower",
	"candidate",
	"leader",
	"shutdown",
	"unknown",
}

const (
	defaultObserveInterval  = 5 * time.Second
	lastContactUnknownValue = -1
)

// RaftRuntime describes a raft group observed by the metrics exporter.
type RaftRuntime struct {
	GroupID      uint64
	StatusReader raftengine.StatusReader
	ConfigReader raftengine.ConfigReader
}

// RaftMetrics holds all Prometheus gauge vectors for a single Raft group.
// Fields are populated by observeRuntime from raftengine status snapshots.
type RaftMetrics struct {
	// localState reports the current state (follower/candidate/leader/shutdown/unknown).
	localState *prometheus.GaugeVec
	// leaderIdentity tracks the current leader's ID and address as observed by this node.
	leaderIdentity *prometheus.GaugeVec
	// memberPresent lists all known configuration members for the group.
	memberPresent *prometheus.GaugeVec
	// memberIsLeader indicates whether a given member is the current leader.
	memberIsLeader *prometheus.GaugeVec
	// memberCount is the number of configuration members currently known.
	memberCount *prometheus.GaugeVec
	// commitIndex is the latest committed log index.
	commitIndex *prometheus.GaugeVec
	// appliedIndex is the latest applied log index.
	appliedIndex *prometheus.GaugeVec
	// lastContact is the time in seconds since the last observed leader contact.
	lastContact *prometheus.GaugeVec
	// term is the current Raft term; increments on each leader election.
	term *prometheus.GaugeVec
	// lastLogIndex is the index of the last log entry written to stable storage.
	lastLogIndex *prometheus.GaugeVec
	// lastSnapshotIndex is the index covered by the most recent snapshot.
	lastSnapshotIndex *prometheus.GaugeVec
	// fsmPending is the number of commands queued to the FSM but not yet applied.
	fsmPending *prometheus.GaugeVec
	// numPeers is the number of other voting servers in the cluster, excluding this node.
	numPeers *prometheus.GaugeVec
	// leaderChanges counts observed leader transitions for each group.
	leaderChanges *prometheus.CounterVec
	// proposalsFailed counts raft proposals that failed before yielding a usable response.
	proposalsFailed *prometheus.CounterVec
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
		term: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_term",
				Help: "Current Raft term for each group.",
			},
			[]string{"group"},
		),
		lastLogIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_last_log_index",
				Help: "Index of the last log entry written for each group.",
			},
			[]string{"group"},
		),
		lastSnapshotIndex: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_last_snapshot_index",
				Help: "Index of the most recent snapshot for each group.",
			},
			[]string{"group"},
		),
		fsmPending: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_fsm_pending",
				Help: "Number of commands queued to the FSM but not yet applied for each group.",
			},
			[]string{"group"},
		),
		numPeers: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_raft_num_peers",
				Help: "Number of other voting servers in the cluster for each group, not including this node.",
			},
			[]string{"group"},
		),
		leaderChanges: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_raft_leader_changes_seen_total",
				Help: "Total number of observed leader changes for each group.",
			},
			[]string{"group"},
		),
		proposalsFailed: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_raft_proposals_failed_total",
				Help: "Total number of raft proposals that failed before returning a usable apply response.",
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
		m.term,
		m.lastLogIndex,
		m.lastSnapshotIndex,
		m.fsmPending,
		m.numPeers,
		m.leaderChanges,
		m.proposalsFailed,
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
	if o == nil || o.metrics == nil || runtime.StatusReader == nil {
		return
	}

	group := strconv.FormatUint(runtime.GroupID, 10)
	status := runtime.StatusReader.Status()
	state := normalizeRaftState(status.State)
	for _, candidate := range raftStates {
		value := 0.0
		if candidate == state {
			value = 1
		}
		o.metrics.localState.WithLabelValues(group, candidate).Set(value)
	}

	o.metrics.commitIndex.WithLabelValues(group).Set(float64(status.CommitIndex))
	o.metrics.appliedIndex.WithLabelValues(group).Set(float64(status.AppliedIndex))
	o.metrics.lastContact.WithLabelValues(group).Set(lastContactSeconds(status.LastContact))
	o.metrics.term.WithLabelValues(group).Set(float64(status.Term))
	o.metrics.lastLogIndex.WithLabelValues(group).Set(float64(status.LastLogIndex))
	o.metrics.lastSnapshotIndex.WithLabelValues(group).Set(float64(status.LastSnapshotIndex))
	o.metrics.fsmPending.WithLabelValues(group).Set(float64(status.FSMPending))
	o.metrics.numPeers.WithLabelValues(group).Set(float64(status.NumPeers))

	o.setLeaderMetric(group, status.Leader.ID, status.Leader.Address)

	if runtime.ConfigReader == nil {
		return
	}
	cfg, err := runtime.ConfigReader.Configuration(context.Background())
	if err != nil {
		return
	}
	o.metrics.memberCount.WithLabelValues(group).Set(float64(len(cfg.Servers)))
	o.setMembers(group, status.Leader.ID, cfg.Servers)
}

func (o *RaftObserver) setLeaderMetric(group string, leaderID string, leaderAddress string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	previous, hadPrevious := o.leaderLabels[group]
	if hadPrevious {
		o.metrics.leaderIdentity.Delete(previous)
		delete(o.leaderLabels, group)
	}
	if hadPrevious &&
		(previous["leader_id"] != leaderID || previous["leader_address"] != leaderAddress) &&
		(previous["leader_id"] != "" || previous["leader_address"] != "") {
		o.metrics.leaderChanges.WithLabelValues(group).Inc()
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

func (o *RaftObserver) observeLeaderChange(group string) {
	if o == nil || o.metrics == nil {
		return
	}
	o.metrics.leaderChanges.WithLabelValues(group).Inc()
}

func (o *RaftObserver) setMembers(group string, leaderID string, servers []raftengine.Server) {
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
		memberID := server.ID
		memberAddress := server.Address
		memberKey := fmt.Sprintf("%s|%s", memberID, memberAddress)

		memberLabels := prometheus.Labels{
			"group":          group,
			"member_id":      memberID,
			"member_address": memberAddress,
			"suffrage":       server.Suffrage,
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

func normalizeRaftState(state raftengine.State) string {
	switch state {
	case raftengine.StateFollower:
		return "follower"
	case raftengine.StateCandidate:
		return "candidate"
	case raftengine.StateLeader:
		return "leader"
	case raftengine.StateShutdown:
		return "shutdown"
	case raftengine.StateUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

func lastContactSeconds(d time.Duration) float64 {
	if d < 0 {
		return lastContactUnknownValue
	}
	return d.Seconds()
}

type raftProposalObserver struct {
	metrics *RaftMetrics
	group   string
}

func (o *raftProposalObserver) ObserveProposalFailure() {
	if o == nil || o.metrics == nil || o.group == "" {
		return
	}
	o.metrics.proposalsFailed.WithLabelValues(o.group).Inc()
}
