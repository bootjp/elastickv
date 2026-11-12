package main

import (
	"context"
	"flag"
	"log/slog"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/leaderbalance"
	"github.com/bootjp/elastickv/internal/noderuntime"
	"github.com/bootjp/elastickv/kv"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

// raftMemberSuffrageVoter is the Suffrage value of a voting member in a
// raftengine.Configuration.
const raftMemberSuffrageVoter = "voter"

// leaderBalanceFlags configures the automatic leader-balance scheduler.
type leaderBalanceFlags struct {
	enabled            bool
	interval           time.Duration
	groupCooldown      time.Duration
	globalCooldown     time.Duration
	startupGrace       time.Duration
	imbalanceThreshold int
	maxTargetLag       uint64
	pinGroups          string
	killSwitchFile     string
}

func (f *leaderBalanceFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&f.enabled, "leaderBalance", false, "Enable automatic count-based Raft-group leader balancing on the default-group leader")
	fs.DurationVar(&f.interval, "leaderBalanceInterval", leaderbalance.DefaultInterval, "Interval between leader-balance scheduler evaluations")
	fs.DurationVar(&f.groupCooldown, "leaderBalanceGroupCooldown", leaderbalance.DefaultGroupCooldown, "Minimum time before the scheduler can move the same raft group again")
	fs.DurationVar(&f.globalCooldown, "leaderBalanceGlobalCooldown", leaderbalance.DefaultGlobalCooldown, "Minimum time between any two automatic leadership transfers")
	fs.DurationVar(&f.startupGrace, "leaderBalanceStartupGrace", 0, "Grace period after acquiring default-group leadership before issuing transfers; 0 uses max(interval, global cooldown)")
	fs.IntVar(&f.imbalanceThreshold, "leaderBalanceImbalanceThreshold", leaderbalance.DefaultImbalanceThreshold, "Minimum leader-count spread required before balancing")
	fs.Uint64Var(&f.maxTargetLag, "leaderBalanceMaxTargetLag", leaderbalance.DefaultMaxTargetLag, "Maximum target lag in raft log entries for gated automatic leadership transfers; 0 requires the target to match the leader's last log index")
	fs.StringVar(&f.pinGroups, "leaderBalancePinGroups", "", "Comma-separated raft group IDs excluded from automatic leader balancing")
	fs.StringVar(&f.killSwitchFile, "leaderBalanceKillSwitchFile", "", "If non-empty and the file exists, the leader-balance scheduler observes but skips transfers")
}

func leaderBalanceOptions(f leaderBalanceFlags, localNodeID string, defaultGroupID uint64, partitionMap map[string]noderuntime.SQSFifoQueueRouting, registerer prometheus.Registerer) leaderbalance.Options {
	return leaderbalance.Options{
		Enabled:            f.enabled,
		LocalNodeID:        localNodeID,
		DefaultGroupID:     defaultGroupID,
		Interval:           f.interval,
		GroupCooldown:      f.groupCooldown,
		GlobalCooldown:     f.globalCooldown,
		StartupGrace:       f.startupGrace,
		ImbalanceThreshold: f.imbalanceThreshold,
		MaxTargetLag:       f.maxTargetLag,
		PinnedGroups:       leaderbalance.ParsePinnedGroups(f.pinGroups, slog.Default()),
		PartitionedGroups:  partitionedGroupSet(partitionMap, slog.Default()),
		KillSwitchFile:     strings.TrimSpace(f.killSwitchFile),
		Logger:             slog.Default(),
		Metrics:            leaderbalance.NewMetrics(registerer),
	}
}

// leaderBalanceGroups adapts the node's Raft group runtimes to the
// balancer's view, dropping nil entries so no typed-nil interface reaches
// the scheduler.
func leaderBalanceGroups(runtimes []*raftGroupRuntime) []leaderbalance.Group {
	groups := make([]leaderbalance.Group, 0, len(runtimes))
	for _, rt := range runtimes {
		if rt != nil {
			groups = append(groups, rt)
		}
	}
	return groups
}

// GroupID is the Raft group this runtime serves.
func (r *raftGroupRuntime) GroupID() uint64 {
	return r.Spec.ID
}

func startLeaderBalance(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime, opts leaderbalance.Options, done chan<- struct{}) {
	leaderbalance.Start(ctx, eg, leaderBalanceGroups(runtimes), opts, func() *kv.GRPCConnCache {
		return newBootstrapRuntimeConnCache(runtimes)
	}, done)
}
