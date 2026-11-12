package main

import (
	"context"
	"log/slog"
	"strings"

	"github.com/bootjp/elastickv/internal/leaderbalance"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

// raftMemberSuffrageVoter is the Suffrage value of a voting member in a
// raftengine.Configuration.
const raftMemberSuffrageVoter = "voter"

func leaderBalanceConfigFromFlags(localNodeID string, defaultGroupID uint64, partitionMap map[string]sqsFifoQueueRouting, registerer prometheus.Registerer) leaderbalance.Options {
	return leaderbalance.Options{
		Enabled:            *leaderBalance,
		LocalNodeID:        localNodeID,
		DefaultGroupID:     defaultGroupID,
		Interval:           *leaderBalanceInterval,
		GroupCooldown:      *leaderBalanceGroupCooldown,
		GlobalCooldown:     *leaderBalanceGlobalCooldown,
		StartupGrace:       *leaderBalanceStartupGrace,
		ImbalanceThreshold: *leaderBalanceImbalanceThreshold,
		MaxTargetLag:       *leaderBalanceMaxTargetLag,
		PinnedGroups:       leaderbalance.ParsePinnedGroups(*leaderBalancePinGroups, slog.Default()),
		PartitionedGroups:  partitionedGroupSet(partitionMap, slog.Default()),
		KillSwitchFile:     strings.TrimSpace(*leaderBalanceKillSwitchFile),
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
	return r.spec.id
}

// Engine is the group's Raft engine, or nil while it is not open.
func (r *raftGroupRuntime) Engine() raftengine.Engine {
	return r.snapshotEngine()
}

func startLeaderBalance(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime, opts leaderbalance.Options, done chan<- struct{}) {
	leaderbalance.Start(ctx, eg, leaderBalanceGroups(runtimes), opts, func() *kv.GRPCConnCache {
		return newBootstrapRuntimeConnCache(runtimes)
	}, done)
}
