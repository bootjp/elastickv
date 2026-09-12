package main

import (
	"context"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

const (
	defaultLeaderBalanceInterval           = 30 * time.Second
	defaultLeaderBalanceGroupCooldown      = 30 * time.Second
	defaultLeaderBalanceGlobalCooldown     = 10 * time.Second
	defaultLeaderBalanceImbalanceThreshold = 2
	defaultLeaderBalanceMaxTargetLag       = 1024
	defaultLeaderBalanceRPCTimeout         = 5 * time.Second
	raftMemberSuffrageVoter                = "voter"
)

type leaderBalanceConfig struct {
	enabled            bool
	localNodeID        string
	defaultGroupID     uint64
	interval           time.Duration
	groupCooldown      time.Duration
	globalCooldown     time.Duration
	startupGrace       time.Duration
	imbalanceThreshold int
	maxTargetLag       uint64
	pinnedGroups       map[uint64]bool
	partitionedGroups  map[uint64]bool
	killSwitchFile     string
	logger             *slog.Logger
	metrics            *leaderBalanceMetrics
}

type leaderBalanceLoop struct {
	cfg                 leaderBalanceConfig
	runtimes            []*raftGroupRuntime
	defaultRuntime      *raftGroupRuntime
	connCache           *kv.GRPCConnCache
	groupCooldownUntil  map[uint64]time.Time
	globalCooldownUntil time.Time
	startupGraceUntil   time.Time
	wasDefaultLeader    bool
}

type leaderBalanceObservation struct {
	groups       []leaderBalanceGroupSnapshot
	counts       map[string]int
	voters       map[string]raftengine.Server
	partial      bool
	partialCount int
}

type leaderBalanceGroupSnapshot struct {
	groupID     uint64
	engine      raftengine.Engine
	status      raftengine.Status
	leader      raftengine.LeaderInfo
	voters      []raftengine.Server
	localLeader bool
}

type leaderBalanceMove struct {
	groupID      uint64
	sourceID     string
	sourceAddr   string
	localSource  bool
	engine       raftengine.Engine
	candidates   []raftengine.TransferTarget
	chosenTarget raftengine.TransferTarget
}

type leaderBalanceDecision struct {
	move   leaderBalanceMove
	reason string
}

func startLeaderBalanceScheduler(ctx context.Context, eg *errgroup.Group, runtimes []*raftGroupRuntime, cfg leaderBalanceConfig) {
	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}
	if cfg.metrics != nil {
		cfg.metrics.setEnabled(cfg.enabled)
	}
	if !cfg.enabled || eg == nil {
		return
	}
	cfg.normalize()
	loop := &leaderBalanceLoop{
		cfg:                cfg,
		runtimes:           runtimes,
		defaultRuntime:     runtimeForGroup(runtimes, cfg.defaultGroupID),
		connCache:          &kv.GRPCConnCache{},
		groupCooldownUntil: make(map[uint64]time.Time),
	}
	eg.Go(func() error {
		defer func() {
			if err := loop.connCache.Close(); err != nil {
				cfg.logger.Warn("leader-balance: failed to close grpc cache", "err", err)
			}
		}()
		loop.run(ctx)
		return nil
	})
}

func leaderBalanceConfigFromFlags(localNodeID string, defaultGroupID uint64, partitionMap map[string]sqsFifoQueueRouting, registerer prometheus.Registerer) leaderBalanceConfig {
	cfg := leaderBalanceConfig{
		enabled:            *leaderBalance,
		localNodeID:        localNodeID,
		defaultGroupID:     defaultGroupID,
		interval:           *leaderBalanceInterval,
		groupCooldown:      *leaderBalanceGroupCooldown,
		globalCooldown:     *leaderBalanceGlobalCooldown,
		startupGrace:       *leaderBalanceStartupGrace,
		imbalanceThreshold: *leaderBalanceImbalanceThreshold,
		maxTargetLag:       *leaderBalanceMaxTargetLag,
		pinnedGroups:       parseLeaderBalancePinnedGroups(*leaderBalancePinGroups, slog.Default()),
		partitionedGroups:  partitionedGroupSet(partitionMap, slog.Default()),
		killSwitchFile:     strings.TrimSpace(*leaderBalanceKillSwitchFile),
		logger:             slog.Default(),
		metrics:            newLeaderBalanceMetrics(registerer),
	}
	cfg.normalize()
	return cfg
}

func (c *leaderBalanceConfig) normalize() {
	if c.interval <= 0 {
		c.interval = defaultLeaderBalanceInterval
	}
	if c.groupCooldown <= 0 {
		c.groupCooldown = defaultLeaderBalanceGroupCooldown
	}
	if c.globalCooldown <= 0 {
		c.globalCooldown = defaultLeaderBalanceGlobalCooldown
	}
	if c.startupGrace <= 0 {
		c.startupGrace = c.interval
		if c.globalCooldown > c.startupGrace {
			c.startupGrace = c.globalCooldown
		}
	}
	if c.imbalanceThreshold < defaultLeaderBalanceImbalanceThreshold {
		c.imbalanceThreshold = defaultLeaderBalanceImbalanceThreshold
	}
}

func (l *leaderBalanceLoop) run(ctx context.Context) {
	if l.defaultRuntime == nil {
		l.cfg.logger.Warn("leader-balance: default raft group runtime not found", "group", l.cfg.defaultGroupID)
		return
	}
	ticker := time.NewTicker(l.cfg.interval)
	defer ticker.Stop()
	l.tick(ctx, time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			l.tick(ctx, now)
		}
	}
}

func (l *leaderBalanceLoop) tick(ctx context.Context, now time.Time) {
	defaultEngine := l.defaultRuntime.snapshotEngine()
	defaultLeader := defaultEngine != nil && defaultEngine.State() == raftengine.StateLeader
	if !defaultLeader {
		l.wasDefaultLeader = false
		return
	}
	if !l.wasDefaultLeader {
		l.wasDefaultLeader = true
		l.groupCooldownUntil = make(map[uint64]time.Time)
		l.globalCooldownUntil = time.Time{}
		l.startupGraceUntil = now.Add(l.cfg.startupGrace)
	}
	obs := observeLeaderBalance(ctx, l.runtimes, l.cfg.localNodeID)
	if l.cfg.metrics != nil {
		l.cfg.metrics.observe(obs)
	}
	decision := chooseLeaderBalanceMove(obs, l.cfg, l.groupCooldownUntil, l.globalCooldownUntil, l.startupGraceUntil, now)
	if decision.reason != "" {
		l.observeSkip(decision.reason)
		return
	}
	if l.killSwitchActive() {
		l.observeSkip("kill_switch")
		return
	}
	err := l.executeMove(ctx, decision.move)
	if err != nil {
		if l.cfg.metrics != nil {
			l.cfg.metrics.observeTransfer("failed")
		}
		l.cfg.logger.Warn("leader-balance: transfer failed",
			"group", decision.move.groupID,
			"source", decision.move.sourceID,
			"target", decision.move.chosenTarget.ID,
			"err", err)
		return
	}
	if l.cfg.metrics != nil {
		l.cfg.metrics.observeTransfer("success")
	}
	l.groupCooldownUntil[decision.move.groupID] = now.Add(l.cfg.groupCooldown)
	l.globalCooldownUntil = now.Add(l.cfg.globalCooldown)
	l.cfg.logger.Info("leader-balance: transferred leadership",
		"group", decision.move.groupID,
		"source", decision.move.sourceID,
		"target", decision.move.chosenTarget.ID)
}

func (l *leaderBalanceLoop) observeSkip(reason string) {
	if l.cfg.metrics != nil {
		l.cfg.metrics.observeSkip(reason)
	}
}

func (l *leaderBalanceLoop) killSwitchActive() bool {
	if l.cfg.killSwitchFile == "" {
		return false
	}
	_, err := os.Stat(l.cfg.killSwitchFile)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	l.cfg.logger.Warn("leader-balance: kill switch stat failed; skipping transfers",
		"path", l.cfg.killSwitchFile,
		"err", err)
	return true
}

func (l *leaderBalanceLoop) executeMove(ctx context.Context, move leaderBalanceMove) error {
	if len(move.candidates) == 0 {
		return errors.New("leader-balance: move has no target candidates")
	}
	execCtx, cancel := context.WithTimeout(ctx, defaultLeaderBalanceRPCTimeout)
	defer cancel()
	if move.localSource {
		admin, ok := move.engine.(raftengine.Admin)
		if !ok {
			return errors.New("leader-balance: local source engine does not implement raft admin")
		}
		if err := admin.TransferLeadershipToServerIfEligible(execCtx, move.candidates, l.cfg.maxTargetLag); err != nil {
			return errors.Wrap(err, "leader-balance: transfer local leadership")
		}
		return nil
	}
	if move.sourceAddr == "" {
		return errors.New("leader-balance: remote source leader has no address")
	}
	conn, err := l.connCache.ConnFor(move.sourceAddr)
	if err != nil {
		return errors.Wrap(err, "leader-balance: connect to remote source")
	}
	req := buildLeaderBalanceTransferRequest(move.candidates, l.cfg.maxTargetLag)
	_, err = pb.NewRaftAdminClient(conn).TransferLeadership(execCtx, req)
	if err != nil {
		return errors.Wrap(err, "leader-balance: transfer remote leadership")
	}
	return nil
}

func buildLeaderBalanceTransferRequest(candidates []raftengine.TransferTarget, maxLag uint64) *pb.RaftAdminTransferLeadershipRequest {
	req := &pb.RaftAdminTransferLeadershipRequest{
		Gated:            true,
		MaxLag:           maxLag,
		TargetCandidates: make([]*pb.TransferTarget, 0, len(candidates)),
	}
	if len(candidates) > 0 {
		// Rolling-upgrade guard: old RaftAdmin servers ignore Gated and
		// TargetCandidates, but they still understand the legacy target fields.
		// Supplying only target_address makes those older handlers reject the
		// request as an incomplete ungated transfer instead of executing it.
		req.TargetAddress = candidates[0].Address
	}
	for _, candidate := range candidates {
		req.TargetCandidates = append(req.TargetCandidates, &pb.TransferTarget{
			TargetId:      candidate.ID,
			TargetAddress: candidate.Address,
		})
	}
	return req
}

func observeLeaderBalance(ctx context.Context, runtimes []*raftGroupRuntime, localNodeID string) leaderBalanceObservation {
	obs := leaderBalanceObservation{
		counts: make(map[string]int),
		voters: make(map[string]raftengine.Server),
	}
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		engine := rt.snapshotEngine()
		if engine == nil {
			continue
		}
		cfg, err := engine.Configuration(ctx)
		if err != nil {
			obs.partial = true
			obs.partialCount++
			continue
		}
		status := engine.Status()
		snap := leaderBalanceGroupSnapshot{
			groupID:     rt.spec.id,
			engine:      engine,
			status:      status,
			leader:      status.Leader,
			localLeader: status.State == raftengine.StateLeader && status.Leader.ID == localNodeID,
		}
		for _, server := range cfg.Servers {
			if server.Suffrage != raftMemberSuffrageVoter {
				continue
			}
			obs.voters[server.ID] = server
			if _, ok := obs.counts[server.ID]; !ok {
				obs.counts[server.ID] = 0
			}
			snap.voters = append(snap.voters, server)
		}
		if status.Leader.ID == "" {
			obs.partial = true
			obs.partialCount++
		} else {
			obs.counts[status.Leader.ID]++
		}
		obs.groups = append(obs.groups, snap)
	}
	sort.Slice(obs.groups, func(i, j int) bool {
		return obs.groups[i].groupID < obs.groups[j].groupID
	})
	return obs
}

func chooseLeaderBalanceMove(
	obs leaderBalanceObservation,
	cfg leaderBalanceConfig,
	groupCooldownUntil map[uint64]time.Time,
	globalCooldownUntil time.Time,
	startupGraceUntil time.Time,
	now time.Time,
) leaderBalanceDecision {
	if obs.partial {
		return leaderBalanceDecision{reason: "partial_observation"}
	}
	if len(obs.groups) == 0 || len(obs.voters) < 2 {
		return leaderBalanceDecision{reason: "no_voters"}
	}
	if now.Before(startupGraceUntil) {
		return leaderBalanceDecision{reason: "startup_grace"}
	}
	if now.Before(globalCooldownUntil) {
		return leaderBalanceDecision{reason: "global_cooldown"}
	}
	minCount, maxCount := leaderBalanceMinMax(obs.counts)
	if maxCount-minCount < cfg.imbalanceThreshold {
		return leaderBalanceDecision{reason: "balanced"}
	}
	for _, includeDefault := range []bool{false, true} {
		if move, ok := chooseLeaderBalanceMovePass(obs, cfg, groupCooldownUntil, now, includeDefault); ok {
			return leaderBalanceDecision{move: move}
		}
	}
	return leaderBalanceDecision{reason: "no_eligible_move"}
}

func chooseLeaderBalanceMovePass(
	obs leaderBalanceObservation,
	cfg leaderBalanceConfig,
	groupCooldownUntil map[uint64]time.Time,
	now time.Time,
	includeDefault bool,
) (leaderBalanceMove, bool) {
	var best leaderBalanceMove
	var found bool
	for _, group := range sortedLeaderBalanceGroups(obs.groups, cfg.defaultGroupID) {
		if !leaderBalanceGroupEligible(group, cfg, groupCooldownUntil, now, includeDefault) {
			continue
		}
		candidates := leaderBalanceTargetCandidates(group, obs.counts, cfg.imbalanceThreshold)
		if len(candidates) == 0 {
			continue
		}
		move := leaderBalanceMove{
			groupID:      group.groupID,
			sourceID:     group.leader.ID,
			sourceAddr:   group.leader.Address,
			localSource:  group.localLeader,
			engine:       group.engine,
			candidates:   candidates,
			chosenTarget: candidates[0],
		}
		if !found || leaderBalanceMovePreferred(move, best, obs.counts) {
			best = move
			found = true
		}
	}
	return best, found
}

func leaderBalanceMovePreferred(next, current leaderBalanceMove, counts map[string]int) bool {
	nextSourceCount := counts[next.sourceID]
	currentSourceCount := counts[current.sourceID]
	if nextSourceCount != currentSourceCount {
		return nextSourceCount > currentSourceCount
	}
	nextTargetCount := counts[next.chosenTarget.ID]
	currentTargetCount := counts[current.chosenTarget.ID]
	if nextTargetCount != currentTargetCount {
		return nextTargetCount < currentTargetCount
	}
	return next.groupID < current.groupID
}

func sortedLeaderBalanceGroups(groups []leaderBalanceGroupSnapshot, defaultGroupID uint64) []leaderBalanceGroupSnapshot {
	sorted := append([]leaderBalanceGroupSnapshot(nil), groups...)
	sort.SliceStable(sorted, func(i, j int) bool {
		aDefault := sorted[i].groupID == defaultGroupID
		bDefault := sorted[j].groupID == defaultGroupID
		if aDefault != bDefault {
			return !aDefault
		}
		return sorted[i].groupID < sorted[j].groupID
	})
	return sorted
}

func leaderBalanceGroupEligible(
	group leaderBalanceGroupSnapshot,
	cfg leaderBalanceConfig,
	groupCooldownUntil map[uint64]time.Time,
	now time.Time,
	includeDefault bool,
) bool {
	if group.groupID == cfg.defaultGroupID && !includeDefault {
		return false
	}
	if cfg.pinnedGroups[group.groupID] || cfg.partitionedGroups[group.groupID] {
		return false
	}
	if until := groupCooldownUntil[group.groupID]; now.Before(until) {
		return false
	}
	return group.leader.ID != "" && !group.status.PendingConfChange && group.status.LeadTransferee == 0
}

func leaderBalanceTargetCandidates(group leaderBalanceGroupSnapshot, counts map[string]int, threshold int) []raftengine.TransferTarget {
	sourceCount := counts[group.leader.ID]
	if threshold < defaultLeaderBalanceImbalanceThreshold {
		threshold = defaultLeaderBalanceImbalanceThreshold
	}
	servers := append([]raftengine.Server(nil), group.voters...)
	sort.Slice(servers, func(i, j int) bool {
		ci := counts[servers[i].ID]
		cj := counts[servers[j].ID]
		if ci != cj {
			return ci < cj
		}
		return servers[i].ID < servers[j].ID
	})
	candidates := make([]raftengine.TransferTarget, 0, len(servers))
	for _, server := range servers {
		if server.ID == group.leader.ID || server.Address == "" {
			continue
		}
		if sourceCount-counts[server.ID] < threshold {
			continue
		}
		candidates = append(candidates, raftengine.TransferTarget{
			ID:      server.ID,
			Address: server.Address,
		})
	}
	return candidates
}

func leaderBalanceMinMax(counts map[string]int) (int, int) {
	first := true
	minCount, maxCount := 0, 0
	for _, count := range counts {
		if first {
			minCount, maxCount = count, count
			first = false
			continue
		}
		if count < minCount {
			minCount = count
		}
		if count > maxCount {
			maxCount = count
		}
	}
	return minCount, maxCount
}

func runtimeForGroup(runtimes []*raftGroupRuntime, groupID uint64) *raftGroupRuntime {
	for _, rt := range runtimes {
		if rt != nil && rt.spec.id == groupID {
			return rt
		}
	}
	return nil
}

func parseLeaderBalancePinnedGroups(raw string, logger *slog.Logger) map[uint64]bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	out := make(map[uint64]bool)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, err := strconv.ParseUint(part, 10, 64)
		if err != nil {
			logger.Warn("leader-balance: skipping malformed pinned group", "group", part, "err", err)
			continue
		}
		out[id] = true
	}
	return out
}

type leaderBalanceMetrics struct {
	enabled            prometheus.Gauge
	leadersPerNode     *prometheus.GaugeVec
	unobservableGroups prometheus.Gauge
	skipped            *prometheus.CounterVec
	transfers          *prometheus.CounterVec
}

func newLeaderBalanceMetrics(registerer prometheus.Registerer) *leaderBalanceMetrics {
	if registerer == nil {
		return nil
	}
	m := &leaderBalanceMetrics{
		enabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "elastickv_leaderbalance_enabled",
			Help: "Whether the leader balance scheduler is enabled on this node.",
		}),
		leadersPerNode: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "elastickv_leaderbalance_leaders_per_node",
				Help: "Observed raft-group leader count per voter node.",
			},
			[]string{"raft_node_id"},
		),
		unobservableGroups: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "elastickv_leaderbalance_unobservable_groups",
			Help: "Number of raft groups with no observable leader in the latest leader-balance tick.",
		}),
		skipped: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_leaderbalance_skipped_total",
				Help: "Leader-balance scheduler skipped ticks by reason.",
			},
			[]string{"reason"},
		),
		transfers: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "elastickv_leaderbalance_transfers_total",
				Help: "Leader-balance transfer attempts by result.",
			},
			[]string{"result"},
		),
	}
	registerer.MustRegister(m.enabled, m.leadersPerNode, m.unobservableGroups, m.skipped, m.transfers)
	return m
}

func (m *leaderBalanceMetrics) setEnabled(enabled bool) {
	if m == nil {
		return
	}
	if enabled {
		m.enabled.Set(1)
		return
	}
	m.enabled.Set(0)
}

func (m *leaderBalanceMetrics) observe(obs leaderBalanceObservation) {
	if m == nil {
		return
	}
	m.leadersPerNode.Reset()
	ids := make([]string, 0, len(obs.counts))
	for id := range obs.counts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		m.leadersPerNode.WithLabelValues(id).Set(float64(obs.counts[id]))
	}
	m.unobservableGroups.Set(float64(obs.partialCount))
}

func (m *leaderBalanceMetrics) observeSkip(reason string) {
	if m == nil {
		return
	}
	m.skipped.WithLabelValues(reason).Inc()
}

func (m *leaderBalanceMetrics) observeTransfer(result string) {
	if m == nil {
		return
	}
	m.transfers.WithLabelValues(result).Inc()
}
