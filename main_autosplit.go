package main

import (
	"context"
	"flag"
	"log/slog"
	"math"
	"strings"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

var (
	autoSplit                    = flag.Bool("autoSplit", false, "Enable automatic same-group hotspot range splits on the catalog leader")
	autoSplitEvalInterval        = flag.Duration("autoSplitEvalInterval", autosplit.DefaultEvalInterval, "Interval between automatic hotspot split evaluations")
	autoSplitSplitCooldown       = flag.Duration("autoSplitCooldown", autosplit.DefaultSplitCooldown, "Minimum cooldown before a route created by SplitRange can be auto-split again")
	autoSplitSplitTimeout        = flag.Duration("autoSplitSplitTimeout", autosplit.DefaultSplitTimeout, "Timeout for each automatic SplitRange RPC")
	autoSplitKillSwitchFile      = flag.String("autoSplitKillSwitchFile", "", "If non-empty and the file exists, the auto-split scheduler observes but skips new splits")
	autoSplitDefaultBuckets      = flag.Int("autoSplitDefaultBuckets", autosplit.DefaultSamplerBuckets, "KeyViz buckets per route implied by --autoSplit when --keyvizKeyBucketsPerRoute was not explicitly set")
	autoSplitWriteWeight         = flag.Float64("autoSplitWriteWeight", autosplit.DefaultConfig().WriteWeight, "Weighted ops/min multiplier for writes in automatic hotspot detection")
	autoSplitReadWeight          = flag.Float64("autoSplitReadWeight", autosplit.DefaultConfig().ReadWeight, "Weighted ops/min multiplier for reads in automatic hotspot detection")
	autoSplitThresholdOpsMin     = flag.Float64("autoSplitThreshold", autosplit.DefaultConfig().ThresholdOpsMin, "Weighted ops/min threshold per committed KeyViz column for automatic hotspot detection")
	autoSplitCandidateWindows    = flag.Int("autoSplitCandidateWindows", autosplit.DefaultConfig().CandidateWindows, "Consecutive committed KeyViz columns over threshold required before an automatic split")
	autoSplitMaxRoutes           = flag.Int("autoSplitMaxRoutes", autosplit.DefaultConfig().MaxRoutes, "Maximum live routes allowed after automatic splits")
	autoSplitMaxSplitsPerCycle   = flag.Int("autoSplitMaxPerCycle", autosplit.DefaultConfig().MaxSplitsPerCycle, "Maximum automatic split decisions scheduled per evaluation cycle")
	autoSplitTopKeyShare         = flag.Float64("autoSplitTopKeyShare", autosplit.DefaultConfig().TopKeyShare, "Minimum lower-bound share of route writes required for hot-key isolation")
	autoSplitTopKeyAbsoluteFloor = flag.Float64("autoSplitTopKeyAbsoluteFloor", 0, "Minimum lower-bound hot-key weighted ops/min; zero uses half --autoSplitThreshold")

	keyvizKeyBucketsPerRouteExplicit bool
)

type autoSplitLeader interface {
	IsLeader() bool
}

type autoSplitKeyLeader interface {
	IsLeaderForKey(key []byte) bool
}

type autoSplitTermLeader interface {
	LeadershipForKey(key []byte) (bool, uint64)
	GroupLeadership(groupID uint64) (bool, uint64)
}

func recordExplicitRuntimeFlags(fs *flag.FlagSet) {
	if fs == nil {
		return
	}
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "keyvizKeyBucketsPerRoute" {
			keyvizKeyBucketsPerRouteExplicit = true
		}
	})
}

func effectiveKeyVizBucketsPerRoute(autoEnabled, bucketsExplicit bool, configuredBuckets, defaultBuckets int) int {
	if autoEnabled && !bucketsExplicit && configuredBuckets <= keyviz.DefaultKeyBucketsPerRoute {
		return defaultBuckets
	}
	return configuredBuckets
}

func autoSplitConfigFromFlags(leader autoSplitLeader) (autosplit.SchedulerConfig, error) {
	cfg := autosplit.SchedulerConfig{
		Enabled:        *autoSplit,
		EvalInterval:   *autoSplitEvalInterval,
		SplitCooldown:  *autoSplitSplitCooldown,
		SplitTimeout:   *autoSplitSplitTimeout,
		KillSwitchFile: strings.TrimSpace(*autoSplitKillSwitchFile),
		Logger:         slog.Default(),
		Detector: autosplit.Config{
			WriteWeight:         *autoSplitWriteWeight,
			ReadWeight:          *autoSplitReadWeight,
			ThresholdOpsMin:     *autoSplitThresholdOpsMin,
			CandidateWindows:    *autoSplitCandidateWindows,
			MaxRoutes:           *autoSplitMaxRoutes,
			MaxSplitsPerCycle:   *autoSplitMaxSplitsPerCycle,
			TopKeyShare:         *autoSplitTopKeyShare,
			TopKeyAbsoluteFloor: *autoSplitTopKeyAbsoluteFloor,
		},
	}
	if leader != nil {
		applyAutoSplitLeadership(&cfg, leader)
	}
	if err := validateAutoSplitConfig(cfg); err != nil {
		return autosplit.SchedulerConfig{}, err
	}
	return cfg, nil
}

func configureRuntimeAutoSplit(coordinate *kv.ShardedCoordinator) (autosplit.SchedulerConfig, error) {
	cfg, err := autoSplitConfigFromFlags(coordinate)
	if err != nil {
		return autosplit.SchedulerConfig{}, err
	}
	return cfg, nil
}

func setupDistributionWatcherAndAutoSplit(
	ctx context.Context,
	eg *errgroup.Group,
	catalog *distribution.CatalogStore,
	engine *distribution.Engine,
	distServer *adapter.DistributionServer,
	coordinate *kv.ShardedCoordinator,
	sampler *keyviz.MemSampler,
	observer autosplit.Observer,
) (adapter.AutoSplitRuntime, error) {
	cfg, err := configureRuntimeAutoSplit(coordinate)
	if err != nil {
		return nil, err
	}
	cfg.Observer = observer
	var runtime adapter.AutoSplitRuntime
	if cfg.Enabled {
		switchRuntime := autosplit.NewRuntimeSwitch(true)
		runtime = switchRuntime
		cfg.KillSwitch = switchRuntime.KillSwitch
	}
	var reconciler *autosplit.RouteReconciler
	if sampler != nil {
		reconciler = autosplit.NewRouteReconciler(sampler)
		initialSnapshot, snapshotErr := catalog.Snapshot(ctx)
		if snapshotErr != nil {
			return nil, errors.Wrap(snapshotErr, "load initial catalog for keyviz reconciliation")
		}
		reconciler.Reconcile(initialSnapshot.Routes)
		cfg.Reconciler = reconciler
	}
	eg.Go(func() error {
		return runDistributionCatalogWatcher(ctx, catalog, engine, func(snapshot distribution.CatalogSnapshot) {
			if reconciler != nil {
				reconciler.Reconcile(snapshot.Routes)
			}
		})
	})
	startAutoSplitScheduler(ctx, eg, catalog, distServer, coordinate, sampler, cfg)
	return runtime, nil
}

func validateAutoSplitConfig(cfg autosplit.SchedulerConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if err := validateAutoSplitSamplerConfig(cfg.Detector); err != nil {
		return err
	}
	if err := validateAutoSplitDetectorConfig(cfg.Detector); err != nil {
		return err
	}
	return validateAutoSplitSchedulerConfig(cfg)
}

func validateAutoSplitSamplerConfig(cfg autosplit.Config) error {
	if autoSplitUsesDefaultBuckets() {
		if *autoSplitDefaultBuckets <= keyviz.DefaultKeyBucketsPerRoute {
			return errors.New("--autoSplitDefaultBuckets must be greater than 1")
		}
		if *autoSplitDefaultBuckets > keyviz.MaxKeyBucketsPerRoute {
			return errors.Errorf("--autoSplitDefaultBuckets (%d) must be <= %d", *autoSplitDefaultBuckets, keyviz.MaxKeyBucketsPerRoute)
		}
	}
	if cfg.MaxRoutes > *keyvizMaxTrackedRoutes {
		return errors.Errorf("--autoSplitMaxRoutes (%d) must be <= --keyvizMaxTrackedRoutes (%d); raise keyviz capacity or lower the auto-split route cap", cfg.MaxRoutes, *keyvizMaxTrackedRoutes)
	}
	return nil
}

func autoSplitUsesDefaultBuckets() bool {
	return *autoSplit &&
		!keyvizKeyBucketsPerRouteExplicit &&
		*keyvizKeyBucketsPerRoute <= keyviz.DefaultKeyBucketsPerRoute
}

func validateAutoSplitDetectorConfig(cfg autosplit.Config) error {
	if err := validateAutoSplitWeights(cfg.WriteWeight, cfg.ReadWeight); err != nil {
		return err
	}
	if err := validateAutoSplitDetectorLimits(cfg); err != nil {
		return err
	}
	return validateAutoSplitHotKeyThresholds(cfg)
}

func validateAutoSplitDetectorLimits(cfg autosplit.Config) error {
	if !isFiniteAutoSplitFloat(cfg.ThresholdOpsMin) || cfg.ThresholdOpsMin <= 0 {
		return errors.New("--autoSplitThreshold must be positive")
	}
	if cfg.CandidateWindows <= 0 {
		return errors.New("--autoSplitCandidateWindows must be positive")
	}
	if cfg.MaxRoutes <= 0 {
		return errors.New("--autoSplitMaxRoutes must be positive")
	}
	if cfg.MaxSplitsPerCycle <= 0 {
		return errors.New("--autoSplitMaxPerCycle must be positive")
	}
	return nil
}

func validateAutoSplitHotKeyThresholds(cfg autosplit.Config) error {
	if !isFiniteAutoSplitFloat(cfg.TopKeyShare) || cfg.TopKeyShare <= 0 || cfg.TopKeyShare > 1 {
		return errors.New("--autoSplitTopKeyShare must be in (0, 1]")
	}
	if !isFiniteAutoSplitFloat(cfg.TopKeyAbsoluteFloor) || cfg.TopKeyAbsoluteFloor < 0 {
		return errors.New("--autoSplitTopKeyAbsoluteFloor must be non-negative")
	}
	return nil
}

func validateAutoSplitWeights(writeWeight, readWeight float64) error {
	if !isFiniteAutoSplitFloat(writeWeight) || !isFiniteAutoSplitFloat(readWeight) ||
		writeWeight < 0 || readWeight < 0 || (writeWeight == 0 && readWeight == 0) {
		return errors.New("--autoSplitWriteWeight and --autoSplitReadWeight must be non-negative and at least one must be positive")
	}
	return nil
}

func isFiniteAutoSplitFloat(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func validateAutoSplitSchedulerConfig(cfg autosplit.SchedulerConfig) error {
	if cfg.EvalInterval <= 0 {
		return errors.New("--autoSplitEvalInterval must be positive")
	}
	if cfg.SplitCooldown <= 0 {
		return errors.New("--autoSplitCooldown must be positive")
	}
	if cfg.SplitTimeout <= 0 {
		return errors.New("--autoSplitSplitTimeout must be positive")
	}
	return nil
}

func startAutoSplitScheduler(
	ctx context.Context,
	eg *errgroup.Group,
	catalog *distribution.CatalogStore,
	distServer *adapter.DistributionServer,
	leader autoSplitLeader,
	sampler *keyviz.MemSampler,
	cfg autosplit.SchedulerConfig,
) {
	if eg == nil || !cfg.Enabled {
		return
	}
	if sampler == nil {
		cfg.Logger.Warn("autosplit: sampler not configured; scheduler disabled")
		return
	}
	if leader != nil {
		applyAutoSplitLeadership(&cfg, leader)
	}
	scheduler := autosplit.NewScheduler(
		cfg,
		catalog,
		autoSplitDistributionSplitter{server: distServer},
		sampler,
		sampler,
	)
	eg.Go(func() error {
		return scheduler.Run(ctx)
	})
}

type autoSplitDistributionSplitter struct {
	server *adapter.DistributionServer
}

func (s autoSplitDistributionSplitter) SplitRange(ctx context.Context, req autosplit.SplitRequest) (autosplit.SplitResult, error) {
	if s.server == nil {
		return autosplit.SplitResult{}, errors.New("autosplit: distribution server is not configured")
	}
	if req.TargetGroupID != 0 {
		return autosplit.SplitResult{}, errors.New("autosplit: cross-group target selection is not enabled")
	}
	resp, err := s.server.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: req.ExpectedCatalogVersion,
		RouteId:                req.RouteID,
		SplitKey:               distribution.CloneBytes(req.SplitKey),
	})
	if err != nil {
		return autosplit.SplitResult{}, errors.Wrap(err, "autosplit: split range")
	}
	return autosplit.SplitResult{
		CatalogVersion: resp.GetCatalogVersion(),
		Left:           autoSplitRouteFromProto(resp.GetLeft()),
		Right:          autoSplitRouteFromProto(resp.GetRight()),
	}, nil
}

func autoSplitRouteFromProto(route *pb.RouteDescriptor) distribution.RouteDescriptor {
	if route == nil {
		return distribution.RouteDescriptor{}
	}
	return distribution.RouteDescriptor{
		RouteID:       route.GetRouteId(),
		Start:         distribution.CloneBytes(route.GetStart()),
		End:           distribution.CloneBytes(route.GetEnd()),
		GroupID:       route.GetRaftGroupId(),
		State:         autoSplitRouteStateFromProto(route.GetState()),
		ParentRouteID: route.GetParentRouteId(),
		SplitAtHLC:    route.GetSplitAtHlc(),
	}
}

func autoSplitRouteStateFromProto(state pb.RouteState) distribution.RouteState {
	switch state {
	case pb.RouteState_ROUTE_STATE_UNSPECIFIED:
		return distribution.RouteStateWriteFenced
	case pb.RouteState_ROUTE_STATE_ACTIVE:
		return distribution.RouteStateActive
	case pb.RouteState_ROUTE_STATE_WRITE_FENCED:
		return distribution.RouteStateWriteFenced
	case pb.RouteState_ROUTE_STATE_MIGRATING_SOURCE:
		return distribution.RouteStateMigratingSource
	case pb.RouteState_ROUTE_STATE_MIGRATING_TARGET:
		return distribution.RouteStateMigratingTarget
	default:
		return distribution.RouteStateWriteFenced
	}
}

var _ autoSplitLeader = (*kv.ShardedCoordinator)(nil)

func autoSplitLeaderGate(leader autoSplitLeader) func() bool {
	if leader == nil {
		return nil
	}
	if keyLeader, ok := leader.(autoSplitKeyLeader); ok {
		return func() bool {
			return keyLeader.IsLeaderForKey(distribution.CatalogVersionKey())
		}
	}
	return leader.IsLeader
}

func applyAutoSplitLeadership(cfg *autosplit.SchedulerConfig, leader autoSplitLeader) {
	if cfg == nil || leader == nil {
		return
	}
	cfg.IsLeader = autoSplitLeaderGate(leader)
	termLeader, ok := leader.(autoSplitTermLeader)
	if !ok {
		return
	}
	cfg.Leadership = func() (bool, uint64) {
		return termLeader.LeadershipForKey(distribution.CatalogVersionKey())
	}
	cfg.GroupLeadership = termLeader.GroupLeadership
}
