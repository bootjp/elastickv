package main

import (
	"context"
	"flag"
	"log/slog"
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
	autoSplit                  = flag.Bool("autoSplit", false, "Enable automatic same-group hotspot range splits on the catalog leader")
	autoSplitEvalInterval      = flag.Duration("autoSplitEvalInterval", autosplit.DefaultEvalInterval, "Interval between automatic hotspot split evaluations")
	autoSplitSplitCooldown     = flag.Duration("autoSplitSplitCooldown", autosplit.DefaultSplitCooldown, "Minimum wall-clock cooldown before a route created by SplitRange can be auto-split again")
	autoSplitSplitTimeout      = flag.Duration("autoSplitSplitTimeout", autosplit.DefaultSplitTimeout, "Timeout for each automatic SplitRange RPC")
	autoSplitKillSwitchFile    = flag.String("autoSplitKillSwitchFile", "", "If non-empty and the file exists, the auto-split scheduler observes but skips new splits")
	autoSplitDefaultBuckets    = flag.Int("autoSplitDefaultBuckets", autosplit.DefaultSamplerBuckets, "KeyViz buckets per route implied by --autoSplit when --keyvizKeyBucketsPerRoute was not explicitly set")
	autoSplitWriteWeight       = flag.Float64("autoSplitWriteWeight", autosplit.DefaultConfig().WriteWeight, "Weighted ops/min multiplier for writes in automatic hotspot detection")
	autoSplitReadWeight        = flag.Float64("autoSplitReadWeight", autosplit.DefaultConfig().ReadWeight, "Weighted ops/min multiplier for reads in automatic hotspot detection")
	autoSplitThresholdOpsMin   = flag.Float64("autoSplitThresholdOpsMin", autosplit.DefaultConfig().ThresholdOpsMin, "Weighted ops/min threshold per committed KeyViz column for automatic hotspot detection")
	autoSplitCandidateWindows  = flag.Int("autoSplitCandidateWindows", autosplit.DefaultConfig().CandidateWindows, "Consecutive committed KeyViz columns over threshold required before an automatic split")
	autoSplitMaxRoutes         = flag.Int("autoSplitMaxRoutes", autosplit.DefaultConfig().MaxRoutes, "Maximum live routes allowed after automatic splits")
	autoSplitMaxSplitsPerCycle = flag.Int("autoSplitMaxSplitsPerCycle", autosplit.DefaultConfig().MaxSplitsPerCycle, "Maximum automatic split decisions scheduled per evaluation cycle")

	keyvizKeyBucketsPerRouteExplicit bool
)

type autoSplitLeader interface {
	IsLeader() bool
}

type autoSplitKeyLeader interface {
	IsLeaderForKey(key []byte) bool
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
			WriteWeight:       *autoSplitWriteWeight,
			ReadWeight:        *autoSplitReadWeight,
			ThresholdOpsMin:   *autoSplitThresholdOpsMin,
			CandidateWindows:  *autoSplitCandidateWindows,
			MaxRoutes:         *autoSplitMaxRoutes,
			MaxSplitsPerCycle: *autoSplitMaxSplitsPerCycle,
		},
	}
	if leader != nil {
		cfg.IsLeader = autoSplitLeaderGate(leader)
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

func validateAutoSplitConfig(cfg autosplit.SchedulerConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if *autoSplitDefaultBuckets <= keyviz.DefaultKeyBucketsPerRoute {
		return errors.New("--autoSplitDefaultBuckets must be greater than 1")
	}
	if *autoSplitDefaultBuckets > keyviz.MaxKeyBucketsPerRoute {
		return errors.Errorf("--autoSplitDefaultBuckets (%d) must be <= %d", *autoSplitDefaultBuckets, keyviz.MaxKeyBucketsPerRoute)
	}
	if cfg.Detector.MaxRoutes > *keyvizMaxTrackedRoutes {
		return errors.Errorf("--autoSplitMaxRoutes (%d) must be <= --keyvizMaxTrackedRoutes (%d); raise keyviz capacity or lower the auto-split route cap", cfg.Detector.MaxRoutes, *keyvizMaxTrackedRoutes)
	}
	if cfg.EvalInterval <= 0 {
		return errors.New("--autoSplitEvalInterval must be positive")
	}
	if cfg.SplitCooldown <= 0 {
		return errors.New("--autoSplitSplitCooldown must be positive")
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
		cfg.IsLeader = autoSplitLeaderGate(leader)
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
	return autosplit.SplitResult{CatalogVersion: resp.GetCatalogVersion()}, nil
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
