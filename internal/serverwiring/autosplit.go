package serverwiring

import (
	"context"
	"flag"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/distribution/autosplit"
	"github.com/bootjp/elastickv/keyviz"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// AutoSplitFlags is the --autoSplit* flag group that configures automatic
// same-group hotspot splits on the catalog leader.
type AutoSplitFlags struct {
	Enabled             *bool
	EvalInterval        *time.Duration
	SplitCooldown       *time.Duration
	SplitTimeout        *time.Duration
	KillSwitchFile      *string
	DefaultBuckets      *int
	WriteWeight         *float64
	ReadWeight          *float64
	ThresholdOpsMin     *float64
	CandidateWindows    *int
	MaxRoutes           *int
	MaxSplitsPerCycle   *int
	TopKeyShare         *float64
	TopKeyAbsoluteFloor *float64
}

// RegisterAutoSplitFlags registers the --autoSplit* flags on fs.
func RegisterAutoSplitFlags(fs *flag.FlagSet) *AutoSplitFlags {
	defaults := autosplit.DefaultConfig()
	return &AutoSplitFlags{
		Enabled:             fs.Bool("autoSplit", false, "Enable automatic same-group hotspot range splits on the catalog leader"),
		EvalInterval:        fs.Duration("autoSplitEvalInterval", autosplit.DefaultEvalInterval, "Interval between automatic hotspot split evaluations"),
		SplitCooldown:       fs.Duration("autoSplitCooldown", autosplit.DefaultSplitCooldown, "Minimum cooldown before a route created by SplitRange can be auto-split again"),
		SplitTimeout:        fs.Duration("autoSplitSplitTimeout", autosplit.DefaultSplitTimeout, "Timeout for each automatic SplitRange RPC"),
		KillSwitchFile:      fs.String("autoSplitKillSwitchFile", "", "If non-empty and the file exists, the auto-split scheduler observes but skips new splits"),
		DefaultBuckets:      fs.Int("autoSplitDefaultBuckets", autosplit.DefaultSamplerBuckets, "KeyViz buckets per route implied by --autoSplit when --keyvizKeyBucketsPerRoute was not explicitly set"),
		WriteWeight:         fs.Float64("autoSplitWriteWeight", defaults.WriteWeight, "Weighted ops/min multiplier for writes in automatic hotspot detection"),
		ReadWeight:          fs.Float64("autoSplitReadWeight", defaults.ReadWeight, "Weighted ops/min multiplier for reads in automatic hotspot detection"),
		ThresholdOpsMin:     fs.Float64("autoSplitThreshold", defaults.ThresholdOpsMin, "Weighted ops/min threshold per committed KeyViz column for automatic hotspot detection"),
		CandidateWindows:    fs.Int("autoSplitCandidateWindows", defaults.CandidateWindows, "Consecutive committed KeyViz columns over threshold required before an automatic split"),
		MaxRoutes:           fs.Int("autoSplitMaxRoutes", defaults.MaxRoutes, "Maximum live routes allowed after automatic splits"),
		MaxSplitsPerCycle:   fs.Int("autoSplitMaxPerCycle", defaults.MaxSplitsPerCycle, "Maximum automatic split decisions scheduled per evaluation cycle"),
		TopKeyShare:         fs.Float64("autoSplitTopKeyShare", defaults.TopKeyShare, "Minimum lower-bound share of route writes required for hot-key isolation"),
		TopKeyAbsoluteFloor: fs.Float64("autoSplitTopKeyAbsoluteFloor", 0, "Minimum lower-bound hot-key weighted ops/min; zero uses half --autoSplitThreshold"),
	}
}

// Leader is the leadership the auto-split scheduler consults. Both
// *kv.ShardedCoordinator and *kv.Coordinate implement it, along with the
// key- and term-scoped leadership the scheduler prefers when present.
type Leader interface {
	IsLeader() bool
}

type keyLeader interface {
	IsLeaderForKey(key []byte) bool
}

type termLeader interface {
	LeadershipForKey(key []byte) (bool, uint64)
	GroupLeadership(groupID uint64) (bool, uint64)
}

// SchedulerConfig builds the scheduler configuration the flags describe,
// wires leader into it when leader is non-nil, and validates it against the
// KeyViz flags the sampler is built from.
func (a *AutoSplitFlags) SchedulerConfig(leader Leader, k *KeyVizFlags) (autosplit.SchedulerConfig, error) {
	cfg := autosplit.SchedulerConfig{
		Enabled:        *a.Enabled,
		EvalInterval:   *a.EvalInterval,
		SplitCooldown:  *a.SplitCooldown,
		SplitTimeout:   *a.SplitTimeout,
		KillSwitchFile: strings.TrimSpace(*a.KillSwitchFile),
		Logger:         slog.Default(),
		Detector: autosplit.Config{
			WriteWeight:         *a.WriteWeight,
			ReadWeight:          *a.ReadWeight,
			ThresholdOpsMin:     *a.ThresholdOpsMin,
			CandidateWindows:    *a.CandidateWindows,
			MaxRoutes:           *a.MaxRoutes,
			MaxSplitsPerCycle:   *a.MaxSplitsPerCycle,
			TopKeyShare:         *a.TopKeyShare,
			TopKeyAbsoluteFloor: *a.TopKeyAbsoluteFloor,
		},
	}
	if leader != nil {
		ApplyLeadership(&cfg, leader)
	}
	if err := a.validate(cfg, k); err != nil {
		return autosplit.SchedulerConfig{}, err
	}
	return cfg, nil
}

// ApplyLeadership gates the scheduler on leadership of the catalog key when
// leader can answer per key (falling back to IsLeader), and wires term-scoped
// catalog and group leadership when leader reports terms.
func ApplyLeadership(cfg *autosplit.SchedulerConfig, leader Leader) {
	if cfg == nil || leader == nil {
		return
	}
	cfg.IsLeader = leaderGate(leader)
	terms, ok := leader.(termLeader)
	if !ok {
		return
	}
	cfg.Leadership = func() (bool, uint64) {
		return terms.LeadershipForKey(distribution.CatalogVersionKey())
	}
	cfg.GroupLeadership = terms.GroupLeadership
}

func leaderGate(leader Leader) func() bool {
	if leader == nil {
		return nil
	}
	if byKey, ok := leader.(keyLeader); ok {
		return func() bool {
			return byKey.IsLeaderForKey(distribution.CatalogVersionKey())
		}
	}
	return leader.IsLeader
}

func (a *AutoSplitFlags) validate(cfg autosplit.SchedulerConfig, k *KeyVizFlags) error {
	if !cfg.Enabled {
		return nil
	}
	if err := a.validateSampler(cfg.Detector, k); err != nil {
		return err
	}
	if err := validateDetectorConfig(cfg.Detector); err != nil {
		return err
	}
	return validateSchedulerTimings(cfg)
}

// validateSampler checks the KeyViz capacity auto-split depends on.
func (a *AutoSplitFlags) validateSampler(cfg autosplit.Config, k *KeyVizFlags) error {
	if k.autoSplitUsesDefaultBuckets(a) {
		if *a.DefaultBuckets <= keyviz.DefaultKeyBucketsPerRoute {
			return errors.New("--autoSplitDefaultBuckets must be greater than 1")
		}
		if *a.DefaultBuckets > keyviz.MaxKeyBucketsPerRoute {
			return errors.Errorf("--autoSplitDefaultBuckets (%d) must be <= %d", *a.DefaultBuckets, keyviz.MaxKeyBucketsPerRoute)
		}
	}
	if cfg.MaxRoutes > *k.MaxTrackedRoutes {
		return errors.Errorf("--autoSplitMaxRoutes (%d) must be <= --keyvizMaxTrackedRoutes (%d); raise keyviz capacity or lower the auto-split route cap", cfg.MaxRoutes, *k.MaxTrackedRoutes)
	}
	return nil
}

func validateDetectorConfig(cfg autosplit.Config) error {
	if err := validateWeights(cfg.WriteWeight, cfg.ReadWeight); err != nil {
		return err
	}
	if err := validateDetectorLimits(cfg); err != nil {
		return err
	}
	return validateHotKeyThresholds(cfg)
}

func validateDetectorLimits(cfg autosplit.Config) error {
	if !isFinite(cfg.ThresholdOpsMin) || cfg.ThresholdOpsMin <= 0 {
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

func validateHotKeyThresholds(cfg autosplit.Config) error {
	if !isFinite(cfg.TopKeyShare) || cfg.TopKeyShare <= 0 || cfg.TopKeyShare > 1 {
		return errors.New("--autoSplitTopKeyShare must be in (0, 1]")
	}
	if !isFinite(cfg.TopKeyAbsoluteFloor) || cfg.TopKeyAbsoluteFloor < 0 {
		return errors.New("--autoSplitTopKeyAbsoluteFloor must be non-negative")
	}
	return nil
}

func validateWeights(writeWeight, readWeight float64) error {
	if !isFinite(writeWeight) || !isFinite(readWeight) ||
		writeWeight < 0 || readWeight < 0 || (writeWeight == 0 && readWeight == 0) {
		return errors.New("--autoSplitWriteWeight and --autoSplitReadWeight must be non-negative and at least one must be positive")
	}
	return nil
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func validateSchedulerTimings(cfg autosplit.SchedulerConfig) error {
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

// StartScheduler runs the auto-split scheduler in eg when cfg is enabled.
// Without a sampler there is nothing to evaluate, so it logs and returns.
func StartScheduler(
	ctx context.Context,
	eg *errgroup.Group,
	catalog *distribution.CatalogStore,
	distServer *adapter.DistributionServer,
	leader Leader,
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
		ApplyLeadership(&cfg, leader)
	}
	scheduler := autosplit.NewScheduler(cfg, catalog, DistributionSplitter{Server: distServer}, sampler, sampler)
	eg.Go(func() error {
		return scheduler.Run(ctx)
	})
}

// DistributionSplitter executes auto-split decisions through the local
// distribution server's SplitRange.
type DistributionSplitter struct {
	Server *adapter.DistributionServer
}

// SplitRange implements autosplit.Splitter. Only same-group splits are
// supported.
func (s DistributionSplitter) SplitRange(ctx context.Context, req autosplit.SplitRequest) (autosplit.SplitResult, error) {
	if s.Server == nil {
		return autosplit.SplitResult{}, errors.New("autosplit: distribution server is not configured")
	}
	if req.TargetGroupID != 0 {
		return autosplit.SplitResult{}, errors.New("autosplit: cross-group target selection is not enabled")
	}
	resp, err := s.Server.SplitRange(ctx, &pb.SplitRangeRequest{
		ExpectedCatalogVersion: req.ExpectedCatalogVersion,
		RouteId:                req.RouteID,
		SplitKey:               distribution.CloneBytes(req.SplitKey),
	})
	if err != nil {
		return autosplit.SplitResult{}, errors.Wrap(err, "autosplit: split range")
	}
	return splitResultFromProto(resp)
}

// splitResultFromProto converts the children with the catalog watcher's
// converter, so every route field (including the migration fields SplitRange
// copies from the parent) survives.
func splitResultFromProto(resp *pb.SplitRangeResponse) (autosplit.SplitResult, error) {
	left, err := distribution.RouteDescriptorFromProto(resp.GetLeft())
	if err != nil {
		return autosplit.SplitResult{}, errors.Wrap(err, "autosplit: decode left child")
	}
	right, err := distribution.RouteDescriptorFromProto(resp.GetRight())
	if err != nil {
		return autosplit.SplitResult{}, errors.Wrap(err, "autosplit: decode right child")
	}
	return autosplit.SplitResult{CatalogVersion: resp.GetCatalogVersion(), Left: left, Right: right}, nil
}
