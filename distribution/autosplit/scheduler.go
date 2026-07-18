package autosplit

import (
	"context"
	"encoding/hex"
	"log/slog"
	"math"
	"os"
	"sort"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const (
	DefaultEvalInterval         = keyviz.DefaultStep
	DefaultSplitCooldown        = 10 * time.Minute
	DefaultSplitTimeout         = 5 * time.Second
	DefaultSamplerBuckets       = 16
	defaultSnapshotLookbackCols = 6
	minCommittedWindowColumns   = 2
	snapshotLookbackPaddingCols = 2
	maxLoggedSplitKeyBytes      = 64
)

// SnapshotSource supplies the latest route catalog snapshot used for one
// detector/scheduler cycle.
type SnapshotSource interface {
	Snapshot(ctx context.Context) (distribution.CatalogSnapshot, error)
}

// Splitter executes catalog mutations. Production wiring calls
// proto.Distribution.SplitRange through the local DistributionServer.
type Splitter interface {
	SplitRange(ctx context.Context, req SplitRequest) (SplitResult, error)
}

// SplitRequest is the scheduler's stable request surface. TargetGroupID is
// carried for the post-M2 hook; M3 standalone always sends zero.
type SplitRequest struct {
	ExpectedCatalogVersion uint64
	RouteID                uint64
	SplitKey               []byte
	TargetGroupID          uint64
}

// SplitResult contains the committed catalog version when SplitRange succeeds.
type SplitResult struct {
	CatalogVersion uint64
}

// MatrixSampler is the keyviz surface the scheduler consumes.
type MatrixSampler interface {
	Snapshot(from, to time.Time) []keyviz.MatrixColumn
	Step() time.Duration
}

type historyColumnser interface {
	HistoryColumns() int
}

// RouteRegistrar mirrors keyviz.MemSampler's route-membership methods.
type RouteRegistrar interface {
	RegisterRoute(routeID uint64, start, end []byte, groupID uint64) bool
	RemoveRoute(routeID uint64)
}

// KillSwitch reports whether the current cycle must observe only and skip new
// SplitRange calls.
type KillSwitch func(ctx context.Context) bool

// SchedulerConfig controls background auto-split execution.
type SchedulerConfig struct {
	Enabled        bool
	Detector       Config
	EvalInterval   time.Duration
	SplitCooldown  time.Duration
	SplitTimeout   time.Duration
	KillSwitchFile string
	KillSwitch     KillSwitch
	IsLeader       func() bool
	Logger         *slog.Logger
}

// SchedulerResult describes one scheduler tick.
type SchedulerResult struct {
	CatalogVersion uint64
	Detector       Result
	Scheduled      int
	Failed         int
	KillSwitch     bool
	Leader         bool
}

// Scheduler runs the pure detector and commits accepted decisions through
// SplitRange.
type Scheduler struct {
	cfg              SchedulerConfig
	source           SnapshotSource
	splitter         Splitter
	sampler          MatrixSampler
	registrar        RouteRegistrar
	state            *DetectorState
	registeredRoutes map[uint64]struct{}
	wasLeader        bool
	leaderStartedAt  time.Time
	leaderWatermark  time.Time
}

// NewScheduler builds a leader-local scheduler. It is inert when cfg.Enabled is
// false; callers may still construct it in tests.
func NewScheduler(cfg SchedulerConfig, source SnapshotSource, splitter Splitter, sampler MatrixSampler, registrar RouteRegistrar) *Scheduler {
	cfg = cfg.withDefaults()
	return &Scheduler{
		cfg:              cfg,
		source:           source,
		splitter:         splitter,
		sampler:          sampler,
		registrar:        registrar,
		state:            NewDetectorState(),
		registeredRoutes: make(map[uint64]struct{}),
	}
}

// Run ticks until ctx is canceled. Per-cycle errors are logged and retried; a
// detector cycle is best-effort and must not tear down the data plane.
func (s *Scheduler) Run(ctx context.Context) error {
	if s == nil || !s.cfg.Enabled {
		return nil
	}
	ticker := time.NewTicker(s.cfg.EvalInterval)
	defer ticker.Stop()
	s.tickAndLog(ctx, time.Now())
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			s.tickAndLog(ctx, now)
		}
	}
}

// Tick executes one scheduler cycle. Tests call this directly.
func (s *Scheduler) Tick(ctx context.Context, now time.Time) (SchedulerResult, error) {
	out := SchedulerResult{}
	if s == nil || !s.cfg.Enabled {
		return out, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if !s.isLeader() {
		s.wasLeader = false
		return out, nil
	}
	out.Leader = true
	if !s.wasLeader {
		s.resetForLeadership(now)
		s.wasLeader = true
	}

	snapshot, err := s.source.Snapshot(ctx)
	if err != nil {
		return out, errors.Wrap(err, "autosplit: load catalog snapshot")
	}
	out.CatalogVersion = snapshot.Version
	s.reconcileSamplerRoutes(snapshot.Routes)
	SeedCooldownsFromRoutes(s.state, snapshot.Routes, s.cfg.SplitCooldown, now)

	windows := s.committedWindows(snapshot.Routes, now)
	out.Detector = Evaluate(s.cfg.Detector, s.state, Input{
		Routes:  snapshot.Routes,
		Windows: windows,
		Now:     now,
	})
	for _, event := range out.Detector.Events {
		s.logEvent(event)
	}

	if s.killSwitchActive(ctx) {
		out.KillSwitch = true
		if len(out.Detector.Decisions) > 0 {
			s.cfg.Logger.InfoContext(ctx, "autosplit: kill switch active; skipping splits",
				slog.Int("decisions", len(out.Detector.Decisions)))
		}
		return out, nil
	}

	out.Scheduled, out.Failed = s.executeDecisions(ctx, snapshot.Version, out.Detector.Decisions)
	return out, nil
}

func (s *Scheduler) tickAndLog(ctx context.Context, now time.Time) {
	if _, err := s.Tick(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
		s.cfg.Logger.WarnContext(ctx, "autosplit: cycle failed", slog.Any("err", err))
	}
}

func (s *Scheduler) executeDecision(ctx context.Context, catalogVersion uint64, decision Decision) error {
	execCtx, cancel := context.WithTimeout(ctx, s.cfg.SplitTimeout)
	defer cancel()

	s.cfg.Logger.InfoContext(ctx, "autosplit: scheduling split",
		slog.Uint64("route_id", decision.RouteID),
		slog.Uint64("catalog_version", catalogVersion),
		slog.String("split_key", loggedSplitKey(decision.SplitKey)),
		slog.String("split_origin", string(decision.SplitOrigin)),
		slog.Float64("score", decision.ScoreOpsMin),
		slog.Int("consecutive_over", decision.ConsecutiveOver),
		slog.Uint64("target_group_id", decision.TargetGroupID))

	result, err := s.splitter.SplitRange(execCtx, SplitRequest{
		ExpectedCatalogVersion: catalogVersion,
		RouteID:                decision.RouteID,
		SplitKey:               distribution.CloneBytes(decision.SplitKey),
		TargetGroupID:          decision.TargetGroupID,
	})
	if err != nil {
		return errors.Wrap(err, "autosplit: split range")
	}
	s.cfg.Logger.InfoContext(ctx, "autosplit: split committed",
		slog.Uint64("route_id", decision.RouteID),
		slog.Uint64("catalog_version", result.CatalogVersion),
		slog.String("split_key", loggedSplitKey(decision.SplitKey)),
		slog.String("split_origin", string(decision.SplitOrigin)))
	return nil
}

func (s *Scheduler) executeDecisions(ctx context.Context, catalogVersion uint64, decisions []Decision) (int, int) {
	scheduled := 0
	failed := 0
	for _, decision := range decisions {
		err := s.executeDecision(ctx, catalogVersion, decision)
		if err != nil {
			failed++
			s.cfg.Logger.WarnContext(ctx, "autosplit: split failed",
				slog.Uint64("route_id", decision.RouteID),
				slog.Uint64("catalog_version", catalogVersion),
				slog.String("split_key", loggedSplitKey(decision.SplitKey)),
				slog.String("split_origin", string(decision.SplitOrigin)),
				slog.Uint64("target_group_id", decision.TargetGroupID),
				slog.Any("err", err))
			continue
		}
		scheduled++
	}
	return scheduled, failed
}

func (s *Scheduler) committedWindows(routes []distribution.RouteDescriptor, now time.Time) []ColumnWindow {
	if s.sampler == nil {
		return nil
	}
	step := s.sampler.Step()
	if step <= 0 {
		step = keyviz.DefaultStep
	}
	from := s.snapshotStart(routes, now, step)
	cols := s.sampler.Snapshot(from, now.Add(time.Nanosecond))
	windows := CommittedWindowsFromColumns(cols)
	if s.leaderWatermark.IsZero() && s.leaderStartedAt.IsZero() {
		return windows
	}
	filtered := windows[:0]
	for _, window := range windows {
		if !s.leaderWatermark.IsZero() && !window.Column.At.After(s.leaderWatermark) {
			continue
		}
		windowStart := window.Column.At.Add(-window.Duration)
		if !s.leaderStartedAt.IsZero() && windowStart.Before(s.leaderStartedAt) {
			continue
		}
		filtered = append(filtered, window)
	}
	return filtered
}

func (s *Scheduler) snapshotStart(routes []distribution.RouteDescriptor, now time.Time, step time.Duration) time.Time {
	oldest := time.Time{}
	for _, route := range routes {
		status := s.state.RouteStatus(route.RouteID)
		if status.LastProcessedAt.IsZero() {
			continue
		}
		if oldest.IsZero() || status.LastProcessedAt.Before(oldest) {
			oldest = status.LastProcessedAt
		}
	}
	if !oldest.IsZero() {
		return oldest.Add(-step)
	}
	cols := defaultSnapshotLookbackCols
	if s.cfg.Detector.CandidateWindows > 0 {
		cols = s.cfg.Detector.CandidateWindows + snapshotLookbackPaddingCols
	}
	if h, ok := s.sampler.(historyColumnser); ok && h.HistoryColumns() > cols {
		cols = h.HistoryColumns()
	}
	return now.Add(-time.Duration(cols+1) * step)
}

func (s *Scheduler) reconcileSamplerRoutes(routes []distribution.RouteDescriptor) {
	if s.registrar == nil {
		return
	}
	live := make(map[uint64]struct{}, len(routes))
	for _, route := range routes {
		live[route.RouteID] = struct{}{}
		if _, ok := s.registeredRoutes[route.RouteID]; ok {
			continue
		}
		s.registrar.RegisterRoute(route.RouteID, route.Start, route.End, route.GroupID)
		s.registeredRoutes[route.RouteID] = struct{}{}
	}
	for routeID := range s.registeredRoutes {
		if _, ok := live[routeID]; ok {
			continue
		}
		s.registrar.RemoveRoute(routeID)
		delete(s.registeredRoutes, routeID)
	}
}

func (s *Scheduler) resetForLeadership(now time.Time) {
	s.state = NewDetectorState()
	s.leaderStartedAt = now
	s.leaderWatermark = s.freshestColumnAt(now)
	s.cfg.Logger.Info("autosplit: leadership acquired; detector state reset",
		slog.Time("leader_started_at", s.leaderStartedAt),
		slog.Time("processed_watermark", s.leaderWatermark))
}

func (s *Scheduler) freshestColumnAt(now time.Time) time.Time {
	if s.sampler == nil {
		return time.Time{}
	}
	step := s.sampler.Step()
	if step <= 0 {
		step = keyviz.DefaultStep
	}
	from := now.Add(-time.Duration(defaultSnapshotLookbackCols) * step)
	if h, ok := s.sampler.(historyColumnser); ok && h.HistoryColumns() > 0 {
		from = now.Add(-time.Duration(h.HistoryColumns()+1) * step)
	}
	cols := s.sampler.Snapshot(from, now.Add(time.Nanosecond))
	if len(cols) == 0 {
		return time.Time{}
	}
	sort.SliceStable(cols, func(i, j int) bool {
		return cols[i].At.Before(cols[j].At)
	})
	return cols[len(cols)-1].At
}

func (s *Scheduler) isLeader() bool {
	if s.cfg.IsLeader == nil {
		return true
	}
	return s.cfg.IsLeader()
}

func (s *Scheduler) killSwitchActive(ctx context.Context) bool {
	if s.cfg.KillSwitch != nil && s.cfg.KillSwitch(ctx) {
		return true
	}
	if s.cfg.KillSwitchFile == "" {
		return false
	}
	_, err := os.Stat(s.cfg.KillSwitchFile)
	if err == nil {
		return true
	}
	if errors.Is(err, os.ErrNotExist) {
		return false
	}
	s.cfg.Logger.WarnContext(ctx, "autosplit: kill switch stat failed; skipping splits",
		slog.String("path", s.cfg.KillSwitchFile),
		slog.Any("err", err))
	return true
}

func (s *Scheduler) logEvent(event Event) {
	if event.Reason == "" {
		return
	}
	args := []any{
		slog.String("reason", string(event.Reason)),
	}
	if event.RouteID != 0 {
		args = append(args, slog.Uint64("route_id", event.RouteID))
	}
	if !event.At.IsZero() {
		args = append(args, slog.Time("at", event.At))
	}
	s.cfg.Logger.Debug("autosplit: detector skip", args...)
}

func (cfg SchedulerConfig) withDefaults() SchedulerConfig {
	cfg.Detector = cfg.Detector.withDefaults()
	if cfg.EvalInterval <= 0 {
		cfg.EvalInterval = DefaultEvalInterval
	}
	if cfg.SplitCooldown <= 0 {
		cfg.SplitCooldown = DefaultSplitCooldown
	}
	if cfg.SplitTimeout <= 0 {
		cfg.SplitTimeout = DefaultSplitTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return cfg
}

// CommittedWindowsFromColumns derives committed window durations from adjacent
// flushed keyviz columns. The first column is history only because its lower
// boundary is not proven by MatrixColumn itself.
func CommittedWindowsFromColumns(cols []keyviz.MatrixColumn) []ColumnWindow {
	if len(cols) < minCommittedWindowColumns {
		return nil
	}
	cols = append([]keyviz.MatrixColumn(nil), cols...)
	sort.SliceStable(cols, func(i, j int) bool {
		return cols[i].At.Before(cols[j].At)
	})
	windows := make([]ColumnWindow, 0, len(cols)-1)
	for i := 1; i < len(cols); i++ {
		duration := cols[i].At.Sub(cols[i-1].At)
		if duration <= 0 {
			continue
		}
		windows = append(windows, ColumnWindow{
			Column:   cols[i],
			Duration: duration,
		})
	}
	return windows
}

// SeedCooldownsFromRoutes rebuilds leader-local cooldown state from durable
// child-route lineage. It uses only the HLC physical millis component.
func SeedCooldownsFromRoutes(state *DetectorState, routes []distribution.RouteDescriptor, cooldown time.Duration, now time.Time) {
	if state == nil || cooldown <= 0 {
		return
	}
	for _, route := range routes {
		until := CooldownUntilFromSplitAtHLC(route.SplitAtHLC, cooldown, now)
		if until.IsZero() {
			continue
		}
		status := state.RouteStatus(route.RouteID)
		if until.After(status.CooldownUntil) {
			state.SetCooldown(route.RouteID, until)
		}
	}
}

// CooldownUntilFromSplitAtHLC returns a monotonic enforcement deadline derived
// from SplitAtHLC's physical millis. A zero result means no remaining cooldown.
func CooldownUntilFromSplitAtHLC(splitAtHLC uint64, cooldown time.Duration, now time.Time) time.Time {
	if splitAtHLC == 0 || cooldown <= 0 {
		return time.Time{}
	}
	remaining := RemainingCooldownFromSplitAtHLC(splitAtHLC, cooldown, now)
	if remaining <= 0 {
		return time.Time{}
	}
	return now.Add(remaining)
}

// RemainingCooldownFromSplitAtHLC exposes the arithmetic for regression tests.
func RemainingCooldownFromSplitAtHLC(splitAtHLC uint64, cooldown time.Duration, now time.Time) time.Duration {
	if splitAtHLC == 0 || cooldown <= 0 {
		return 0
	}
	splitMs := HLCPhysicalMillis(splitAtHLC)
	elapsedMs := now.UnixMilli() - splitMs
	if elapsedMs < 0 {
		elapsedMs = 0
	}
	remaining := cooldown - time.Duration(elapsedMs)*time.Millisecond
	if remaining < 0 {
		return 0
	}
	return remaining
}

// HLCPhysicalMillis extracts the physical Unix-ms half of a packed HLC value.
func HLCPhysicalMillis(v uint64) int64 {
	physical := v >> kv.HLCLogicalBits
	if physical > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(physical)
}

func loggedSplitKey(key []byte) string {
	if len(key) <= maxLoggedSplitKeyBytes {
		return hex.EncodeToString(key)
	}
	return hex.EncodeToString(key[:maxLoggedSplitKeyBytes]) + "..."
}
