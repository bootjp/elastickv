package autosplit

import (
	"bytes"
	"context"
	"encoding/hex"
	"log/slog"
	"math"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	DefaultEvalInterval         = keyviz.DefaultStep
	DefaultSplitCooldown        = 10 * time.Minute
	DefaultSplitTimeout         = 5 * time.Second
	DefaultSamplerBuckets       = 16
	defaultSnapshotLookbackCols = 6
	snapshotLookbackPaddingCols = 2
	maxLoggedSplitKeyBytes      = 64
)

// CatalogSnapshotSource supplies the latest route catalog snapshot used for one
// detector/scheduler cycle.
type CatalogSnapshotSource interface {
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
	ParentStart            []byte
	ParentEnd              []byte
	ParentGroupID          uint64
}

// SplitResult contains the committed catalog version and children when
// SplitRange succeeds.
type SplitResult struct {
	CatalogVersion uint64
	Left           distribution.RouteDescriptor
	Right          distribution.RouteDescriptor
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

// LeadershipSnapshot reports whether this node owns the catalog key and the
// current Raft term for that group.
type LeadershipSnapshot func() (bool, uint64)

// GroupLeadershipSnapshot reports local leadership and term for one shard
// group.
type GroupLeadershipSnapshot func(groupID uint64) (bool, uint64)

// SchedulerConfig controls background auto-split execution.
type SchedulerConfig struct {
	Enabled         bool
	Detector        Config
	EvalInterval    time.Duration
	SplitCooldown   time.Duration
	SplitTimeout    time.Duration
	KillSwitchFile  string
	KillSwitch      KillSwitch
	IsLeader        func() bool
	Leadership      LeadershipSnapshot
	GroupLeadership GroupLeadershipSnapshot
	Logger          *slog.Logger
	Reconciler      *RouteReconciler
	Observer        Observer
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
	source           CatalogSnapshotSource
	splitter         Splitter
	sampler          MatrixSampler
	reconciler       *RouteReconciler
	state            *DetectorState
	wasLeader        bool
	leaderTerm       uint64
	leaderStartedAt  time.Time
	leaderWatermark  time.Time
	catalogVersion   uint64
	pendingCompounds map[uint64]pendingCompound
	groupLeadership  map[uint64]groupLeadershipState
}

type groupLeadershipState struct {
	leader    bool
	term      uint64
	startedAt time.Time
	watermark time.Time
}

type pendingCompound struct {
	parentRouteID uint64
	intermediate  distribution.RouteDescriptor
	splitKey      []byte
}

type registeredRoute struct {
	start   []byte
	end     []byte
	groupID uint64
}

// RouteReconciler keeps a sampler's registered route descriptors synchronized
// with catalog snapshots. It is safe for the catalog watcher and scheduler to
// share across goroutines.
type RouteReconciler struct {
	mu         sync.Mutex
	registrar  RouteRegistrar
	registered map[uint64]registeredRoute
}

// NewRouteReconciler creates a catalog-to-sampler membership reconciler.
func NewRouteReconciler(registrar RouteRegistrar) *RouteReconciler {
	return &RouteReconciler{
		registrar:  registrar,
		registered: make(map[uint64]registeredRoute),
	}
}

// NewScheduler builds a leader-local scheduler. It is inert when cfg.Enabled is
// false; callers may still construct it in tests.
func NewScheduler(cfg SchedulerConfig, source CatalogSnapshotSource, splitter Splitter, sampler MatrixSampler, registrar RouteRegistrar) *Scheduler {
	cfg = cfg.withDefaults()
	reconciler := cfg.Reconciler
	if reconciler == nil {
		reconciler = NewRouteReconciler(registrar)
	}
	scheduler := &Scheduler{
		cfg:              cfg,
		source:           source,
		splitter:         splitter,
		sampler:          sampler,
		reconciler:       reconciler,
		state:            NewDetectorState(),
		pendingCompounds: make(map[uint64]pendingCompound),
		groupLeadership:  make(map[uint64]groupLeadershipState),
	}
	if cfg.Observer != nil {
		cfg.Observer.ObserveState(cfg.Enabled, 0, 0, 0)
	}
	return scheduler
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
	cycleStarted := time.Now()
	runtimeEnabled := false
	defer func() {
		if s != nil && s.cfg.Observer != nil {
			tracked, cooldown := s.stateCounts(now)
			s.cfg.Observer.ObserveState(runtimeEnabled, tracked, cooldown, time.Since(cycleStarted))
		}
	}()
	out := SchedulerResult{}
	if s == nil || !s.cfg.Enabled {
		return out, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	runtimeEnabled = !s.killSwitchActive(ctx)
	if !s.ensureLeadership(now) {
		return out, nil
	}
	prep, err := s.prepareTick(ctx, now, runtimeEnabled)
	if err != nil {
		return out, err
	}
	prep.result.Detector = s.evaluateTick(now, prep)
	return s.finishTick(ctx, runtimeEnabled, prep), nil
}

type tickPreparation struct {
	snapshot         distribution.CatalogSnapshot
	routes           []distribution.RouteDescriptor
	fences           map[uint64]EvidenceFence
	usedBudget       int
	catalogGap       bool
	stopAfterPrepare bool
	result           SchedulerResult
}

func (s *Scheduler) ensureLeadership(now time.Time) bool {
	leader, leaderTerm := s.leadership()
	if !leader {
		s.wasLeader = false
		return false
	}
	if !s.wasLeader || (leaderTerm != 0 && leaderTerm != s.leaderTerm) {
		s.resetForLeadership(now)
		s.wasLeader = true
	}
	s.leaderTerm = leaderTerm
	return true
}

func (s *Scheduler) prepareTick(
	ctx context.Context,
	now time.Time,
	runtimeEnabled bool,
) (tickPreparation, error) {
	snapshot, err := s.source.Snapshot(ctx)
	if err != nil {
		return tickPreparation{}, errors.Wrap(err, "autosplit: load catalog snapshot")
	}
	prep := tickPreparation{
		snapshot:   snapshot,
		catalogGap: s.catalogVersion != 0 && snapshot.Version != s.catalogVersion,
		result: SchedulerResult{
			CatalogVersion: snapshot.Version,
			Leader:         true,
		},
	}
	if runtimeEnabled {
		var pendingScheduled, pendingFailed int
		prep.snapshot, pendingScheduled, pendingFailed, prep.usedBudget, prep.stopAfterPrepare = s.finalizePendingCompounds(ctx, snapshot)
		prep.result.CatalogVersion = prep.snapshot.Version
		prep.result.Scheduled += pendingScheduled
		prep.result.Failed += pendingFailed
		if prep.stopAfterPrepare {
			s.catalogVersion = prep.snapshot.Version
			return prep, nil
		}
	}
	prep.routes, prep.fences = s.syncCatalogSnapshot(prep.snapshot, now)
	s.catalogVersion = prep.snapshot.Version
	return prep, nil
}

func (s *Scheduler) syncCatalogSnapshot(
	snapshot distribution.CatalogSnapshot,
	now time.Time,
) ([]distribution.RouteDescriptor, map[uint64]EvidenceFence) {
	s.reconcileSamplerRoutes(snapshot.Routes)
	SeedCooldownsFromRoutes(s.state, snapshot.Routes, s.cfg.SplitCooldown, now)
	return s.routesLedLocally(snapshot.Routes, now)
}

func (s *Scheduler) evaluateTick(now time.Time, prep tickPreparation) Result {
	if prep.stopAfterPrepare {
		return Result{}
	}
	windows := s.committedWindows(prep.routes, now)
	s.resetConfidenceForHistoryGaps(prep.routes, windows)
	if prep.catalogGap {
		if prep.fences == nil {
			prep.fences = make(map[uint64]EvidenceFence, len(prep.routes))
		}
		s.fenceUnprovenCatalogGap(prep.routes, prep.fences, newestWindowAt(windows))
	}
	result := Evaluate(s.cfg.Detector, s.state, Input{
		Routes:         prep.routes,
		Windows:        windows,
		EvidenceFences: prep.fences,
		Now:            now,
		LiveRouteCount: len(prep.snapshot.Routes),
	})
	s.observeDetectorResult(result)
	return result
}

func (s *Scheduler) observeDetectorResult(result Result) {
	for _, event := range result.Events {
		s.logEvent(event)
		if s.cfg.Observer != nil {
			s.cfg.Observer.ObserveSkipped(event.Reason)
			s.cfg.Observer.ObserveIsolationDeclined(event.IsolationReason)
		}
	}
	if s.cfg.Observer != nil {
		s.cfg.Observer.ObserveCandidatesPromoted(result.Promoted)
	}
}

func (s *Scheduler) finishTick(
	ctx context.Context,
	runtimeEnabled bool,
	prep tickPreparation,
) SchedulerResult {
	if prep.stopAfterPrepare {
		return prep.result
	}
	if !runtimeEnabled {
		prep.result.KillSwitch = true
		if len(prep.result.Detector.Decisions) > 0 {
			s.cfg.Logger.InfoContext(ctx, "autosplit: kill switch active; skipping splits",
				slog.Int("decisions", len(prep.result.Detector.Decisions)))
		}
		return prep.result
	}

	remainingBudget := s.cfg.Detector.MaxSplitsPerCycle - prep.usedBudget
	if remainingBudget <= 0 {
		return prep.result
	}
	decisions := prep.result.Detector.Decisions
	if len(decisions) > remainingBudget {
		decisions = decisions[:remainingBudget]
	}
	scheduled, failed, catalogVersion := s.executeDecisions(ctx, prep.snapshot.Version, decisions)
	prep.result.Scheduled += scheduled
	prep.result.Failed += failed
	prep.result.CatalogVersion = catalogVersion
	s.catalogVersion = catalogVersion
	return prep.result
}

func (s *Scheduler) resetConfidenceForHistoryGaps(
	routes []distribution.RouteDescriptor,
	windows []ColumnWindow,
) {
	for _, route := range routes {
		processedThrough := s.state.RouteStatus(route.RouteID).LastProcessedAt
		if processedThrough.IsZero() {
			continue
		}
		for _, window := range windows {
			if !window.Column.At.After(processedThrough) {
				continue
			}
			windowStart := window.Column.WindowStart
			if windowStart.IsZero() {
				windowStart = window.Column.At.Add(-window.Duration)
			}
			if windowStart.After(processedThrough) {
				s.state.ResetConfidence(route.RouteID)
			}
			break
		}
	}
}

func (s *Scheduler) fenceUnprovenCatalogGap(
	routes []distribution.RouteDescriptor,
	fences map[uint64]EvidenceFence,
	processedThrough time.Time,
) {
	for _, route := range routes {
		s.state.ResetConfidence(route.RouteID)
		fence := fences[route.RouteID]
		if processedThrough.After(fence.ProcessedThrough) {
			fence.ProcessedThrough = processedThrough
		}
		fences[route.RouteID] = fence
	}
}

func (s *Scheduler) tickAndLog(ctx context.Context, now time.Time) {
	if _, err := s.Tick(ctx, now); err != nil && !errors.Is(err, context.Canceled) {
		s.cfg.Logger.WarnContext(ctx, "autosplit: cycle failed", slog.Any("err", err))
	}
}

func (s *Scheduler) executeSplit(
	ctx context.Context,
	catalogVersion uint64,
	routeID uint64,
	splitKey []byte,
	targetGroupID uint64,
	parentStart []byte,
	parentEnd []byte,
	parentGroupID uint64,
	splitOrigin SplitOrigin,
	score float64,
	consecutiveOver int,
) (SplitResult, error) {
	execCtx, cancel := context.WithTimeout(ctx, s.cfg.SplitTimeout)
	defer cancel()

	s.cfg.Logger.InfoContext(ctx, "autosplit: scheduling split",
		slog.Uint64("route_id", routeID),
		slog.Uint64("catalog_version", catalogVersion),
		slog.String("split_key", loggedSplitKey(splitKey)),
		slog.String("split_origin", string(splitOrigin)),
		slog.Float64("score", score),
		slog.Int("consecutive_over", consecutiveOver),
		slog.Uint64("target_group_id", targetGroupID))

	result, err := s.splitter.SplitRange(execCtx, SplitRequest{
		ExpectedCatalogVersion: catalogVersion,
		RouteID:                routeID,
		SplitKey:               distribution.CloneBytes(splitKey),
		TargetGroupID:          targetGroupID,
		ParentStart:            distribution.CloneBytes(parentStart),
		ParentEnd:              distribution.CloneBytes(parentEnd),
		ParentGroupID:          parentGroupID,
	})
	if s.cfg.Observer != nil {
		s.cfg.Observer.ObserveSplitScheduled()
	}
	if err != nil {
		if s.cfg.Observer != nil {
			s.cfg.Observer.ObserveSplitFailed(splitFailureReason(err, targetGroupID))
		}
		return SplitResult{}, errors.Wrap(err, "autosplit: split range")
	}
	s.cfg.Logger.InfoContext(ctx, "autosplit: split committed",
		slog.Uint64("route_id", routeID),
		slog.Uint64("catalog_version", result.CatalogVersion),
		slog.String("split_key", loggedSplitKey(splitKey)),
		slog.String("split_origin", string(splitOrigin)))
	return result, nil
}

func (s *Scheduler) executeDecisions(ctx context.Context, catalogVersion uint64, decisions []Decision) (int, int, uint64) {
	scheduled := 0
	failed := 0
	for _, decision := range decisions {
		nextCatalogVersion, err := s.executeDecision(ctx, catalogVersion, decision)
		if err != nil {
			failed++
			s.cfg.Logger.WarnContext(ctx, "autosplit: split failed",
				slog.Uint64("route_id", decision.RouteID),
				slog.Uint64("catalog_version", catalogVersion),
				slog.String("split_key", loggedSplitKey(decision.SplitKey)),
				slog.String("split_origin", string(decision.SplitOrigin)),
				slog.Uint64("target_group_id", decision.TargetGroupID),
				slog.Any("err", err))
			if nextCatalogVersion > catalogVersion {
				catalogVersion = nextCatalogVersion
			}
			continue
		}
		catalogVersion = nextCatalogVersion
		scheduled++
	}
	return scheduled, failed, catalogVersion
}

func (s *Scheduler) executeDecision(ctx context.Context, catalogVersion uint64, decision Decision) (uint64, error) {
	first, err := s.executeSplit(
		ctx,
		catalogVersion,
		decision.RouteID,
		decision.SplitKey,
		decision.TargetGroupID,
		decision.RouteStart,
		decision.RouteEnd,
		decision.RouteGroupID,
		decision.SplitOrigin,
		decision.ScoreOpsMin,
		decision.ConsecutiveOver,
	)
	if err != nil {
		return catalogVersion, err
	}
	if decision.SplitOrigin != SplitOriginIsolationCompound {
		return first.CatalogVersion, nil
	}
	if !validCompoundIntermediate(first, decision) {
		return first.CatalogVersion, errors.New("autosplit: split response did not contain the expected compound intermediate child")
	}

	pending := pendingCompound{
		parentRouteID: decision.RouteID,
		intermediate:  distribution.CloneRouteDescriptor(first.Right),
		splitKey:      distribution.CloneBytes(decision.SecondSplitKey),
	}
	s.pendingCompounds[decision.RouteID] = pending
	second, err := s.executePendingCompound(ctx, first.CatalogVersion, pending)
	if err != nil {
		if s.cfg.Observer != nil {
			s.cfg.Observer.ObserveCompoundPartial()
		}
		return first.CatalogVersion, err
	}
	delete(s.pendingCompounds, decision.RouteID)
	return second.CatalogVersion, nil
}

func validCompoundIntermediate(result SplitResult, decision Decision) bool {
	left, right := result.Left, result.Right
	if !validCompoundChildTopology(left, right, decision) {
		return false
	}
	parent := distribution.RouteDescriptor{Start: left.Start, End: right.End}
	return splitKeyInsideRoute(parent, right.Start) &&
		splitKeyInsideRoute(right, decision.SecondSplitKey)
}

func validCompoundChildTopology(left, right distribution.RouteDescriptor, decision Decision) bool {
	return left.RouteID != 0 && right.RouteID != 0 && left.RouteID != right.RouteID &&
		left.State == distribution.RouteStateActive && right.State == distribution.RouteStateActive &&
		left.GroupID == decision.RouteGroupID && right.GroupID == decision.RouteGroupID &&
		bytes.Equal(left.Start, decision.RouteStart) &&
		bytes.Equal(left.End, right.Start) &&
		bytes.Equal(right.End, decision.RouteEnd)
}

func (s *Scheduler) executePendingCompound(
	ctx context.Context,
	catalogVersion uint64,
	pending pendingCompound,
) (SplitResult, error) {
	return s.executeSplit(
		ctx,
		catalogVersion,
		pending.intermediate.RouteID,
		pending.splitKey,
		0,
		pending.intermediate.Start,
		pending.intermediate.End,
		pending.intermediate.GroupID,
		SplitOriginIsolationCompound,
		0,
		0,
	)
}

func (s *Scheduler) finalizePendingCompounds(
	ctx context.Context,
	snapshot distribution.CatalogSnapshot,
) (distribution.CatalogSnapshot, int, int, int, bool) {
	if len(s.pendingCompounds) == 0 {
		return snapshot, 0, 0, 0, false
	}
	parentIDs := make([]uint64, 0, len(s.pendingCompounds))
	for parentID := range s.pendingCompounds {
		parentIDs = append(parentIDs, parentID)
	}
	sort.Slice(parentIDs, func(i, j int) bool { return parentIDs[i] < parentIDs[j] })

	scheduled := 0
	failed := 0
	attempted := 0
	for _, parentID := range parentIDs {
		if attempted >= s.cfg.Detector.MaxSplitsPerCycle {
			break
		}
		pending := s.pendingCompounds[parentID]
		intermediate, ok := findActiveRoute(snapshot.Routes, pending.intermediate.RouteID)
		if !ok || !samePendingIntermediate(intermediate, pending) ||
			len(snapshot.Routes)+1 > s.cfg.Detector.MaxRoutes {
			delete(s.pendingCompounds, parentID)
			s.cfg.Logger.WarnContext(ctx, "autosplit: dropping invalid compound finalization",
				slog.Uint64("parent_route_id", parentID),
				slog.Uint64("intermediate_route_id", pending.intermediate.RouteID))
			continue
		}
		pending.intermediate = intermediate
		s.pendingCompounds[parentID] = pending
		attempted++
		result, err := s.executePendingCompound(ctx, snapshot.Version, pending)
		if err != nil {
			failed++
			s.cfg.Logger.WarnContext(ctx, "autosplit: compound finalization failed",
				slog.Uint64("parent_route_id", parentID),
				slog.Uint64("intermediate_route_id", pending.intermediate.RouteID),
				slog.Any("err", err))
			continue
		}
		delete(s.pendingCompounds, parentID)
		scheduled++
		snapshot.Version = result.CatalogVersion
		refreshed, refreshErr := s.source.Snapshot(ctx)
		if refreshErr != nil {
			s.cfg.Logger.WarnContext(ctx, "autosplit: refresh after compound finalization failed",
				slog.Any("err", refreshErr))
			return snapshot, scheduled, failed, attempted, true
		}
		snapshot = refreshed
	}
	return snapshot, scheduled, failed, attempted, false
}

func findActiveRoute(routes []distribution.RouteDescriptor, routeID uint64) (distribution.RouteDescriptor, bool) {
	for _, route := range routes {
		if route.RouteID == routeID && route.State == distribution.RouteStateActive {
			return route, true
		}
	}
	return distribution.RouteDescriptor{}, false
}

func samePendingIntermediate(route distribution.RouteDescriptor, pending pendingCompound) bool {
	return route.GroupID == pending.intermediate.GroupID &&
		bytes.Equal(route.Start, pending.intermediate.Start) &&
		bytes.Equal(route.End, pending.intermediate.End) &&
		splitKeyInsideRoute(route, pending.splitKey)
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
	windows, _, _ := CommittedWindowsFromColumns(cols, s.oldestProcessedAt(routes))
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

func (s *Scheduler) oldestProcessedAt(routes []distribution.RouteDescriptor) time.Time {
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
	return oldest
}

func (s *Scheduler) snapshotStart(routes []distribution.RouteDescriptor, now time.Time, step time.Duration) time.Time {
	oldest := s.oldestProcessedAt(routes)
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
	if s.reconciler == nil {
		return
	}
	s.reconciler.Reconcile(routes)
}

// Reconcile applies one complete live catalog snapshot to sampler membership.
func (r *RouteReconciler) Reconcile(routes []distribution.RouteDescriptor) {
	if r == nil || r.registrar == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	live := make(map[uint64]struct{}, len(routes))
	for _, route := range routes {
		live[route.RouteID] = struct{}{}
		registered, ok := r.registered[route.RouteID]
		next := registeredRouteFromDescriptor(route)
		if ok && registered.equal(next) {
			continue
		}
		if ok {
			r.registrar.RemoveRoute(route.RouteID)
		}
		r.registrar.RegisterRoute(route.RouteID, route.Start, route.End, route.GroupID)
		r.registered[route.RouteID] = next
	}
	for routeID := range r.registered {
		if _, ok := live[routeID]; ok {
			continue
		}
		r.registrar.RemoveRoute(routeID)
		delete(r.registered, routeID)
	}
}

func registeredRouteFromDescriptor(route distribution.RouteDescriptor) registeredRoute {
	return registeredRoute{
		start:   distribution.CloneBytes(route.Start),
		end:     distribution.CloneBytes(route.End),
		groupID: route.GroupID,
	}
}

func (r registeredRoute) equal(other registeredRoute) bool {
	return r.groupID == other.groupID &&
		bytes.Equal(r.start, other.start) &&
		bytes.Equal(r.end, other.end)
}

func (s *Scheduler) routesLedLocally(
	routes []distribution.RouteDescriptor,
	now time.Time,
) ([]distribution.RouteDescriptor, map[uint64]EvidenceFence) {
	if s.cfg.GroupLeadership == nil {
		return routes, nil
	}
	groupRoutes := make(map[uint64][]distribution.RouteDescriptor)
	for _, route := range routes {
		groupRoutes[route.GroupID] = append(groupRoutes[route.GroupID], route)
	}
	eligible := make([]distribution.RouteDescriptor, 0, len(routes))
	fences := make(map[uint64]EvidenceFence, len(routes))
	watermark := time.Time{}
	watermarkLoaded := false
	for groupID, ownedRoutes := range groupRoutes {
		state, ledLocally := s.locallyLedGroupState(groupID, now, &watermark, &watermarkLoaded)
		if !ledLocally {
			continue
		}
		for _, route := range ownedRoutes {
			eligible = append(eligible, route)
			fences[route.RouteID] = EvidenceFence{
				ProcessedThrough:     state.watermark,
				WindowStartNotBefore: state.startedAt,
			}
		}
	}
	for groupID := range s.groupLeadership {
		if _, ok := groupRoutes[groupID]; !ok {
			delete(s.groupLeadership, groupID)
			s.dropPendingCompoundsForGroup(groupID)
		}
	}
	return eligible, fences
}

func (s *Scheduler) locallyLedGroupState(
	groupID uint64,
	now time.Time,
	watermark *time.Time,
	watermarkLoaded *bool,
) (groupLeadershipState, bool) {
	leader, term := s.cfg.GroupLeadership(groupID)
	previous, known := s.groupLeadership[groupID]
	if !leader {
		previous.leader = false
		previous.term = term
		s.groupLeadership[groupID] = previous
		s.dropPendingCompoundsForGroup(groupID)
		return previous, false
	}
	transition := !known || !previous.leader || (term != 0 && term != previous.term)
	if transition {
		if !*watermarkLoaded {
			*watermark = s.freshestColumnAt(now)
			*watermarkLoaded = true
		}
		previous = groupLeadershipState{
			leader:    true,
			term:      term,
			startedAt: now,
			watermark: *watermark,
		}
		s.dropPendingCompoundsForGroup(groupID)
	} else {
		previous.leader = true
		previous.term = term
	}
	s.groupLeadership[groupID] = previous
	return previous, true
}

func (s *Scheduler) dropPendingCompoundsForGroup(groupID uint64) {
	for parentID, pending := range s.pendingCompounds {
		if pending.intermediate.GroupID == groupID {
			delete(s.pendingCompounds, parentID)
		}
	}
}

func (s *Scheduler) resetForLeadership(now time.Time) {
	s.state = NewDetectorState()
	s.pendingCompounds = make(map[uint64]pendingCompound)
	s.catalogVersion = 0
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

func (s *Scheduler) leadership() (bool, uint64) {
	if s.cfg.Leadership != nil {
		return s.cfg.Leadership()
	}
	if s.cfg.IsLeader == nil {
		return true, 0
	}
	return s.cfg.IsLeader(), 0
}

func (s *Scheduler) stateCounts(now time.Time) (int, int) {
	if s == nil || s.state == nil {
		return 0, 0
	}
	tracked := len(s.state.routes)
	cooldown := 0
	for _, route := range s.state.routes {
		if now.Before(route.CooldownUntil) {
			cooldown++
		}
	}
	return tracked, cooldown
}

func splitFailureReason(err error, targetGroupID uint64) string {
	if status.Code(err) == codes.Aborted {
		return "cas_conflict"
	}
	code := status.Code(err)
	if targetGroupID != 0 &&
		(code == codes.FailedPrecondition || code == codes.NotFound || code == codes.Unavailable) {
		return "target_unavailable"
	}
	return "rpc_error"
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
	if event.IsolationReason != "" {
		args := []any{slog.String("reason", string(event.IsolationReason))}
		if event.RouteID != 0 {
			args = append(args, slog.Uint64("route_id", event.RouteID))
		}
		s.cfg.Logger.Debug("autosplit: isolation declined", args...)
	}
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
