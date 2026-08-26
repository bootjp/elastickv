package autosplit

import (
	"bytes"
	"math"
	"sort"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
)

const (
	defaultWriteWeight        = 4
	defaultReadWeight         = 1
	defaultThresholdOpsMin    = 50_000
	defaultCandidateWindows   = 3
	defaultMaxRoutes          = 1024
	defaultMaxSplitsPerCycle  = 1
	defaultTopKeyShare        = 0.8
	defaultTopKeyFloorDivisor = 2
	opsPerMinute              = 60
)

// SplitOrigin describes how an automatic split key was selected.
type SplitOrigin string

const (
	SplitOriginP50Mid                   SplitOrigin = "p50_mid"
	SplitOriginP50LastBucketLo          SplitOrigin = "p50_last_bucket_lo"
	SplitOriginP50FirstBucketHi         SplitOrigin = "p50_first_bucket_hi"
	SplitOriginIsolationCompound        SplitOrigin = "isolation_compound"
	SplitOriginIsolationSingleLowerEdge SplitOrigin = "isolation_single_lower_edge"
	SplitOriginIsolationSingleUpperEdge SplitOrigin = "isolation_single_upper_edge"
)

// IsolationDeclineReason explains why aligned Top-K evidence fell through to
// the sub-range p50 selector.
type IsolationDeclineReason string

const (
	IsolationDeclineAbsoluteFloor    IsolationDeclineReason = "absolute_floor"
	IsolationDeclineTopKDegraded     IsolationDeclineReason = "topk_degraded"
	IsolationDeclineTopKInsufficient IsolationDeclineReason = "topk_insufficient"
	IsolationDeclineTopKErrorBound   IsolationDeclineReason = "topk_error_bound"
)

// SkipReason explains why a route did not produce a split decision.
type SkipReason string

const (
	SkipReasonNoSplitKey         SkipReason = "no_split_key"
	SkipReasonRouteCap           SkipReason = "route_cap"
	SkipReasonBudgetExhausted    SkipReason = "budget_exhausted"
	SkipReasonNonActiveState     SkipReason = "non_active_state"
	SkipReasonAggregateRow       SkipReason = "aggregate_row"
	SkipReasonCooldown           SkipReason = "cooldown"
	SkipReasonInvalidWindow      SkipReason = "invalid_window"
	SkipReasonLeadershipFence    SkipReason = "leadership_fence"
	SkipReasonUnsplittableHotKey SkipReason = "unsplittable_hot_key"
)

// Config controls the pure detector and scheduler admission checks.
type Config struct {
	WriteWeight         float64
	ReadWeight          float64
	ThresholdOpsMin     float64
	CandidateWindows    int
	MaxRoutes           int
	MaxSplitsPerCycle   int
	TopKeyShare         float64
	TopKeyAbsoluteFloor float64
}

// DefaultConfig returns the M3 detector defaults from the design doc.
func DefaultConfig() Config {
	return Config{
		WriteWeight:         defaultWriteWeight,
		ReadWeight:          defaultReadWeight,
		ThresholdOpsMin:     defaultThresholdOpsMin,
		CandidateWindows:    defaultCandidateWindows,
		MaxRoutes:           defaultMaxRoutes,
		MaxSplitsPerCycle:   defaultMaxSplitsPerCycle,
		TopKeyShare:         defaultTopKeyShare,
		TopKeyAbsoluteFloor: defaultThresholdOpsMin / defaultTopKeyFloorDivisor,
	}
}

// RouteKey is the per-column aggregation key for keyviz rows.
type RouteKey struct {
	RouteID     uint64
	RaftGroupID uint64
}

// RouteLoad is the route-level load aggregated from a committed keyviz column.
type RouteLoad struct {
	Reads      uint64
	Writes     uint64
	ReadBytes  uint64
	WriteBytes uint64
}

// ColumnWindow is a committed keyviz column plus its proven committed duration.
//
// Runtime integration passes only committed windows with a proven duration.
// keyviz.MatrixColumn.WindowStart is authoritative when present; legacy
// in-memory rows may be accepted only when the previous contiguous column proves
// the lower boundary. MatrixColumn carries the exact committed (WindowStart,
// At] boundary used to align route load and Top-K evidence.
type ColumnWindow struct {
	Column   keyviz.MatrixColumn
	Duration time.Duration
}

// Input is one pure detector evaluation.
type Input struct {
	Routes         []distribution.RouteDescriptor
	Windows        []ColumnWindow
	EvidenceFences map[uint64]EvidenceFence
	Now            time.Time
	LiveRouteCount int
}

// EvidenceFence excludes sampler history that was not collected wholly while
// this node held the relevant default-group and shard-group leadership.
type EvidenceFence struct {
	ProcessedThrough     time.Time
	WindowStartNotBefore time.Time
}

// Decision is a scheduler-ready automatic split decision.
type Decision struct {
	RouteID        uint64
	SplitKey       []byte
	SecondSplitKey []byte
	SplitOrigin    SplitOrigin
	TargetGroupID  uint64
	RouteDelta     int
	RouteStart     []byte
	RouteEnd       []byte
	RouteGroupID   uint64

	ScoreOpsMin          float64
	PerColumnScoreOpsMin float64
	ConsecutiveOver      int
	LeftLoad             float64
	RightLoad            float64
}

// Event records a deterministic skip or reset reason from an evaluation.
type Event struct {
	RouteID         uint64
	Reason          SkipReason
	IsolationReason IsolationDeclineReason
	At              time.Time
}

// Result is the complete output of one detector evaluation.
type Result struct {
	Decisions []Decision
	Events    []Event
	Promoted  int
}

// RouteStatus is the observable detector state for one route.
type RouteStatus struct {
	ConsecutiveOver int
	CooldownUntil   time.Time
	LastProcessedAt time.Time
}

// DetectorState carries leader-local confidence and cooldown state.
type DetectorState struct {
	routes       map[uint64]RouteStatus
	scoreHistory map[uint64][]scoreSample
}

// NewDetectorState creates empty detector state.
func NewDetectorState() *DetectorState {
	return &DetectorState{
		routes:       map[uint64]RouteStatus{},
		scoreHistory: map[uint64][]scoreSample{},
	}
}

// RouteStatus returns the current detector state for routeID.
func (s *DetectorState) RouteStatus(routeID uint64) RouteStatus {
	if s == nil {
		return RouteStatus{}
	}
	return s.routes[routeID]
}

// ResetConfidence clears candidate confidence while preserving cooldown.
func (s *DetectorState) ResetConfidence(routeID uint64) {
	if s == nil {
		return
	}
	s.ensure()
	status := s.routes[routeID]
	status.ConsecutiveOver = 0
	s.routes[routeID] = status
	delete(s.scoreHistory, routeID)
}

// ApplyRouteState applies a catalog state transition to the detector state.
//
// Non-active route states clear confidence and advance the processed watermark
// through the newest committed column the caller intentionally skipped.
func (s *DetectorState) ApplyRouteState(routeID uint64, state distribution.RouteState, processedThrough time.Time) {
	if state != distribution.RouteStateActive {
		s.resetConfidenceThrough(routeID, processedThrough)
	}
}

// SetCooldown blocks routeID from promotion until until.
func (s *DetectorState) SetCooldown(routeID uint64, until time.Time) {
	if s == nil {
		return
	}
	s.ensure()
	status := s.routes[routeID]
	status.CooldownUntil = until
	status.ConsecutiveOver = 0
	s.routes[routeID] = status
	delete(s.scoreHistory, routeID)
}

// AggregateColumnRows groups all non-aggregate rows by (RouteID, RaftGroupID).
func AggregateColumnRows(col keyviz.MatrixColumn) map[RouteKey]RouteLoad {
	out := make(map[RouteKey]RouteLoad)
	for _, row := range col.Rows {
		if row.Aggregate {
			continue
		}
		key := RouteKey{RouteID: row.RouteID, RaftGroupID: row.RaftGroupID}
		load := out[key]
		load.Reads += row.Reads
		load.Writes += row.Writes
		load.ReadBytes += row.ReadBytes
		load.WriteBytes += row.WriteBytes
		out[key] = load
	}
	return out
}

// Evaluate consumes committed keyviz windows and emits scheduler-ready decisions.
func Evaluate(cfg Config, state *DetectorState, in Input) Result {
	cfg = cfg.withDefaults()
	if state == nil {
		state = NewDetectorState()
	} else {
		state.ensure()
	}

	latestHot := map[uint64]candidate{}
	windows := append([]ColumnWindow(nil), in.Windows...)
	sort.SliceStable(windows, func(i, j int) bool {
		return windows[i].Column.At.Before(windows[j].Column.At)
	})
	live, active, result := prepareRoutes(state, in.Routes, newestWindowAt(windows))
	state.gc(live)

	for _, window := range windows {
		processWindow(cfg, state, active, window, in.EvidenceFences, in.Now, latestHot, &result)
	}
	liveRouteCount := in.LiveRouteCount
	if liveRouteCount <= 0 || liveRouteCount < len(live) {
		liveRouteCount = len(live)
	}
	selectDecisions(cfg, state, liveRouteCount, latestHot, &result)

	return result
}

func prepareRoutes(
	state *DetectorState,
	routes []distribution.RouteDescriptor,
	processedThrough time.Time,
) (map[uint64]distribution.RouteDescriptor, []distribution.RouteDescriptor, Result) {
	live := make(map[uint64]distribution.RouteDescriptor, len(routes))
	active := make([]distribution.RouteDescriptor, 0, len(routes))
	result := Result{}
	for _, route := range routes {
		live[route.RouteID] = route
		if route.State != distribution.RouteStateActive {
			state.ApplyRouteState(route.RouteID, route.State, processedThrough)
			result.Events = append(result.Events, Event{RouteID: route.RouteID, Reason: SkipReasonNonActiveState})
			continue
		}
		active = append(active, route)
	}
	return live, active, result
}

func newestWindowAt(windows []ColumnWindow) time.Time {
	if len(windows) == 0 {
		return time.Time{}
	}
	return windows[len(windows)-1].Column.At
}

func processWindow(
	cfg Config,
	state *DetectorState,
	active []distribution.RouteDescriptor,
	window ColumnWindow,
	fences map[uint64]EvidenceFence,
	now time.Time,
	latestHot map[uint64]candidate,
	result *Result,
) {
	if window.Duration <= 0 {
		result.Events = append(result.Events, Event{Reason: SkipReasonInvalidWindow, At: window.Column.At})
		for _, route := range active {
			if state.resetConfidenceAt(route.RouteID, window.Column.At) {
				delete(latestHot, route.RouteID)
			}
		}
		return
	}
	aggregated := aggregateColumn(window.Column)
	for _, row := range aggregated.aggregateRows {
		result.Events = append(result.Events, Event{RouteID: row.RouteID, Reason: SkipReasonAggregateRow, At: window.Column.At})
	}

	for _, route := range active {
		processRouteWindow(cfg, state, route, window, fences[route.RouteID], now, aggregated, latestHot, result)
	}
}

func processRouteWindow(
	cfg Config,
	state *DetectorState,
	route distribution.RouteDescriptor,
	window ColumnWindow,
	fence EvidenceFence,
	now time.Time,
	aggregated columnAggregation,
	latestHot map[uint64]candidate,
	result *Result,
) {
	status := state.routes[route.RouteID]
	if !window.Column.At.After(status.LastProcessedAt) {
		return
	}
	windowStart := window.Column.WindowStart
	if windowStart.IsZero() {
		windowStart = window.Column.At.Add(-window.Duration)
	}
	if !fence.ProcessedThrough.IsZero() && !window.Column.At.After(fence.ProcessedThrough) {
		state.resetConfidenceThrough(route.RouteID, window.Column.At)
		delete(latestHot, route.RouteID)
		result.Events = append(result.Events, Event{RouteID: route.RouteID, Reason: SkipReasonLeadershipFence, At: window.Column.At})
		return
	}
	if !fence.WindowStartNotBefore.IsZero() && windowStart.Before(fence.WindowStartNotBefore) {
		state.resetConfidenceThrough(route.RouteID, window.Column.At)
		delete(latestHot, route.RouteID)
		result.Events = append(result.Events, Event{RouteID: route.RouteID, Reason: SkipReasonLeadershipFence, At: window.Column.At})
		return
	}
	if now.Before(status.CooldownUntil) || windowOverlapsCooldown(window, status.CooldownUntil) {
		status.ConsecutiveOver = 0
		status.LastProcessedAt = window.Column.At
		state.routes[route.RouteID] = status
		delete(latestHot, route.RouteID)
		result.Events = append(result.Events, Event{RouteID: route.RouteID, Reason: SkipReasonCooldown, At: window.Column.At})
		return
	}

	key := RouteKey{RouteID: route.RouteID, RaftGroupID: route.GroupID}
	load := aggregated.loads[key]
	score := scoreOpsPerMinute(load, window.Duration, cfg)
	state.recordScore(route.RouteID, load, window.Duration, cfg.CandidateWindows)
	smoothedScore := state.smoothedScore(route.RouteID, cfg)
	if score < cfg.ThresholdOpsMin {
		status.ConsecutiveOver = 0
		status.LastProcessedAt = window.Column.At
		state.routes[route.RouteID] = status
		delete(latestHot, route.RouteID)
		return
	}

	status.ConsecutiveOver++
	status.LastProcessedAt = window.Column.At
	state.routes[route.RouteID] = status
	latestHot[route.RouteID] = candidate{
		route:                route,
		rows:                 aggregated.rows[key],
		load:                 load,
		duration:             window.Duration,
		hotKeys:              alignedHotKeys(window.Column, route.RouteID),
		perColumnScoreOpsMin: score,
		smoothedScoreOpsMin:  smoothedScore,
		consecutiveOver:      status.ConsecutiveOver,
	}
}

func selectDecisions(
	cfg Config,
	state *DetectorState,
	liveRouteCount int,
	latestHot map[uint64]candidate,
	result *Result,
) {
	ordered := make([]candidate, 0, len(latestHot))
	for routeID, candidate := range latestHot {
		if state.routes[routeID].ConsecutiveOver >= cfg.CandidateWindows {
			ordered = append(ordered, candidate)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].perColumnScoreOpsMin == ordered[j].perColumnScoreOpsMin {
			return ordered[i].route.RouteID < ordered[j].route.RouteID
		}
		return ordered[i].perColumnScoreOpsMin > ordered[j].perColumnScoreOpsMin
	})

	reservedDelta := 0
	result.Promoted = len(ordered)
	for _, candidate := range ordered {
		if len(result.Decisions) >= cfg.MaxSplitsPerCycle {
			result.Events = append(result.Events, Event{RouteID: candidate.route.RouteID, Reason: SkipReasonBudgetExhausted})
			continue
		}
		decision, ok := admitCandidate(cfg, liveRouteCount, reservedDelta, candidate, result)
		if !ok {
			continue
		}
		reservedDelta += decision.RouteDelta
		result.Decisions = append(result.Decisions, decision)
	}
}

func admitCandidate(
	cfg Config,
	liveRoutes int,
	reservedDelta int,
	candidate candidate,
	result *Result,
) (Decision, bool) {
	decision, isolationReason, terminalReason, ok := selectDecision(cfg, candidate)
	if isolationReason != "" {
		result.Events = append(result.Events, Event{
			RouteID:         candidate.route.RouteID,
			IsolationReason: isolationReason,
		})
	}
	if terminalReason != "" {
		result.Events = append(result.Events, Event{RouteID: candidate.route.RouteID, Reason: terminalReason})
		return Decision{}, false
	}
	if !ok {
		result.Events = append(result.Events, Event{RouteID: candidate.route.RouteID, Reason: SkipReasonNoSplitKey})
		return Decision{}, false
	}
	if cfg.MaxRoutes > 0 && liveRoutes+reservedDelta+decision.RouteDelta > cfg.MaxRoutes {
		result.Events = append(result.Events, Event{RouteID: candidate.route.RouteID, Reason: SkipReasonRouteCap})
		return Decision{}, false
	}
	return decision, true
}

func windowOverlapsCooldown(window ColumnWindow, cooldownUntil time.Time) bool {
	if cooldownUntil.IsZero() {
		return false
	}
	return window.Column.At.Before(cooldownUntil) || window.Column.At.Add(-window.Duration).Before(cooldownUntil)
}

type candidate struct {
	route                distribution.RouteDescriptor
	rows                 []keyviz.MatrixRow
	load                 RouteLoad
	duration             time.Duration
	hotKeys              []keyviz.KeyvizHotKeysSnapshot
	perColumnScoreOpsMin float64
	smoothedScoreOpsMin  float64
	consecutiveOver      int
}

type scoreSample struct {
	load     RouteLoad
	duration time.Duration
}

type columnAggregation struct {
	loads         map[RouteKey]RouteLoad
	rows          map[RouteKey][]keyviz.MatrixRow
	aggregateRows []keyviz.MatrixRow
}

func (s *DetectorState) ensure() {
	if s.routes == nil {
		s.routes = map[uint64]RouteStatus{}
	}
	if s.scoreHistory == nil {
		s.scoreHistory = map[uint64][]scoreSample{}
	}
}

func (s *DetectorState) resetConfidenceThrough(routeID uint64, through time.Time) {
	if s == nil {
		return
	}
	s.ensure()
	status := s.routes[routeID]
	status.ConsecutiveOver = 0
	if through.After(status.LastProcessedAt) {
		status.LastProcessedAt = through
	}
	s.routes[routeID] = status
	delete(s.scoreHistory, routeID)
}

func (s *DetectorState) resetConfidenceAt(routeID uint64, at time.Time) bool {
	if s == nil {
		return false
	}
	s.ensure()
	status := s.routes[routeID]
	if !at.After(status.LastProcessedAt) {
		return false
	}
	status.ConsecutiveOver = 0
	status.LastProcessedAt = at
	s.routes[routeID] = status
	delete(s.scoreHistory, routeID)
	return true
}

func (s *DetectorState) gc(live map[uint64]distribution.RouteDescriptor) {
	for routeID := range s.routes {
		if _, ok := live[routeID]; !ok {
			delete(s.routes, routeID)
			delete(s.scoreHistory, routeID)
		}
	}
}

func (s *DetectorState) recordScore(routeID uint64, load RouteLoad, duration time.Duration, limit int) {
	if duration <= 0 || limit <= 0 {
		return
	}
	history := s.scoreHistory[routeID]
	history = append(history, scoreSample{load: load, duration: duration})
	if len(history) > limit {
		history = history[len(history)-limit:]
	}
	s.scoreHistory[routeID] = history
}

func (s *DetectorState) smoothedScore(routeID uint64, cfg Config) float64 {
	var load RouteLoad
	var duration time.Duration
	for _, sample := range s.scoreHistory[routeID] {
		load.Reads += sample.load.Reads
		load.Writes += sample.load.Writes
		load.ReadBytes += sample.load.ReadBytes
		load.WriteBytes += sample.load.WriteBytes
		duration += sample.duration
	}
	return scoreOpsPerMinute(load, duration, cfg)
}

func (cfg Config) withDefaults() Config {
	defaults := DefaultConfig()
	if cfg.WriteWeight == 0 && cfg.ReadWeight == 0 {
		cfg.WriteWeight = defaults.WriteWeight
		cfg.ReadWeight = defaults.ReadWeight
	}
	if cfg.ThresholdOpsMin == 0 {
		cfg.ThresholdOpsMin = defaults.ThresholdOpsMin
	}
	if cfg.CandidateWindows <= 0 {
		cfg.CandidateWindows = defaults.CandidateWindows
	}
	if cfg.MaxRoutes <= 0 {
		cfg.MaxRoutes = defaults.MaxRoutes
	}
	if cfg.MaxSplitsPerCycle <= 0 {
		cfg.MaxSplitsPerCycle = defaults.MaxSplitsPerCycle
	}
	if cfg.TopKeyShare <= 0 || cfg.TopKeyShare > 1 {
		cfg.TopKeyShare = defaults.TopKeyShare
	}
	if cfg.TopKeyAbsoluteFloor <= 0 {
		cfg.TopKeyAbsoluteFloor = cfg.ThresholdOpsMin / defaultTopKeyFloorDivisor
	}
	return cfg
}

func aggregateColumn(col keyviz.MatrixColumn) columnAggregation {
	out := columnAggregation{
		loads: make(map[RouteKey]RouteLoad),
		rows:  make(map[RouteKey][]keyviz.MatrixRow),
	}
	for _, row := range col.Rows {
		if row.Aggregate {
			out.aggregateRows = append(out.aggregateRows, row)
			continue
		}
		key := RouteKey{RouteID: row.RouteID, RaftGroupID: row.RaftGroupID}
		load := out.loads[key]
		load.Reads += row.Reads
		load.Writes += row.Writes
		load.ReadBytes += row.ReadBytes
		load.WriteBytes += row.WriteBytes
		out.loads[key] = load
		out.rows[key] = append(out.rows[key], row)
	}
	return out
}

func scoreOpsPerMinute(load RouteLoad, duration time.Duration, cfg Config) float64 {
	seconds := duration.Seconds()
	if seconds <= 0 {
		return 0
	}
	writeRate := float64(load.Writes) / seconds * opsPerMinute
	readRate := float64(load.Reads) / seconds * opsPerMinute
	return writeRate*cfg.WriteWeight + readRate*cfg.ReadWeight
}

func alignedHotKeys(col keyviz.MatrixColumn, routeID uint64) []keyviz.KeyvizHotKeysSnapshot {
	out := make([]keyviz.KeyvizHotKeysSnapshot, 0, len(col.HotKeys))
	for _, snapshot := range col.HotKeys {
		if snapshot.RouteID != routeID ||
			!snapshot.WindowStart.Equal(col.WindowStart) ||
			!snapshot.WindowEnd.Equal(col.At) {
			continue
		}
		out = append(out, snapshot)
	}
	return out
}

func selectDecision(cfg Config, candidate candidate) (Decision, IsolationDeclineReason, SkipReason, bool) {
	decision, reason, terminalReason, considered, ok := selectTopKeyDecision(cfg, candidate)
	if ok {
		return decision, "", "", true
	}
	if terminalReason != "" {
		return Decision{}, reason, terminalReason, false
	}
	p50, p50OK := selectP50Decision(cfg, candidate)
	if considered {
		return p50, reason, "", p50OK
	}
	return p50, "", "", p50OK
}

type hotKeyEstimate struct {
	key   []byte
	lower float64
	upper float64
}

func selectTopKeyDecision(cfg Config, candidate candidate) (Decision, IsolationDeclineReason, SkipReason, bool, bool) {
	if len(candidate.hotKeys) == 0 || candidate.load.Writes == 0 || candidate.duration <= 0 {
		return Decision{}, "", "", false, false
	}

	estimates, reason := buildHotKeyEstimates(candidate.hotKeys)
	if reason != "" {
		return Decision{}, reason, "", true, false
	}
	if len(estimates) == 0 {
		return Decision{}, IsolationDeclineTopKInsufficient, "", true, false
	}

	seconds := candidate.duration.Seconds()
	writes := float64(candidate.load.Writes)
	for _, estimate := range sortedHotKeyEstimates(estimates) {
		decision, isolationReason, terminalReason, matched, ok := evaluateHotKeyEstimate(
			cfg, candidate, estimate, writes, seconds,
		)
		if matched {
			return decision, isolationReason, terminalReason, true, ok
		}
	}
	return Decision{}, "", "", true, false
}

func buildHotKeyEstimates(
	snapshots []keyviz.KeyvizHotKeysSnapshot,
) (map[string]hotKeyEstimate, IsolationDeclineReason) {
	estimates := make(map[string]hotKeyEstimate)
	for _, snapshot := range snapshots {
		if snapshot.DroppedSamples > 0 || snapshot.SkippedLongKeys > 0 {
			return nil, IsolationDeclineTopKDegraded
		}
		if snapshot.Capacity <= 0 || snapshot.SampledN == 0 || snapshot.SampleRate <= 0 {
			return nil, IsolationDeclineTopKInsufficient
		}
		errorBound := uint64(math.Ceil(float64(snapshot.SampledN) / float64(snapshot.Capacity)))
		for _, entry := range snapshot.Entries {
			lowerCount := uint64(0)
			if entry.Count > errorBound {
				lowerCount = entry.Count - errorBound
			}
			key := string(entry.Key)
			estimate := estimates[key]
			if estimate.key == nil {
				estimate.key = distribution.CloneBytes(entry.Key)
			}
			estimate.lower += float64(lowerCount) * float64(snapshot.SampleRate)
			estimate.upper += float64(entry.Count) * float64(snapshot.SampleRate)
			estimates[key] = estimate
		}
	}
	return estimates, ""
}

func sortedHotKeyEstimates(estimates map[string]hotKeyEstimate) []hotKeyEstimate {
	ordered := make([]hotKeyEstimate, 0, len(estimates))
	for _, estimate := range estimates {
		ordered = append(ordered, estimate)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].lower == ordered[j].lower {
			return bytes.Compare(ordered[i].key, ordered[j].key) < 0
		}
		return ordered[i].lower > ordered[j].lower
	})
	return ordered
}

func evaluateHotKeyEstimate(
	cfg Config,
	candidate candidate,
	estimate hotKeyEstimate,
	writes float64,
	seconds float64,
) (Decision, IsolationDeclineReason, SkipReason, bool, bool) {
	lowerShare := math.Min(estimate.lower, writes) / writes
	upperShare := math.Min(estimate.upper, writes) / writes
	lowerContribution := estimate.lower / seconds * opsPerMinute * cfg.WriteWeight
	upperContribution := estimate.upper / seconds * opsPerMinute * cfg.WriteWeight
	if lowerShare < cfg.TopKeyShare {
		if upperShare >= cfg.TopKeyShare && upperContribution >= cfg.TopKeyAbsoluteFloor {
			return Decision{}, IsolationDeclineTopKErrorBound, "", true, false
		}
		return Decision{}, "", "", false, false
	}
	if lowerContribution < cfg.TopKeyAbsoluteFloor {
		return Decision{}, IsolationDeclineAbsoluteFloor, "", true, false
	}
	decision, ok := isolationDecision(candidate, estimate.key)
	if !ok {
		return Decision{}, "", SkipReasonUnsplittableHotKey, true, false
	}
	return decision, "", "", true, true
}

func isolationDecision(candidate candidate, hotKey []byte) (Decision, bool) {
	route := candidate.route
	successor := append(distribution.CloneBytes(hotKey), 0)
	decision := baseDecision(candidate)

	switch {
	case bytes.Equal(hotKey, route.Start):
		if route.End != nil && bytes.Compare(successor, route.End) >= 0 {
			return Decision{}, false
		}
		decision.SplitKey = successor
		decision.SplitOrigin = SplitOriginIsolationSingleLowerEdge
		decision.RouteDelta = 1
	case !splitKeyInsideRoute(route, hotKey):
		return Decision{}, false
	case route.End != nil && bytes.Compare(successor, route.End) >= 0:
		decision.SplitKey = distribution.CloneBytes(hotKey)
		decision.SplitOrigin = SplitOriginIsolationSingleUpperEdge
		decision.RouteDelta = 1
	default:
		decision.SplitKey = distribution.CloneBytes(hotKey)
		decision.SecondSplitKey = successor
		decision.SplitOrigin = SplitOriginIsolationCompound
		decision.RouteDelta = 2
	}
	if !splitKeyInsideRoute(route, decision.SplitKey) {
		return Decision{}, false
	}
	return decision, true
}

func baseDecision(candidate candidate) Decision {
	return Decision{
		RouteID:              candidate.route.RouteID,
		RouteStart:           distribution.CloneBytes(candidate.route.Start),
		RouteEnd:             distribution.CloneBytes(candidate.route.End),
		RouteGroupID:         candidate.route.GroupID,
		TargetGroupID:        0,
		ScoreOpsMin:          candidate.smoothedScoreOpsMin,
		PerColumnScoreOpsMin: candidate.perColumnScoreOpsMin,
		ConsecutiveOver:      candidate.consecutiveOver,
	}
}

func selectP50Decision(cfg Config, candidate candidate) (Decision, bool) {
	key, origin, leftLoad, rightLoad, ok := selectP50SplitKey(cfg, candidate.route, candidate.rows)
	if !ok {
		return Decision{}, false
	}
	decision := baseDecision(candidate)
	decision.SplitKey = key
	decision.SplitOrigin = origin
	decision.RouteDelta = 1
	decision.LeftLoad = leftLoad
	decision.RightLoad = rightLoad
	return decision, true
}

func selectP50SplitKey(cfg Config, route distribution.RouteDescriptor, rows []keyviz.MatrixRow) ([]byte, SplitOrigin, float64, float64, bool) {
	rows = usableSubBucketRows(rows)
	if len(rows) == 0 {
		return nil, "", 0, 0, false
	}
	sortSubBucketRows(rows)
	rows = coalesceSubBucketRows(rows)

	median, total, leftLoad, loadBeforeMedian, ok := weightedMedianRow(cfg, rows)
	if !ok {
		return nil, "", 0, 0, false
	}
	splitKey, origin, leftLoad, rightLoad := splitKeyForMedian(route, median, total, leftLoad, loadBeforeMedian)
	if !splitKeyInsideRoute(route, splitKey) {
		return nil, "", 0, 0, false
	}
	return distribution.CloneBytes(splitKey), origin, leftLoad, rightLoad, true
}

func sortSubBucketRows(rows []keyviz.MatrixRow) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].SubBucket == rows[j].SubBucket {
			return bytes.Compare(rows[i].Start, rows[j].Start) < 0
		}
		return rows[i].SubBucket < rows[j].SubBucket
	})
}

func coalesceSubBucketRows(rows []keyviz.MatrixRow) []keyviz.MatrixRow {
	if len(rows) <= 1 {
		return rows
	}
	out := rows[:0]
	for _, row := range rows {
		if len(out) == 0 || !sameSubBucketRow(out[len(out)-1], row) {
			out = append(out, row)
			continue
		}
		last := &out[len(out)-1]
		last.Reads += row.Reads
		last.Writes += row.Writes
		last.ReadBytes += row.ReadBytes
		last.WriteBytes += row.WriteBytes
	}
	return out
}

func sameSubBucketRow(a, b keyviz.MatrixRow) bool {
	return a.RouteID == b.RouteID &&
		a.RaftGroupID == b.RaftGroupID &&
		a.SubBucket == b.SubBucket &&
		a.SubBucketCount == b.SubBucketCount &&
		bytes.Equal(a.Start, b.Start) &&
		bytes.Equal(a.End, b.End)
}

func weightedMedianRow(cfg Config, rows []keyviz.MatrixRow) (keyviz.MatrixRow, float64, float64, float64, bool) {
	total := 0.0
	for _, row := range rows {
		total += rowWeightedLoad(cfg, row)
	}
	if total <= 0 {
		return keyviz.MatrixRow{}, 0, 0, 0, false
	}
	var median keyviz.MatrixRow
	loadBeforeMedian := 0.0
	leftLoad := 0.0
	cumulative := 0.0
	for _, row := range rows {
		load := rowWeightedLoad(cfg, row)
		cumulative += load
		leftLoad += load
		if cumulative >= total/2 {
			median = row
			break
		}
		loadBeforeMedian = leftLoad
	}
	return median, total, leftLoad, loadBeforeMedian, true
}

func splitKeyForMedian(
	route distribution.RouteDescriptor,
	median keyviz.MatrixRow,
	total float64,
	leftLoad float64,
	loadBeforeMedian float64,
) ([]byte, SplitOrigin, float64, float64) {
	rightLoad := total - leftLoad

	splitKey := median.End
	origin := SplitOriginP50Mid
	if median.SubBucket == 0 && bytes.Equal(median.Start, route.Start) {
		origin = SplitOriginP50FirstBucketHi
	}
	if splitKey == nil || (route.End != nil && bytes.Compare(splitKey, route.End) >= 0) {
		splitKey = median.Start
		origin = SplitOriginP50LastBucketLo
		leftLoad = loadBeforeMedian
		rightLoad = total - leftLoad
	}
	return splitKey, origin, leftLoad, rightLoad
}

func usableSubBucketRows(rows []keyviz.MatrixRow) []keyviz.MatrixRow {
	out := make([]keyviz.MatrixRow, 0, len(rows))
	for _, row := range rows {
		if row.Aggregate || row.SubBucketCount <= 1 {
			continue
		}
		out = append(out, row)
	}
	return out
}

func rowWeightedLoad(cfg Config, row keyviz.MatrixRow) float64 {
	return float64(row.Writes)*cfg.WriteWeight + float64(row.Reads)*cfg.ReadWeight
}

func splitKeyInsideRoute(route distribution.RouteDescriptor, splitKey []byte) bool {
	if len(splitKey) == 0 && len(route.Start) == 0 {
		return false
	}
	if bytes.Compare(splitKey, route.Start) <= 0 {
		return false
	}
	if route.End != nil && bytes.Compare(splitKey, route.End) >= 0 {
		return false
	}
	return true
}
