package autosplit

import (
	"bytes"
	"sort"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
)

const (
	defaultWriteWeight       = 4
	defaultReadWeight        = 1
	defaultThresholdOpsMin   = 50_000
	defaultCandidateWindows  = 3
	defaultMaxSplitsPerCycle = 1
	opsPerMinute             = 60
)

// SplitOrigin describes how an automatic split key was selected.
type SplitOrigin string

const (
	SplitOriginP50Mid           SplitOrigin = "p50_mid"
	SplitOriginP50LastBucketLo  SplitOrigin = "p50_last_bucket_lo"
	SplitOriginP50FirstBucketHi SplitOrigin = "p50_first_bucket_hi"
)

// SkipReason explains why a route did not produce a split decision.
type SkipReason string

const (
	SkipReasonNoSplitKey      SkipReason = "no_split_key"
	SkipReasonRouteCap        SkipReason = "route_cap"
	SkipReasonBudgetExhausted SkipReason = "budget_exhausted"
	SkipReasonNonActiveState  SkipReason = "non_active_state"
	SkipReasonAggregateRow    SkipReason = "aggregate_row"
	SkipReasonCooldown        SkipReason = "cooldown"
	SkipReasonInvalidWindow   SkipReason = "invalid_window"
)

// Config controls the pure detector and scheduler admission checks.
type Config struct {
	WriteWeight       float64
	ReadWeight        float64
	ThresholdOpsMin   float64
	CandidateWindows  int
	MaxRoutes         int
	MaxSplitsPerCycle int
}

// DefaultConfig returns the M3 detector defaults from the design doc.
func DefaultConfig() Config {
	return Config{
		WriteWeight:       defaultWriteWeight,
		ReadWeight:        defaultReadWeight,
		ThresholdOpsMin:   defaultThresholdOpsMin,
		CandidateWindows:  defaultCandidateWindows,
		MaxSplitsPerCycle: defaultMaxSplitsPerCycle,
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
// keyviz.MatrixColumn does not yet carry WindowStart. Runtime integration can
// derive Duration from the previous contiguous MatrixColumn.At boundary and pass
// only committed windows here.
type ColumnWindow struct {
	Column   keyviz.MatrixColumn
	Duration time.Duration
}

// Input is one pure detector evaluation.
type Input struct {
	Routes  []distribution.RouteDescriptor
	Windows []ColumnWindow
	Now     time.Time
}

// Decision is a scheduler-ready automatic split decision.
type Decision struct {
	RouteID       uint64
	SplitKey      []byte
	SplitOrigin   SplitOrigin
	TargetGroupID uint64
	RouteDelta    int

	ScoreOpsMin     float64
	ConsecutiveOver int
	LeftLoad        float64
	RightLoad       float64
}

// Event records a deterministic skip or reset reason from an evaluation.
type Event struct {
	RouteID uint64
	Reason  SkipReason
	At      time.Time
}

// Result is the complete output of one detector evaluation.
type Result struct {
	Decisions []Decision
	Events    []Event
}

// RouteStatus is the observable detector state for one route.
type RouteStatus struct {
	ConsecutiveOver int
	CooldownUntil   time.Time
}

// DetectorState carries leader-local confidence and cooldown state.
type DetectorState struct {
	routes map[uint64]RouteStatus
}

// NewDetectorState creates empty detector state.
func NewDetectorState() *DetectorState {
	return &DetectorState{routes: map[uint64]RouteStatus{}}
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
}

// ApplyRouteState applies a catalog state transition to the detector state.
//
// Non-active route states clear confidence but preserve any cooldown deadline.
func (s *DetectorState) ApplyRouteState(routeID uint64, state distribution.RouteState) {
	if state != distribution.RouteStateActive {
		s.ResetConfidence(routeID)
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

	live, active, result := prepareRoutes(state, in.Routes)
	state.gc(live)

	latestHot := map[uint64]candidate{}
	windows := append([]ColumnWindow(nil), in.Windows...)
	sort.SliceStable(windows, func(i, j int) bool {
		return windows[i].Column.At.Before(windows[j].Column.At)
	})
	for _, window := range windows {
		processWindow(cfg, state, active, window, in.Now, latestHot, &result)
	}
	selectDecisions(cfg, state, live, latestHot, &result)

	return result
}

func prepareRoutes(
	state *DetectorState,
	routes []distribution.RouteDescriptor,
) (map[uint64]distribution.RouteDescriptor, []distribution.RouteDescriptor, Result) {
	live := make(map[uint64]distribution.RouteDescriptor, len(routes))
	active := make([]distribution.RouteDescriptor, 0, len(routes))
	result := Result{}
	for _, route := range routes {
		live[route.RouteID] = route
		if route.State != distribution.RouteStateActive {
			state.ApplyRouteState(route.RouteID, route.State)
			result.Events = append(result.Events, Event{RouteID: route.RouteID, Reason: SkipReasonNonActiveState})
			continue
		}
		active = append(active, route)
	}
	return live, active, result
}

func processWindow(
	cfg Config,
	state *DetectorState,
	active []distribution.RouteDescriptor,
	window ColumnWindow,
	now time.Time,
	latestHot map[uint64]candidate,
	result *Result,
) {
	if window.Duration <= 0 {
		result.Events = append(result.Events, Event{Reason: SkipReasonInvalidWindow, At: window.Column.At})
		for _, route := range active {
			state.ResetConfidence(route.RouteID)
			delete(latestHot, route.RouteID)
		}
		return
	}
	aggregated := aggregateColumn(window.Column)
	for _, row := range aggregated.aggregateRows {
		result.Events = append(result.Events, Event{RouteID: row.RouteID, Reason: SkipReasonAggregateRow, At: window.Column.At})
	}

	for _, route := range active {
		processRouteWindow(cfg, state, route, window, now, aggregated, latestHot, result)
	}
}

func processRouteWindow(
	cfg Config,
	state *DetectorState,
	route distribution.RouteDescriptor,
	window ColumnWindow,
	now time.Time,
	aggregated columnAggregation,
	latestHot map[uint64]candidate,
	result *Result,
) {
	status := state.routes[route.RouteID]
	if now.Before(status.CooldownUntil) || window.Column.At.Before(status.CooldownUntil) {
		status.ConsecutiveOver = 0
		state.routes[route.RouteID] = status
		delete(latestHot, route.RouteID)
		result.Events = append(result.Events, Event{RouteID: route.RouteID, Reason: SkipReasonCooldown, At: window.Column.At})
		return
	}

	key := RouteKey{RouteID: route.RouteID, RaftGroupID: route.GroupID}
	load := aggregated.loads[key]
	score := scoreOpsPerMinute(load, window.Duration, cfg)
	if score < cfg.ThresholdOpsMin {
		status.ConsecutiveOver = 0
		state.routes[route.RouteID] = status
		delete(latestHot, route.RouteID)
		return
	}

	status.ConsecutiveOver++
	state.routes[route.RouteID] = status
	latestHot[route.RouteID] = candidate{
		route:           route,
		rows:            aggregated.rows[key],
		scoreOpsMin:     score,
		consecutiveOver: status.ConsecutiveOver,
	}
}

func selectDecisions(
	cfg Config,
	state *DetectorState,
	live map[uint64]distribution.RouteDescriptor,
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
		if ordered[i].scoreOpsMin == ordered[j].scoreOpsMin {
			return ordered[i].route.RouteID < ordered[j].route.RouteID
		}
		return ordered[i].scoreOpsMin > ordered[j].scoreOpsMin
	})

	reservedDelta := 0
	for _, candidate := range ordered {
		if len(result.Decisions) >= cfg.MaxSplitsPerCycle {
			result.Events = append(result.Events, Event{RouteID: candidate.route.RouteID, Reason: SkipReasonBudgetExhausted})
			continue
		}
		decision, ok := selectP50Decision(cfg, candidate)
		if !ok {
			result.Events = append(result.Events, Event{RouteID: candidate.route.RouteID, Reason: SkipReasonNoSplitKey})
			continue
		}
		if cfg.MaxRoutes > 0 && len(live)+reservedDelta+decision.RouteDelta > cfg.MaxRoutes {
			result.Events = append(result.Events, Event{RouteID: candidate.route.RouteID, Reason: SkipReasonRouteCap})
			continue
		}
		reservedDelta += decision.RouteDelta
		result.Decisions = append(result.Decisions, decision)
	}
}

type candidate struct {
	route           distribution.RouteDescriptor
	rows            []keyviz.MatrixRow
	scoreOpsMin     float64
	consecutiveOver int
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
}

func (s *DetectorState) gc(live map[uint64]distribution.RouteDescriptor) {
	for routeID := range s.routes {
		if _, ok := live[routeID]; !ok {
			delete(s.routes, routeID)
		}
	}
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
	if cfg.MaxSplitsPerCycle <= 0 {
		cfg.MaxSplitsPerCycle = defaults.MaxSplitsPerCycle
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

func selectP50Decision(cfg Config, candidate candidate) (Decision, bool) {
	key, origin, leftLoad, rightLoad, ok := selectP50SplitKey(cfg, candidate.route, candidate.rows)
	if !ok {
		return Decision{}, false
	}
	return Decision{
		RouteID:         candidate.route.RouteID,
		SplitKey:        key,
		SplitOrigin:     origin,
		TargetGroupID:   0,
		RouteDelta:      1,
		ScoreOpsMin:     candidate.scoreOpsMin,
		ConsecutiveOver: candidate.consecutiveOver,
		LeftLoad:        leftLoad,
		RightLoad:       rightLoad,
	}, true
}

func selectP50SplitKey(cfg Config, route distribution.RouteDescriptor, rows []keyviz.MatrixRow) ([]byte, SplitOrigin, float64, float64, bool) {
	rows = usableSubBucketRows(rows)
	if len(rows) == 0 {
		return nil, "", 0, 0, false
	}
	sortSubBucketRows(rows)

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
