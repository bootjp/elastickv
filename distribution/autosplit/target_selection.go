package autosplit

import (
	"math"
	"time"

	"github.com/bootjp/elastickv/distribution"
)

// TargetRouteEvidence is a route's already-aggregated weighted ops/min for one
// committed window. The producer must establish serving authority and sampler
// membership for the entire window; an ingress or follower observation cannot
// set these fields to true.
type TargetRouteEvidence struct {
	RouteID                uint64
	GroupID                uint64
	WindowStart            time.Time
	WindowEnd              time.Time
	LoadOpsMin             float64
	Committed              bool
	ServedWholeWindow      bool
	RegisteredBeforeWindow bool
}

// TargetSelectionInput contains only facts needed by the opt-in policy.
// CandidateGroupIDs is the explicit list of existing groups; an empty group
// absent from that list is not a target. MaxEvidenceAge must be positive for
// non-empty candidates.
type TargetSelectionInput struct {
	Enabled           bool
	Decision          Decision
	P50LoadKnown      bool
	CandidateGroupIDs []uint64
	Routes            []distribution.RouteDescriptor
	Evidence          []TargetRouteEvidence
	WindowStart       time.Time
	WindowEnd         time.Time
	Now               time.Time
	MaxEvidenceAge    time.Duration
}

// SelectTargetGroup returns zero for a same-group split. It never collects
// evidence or changes scheduler state. Equal-load targets use the smaller ID.
func SelectTargetGroup(in TargetSelectionInput) uint64 {
	if !in.Enabled || in.Decision.RouteGroupID == 0 || !rightChildEligible(in) {
		return 0
	}

	active, transitional := groupRoutes(in.Routes)
	evidence, duplicates := indexTargetEvidence(in.Evidence)
	sourceLoad, sourceKnown := candidateLoad(in, in.Decision.RouteGroupID,
		active[in.Decision.RouteGroupID], transitional[in.Decision.RouteGroupID], evidence, duplicates)
	if !sourceKnown {
		return 0
	}
	return lowestLoadTarget(in, sourceLoad, active, transitional, evidence, duplicates)
}

func lowestLoadTarget(
	in TargetSelectionInput, sourceLoad float64, active map[uint64]map[uint64]struct{}, transitional map[uint64]bool,
	evidence map[RouteKey]TargetRouteEvidence, duplicates map[RouteKey]bool,
) uint64 {
	var bestID uint64
	bestLoad := math.Inf(1)
	for _, groupID := range in.CandidateGroupIDs {
		if groupID == 0 || groupID == in.Decision.RouteGroupID {
			continue
		}
		load, known := candidateLoad(in, groupID, active[groupID], transitional[groupID], evidence, duplicates)
		if candidateBelowSource(known, load, sourceLoad) &&
			(load < bestLoad || load == bestLoad && (bestID == 0 || groupID < bestID)) {
			bestID, bestLoad = groupID, load
		}
	}
	return bestID
}

func candidateBelowSource(known bool, load, sourceLoad float64) bool {
	return known && finiteNonnegative(load) && load < sourceLoad
}

func groupRoutes(routes []distribution.RouteDescriptor) (map[uint64]map[uint64]struct{}, map[uint64]bool) {
	active := make(map[uint64]map[uint64]struct{})
	transitional := make(map[uint64]bool)
	for _, route := range routes {
		if route.State != distribution.RouteStateActive {
			transitional[route.GroupID] = true
			continue
		}
		if active[route.GroupID] == nil {
			active[route.GroupID] = make(map[uint64]struct{})
		}
		active[route.GroupID][route.RouteID] = struct{}{}
	}
	return active, transitional
}

func indexTargetEvidence(rows []TargetRouteEvidence) (map[RouteKey]TargetRouteEvidence, map[RouteKey]bool) {
	evidence := make(map[RouteKey]TargetRouteEvidence, len(rows))
	duplicates := make(map[RouteKey]bool)
	for _, row := range rows {
		key := RouteKey{RouteID: row.RouteID, RaftGroupID: row.GroupID}
		if _, exists := evidence[key]; exists {
			duplicates[key] = true
		}
		evidence[key] = row
	}
	return evidence, duplicates
}

func candidateLoad(
	in TargetSelectionInput, groupID uint64, routes map[uint64]struct{}, transitional bool,
	evidence map[RouteKey]TargetRouteEvidence, duplicates map[RouteKey]bool,
) (float64, bool) {
	if transitional || !validTargetWindow(in) {
		return 0, false
	}
	load := 0.0
	for routeID := range routes {
		key := RouteKey{RouteID: routeID, RaftGroupID: groupID}
		row, present := evidence[key]
		if !present || duplicates[key] || !validTargetEvidence(in, row) {
			return 0, false
		}
		load += row.LoadOpsMin
	}
	return load, finiteNonnegative(load)
}

func validTargetWindow(in TargetSelectionInput) bool {
	return !in.WindowStart.IsZero() && in.WindowEnd.After(in.WindowStart) &&
		!in.Now.Before(in.WindowEnd) && in.MaxEvidenceAge > 0 &&
		in.Now.Sub(in.WindowEnd) <= in.MaxEvidenceAge
}

func validTargetEvidence(in TargetSelectionInput, row TargetRouteEvidence) bool {
	return row.Committed && row.ServedWholeWindow && row.RegisteredBeforeWindow &&
		row.WindowStart.Equal(in.WindowStart) && row.WindowEnd.Equal(in.WindowEnd) &&
		finiteNonnegative(row.LoadOpsMin)
}

func rightChildEligible(in TargetSelectionInput) bool {
	switch in.Decision.SplitOrigin {
	case SplitOriginIsolationSingleUpperEdge:
		return true
	case SplitOriginP50Mid, SplitOriginP50LastBucketLo:
		return in.P50LoadKnown && finiteNonnegative(in.Decision.LeftLoad) &&
			finiteNonnegative(in.Decision.RightLoad) &&
			in.Decision.RightLoad >= in.Decision.LeftLoad
	case SplitOriginP50FirstBucketHi, SplitOriginIsolationCompound, SplitOriginIsolationSingleLowerEdge:
		return false
	}
	return false
}

func finiteNonnegative(value float64) bool {
	return value >= 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}
