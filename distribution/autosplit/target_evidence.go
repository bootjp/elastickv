package autosplit

import (
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
)

// RouteWindowRegistration proves a route kept its current descriptor across
// an exact committed sampler window.
type RouteWindowRegistration interface {
	RouteRegistrationCoversWindow(routeID, groupID uint64, start, end []byte, windowStart, windowEnd time.Time) bool
}

// ReadWindowCoverage is an independent protocol-ingress proof. Leadership,
// route registration, and an absence of uncovered-read marks are insufficient.
type ReadWindowCoverage interface {
	ReadCoverageCoversWindow(routeID uint64, windowStart, windowEnd time.Time) bool
}

// ServingWindowProof must prove this node served the group for the whole
// window at term, not merely that it leads the group at the current instant.
type ServingWindowProof func(groupID, term uint64, windowStart, windowEnd time.Time) bool

type targetRouteSample struct {
	load  RouteLoad
	term  uint64
	valid bool
}

// BuildLocalTargetEvidence turns one retained committed KeyViz column into
// route-level target load. Missing or unproven rows remain unknown, not idle.
func BuildLocalTargetEvidence(
	col keyviz.MatrixColumn, routes []distribution.RouteDescriptor,
	registrations RouteWindowRegistration, serving ServingWindowProof, cfg Config,
) []TargetRouteEvidence {
	coverage, ok := registrations.(ReadWindowCoverage)
	if registrations == nil || !ok || serving == nil || !col.WindowStart.Before(col.At) {
		return nil
	}
	registrationCovers, readCoverageCovers, ok := targetWindowChecks(registrations, coverage, col)
	if !ok {
		return nil
	}
	byID := make(map[uint64]distribution.RouteDescriptor, len(routes))
	for _, route := range routes {
		byID[route.RouteID] = route
	}
	samples := indexTargetSamples(col.Rows)
	out := make([]TargetRouteEvidence, 0, len(samples))
	for key, sample := range samples {
		route, exists := byID[key.RouteID]
		if !exists {
			continue
		}
		if evidence, ok := provenTargetSample(col, route, key, sample, registrationCovers, readCoverageCovers, serving, cfg); ok {
			out = append(out, evidence)
		}
	}
	return out
}

func targetWindowChecks(
	registrations RouteWindowRegistration, coverage ReadWindowCoverage, col keyviz.MatrixColumn,
) (func(distribution.RouteDescriptor) bool, func(uint64) bool, bool) {
	if provider, ok := registrations.(interface {
		ProofForCommittedWindow(time.Time, time.Time) (keyviz.CommittedWindowProof, bool)
	}); ok {
		proof, found := provider.ProofForCommittedWindow(col.WindowStart, col.At)
		if !found {
			return nil, nil, false
		}
		return func(route distribution.RouteDescriptor) bool {
			return proof.RouteRegistrationCovers(route.RouteID, route.GroupID, route.Start, route.End)
		}, proof.ReadCoverageCovers, true
	}
	return func(route distribution.RouteDescriptor) bool {
			return registrations.RouteRegistrationCoversWindow(route.RouteID, route.GroupID, route.Start, route.End, col.WindowStart, col.At)
		}, func(routeID uint64) bool {
			return coverage.ReadCoverageCoversWindow(routeID, col.WindowStart, col.At)
		}, true
}

func indexTargetSamples(rows []keyviz.MatrixRow) map[RouteKey]targetRouteSample {
	samples := make(map[RouteKey]targetRouteSample)
	for _, row := range rows {
		if row.Aggregate || row.RaftGroupID == 0 {
			continue
		}
		key := RouteKey{RouteID: row.RouteID, RaftGroupID: row.RaftGroupID}
		sample, exists := samples[key]
		if row.LeaderTerm == 0 || exists && sample.term != row.LeaderTerm {
			sample.valid = false
		} else if !exists {
			sample.term = row.LeaderTerm
			sample.valid = true
		}
		if sample.load.Reads > ^uint64(0)-row.Reads || sample.load.Writes > ^uint64(0)-row.Writes {
			sample.valid = false
		} else {
			sample.load.Reads += row.Reads
			sample.load.Writes += row.Writes
		}
		samples[key] = sample
	}
	return samples
}

func provenTargetSample(
	col keyviz.MatrixColumn, route distribution.RouteDescriptor, key RouteKey, sample targetRouteSample,
	registrationCovers func(distribution.RouteDescriptor) bool, readCoverageCovers func(uint64) bool,
	serving ServingWindowProof, cfg Config,
) (TargetRouteEvidence, bool) {
	if route.GroupID != key.RaftGroupID || route.State != distribution.RouteStateActive || !sample.valid {
		return TargetRouteEvidence{}, false
	}
	if !registrationCovers(route) || !readCoverageCovers(route.RouteID) ||
		!serving(route.GroupID, sample.term, col.WindowStart, col.At) {
		return TargetRouteEvidence{}, false
	}
	load := scoreOpsPerMinute(sample.load, col.At.Sub(col.WindowStart), cfg)
	if !finiteNonnegative(load) {
		return TargetRouteEvidence{}, false
	}
	return TargetRouteEvidence{
		RouteID: route.RouteID, GroupID: route.GroupID,
		WindowStart: col.WindowStart, WindowEnd: col.At,
		LoadOpsMin: load, Committed: true,
		ServedWholeWindow: true, RegisteredBeforeWindow: true,
		ReadCoverageComplete: true,
	}, true
}
