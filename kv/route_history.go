package kv

import (
	"github.com/bootjp/elastickv/distribution"
)

// WrapDistributionEngine adapts a *distribution.Engine so it satisfies the
// kv.RouteHistory interface that kvFSM's M2 Composed-1 plumbing consumes.
//
// The adapter is a thin two-hop boxing: kv.RouteHistory.SnapshotAt returns
// a kv.RouteSnapshot interface; distribution.Engine.SnapshotAt returns a
// concrete distribution.RouteHistorySnapshot struct.  Go's structural
// interface satisfaction is byte-equivalent on return types, so we cannot
// have *distribution.Engine satisfy kv.RouteHistory directly — and moving
// the interface to the distribution package would create an import cycle
// (kv already imports distribution via kv/sharded_coordinator.go).
//
// Production wiring in main.go uses WrapDistributionEngine to install the
// engine as the FSM's route-history provider; tests that want to mock
// kv.RouteHistory bypass the wrapper entirely and implement the kv
// interface directly.
//
// See docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md
// §M2.
func WrapDistributionEngine(e *distribution.Engine) RouteHistory {
	if e == nil {
		return nil
	}
	return &distributionEngineAdapter{e: e}
}

type distributionEngineAdapter struct {
	e *distribution.Engine
}

func (a *distributionEngineAdapter) SnapshotAt(v uint64) (RouteSnapshot, bool) {
	snap, ok := a.e.SnapshotAt(v)
	if !ok {
		return nil, false
	}
	return distributionRouteSnapshot{snap: snap}, true
}

func (a *distributionEngineAdapter) Current() (RouteSnapshot, bool) {
	snap, ok := a.e.Current()
	if !ok {
		return nil, false
	}
	return distributionRouteSnapshot{snap: snap}, true
}

type distributionRouteSnapshot struct {
	snap distribution.RouteHistorySnapshot
}

func (s distributionRouteSnapshot) Version() uint64 {
	return s.snap.Version()
}

func (s distributionRouteSnapshot) OwnerOf(key []byte) (uint64, bool) {
	return s.snap.OwnerOf(key)
}

func (s distributionRouteSnapshot) WriteFencedForKey(key []byte) bool {
	route, ok := s.snap.RouteOf(key)
	return ok && route.State == distribution.RouteStateWriteFenced
}

func (s distributionRouteSnapshot) WriteFencedIntersects(start, end []byte) bool {
	for _, route := range s.snap.IntersectingRoutes(start, end) {
		if route.State == distribution.RouteStateWriteFenced {
			return true
		}
	}
	return false
}
