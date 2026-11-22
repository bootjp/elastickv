package kv

import (
	"context"

	"github.com/bootjp/elastickv/store"
)

// OnlineMigrationHandoffActive is an error-path hint, never write authority.
// Every retry repeats ordinary routing, ownership and proposal/apply admission.
func (s *ShardStore) OnlineMigrationHandoffActive(ctx context.Context, key []byte) bool {
	route, group, ok := s.routeAndGroupForKey(key)
	if !ok || group == nil || group.Store == nil {
		return false
	}
	if routeHasStagedVisibility(route) {
		_, err := store.ReadOnlineMigrationTarget(ctx, group.Store, route.MigrationJobID)
		return err == nil
	}
	// Promotion can clear the target descriptor before an old-source fence
	// rejection reaches ingress. Its source tracker remains armed until source
	// cleanup finishes, even though the route now names the bare target.
	for _, source := range s.groups {
		if source != nil && source.Store != nil && onlineSourceHandoffActive(ctx, source.Store, key) {
			return true
		}
	}
	return false
}

func onlineSourceHandoffActive(ctx context.Context, st store.MVCCStore, key []byte) bool {
	reader, ok := st.(store.MigrationTargetReadinessReader)
	if !ok {
		return false
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return false
	}
	for _, state := range states {
		if state.Armed && state.TrackWrites && routeRangeIntersects(state.RouteStart, state.RouteEnd, key, nextScanCursor(key)) {
			_, err := store.ReadOnlineMigrationSession(ctx, st, state.JobID)
			if err == nil {
				return true
			}
		}
	}
	return false
}
