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
	return onlineSourceHandoffActive(ctx, group.Store, key)
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
