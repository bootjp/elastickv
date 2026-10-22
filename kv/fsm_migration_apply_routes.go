package kv

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

type migrationApplyRoutesKey struct{}

type migrationApplyRange struct{ start, end []byte }

// The runner installs these guards on every target voter before publishing
// cutover. The entry's observed version, not a catalog watcher, opens the gate.
func (f *kvFSM) withMigrationApplyRoutes(ctx context.Context, req *pb.Request) (context.Context, error) {
	if req.GetPhase() == pb.Phase_ABORT {
		return ctx, nil
	}
	reader, ok := f.store.(store.MigrationTargetReadinessReader)
	if !ok {
		return ctx, nil
	}
	states, err := reader.MigrationTargetReadinessStates(ctx)
	if err != nil {
		return ctx, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
	}
	ranges := migrationApplyFootprint(req)
	guards := migrationApplyTargetGuards(states, ranges)
	if len(guards) == 0 {
		return ctx, nil
	}
	version, pinned := DecodeObservedRouteVersion(req.GetObservedRouteVersion())
	if !pinned || f.shardGroupID == 0 || !migrationApplyGuardsCover(guards, ranges, version) {
		return ctx, errors.WithStack(ErrRouteCutoverPending)
	}
	history, err := migrationApplyHistory(guards, f.shardGroupID, version)
	if err != nil {
		return ctx, errors.WithStack(errors.Mark(err, errTargetReadinessUnproven))
	}
	return context.WithValue(ctx, migrationApplyRoutesKey{}, history), nil
}

func migrationApplyFootprint(req *pb.Request) []migrationApplyRange {
	ranges := make([]migrationApplyRange, 0, len(req.GetMutations())+len(req.GetReadKeys()))
	for _, mut := range req.GetMutations() {
		if mut == nil || isTxnInternalKey(mut.GetKey()) {
			continue
		}
		var start, end []byte
		if mut.GetOp() == pb.Op_DEL_PREFIX {
			start, end = routePrefixRange(mut.GetKey())
		} else {
			start, end = readinessRouteRangeForScan(mut.GetKey(), nextScanCursor(mut.GetKey()))
		}
		ranges = append(ranges, migrationApplyRange{start, end})
	}
	for _, key := range req.GetReadKeys() {
		if !isTxnInternalKey(key) {
			start, end := readinessRouteRangeForScan(key, nextScanCursor(key))
			ranges = append(ranges, migrationApplyRange{start, end})
		}
	}
	return ranges
}

func migrationApplyTargetGuards(states []store.TargetStagedReadinessState, ranges []migrationApplyRange) []store.TargetStagedReadinessState {
	var guards []store.TargetStagedReadinessState
	for _, state := range states {
		if !state.Armed || state.SourceWriteFence || state.SourceReadFence || state.TrackWrites {
			continue
		}
		for _, r := range ranges {
			if routeRangeIntersects(r.start, r.end, state.RouteStart, state.RouteEnd) {
				guards = append(guards, state)
				break
			}
		}
	}
	return guards
}

func migrationApplyGuardsCover(guards []store.TargetStagedReadinessState, ranges []migrationApplyRange, version uint64) bool {
	for _, r := range ranges {
		covered := false
		for _, guard := range guards {
			if guard.ExpectedCutoverVersion != 0 && guard.ExpectedCutoverVersion <= version &&
				bytes.Compare(guard.RouteStart, r.start) <= 0 &&
				(guard.RouteEnd == nil || (r.end != nil && bytes.Compare(r.end, guard.RouteEnd) <= 0)) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

func migrationApplyHistory(guards []store.TargetStagedReadinessState, groupID, version uint64) (RouteHistory, error) {
	routes := make([]distribution.RouteDescriptor, 0, len(guards))
	for _, guard := range guards {
		if guard.JobID == 0 || guard.MigrationJobID != guard.JobID || guard.MinWriteTSExclusive == 0 {
			return nil, errors.New("invalid durable migration apply evidence")
		}
		routes = append(routes, distribution.RouteDescriptor{RouteID: guard.JobID, Start: guard.RouteStart, End: guard.RouteEnd,
			GroupID: groupID, State: distribution.RouteStateActive, MigrationJobID: guard.MigrationJobID,
			StagedVisibilityActive: true, MinWriteTSExclusive: guard.MinWriteTSExclusive})
	}
	engine := distribution.NewEngine()
	if err := engine.ApplySnapshot(distribution.CatalogSnapshot{Version: version, Routes: routes}); err != nil {
		return nil, errors.WithStack(err)
	}
	return WrapDistributionEngine(engine), nil
}

func (f *kvFSM) routeHistoryForContext(ctx context.Context) RouteHistory {
	if history, ok := ctx.Value(migrationApplyRoutesKey{}).(RouteHistory); ok {
		return history
	}
	return f.routes
}
