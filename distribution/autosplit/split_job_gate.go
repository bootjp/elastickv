package autosplit

import (
	"context"
	"fmt"

	"github.com/bootjp/elastickv/distribution"
)

type splitJobSnapshotSource interface {
	ListSplitJobsAt(ctx context.Context, ts uint64) ([]distribution.SplitJob, error)
}

// activeSplitJobRouteIDs reads the exact catalog snapshot used by a tick.
// FAILED jobs remain active until they are retried or abandoned.
func activeSplitJobRouteIDs(
	ctx context.Context, source CatalogSnapshotSource, snapshot distribution.CatalogSnapshot,
) (map[uint64]struct{}, error) {
	busyRoutes, _, err := activeSplitJobRoutesAndReservedDelta(ctx, source, snapshot)
	return busyRoutes, err
}

// activeSplitJobRoutesAndReservedDelta counts only jobs whose source is in the
// catalog snapshot but whose working-source route has not appeared there yet.
func activeSplitJobRoutesAndReservedDelta(
	ctx context.Context, source CatalogSnapshotSource, snapshot distribution.CatalogSnapshot,
) (map[uint64]struct{}, int, error) {
	jobSource, ok := source.(splitJobSnapshotSource)
	if !ok {
		return nil, 0, fmt.Errorf("autosplit split-job gate: catalog source does not support ListSplitJobsAt")
	}
	jobs, err := jobSource.ListSplitJobsAt(ctx, snapshot.ReadTS)
	if err != nil {
		return nil, 0, fmt.Errorf("autosplit split-job gate: list jobs at read timestamp %d: %w", snapshot.ReadTS, err)
	}

	visibleRoutes := make(map[uint64]struct{}, len(snapshot.Routes))
	for _, route := range snapshot.Routes {
		visibleRoutes[route.RouteID] = struct{}{}
	}
	busyRoutes := make(map[uint64]struct{}, len(jobs))
	reservedDelta := 0
	for _, job := range jobs {
		if job.Phase != distribution.SplitJobPhaseDone && job.Phase != distribution.SplitJobPhaseAbandoned {
			busyRoutes[job.SourceRouteID] = struct{}{}
			if job.WorkingSourceRouteID != 0 {
				busyRoutes[job.WorkingSourceRouteID] = struct{}{}
			}
			_, sourceVisible := visibleRoutes[job.SourceRouteID]
			_, workingVisible := visibleRoutes[job.WorkingSourceRouteID]
			if sourceVisible && !workingVisible {
				reservedDelta++
			}
		}
	}
	return busyRoutes, reservedDelta, nil
}
