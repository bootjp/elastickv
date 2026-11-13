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
	jobSource, ok := source.(splitJobSnapshotSource)
	if !ok {
		return nil, fmt.Errorf("autosplit split-job gate: catalog source does not support ListSplitJobsAt")
	}
	jobs, err := jobSource.ListSplitJobsAt(ctx, snapshot.ReadTS)
	if err != nil {
		return nil, fmt.Errorf("autosplit split-job gate: list jobs at read timestamp %d: %w", snapshot.ReadTS, err)
	}

	busyRoutes := make(map[uint64]struct{}, len(jobs))
	for _, job := range jobs {
		if job.Phase != distribution.SplitJobPhaseDone && job.Phase != distribution.SplitJobPhaseAbandoned {
			busyRoutes[job.SourceRouteID] = struct{}{}
			if job.WorkingSourceRouteID != 0 {
				busyRoutes[job.WorkingSourceRouteID] = struct{}{}
			}
		}
	}
	return busyRoutes, nil
}
