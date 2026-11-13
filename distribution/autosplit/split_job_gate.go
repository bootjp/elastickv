package autosplit

import (
	"context"
	"fmt"

	"github.com/bootjp/elastickv/distribution"
)

type splitJobSnapshotSource interface {
	ListSplitJobsAt(ctx context.Context, ts uint64) ([]distribution.SplitJob, error)
}

// FilterDecisionsWithActiveSplitJobs excludes source routes with a nonterminal
// split job at the supplied catalog snapshot's read timestamp. It is opt-in;
// callers must explicitly use the filtered decisions.
func FilterDecisionsWithActiveSplitJobs(
	ctx context.Context,
	source CatalogSnapshotSource,
	snapshot distribution.CatalogSnapshot,
	decisions []Decision,
) ([]Decision, error) {
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
	filtered := make([]Decision, 0, len(decisions))
	for _, decision := range decisions {
		if _, busy := busyRoutes[decision.RouteID]; !busy {
			filtered = append(filtered, decision)
		}
	}
	return filtered, nil
}
