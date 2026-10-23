package adapter

import (
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

func (s *DistributionServer) catalogSplitPartitionOps(ctx context.Context, readTS, parentID uint64,
	left, right distribution.RouteDescriptor, version uint64, workingJob *distribution.SplitJob,
) ([]*kv.Elem[kv.OP], [][]byte, error) {
	// A legacy split-and-fence changes authority rather than partitioning it.
	// The new working-child flow records geometry while both children are active.
	if left.State != distribution.RouteStateActive || right.State != distribution.RouteStateActive {
		return nil, nil, nil
	}
	snapshot, err := s.loadCatalogSnapshotAt(ctx, readTS)
	if err != nil {
		return nil, nil, err
	}
	parent, found := findRouteByID(snapshot.Routes, parentID)
	if !found || snapshot.Version != version {
		return nil, nil, errors.WithStack(store.ErrWriteConflict)
	}
	manifest := distribution.OwnershipPartitionManifest{PreviousVersion: version, Version: version + 1,
		Parent: parent, Children: []distribution.RouteDescriptor{left, right}}
	if workingJob != nil {
		manifest.JobID = workingJob.JobID
	}
	mutation, err := s.catalog.BuildOwnershipPartitionMutationAt(ctx, readTS, manifest)
	if err != nil {
		return nil, nil, err
	}
	ops, err := catalogStoreMutationsToOps([]*store.KVPairMutation{mutation})
	if err != nil {
		return nil, nil, err
	}
	reads := [][]byte{mutation.Key}
	if workingJob == nil {
		return ops, reads, nil
	}
	current, exists, err := s.catalog.SplitJobAt(ctx, workingJob.JobID, readTS)
	if err != nil {
		return nil, nil, err
	}
	if !exists || !distribution.SplitJobsEquivalent(current, *workingJob) || current.Phase != distribution.SplitJobPhasePlanned ||
		current.WorkingSourceRouteID != 0 || parent.RouteID != current.SourceRouteID {
		return nil, nil, errors.WithStack(distribution.ErrCatalogSplitJobConflict)
	}
	current.WorkingSourceRouteID = right.RouteID
	encoded, err := distribution.EncodeSplitJob(current)
	if err != nil {
		return nil, nil, err
	}
	jobKey := distribution.CatalogSplitJobKey(current.JobID)
	ops = append(ops, &kv.Elem[kv.OP]{Op: kv.Put, Key: jobKey, Value: encoded})
	return ops, append(reads, jobKey), nil
}
