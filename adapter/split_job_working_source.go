package adapter

import (
	"bytes"
	"context"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// sourceLookupRouteID is geometry, not lineage. SourceRouteID remains the
// original parent in durable job history and in target ancestry checks.
func sourceLookupRouteID(job distribution.SplitJob) uint64 {
	if job.WorkingSourceRouteID != 0 {
		return job.WorkingSourceRouteID
	}
	return job.SourceRouteID
}

func (s *DistributionServer) prepareSplitJobWorkingSource(ctx context.Context, job distribution.SplitJob) (distribution.SplitJob, error) {
	if !s.MigrationPartitionRuntimeConfigured(job.SourceGroupID) {
		return job, nil
	}
	if err := s.ensureSplitJobNamespaceAdmission(ctx, job); err != nil {
		return job, err
	}
	updated, err := s.ensureSplitWorkingSource(ctx, job)
	if err != nil {
		return job, err
	}
	if err := s.reconcileMigrationPartitions(ctx, updated.SourceGroupID); err != nil {
		return updated, err
	}
	// A move-back target may retain an older NONOWNER interval covering the
	// whole parent. Partition it without changing its role before a guarded
	// acquisition can replace only the new moving child.
	return updated, s.reconcileMigrationPartitions(ctx, updated.TargetGroupID)
}

func (s *DistributionServer) ensureSplitWorkingSource(ctx context.Context, job distribution.SplitJob) (distribution.SplitJob, error) {
	if job.WorkingSourceRouteID != 0 {
		return job, nil
	}
	snapshot, left, right, err := s.planWorkingSourceSplit(ctx, job)
	if err != nil {
		return job, err
	}
	read, err := kv.BeginReadTimestampThrough(ctx, s.coordinator, snapshot.ReadTS, "split working source: catalog transaction")
	if err != nil {
		return job, errors.WithStack(err)
	}
	saved, err := s.saveSplitPartitionViaCoordinator(ctx, read, snapshot.Version, job.SourceRouteID,
		[][]byte{distribution.CatalogSplitJobKey(job.JobID)}, left, right, &job)
	if err != nil {
		return job, err
	}
	if err := s.applyEngineSnapshot(saved); err != nil {
		return job, err
	}
	updated, exists, err := s.catalog.SplitJobAt(ctx, job.JobID, saved.ReadTS)
	if err != nil {
		return job, errors.WithStack(err)
	}
	if !exists || updated.WorkingSourceRouteID != right.RouteID {
		return job, errors.WithStack(distribution.ErrCatalogSplitJobConflict)
	}
	return updated, nil
}

func (s *DistributionServer) planWorkingSourceSplit(ctx context.Context, job distribution.SplitJob) (distribution.CatalogSnapshot, distribution.RouteDescriptor, distribution.RouteDescriptor, error) {
	snapshot, err := s.loadCommittedMigrationCatalog(ctx)
	if err != nil {
		return snapshot, distribution.RouteDescriptor{}, distribution.RouteDescriptor{}, err
	}
	parent, found := findRouteByID(snapshot.Routes, job.SourceRouteID)
	if !found || parent.GroupID != job.SourceGroupID || parent.State != distribution.RouteStateActive || parent.StagedVisibilityActive {
		return snapshot, distribution.RouteDescriptor{}, distribution.RouteDescriptor{}, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	if err := validateSplitKey(parent, job.SplitKey); err != nil {
		return snapshot, distribution.RouteDescriptor{}, distribution.RouteDescriptor{}, err
	}
	leftID, rightID, err := s.allocateChildRouteIDs(ctx, snapshot.ReadTS, snapshot.Routes)
	if err != nil {
		return snapshot, distribution.RouteDescriptor{}, distribution.RouteDescriptor{}, err
	}
	left, right := splitCatalogRoutes(parent, job.SplitKey, leftID, rightID, 0)
	return snapshot, left, right, nil
}

func (s *DistributionServer) fenceSplitWorkingSource(ctx context.Context, job distribution.SplitJob,
	snapshot distribution.CatalogSnapshot, route distribution.RouteDescriptor,
) (distribution.CatalogSnapshot, distribution.RouteDescriptor, error) {
	if !workingSourceFenceRouteMatches(job, route) {
		return snapshot, route, errors.WithStack(distribution.ErrMigrationSourceRouteChanged)
	}
	if route.State == distribution.RouteStateWriteFenced {
		return snapshot, route, nil
	}
	if route.State != distribution.RouteStateActive {
		return snapshot, route, errors.WithStack(store.ErrWriteConflict)
	}
	if err := s.verifyWorkingSourceFenceJob(ctx, job, snapshot.ReadTS); err != nil {
		return snapshot, route, err
	}
	return s.commitWorkingSourceFence(ctx, job, snapshot, route)
}

func workingSourceFenceRouteMatches(job distribution.SplitJob, route distribution.RouteDescriptor) bool {
	return route.RouteID == job.WorkingSourceRouteID && bytes.Equal(route.Start, job.SplitKey) &&
		route.GroupID == job.SourceGroupID && !route.StagedVisibilityActive
}

func (s *DistributionServer) verifyWorkingSourceFenceJob(ctx context.Context, job distribution.SplitJob, readTS uint64) error {
	current, found, err := s.catalog.SplitJobAt(ctx, job.JobID, readTS)
	if err != nil {
		return errors.WithStack(err)
	}
	if !found || current.Phase != distribution.SplitJobPhaseFence || !distribution.SplitJobsEquivalent(current, job) {
		return errors.WithStack(distribution.ErrCatalogSplitJobConflict)
	}
	return nil
}

func (s *DistributionServer) commitWorkingSourceFence(ctx context.Context, job distribution.SplitJob, snapshot distribution.CatalogSnapshot, route distribution.RouteDescriptor) (distribution.CatalogSnapshot, distribution.RouteDescriptor, error) {
	nextVersion, _, err := nextCatalogSplitIDs(snapshot.Version, route.RouteID)
	if err != nil {
		return snapshot, route, err
	}
	route.State = distribution.RouteStateWriteFenced
	encoded, err := distribution.EncodeRouteDescriptorForCatalogWrite(route, s.catalog.AllowsRouteDescriptorV2Writes())
	if err != nil {
		return snapshot, route, errors.WithStack(err)
	}
	read, err := kv.BeginReadTimestampThrough(ctx, s.coordinator, snapshot.ReadTS, "split working source: fence transaction")
	if err != nil {
		return snapshot, route, errors.WithStack(err)
	}
	routeKey, versionKey, jobKey := distribution.CatalogRouteKey(route.RouteID), distribution.CatalogVersionKey(), distribution.CatalogSplitJobKey(job.JobID)
	resp, err := kv.DispatchWithReadTimestamp(read.WithDispatchVoucher(ctx), s.coordinator, &kv.OperationGroup[kv.OP]{
		Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: routeKey, Value: encoded},
			{Op: kv.Put, Key: versionKey, Value: distribution.EncodeCatalogVersion(nextVersion)}},
		IsTxn: true, StartTS: read.Timestamp(), ReadKeys: [][]byte{routeKey, versionKey, jobKey},
	})
	if err != nil {
		return snapshot, route, errors.WithStack(err)
	}
	if resp == nil || resp.CommitTS == 0 {
		return snapshot, route, errors.New("working source fence commit timestamp missing")
	}
	updated, err := s.loadCatalogSnapshotAtVersion(ctx, resp.CommitTS, nextVersion)
	if err != nil {
		return snapshot, route, err
	}
	return updated, route, s.applyEngineSnapshot(updated)
}
