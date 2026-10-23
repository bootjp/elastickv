package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestSplitPartitionManifestConflictDoesNotPublishRoutes(t *testing.T) {
	t.Parallel()
	s, version := seededDistributionServer(t)
	before, err := s.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	hash, err := distribution.OwnershipPartitionDescriptorHash(before.Routes[0])
	require.NoError(t, err)
	key, err := distribution.CatalogOwnershipPartitionKey(1, hash)
	require.NoError(t, err)
	coordinator, ok := s.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	coordinator.beforeApply = func(ctx context.Context, st store.MVCCStore) error {
		return st.PutAt(ctx, key, []byte("conflicting ledger publication"), coordinator.lastCommitTS, 0)
	}
	_, err = s.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: 1, SplitKey: []byte("g"), ExpectedCatalogVersion: version})
	require.Error(t, err)
	after, err := s.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Version, after.Version)
	require.Equal(t, before.Routes, after.Routes)
	requireReadKeysContain(t, coordinator.lastReadKeys, key)
}

func TestSplitRangeCommitsPartitionManifestWithRoutes(t *testing.T) {
	t.Parallel()
	s, version := seededDistributionServer(t)
	before, err := s.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	parent := before.Routes[0]
	response, err := s.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: parent.RouteID,
		SplitKey: []byte("g"), ExpectedCatalogVersion: version})
	require.NoError(t, err)
	after, err := s.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	hash, err := distribution.OwnershipPartitionDescriptorHash(parent)
	require.NoError(t, err)
	manifest, exists, err := s.catalog.OwnershipPartitionAt(t.Context(), parent.RouteID, hash, after.ReadTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, response.CatalogVersion, manifest.Version)
	require.Equal(t, version, manifest.PreviousVersion)
	require.Equal(t, after.Routes, manifest.Children)
	_, exists, err = s.catalog.OwnershipPartitionAt(t.Context(), parent.RouteID, hash, before.ReadTS)
	require.NoError(t, err)
	require.False(t, exists, "lineage must not become visible before the route transaction")
}

func TestSplitWorkingSourcePersistsJobWithManifestAndRecovers(t *testing.T) {
	t.Parallel()
	s, _ := seededDistributionServer(t)
	coordinator, ok := s.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	s.coordinator = &ownershipCatalogCoordinator{distributionCoordinatorStub: coordinator}
	before, err := s.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	job := distribution.SplitJob{JobID: 13, SourceRouteID: 1, SourceGroupID: 7,
		TargetGroupID: 9, SplitKey: []byte("g"), Phase: distribution.SplitJobPhasePlanned}
	require.NoError(t, s.catalog.CreateSplitJob(t.Context(), job))
	updated, err := s.ensureSplitWorkingSource(t.Context(), job)
	require.NoError(t, err)
	require.NotZero(t, updated.WorkingSourceRouteID)
	require.Equal(t, job.SourceRouteID, updated.SourceRouteID)
	after, route, err := s.splitJobSourceRoute(t.Context(), updated)
	require.NoError(t, err)
	require.Equal(t, updated.WorkingSourceRouteID, route.RouteID)
	require.Equal(t, []byte("g"), route.Start)
	require.Len(t, after.Routes, 2)
	hash, err := distribution.OwnershipPartitionDescriptorHash(before.Routes[0])
	require.NoError(t, err)
	manifest, exists, err := s.catalog.OwnershipPartitionAt(t.Context(), 1, hash, after.ReadTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, job.JobID, manifest.JobID)
	require.Equal(t, after.Routes, manifest.Children)
	loaded, exists, err := s.catalog.SplitJobAt(t.Context(), job.JobID, after.ReadTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, updated, loaded)
	resumed, err := s.ensureSplitWorkingSource(t.Context(), loaded)
	require.NoError(t, err)
	require.Equal(t, updated, resumed)
	fencing := distribution.CloneSplitJob(loaded)
	fencing.Phase = distribution.SplitJobPhaseFence
	fencing.SnapshotTS = 100
	require.NoError(t, s.catalog.SaveSplitJob(t.Context(), loaded, fencing))
	fenceSnapshot, moving, err := s.splitJobSourceRoute(t.Context(), fencing)
	require.NoError(t, err)
	fenced, moving, err := s.ensureSplitJobCatalogFence(t.Context(), fencing, fenceSnapshot, moving)
	require.NoError(t, err)
	require.Len(t, fenced.Routes, 2, "the working child must not be split at its start key again")
	require.Equal(t, route.RouteID, moving.RouteID)
	require.Equal(t, distribution.RouteStateWriteFenced, moving.State)
	require.Equal(t, after.Routes[0], fenced.Routes[0], "the retained side keeps its descriptor and floor")
	replayed, replayedMoving, err := s.ensureSplitJobCatalogFence(t.Context(), fencing, fenced, moving)
	require.NoError(t, err)
	require.Equal(t, fenced.Version, replayed.Version)
	require.Equal(t, moving, replayedMoving)
}
