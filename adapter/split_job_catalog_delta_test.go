package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestSplitJobCutoverAndPromotionPublishCatalogDeltas(t *testing.T) {
	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	routes := promotionCompleteDistributionRoutes()
	routes[1].GroupID = 1
	routes[1].StagedVisibilityActive = false
	routes[1].MigrationJobID = 0
	_, err := catalog.Save(ctx, 0, routes)
	require.NoError(t, err)
	saved, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
	job := promotionCompleteDistributionJob()
	job.Phase = distribution.SplitJobPhaseCutover

	require.NoError(t, s.commitSplitJobCutover(ctx, saved, routes[1], job, saved.Version+1))
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogDeltaKey(saved.Version+1))
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogDeltaFloorKey())
	cutover, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	changes, err := catalog.ChangesSince(ctx, saved.Version, 10)
	require.NoError(t, err)
	require.Len(t, changes.Deltas, 1)
	require.Equal(t, cutover.Version, changes.Deltas[0].Version)
	require.Equal(t, []distribution.CatalogRouteMutation{{Op: distribution.CatalogMutationUpsert, RouteID: 3, Route: distributionRouteByID(t, cutover.Routes, 3)}}, changes.Deltas[0].Mutations)

	current, found, err := catalog.SplitJob(ctx, job.JobID)
	require.NoError(t, err)
	require.True(t, found)
	_, completed, err := s.completeSplitJobTargetPromotionViaCoordinator(ctx, cutover.Version, current, 2000)
	require.NoError(t, err)
	require.True(t, completed.TargetPromotionDone)
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogRouteKey(3))
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogDeltaKey(cutover.Version+1))
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogDeltaFloorKey())
	changes, err = catalog.ChangesSince(ctx, saved.Version, 10)
	require.NoError(t, err)
	require.Len(t, changes.Deltas, 2)
	latest, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	require.Equal(t, []distribution.CatalogRouteMutation{{Op: distribution.CatalogMutationUpsert, RouteID: 3, Route: distributionRouteByID(t, latest.Routes, 3)}}, changes.Deltas[1].Mutations)
}

func TestSplitJobWorkingSourceFencePublishesCatalogDelta(t *testing.T) {
	ctx := context.Background()
	baseStore := store.NewMVCCStore()
	catalog := distribution.NewCatalogStore(baseStore, distribution.WithCatalogRouteDescriptorV2Writes(true))
	route := distribution.RouteDescriptor{RouteID: 3, Start: []byte("m"), GroupID: 1, State: distribution.RouteStateActive, ParentRouteID: 42}
	_, err := catalog.Save(ctx, 0, []distribution.RouteDescriptor{route})
	require.NoError(t, err)
	saved, err := catalog.Snapshot(ctx)
	require.NoError(t, err)
	coordinator := newDistributionCoordinatorStub(baseStore, true)
	s := NewDistributionServer(distribution.NewEngine(), catalog, WithDistributionCoordinator(coordinator))
	job := promotionCompleteDistributionJob()
	job.Phase = distribution.SplitJobPhaseFence
	job.WorkingSourceRouteID = route.RouteID

	_, fenced, err := s.commitWorkingSourceFence(ctx, job, saved, route)
	require.NoError(t, err)
	require.Equal(t, distribution.RouteStateWriteFenced, fenced.State)
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogDeltaKey(saved.Version+1))
	requireReadKeysContain(t, coordinator.lastReadKeys, distribution.CatalogDeltaFloorKey())
	changes, err := catalog.ChangesSince(ctx, saved.Version, 10)
	require.NoError(t, err)
	require.Len(t, changes.Deltas, 1)
	require.Equal(t, []distribution.CatalogRouteMutation{{Op: distribution.CatalogMutationUpsert, RouteID: route.RouteID, Route: fenced}}, changes.Deltas[0].Mutations)
}
