package adapter

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSplitRangeCannotTreatUnfencedEmptyFrontierAsProofFree(t *testing.T) {
	t.Parallel()
	server, version := seededDistributionServer(t)
	coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	before, err := server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	blocked := errors.New("data group cannot establish a current read fence")
	groups := map[uint64]*kv.ShardGroup{7: {Store: coordinator.store,
		Engine: ownershipRefreshEngine{read: func(context.Context) error { return blocked }}}}
	WithMigrationOwnershipRefresh(groups, nil)(server)
	_, err = server.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: 1,
		SplitKey: []byte("g"), ExpectedCatalogVersion: version})
	require.Error(t, err)
	after, err := server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Version, after.Version)
	require.Equal(t, before.Routes, after.Routes)
}

func TestSplitWorkingSourceCannotDowngradeConfiguredPartitionRuntime(t *testing.T) {
	t.Parallel()
	server, _ := seededDistributionServer(t)
	WithMigrationOwnershipRefresh(map[uint64]*kv.ShardGroup{}, nil)(server)
	job := distribution.SplitJob{JobID: 11, SourceRouteID: 1, SourceGroupID: 7, TargetGroupID: 9}
	updated, err := server.prepareSplitJobWorkingSource(t.Context(), job)
	require.ErrorIs(t, err, store.ErrNotSupported)
	require.Equal(t, job, updated)
}

type splitPartitionFollower struct {
	ownershipRefreshEngine
	applied atomic.Uint64
}

func (e *splitPartitionFollower) AppliedIndex() uint64 { return e.applied.Load() }

type splitPartitionFenceCoordinator struct {
	*distributionCoordinatorStub
	group uint64
}

func (c *splitPartitionFenceCoordinator) LeaseReadForGroup(_ context.Context, id uint64) (uint64, error) {
	c.group = id
	return 42, nil
}

func TestSplitPartitionFollowerWaitsForExactGroupFenceBeforeReading(t *testing.T) {
	t.Parallel()
	server, _ := seededDistributionServer(t)
	base, ok := server.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	coordinator := &splitPartitionFenceCoordinator{distributionCoordinatorStub: base}
	server.coordinator = coordinator
	engine := &splitPartitionFollower{ownershipRefreshEngine: ownershipRefreshEngine{follower: true}}
	engine.applied.Store(41)
	group := &kv.ShardGroup{Engine: engine, Store: store.NewMVCCStore()}
	t.Cleanup(func() { require.NoError(t, group.Store.Close()) })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err := server.splitPartitionAdmissionView(ctx, 7, group)
	require.ErrorIs(t, err, context.DeadlineExceeded, "a leader fence does not imply local catch-up")
	require.EqualValues(t, 7, coordinator.group)
	engine.applied.Store(42)
	view, err := server.splitPartitionAdmissionView(t.Context(), 7, group)
	require.NoError(t, err)
	require.Empty(t, view.Ownership)
	require.Empty(t, view.Partitions.Partitions)
}
