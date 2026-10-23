package adapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPartitionNegativeDeferredUntilCommittedGeometry(t *testing.T) {
	h := newPartitionRefreshHarness(t, 9)
	changed := h.parent
	changed.GroupID, changed.MinWriteTSExclusive = 8, 200
	_, err := h.root.catalog.Save(t.Context(), 1, []distribution.RouteDescriptor{changed})
	require.NoError(t, err)
	require.ErrorIs(t, h.consumer.reconcileMigrationPartitions(t.Context(), 9), ErrMigrationNegativePartitionDeferred)
	for range 12 {
		before := h.rootClient.calls
		require.NoError(t, h.consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
		require.LessOrEqual(t, h.rootClient.calls-before, ownershipRefreshBatch)
	}
	require.Empty(t, partitionOnlyCleanups(h.clients[0]))
	require.Empty(t, h.frontier(t, 0).Partitions)
	h.split(t, changed, 2, 3, "m")
	require.NoError(t, h.consumer.reconcileMigrationPartitions(t.Context(), 9))
	state := h.frontier(t, 0)
	require.Len(t, state.Partitions, 2)
	for _, p := range state.Partitions {
		require.Equal(t, store.MigrationNonOwner, p.Role)
		require.EqualValues(t, 7, p.OwnerGroupID)
		require.EqualValues(t, 100, p.MinWriteTSExclusive)
		require.EqualValues(t, 11, p.MigrationJobID)
	}
	for range 12 {
		before := h.rootClient.calls
		require.NoError(t, h.consumer.RunMigrationOwnershipRefreshOnce(t.Context()))
		require.LessOrEqual(t, h.rootClient.calls-before, ownershipRefreshBatch)
	}
	for i := range h.stores {
		require.Equal(t, state, h.frontier(t, i))
	}
}

func TestPartitionNegativeCannotCatchUpToLocalOwner(t *testing.T) {
	h := newPartitionRefreshHarness(t, 9)
	changed := h.parent
	changed.GroupID, changed.MinWriteTSExclusive = 9, 200
	_, err := h.root.catalog.Save(t.Context(), 1, []distribution.RouteDescriptor{changed})
	require.NoError(t, err)
	err = h.consumer.reconcileMigrationPartitions(t.Context(), 9)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrMigrationNegativePartitionDeferred)
	h.split(t, changed, 2, 3, "m")
	require.Error(t, h.consumer.reconcileMigrationPartitions(t.Context(), 9))
	require.Empty(t, partitionOnlyCleanups(h.clients[0]))
}

func TestPartitionNegativeFallbackDoesNotMaskLookupFailure(t *testing.T) {
	for _, failure := range []error{errors.New("storage I/O failed"), distribution.ErrOwnershipPartitionManifest} {
		t.Run(failure.Error(), func(t *testing.T) {
			h := newPartitionRefreshHarness(t, 9)
			h.split(t, h.parent, 2, 3, "m")
			hash, err := distribution.OwnershipPartitionDescriptorHash(h.parent)
			require.NoError(t, err)
			key, err := distribution.CatalogOwnershipPartitionKey(h.parent.RouteID, hash)
			require.NoError(t, err)
			h.root.catalog = distribution.NewCatalogStore(&partitionNegativeReadFailure{MVCCStore: h.rootStore, key: key, failure: failure})
			require.ErrorIs(t, h.consumer.reconcileMigrationPartitions(t.Context(), 9), failure)
			require.Empty(t, partitionOnlyCleanups(h.clients[0]), "a discoverable manifest must not mask exact-lookup failure")
		})
	}
}

type partitionNegativeReadFailure struct {
	store.MVCCStore
	key     []byte
	failure error
}

func (s *partitionNegativeReadFailure) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if bytes.Equal(key, s.key) {
		return nil, s.failure
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func TestPartitionNegativeMoveBackRepeatedRefresh(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	r.migrate(t, 1, 2, "m")
	r.migrate(t, r.ownership(t, 2)[0].RouteID, 3, "t")
	require.NoError(t, r.server.RunMigrationOwnershipRefreshOnce(t.Context()))
	third := r.migrate(t, r.ownership(t, 3)[0].RouteID, 1, "x")
	rootCalls := 0
	r.server.ownershipRefresh.root = func(context.Context) (MigrationOwnershipAttestor, error) {
		rootCalls++
		return r.client, nil
	}
	for range 16 {
		before := rootCalls
		require.NoError(t, r.server.RunMigrationOwnershipRefreshOnce(t.Context()))
		require.LessOrEqual(t, rootCalls-before, ownershipRefreshBatch)
		require.Error(t, r.put(t, 2, "y", "retired-two", third.FenceTS+100))
		require.Error(t, r.put(t, 3, "y", "retired-three", third.FenceTS+100))
		require.ErrorIs(t, r.put(t, 1, "y", "old-floor", third.FenceTS), kv.ErrRouteWriteTimestampTooLow)
	}
	require.NoError(t, r.put(t, 1, "y", "returned-owner", third.FenceTS+100))
	r.read(t, "y", "returned-owner")
}
