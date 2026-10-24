package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMigrationResplitDoesNotWaitForOfflineHistoricalNonOwner(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	first := r.migrate(t, 1, 2, "m")
	before, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	factory := r.server.splitApplyMemberFactory
	r.server.splitApplyMemberFactory = func(ctx context.Context, id uint64) (SplitMigrationApplyMemberSnapshot, error) {
		if id == 1 {
			return SplitMigrationApplyMemberSnapshot{}, errors.New("historical nonowner is offline")
		}
		return factory(ctx, id)
	}
	owner := r.ownership(t, 2)[0]
	_, err = r.client.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: owner.RouteID, SplitKey: []byte("t"), ExpectedCatalogVersion: before.Version})
	require.NoError(t, err, "negative groups can reconcile the retained manifest after they return")
	for _, key := range []string{"n", "y"} {
		require.NoError(t, r.put(t, 2, key, "available", first.FenceTS+100))
		r.read(t, key, "available")
		require.Error(t, r.put(t, 1, key, "stale-owner", first.FenceTS+101))
		_, err := r.groups[1].Store.GetAt(t.Context(), []byte(key), ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
}

func TestSplitRangeWithoutOwnershipDoesNotRequireMigrationProtocols(t *testing.T) {
	t.Parallel()
	server, version := seededDistributionServer(t)
	coordinator, ok := server.coordinator.(*distributionCoordinatorStub)
	require.True(t, ok)
	groups := map[uint64]*kv.ShardGroup{0: {}, 7: {Store: coordinator.store, Engine: ownershipRefreshEngine{}}}
	WithMigrationOwnershipRefresh(groups, func(context.Context) (MigrationOwnershipAttestor, error) {
		t.Fatal("ordinary split attempted ownership attestation")
		return nil, nil
	})(server)
	_, err := server.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: 1, SplitKey: []byte("g"), ExpectedCatalogVersion: version})
	require.NoError(t, err)
}

func TestMigrationResplitRejectsUnsupportedMemberBeforeCatalogChange(t *testing.T) {
	r := newResplitAcceptanceRuntime(t)
	first := r.migrate(t, 1, 2, "m")
	owner := r.ownership(t, 2)[0]
	before, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	indices := make(map[uint64]uint64)
	for id, client := range r.clients {
		proposer, ok := client.internal.migrationProposer.(*ownershipRuntimeProposer)
		require.True(t, ok)
		indices[id] = proposer.index
	}
	r.clients[2].internal.migrationPartitionProtocolReady = func() bool { return false }
	_, err = r.client.SplitRange(t.Context(), &pb.SplitRangeRequest{RouteId: owner.RouteID, SplitKey: []byte("t"), ExpectedCatalogVersion: before.Version})
	require.Error(t, err)
	after, err := r.server.catalog.Snapshot(t.Context())
	require.NoError(t, err)
	require.Equal(t, before.Version, after.Version)
	require.Equal(t, before.Routes, after.Routes)
	for id, client := range r.clients {
		proposer, ok := client.internal.migrationProposer.(*ownershipRuntimeProposer)
		require.True(t, ok)
		require.Equal(t, indices[id], proposer.index, "all static checks must precede even namespace activation")
	}
	require.NoError(t, r.put(t, 2, "n", "retained", first.FenceTS+10))
	r.read(t, "n", "retained")
	_, exists, err := r.server.catalog.OwnershipPartitionAt(t.Context(), owner.RouteID, owner.DescriptorHash, after.ReadTS)
	require.NoError(t, err)
	require.False(t, exists)
}
