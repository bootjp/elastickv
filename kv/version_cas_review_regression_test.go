package kv

import (
	"errors"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestCASReplicasAgreeAcrossSameOwnerCatalogSplit(t *testing.T) {
	request := casApplyRequest(15, 20, 10, "a", "after")
	request.ObservedRouteVersion = EncodeObservedRouteVersion(2)
	frame, err := marshalRaftCommand([]*pb.Request{request})
	require.NoError(t, err)
	for _, replica := range []string{"up_to_date", "lagging"} {
		t.Run(replica, func(t *testing.T) {
			catalog := distribution.NewEngine()
			require.NoError(t, catalog.ApplySnapshot(distribution.CatalogSnapshot{Version: 1, Routes: []distribution.RouteDescriptor{
				{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive},
			}}))
			split := distribution.CatalogSnapshot{Version: 2, Routes: []distribution.RouteDescriptor{
				{RouteID: 1, GroupID: 1, End: []byte("m"), State: distribution.RouteStateActive},
				{RouteID: 2, GroupID: 1, Start: []byte("m"), State: distribution.RouteStateActive},
			}}
			if replica == "up_to_date" {
				require.NoError(t, catalog.ApplySnapshot(split))
			}
			backing, err := store.NewPebbleStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, backing.Close()) })
			f, policy := newCASCatalogFSM(t, backing, catalog)
			applyCASCatalogRoutes(t, f, policy, 12, 5, split)
			activateCommitTSFence(t, f)
			f.SetApplyIndex(13)
			require.Nil(t, f.Apply(EncodeMigrationVersionIdentityActivation()))
			require.NoError(t, backing.PutAt(t.Context(), []byte("a"), []byte("before"), 10, 0))
			f.SetApplyIndex(14)
			require.Nil(t, f.Apply(frame))
			requireCASProgress(t, f, 14)
			if replica == "lagging" {
				require.NoError(t, catalog.ApplySnapshot(split))
				f.SetApplyIndex(14)
				require.Nil(t, f.Apply(frame))
			}
			row, err := store.GetVersionAt(t.Context(), backing, []byte("a"), 30)
			require.NoError(t, err)
			require.Equal(t, store.VersionedValue{TS: 20, Value: []byte("after")}, row)
		})
	}
}

func TestCASConsumedVersionStaysConsumedAfterBoundedCleanup(t *testing.T) {
	dir := t.TempDir()
	backing, err := store.NewPebbleStore(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, backing.Close()) })
	f, ok := NewKvFSMWithHLC(backing, NewHLC()).(*kvFSM)
	require.True(t, ok)
	enableCASApplyForTest(t, f)
	key := []byte("a")
	require.NoError(t, backing.PutAt(t.Context(), key, []byte("before"), 10, 0))
	require.Nil(t, applyCASFrame(t, f, 2, casApplyRequest(15, 20, 10, "a", "first")))
	cleanup, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
		JobId: 7, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		RangeStart: key, RangeEnd: nextScanCursor(key), MaxCommitTs: 20, MaxVersions: 1,
	})
	require.NoError(t, err)
	f.SetApplyIndex(3)
	cleaned := f.Apply(cleanup)
	require.NoError(t, haltApplyOf(cleaned))
	requireCASRejection(t, cleaned, store.ErrMigrationOwnership)
	requireCASProgress(t, f, 3)
	beforeRetry, err := store.GetVersionAt(t.Context(), backing, key, 25)
	require.NoError(t, err)
	wantHead := store.VersionedValue{TS: 20, Value: []byte("first")}
	require.Equal(t, wantHead, beforeRetry)
	require.NoError(t, backing.Close())
	backing, err = store.NewPebbleStore(dir)
	require.NoError(t, err)
	f, ok = NewKvFSMWithHLC(backing, NewHLC()).(*kvFSM)
	require.True(t, ok)
	requireCASProgress(t, f, 3)
	f.SetApplyIndex(3)
	require.Nil(t, f.Apply(cleanup), "durable replay skips the decided cleanup")
	verdict := applyCASFrame(t, f, 4, casApplyRequest(25, 30, 10, "a", "second"))
	requireCASRejection(t, verdict, ErrCASMismatch)
	requireCASProgress(t, f, 4)
	afterRetry, err := store.GetVersionAt(t.Context(), backing, key, 35)
	require.NoError(t, err)
	require.Equal(t, wantHead, afterRetry)
}

func TestCoveredCleanupAgreesAcrossReplicaRetention(t *testing.T) {
	ctx := t.Context()
	key := []byte("n")
	frame, err := MarshalMigrationCleanupCommand(&pb.CleanupMigrationRequest{
		JobId: 11, Mode: pb.MigrationCleanupMode_MIGRATION_CLEANUP_MODE_VERSIONS,
		RangeStart: key, RangeEnd: nextScanCursor(key), MaxCommitTs: 20, MaxVersions: 1,
		RequireConfigurationIndex: true, ExpectedConfigurationIndex: 9,
	})
	require.NoError(t, err)
	var heads []store.VersionedValue
	for _, compact := range []bool{false, true} {
		st, err := store.NewPebbleStore(t.TempDir())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, st.Close()) })
		f := &kvFSM{store: st, shardGroupID: 1}
		enableCASApplyForTest(t, f)
		require.NoError(t, st.PutAt(ctx, key, []byte("old"), 10, 0))
		require.NoError(t, st.PutAt(ctx, key, []byte("current"), 20, 0))
		if compact {
			require.NoError(t, st.Compact(ctx, 21))
		}
		installSourceOwnershipForTest(t, f, 0)
		before, err := store.GetVersionAt(ctx, st, key, 30)
		require.NoError(t, err)
		require.Equal(t, store.VersionedValue{TS: 20, Value: []byte("current")}, before)
		f.SetApplyIndex(12)
		require.IsType(t, store.CleanupVersionsResult{}, f.Apply(frame))
		requireCASProgress(t, f, 12)
		after, err := store.GetVersionAt(ctx, st, key, 30)
		if !errors.Is(err, store.ErrKeyNotFound) {
			require.NoError(t, err)
		}
		heads = append(heads, after)
	}
	require.Equal(t, heads[0], heads[1], "a protected cleanup must preserve agreement on the visible head")
}
