package adapter

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestGRPCVersionedPointReads(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	seedVersionReadCatalog(t, st, 1)
	key := []byte("key")
	require.NoError(t, st.PutAt(ctx, key, []byte("old"), 10, 0))
	require.NoError(t, st.PutAt(ctx, key, []byte{}, 20, 0))
	require.NoError(t, st.PutAt(ctx, []byte("other-key"), []byte("other"), 30, 0))
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive}},
	}))
	group := &kv.ShardGroup{Store: st, Engine: &localCompactionReadEngine{}}
	group.ConfigureVersionReadAdmission(func(context.Context) error { return nil })
	shards := kv.NewShardStore(engine, map[uint64]*kv.ShardGroup{1: group})
	t.Cleanup(func() { require.NoError(t, shards.Close()) })
	server := NewGRPCServer(shards, &stubAdapterCoordinator{})
	addr := grpctest.Serve(t, func(grpcServer *grpc.Server) {
		pb.RegisterRawKVServer(grpcServer, server)
		pb.RegisterTransactionalKVServer(grpcServer, server)
	})
	conn := grpctest.Dial(t, addr)

	txn := pb.NewTransactionalKVClient(conn)
	got, err := txn.Get(ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, &pb.GetResponse_CommitTs{CommitTs: 20}, got.GetVersion())
	require.Empty(t, got.GetValue())

	got, err = txn.Get(ctx, &pb.GetRequest{Key: []byte("missing"), NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, &pb.GetResponse_Missing{Missing: true}, got.GetVersion())
	got, err = txn.Get(ctx, &pb.GetRequest{Key: key})
	require.NoError(t, err)
	require.Nil(t, got.GetVersion())

	raw := pb.NewRawKVClient(conn)
	latest, err := raw.RawGet(ctx, &pb.RawGetRequest{Key: key, NeedCommitTs: true})
	require.NoError(t, err)
	require.True(t, latest.GetExists())
	require.Equal(t, &pb.RawGetResponse_CommitTs{CommitTs: 20}, latest.GetVersion())
	historical, err := raw.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: 15, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, &pb.RawGetResponse_CommitTs{CommitTs: 10}, historical.GetVersion())
	require.Equal(t, []byte("old"), historical.GetValue())
	empty, err := raw.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: 30, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, &pb.RawGetResponse_CommitTs{CommitTs: 20}, empty.GetVersion())
	require.True(t, empty.GetExists())
	require.Empty(t, empty.GetValue())
	missing, err := raw.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: 5, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, &pb.RawGetResponse_Missing{Missing: true}, missing.GetVersion())
	require.False(t, missing.GetExists())
	legacy, err := raw.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: 15})
	require.NoError(t, err)
	require.Nil(t, legacy.GetVersion())
	require.Equal(t, []byte("old"), legacy.GetValue())
}

func TestGRPCVersionedPointReadRequiresVersionAwareRoute(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := &recordingRawGroupStore{MVCCStore: store.NewMVCCStore()}
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	key := []byte("key")
	require.NoError(t, st.PutAt(ctx, key, []byte("value"), 10, 0))
	server := NewGRPCServer(st, &stubAdapterCoordinator{})
	_, err := server.Get(ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = server.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: 10, GroupId: 42, NeedCommitTs: true})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.False(t, st.fallbackGet)
	require.Zero(t, st.getGroupID)

	fenceErr := context.DeadlineExceeded
	server = NewGRPCServer(st, &stubAdapterCoordinator{verifyLeaderErr: fenceErr})
	_, err = server.Get(ctx, &pb.GetRequest{Key: key, NeedCommitTs: true})
	require.ErrorIs(t, err, fenceErr)
}

func TestGRPCVersionedRawGetHonorsExplicitGroup(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 8,
		Routes:  []distribution.RouteDescriptor{{RouteID: 1, GroupID: 2, State: distribution.RouteStateActive}},
	}))
	groups := map[uint64]*kv.ShardGroup{
		1: {Store: store.NewMVCCStore()},
		2: {Store: store.NewMVCCStore()},
	}
	for _, group := range groups {
		t.Cleanup(func() { require.NoError(t, group.Store.Close()) })
	}
	groups[2].ConfigureVersionReadAdmission(func(context.Context) error { return nil })
	seedVersionReadCatalog(t, groups[2].Store, 2)
	st := kv.NewShardStore(engine, groups)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	key := []byte("key")
	require.NoError(t, groups[1].Store.PutAt(ctx, key, []byte("routed"), 10, 0))
	require.NoError(t, groups[2].Store.PutAt(ctx, key, []byte("explicit"), 20, 0))
	server := NewGRPCServer(st, nil)

	got, err := server.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: 30, GroupId: 2, ReadRouteVersion: 8, NeedCommitTs: true})
	require.NoError(t, err)
	require.Equal(t, &pb.RawGetResponse_CommitTs{CommitTs: 20}, got.GetVersion())
	require.Equal(t, []byte("explicit"), got.GetValue())

	_, err = server.RawGet(ctx, &pb.RawGetRequest{Key: key, Ts: 30, GroupId: 1, ReadRouteVersion: 8, NeedCommitTs: true})
	require.Equal(t, codes.Unimplemented, status.Code(err))
}

func seedVersionReadCatalog(t *testing.T, st store.MVCCStore, groupID uint64) {
	t.Helper()
	plan, err := distribution.NewCatalogStore(st).BuildBootstrapPlanAt(t.Context(), 1, []distribution.Route{
		{GroupID: groupID, State: distribution.RouteStateActive},
	})
	require.NoError(t, err)
	require.NoError(t, st.ApplyMutationsRaftAtFenced(t.Context(), plan.Mutations, plan.ReadKeys, 1, 2, 0))
}
