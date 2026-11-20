package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/testutil/grpctest"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestFollowerVersionReadEncryptedTTL(t *testing.T) {
	keys := encryption.NewKeystore()
	require.NoError(t, keys.Set(1, make([]byte, encryption.KeySize)))
	cipher, err := encryption.NewCipher(keys)
	require.NoError(t, err)
	primary, err := store.NewPebbleStore(t.TempDir(), store.WithEncryption(cipher,
		encryption.NewDeterministicNonceFactory(1, 1), func() (uint32, bool) { return 1, true }))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, primary.Close()) })
	key := []byte("ttl-key")
	require.NoError(t, primary.PutAt(t.Context(), key, []byte("secret"), 10, 20))
	require.NoError(t, primary.PutAt(t.Context(), []byte("watermark"), []byte("later"), 30, 0))

	routes := distribution.NewEngine()
	require.NoError(t, routes.ApplySnapshot(distribution.CatalogSnapshot{Version: 1,
		Routes: []distribution.RouteDescriptor{{RouteID: 1, GroupID: 1, State: distribution.RouteStateActive}}}))
	makeShards := func(st store.MVCCStore, engine raftengine.Engine) *kv.ShardStore {
		group := &kv.ShardGroup{Store: st, Engine: engine}
		group.ConfigureVersionReadAdmission(func(context.Context) error { return nil })
		shards := kv.NewShardStore(routes, map[uint64]*kv.ShardGroup{1: group})
		t.Cleanup(func() { require.NoError(t, shards.Close()) })
		return shards
	}
	leader := makeShards(primary, &localCompactionReadEngine{})
	addr := grpctest.Serve(t, func(s *grpc.Server) {
		pb.RegisterRawKVServer(s, NewGRPCServer(leader, &stubAdapterCoordinator{}))
	})
	empty := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, empty.Close()) })
	follower := makeShards(empty, &casRPCFollower{leader: raftengine.LeaderInfo{ID: "leader", Address: addr}})

	got, err := follower.GetVersionAt(t.Context(), key, 19)
	require.NoError(t, err)
	require.Equal(t, []byte("secret"), got.Value)
	require.EqualValues(t, 10, got.TS)
	_, err = follower.GetVersionAt(t.Context(), key, 20)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
}
