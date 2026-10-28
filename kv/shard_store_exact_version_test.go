package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestShardStoreAuthoritativeExactVersionProxiesFollower(t *testing.T) {
	t.Parallel()
	server := &fakeRawKVServer{latestResp: &pb.RawLatestCommitTSResponse{
		ExactVersionExists: true, ExactVersionSupported: true,
	}}
	address := startRawKVServer(t, server)
	routes := distribution.NewEngine()
	routes.UpdateRoute([]byte("a"), nil, 1)
	follower := newStubFollowerEngine(address)
	shards := NewShardStore(routes, map[uint64]*ShardGroup{
		1: {Engine: follower, Store: store.NewMVCCStore()},
	})
	t.Cleanup(func() { _ = shards.connCache.Close() })

	exists, err := shards.CommittedVersionAtAuthoritative(context.Background(), []byte("key"), 77)
	require.NoError(t, err)
	require.True(t, exists)
	server.mu.Lock()
	require.Equal(t, uint64(77), server.lastLatestReq.GetExactVersionAtTs())
	server.latestResp.ExactVersionSupported = false
	server.mu.Unlock()
	_, err = shards.CommittedVersionAtAuthoritative(context.Background(), []byte("key"), 77)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	server.mu.Lock()
	server.latestResp.ExactVersionSupported = true
	server.mu.Unlock()
	exists, err = shards.CommittedVersionAtGroupAuthoritative(context.Background(), []byte("receipt"), 1, 77)
	require.NoError(t, err)
	require.True(t, exists)
	server.mu.Lock()
	require.Equal(t, uint64(1), server.lastLatestReq.GetGroupId())
	require.Equal(t, []byte("receipt"), server.lastLatestReq.GetKey())
	server.mu.Unlock()
}
