package kv

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type promotingExactVersionStore struct {
	store.MVCCStore
	afterFirstProbe func()
}

func (s *promotingExactVersionStore) CommittedVersionAt(ctx context.Context, key []byte, ts uint64) (bool, error) {
	exists, err := s.MVCCStore.CommittedVersionAt(ctx, key, ts)
	if s.afterFirstProbe != nil {
		promote := s.afterFirstProbe
		s.afterFirstProbe = nil
		promote()
	}
	return exists, err
}

func TestShardStoreExactVersionDuringPromotion(t *testing.T) {
	t.Parallel()
	for _, fixture := range []struct {
		name     string
		newStore func(*testing.T) (*ShardStore, *ShardGroup)
	}{
		{"memory", newStagedVisibilityShardStore},
		{"pebble", newStagedVisibilityPebbleShardStore},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			for _, tombstone := range []bool{false, true} {
				t.Run(map[bool]string{false: "value", true: "tombstone"}[tombstone], func(t *testing.T) {
					shards, group := fixture.newStore(t)
					checkExactVersionDuringPromotion(t, shards, group, tombstone)
				})
			}
		})
	}
}

func checkExactVersionDuringPromotion(t *testing.T, shards *ShardStore, group *ShardGroup, tombstone bool) {
	t.Helper()
	ctx := t.Context()
	key := []byte("k")
	stagedKey := distribution.MigrationStagedDataKey(9, key)
	if tombstone {
		require.NoError(t, group.Store.DeleteAt(ctx, stagedKey, 77))
	} else {
		require.NoError(t, group.Store.PutAt(ctx, stagedKey, []byte("value"), 77, 0))
	}
	original := group.Store
	promoter, ok := original.(store.MigrationPromoter)
	require.True(t, ok)
	group.Store = &promotingExactVersionStore{MVCCStore: original, afterFirstProbe: func() {
		result, err := promoter.PromoteVersions(ctx, store.PromoteVersionsOptions{
			JobID: 9, StartKey: stagedKey, EndKey: store.PrefixScanEnd(stagedKey), MaxVersions: 1,
			TargetKey: func(staged []byte) ([]byte, bool) {
				_, raw, found := distribution.MigrationStagedDataKeyParts(staged)
				return raw, found && bytes.Equal(raw, key)
			},
		})
		require.NoError(t, err)
		require.Equal(t, uint64(1), result.PromotedRows)
	}}
	exists, err := shards.CommittedVersionAt(ctx, key, 77)
	require.NoError(t, err)
	require.True(t, exists, "promotion must not hide an already committed exact version")
	// The alias has been physically removed; subsequent probes must use live.
	exists, err = shards.CommittedVersionAt(ctx, key, 77)
	require.NoError(t, err)
	require.True(t, exists)
	exists, err = shards.CommittedVersionAt(ctx, key, 76)
	require.NoError(t, err)
	require.False(t, exists, "another timestamp is not evidence of this commit")
}

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
