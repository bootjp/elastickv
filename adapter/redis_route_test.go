package adapter

import (
	"bytes"
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/redcon"
)

type recordingRedisRouteCoordinator struct {
	stubAdapterCoordinator
	isLeaderKeys [][]byte
	verifyKeys   [][]byte
}

func (c *recordingRedisRouteCoordinator) IsLeaderForKey(key []byte) bool {
	c.isLeaderKeys = append(c.isLeaderKeys, bytes.Clone(key))
	return true
}

func (c *recordingRedisRouteCoordinator) VerifyLeaderForKey(_ context.Context, key []byte) error {
	c.verifyKeys = append(c.verifyKeys, bytes.Clone(key))
	return nil
}

func TestRedisUserRouteKeyPreservesListStorageShapedUserKey(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		userKey []byte
	}{
		{"empty", []byte{}},
		{"delta", store.ListMetaDeltaKey([]byte("other"), 1, 1)},
		{"claim", store.ListClaimKey([]byte("other"), 3)},
		{"meta", store.ListMetaKey([]byte("other"))},
		{"item", store.ListItemKey([]byte("other"), 5)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.userKey, kv.RouteKey(redisUserRouteKey(tc.userKey)))
		})
	}
}

func TestProxyToLeaderRoutesRawRedisKeyThroughLiteralWrapper(t *testing.T) {
	t.Parallel()

	userKey := store.ListMetaDeltaKey([]byte("other"), 1, 1)
	coord := &recordingRedisRouteCoordinator{}
	server := &RedisServer{coordinator: coord}

	proxied := server.proxyToLeader(&recordingConn{}, redcon.Command{
		Args: [][]byte{[]byte("GET"), userKey},
	}, userKey)

	require.False(t, proxied)
	require.Equal(t, [][]byte{redisUserRouteKey(userKey)}, coord.isLeaderKeys)
}

func TestLeaderAwareGetAtRoutesRedisInternalKeyThroughLiteralWrapper(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	userKey := store.ListMetaDeltaKey([]byte("other"), 1, 1)
	storageKey := redisStrKey(userKey)
	st := store.NewMVCCStore()
	require.NoError(t, st.PutAt(ctx, storageKey, []byte("value"), 1, 0))
	coord := &recordingRedisRouteCoordinator{}
	server := &RedisServer{store: st, coordinator: coord}

	got, err := server.leaderAwareGetAt(storageKey, 1)

	require.NoError(t, err)
	require.Equal(t, []byte("value"), got)
	require.Equal(t, [][]byte{redisUserRouteKey(userKey)}, coord.isLeaderKeys)
	require.Equal(t, [][]byte{redisUserRouteKey(userKey)}, coord.verifyKeys)
}
