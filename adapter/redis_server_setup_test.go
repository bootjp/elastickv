package adapter

import (
	"testing"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

func TestRedisServerUsesLeaderRoutedStoreInSingleGroupCluster(t *testing.T) {
	t.Parallel()

	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)

	require.IsType(t, &kv.LeaderRoutedStore{}, nodes[0].redisServer.store)
	require.IsType(t, &kv.LeaderRoutedStore{}, nodes[1].redisServer.store)
	require.IsType(t, &kv.LeaderRoutedStore{}, nodes[2].redisServer.store)
}
