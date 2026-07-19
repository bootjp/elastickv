package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

type membershipConfigEngine struct {
	noopEngine
	config raftengine.Configuration
}

func (e membershipConfigEngine) Configuration(context.Context) (raftengine.Configuration, error) {
	return e.config, nil
}

func TestShardedCoordinatorRaftMembersIncludesEveryGroupEndpoint(t *testing.T) {
	t.Parallel()

	routes := distribution.NewEngine()
	routes.UpdateRoute(nil, []byte("m"), 1)
	routes.UpdateRoute([]byte("m"), nil, 2)
	groups := map[uint64]*ShardGroup{
		1: {Engine: membershipConfigEngine{config: raftengine.Configuration{Servers: []raftengine.Server{
			{ID: "n1", Address: "127.0.0.1:5101", Suffrage: "voter"},
			{ID: "n2", Address: "127.0.0.1:5102", Suffrage: "voter"},
		}}}},
		2: {Engine: membershipConfigEngine{config: raftengine.Configuration{Servers: []raftengine.Server{
			{ID: "n1", Address: "127.0.0.1:5201", Suffrage: "voter"},
			{ID: "n3", Address: "127.0.0.1:5203", Suffrage: "learner"},
		}}}},
	}
	coordinator := NewShardedCoordinator(routes, groups, 1, NewHLC(), nil)

	members, err := coordinator.RaftMembers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []RaftMember{
		{NodeID: "n1", Address: "127.0.0.1:5101", Suffrage: "voter"},
		{NodeID: "n1", Address: "127.0.0.1:5201", Suffrage: "voter"},
		{NodeID: "n2", Address: "127.0.0.1:5102", Suffrage: "voter"},
		{NodeID: "n3", Address: "127.0.0.1:5203", Suffrage: "learner"},
	}, members)

	keyMembers, err := coordinator.RaftMembersForKey(context.Background(), []byte("z"))
	require.NoError(t, err)
	require.Equal(t, []RaftMember{
		{NodeID: "n1", Address: "127.0.0.1:5201", Suffrage: "voter"},
		{NodeID: "n3", Address: "127.0.0.1:5203", Suffrage: "learner"},
	}, keyMembers)
}
