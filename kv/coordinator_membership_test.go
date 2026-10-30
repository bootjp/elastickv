package kv

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

func TestCoordinateRaftMembers(t *testing.T) {
	t.Parallel()
	engine := membershipConfigEngine(raftengine.Configuration{Servers: []raftengine.Server{
		{ID: "n1", Address: "127.0.0.1:5101", Suffrage: "voter"},
		{ID: "n2", Address: "127.0.0.1:5102", Suffrage: "learner"},
	}})
	coordinate := NewCoordinatorWithEngine(nil, engine)
	members, err := coordinate.RaftMembers(context.Background())
	require.NoError(t, err)
	require.Equal(t, []RaftMember{
		{NodeID: "n1", Address: "127.0.0.1:5101", Suffrage: "voter"},
		{NodeID: "n2", Address: "127.0.0.1:5102", Suffrage: "learner"},
	}, members)
	keyMembers, err := coordinate.RaftMembersForKey(context.Background(), []byte("key"))
	require.NoError(t, err)
	require.Equal(t, members, keyMembers)
	members[0].NodeID = "mutated"
	again, err := coordinate.RaftMembers(context.Background())
	require.NoError(t, err)
	require.Equal(t, "n1", again[0].NodeID)
}

func TestCoordinateRaftMembersMissingEngine(t *testing.T) {
	t.Parallel()
	for _, coordinate := range []*Coordinate{nil, NewCoordinatorWithEngine(nil, nil)} {
		_, err := coordinate.RaftMembers(context.Background())
		require.ErrorIs(t, err, ErrLeaderNotFound)
	}
}
