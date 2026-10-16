package kv

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// singleGroupLeader is a coordinator that fronts one Raft group and so has
// no group-keyed leadership methods, like *Coordinate.
type singleGroupLeader struct {
	Coordinator

	leader bool
	addr   string
}

func (c singleGroupLeader) IsLeader() bool     { return c.leader }
func (c singleGroupLeader) RaftLeader() string { return c.addr }

// groupLeader answers leadership per group.
type groupLeader struct {
	Coordinator

	leaders map[uint64]string
	local   uint64
}

func (c groupLeader) IsLeaderForGroup(groupID uint64) bool     { return groupID == c.local }
func (c groupLeader) RaftLeaderForGroup(groupID uint64) string { return c.leaders[groupID] }

// The forwarder exposes GroupLeaderRoutableCoordinator, so the Redis read
// fence asks it for group leadership whenever a fence target carries a group
// ID. A single-group coordinator's one group is the group being fenced, so
// the answer must be its own leadership, not "no leader".
func TestCoordinatorForwarderGroupLeadershipFallsBackToSingleGroupLeader(t *testing.T) {
	t.Parallel()

	for _, leader := range []bool{true, false} {
		f := CoordinatorForwarder{Inner: singleGroupLeader{leader: leader, addr: "n1:50051"}}
		require.Equal(t, leader, f.IsLeaderForGroup(1), "leader=%v", leader)
		require.Equal(t, "n1:50051", f.RaftLeaderForGroup(1), "leader=%v", leader)
	}
}

func TestCoordinatorForwarderGroupLeadershipDelegatesToGroupRouter(t *testing.T) {
	t.Parallel()

	f := CoordinatorForwarder{Inner: groupLeader{
		leaders: map[uint64]string{1: "n1:50051", 2: "n2:50051"},
		local:   1,
	}}
	require.True(t, f.IsLeaderForGroup(1))
	require.False(t, f.IsLeaderForGroup(2))
	require.Equal(t, "n2:50051", f.RaftLeaderForGroup(2))
}
