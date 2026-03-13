package monitoring

import (
	"strings"
	"testing"

	"github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestRaftObserverSetMembersReplacesRemovedMembers(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RaftObserver()
	require.NotNil(t, observer)

	observer.setLeaderMetric("1", "n2", "10.0.0.2:50051")
	observer.setMembers("1", "n2", []raft.Server{
		{ID: "n1", Address: "10.0.0.1:50051", Suffrage: raft.Voter},
		{ID: "n2", Address: "10.0.0.2:50051", Suffrage: raft.Voter},
	})
	observer.setLeaderMetric("1", "n1", "10.0.0.1:50051")
	observer.setMembers("1", "n1", []raft.Server{
		{ID: "n1", Address: "10.0.0.1:50051", Suffrage: raft.Voter},
	})

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_raft_leader_identity Current leader identity for each raft group, as observed by this node.
# TYPE elastickv_raft_leader_identity gauge
elastickv_raft_leader_identity{group="1",leader_address="10.0.0.1:50051",leader_id="n1",node_address="10.0.0.1:50051",node_id="n1"} 1
# HELP elastickv_raft_member_present Current raft configuration members known to this node.
# TYPE elastickv_raft_member_present gauge
elastickv_raft_member_present{group="1",member_address="10.0.0.1:50051",member_id="n1",node_address="10.0.0.1:50051",node_id="n1",suffrage="voter"} 1
`),
		"elastickv_raft_leader_identity",
		"elastickv_raft_member_present",
	)
	require.NoError(t, err)
}

func TestParseLastContactSeconds(t *testing.T) {
	require.Equal(t, float64(lastContactUnknownValue), parseLastContactSeconds(""))
	require.Equal(t, float64(lastContactUnknownValue), parseLastContactSeconds("never"))
	require.Equal(t, 0.0, parseLastContactSeconds("0"))
	require.Equal(t, 0.25, parseLastContactSeconds("250ms"))
	require.Equal(t, float64(lastContactUnknownValue), parseLastContactSeconds("invalid"))
}
