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

func TestRaftMetricsNewFields(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	m := registry.raft

	// Populate the new metrics directly to verify they are registered and exported.
	m.term.WithLabelValues("1").Set(5)
	m.lastLogIndex.WithLabelValues("1").Set(42)
	m.lastSnapshotIndex.WithLabelValues("1").Set(10)
	m.fsmPending.WithLabelValues("1").Set(3)
	m.numPeers.WithLabelValues("1").Set(2)

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_raft_term Current Raft term for each group.
# TYPE elastickv_raft_term gauge
elastickv_raft_term{group="1",node_address="10.0.0.1:50051",node_id="n1"} 5
# HELP elastickv_raft_last_log_index Index of the last log entry written for each group.
# TYPE elastickv_raft_last_log_index gauge
elastickv_raft_last_log_index{group="1",node_address="10.0.0.1:50051",node_id="n1"} 42
# HELP elastickv_raft_last_snapshot_index Index of the most recent snapshot for each group.
# TYPE elastickv_raft_last_snapshot_index gauge
elastickv_raft_last_snapshot_index{group="1",node_address="10.0.0.1:50051",node_id="n1"} 10
# HELP elastickv_raft_fsm_pending Number of commands queued to the FSM but not yet applied for each group.
# TYPE elastickv_raft_fsm_pending gauge
elastickv_raft_fsm_pending{group="1",node_address="10.0.0.1:50051",node_id="n1"} 3
# HELP elastickv_raft_num_peers Number of other voting servers in the cluster for each group, not including this node.
# TYPE elastickv_raft_num_peers gauge
elastickv_raft_num_peers{group="1",node_address="10.0.0.1:50051",node_id="n1"} 2
`),
		"elastickv_raft_term",
		"elastickv_raft_last_log_index",
		"elastickv_raft_last_snapshot_index",
		"elastickv_raft_fsm_pending",
		"elastickv_raft_num_peers",
	)
	require.NoError(t, err)
}

func TestRaftMetricsExportLeaderChangesAndProposalFailures(t *testing.T) {
	registry := NewRegistry("n1", "10.0.0.1:50051")
	observer := registry.RaftObserver()
	require.NotNil(t, observer)

	observer.observeLeaderChange("1")
	registry.RaftProposalObserver(1).ObserveProposalFailure()

	err := testutil.GatherAndCompare(
		registry.Gatherer(),
		strings.NewReader(`
# HELP elastickv_raft_leader_changes_seen_total Total number of observed leader changes for each group.
# TYPE elastickv_raft_leader_changes_seen_total counter
elastickv_raft_leader_changes_seen_total{group="1",node_address="10.0.0.1:50051",node_id="n1"} 1
# HELP elastickv_raft_proposals_failed_total Total number of raft proposals that failed before returning a usable apply response.
# TYPE elastickv_raft_proposals_failed_total counter
elastickv_raft_proposals_failed_total{group="1",node_address="10.0.0.1:50051",node_id="n1"} 1
`),
		"elastickv_raft_leader_changes_seen_total",
		"elastickv_raft_proposals_failed_total",
	)
	require.NoError(t, err)
}
