package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// TestStatusPerPeerReportsLearnerProgressOnTheLeader closes the
// Milestone 3 hardening item from
// docs/design/2026_04_26_implemented_raft_learner.md §6: an operator
// choosing PromoteLearner's min_applied_index had no way to read the
// learner's actual Match, so the value was a guess — too high fails
// the precondition, too low promotes a replica that has not caught up.
func TestStatusPerPeerReportsLearnerProgressOnTheLeader(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	_, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	learnerNodeID := nodes[1].peer.NodeID

	// The leader eventually reports the learner's replication progress.
	var progress raftengine.PeerProgress
	require.Eventually(t, func() bool {
		status := leader.engine.Status()
		p, ok := status.PerPeer[learnerNodeID]
		if !ok || p.Match == 0 {
			return false
		}
		progress = p
		return true
	}, 10*time.Second, 25*time.Millisecond,
		"the leader must report the learner's Match so min_applied_index need not be guessed")

	require.True(t, progress.IsLearner,
		"progress must come from the live tracker, which knows this peer is a learner")
	require.GreaterOrEqual(t, progress.Next, progress.Match)

	// The CORRECT use: watch Match climb to a target and pass the
	// TARGET. Passing the learner's own current Match would satisfy
	// the engine's Match >= minAppliedIndex check by construction,
	// turning the precondition into a no-op that promotes a lagging
	// replica into the voter quorum.
	var target uint64
	require.Eventually(t, func() bool {
		status := leader.engine.Status()
		target = status.CommitIndex
		p, ok := status.PerPeer[learnerNodeID]
		return ok && target > 0 && p.Match >= target
	}, 10*time.Second, 25*time.Millisecond,
		"the learner must be observed catching up to the leader's commit index")

	_, err = leader.engine.PromoteLearner(ctx, nodes[1].peer.ID, 0, target, false)
	require.NoError(t, err,
		"promotion at the observed catch-up target must satisfy the precondition")
}

// TestStatusPerPeerIsEmptyNotNilOnAPeerlessLeader pins the other half
// of the nil-versus-empty contract. A single-node leader has no remote
// replicas, but it IS the leader — reporting nil would make it
// indistinguishable from a follower to any consumer testing
// PerPeer == nil.
func TestStatusPerPeerIsEmptyNotNilOnAPeerlessLeader(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 1)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])

	status := leader.engine.Status()
	require.Equal(t, raftengine.StateLeader, status.State)
	require.NotNil(t, status.PerPeer,
		"a leader with no remote replicas must report an empty map, not nil")
	require.Empty(t, status.PerPeer)
}

// TestStatusPerPeerIsNilOnAFollower pins the nil-versus-empty
// distinction. Only the leader tracks progress, and an empty map on a
// follower would be indistinguishable from "the leader knows about no
// peers" — an operator could read that as a healthy single-node
// cluster.
func TestStatusPerPeerIsNilOnAFollower(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	_, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, nodes[1].engine, 2)

	require.Eventually(t, func() bool {
		return nodes[1].engine.Status().State != raftengine.StateLeader
	}, 5*time.Second, 25*time.Millisecond)

	require.Nil(t, nodes[1].engine.Status().PerPeer,
		"a follower tracks no peer progress and must report nil, not an empty map")
}

// TestStatusPerPeerExcludesTheLocalNode keeps the map to REMOTE
// replicas. The leader's own tracker entry is about itself and would
// invite an operator to read their own node as a promotion candidate.
func TestStatusPerPeerExcludesTheLocalNode(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	_, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)

	require.Eventually(t, func() bool {
		return len(leader.engine.Status().PerPeer) > 0
	}, 10*time.Second, 25*time.Millisecond)

	perPeer := leader.engine.Status().PerPeer
	require.NotContains(t, perPeer, nodes[0].peer.NodeID,
		"the leader's own tracker entry must not appear among remote peers")
	require.Contains(t, perPeer, nodes[1].peer.NodeID)
}
