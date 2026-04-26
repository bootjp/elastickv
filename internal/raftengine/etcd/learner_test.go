package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/stretchr/testify/require"
)

// TestAddLearnerReplicatesWithoutCountingAsVoter is the headline M1
// test from docs/design/2026_04_26_proposed_raft_learner.md exit
// criterion 1: a learner attached to a 1-voter cluster receives log
// entries, but does not count toward the voter quorum.
func TestAddLearnerReplicatesWithoutCountingAsVoter(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	index, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	require.Greater(t, index, uint64(0))

	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	// Suffrage is reported correctly for both peers via Configuration().
	cfg, err := leader.engine.Configuration(ctx)
	require.NoError(t, err)
	require.Len(t, cfg.Servers, 2)
	suffrageByID := map[string]string{}
	for _, s := range cfg.Servers {
		suffrageByID[s.ID] = s.Suffrage
	}
	require.Equal(t, SuffrageVoter, suffrageByID[nodes[0].peer.ID])
	require.Equal(t, SuffrageLearner, suffrageByID[nodes[1].peer.ID])

	// Re-confirm leadership before proposing.
	require.Eventually(t, func() bool {
		return leader.engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)

	// Writes commit on the leader (1 voter is its own quorum) and
	// replicate to the learner.
	result, err := leader.engine.Propose(context.Background(), []byte("alpha"))
	require.NoError(t, err)
	require.NotZero(t, result.CommitIndex)
	require.Eventually(t, func() bool {
		return len(nodes[1].fsm.Applied()) == 1 && string(nodes[1].fsm.Applied()[0]) == "alpha"
	}, 5*time.Second, 20*time.Millisecond)

	// The learner reports learner suffrage in its own configuration view too.
	cfg, err = nodes[1].engine.Configuration(ctx)
	require.NoError(t, err)
	require.Len(t, cfg.Servers, 2)
	for _, s := range cfg.Servers {
		if s.ID == nodes[1].peer.ID {
			require.Equal(t, SuffrageLearner, s.Suffrage)
		}
	}
}

// TestPromoteLearnerSwapsRoleToVoter exercises the engine's promote
// path: AddLearner → wait for catch-up → PromoteLearner → suffrage
// flips to "voter".
func TestPromoteLearnerSwapsRoleToVoter(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	addIndex, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	require.Eventually(t, func() bool {
		return leader.engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)

	// Drive a write through so the learner has caught up to a known
	// commit index before promotion.
	_, err = leader.engine.Propose(context.Background(), []byte("warm"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(nodes[1].fsm.Applied()) == 1
	}, 5*time.Second, 20*time.Millisecond)

	promoteIndex, err := leader.engine.PromoteLearner(ctx, nodes[1].peer.ID, addIndex, 0)
	require.NoError(t, err)
	require.Greater(t, promoteIndex, addIndex)

	// Both peers should report two voters and zero learners after promotion.
	require.Eventually(t, func() bool {
		cfg, err := leader.engine.Configuration(context.Background())
		if err != nil {
			return false
		}
		voters := 0
		for _, s := range cfg.Servers {
			if s.Suffrage == SuffrageVoter {
				voters++
			}
		}
		return voters == 2
	}, 5*time.Second, 20*time.Millisecond)
}

// TestPromoteLearnerRejectsNonLearner ensures the precondition
// (target must be in ConfState.Learners) is enforced before propose.
func TestPromoteLearnerRejectsNonLearner(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	// Add as a regular voter, then attempt to promote — should fail.
	_, err := leader.engine.AddVoter(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)

	_, err = leader.engine.PromoteLearner(ctx, nodes[1].peer.ID, 0, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, errPromoteLearnerNotLearner), "expected errPromoteLearnerNotLearner, got %v", err)
}

// TestPromoteLearnerRejectsNotCaughtUp ensures the
// minAppliedIndex precondition is enforced pre-propose.
func TestPromoteLearnerRejectsNotCaughtUp(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	addIndex, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)

	// Demand catch-up to a future index that has not been committed.
	const unreachableIndex = uint64(1 << 60)
	_, err = leader.engine.PromoteLearner(ctx, nodes[1].peer.ID, addIndex, unreachableIndex)
	require.Error(t, err)
	require.True(t, errors.Is(err, errPromoteLearnerNotCaughtUp), "expected errPromoteLearnerNotCaughtUp, got %v", err)
}

// TestRemovePeerLearnerKeepsSingleNodeFastPath is the §4.6 lease-read
// regression: with 1 voter + 1 learner, removing the learner must
// leave the leader on the single-node fast path. The complementary
// case (lease-read works WHILE the learner exists) is covered
// implicitly by the AddLearner test above succeeding without a
// quorum-ack timeout.
func TestRemovePeerLearnerKeepsSingleNodeFastPath(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	addIndex, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)

	// While the learner is attached, the leader's voter count is still
	// 1, so the single-node fast path stays warm.
	require.Eventually(t, func() bool {
		return !leader.engine.LastQuorumAck().IsZero()
	}, 2*time.Second, 20*time.Millisecond)

	_, err = leader.engine.RemoveServer(ctx, nodes[1].peer.ID, addIndex)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 1)

	// After removal, the fast path is still warm — voter count stayed at 1.
	require.Eventually(t, func() bool {
		return !leader.engine.LastQuorumAck().IsZero()
	}, 2*time.Second, 20*time.Millisecond)
}
