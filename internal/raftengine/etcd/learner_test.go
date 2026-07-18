package etcd

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// TestAddLearnerReplicatesWithoutCountingAsVoter is the headline M1
// test from docs/design/2026_04_26_implemented_raft_learner.md exit
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
	warmResult, err := leader.engine.Propose(context.Background(), []byte("warm"))
	require.NoError(t, err)
	require.NotZero(t, warmResult.CommitIndex)
	require.Eventually(t, func() bool {
		return len(nodes[1].fsm.Applied()) == 1
	}, 5*time.Second, 20*time.Millisecond)

	// We just propagated "warm" to the learner; use that committed
	// commit index as the catch-up bar so promotion is gated on the
	// learner having actually applied something.
	var promoteIndex uint64
	var promoteErr error
	require.Eventually(t, func() bool {
		promoteIndex, promoteErr = leader.engine.PromoteLearner(ctx, nodes[1].peer.ID, addIndex, warmResult.CommitIndex, false)
		if promoteErr == nil {
			return true
		}
		if errors.Is(promoteErr, errPromoteLearnerNotCaughtUp) {
			return false
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	err = promoteErr
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

func TestFreshLearnerJoinPromotesAndRestartsFromPersistedMembership(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 3)
	activeNodes := nodes[:2]
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, activeNodes[:1])

	_, ok, err := LoadPersistedPeers(nodes[1].dir)
	require.NoError(t, err)
	require.False(t, ok, "fresh joiner must start without copied peer metadata")

	nodes[1].joinAsLearner = true
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))
	joinerConfig, err := nodes[1].engine.Configuration(ctx)
	require.NoError(t, err)
	require.Empty(t, joinerConfig.Servers, "join transport peers must not bootstrap ConfState")

	addIndex, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	warm, err := leader.engine.Propose(ctx, []byte("fresh-join"))
	require.NoError(t, err)
	requireTransportNodeAppliedValue(t, nodes[1], "fresh-join")
	requireLearnerPromoted(t, ctx, leader.engine, nodes[1].peer.ID, addIndex, warm.CommitIndex)
	requireTransportNodeSuffrage(t, ctx, nodes[1], SuffrageVoter, 2)
	requirePersistedPeersExcludeNode(t, nodes[1].dir, nodes[2].peer.NodeID, 2)
	restartTransportTestNodesWithoutJoin(t, ctx, activeNodes)

	restartedLeader := waitForLeaderNode(t, activeNodes)
	requireTransportNodesConfigSize(t, ctx, activeNodes, 2)
	result, err := restartedLeader.engine.Propose(ctx, []byte("after-full-restart"))
	require.NoError(t, err)
	require.NotZero(t, result.CommitIndex)
}

func TestFreshLearnerJoinReusesRemovedVoterID(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 3)
	for i := range nodes {
		peers[i].NodeID = DeriveNodeID(peers[i].ID)
		nodes[i].peer.NodeID = peers[i].NodeID
	}
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	_, err := leader.engine.AddVoter(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)
	require.NoError(t, openTransportTestNode(ctx, nodes[2], peers, false))
	_, err = leader.engine.AddVoter(ctx, nodes[2].peer.ID, nodes[2].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 3)
	waitForConfigSize(t, nodes[1].engine, 3)
	waitForConfigSize(t, nodes[2].engine, 3)

	require.NoError(t, nodes[1].engine.Close())
	nodes[1].engine = nil
	leader = waitForLeaderNode(t, []*transportTestNode{nodes[0], nodes[2]})
	_, err = leader.engine.RemoveServer(ctx, nodes[1].peer.ID, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[0].engine, 2)
	waitForConfigSize(t, nodes[2].engine, 2)

	require.NoError(t, os.RemoveAll(nodes[1].dir))
	require.NoError(t, os.MkdirAll(nodes[1].dir, 0o750))
	nodes[1].fsm = &testStateMachine{}
	nodes[1].joinAsLearner = true
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	joinerConfig, err := nodes[1].engine.Configuration(ctx)
	require.NoError(t, err)
	require.Empty(t, joinerConfig.Servers, "wiped replacement must not recover its removed voter role")

	addIndex, err := leader.engine.AddLearner(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 3)
	waitForConfigSize(t, nodes[1].engine, 3)
	waitForConfigSize(t, nodes[2].engine, 3)

	warm, err := leader.engine.Propose(ctx, []byte("same-id-replacement"))
	require.NoError(t, err)
	requireTransportNodeAppliedValue(t, nodes[1], "same-id-replacement")
	requireLearnerPromoted(t, ctx, leader.engine, nodes[1].peer.ID, addIndex, warm.CommitIndex)
	requireTransportNodeSuffrage(t, ctx, nodes[1], SuffrageVoter, 3)

	restartTransportTestNodesWithoutJoin(t, ctx, nodes)
	restartedLeader := waitForLeaderNode(t, nodes)
	requireTransportNodesConfigSize(t, ctx, nodes, 3)
	result, err := restartedLeader.engine.Propose(ctx, []byte("after-same-id-restart"))
	require.NoError(t, err)
	require.NotZero(t, result.CommitIndex)
}

func requireTransportNodeAppliedValue(t *testing.T, node *transportTestNode, value string) {
	t.Helper()
	require.Eventually(t, func() bool {
		applied := node.fsm.Applied()
		return len(applied) == 1 && string(applied[0]) == value
	}, 5*time.Second, 20*time.Millisecond)
}

func requireLearnerPromoted(
	t *testing.T,
	ctx context.Context,
	engine *Engine,
	learnerID string,
	addIndex uint64,
	minAppliedIndex uint64,
) {
	t.Helper()
	var promoteErr error
	require.Eventually(t, func() bool {
		_, promoteErr = engine.PromoteLearner(ctx, learnerID, addIndex, minAppliedIndex, false)
		return promoteErr == nil || !errors.Is(promoteErr, errPromoteLearnerNotCaughtUp)
	}, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, promoteErr)
}

func requireTransportNodeSuffrage(
	t *testing.T,
	ctx context.Context,
	node *transportTestNode,
	suffrage string,
	configSize int,
) {
	t.Helper()
	require.Eventually(t, func() bool {
		cfg, err := node.engine.Configuration(ctx)
		if err != nil || len(cfg.Servers) != configSize {
			return false
		}
		for _, server := range cfg.Servers {
			if server.ID == node.peer.ID {
				return server.Suffrage == suffrage
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
}

func requirePersistedPeersExcludeNode(t *testing.T, dataDir string, excludedNodeID uint64, expectedSize int) {
	t.Helper()
	persisted, ok, err := LoadPersistedPeers(dataDir)
	require.NoError(t, err)
	require.True(t, ok)
	require.Len(t, persisted, expectedSize, "authoritative ConfState must prune stale discovery peers")
	for _, peer := range persisted {
		require.NotEqual(t, excludedNodeID, peer.NodeID)
	}
}

func restartTransportTestNodesWithoutJoin(t *testing.T, ctx context.Context, nodes []*transportTestNode) {
	t.Helper()
	for _, node := range nodes {
		require.NoError(t, node.engine.Close())
		node.engine = nil
		node.joinAsLearner = false
	}

	for _, node := range nodes {
		reopened, err := Open(ctx, OpenConfig{
			NodeID:       node.peer.NodeID,
			LocalID:      node.peer.ID,
			LocalAddress: node.peer.Address,
			DataDir:      node.dir,
			Transport:    node.transport,
			StateMachine: node.fsm,
		})
		require.NoError(t, err, "%s must restart from durable membership without join peers", node.peer.ID)
		node.engine = reopened
	}
}

func requireTransportNodesConfigSize(t *testing.T, ctx context.Context, nodes []*transportTestNode, expected int) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, node := range nodes {
			cfg, err := node.engine.Configuration(ctx)
			if err != nil || len(cfg.Servers) != expected {
				return false
			}
		}
		return true
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

	_, err = leader.engine.PromoteLearner(ctx, nodes[1].peer.ID, 0, 0, true)
	require.Error(t, err)
	require.True(t, errors.Is(err, errPromoteLearnerNotLearner), "expected errPromoteLearnerNotLearner, got %v", err)
}

// TestLearnerPeersFilePersistsSuffrageAcrossRestart locks down the
// design doc §4.3 contract that the v2 peers file round-trips
// suffrage across restarts. Without this, AddLearner writes the
// learner peer with Suffrage="" (the ConfChange context bytes do
// not carry suffrage and e.peers stores only nodeID/ID/address);
// persistedSuffrageByte("") writes voter; on restart
// validateConfState sees a learner-as-voter peers list and rejects
// startup with errClusterMismatch. The fix is that
// nextPeersAfterConfigChange annotates Peer.Suffrage from the
// post-change ConfState before persistConfigState writes the file.
func TestLearnerPeersFilePersistsSuffrageAcrossRestart(t *testing.T) {
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

	// Drive a write so the learner has applied something — without
	// this the engines may race close vs. apply on the bootstrap
	// snapshot and surface unrelated errors.
	require.Eventually(t, func() bool {
		return leader.engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)
	_, err = leader.engine.Propose(context.Background(), []byte("warm"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(nodes[1].fsm.Applied()) >= 1
	}, 5*time.Second, 20*time.Millisecond)

	// Verify the v2 peers file actually carries the learner's suffrage.
	// Both nodes must have flushed the post-AddLearner config, so check
	// each persisted file directly.
	for _, n := range nodes {
		persisted, ok, err := LoadPersistedPeers(n.dir)
		require.NoError(t, err, "load persisted peers for %s", n.peer.ID)
		require.True(t, ok, "persisted peers exist for %s", n.peer.ID)
		var sawLearner bool
		for _, p := range persisted {
			if p.NodeID == nodes[1].peer.NodeID {
				require.Equal(t, SuffrageLearner, p.Suffrage,
					"learner persisted as %q in %s", p.Suffrage, n.peer.ID)
				sawLearner = true
			}
		}
		require.True(t, sawLearner, "learner peer missing from %s peers file", n.peer.ID)
	}

	// Close both engines and reopen them. Without the suffrage
	// round-trip, validateConfState would reject startup because
	// the persisted peers file would list the learner as a voter
	// while the snapshot ConfState lists it as a learner.
	require.NoError(t, leader.engine.Close())
	require.NoError(t, nodes[1].engine.Close())

	for _, n := range nodes {
		// Stale fsm state from the earlier run does not matter; we
		// only care that Open() does not fail with errClusterMismatch.
		engine, err := Open(ctx, OpenConfig{
			NodeID:       n.peer.NodeID,
			LocalID:      n.peer.ID,
			LocalAddress: n.peer.Address,
			DataDir:      n.dir,
			Transport:    n.transport,
			StateMachine: n.fsm,
		})
		require.NoError(t, err, "reopen failed for %s", n.peer.ID)
		n.engine = engine
	}
}

// TestPromoteLearnerRejectsZeroMinAppliedWithoutSkip is the §8 open
// question 3 fix: passing min_applied_index=0 without
// skip_min_applied_check returns a clean FailedPrecondition rather
// than silently disabling the catch-up safety check.
func TestPromoteLearnerRejectsZeroMinAppliedWithoutSkip(t *testing.T) {
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

	// minApplied=0, skip=false  ->  rejected up-front before any
	// rawNode interaction.
	_, err = leader.engine.PromoteLearner(ctx, nodes[1].peer.ID, addIndex, 0, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errPromoteLearnerMinAppliedZero), "expected errPromoteLearnerMinAppliedZero, got %v", err)
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
	_, err = leader.engine.PromoteLearner(ctx, nodes[1].peer.ID, addIndex, unreachableIndex, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errPromoteLearnerNotCaughtUp), "expected errPromoteLearnerNotCaughtUp, got %v", err)
}

// TestJoinAsLearnerAlarmFiresWhenAddedAsVoter exercises the §4.5
// post-apply local alarm: a node booted with JoinAsLearner=true that
// then sees itself in ConfState.Voters logs an ERROR and bumps the
// process-wide JoinRoleViolationCount counter, but keeps running.
func TestJoinAsLearnerAlarmFiresWhenAddedAsVoter(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])

	// Boot the joiner with the alarm flag set, then add it as a VOTER
	// (not a learner) — that's the misuse the alarm catches.
	nodes[1].joinAsLearner = true
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))

	before := JoinRoleViolationCount()
	_, err := leader.engine.AddVoter(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	require.Eventually(t, func() bool {
		return JoinRoleViolationCount() > before
	}, 5*time.Second, 20*time.Millisecond, "expected join-as-learner alarm to fire")

	// Joiner stays running -- §4.5: shutdown is explicitly rejected.
	require.NotEqual(t, raftengine.StateShutdown, nodes[1].engine.State())
}

// TestLinearizableReadOnLearnerForwardsToLeader is the §4.6 / §5.5
// behaviour test: LinearizableRead on a learner forwards to the
// leader's ReadIndex and returns once local apply catches up. A
// learner must NOT serve LinearizableRead from a leader-local fast
// path (it isn't leader; LeaderView guards return errNotLeader).
// Until follower-served reads land in a separate proposal,
// "learner LinearizableRead" is the same code path as a voter
// follower's: it forwards to the leader.
func TestLinearizableReadOnLearnerForwardsToLeader(t *testing.T) {
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

	require.Eventually(t, func() bool {
		return leader.engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)

	// Drive a write through the leader. This commits at the leader's
	// next index; the learner replicates and applies.
	_, err = leader.engine.Propose(context.Background(), []byte("payload"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(nodes[1].fsm.Applied()) >= 1
	}, 5*time.Second, 20*time.Millisecond)

	// LinearizableRead on the leader returns the latest commit index.
	leaderIdx, err := leader.engine.LinearizableRead(ctx)
	require.NoError(t, err)
	require.NotZero(t, leaderIdx)

	// LinearizableRead on the learner: today we return errNotLeader
	// (the learner is a follower, and follower-served reads are an
	// explicit non-goal of this milestone). Guarantee that surface
	// with a typed error so any future regression that lets the
	// learner accidentally answer LinearizableRead from local FSM
	// gets caught here.
	_, err = nodes[1].engine.LinearizableRead(ctx)
	require.Error(t, err)
	require.True(t, errors.Is(err, raftengine.ErrNotLeader), "expected ErrNotLeader, got %v", err)
}

// TestJoinAsLearnerAlarmFiresAtStartupForPersistedVoterRole is the
// regression for the codex Round 4 finding: when a node previously
// mis-joined as voter, then restarts with --raftJoinAsLearner=true,
// the alarm needs to fire even though no apply event happens — Open
// loads the post-mis-join ConfState from disk and never replays it.
// Verifies that JoinRoleViolationCount increments on the second
// Open and that the joiner stays running.
func TestJoinAsLearnerAlarmFiresAtStartupForPersistedVoterRole(t *testing.T) {
	nodes, peers := newTransportTestNodes(t, 2)
	startTransportTestServers(nodes, peers)
	t.Cleanup(func() { cleanupTransportTestNodes(t, nodes) })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	require.NoError(t, openTransportTestNode(ctx, nodes[0], peers[:1], true))
	leader := waitForLeaderNode(t, nodes[:1])

	// First open with joinAsLearner=false (the misuse): leader
	// AddVoter the joiner, conf change persists to disk.
	require.NoError(t, openTransportTestNode(ctx, nodes[1], peers, false))
	_, err := leader.engine.AddVoter(ctx, nodes[1].peer.ID, nodes[1].peer.Address, 0)
	require.NoError(t, err)
	waitForConfigSize(t, leader.engine, 2)
	waitForConfigSize(t, nodes[1].engine, 2)

	// Drive a write so the conf change snapshot lands on the joiner's
	// disk before we close it.
	require.Eventually(t, func() bool {
		return leader.engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)
	_, err = leader.engine.Propose(context.Background(), []byte("warm"))
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return len(nodes[1].fsm.Applied()) >= 1
	}, 5*time.Second, 20*time.Millisecond)

	// Close the joiner, then reopen with joinAsLearner=true. The
	// persisted snapshot encodes the local node in ConfState.Voters.
	require.NoError(t, nodes[1].engine.Close())

	before := JoinRoleViolationCount()
	engine, err := Open(ctx, OpenConfig{
		NodeID:        nodes[1].peer.NodeID,
		LocalID:       nodes[1].peer.ID,
		LocalAddress:  nodes[1].peer.Address,
		DataDir:       nodes[1].dir,
		JoinAsLearner: true,
		Transport:     nodes[1].transport,
		StateMachine:  nodes[1].fsm,
	})
	require.NoError(t, err)
	nodes[1].engine = engine
	require.Greater(t, JoinRoleViolationCount(), before, "expected join-as-learner alarm to fire on startup")
	require.NotEqual(t, raftengine.StateShutdown, engine.State())
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
