package adapter

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMigrationQuorumCommitFollowsForcedLeadershipTransfer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	c := newQuorumAcceptanceCluster(t, ctx)
	initial := c.leader(t)
	epoch := c.waitConfiguration(t, 3)
	members, err := c.members(ctx, quorumAcceptanceGroup)
	require.NoError(t, err)
	quorumAcceptanceEventually(t, func() error { return EnsureMigrationNamespaceAdmission(ctx, initial.client, members) })
	_, root := c.partitionedSourceForDrain(t, ctx, initial, epoch)
	control := quorumAcceptanceAcquire(t, ctx, initial, 12, epoch, quorumAcceptanceCatalogVersion, root.later.Children[1], "t")
	large := make([]byte, store.MigrationPagePayloadBudget+(1<<19))
	_, prepared, err := c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptanceDrainRequest(pb.Phase_PREPARE, "u", large)})
	require.NoError(t, err)
	require.Positive(t, prepared.CommitIndex)
	old, _, err := c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptanceWriteRequest("a-ready", []byte("ready"), 90)})
	require.NoError(t, err)
	consumer := c.consumer(t, old, root)
	job := distribution.SplitJob{JobID: 12, SourceRouteID: 5, SplitKey: []byte("t")}
	quorumAcceptanceEventually(t, func() error {
		_, guardErr := consumer.ensureSplitJobTransferControl(ctx, job, quorumAcceptanceGroup, old.client, control, nil)
		return guardErr
	})
	current := transferQuorumLeaderForChurn(t, ctx, c, old)
	stale := quorumAcceptanceWriteRequest("b-stale", []byte("must-not-land"), 100)
	_, err = old.tx.Commit(ctx, []*pb.Request{stale})
	require.True(t, errors.Is(err, raftengine.ErrNotLeader), "the cached old transaction handle must reproduce the CI failure")
	t.Logf("forced transfer %s -> %s: old direct transaction returns ErrNotLeader", old.peer.ID, current.peer.ID)
	followedAt, _, err := c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptanceWriteRequest("b-handoff", []byte("new leader"), 105)})
	require.NoError(t, err)
	require.Same(t, current, followedAt, "the first post-transfer commit must follow the actual new voter")
	committedAt, written, err := c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptanceWriteRequest("b", large, 110)})
	require.NoError(t, err)
	requireRealQuorumChurnVoter(t, c, committedAt)
	require.Greater(t, written.CommitIndex, prepared.CommitIndex)
	verdictAt, _, err := c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptanceWriteRequest("w", large, 120)})
	require.ErrorIs(t, err, kv.ErrMigrationTransferTooLarge)
	requireRealQuorumChurnVoter(t, c, verdictAt)
	require.NoError(t, ctx.Err(), "a typed domain verdict must return without exhausting the original context")
	staleVersion := quorumAcceptanceWriteRequest("b-wrong-version", []byte("must-not-land"), 130)
	staleVersion.ObservedRouteVersion = kv.EncodeObservedRouteVersion(quorumAcceptanceCatalogVersion - 1)
	_, _, err = c.commitToCurrentLeader(ctx, []*pb.Request{staleVersion})
	require.ErrorIs(t, err, kv.ErrRouteCutoverPending, "leader retry must not relax a stale catalog verdict")
	require.NoError(t, ctx.Err())
	resolvedAt, resolved, err := c.commitToCurrentLeader(ctx, []*pb.Request{quorumAcceptanceDrainRequest(pb.Phase_COMMIT, "u", nil)})
	require.NoError(t, err, "the new leader must resolve the pre-transfer oversized intent using its stored bytes")
	requireRealQuorumChurnVoter(t, c, resolvedAt)
	require.Greater(t, resolved.CommitIndex, written.CommitIndex)
	// Follow the returned node into later phase construction, just as the
	// acceptance harness must after a large write or prepared resolution.
	consumer = c.consumer(t, resolvedAt, root)
	witness, err := consumer.ensureSplitJobTransferControl(ctx, job, quorumAcceptanceGroup, resolvedAt.client, control, nil)
	require.NoError(t, err)
	require.NoError(t, consumer.revalidateSplitTransferAdmission(ctx, witness))
	require.Equal(t, epoch, c.waitConfiguration(t, 3), "leadership transfer must not manufacture a configuration epoch")
	checkQuorumChurnData(t, ctx, c, control, resolved.CommitIndex, large)
}

func requireRealQuorumChurnVoter(t *testing.T, c *quorumAcceptanceCluster, n *quorumAcceptanceNode) {
	t.Helper()
	voters := map[*quorumAcceptanceNode]bool{c.nodes[0]: true, c.nodes[1]: true, c.nodes[2]: true}
	require.True(t, voters[n], "every result must identify an actual configured data voter, even if leadership changes again")
}

func transferQuorumLeaderForChurn(t *testing.T, ctx context.Context, c *quorumAcceptanceCluster, old *quorumAcceptanceNode) *quorumAcceptanceNode {
	t.Helper()
	var target *quorumAcceptanceNode
	for _, n := range c.nodes[:3] {
		if n != old {
			target = n
			break
		}
	}
	require.NotNil(t, target)
	index, err := old.engine.LinearizableRead(ctx)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return target.engine.AppliedIndex() >= index }, 5*time.Second, 20*time.Millisecond)
	require.NoError(t, old.engine.TransferLeadershipToServer(ctx, target.peer.ID, target.peer.Address))
	require.Eventually(t, func() bool {
		return old.engine.State() == raftengine.StateFollower && target.engine.State() == raftengine.StateLeader
	}, 5*time.Second, 20*time.Millisecond)
	return target
}

func checkQuorumChurnData(t *testing.T, ctx context.Context, c *quorumAcceptanceCluster, control *pb.TargetStagedReadinessRequest, index uint64, large []byte) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, n := range c.nodes[:3] {
			if n.engine.AppliedIndex() < index {
				return false
			}
		}
		return true
	}, 5*time.Second, 20*time.Millisecond)
	probe := probeMigrationControlRequest(control)
	probe.RequireConfigurationIndex, probe.ExpectedConfigurationIndex = true, control.GetExpectedConfigurationIndex()
	probe.TransferSizeGuardVersion = 1
	for _, n := range c.nodes[:3] {
		for _, key := range []string{"b", "u"} {
			value, err := n.store.GetAt(ctx, []byte(key), 1000)
			require.NoError(t, err)
			require.True(t, bytes.Equal(large, value), "all real voters must retain exactly the admitted and resolved bytes")
		}
		for _, key := range []string{"b-stale", "b-wrong-version", "w"} {
			_, err := n.store.GetAt(ctx, []byte(key), 1000)
			require.ErrorIs(t, err, store.ErrKeyNotFound)
		}
		ack, err := n.client.ProbeMigrationState(ctx, probe)
		require.NoError(t, err)
		require.True(t, ack.GetReady(), "every actual voter must preserve the exact configured guard through handoff")
		require.Equal(t, control.GetExpectedConfigurationIndex(), ack.GetAppliedConfigurationIndex())
	}
}
