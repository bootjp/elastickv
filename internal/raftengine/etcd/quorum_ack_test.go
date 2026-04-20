package etcd

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestQuorumAckTracker_SingleNodeFollowerQuorumZeroIsNoop(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	// followerQuorum == 0 means single-node cluster -- caller handles
	// that case elsewhere. recordAck must not mutate state, otherwise
	// a re-election into multi-node would surface a stale instant.
	tr.recordAck(42, 0)
	require.Equal(t, time.Time{}, tr.load())
}

func TestQuorumAckTracker_QuorumAckWaitsForMajority(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	// 3-node cluster: followerQuorum = 1 (need 1 follower + self).
	tr.recordAck(2, 1)
	first := tr.load()
	require.False(t, first.IsZero(), "single follower ack already satisfies 3-node quorum")

	// 5-node cluster: followerQuorum = 2. One follower ack alone is
	// NOT enough -- tracker must wait until a second follower has
	// reported before publishing.
	var tr2 quorumAckTracker
	tr2.recordAck(2, 2)
	require.Equal(t, time.Time{}, tr2.load(), "one follower is not a 5-node quorum")
	tr2.recordAck(3, 2)
	require.False(t, tr2.load().IsZero(), "two followers + self make a 5-node quorum")
}

func TestQuorumAckTracker_QuorumAckIsOldestOfTopN(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	// 5-node cluster (quorum = 2 followers). Record acks in staggered
	// order and verify the published instant is the OLDER of the top
	// two -- i.e. the boundary by which a majority was last confirmed.
	tr.recordAck(2, 2)
	first := tr.load()
	require.True(t, first.IsZero(), "still only one follower, no quorum")

	time.Sleep(2 * time.Millisecond)
	tr.recordAck(3, 2)
	second := tr.load()
	require.False(t, second.IsZero())

	// Now peer 4 acks with a later timestamp. Quorum ack should still
	// be the older of the top two (either 2 or 3, not 4) because the
	// 5-node quorum is 3 including self (2 followers + self), and the
	// OLDEST of the top two followers is still the limiting factor.
	time.Sleep(2 * time.Millisecond)
	tr.recordAck(4, 2)
	third := tr.load()
	require.False(t, third.Before(second), "quorum instant must not regress")
}

// TestQuorumAckTracker_RemovedPeerCannotSatisfyQuorum exercises the
// safety invariant: a peer that leaves the cluster must have its
// recorded ack pruned, otherwise a shrink-then-grow that ends with
// fresh peers who have not yet acked could let the removed peer's
// pre-removal ack falsely satisfy the new cluster's majority.
func TestQuorumAckTracker_RemovedPeerCannotSatisfyQuorum(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	// 5-node cluster, followerQuorum = 2. Peers 2 and 3 ack.
	tr.recordAck(2, 2)
	tr.recordAck(3, 2)
	require.False(t, tr.load().IsZero(), "baseline: 5-node quorum satisfied")

	// Cluster shrinks to 3 (followerQuorum = 1). After removing both
	// acked peers we have zero recorded entries -- not enough to
	// satisfy even the smaller quorum.
	tr.removePeer(2, 1)
	tr.removePeer(3, 1)
	require.Equal(t, time.Time{}, tr.load(),
		"after removing every acked peer the quorum instant must clear")
}

func TestQuorumAckTracker_RemovePeerZeroQuorumKeepsCurrent(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	tr.recordAck(2, 1)
	before := tr.load()
	require.False(t, before.IsZero())

	// followerQuorum = 0 means the caller doesn't have the post-
	// removal size yet. Entry is dropped but the published instant is
	// retained; the next recordAck will refresh it.
	tr.removePeer(2, 0)
	require.Equal(t, before, tr.load(),
		"removePeer with followerQuorum=0 must not clobber the current instant")
}

func TestQuorumAckTracker_ResetClearsState(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	tr.recordAck(2, 1)
	require.False(t, tr.load().IsZero())

	tr.reset()
	require.Equal(t, time.Time{}, tr.load())

	// After reset, a subsequent ack must still populate correctly.
	tr.recordAck(2, 1)
	require.False(t, tr.load().IsZero())
}

func TestQuorumAckTracker_ConcurrentRecordAndLoad(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	var wg sync.WaitGroup
	wg.Add(2)
	stop := make(chan struct{})

	// Recorder alternates between two peer IDs so a 3-node followerQuorum
	// always has at least one entry and the sort path runs.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				tr.recordAck(2, 1)
				tr.recordAck(3, 1)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = tr.load()
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
}
