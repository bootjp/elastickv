package etcd

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/stretchr/testify/require"
)

func TestQuorumAckTracker_SingleNodeFollowerQuorumZeroIsNoop(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	// followerQuorum == 0 means single-node cluster -- caller handles
	// that case elsewhere. recordAck must not mutate state, otherwise
	// a re-election into multi-node would surface a stale instant.
	tr.recordAck(42, 0)
	require.True(t, tr.load().IsZero())
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
	require.True(t, tr2.load().IsZero(), "one follower is not a 5-node quorum")
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

	tr.recordAck(3, 2)
	second := tr.load()
	require.False(t, second.IsZero())

	// Now peer 4 acks. Even if monoclock.Now() granularity places
	// every sample at the same nanosecond, the quorum instant must
	// NOT regress: the 5-node quorum requires 2 follower acks (self
	// makes 3 = majority), and the OLDEST of the top two followers
	// bounds the boundary. require.False(third.Before(second)) holds
	// trivially when timestamps are equal, so this test does not rely
	// on clock granularity and is deterministic on fast CI.
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
	require.True(t, tr.load().IsZero(),
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
	require.True(t, tr.load().IsZero())

	// After reset, a subsequent ack must still populate correctly.
	tr.recordAck(2, 1)
	require.False(t, tr.load().IsZero())
}

// TestQuorumAckTracker_LoadReturnsMonotonicRaw pins the clock source:
// quorum acks must be CLOCK_MONOTONIC_RAW readings, not time.Now().
// A regression that stored wall-clock unix nanos would put the tracker
// back on the NTP-slew-sensitive path that #551 removed.
func TestQuorumAckTracker_LoadReturnsMonotonicRaw(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	before := monoclock.Now()
	tr.recordAck(2, 1)
	after := monoclock.Now()

	got := tr.load()
	require.False(t, got.IsZero())
	require.False(t, got.Before(before),
		"recorded ack must not predate the monotonic-raw sample taken before recordAck")
	require.False(t, got.After(after),
		"recorded ack must not postdate the monotonic-raw sample taken after recordAck")
	// Sanity: the gap between before and after bounds the recorded
	// value; if recordAck stored wall-clock time, the comparison
	// arithmetic below would be against a different epoch entirely
	// and this subtraction would produce an out-of-range duration.
	require.Less(t, got.Sub(before), time.Second,
		"recorded ack must sit inside the [before, after] monotonic window")
}

func TestQuorumAckTracker_ConcurrentRecordAndLoad(t *testing.T) {
	t.Parallel()
	var tr quorumAckTracker
	var wg sync.WaitGroup
	wg.Add(2)
	stop := make(chan struct{})

	// Recorder alternates between two peer IDs so a 3-node followerQuorum
	// always has at least one entry and the sort path runs.
	// runtime.Gosched between iterations keeps the loops from pegging a
	// core under `-race` while still interleaving enough recordAck /
	// load pairs to exercise the atomic-pointer invariants.
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				tr.recordAck(2, 1)
				tr.recordAck(3, 1)
				runtime.Gosched()
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
				runtime.Gosched()
			}
		}
	}()

	time.Sleep(20 * time.Millisecond)
	close(stop)
	wg.Wait()
}
