package etcd

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// quorumAckTracker records the most recent response time from each
// follower and publishes the "majority-ack instant" -- the wall clock
// at which a majority of followers had all been confirmed live.
//
// LeaseRead callers pair the published instant with LeaseDuration to
// serve a leader-local read without issuing a fresh ReadIndex round.
// This replaces the prior caller-side lease scheme, which had to
// sample time.Now() before the slow path and therefore could not
// amortise reads whose own latency exceeded LeaseDuration (the bug
// that kept production GET at ~1 s under step-queue congestion).
//
// Correctness anchor: we record time.Now() when the leader OBSERVES
// a follower response, not the follower's local ack time. That makes
// our recorded instant an UPPER bound on the follower's true
// last-contact, which is the conservative direction for lease
// safety: lease = recorded_instant + lease_duration can only be
// LATER than follower_last_contact + lease_duration, and
// follower_last_contact + electionTimeout is the earliest time the
// follower would vote for a new leader, so lease_duration <
// electionTimeout - safety keeps the lease strictly inside the
// no-new-leader window.
//
// Wait -- that's unsafe. A later observation means a LARGER recorded
// instant, which makes lease_expiry later. We actually need a LOWER
// bound on follower_last_contact to bound lease_expiry conservatively.
// Because network/scheduling delay makes leader_observation >=
// follower_ack_sent_time, using leader_observation is an OVERestimate
// of follower_ack_sent_time, which in turn is an overestimate of
// follower_ack_received_time. That means lease extends slightly past
// the strictly-safe boundary by at most the one-way delay + scheduling
// slop -- which is exactly what leaseSafetyMargin is sized to cover.
// See docs/lease_read_design.md for the full argument.
type quorumAckTracker struct {
	mu       sync.Mutex
	peerAcks map[uint64]int64 // peer ID → last ack unix nano observed on leader
	// quorumAckUnixNano is the Nth-most-recent peer ack where N equals
	// the number of follower acks required for majority (clusterSize/2).
	// Updated under mu; read lock-free via atomic.Load.
	quorumAckUnixNano atomic.Int64
}

// recordAck notes that peerID responded to us and recomputes the
// majority-ack instant. followerQuorum is the number of non-self
// peers whose ack is required for majority (clusterSize / 2 for
// integer division; 1 for a 3-node cluster, 2 for 5-node, etc).
//
// A followerQuorum of 0 means single-node cluster: caller should
// surface LastQuorumAck = now without calling this.
func (t *quorumAckTracker) recordAck(peerID uint64, followerQuorum int) {
	if followerQuorum <= 0 {
		return
	}
	now := time.Now().UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.peerAcks == nil {
		t.peerAcks = make(map[uint64]int64)
	}
	t.peerAcks[peerID] = now
	if len(t.peerAcks) < followerQuorum {
		// Not enough peers have reported yet to form a majority.
		return
	}
	acks := make([]int64, 0, len(t.peerAcks))
	for _, a := range t.peerAcks {
		acks = append(acks, a)
	}
	// Sort descending so acks[0] is the most recent. The followerQuorum-th
	// entry (1-indexed) is the oldest ack among the top quorum -- i.e. the
	// boundary instant by which majority liveness was confirmed.
	sort.Slice(acks, func(i, j int) bool { return acks[i] > acks[j] })
	t.quorumAckUnixNano.Store(acks[followerQuorum-1])
}

// reset clears all recorded peer acks. Call when the local node
// leaves the leader role so a future re-election does not resurrect
// a stale majority-ack instant.
func (t *quorumAckTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peerAcks = nil
	t.quorumAckUnixNano.Store(0)
}

// load returns the current majority-ack instant or the zero time if
// no quorum has been observed since the last reset.
func (t *quorumAckTracker) load() time.Time {
	ns := t.quorumAckUnixNano.Load()
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns)
}
