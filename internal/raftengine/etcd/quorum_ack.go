package etcd

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bootjp/elastickv/internal/monoclock"
)

// quorumAckTracker records the most recent response time from each
// follower and publishes the "majority-ack instant" -- the monotonic-
// raw reading at which a majority of followers had all been confirmed
// live.
//
// LeaseRead callers pair the published instant with LeaseDuration to
// serve a leader-local read without issuing a fresh ReadIndex round.
// This replaces the prior caller-side lease scheme, which had to
// sample time.Now() before the slow path and therefore could not
// amortise reads whose own latency exceeded LeaseDuration (the bug
// that kept production GET at ~1 s under step-queue congestion).
//
// Timestamps are CLOCK_MONOTONIC_RAW readings (see internal/monoclock).
// Using the raw monotonic source instead of Go's time.Now() keeps the
// lease-vs-safety-window comparison immune to NTP rate adjustment and
// wall-clock step events -- TiKV's choice, adopted here per #551.
//
// Safety: we record monoclock.Now() when the leader OBSERVES the
// follower response, which is an UPPER bound on the follower's true
// ack time. Because lease = recorded_instant + lease_duration, that
// upper bound makes the lease extend slightly past the strictly-safe
// follower_ack_time + electionTimeout boundary by at most the one-way
// network delay plus scheduling slop. leaseSafetyMargin is sized to
// cover that overshoot, so leaseDuration = electionTimeout -
// leaseSafetyMargin keeps the lease strictly inside the no-new-leader
// window. See docs/design/2026_04_20_implemented_lease_read.md for the full argument.
type quorumAckTracker struct {
	mu       sync.Mutex
	peerAcks map[uint64]int64 // peer ID → last ack monoclock ns observed on leader
	// ackBuf is reused by recomputeLocked to avoid allocating a fresh
	// []int64 on every MsgAppResp / MsgHeartbeatResp. Sized to
	// len(peerAcks) on first use and grown via append when the cluster
	// expands. Caller must hold t.mu.
	ackBuf []int64
	// quorumAckMonoNs is the Nth-most-recent peer ack where N equals
	// the number of follower acks required for majority (clusterSize/2),
	// stored as a monoclock.Instant nanosecond counter. Updated under
	// mu; read lock-free via atomic.Load.
	quorumAckMonoNs atomic.Int64
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
	now := monoclock.Now().Nanos()
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.peerAcks == nil {
		t.peerAcks = make(map[uint64]int64)
	}
	t.peerAcks[peerID] = now
	t.recomputeLocked(followerQuorum)
}

// removePeer drops peerID's recorded ack. Call when a peer leaves the
// cluster so its pre-removal ack time can no longer satisfy the
// majority threshold after a configuration change: a shrink-then-grow
// that ends with fresh peers who have not yet acked would otherwise
// let the removed peer's last ack falsely advance the quorum instant,
// which is a lease-safety violation.
//
// followerQuorum is the POST-removal follower quorum so the published
// instant is recomputed against the current cluster. Passing 0 keeps
// the current instant; the next recordAck will refresh it.
func (t *quorumAckTracker) removePeer(peerID uint64, followerQuorum int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	// delete is safe on a missing key. We still recompute even when
	// peerID had no recorded entry: a shrink that reduces
	// followerQuorum may let the remaining peers now satisfy the
	// smaller threshold, and without an explicit recompute the
	// published instant would stay at its stale value (or zero) until
	// the next recordAck arrives.
	delete(t.peerAcks, peerID)
	if followerQuorum <= 0 {
		return
	}
	t.recomputeLocked(followerQuorum)
}

// recomputeLocked publishes the followerQuorum-th most recent ack as
// the quorum instant, or clears it if we lack that many recorded
// peers. Caller must hold t.mu.
//
// Reuses t.ackBuf across calls so the hot path (one call per
// MsgAppResp / MsgHeartbeatResp) does not allocate on steady state.
// The buffer is re-sliced in place and the sort is done on that
// slice; a cluster growing past the previous capacity picks up a
// single growth step via append, not a fresh allocation per call.
func (t *quorumAckTracker) recomputeLocked(followerQuorum int) {
	if len(t.peerAcks) < followerQuorum {
		// Not enough peers have reported to form a majority yet.
		t.quorumAckMonoNs.Store(0)
		return
	}
	t.ackBuf = t.ackBuf[:0]
	for _, a := range t.peerAcks {
		t.ackBuf = append(t.ackBuf, a)
	}
	// Sort descending so ackBuf[0] is the most recent. The
	// followerQuorum-th entry (1-indexed) is the oldest ack among the
	// top quorum -- i.e. the boundary instant by which majority
	// liveness was confirmed. Cluster size is small in practice (3-5
	// peers), so sort.Slice is cheaper than a quickselect once the
	// buffer is reused.
	sort.Slice(t.ackBuf, func(i, j int) bool { return t.ackBuf[i] > t.ackBuf[j] })
	t.quorumAckMonoNs.Store(t.ackBuf[followerQuorum-1])
}

// reset clears all recorded peer acks. Call when the local node
// leaves the leader role so a future re-election does not resurrect
// a stale majority-ack instant.
func (t *quorumAckTracker) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peerAcks = nil
	t.ackBuf = t.ackBuf[:0]
	t.quorumAckMonoNs.Store(0)
}

// load returns the current majority-ack instant or the zero Instant
// if no quorum has been observed since the last reset.
func (t *quorumAckTracker) load() monoclock.Instant {
	ns := t.quorumAckMonoNs.Load()
	if ns == 0 {
		return monoclock.Zero
	}
	return monoclock.FromNanos(ns)
}
