package kv

import (
	"errors"
	"strings"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

// isLeadershipLossError reports whether err signals that this node has
// lost leadership and a successor should be contacted. Propose/Commit
// errors that are NOT leadership-related (write conflict, validation,
// deadline on a non-ReadIndex path) must NOT trigger lease
// invalidation -- doing so forces every subsequent read into the slow
// LinearizableRead path and defeats the lease's purpose.
//
// The underlying engines surface leadership loss via distinct
// sentinel errors; we recognize the hashicorp variant directly and
// match the etcd engine's "not leader" string via errors.Is /
// substring match so a future rename in etcd/raft does not silently
// reintroduce the over-invalidation bug.
func isLeadershipLossError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) ||
		errors.Is(err, raft.ErrLeadershipTransferInProgress) {
		return true
	}
	msg := err.Error()
	// etcd engine errors (cockroachdb/errors wraps them, so Is may
	// not traverse reliably across package boundaries; fall back to
	// substring match against the sentinel messages).
	return strings.Contains(msg, "not leader") ||
		strings.Contains(msg, "leadership transfer") ||
		strings.Contains(msg, "leadership lost")
}

// leaseState tracks the wall-clock expiry of a leader-local read lease.
// All operations are lock-free via atomic.Pointer plus a generation
// counter that prevents an in-flight extend from resurrecting a lease
// that a concurrent invalidate has cleared.
//
// A nil expiry means the lease has never been issued or has been
// invalidated. A non-nil expiry is the wall-clock instant after which
// the lease is considered expired; a caller comparing time.Now() against
// the loaded value can decide whether to skip a quorum confirmation.
type leaseState struct {
	gen    atomic.Uint64
	expiry atomic.Pointer[time.Time]
}

// valid reports whether the lease is unexpired at now.
func (s *leaseState) valid(now time.Time) bool {
	if s == nil {
		return false
	}
	exp := s.expiry.Load()
	if exp == nil {
		return false
	}
	return now.Before(*exp)
}

// generation returns the current invalidation counter. Callers MUST
// sample this BEFORE issuing the quorum-confirming operation (Propose
// / LinearizableRead) and pass the result to extend. Sampling inside
// extend (after the operation returned) would see any leader-loss
// invalidation that fired DURING the operation as the "current"
// generation and let a stale lease resurrect.
func (s *leaseState) generation() uint64 {
	if s == nil {
		return 0
	}
	return s.gen.Load()
}

// extend sets the lease expiry to until iff (a) until is strictly
// after the currently stored expiry (or no expiry is stored) and
// (b) no invalidate has happened since the caller captured
// expectedGen via generation() BEFORE the quorum operation. The
// generation guard prevents a Dispatch that returned successfully
// *just before* a leader-loss invalidate from resurrecting the
// lease milliseconds after invalidation.
func (s *leaseState) extend(until time.Time, expectedGen uint64) {
	if s == nil {
		return
	}
	for {
		// Pre-CAS gate: if invalidate already advanced the generation
		// past expectedGen, skip the CAS entirely.
		if s.gen.Load() != expectedGen {
			return
		}
		current := s.expiry.Load()
		if current != nil && !until.After(*current) {
			return
		}
		if !s.expiry.CompareAndSwap(current, &until) {
			continue
		}
		// CAS landed. If invalidate raced in between the pre-CAS gate
		// and the CAS itself, undo our write iff no later writer has
		// replaced it. Using CAS with our own pointer means a fresh
		// extend that captured the post-invalidate generation is left
		// intact.
		if s.gen.Load() != expectedGen {
			s.expiry.CompareAndSwap(&until, nil)
		}
		return
	}
}

// invalidate clears the lease so the next read takes the slow path.
// Bumping the generation first ensures any concurrent extend that
// captured the previous generation will undo its own CAS rather than
// resurrect the lease.
func (s *leaseState) invalidate() {
	if s == nil {
		return
	}
	s.gen.Add(1)
	s.expiry.Store(nil)
}
