package kv

import (
	"sync/atomic"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/cockroachdb/errors"
)

// isLeadershipLossError reports whether err signals that this node has
// lost leadership and a successor should be contacted. Propose/Commit
// errors that are NOT leadership-related (write conflict, validation,
// deadline on a non-ReadIndex path) must NOT trigger lease
// invalidation -- doing so forces every subsequent read into the slow
// LinearizableRead path and defeats the lease's purpose.
//
// Both engine backends mark their internal leadership errors with the
// shared raftengine sentinels via cockroachdb/errors.Mark, so a single
// errors.Is check (using cockroachdb's Is, which understands the
// mark-based equivalence) covers both engines without relying on
// error-message substrings. Note: stdlib errors.Is does NOT traverse
// cockroachdb marks; this file must use cockroachdb/errors.Is.
func isLeadershipLossError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, raftengine.ErrNotLeader) ||
		errors.Is(err, raftengine.ErrLeadershipLost) ||
		errors.Is(err, raftengine.ErrLeadershipTransferInProgress)
}

// leaseState tracks the monotonic-raw expiry of a leader-local read
// lease. All operations are lock-free via atomic.Int64 (the raw
// monoclock.Instant nanoseconds) plus a generation counter that
// prevents an in-flight extend from resurrecting a lease that a
// concurrent invalidate has cleared.
//
// expiry == 0 means the lease has never been issued or has been
// invalidated. A non-zero value is the monotonic-raw instant after
// which the lease is considered expired; a caller comparing
// monoclock.Now() against the loaded value can decide whether to skip
// a quorum confirmation. The monotonic-raw clock is immune to NTP
// rate adjustment and wall-clock steps (see internal/monoclock).
type leaseState struct {
	gen atomic.Uint64
	// expiryNanos stores monoclock.Instant.Nanos(); 0 means "no lease".
	// Stored as int64 so CAS can be expressed without an extra pointer
	// indirection, preserving the lock-free fast-path performance.
	expiryNanos atomic.Int64
}

// valid reports whether the lease is unexpired at now.
//
// A zero-valued now indicates that the caller's monotonic-raw clock
// read failed (e.g. clock_gettime denied under seccomp) and is treated
// as "no lease evidence available" -- fail closed onto the slow path.
// Without this guard, a warmed lease (non-zero expiry) would stay
// forever valid for any caller sampling monoclock.Zero, since
// zero.Before(positive) holds.
func (s *leaseState) valid(now monoclock.Instant) bool {
	if s == nil || now.IsZero() {
		return false
	}
	ns := s.expiryNanos.Load()
	if ns == 0 {
		return false
	}
	return now.Before(monoclock.FromNanos(ns))
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
func (s *leaseState) extend(until monoclock.Instant, expectedGen uint64) {
	if s == nil {
		return
	}
	target := until.Nanos()
	if target == 0 {
		// Refuse to store 0 — that value is the sentinel for "no
		// lease" and would race with invalidate's zero-store.
		return
	}
	for {
		// Pre-CAS gate: if invalidate already advanced the generation
		// past expectedGen, skip the CAS entirely.
		if s.gen.Load() != expectedGen {
			return
		}
		current := s.expiryNanos.Load()
		if current != 0 && target <= current {
			return
		}
		if !s.expiryNanos.CompareAndSwap(current, target) {
			continue
		}
		// CAS landed. If invalidate raced in between the pre-CAS gate
		// and the CAS itself, undo our write iff no later writer has
		// replaced it. Using CAS with our own target means a fresh
		// extend that captured the post-invalidate generation is left
		// intact (its CAS already replaced our target with its own).
		if s.gen.Load() != expectedGen {
			s.expiryNanos.CompareAndSwap(target, 0)
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
	s.expiryNanos.Store(0)
}
