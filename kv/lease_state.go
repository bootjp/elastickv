package kv

import (
	"sync"
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
// lease.
//
// Storage layout. The expiry is held as a single atomic int64 of
// monotonic-raw nanoseconds and the invalidation generation as a single
// atomic uint64. Neither write path allocates: the previous
// pointer-swap design heap-allocated a slot on every successful extend,
// which is 1:1 with Dispatch throughput and a measurable GC source at
// tens of thousands of writes per second. Two plain atomics remove that
// allocation entirely.
//
// Clock source. expiryNanos stores monoclock.Instant.Nanos(), i.e. a
// reading of CLOCK_MONOTONIC_RAW (see internal/monoclock). It is NOT
// wall-clock nanoseconds. valid() compares it against a caller-supplied
// monoclock.Now(), so the lease-vs-safety-window comparison is immune to
// NTP rate adjustment and wall-clock step events: a backward or forward
// wall-clock step cannot prematurely expire the lease or, worse, extend
// it past its true safety window. Both the stored expiry and the now
// passed to valid() MUST originate from monoclock so they share that
// arbitrary zero point; mixing in time.Now()-derived nanoseconds here
// would reintroduce the NTP-step hazard this clock source exists to
// avoid.
//
// expiryNanos == 0 means the lease has never been issued or has been
// invalidated; valid() returns false for it unconditionally. A non-zero
// value is the monotonic-raw instant after which the lease is expired.
//
// Concurrency. valid() is the hot path (one per read) and stays
// lock-free: a single atomic load of expiryNanos. The write paths
// (extend / invalidate) run once per Dispatch / leadership change, not
// per read, and serialize on writeMu so an extend and an invalidate can
// never interleave their (gen, expiry) updates. Serializing only the
// writers keeps the pair update atomic without a 128-bit CAS and without
// the post-write rollback dance a lock-free two-atomic scheme would
// otherwise need, while leaving the read path uncontended.
type leaseState struct {
	// writeMu serializes extend and invalidate so their two-field
	// updates appear atomic to each other. Readers never take it.
	writeMu sync.Mutex
	// expiryNanos is monoclock.Instant.Nanos(); 0 means "no lease".
	// Read lock-free by valid(); written only under writeMu.
	expiryNanos atomic.Int64
	// gen is the monotonic invalidation counter. Each invalidate
	// increments it; each extend observes it via generation() BEFORE
	// the quorum operation and refuses to install an expiry whose
	// generation no longer matches. This preserves the leader-loss
	// guard: a Dispatch that returned just before an invalidate fires
	// must not resurrect the lease. Read lock-free by generation();
	// written only under writeMu.
	gen atomic.Uint64
}

// valid reports whether the lease is unexpired at now.
//
// A zero-valued now indicates that the caller's monotonic-raw clock
// read failed (e.g. clock_gettime denied under seccomp) and is
// treated as "no lease evidence available" -- fail closed onto the
// slow path. Without this guard, a warmed lease (non-zero expiry)
// would stay forever valid for any caller sampling monoclock.Zero,
// since zero.Before(positive) holds.
func (s *leaseState) valid(now monoclock.Instant) bool {
	if s == nil || now.IsZero() {
		return false
	}
	expiry := s.expiryNanos.Load()
	if expiry == 0 {
		return false
	}
	return now.Before(monoclock.FromNanos(expiry))
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
// expectedGen via generation() BEFORE the quorum operation.
//
// The generation recheck under writeMu is the leader-loss guard: an
// invalidate that fired DURING the quorum operation has bumped gen, so
// the captured expectedGen no longer matches and the extend is dropped
// before it can resurrect a lease the leadership-loss callback just
// cleared. Because extend and invalidate are mutually exclusive on
// writeMu, no post-write rollback is needed: an invalidate cannot land
// between the generation recheck and the expiry store.
func (s *leaseState) extend(until monoclock.Instant, expectedGen uint64) {
	if s == nil {
		return
	}
	target := until.Nanos()
	if target == 0 {
		// Refuse to store 0 -- that value is the sentinel for "no
		// lease" and would be indistinguishable from invalidate's
		// zero-store.
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	// Generation guard: an invalidate since expectedGen was captured
	// means a leader-loss callback fired during the quorum op; refuse
	// to resurrect the lease.
	if s.gen.Load() != expectedGen {
		return
	}
	// Monotonicity: never regress a lease. An out-of-order writer that
	// sampled monoclock.Now() earlier must not shorten a freshly
	// extended lease and force callers onto the slow path while the
	// leader is still confirmed.
	cur := s.expiryNanos.Load()
	if cur != 0 && target <= cur {
		return
	}
	s.expiryNanos.Store(target)
}

// invalidate clears the lease so the next read takes the slow path and
// bumps the generation so a concurrent extender that captured the
// pre-invalidate generation cannot resurrect the lease. The store of
// the zero sentinel is unconditional, even when the current expiry is
// in the future: otherwise leadership-loss callbacks would be powerless
// once a lease is in place.
//
// extend serializes on writeMu so the two never interleave: an extend
// either runs fully before this invalidate (and is then cleared here) or
// fully after (and observes the bumped generation via its guard and is
// dropped).
func (s *leaseState) invalidate() {
	if s == nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	s.gen.Add(1)
	s.expiryNanos.Store(0)
}
