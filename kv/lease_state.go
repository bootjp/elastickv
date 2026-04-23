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

// leaseSlot is the immutable payload stored behind leaseState.current.
// Each successful extend installs a freshly-allocated *leaseSlot, so
// pointer identity alone disambiguates two extenders that happened to
// compute the same expiry value (clock-granularity tie). A rollback
// CAS that compares against the extender's own *leaseSlot therefore
// cannot clobber a newer lease installed by a concurrent winner, even
// if the newer lease carries an equal expiryNanos.
//
// expiryNanos == 0 is the sentinel for "no lease"; the zero-valued
// *leaseSlot (returned on startup before any extend) carries gen 0.
type leaseSlot struct {
	// expiryNanos is monoclock.Instant.Nanos(); 0 means "no lease".
	expiryNanos int64
	// gen is the monotonic invalidation counter. Each invalidate
	// installs a new slot with gen+1; each extend observes it via
	// generation() BEFORE the quorum operation and refuses to install
	// a slot whose gen no longer matches. This preserves the
	// leader-loss guard: a Dispatch that returned just before an
	// invalidate fires must not resurrect the lease.
	gen uint64
}

// leaseState tracks the monotonic-raw expiry of a leader-local read
// lease. The hot-path read (valid) remains lock-free via a single
// atomic pointer load; writes go through CAS on the pointer so
// pointer identity gates the extender-vs-rollback race.
//
// expiryNanos == 0 in the current slot means the lease has never been
// issued or has been invalidated. A non-zero value is the
// monotonic-raw instant after which the lease is considered expired;
// a caller comparing monoclock.Now() against the loaded value can
// decide whether to skip a quorum confirmation. The monotonic-raw
// clock is immune to NTP rate adjustment and wall-clock steps (see
// internal/monoclock).
type leaseState struct {
	// current is the live *leaseSlot. A nil pointer means "never
	// extended"; valid() treats it identically to an explicit
	// zero-expiry slot.
	current atomic.Pointer[leaseSlot]
}

// slotOrZero returns the current slot, substituting a synthetic zero
// slot when the pointer has never been stored. Keeps callers free of
// nil checks.
func slotOrZero(p *leaseSlot) *leaseSlot {
	if p == nil {
		return &leaseSlot{}
	}
	return p
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
	slot := s.current.Load()
	if slot == nil || slot.expiryNanos == 0 {
		return false
	}
	return now.Before(monoclock.FromNanos(slot.expiryNanos))
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
	return slotOrZero(s.current.Load()).gen
}

// extend sets the lease expiry to until iff (a) until is strictly
// after the currently stored expiry (or no expiry is stored) and
// (b) no invalidate has happened since the caller captured
// expectedGen via generation() BEFORE the quorum operation.
//
// Pointer-identity CAS guards the rollback: after a racing
// invalidate fires, the rollback clears ONLY the exact *leaseSlot
// this extender installed, so a concurrent extender that captured
// the post-invalidate generation and computed the same expiry value
// (clock-granularity tie) cannot be clobbered -- its slot is a
// distinct allocation with a distinct pointer.
func (s *leaseState) extend(until monoclock.Instant, expectedGen uint64) {
	if s == nil {
		return
	}
	target := until.Nanos()
	if target == 0 {
		// Refuse to store 0 -- that value is the sentinel for "no
		// lease" and would race with invalidate's zero-store.
		return
	}
	for {
		// Load the live pointer once per iteration; all decisions
		// below are based on THIS observation. The CAS old-value
		// must match this exact pointer.
		stored := s.current.Load()
		old := slotOrZero(stored)
		// Pre-CAS gate: if invalidate already advanced the generation
		// past expectedGen, skip the CAS entirely.
		if old.gen != expectedGen {
			return
		}
		if old.expiryNanos != 0 && target <= old.expiryNanos {
			return
		}
		next := &leaseSlot{expiryNanos: target, gen: expectedGen}
		if !s.current.CompareAndSwap(stored, next) {
			continue
		}
		// CAS landed. If invalidate raced in between the pre-CAS
		// gate and the CAS itself, the current gen now exceeds
		// expectedGen; undo our write via a pointer-identity CAS on
		// `next`. A later writer (concurrent extender, concurrent
		// invalidate) that already replaced `next` with its own
		// allocation owns the state; its pointer differs from
		// `next`, our CAS fails, and we leave its value intact --
		// EVEN IF its expiryNanos equals our target.
		if observed := slotOrZero(s.current.Load()); observed.gen != expectedGen {
			rb := &leaseSlot{expiryNanos: 0, gen: observed.gen}
			s.current.CompareAndSwap(next, rb)
		}
		return
	}
}

// invalidate clears the lease so the next read takes the slow path.
// Each call installs a fresh zero-expiry slot whose gen is one more
// than the current slot's, via CAS retry. A concurrent extender that
// captured the pre-invalidate generation will see the advanced gen
// on its post-CAS recheck and roll back its own slot via pointer-
// identity CAS.
func (s *leaseState) invalidate() {
	if s == nil {
		return
	}
	for {
		stored := s.current.Load()
		old := slotOrZero(stored)
		next := &leaseSlot{expiryNanos: 0, gen: old.gen + 1}
		if s.current.CompareAndSwap(stored, next) {
			return
		}
	}
}
