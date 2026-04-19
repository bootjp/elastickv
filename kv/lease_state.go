package kv

import (
	"sync/atomic"
	"time"
)

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

// extend sets the lease expiry to until iff (a) until is strictly after
// the currently stored expiry (or no expiry is stored) and (b) no
// invalidate happened between when this caller started and when its
// CAS landed. Without the generation guard, a Dispatch that returned
// successfully *just before* a leader-loss invalidate could resurrect
// the lease milliseconds after invalidation, defeating the purpose of
// the leader-loss callback.
func (s *leaseState) extend(until time.Time) {
	if s == nil {
		return
	}
	expectedGen := s.gen.Load()
	for {
		current := s.expiry.Load()
		if current != nil && !until.After(*current) {
			return
		}
		if !s.expiry.CompareAndSwap(current, &until) {
			continue
		}
		// CAS landed. If invalidate raced ahead, undo our write iff
		// no later writer has replaced it. Using CAS with our own
		// pointer means a fresh extend that captured the
		// post-invalidate generation is left intact.
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
