package kv

import (
	"sync/atomic"
	"time"
)

// leaseState tracks the wall-clock expiry of a leader-local read lease.
// All operations are lock-free via atomic.Pointer.
//
// A nil pointer means the lease has never been issued or has been
// invalidated. A non-nil pointer is the wall-clock instant after which
// the lease is considered expired; a caller comparing time.Now() against
// the loaded value can decide whether to skip a quorum confirmation.
type leaseState struct {
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

// extend sets the lease expiry to until iff until is strictly after the
// currently stored expiry (or no expiry is stored). The CAS loop prevents
// an out-of-order writer that sampled time.Now() earlier from overwriting
// a fresher extension and prematurely shortening the lease, while still
// allowing invalidate() (which Stores nil) to win unconditionally.
func (s *leaseState) extend(until time.Time) {
	if s == nil {
		return
	}
	for {
		current := s.expiry.Load()
		if current != nil && !until.After(*current) {
			return
		}
		if s.expiry.CompareAndSwap(current, &until) {
			return
		}
	}
}

// invalidate clears the lease so the next read takes the slow path.
func (s *leaseState) invalidate() {
	if s == nil {
		return
	}
	s.expiry.Store(nil)
}
