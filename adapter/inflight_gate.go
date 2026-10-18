package adapter

import (
	"sync"
	"sync/atomic"
	"time"
)

// inflightGate counts the operations being executed so a Stop can wait,
// bounded, for them to finish, and refuses new ones once the drain began.
// A few atomic operations per operation keep it off the hot path's locks.
// The Redis listener counts its commands with it; the S3 server counts the
// admin object operations that use its blob peer cluster, and its
// background cleanups. All methods are
// safe on a nil gate, which admits everything and has nothing to drain.
type inflightGate struct {
	n        atomic.Int64
	draining atomic.Bool
	idle     chan struct{}
	idleOnce sync.Once

	// drainOnce and joinOnce fix, on the first drain and the first join,
	// the deadline every later or concurrent call waits until. Stop runs
	// from two places at once (the context waiter and the deferred call
	// after Run returns), and whichever returns first lets the caller close
	// the resources the operations use, so neither may return earlier than
	// the other, nor spend a fresh bound.
	drainOnce  sync.Once
	drainUntil time.Time
	joinOnce   sync.Once
	joinUntil  time.Time
}

func newInflightGate() *inflightGate {
	return &inflightGate{idle: make(chan struct{})}
}

// enter registers an operation about to run and reports whether it may.
// Once drain has started it refuses: an operation entering after the count
// reached zero would run against resources the caller is closing. (redcon,
// for example, dispatches every command of an already-parsed pipeline even
// after Stop closed the connection; that client's connection is already
// gone, so a refused command never had a reply coming.)
//
// The increment comes before the draining check, and drain sets draining
// before it reads the count, so with sequentially consistent atomics either
// drain sees this operation or this operation sees the drain.
func (i *inflightGate) enter() bool {
	if i == nil {
		return true
	}
	i.n.Add(1)
	if i.draining.Load() {
		i.exit()
		return false
	}
	return true
}

func (i *inflightGate) exit() {
	if i != nil && i.n.Add(-1) == 0 && i.draining.Load() {
		i.signalIdle()
	}
}

func (i *inflightGate) signalIdle() {
	i.idleOnce.Do(func() { close(i.idle) })
}

// remaining is the number of operations still being executed.
func (i *inflightGate) remaining() int64 {
	if i == nil {
		return 0
	}
	return i.n.Load()
}

// drain refuses new operations, waits until timeout after the first drain
// for every in-flight operation to finish, and reports whether they did.
// A call made while an earlier one is still waiting waits until the same
// deadline; one made after it reports at once.
func (i *inflightGate) drain(timeout time.Duration) bool {
	if i == nil {
		return true
	}
	i.drainOnce.Do(func() {
		i.draining.Store(true)
		if i.n.Load() == 0 {
			i.signalIdle()
		}
		i.drainUntil = time.Now().Add(timeout)
	})
	return i.waitIdleUntil(i.drainUntil)
}

// join waits, after a drain that gave up, for the operations still running
// to finish, and reports whether they did. It is meant for a caller that
// has meanwhile told them to stop (by cancelling their context) and must
// not release their resources while they unwind. Like drain, every call
// waits until the deadline the first one set, timeout after it.
func (i *inflightGate) join(timeout time.Duration) bool {
	if i == nil || i.n.Load() == 0 {
		return true
	}
	if !i.draining.Load() {
		return false
	}
	i.joinOnce.Do(func() { i.joinUntil = time.Now().Add(timeout) })
	return i.waitIdleUntil(i.joinUntil)
}

// waitIdleUntil waits, once the drain has begun, until no operation is in
// flight or deadline passes, and reports whether the gate went idle.
func (i *inflightGate) waitIdleUntil(deadline time.Time) bool {
	wait := time.Until(deadline)
	if wait <= 0 {
		select {
		case <-i.idle:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-i.idle:
		return true
	case <-timer.C:
		return i.n.Load() == 0
	}
}
