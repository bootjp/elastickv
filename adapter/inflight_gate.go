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
// admin object operations that use its blob peer cluster. All methods are
// safe on a nil gate, which admits everything and has nothing to drain.
type inflightGate struct {
	n        atomic.Int64
	draining atomic.Bool
	drained  atomic.Bool
	idle     chan struct{}
	idleOnce sync.Once
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

// drain waits up to timeout for every in-flight operation to finish and
// reports whether they did. Only the first call waits: Stop runs twice on
// the normal path (from the context waiter and deferred after Run returns),
// and the second call must not spend the bound again.
func (i *inflightGate) drain(timeout time.Duration) bool {
	if i == nil {
		return true
	}
	if i.drained.Load() {
		return i.n.Load() == 0
	}
	i.draining.Store(true)
	if i.n.Load() == 0 {
		i.signalIdle()
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	finished := false
	select {
	case <-i.idle:
		finished = true
	case <-timer.C:
	}
	i.drained.Store(true)
	return finished
}
