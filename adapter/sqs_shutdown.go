package adapter

import (
	"sync"
	"time"
)

// sqsWorkerStopTimeout bounds how long Stop waits for the background
// workers Run started (the retention reaper and the throttle bucket
// sweep) to return after their context was cancelled. A reaper pass can
// be inside a store scan or a Raft dispatch when the shutdown starts, and
// in a listenless (admin-only) deployment nothing else holds the errgroup
// task open, so without the join the deferred cleanup could close the
// stores underneath it. A variable so tests can shorten it.
var sqsWorkerStopTimeout = 10 * time.Second

// sqsWorkers tracks the SQS server's background goroutines so Stop can
// join them, bounded, and refuses new ones once Stop has begun (a Run that
// races a Stop must not start a worker the join has already missed).
type sqsWorkers struct {
	mu       sync.Mutex
	stopping bool
	wg       sync.WaitGroup

	// stopOnce fixes, on the first stop, the deadline every later or
	// concurrent stop waits until, and starts the one goroutine that waits
	// for the workers (done closes once they all returned).
	stopOnce  sync.Once
	stopUntil time.Time
	done      chan struct{}
}

// start runs fn in a tracked goroutine and reports whether it did; it
// refuses once stop has been called.
func (w *sqsWorkers) start(fn func()) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopping {
		return false
	}
	w.wg.Go(fn)
	return true
}

// stop refuses new workers and waits, until timeout after the first stop,
// for the running ones, reporting whether they all returned. Stop runs
// twice on the normal path, from the context waiter and deferred after
// Run returns, possibly at the same time: a call made while an earlier one
// is still waiting waits until the same deadline (whichever returns first
// lets its caller close the stores, so neither may return early), and one
// made after it reports at once without spending the bound again.
func (w *sqsWorkers) stop(timeout time.Duration) bool {
	w.stopOnce.Do(func() {
		w.mu.Lock()
		w.stopping = true
		w.mu.Unlock()
		w.stopUntil = time.Now().Add(timeout)
		w.done = make(chan struct{})
		go func() {
			w.wg.Wait()
			close(w.done)
		}()
	})
	wait := time.Until(w.stopUntil)
	if wait <= 0 {
		select {
		case <-w.done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-w.done:
		return true
	case <-timer.C:
		return false
	}
}
