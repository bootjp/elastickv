package adapter

import (
	"sync"
	"sync/atomic"
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
	joined   atomic.Bool
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

// stop refuses new workers and waits up to timeout for the running ones,
// reporting whether they all returned. Only the first call waits: Stop
// runs twice on the normal path (from the context waiter and deferred
// after Run returns), and the second call must not spend the bound again.
func (w *sqsWorkers) stop(timeout time.Duration) bool {
	w.mu.Lock()
	w.stopping = true
	w.mu.Unlock()
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()
	if w.joined.Swap(true) {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}
