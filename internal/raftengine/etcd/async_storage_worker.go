package etcd

import (
	"runtime/debug"
	"sync"

	"github.com/cockroachdb/errors"
)

var (
	errStorageWorkerClosed = errors.New("storage worker is quiescing")
	errInvalidStorageWork  = errors.New("invalid storage work")
	errStorageWorkerPanic  = errors.New("storage worker panicked")
)

// storageWork transfers its immutable token and captured resources to a FIFO
// actor. The operation must not access RawNode or mutable owner-loop state.
// Disk operations must not wait for owner/apply callbacks or submit to this same
// actor. Apply operations may request owner or disk receipts without locks held.
type storageWork struct {
	token         any
	retainedBytes uint64
	run           func() (any, error)
}

type storageWorkCompletion struct {
	token  any
	value  any
	err    error
	permit *storageWorkPermit
}

// Release is called by the owner only after it retires the completion's
// resources. Delivery to this channel alone does not release memory accounting.
func (c storageWorkCompletion) Release() {
	c.permit.Release()
}

type reservedStorageWork struct {
	work   storageWork
	permit *storageWorkPermit
}

// storageWorker never owns RawNode or closes storage. Its owner must pump
// completions while quiescing: done is not closed until every accepted task has
// an actual completion, including tasks skipped after the first fatal error.
type storageWorker struct {
	mu          sync.Mutex
	quiescing   bool
	failure     error
	requests    chan reservedStorageWork
	completions chan storageWorkCompletion
	done        chan struct{}
}

func newStorageWorker(queueSize int) (*storageWorker, error) {
	if queueSize <= 0 {
		return nil, errors.WithStack(errInvalidStorageWork)
	}
	w := &storageWorker{
		requests:    make(chan reservedStorageWork, queueSize),
		completions: make(chan storageWorkCompletion, queueSize),
		done:        make(chan struct{}),
	}
	go w.run()
	return w, nil
}

// trySubmit is nonblocking, including when the disk or completion consumer is
// stopped. A rejection transfers no ownership. The reservation covers queued,
// executing and completed-but-not-retired work. Distinct bulk/control budgets
// may feed the same actor without reordering accepted work.
func (w *storageWorker) trySubmit(work storageWork, budget *storageWorkBudget) error {
	if w == nil || work.run == nil {
		return errors.WithStack(errInvalidStorageWork)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failure != nil {
		return errors.Wrap(w.failure, "storage worker failed")
	}
	if w.quiescing {
		return errors.WithStack(errStorageWorkerClosed)
	}
	permit, err := budget.tryAcquire(work.retainedBytes)
	if err != nil {
		return err
	}
	select {
	case w.requests <- reservedStorageWork{work: work, permit: permit}:
		return nil
	default:
		permit.Release()
		return errors.WithStack(errStorageWorkBackpressure)
	}
}

// quiesce stops admission but neither cancels executing I/O nor pretends queued
// work is complete. It is safe to race admission and repeated shutdown requests.
func (w *storageWorker) quiesce() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.quiesceLocked()
}

func (w *storageWorker) quiesceLocked() {
	if !w.quiescing {
		w.quiescing = true
		close(w.requests)
	}
}

func (w *storageWorker) run() {
	defer close(w.done)
	defer close(w.completions)
	var failure error
	for task := range w.requests {
		completion := storageWorkCompletion{token: task.work.token, permit: task.permit, err: failure}
		if failure == nil {
			completion.value, completion.err = runStorageWork(task.work.run)
			if completion.err != nil {
				completion.value = nil
				failure = completion.err
				w.mu.Lock()
				w.failure = failure
				w.quiesceLocked()
				w.mu.Unlock()
			}
		}
		// Do not select on cancellation here: accepted ownership must reach the
		// owner even after a fatal error, with the original error preserved.
		w.completions <- completion
	}
}

func runStorageWork(run func() (any, error)) (value any, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			value = nil
			err = errors.Wrapf(errStorageWorkerPanic, "%v\n%s", recovered, debug.Stack())
		}
	}()
	return run()
}
