package adapter

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// backgroundWorkers runs the jobs a request starts but does not wait for,
// such as the cleanup of a deleted table's old generation. Each job is
// counted in an inflightGate so the server's Stop can join it, bounded,
// before the stores close underneath it. Stop cancels jobs already started
// once the bound has passed; reservations that commit later get a live
// timeout and stay counted in the final join. Reservations cannot start
// cleanup after that final join ends. This bounded drain does not provide
// durable recovery of cleanup for mutations that outlive shutdown.
// A nil *backgroundWorkers
// runs every job detached under context.Background() and has nothing to
// stop, which keeps zero-value servers in tests working.
type backgroundWorkers struct {
	gate    *inflightGate
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	running map[*backgroundReservation]context.CancelFunc
	closed  bool
}

func newBackgroundWorkers() *backgroundWorkers {
	ctx, cancel := context.WithCancel(context.Background())
	return &backgroundWorkers{gate: newInflightGate(), ctx: ctx, cancel: cancel, running: make(map[*backgroundReservation]context.CancelFunc)}
}

// backgroundReservation is a job slot claimed before the commit that makes
// the job necessary, so the job can still start after that commit even when
// stop began in between: stop waits for the slot as for a running job. The
// holder either starts the job through it or releases it.
type backgroundReservation struct {
	mu   sync.Mutex
	w    *backgroundWorkers
	used bool
}

// reserve claims a job slot, or reports false once stop has begun, in which
// case the caller must not commit the change the job would follow up on.
func (w *backgroundWorkers) reserve() (*backgroundReservation, bool) {
	if w != nil && !w.gate.enter() {
		return nil, false
	}
	return &backgroundReservation{w: w}, true
}

// start runs run on its own goroutine under a context bounded by timeout,
// handing it the reserved slot. A reservation starts at most one job.
func (r *backgroundReservation) start(timeout time.Duration, run func(ctx context.Context)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.used {
		return
	}
	r.used = true
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	if r.w != nil {
		r.w.mu.Lock()
		if r.w.closed {
			r.w.mu.Unlock()
			cancel()
			r.w.gate.exit()
			return
		}
		r.w.running[r] = cancel
		r.w.mu.Unlock()
	}
	go func() {
		defer cancel()
		if r.w != nil {
			defer r.w.gate.exit()
			defer func() {
				r.w.mu.Lock()
				delete(r.w.running, r)
				r.w.mu.Unlock()
			}()
		}
		run(ctx)
	}()
}

// release gives back a slot no job was started through; it is a no-op once
// start ran or after an earlier release.
func (r *backgroundReservation) release() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.used {
		return
	}
	r.used = true
	if r.w != nil {
		r.w.gate.exit()
	}
}

// stop refuses new reservations, waits up to timeout for the reserved slots
// and the running jobs, then cancels the context of any jobs that remain and
// waits up to joinTimeout for them to return, so none is still dispatching
// when the stores close. A call made while an earlier one is still waiting
// waits until the same deadlines (see inflightGate); name labels the
// warnings it logs.
func (w *backgroundWorkers) stop(name string, timeout, joinTimeout time.Duration) {
	if w == nil {
		return
	}
	defer w.closeReservations()
	finished := w.gate.drain(timeout)
	w.cancelRunning()
	if finished {
		return
	}
	slog.Warn(name+": background jobs still running after the shutdown bound, cancelling them",
		slog.Int64("in_flight", w.gate.remaining()),
		slog.Duration("timeout", timeout),
	)
	if !w.gate.join(joinTimeout) {
		slog.Warn(name+": cancelled background jobs did not return, continuing shutdown",
			slog.Int64("in_flight", w.gate.remaining()),
			slog.Duration("timeout", joinTimeout),
		)
	}
}

func (w *backgroundWorkers) closeReservations() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	for _, cancel := range w.running {
		cancel()
	}
}

func (w *backgroundWorkers) cancelRunning() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.cancel()
	// Reservations still owned by a mutation are not jobs yet. If that
	// mutation commits later, its cleanup gets a live, independently bounded
	// context and remains counted in the join window.
	for _, cancel := range w.running {
		cancel()
	}
}
