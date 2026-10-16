package adapter

import (
	"context"
	"log/slog"
	"time"
)

// backgroundWorkers runs the jobs a request starts but does not wait for,
// such as the cleanup of a deleted table's old generation. Each job is
// counted in an inflightGate so the server's Stop can join it, bounded,
// before the stores close underneath it, and its context derives from a
// parent Stop cancels once the bound has passed. A nil *backgroundWorkers
// runs every job detached under context.Background() and has nothing to
// stop, which keeps zero-value servers in tests working.
type backgroundWorkers struct {
	gate   *inflightGate
	ctx    context.Context
	cancel context.CancelFunc
}

func newBackgroundWorkers() *backgroundWorkers {
	ctx, cancel := context.WithCancel(context.Background())
	return &backgroundWorkers{gate: newInflightGate(), ctx: ctx, cancel: cancel}
}

// start runs run on its own goroutine under a context bounded by timeout,
// and reports whether it did. Once stop has begun it runs nothing and
// returns false.
func (w *backgroundWorkers) start(timeout time.Duration, run func(ctx context.Context)) bool {
	parent := context.Background()
	if w != nil {
		if !w.gate.enter() {
			return false
		}
		parent = w.ctx
	}
	go func() {
		if w != nil {
			defer w.gate.exit()
		}
		ctx, cancel := context.WithTimeout(parent, timeout)
		defer cancel()
		run(ctx)
	}()
	return true
}

// stop refuses new jobs, waits up to timeout for the running ones, then
// cancels the context of any that remain and waits up to joinTimeout for
// them to return, so none is still dispatching when the stores close. Only
// the first call waits; name labels the warnings it logs.
func (w *backgroundWorkers) stop(name string, timeout, joinTimeout time.Duration) {
	if w == nil {
		return
	}
	finished := w.gate.drain(timeout)
	w.cancel()
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
