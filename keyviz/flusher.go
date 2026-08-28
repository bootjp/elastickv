package keyviz

import (
	"context"
	"time"
)

// RunFlusher drives Sampler.Flush at the supplied interval until ctx
// is cancelled. Returns when ctx fires; the final tick is not
// executed (a graceful shutdown should call Sampler.Flush once more
// after RunFlusher returns if it wants to harvest the in-progress
// step).
//
// step <= 0 falls back to DefaultStep.
//
// This is a tiny wrapper so call sites in main.go don't need to spell
// out the ticker boilerplate; testing the boilerplate is the unit test
// for this package, not for callers.
func RunFlusher(ctx context.Context, s *MemSampler, step time.Duration) {
	if s == nil {
		<-ctx.Done()
		return
	}
	if step <= 0 {
		step = DefaultStep
	}
	t := time.NewTicker(step)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case at := <-t.C:
			if !s.flushWindow(ctx, at) {
				return
			}
		}
	}
}

// RunHotKeysAggregator drains sampled events and services exact-boundary
// snapshot requests from RunFlusher until ctx is cancelled.
func RunHotKeysAggregator(ctx context.Context, s *MemSampler) {
	if s == nil || s.hotKeys == nil {
		<-ctx.Done()
		return
	}
	s.hotKeys.run(ctx)
}
