package kv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
)

const defaultTSOLeaderPollInterval = 25 * time.Millisecond

var (
	ErrTSOAllocatorRequired = errors.New("tso: allocator is required")
	ErrTSOCoordinatorNil    = errors.New("tso: coordinator is required")
	ErrTSOClockNil          = errors.New("tso: coordinator clock is nil")
	ErrInvalidTSOBatchSize  = errors.New("tso: invalid batch size")
)

// TSOAllocator issues globally monotonic timestamps. NextBatch returns the
// first timestamp in a consecutive window [base, base+n-1].
type TSOAllocator interface {
	Next(ctx context.Context) (uint64, error)
	NextBatch(ctx context.Context, n int) (uint64, error)
	IsLeader() bool
	RunLeaseRenewal(ctx context.Context)
}

type tsoCoordinator interface {
	IsLeader() bool
	Clock() *HLC
}

type tsoLeaseRenewer interface {
	RunHLCLeaseRenewal(context.Context)
}

type LocalTSOAllocator struct {
	coord        tsoCoordinator
	pollInterval time.Duration
}

type LocalTSOAllocatorOption func(*LocalTSOAllocator)

func WithTSOLeaderPollInterval(interval time.Duration) LocalTSOAllocatorOption {
	return func(a *LocalTSOAllocator) {
		if interval > 0 {
			a.pollInterval = interval
		}
	}
}

func NewLocalTSOAllocator(coord tsoCoordinator, opts ...LocalTSOAllocatorOption) (*LocalTSOAllocator, error) {
	if coord == nil {
		return nil, ErrTSOCoordinatorNil
	}
	a := &LocalTSOAllocator{
		coord:        coord,
		pollInterval: defaultTSOLeaderPollInterval,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a, nil
}

func (a *LocalTSOAllocator) Next(ctx context.Context) (uint64, error) {
	return a.NextBatch(ctx, 1)
}

func (a *LocalTSOAllocator) NextBatch(ctx context.Context, n int) (uint64, error) {
	if n <= 0 {
		return 0, errors.WithStack(ErrInvalidTSOBatchSize)
	}
	if err := a.waitLeader(ctx); err != nil {
		return 0, err
	}
	clock := a.coord.Clock()
	if clock == nil {
		return 0, ErrTSOClockNil
	}
	base, err := clock.NextBatchFenced(n)
	return base, errors.Wrap(err, "tso next batch")
}

func (a *LocalTSOAllocator) IsLeader() bool {
	return a.coord != nil && a.coord.IsLeader()
}

func (a *LocalTSOAllocator) RunLeaseRenewal(ctx context.Context) {
	if renewer, ok := a.coord.(tsoLeaseRenewer); ok {
		renewer.RunHLCLeaseRenewal(ctx)
		return
	}
	<-ctx.Done()
}

func (a *LocalTSOAllocator) waitLeader(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if a.IsLeader() {
		return nil
	}
	timer := time.NewTimer(a.pollInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "tso wait leader")
		case <-timer.C:
			if a.IsLeader() {
				return nil
			}
			timer.Reset(a.pollInterval)
		}
	}
}

type windowSnapshot struct {
	base   uint64
	size   uint64
	offset atomic.Uint64
}

// BatchAllocator serves local timestamps from immutable windows fetched from a
// TSOAllocator. The hot path is lock-free: callers claim a slot with atomic Add
// on the currently published window.
type BatchAllocator struct {
	tso       TSOAllocator
	batchSize int
	win       atomic.Pointer[windowSnapshot]

	mu         sync.Mutex
	refillDone chan struct{}
}

func NewBatchAllocator(tso TSOAllocator, batchSize int) (*BatchAllocator, error) {
	if tso == nil {
		return nil, ErrTSOAllocatorRequired
	}
	if batchSize <= 0 {
		return nil, errors.WithStack(ErrInvalidTSOBatchSize)
	}
	return &BatchAllocator{tso: tso, batchSize: batchSize}, nil
}

func (b *BatchAllocator) Next(ctx context.Context) (uint64, error) {
	for {
		if err := ctxErr(ctx); err != nil {
			return 0, err
		}
		if ts, ok := b.tryWindow(); ok {
			return ts, nil
		}
		if err := b.refill(ctx); err != nil {
			return 0, err
		}
	}
}

func (b *BatchAllocator) tryWindow() (uint64, bool) {
	w := b.win.Load()
	if w == nil {
		return 0, false
	}
	off := w.offset.Add(1) - 1
	if off >= w.size {
		return 0, false
	}
	return w.base + off, true
}

func (b *BatchAllocator) refill(ctx context.Context) error {
	b.mu.Lock()
	if w := b.win.Load(); w != nil && w.offset.Load() < w.size {
		b.mu.Unlock()
		return nil
	}
	if b.refillDone != nil {
		ch := b.refillDone
		b.mu.Unlock()
		select {
		case <-ch:
			return nil
		case <-ctxDone(ctx):
			return ctxErr(ctx)
		}
	}
	ch := make(chan struct{})
	b.refillDone = ch
	b.mu.Unlock()

	var (
		base    uint64
		success bool
	)
	err := func() (retErr error) {
		defer func() {
			b.mu.Lock()
			if success {
				b.win.Store(&windowSnapshot{base: base, size: positiveIntToUint64(b.batchSize)})
			}
			b.refillDone = nil
			b.mu.Unlock()
			close(ch)
		}()
		base, retErr = b.tso.NextBatch(ctx, b.batchSize)
		success = retErr == nil
		return errors.Wrap(retErr, "tso next batch")
	}()
	return errors.Wrap(err, "tso refill batch")
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return errors.Wrap(ctx.Err(), "context error")
}

func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

func positiveIntToUint64(v int) uint64 {
	return uint64(v) //nolint:gosec // callers validate v as positive batch/window size.
}
