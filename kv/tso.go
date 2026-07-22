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

// TimestampAllocator is the minimal timestamp source the coordinators need.
// TSOAllocator and BatchAllocator both satisfy it; keeping this interface
// narrow lets production use a batched TSO while tests can inject a tiny fake.
type TimestampAllocator interface {
	Next(ctx context.Context) (uint64, error)
}

type TimestampAfterAllocator interface {
	NextAfter(ctx context.Context, min uint64) (uint64, error)
}

type tsoBatchAfterAllocator interface {
	NextBatchAfter(ctx context.Context, n int, min uint64) (uint64, error)
}

type timestampIssuer interface {
	IsTimestampLeader() bool
}

type timestampWindowInvalidator interface {
	Invalidate()
}

func invalidateTimestampWindow(alloc TimestampAllocator) {
	if inv, ok := alloc.(timestampWindowInvalidator); ok {
		inv.Invalidate()
	}
}

// NextTimestampThrough allocates a persistence-grade timestamp through coord's
// optional TSO allocator when available, falling back to the legacy HLC clock
// for older Coordinator implementations and tests.
func NextTimestampThrough(ctx context.Context, coord Coordinator, label string) (uint64, error) {
	if alloc, ok := coordinatorTimestampAllocator(coord); ok {
		return nextTimestampFromAllocator(ctx, alloc, label)
	}
	if coord == nil || coord.Clock() == nil {
		return 1, nil
	}
	ts, err := coord.Clock().NextFenced()
	if err != nil {
		return 0, errors.Wrap(err, label)
	}
	return ts, nil
}

// NextTimestampAfterThrough allocates a timestamp strictly greater than
// startTS. It observes startTS into legacy HLC clocks before allocation so the
// fallback path preserves the previous OCC ordering contract.
func NextTimestampAfterThrough(ctx context.Context, coord Coordinator, startTS uint64, label string) (uint64, error) {
	if startTS == ^uint64(0) {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	if alloc, ok := coordinatorTimestampAllocator(coord); ok {
		return nextTimestampAfterFromAllocator(ctx, alloc, startTS, label)
	}
	clock := coordinatorClock(coord)
	if clock == nil {
		return nextTimestampAfterFallback(startTS)
	}
	clock.Observe(startTS)
	return nextTimestampAfterObserved(ctx, coord, clock, startTS, label)
}

func coordinatorTimestampAllocator(coord Coordinator) (TimestampAllocator, bool) {
	switch c := coord.(type) {
	case *Coordinate:
		if c != nil && c.tsAllocator != nil {
			return c.tsAllocator, true
		}
		return nil, false
	case *ShardedCoordinator:
		if c != nil && c.tsAllocator != nil {
			return c.tsAllocator, true
		}
		return nil, false
	default:
		alloc, ok := coord.(TimestampAllocator)
		return alloc, ok
	}
}

func nextTimestampFromAllocator(ctx context.Context, alloc TimestampAllocator, label string) (uint64, error) {
	ts, err := alloc.Next(ctx)
	if err != nil {
		return 0, errors.Wrap(err, label)
	}
	return ts, nil
}

func nextTimestampAfterFromAllocator(ctx context.Context, alloc TimestampAllocator, min uint64, label string) (uint64, error) {
	if after, ok := alloc.(TimestampAfterAllocator); ok {
		ts, err := after.NextAfter(ctx, min)
		if err != nil {
			return 0, errors.Wrap(err, label)
		}
		return ts, nil
	}
	ts, err := nextTimestampFromAllocator(ctx, alloc, label)
	if err != nil {
		return 0, err
	}
	if ts <= min {
		return 0, errors.Wrap(ErrTxnCommitTSRequired, label)
	}
	return ts, nil
}

func coordinatorClock(coord Coordinator) *HLC {
	if coord == nil {
		return nil
	}
	return coord.Clock()
}

func nextTimestampAfterObserved(ctx context.Context, coord Coordinator, clock *HLC, startTS uint64, label string) (uint64, error) {
	ts, err := NextTimestampThrough(ctx, coord, label)
	if err != nil {
		return 0, err
	}
	if ts > startTS {
		return ts, nil
	}
	clock.Observe(startTS)
	retry, err := NextTimestampThrough(ctx, coord, "re-"+label)
	if err != nil {
		return 0, err
	}
	if retry <= startTS {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return retry, nil
}

func nextTimestampAfterFallback(startTS uint64) (uint64, error) {
	nextTS := startTS + 1
	if nextTS == 0 {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return nextTS, nil
}

type tsoCoordinator interface {
	IsLeader() bool
	Clock() *HLC
}

type tsoLeaseRenewer interface {
	RunHLCLeaseRenewal(context.Context)
}

type tsoLeaseProposer interface {
	ProposeHLCLease(context.Context, int64) error
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
	return a.nextBatchAfter(ctx, n, 0)
}

func (a *LocalTSOAllocator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	return a.NextBatchAfter(ctx, 1, min)
}

func (a *LocalTSOAllocator) NextBatchAfter(ctx context.Context, n int, min uint64) (uint64, error) {
	if min == ^uint64(0) {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return a.nextBatchAfter(ctx, n, min)
}

func (a *LocalTSOAllocator) nextBatchAfter(ctx context.Context, n int, min uint64) (uint64, error) {
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
	if min > 0 {
		clock.Observe(min)
	}
	base, err := clock.NextBatchFenced(n)
	if errors.Is(err, ErrCeilingExpired) {
		if renewErr := a.renewExpiredCeiling(ctx); renewErr != nil {
			return 0, errors.Wrap(renewErr, "tso renew expired HLC lease")
		}
		if min > 0 {
			clock.Observe(min)
		}
		base, err = clock.NextBatchFenced(n)
	}
	return base, errors.Wrap(err, "tso next batch")
}

func (a *LocalTSOAllocator) renewExpiredCeiling(ctx context.Context) error {
	proposer, ok := a.coord.(tsoLeaseProposer)
	if !ok {
		return errors.WithStack(ErrCeilingExpired)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	pctx, cancel := context.WithTimeout(ctx, hlcRenewalInterval)
	defer cancel()
	ceilingMs := time.Now().UnixMilli() + hlcPhysicalWindowMs
	return errors.Wrap(proposer.ProposeHLCLease(pctx, ceilingMs), "propose HLC lease")
}

func (a *LocalTSOAllocator) IsLeader() bool {
	if a == nil || a.coord == nil {
		return false
	}
	if issuer, ok := a.coord.(timestampIssuer); ok {
		return issuer.IsTimestampLeader()
	}
	return a.coord.IsLeader()
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
	epoch  uint64
	offset atomic.Uint64
}

// BatchAllocator serves local timestamps from immutable windows fetched from a
// TSOAllocator. The hot path is lock-free: callers claim a slot with atomic Add
// on the currently published window.
type BatchAllocator struct {
	tso       TSOAllocator
	batchSize int
	win       atomic.Pointer[windowSnapshot]
	epoch     atomic.Uint64

	mu         sync.Mutex
	refillDone chan struct{}
}

func NewBatchAllocator(tso TSOAllocator, batchSize int) (*BatchAllocator, error) {
	if tso == nil {
		return nil, ErrTSOAllocatorRequired
	}
	if batchSize <= 0 || batchSize > maxHLCBatchSize {
		return nil, errors.WithStack(ErrInvalidTSOBatchSize)
	}
	return &BatchAllocator{tso: tso, batchSize: batchSize}, nil
}

func (b *BatchAllocator) Invalidate() {
	if b == nil {
		return
	}
	b.epoch.Add(1)
	b.win.Store(nil)
}

func (b *BatchAllocator) Next(ctx context.Context) (uint64, error) {
	return b.nextAfter(ctx, 0)
}

func (b *BatchAllocator) NextAfter(ctx context.Context, min uint64) (uint64, error) {
	if min == ^uint64(0) {
		return 0, errors.WithStack(ErrTxnCommitTSRequired)
	}
	return b.nextAfter(ctx, min)
}

func (b *BatchAllocator) nextAfter(ctx context.Context, min uint64) (uint64, error) {
	for {
		if err := ctxErr(ctx); err != nil {
			return 0, err
		}
		if ts, ok := b.tryWindowAfter(min); ok {
			return ts, nil
		}
		if err := b.refill(ctx, min); err != nil {
			return 0, err
		}
	}
}

func (b *BatchAllocator) tryWindowAfter(min uint64) (uint64, bool) {
	w := b.win.Load()
	if w == nil {
		return 0, false
	}
	if w.epoch != b.epoch.Load() {
		return 0, false
	}
	for {
		off := w.offset.Add(1) - 1
		if off >= w.size || w.base > ^uint64(0)-off {
			return 0, false
		}
		ts := w.base + off
		if ts <= min {
			continue
		}
		if w.epoch != b.epoch.Load() || b.win.Load() != w {
			return 0, false
		}
		return ts, true
	}
}

func (b *BatchAllocator) refill(ctx context.Context, min uint64) error {
	b.mu.Lock()
	currentEpoch := b.epoch.Load()
	if w := b.win.Load(); w != nil && w.epoch == currentEpoch && w.offset.Load() < w.size {
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
		epoch   = b.epoch.Load()
		success bool
	)
	err := func() (retErr error) {
		defer func() {
			b.mu.Lock()
			if success && epoch == b.epoch.Load() {
				b.win.Store(&windowSnapshot{base: base, size: positiveIntToUint64(b.batchSize), epoch: epoch})
			}
			b.refillDone = nil
			b.mu.Unlock()
			close(ch)
		}()
		base, retErr = b.nextRefillBatch(ctx, min)
		success = retErr == nil
		return errors.Wrap(retErr, "tso next batch")
	}()
	return errors.Wrap(err, "tso refill batch")
}

func (b *BatchAllocator) nextRefillBatch(ctx context.Context, min uint64) (uint64, error) {
	if after, ok := b.tso.(tsoBatchAfterAllocator); ok && min > 0 {
		base, err := after.NextBatchAfter(ctx, b.batchSize, min)
		return base, errors.Wrap(err, "tso next batch after observed ts")
	}
	base, err := b.tso.NextBatch(ctx, b.batchSize)
	return base, errors.Wrap(err, "tso next batch")
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
