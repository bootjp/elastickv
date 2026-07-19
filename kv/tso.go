package kv

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"
)

const defaultTSOLeaderPollInterval = 25 * time.Millisecond

// MaxTSOBatchSize is the largest consecutive timestamp window accepted by a
// TSO allocator or the Distribution.GetTimestamp RPC.
const MaxTSOBatchSize = maxHLCBatchSize

var (
	ErrTSOAllocatorRequired  = errors.New("tso: allocator is required")
	ErrTSOCoordinatorNil     = errors.New("tso: coordinator is required")
	ErrTSOClockNil           = errors.New("tso: coordinator clock is nil")
	ErrInvalidTSOBatchSize   = errors.New("tso: invalid batch size")
	ErrTSOPhaseDInactive     = errors.New("tso: phase D is not active")
	ErrTSOTimestampInvalid   = errors.New("tso: timestamp is not a durable phase-D allocation")
	ErrTSOTimestampPrePhaseD = errors.New("tso: timestamp predates phase D")
	ErrTSOReadVoucherLimit   = errors.New("tso: applied read timestamp voucher limit reached")
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

// TimestampAllocatorProvider lets coordinator decorators preserve access to
// the configured allocator without making the decorator itself an allocator.
type TimestampAllocatorProvider interface {
	TimestampAllocator() TimestampAllocator
}

type TimestampAfterAllocator interface {
	NextAfter(ctx context.Context, min uint64) (uint64, error)
}

// DurableTimestampValidator verifies that a timestamp belongs to the durable
// post-Phase-D allocation range owned by the dedicated TSO group.
type DurableTimestampValidator interface {
	ValidateDurableTimestamp(context.Context, uint64) error
}

// TSOPhaseDState exposes the one-way Phase-D state to coordinators and adapter
// migration helpers without coupling them to TSOStateMachine.
type TSOPhaseDState interface {
	PhaseDActive() bool
	PhaseDRequired() bool
}

// AppliedReadTimestampVoucher records an adapter-provided applied watermark.
// It is a process-local capability used only to distinguish audited adapter
// snapshots from arbitrary caller-supplied StartTS values during Phase D.
type AppliedReadTimestampVoucher interface {
	VouchAppliedReadTimestamp(uint64) error
}

// ReadTimestamp is the adapter-side result of beginning a transaction snapshot.
// When it represents an applied pre-Phase-D watermark, it also carries a
// process-local capability that can reserve exactly one coordinator voucher per
// DispatchWithReadTimestamp call. The capability cannot be constructed outside
// this package because both the timestamp and voucher state are private.
type ReadTimestamp struct {
	timestamp uint64
	voucher   *appliedReadDispatchVoucher
}

func (t ReadTimestamp) Timestamp() uint64 {
	return t.timestamp
}

type appliedReadDispatchVoucher struct {
	mu       sync.Mutex
	prepared uint64
}

type appliedReadDispatchVoucherContextKey struct{}

// WithDispatchVoucher binds this read timestamp's process-local capability to
// ctx. A timestamp without a voucher is still bound so it shadows any parent
// capability instead of accidentally inheriting authority for an older read.
func (t ReadTimestamp) WithDispatchVoucher(ctx context.Context) context.Context {
	return context.WithValue(nonNilTSOContext(ctx), appliedReadDispatchVoucherContextKey{}, t)
}

// DispatchWithReadTimestamp dispatches an OCC operation under the applied-read
// capability bound by ReadTimestamp.WithDispatchVoucher. The first dispatch
// consumes the voucher reserved by BeginReadTimestampThrough; every subsequent
// dispatch reserves one additional use immediately before dispatching.
func DispatchWithReadTimestamp(
	ctx context.Context,
	coord Coordinator,
	reqs *OperationGroup[OP],
) (*CoordinateResponse, error) {
	if ctx == nil {
		resp, err := coord.Dispatch(ctx, reqs)
		return resp, errors.WithStack(err)
	}
	readTimestamp, ok := ctx.Value(appliedReadDispatchVoucherContextKey{}).(ReadTimestamp)
	if !ok || readTimestamp.voucher == nil {
		resp, err := coord.Dispatch(ctx, reqs)
		return resp, errors.WithStack(err)
	}
	if reqs == nil || reqs.StartTS != readTimestamp.timestamp {
		return nil, errors.WithStack(ErrTSOTimestampInvalid)
	}
	if err := readTimestamp.voucher.prepare(coord, readTimestamp.timestamp); err != nil {
		return nil, err
	}
	resp, err := coord.Dispatch(ctx, reqs)
	return resp, errors.WithStack(err)
}

func (v *appliedReadDispatchVoucher) prepare(coord Coordinator, timestamp uint64) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.prepared == 0 {
		v.prepared = 1
		return nil
	}
	voucher, ok := coord.(AppliedReadTimestampVoucher)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	if err := voucher.VouchAppliedReadTimestamp(timestamp); err != nil {
		return errors.WithStack(err)
	}
	v.prepared++
	return nil
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

// TimestampAllocatorThrough returns the allocator configured behind a
// coordinator or coordinator decorator.
func TimestampAllocatorThrough(coord Coordinator) (TimestampAllocator, bool) {
	if provider, ok := coord.(TimestampAllocatorProvider); ok {
		alloc := provider.TimestampAllocator()
		return alloc, alloc != nil
	}
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

func coordinatorTimestampAllocator(coord Coordinator) (TimestampAllocator, bool) {
	return TimestampAllocatorThrough(coord)
}

// BeginReadTimestampThrough preserves the caller's applied-snapshot watermark.
// Once Phase D is requested, this boundary activates it before validation. An
// applied pre-D watermark receives a bounded one-use coordinator voucher;
// arbitrary caller timestamps remain subject to group-0 numeric validation.
// The returned timestamp must be used for every read and OperationGroup.StartTS.
func BeginReadTimestampThrough(
	ctx context.Context,
	coord Coordinator,
	legacyTimestamp uint64,
	label string,
) (ReadTimestamp, error) {
	alloc, ok := coordinatorTimestampAllocator(coord)
	if !ok {
		return ReadTimestamp{timestamp: legacyTimestamp}, nil
	}
	validator, phaseD, phaseDRequired, err := phaseDTimestampValidator(alloc)
	if err != nil {
		return ReadTimestamp{}, errors.Wrap(err, label)
	}
	if !phaseDRequired {
		return ReadTimestamp{timestamp: legacyTimestamp}, nil
	}
	if legacyTimestamp == 0 || legacyTimestamp == ^uint64(0) {
		return ReadTimestamp{}, errors.Wrap(ErrTSOTimestampInvalid, label)
	}
	if err := activatePhaseDForRead(ctx, alloc, phaseD, label); err != nil {
		return ReadTimestamp{}, err
	}
	vouched, err := validateAppliedReadTimestamp(ctx, coord, validator, legacyTimestamp, label)
	if err != nil {
		return ReadTimestamp{}, err
	}
	readTimestamp := ReadTimestamp{timestamp: legacyTimestamp}
	if vouched {
		readTimestamp.voucher = &appliedReadDispatchVoucher{}
	}
	return readTimestamp, nil
}

func activatePhaseDForRead(
	ctx context.Context,
	alloc TimestampAllocator,
	phaseD TSOPhaseDState,
	label string,
) error {
	if phaseD.PhaseDActive() {
		return nil
	}
	// Reserve and discard one post-D timestamp to commit the marker. The
	// caller's applied watermark remains the read snapshot; using this fresh
	// allocation for reads could run ahead of data-group apply.
	_, err := nextTimestampFromAllocator(nonNilTSOContext(ctx), alloc, label+": activate phase D")
	return err
}

func validateAppliedReadTimestamp(
	ctx context.Context,
	coord Coordinator,
	validator DurableTimestampValidator,
	timestamp uint64,
	label string,
) (bool, error) {
	err := validator.ValidateDurableTimestamp(nonNilTSOContext(ctx), timestamp)
	if err == nil {
		return false, nil
	}
	if !errors.Is(err, ErrTSOTimestampPrePhaseD) {
		return false, errors.Wrap(err, label)
	}
	voucher, ok := coord.(AppliedReadTimestampVoucher)
	if !ok {
		return false, errors.Wrap(ErrTSOProtocolUnsupported, label+": applied read voucher unavailable")
	}
	if err := voucher.VouchAppliedReadTimestamp(timestamp); err != nil {
		return false, errors.Wrap(err, label)
	}
	return true, nil
}

func phaseDTimestampValidator(alloc TimestampAllocator) (DurableTimestampValidator, TSOPhaseDState, bool, error) {
	phaseD, ok := alloc.(TSOPhaseDState)
	if !ok || (!phaseD.PhaseDRequired() && !phaseD.PhaseDActive()) {
		return nil, nil, false, nil
	}
	validator, ok := alloc.(DurableTimestampValidator)
	if !ok {
		return nil, phaseD, false, ErrTSOProtocolUnsupported
	}
	return validator, phaseD, true, nil
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
	return base, errors.Wrap(err, "tso next batch")
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
	tso        TSOAllocator
	batchSize  int
	win        atomic.Pointer[windowSnapshot]
	epoch      atomic.Uint64
	phaseDSeen atomic.Bool

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

func (b *BatchAllocator) ValidateDurableTimestamp(ctx context.Context, timestamp uint64) error {
	validator, ok := b.tso.(DurableTimestampValidator)
	if !ok {
		return errors.WithStack(ErrTSOProtocolUnsupported)
	}
	return errors.WithStack(validator.ValidateDurableTimestamp(ctx, timestamp))
}

func (b *BatchAllocator) PhaseDActive() bool {
	state, ok := b.tso.(TSOPhaseDState)
	return ok && state.PhaseDActive()
}

func (b *BatchAllocator) PhaseDRequired() bool {
	state, ok := b.tso.(TSOPhaseDState)
	return ok && state.PhaseDRequired()
}

func (b *BatchAllocator) nextAfter(ctx context.Context, min uint64) (uint64, error) {
	for {
		if err := ctxErr(ctx); err != nil {
			return 0, err
		}
		phaseDAtStart, invalidated := b.ensurePhaseDTransition()
		if invalidated {
			continue
		}
		if ts, ok := b.tryWindowAfter(min); ok {
			phaseDAfterClaim, invalidated := b.ensurePhaseDTransition()
			if invalidated || phaseDAfterClaim != phaseDAtStart {
				continue
			}
			return ts, nil
		}
		if err := b.refill(ctx, min); err != nil {
			return 0, err
		}
	}
}

func (b *BatchAllocator) ensurePhaseDTransition() (required, invalidated bool) {
	if !b.PhaseDRequired() {
		return false, false
	}
	if b.phaseDSeen.Load() {
		return true, false
	}
	// Publish phaseDSeen only after the old epoch is invalidated. Concurrent
	// callers may invalidate redundantly, but none can observe the transition as
	// complete while a pre-Phase-D window is still current.
	b.Invalidate()
	b.phaseDSeen.CompareAndSwap(false, true)
	return true, true
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
