package kv

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/keyviz"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const (
	testTSOBatchSize     = 3
	testTSOInitialBase   = 100
	testTSOPollInterval  = time.Millisecond
	testTSOWaitTimeout   = 5 * time.Millisecond
	testTSOFutureCeiling = time.Hour
)

func TestHLCNextBatchFencedReturnsConsecutiveWindow(t *testing.T) {
	h := NewHLC()
	h.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())

	base, err := h.NextBatchFenced(testTSOBatchSize)
	require.NoError(t, err)
	require.Equal(t, base+testTSOBatchSize-1, h.Current())
}

func TestHLCNextBatchFencedRejectsExpiredCeiling(t *testing.T) {
	h := NewHLC()
	h.SetPhysicalCeiling(time.Now().Add(-time.Millisecond).UnixMilli())

	_, err := h.NextBatchFenced(1)
	require.ErrorIs(t, err, ErrCeilingExpired)
}

func TestHLCNextBatchFencedBumpsWallOnLogicalOverflow(t *testing.T) {
	h := NewHLC()
	futureWall := uint64(time.Now().Add(testTSOFutureCeiling).UnixMilli()) //nolint:gosec // Unix ms is non-negative.
	h.Observe((futureWall << hlcLogicalBits) | (hlcLogicalMask - 1))

	base, err := h.NextBatchFenced(testTSOBatchSize)
	require.NoError(t, err)
	require.Equal(t, (futureWall+1)<<hlcLogicalBits, base)
	require.Equal(t, base+testTSOBatchSize-1, h.Current())
}

func TestBatchAllocatorReusesWindowUntilExhausted(t *testing.T) {
	tso := &fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true}
	alloc, err := NewBatchAllocator(tso, testTSOBatchSize)
	require.NoError(t, err)

	ctx := context.Background()
	for i := range testTSOBatchSize {
		got, err := alloc.Next(ctx)
		require.NoError(t, err)
		require.EqualValues(t, testTSOInitialBase+i, got)
	}
	got, err := alloc.Next(ctx)
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+testTSOBatchSize, got)
	require.EqualValues(t, 2, tso.calls.Load())
}

func TestBatchAllocatorInvalidateDropsCachedWindow(t *testing.T) {
	tso := &fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true}
	alloc, err := NewBatchAllocator(tso, testTSOBatchSize)
	require.NoError(t, err)

	got, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase, got)

	alloc.Invalidate()

	got, err = alloc.Next(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+testTSOBatchSize, got)
	require.EqualValues(t, 2, tso.calls.Load())
}

func TestBatchAllocatorNextAfterSkipsCachedSlotsBelowFloor(t *testing.T) {
	tso := &fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true}
	alloc, err := NewBatchAllocator(tso, testTSOBatchSize)
	require.NoError(t, err)

	got, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase, got)

	got, err = alloc.NextAfter(context.Background(), testTSOInitialBase+5)
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+6, got)
	require.EqualValues(t, 3, tso.calls.Load())
}

func TestBatchAllocatorRejectsInvalidatedWindowSnapshot(t *testing.T) {
	tso := &fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true}
	alloc, err := NewBatchAllocator(tso, testTSOBatchSize)
	require.NoError(t, err)

	alloc.win.Store(&windowSnapshot{
		base:  testTSOInitialBase,
		size:  positiveIntToUint64(testTSOBatchSize),
		epoch: alloc.epoch.Load(),
	})
	alloc.Invalidate()

	got, ok := alloc.tryWindowAfter(0)
	require.False(t, ok)
	require.Zero(t, got)
}

func TestCoordinateLeaderLossInvalidatesTimestampWindow(t *testing.T) {
	tso := &fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true}
	alloc, err := NewBatchAllocator(tso, testTSOBatchSize)
	require.NoError(t, err)
	eng := &fakeLeaseEngine{leaseDur: time.Second}
	coord := NewCoordinatorWithEngine(nil, eng, WithTSOAllocator(alloc))
	defer func() { require.NoError(t, coord.Close()) }()

	got, err := coord.Next(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase, got)

	eng.fireLeaderLoss()

	got, err = coord.Next(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+testTSOBatchSize, got)
	require.EqualValues(t, 2, tso.calls.Load())
}

func TestShardedCoordinatorLeaderLossInvalidatesTimestampWindow(t *testing.T) {
	tso := &fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true}
	alloc, err := NewBatchAllocator(tso, testTSOBatchSize)
	require.NoError(t, err)
	eng := &fakeLeaseEngine{leaseDur: time.Second}
	dist := distribution.NewEngine()
	dist.UpdateRoute(nil, nil, 1)
	coord := NewShardedCoordinator(dist, map[uint64]*ShardGroup{
		1: {Engine: eng},
	}, 1, NewHLC(), nil).WithTSOAllocator(alloc)
	defer func() { require.NoError(t, coord.Close()) }()

	got, err := coord.Next(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase, got)

	eng.fireLeaderLoss()

	got, err = coord.Next(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+testTSOBatchSize, got)
	require.EqualValues(t, 2, tso.calls.Load())
}

func TestNewBatchAllocatorRejectsOversizedBatch(t *testing.T) {
	_, err := NewBatchAllocator(&fakeTSOAllocator{leader: true}, maxHLCBatchSize+1)
	require.ErrorIs(t, err, ErrInvalidTSOBatchSize)
}

func TestLocalTSOAllocatorWaitsForLeadership(t *testing.T) {
	coord := &fakeTSOCoordinator{clock: NewHLC()}
	alloc, err := NewLocalTSOAllocator(coord, WithTSOLeaderPollInterval(testTSOPollInterval))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), testTSOWaitTimeout)
	defer cancel()
	_, err = alloc.Next(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	coord.leader.Store(true)
	coord.clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	got, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.NotZero(t, got)
}

func TestLocalTSOAllocatorAcceptsTimestampLeader(t *testing.T) {
	coord := &fakeTimestampLeaderCoordinator{clock: NewHLC()}
	coord.clock.SetPhysicalCeiling(time.Now().Add(testTSOFutureCeiling).UnixMilli())
	coord.timestampLeader.Store(true)

	alloc, err := NewLocalTSOAllocator(coord, WithTSOLeaderPollInterval(testTSOPollInterval))
	require.NoError(t, err)

	got, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.NotZero(t, got)
	require.False(t, coord.IsLeader())
}

func TestShardedCoordinatorReportsAnyShardAsTimestampLeader(t *testing.T) {
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {},
		2: {Engine: stubLeaderEngine{}},
	}, 1, NewHLC(), nil)

	require.False(t, coord.IsLeader())
	require.True(t, coord.IsTimestampLeader())
}

func TestShardedCoordinatorDedicatedGroupDoesNotBlockDataLeaderTimestampBridge(t *testing.T) {
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		0: {Engine: &stubFollowerEngine{}},
		1: {Engine: stubLeaderEngine{}},
	}, 1, NewHLC(), nil)

	require.True(t, coord.IsTimestampLeader())
}

func TestShardedCoordinatorTimestampBridgeIgnoresReservedGroupLeader(t *testing.T) {
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	newCoord := func() *ShardedCoordinator {
		return NewShardedCoordinator(engine, map[uint64]*ShardGroup{
			0: {Engine: stubLeaderEngine{}},
			1: {Engine: &stubFollowerEngine{}},
		}, 1, NewHLC(), nil)
	}

	t.Run("configured data groups", func(t *testing.T) {
		require.False(t, newCoord().WithAllShardGroups(1).IsTimestampLeader())
	})

	t.Run("legacy fallback", func(t *testing.T) {
		require.False(t, newCoord().IsTimestampLeader())
	})
}

func TestShardedCoordinatorUsesDedicatedTimestampGroupLeader(t *testing.T) {
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	dataLeaderOnly := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		0: {Engine: &stubFollowerEngine{}},
		1: {Engine: stubLeaderEngine{}},
		2: {Engine: &stubFollowerEngine{}},
	}, 1, NewHLC(), nil).WithTimestampGroup(0)
	require.False(t, dataLeaderOnly.IsTimestampLeader())

	tsoLeader := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		0: {Engine: stubLeaderEngine{}},
		1: {Engine: &stubFollowerEngine{}},
		2: {Engine: &stubFollowerEngine{}},
	}, 1, NewHLC(), nil).WithTimestampGroup(0)
	require.True(t, tsoLeader.IsTimestampLeader())
}

func TestNextTimestampAfterThroughFallbackWithoutCoordinatorClock(t *testing.T) {
	got, err := NextTimestampThrough(context.Background(), &Coordinate{}, "test")
	require.NoError(t, err)
	require.EqualValues(t, 1, got)

	got, err = NextTimestampAfterThrough(context.Background(), nil, testTSOInitialBase, "test")
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+1, got)

	got, err = NextTimestampAfterThrough(context.Background(), &Coordinate{}, testTSOInitialBase, "test")
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+1, got)

	_, err = NextTimestampAfterThrough(context.Background(), nil, ^uint64(0), "test")
	require.ErrorIs(t, err, ErrTxnCommitTSRequired)
}

func TestNextTimestampAfterThroughUsesAllocatorFloor(t *testing.T) {
	tso := &fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true}
	alloc, err := NewBatchAllocator(tso, testTSOBatchSize)
	require.NoError(t, err)
	coord := &Coordinate{tsAllocator: alloc}

	got, err := NextTimestampThrough(context.Background(), coord, "test")
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase, got)

	got, err = NextTimestampAfterThrough(context.Background(), coord, testTSOInitialBase+5, "test")
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+6, got)
}

func TestBeginReadTimestampThroughPreservesLegacyBeforePhaseD(t *testing.T) {
	alloc := &phaseDTestAllocator{next: testTSOInitialBase}
	coord := &Coordinate{tsAllocator: alloc}

	readTS, err := BeginReadTimestampThrough(context.Background(), coord, 42, "test read timestamp")
	require.NoError(t, err)
	require.Equal(t, uint64(42), readTS.Timestamp())
	require.Zero(t, alloc.nextCalls.Load())
	require.Zero(t, alloc.validateCalls.Load())
}

func TestBeginReadTimestampThroughValidatesAppliedWatermarkDuringPhaseD(t *testing.T) {
	alloc := &phaseDTestAllocator{next: testTSOInitialBase, phaseDActive: true, phaseDRequired: true}
	coord := &Coordinate{tsAllocator: alloc}

	readTS, err := BeginReadTimestampThrough(context.Background(), coord, 42, "test read timestamp")
	require.NoError(t, err)
	require.Equal(t, uint64(42), readTS.Timestamp())
	require.Zero(t, alloc.nextCalls.Load())
	require.Equal(t, uint64(1), alloc.validateCalls.Load())
	require.Equal(t, uint64(42), alloc.validated.Load())
}

func TestBeginReadTimestampThroughFailsClosedOnValidationError(t *testing.T) {
	alloc := &phaseDTestAllocator{
		next:           testTSOInitialBase,
		phaseDActive:   true,
		phaseDRequired: true,
		validateErr:    ErrTSOTimestampInvalid,
	}
	coord := &Coordinate{tsAllocator: alloc}

	_, err := BeginReadTimestampThrough(context.Background(), coord, 42, "test read timestamp")
	require.ErrorIs(t, err, ErrTSOTimestampInvalid)
	require.Zero(t, alloc.nextCalls.Load())
	require.Equal(t, uint64(1), alloc.validateCalls.Load())
}

func TestBeginReadTimestampThroughActivatesPhaseDBeforeValidation(t *testing.T) {
	alloc := &phaseDTestAllocator{next: testTSOInitialBase, phaseDRequired: true}
	coord := &Coordinate{tsAllocator: alloc}

	readTS, err := BeginReadTimestampThrough(context.Background(), coord, 42, "test phase D activation")
	require.NoError(t, err)
	require.Equal(t, uint64(42), readTS.Timestamp())
	require.Equal(t, uint64(1), alloc.nextCalls.Load())
	require.Equal(t, uint64(1), alloc.validateCalls.Load())
}

func TestBatchAllocatorDropsPrePhaseDWindowAfterActivation(t *testing.T) {
	raw := &phaseDWindowTSO{nextBase: testTSOInitialBase, leader: true}
	alloc, err := NewBatchAllocator(raw, testTSOBatchSize)
	require.NoError(t, err)

	first, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(testTSOInitialBase), first)
	raw.phaseD = true
	raw.floor = testTSOInitialBase + testTSOBatchSize - 1

	readTS, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(testTSOInitialBase+testTSOBatchSize), readTS)
	require.Equal(t, uint64(2), raw.calls.Load())
}

func TestBeginReadTimestampThroughFindsAllocatorBehindKeyVizDecorator(t *testing.T) {
	alloc := &phaseDTestAllocator{next: testTSOInitialBase, phaseDActive: true, phaseDRequired: true}
	coord := WithKeyVizLabel(&Coordinate{tsAllocator: alloc}, keyviz.LabelRedis)

	readTS, err := BeginReadTimestampThrough(context.Background(), coord, 42, "test decorated read timestamp")
	require.NoError(t, err)
	require.Equal(t, uint64(42), readTS.Timestamp())
	require.Equal(t, uint64(1), alloc.validateCalls.Load())
}

func TestCoordinateUsesTSOAllocatorForIssuedTimestamps(t *testing.T) {
	t.Parallel()

	staleClock := NewHLC()
	staleClock.SetPhysicalCeiling(time.Now().Add(-time.Millisecond).UnixMilli())
	coord := &Coordinate{
		clock:       staleClock,
		tsAllocator: &fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true},
	}

	startTS, err := coord.nextStartTS(context.Background())
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase, startTS)

	commitTS, err := coord.resolveDispatchCommitTS(context.Background(), 0, startTS)
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+1, commitTS)
}

func TestShardedCoordinatorRawFollowerDefersTSOAllocationToLeaderPath(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), nil, 1)
	txn := &recordingTransactional{
		responses: []*TransactionResponse{{CommitIndex: 3}},
	}
	alloc := &blockingTimestampAllocator{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {
			Engine: &stubFollowerEngine{},
			Txn:    txn,
		},
	}, 1, NewHLC(), nil).WithTSOAllocator(alloc)

	ctx, cancel := context.WithTimeout(context.Background(), testTSOWaitTimeout)
	defer cancel()
	_, err := coord.Dispatch(ctx, &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("b"), Value: []byte("raw")}},
	})
	require.NoError(t, err)
	require.EqualValues(t, 0, alloc.calls.Load())
	require.EqualValues(t, 0, txn.requests[0].Ts)
}

func TestShardedCoordinatorUsesTSOAllocatorForRawTxnAndDelPrefix(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{
		responses: []*TransactionResponse{
			{CommitIndex: 3},
			{CommitIndex: 7},
			{CommitIndex: 11},
		},
	}
	g2Txn := &recordingTransactional{
		responses: []*TransactionResponse{
			{CommitIndex: 5},
		},
	}
	staleClock := NewHLC()
	staleClock.SetPhysicalCeiling(time.Now().Add(-time.Millisecond).UnixMilli())

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, staleClock, nil).WithTSOAllocator(&fakeTSOAllocator{nextBase: testTSOInitialBase, leader: true})

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("b"), Value: []byte("raw")}},
	})
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase, g1Txn.requests[0].Ts)

	_, err = coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{{Op: DelPrefix, Key: []byte("prefix")}},
	})
	require.NoError(t, err)
	require.EqualValues(t, testTSOInitialBase+1, g1Txn.requests[1].Ts)
	require.EqualValues(t, testTSOInitialBase+1, g2Txn.requests[0].Ts)

	_, err = coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn: true,
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("b"), Value: []byte("txn")}},
	})
	require.NoError(t, err)
	txnReq := g1Txn.requests[2]
	require.EqualValues(t, testTSOInitialBase+2, txnReq.Ts)
	require.EqualValues(t, testTSOInitialBase+3, requestTxnMeta(t, txnReq).CommitTS)
}

type fakeTSOAllocator struct {
	nextBase uint64
	calls    atomic.Uint64
	leader   bool
}

type phaseDTestAllocator struct {
	next           uint64
	phaseDActive   bool
	phaseDRequired bool
	validateErr    error
	nextCalls      atomic.Uint64
	validateCalls  atomic.Uint64
	validated      atomic.Uint64
}

func (a *phaseDTestAllocator) Next(context.Context) (uint64, error) {
	a.nextCalls.Add(1)
	return a.next, nil
}

func (a *phaseDTestAllocator) NextAfter(_ context.Context, min uint64) (uint64, error) {
	a.nextCalls.Add(1)
	if a.next <= min {
		return min + 1, nil
	}
	return a.next, nil
}

func (a *phaseDTestAllocator) ValidateDurableTimestamp(_ context.Context, timestamp uint64) error {
	a.validateCalls.Add(1)
	a.validated.Store(timestamp)
	return a.validateErr
}

func (a *phaseDTestAllocator) PhaseDActive() bool   { return a.phaseDActive }
func (a *phaseDTestAllocator) PhaseDRequired() bool { return a.phaseDRequired }

type phaseDWindowTSO struct {
	nextBase uint64
	floor    uint64
	phaseD   bool
	leader   bool
	calls    atomic.Uint64
}

func (a *phaseDWindowTSO) Next(ctx context.Context) (uint64, error) {
	return a.NextBatch(ctx, 1)
}

func (a *phaseDWindowTSO) NextBatch(_ context.Context, n int) (uint64, error) {
	a.calls.Add(1)
	base := a.nextBase
	a.nextBase += uint64(n) //nolint:gosec // positive test batch size.
	return base, nil
}

func (a *phaseDWindowTSO) IsLeader() bool { return a.leader }

func (a *phaseDWindowTSO) RunLeaseRenewal(ctx context.Context) { <-ctx.Done() }

func (a *phaseDWindowTSO) ValidateDurableTimestamp(_ context.Context, timestamp uint64) error {
	if timestamp <= a.floor {
		return ErrTSOTimestampInvalid
	}
	return nil
}

func (a *phaseDWindowTSO) PhaseDActive() bool   { return a.phaseD }
func (a *phaseDWindowTSO) PhaseDRequired() bool { return a.phaseD }

func (f *fakeTSOAllocator) Next(ctx context.Context) (uint64, error) {
	return f.NextBatch(ctx, 1)
}

func (f *fakeTSOAllocator) NextBatch(_ context.Context, n int) (uint64, error) {
	if n <= 0 {
		return 0, errors.WithStack(ErrInvalidTSOBatchSize)
	}
	f.calls.Add(1)
	base := f.nextBase
	f.nextBase += uint64(n) //nolint:gosec // n is validated positive above.
	return base, nil
}

func (f *fakeTSOAllocator) IsLeader() bool { return f.leader }

func (f *fakeTSOAllocator) RunLeaseRenewal(ctx context.Context) {
	<-ctx.Done()
}

type blockingTimestampAllocator struct {
	calls atomic.Uint64
}

func (b *blockingTimestampAllocator) Next(ctx context.Context) (uint64, error) {
	b.calls.Add(1)
	<-ctx.Done()
	return 0, ctx.Err()
}

type fakeTSOCoordinator struct {
	leader atomic.Bool
	clock  *HLC
}

func (f *fakeTSOCoordinator) IsLeader() bool {
	return f.leader.Load()
}

func (f *fakeTSOCoordinator) Clock() *HLC {
	return f.clock
}

type fakeTimestampLeaderCoordinator struct {
	leader          atomic.Bool
	timestampLeader atomic.Bool
	clock           *HLC
}

func (f *fakeTimestampLeaderCoordinator) IsLeader() bool {
	return f.leader.Load()
}

func (f *fakeTimestampLeaderCoordinator) IsTimestampLeader() bool {
	return f.timestampLeader.Load()
}

func (f *fakeTimestampLeaderCoordinator) Clock() *HLC {
	return f.clock
}
