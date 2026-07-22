package kv

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
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

func TestLocalTSOAllocatorRenewsExpiredCeilingBeforeIssuing(t *testing.T) {
	coord := &renewingTSOCoordinator{clock: NewHLC()}
	coord.leader.Store(true)
	coord.clock.SetPhysicalCeiling(time.Now().Add(-time.Millisecond).UnixMilli())
	alloc, err := NewLocalTSOAllocator(coord, WithTSOLeaderPollInterval(testTSOPollInterval))
	require.NoError(t, err)

	got, err := alloc.Next(context.Background())
	require.NoError(t, err)
	require.NotZero(t, got)
	require.EqualValues(t, 1, coord.proposeCalls.Load())
	require.Greater(t, coord.clock.PhysicalCeiling(), time.Now().UnixMilli())
}

func TestLocalTSOAllocatorKeepsFailClosedWhenRenewalFails(t *testing.T) {
	sentinel := errors.New("renewal rejected")
	coord := &renewingTSOCoordinator{clock: NewHLC(), proposeErr: sentinel}
	coord.leader.Store(true)
	coord.clock.SetPhysicalCeiling(time.Now().Add(-time.Millisecond).UnixMilli())
	alloc, err := NewLocalTSOAllocator(coord, WithTSOLeaderPollInterval(testTSOPollInterval))
	require.NoError(t, err)

	_, err = alloc.Next(context.Background())
	require.ErrorIs(t, err, sentinel)
	require.EqualValues(t, 1, coord.proposeCalls.Load())
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

type renewingTSOCoordinator struct {
	leader       atomic.Bool
	clock        *HLC
	proposeErr   error
	proposeCalls atomic.Uint64
}

func (f *renewingTSOCoordinator) IsLeader() bool {
	return f.leader.Load()
}

func (f *renewingTSOCoordinator) Clock() *HLC {
	return f.clock
}

func (f *renewingTSOCoordinator) ProposeHLCLease(_ context.Context, ceilingMs int64) error {
	f.proposeCalls.Add(1)
	if f.proposeErr != nil {
		return f.proposeErr
	}
	f.clock.SetPhysicalCeiling(ceilingMs)
	return nil
}
