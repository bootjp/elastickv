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
