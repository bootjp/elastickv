package kv

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

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
