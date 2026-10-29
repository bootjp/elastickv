package etcd

import (
	"errors"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireStorageWorkUsage(t *testing.T, budget *storageWorkBudget, bytes uint64, items int) {
	t.Helper()
	usedBytes, usedItems := budget.usage()
	require.Equal(t, bytes, usedBytes)
	require.Equal(t, items, usedItems)
}

func TestStorageWorkBudgetExactCaps(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		maxBytes uint64
		maxItems int
		work     []uint64
		rejected uint64
	}{
		{name: "bytes", maxBytes: 10, maxItems: 3, work: []uint64{4, 6}, rejected: 1},
		{name: "items", maxBytes: 10, maxItems: 2, work: []uint64{1, 2}, rejected: 0},
		{name: "both", maxBytes: 10, maxItems: 2, work: []uint64{4, 6}, rejected: 1},
		{name: "zero byte work", maxBytes: 10, maxItems: 2, work: []uint64{0, 0}, rejected: 0},
		{name: "zero bytes at byte cap", maxBytes: 10, maxItems: 2, work: []uint64{10, 0}, rejected: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			budget := newStorageWorkBudget(test.maxBytes, test.maxItems)
			var total uint64
			permits := make([]*storageWorkPermit, 0, len(test.work))
			for index, bytes := range test.work {
				permit, err := budget.tryAcquire(bytes)
				require.NoError(t, err)
				require.NotNil(t, permit)
				t.Cleanup(permit.Release)
				permits = append(permits, permit)
				total += bytes
				requireStorageWorkUsage(t, budget, total, index+1)
			}
			for range 3 {
				permit, err := budget.tryAcquire(test.rejected)
				require.ErrorIs(t, err, errStorageWorkBackpressure)
				require.Nil(t, permit)
				requireStorageWorkUsage(t, budget, total, len(test.work))
			}
			for _, permit := range permits {
				permit.Release()
			}
			requireStorageWorkUsage(t, budget, 0, 0)
			permit, err := budget.tryAcquire(test.maxBytes)
			require.NoError(t, err)
			t.Cleanup(permit.Release)
			requireStorageWorkUsage(t, budget, test.maxBytes, 1)
		})
	}
}

func TestStorageWorkBudgetInvalidReservations(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		budget *storageWorkBudget
	}{
		{name: "nil"},
		{name: "zero value", budget: &storageWorkBudget{}},
		{name: "zero bytes", budget: newStorageWorkBudget(0, 1)},
		{name: "zero items", budget: newStorageWorkBudget(1, 0)},
		{name: "negative items", budget: newStorageWorkBudget(1, -1)},
		{name: "minimum items", budget: newStorageWorkBudget(1, math.MinInt)},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			for _, bytes := range []uint64{0, 1, math.MaxUint64} {
				permit, err := test.budget.tryAcquire(bytes)
				require.ErrorIs(t, err, errStorageWorkInvalidReservation)
				require.NotErrorIs(t, err, errStorageWorkBackpressure)
				require.Nil(t, permit)
				requireStorageWorkUsage(t, test.budget, 0, 0)
			}
		})
	}
	budget := newStorageWorkBudget(10, 1)
	for _, existing := range []uint64{0, 10} {
		if existing != 0 {
			permit, err := budget.tryAcquire(existing)
			require.NoError(t, err)
			t.Cleanup(permit.Release)
		}
		for _, bytes := range []uint64{11, math.MaxUint64} {
			permit, err := budget.tryAcquire(bytes)
			require.ErrorIs(t, err, errStorageWorkInvalidReservation)
			require.NotErrorIs(t, err, errStorageWorkBackpressure)
			require.Nil(t, permit)
		}
	}
	requireStorageWorkUsage(t, budget, 10, 1)
}

func TestStorageWorkBudgetUint64Boundary(t *testing.T) {
	t.Parallel()
	budget := newStorageWorkBudget(math.MaxUint64, math.MaxInt)
	large, err := budget.tryAcquire(math.MaxUint64 - 1)
	require.NoError(t, err)
	t.Cleanup(large.Release)
	for _, bytes := range []uint64{2, math.MaxUint64} {
		permit, acquireErr := budget.tryAcquire(bytes)
		require.ErrorIs(t, acquireErr, errStorageWorkBackpressure)
		require.Nil(t, permit)
		requireStorageWorkUsage(t, budget, math.MaxUint64-1, 1)
	}
	last, err := budget.tryAcquire(1)
	require.NoError(t, err)
	t.Cleanup(last.Release)
	zero, err := budget.tryAcquire(0)
	require.NoError(t, err)
	t.Cleanup(zero.Release)
	requireStorageWorkUsage(t, budget, math.MaxUint64, 3)
	permit, err := budget.tryAcquire(1)
	require.ErrorIs(t, err, errStorageWorkBackpressure)
	require.Nil(t, permit)
	large.Release()
	requireStorageWorkUsage(t, budget, 1, 2)
	last.Release()
	zero.Release()
	requireStorageWorkUsage(t, budget, 0, 0)
	full, err := budget.tryAcquire(math.MaxUint64)
	require.NoError(t, err)
	t.Cleanup(full.Release)
	requireStorageWorkUsage(t, budget, math.MaxUint64, 1)
	full.Release()
	requireStorageWorkUsage(t, budget, 0, 0)
}

func TestStorageWorkPermitRelease(t *testing.T) {
	t.Parallel()
	var nilPermit *storageWorkPermit
	nilPermit.Release()
	new(storageWorkPermit).Release()
	budget := newStorageWorkBudget(10, 2)
	for _, bytes := range []uint64{0, 10} {
		permit, err := budget.tryAcquire(bytes)
		require.NoError(t, err)
		t.Cleanup(permit.Release)
		var workers sync.WaitGroup
		for range 32 {
			workers.Go(func() {
				permit.Release()
				permit.Release()
				budget.usage()
			})
		}
		workers.Wait()
		requireStorageWorkUsage(t, budget, 0, 0)
		replacement, acquireErr := budget.tryAcquire(10)
		require.NoError(t, acquireErr)
		t.Cleanup(replacement.Release)
		permit.Release()
		requireStorageWorkUsage(t, budget, 10, 1)
		replacement.Release()
	}
	requireStorageWorkUsage(t, budget, 0, 0)
}

func TestStorageWorkBudgetConcurrentCaps(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		maxBytes uint64
		maxItems int
		bytes    uint64
		want     int
	}{
		{name: "bytes", maxBytes: 8, maxItems: 32, bytes: 2, want: 4},
		{name: "items", maxBytes: 64, maxItems: 4, bytes: 2, want: 4},
		{name: "zero bytes", maxBytes: 64, maxItems: 4, bytes: 0, want: 4},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			budget := newStorageWorkBudget(test.maxBytes, test.maxItems)
			const contenders = 32
			results := make(chan *storageWorkPermit, contenders)
			start := make(chan struct{})
			var workers sync.WaitGroup
			for range contenders {
				workers.Go(func() {
					<-start
					permit, err := budget.tryAcquire(test.bytes)
					if err != nil && !errors.Is(err, errStorageWorkBackpressure) {
						t.Errorf("unexpected reservation error: %v", err)
					}
					results <- permit
				})
			}
			close(start)
			workers.Wait()
			close(results)
			var permits []*storageWorkPermit
			for permit := range results {
				if permit != nil {
					t.Cleanup(permit.Release)
					permits = append(permits, permit)
				}
			}
			require.Len(t, permits, test.want)
			requireStorageWorkUsage(t, budget, test.bytes*uint64(test.want), test.want)
			for _, permit := range permits {
				workers.Go(func() { permit.Release() })
				workers.Go(func() { permit.Release() })
			}
			workers.Wait()
			requireStorageWorkUsage(t, budget, 0, 0)
		})
	}
}

func TestStorageWorkBudgetConcurrentReservationsAndReleases(t *testing.T) {
	t.Parallel()
	const maxBytes, maxItems = 8, 4
	budget := newStorageWorkBudget(maxBytes, maxItems)
	var workers sync.WaitGroup
	for worker := range 32 {
		workers.Go(func() {
			for iteration := range 100 {
				bytes := uint64((worker + iteration) % 4)
				permit, err := budget.tryAcquire(bytes)
				if err != nil && !errors.Is(err, errStorageWorkBackpressure) {
					t.Errorf("unexpected reservation error: %v", err)
				}
				usedBytes, usedItems := budget.usage()
				if usedBytes > maxBytes || usedItems < 0 || usedItems > maxItems {
					t.Errorf("usage exceeds caps: (%d, %d)", usedBytes, usedItems)
				}
				permit.Release()
				permit.Release()
			}
		})
	}
	workers.Wait()
	requireStorageWorkUsage(t, budget, 0, 0)
}
