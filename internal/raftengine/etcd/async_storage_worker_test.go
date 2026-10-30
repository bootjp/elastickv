package etcd

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func testStorageWorker(t *testing.T, capacity int) *storageWorker {
	t.Helper()
	worker, err := newStorageWorker(capacity)
	require.NoError(t, err)
	t.Cleanup(func() {
		worker.quiesce()
		for completion := range worker.completions {
			completion.Release()
		}
		<-worker.done
	})
	return worker
}

func awaitStorageSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("storage worker did not reach barrier")
	}
}

func TestStorageWorkerFIFOAndQuiesce(t *testing.T) {
	t.Parallel()
	worker := testStorageWorker(t, 3)
	budget := newStorageWorkBudget(9, 3)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var order []int
	for i := range 3 {
		work := storageWork{token: i, retainedBytes: 3, run: func() (any, error) {
			if i == 0 {
				close(entered)
				<-release
			}
			order = append(order, i)
			return i + 10, nil
		}}
		require.NoError(t, worker.trySubmit(work, budget))
		if i == 0 {
			awaitStorageSignal(t, entered)
		}
	}
	worker.quiesce()
	worker.quiesce()
	require.ErrorIs(t, worker.trySubmit(storageWork{run: func() (any, error) { return nil, nil }}, budget), errStorageWorkerClosed)
	select {
	case <-worker.done:
		t.Fatal("quiesce must not invent an executing Save completion")
	case <-worker.completions:
		t.Fatal("blocked work cannot be acknowledged")
	default:
	}
	usedBytes, usedItems := budget.usage()
	require.Equal(t, uint64(9), usedBytes)
	require.Equal(t, 3, usedItems)
	unblock()
	var completions []storageWorkCompletion
	for completion := range worker.completions {
		require.NoError(t, completion.err)
		require.Equal(t, len(completions), completion.token)
		require.Equal(t, len(completions)+10, completion.value)
		completions = append(completions, completion)
	}
	awaitStorageSignal(t, worker.done)
	require.Equal(t, []int{0, 1, 2}, order)
	usedBytes, usedItems = budget.usage()
	require.Equal(t, uint64(9), usedBytes, "completion delivery does not retire ownership")
	require.Equal(t, 3, usedItems)
	for _, completion := range completions {
		completion.Release()
		completion.Release()
	}
	usedBytes, usedItems = budget.usage()
	require.Zero(t, usedBytes)
	require.Zero(t, usedItems)
}

func TestStorageWorkerFatalPrefix(t *testing.T) {
	t.Parallel()
	for _, panics := range []bool{false, true} {
		name := "error"
		if panics {
			name = "panic"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			worker := testStorageWorker(t, 3)
			budget := newStorageWorkBudget(3, 3)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			failure := errors.New("disk write failed")
			var calls atomic.Int32
			require.NoError(t, worker.trySubmit(storageWork{token: 0, retainedBytes: 1, run: func() (any, error) {
				calls.Add(1)
				close(entered)
				<-release
				if panics {
					panic("disk panic")
				}
				return nil, failure
			}}, budget))
			awaitStorageSignal(t, entered)
			for i := 1; i < 3; i++ {
				require.NoError(t, worker.trySubmit(storageWork{token: i, retainedBytes: 1, run: func() (any, error) {
					calls.Add(1)
					return "must not run", nil
				}}, budget))
			}
			unblock()
			want := failure
			if panics {
				want = errStorageWorkerPanic
			}
			count := 0
			for completion := range worker.completions {
				require.Equal(t, count, completion.token)
				require.ErrorIs(t, completion.err, want)
				require.Nil(t, completion.value)
				completion.Release()
				count++
			}
			awaitStorageSignal(t, worker.done)
			require.Equal(t, 3, count)
			require.Equal(t, int32(1), calls.Load(), "a failed disk prefix fences all later operations")
			require.ErrorIs(t, worker.trySubmit(storageWork{run: func() (any, error) { return nil, nil }}, budget), want)
			usedBytes, usedItems := budget.usage()
			require.Zero(t, usedBytes)
			require.Zero(t, usedItems)
		})
	}
}

func TestStorageWorkerQueueRejectionRetainsCallerOwnership(t *testing.T) {
	t.Parallel()
	worker := testStorageWorker(t, 1)
	budget := newStorageWorkBudget(10, 10)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	require.NoError(t, worker.trySubmit(storageWork{retainedBytes: 1, run: func() (any, error) {
		close(entered)
		<-release
		return nil, nil
	}}, budget))
	awaitStorageSignal(t, entered)
	work := storageWork{retainedBytes: 1, run: func() (any, error) { return nil, nil }}
	require.NoError(t, worker.trySubmit(work, budget))
	require.ErrorIs(t, worker.trySubmit(work, budget), errStorageWorkBackpressure)
	usedBytes, usedItems := budget.usage()
	require.Equal(t, uint64(2), usedBytes, "rejected queue insertion rolls back only its own reservation")
	require.Equal(t, 2, usedItems)
	unblock()
}

func TestStorageWorkerRetirementControlsAdmission(t *testing.T) {
	t.Parallel()
	worker := testStorageWorker(t, 1)
	budget := newStorageWorkBudget(1, 1)
	work := storageWork{token: "first", retainedBytes: 1, run: func() (any, error) { return nil, nil }}
	require.NoError(t, worker.trySubmit(work, budget))
	completion := <-worker.completions
	t.Cleanup(completion.Release)
	require.ErrorIs(t, worker.trySubmit(work, budget), errStorageWorkBackpressure)
	completion.Release()
	require.NoError(t, worker.trySubmit(work, budget))
}

func TestStorageWorkerInvalidAdmission(t *testing.T) {
	t.Parallel()
	_, err := newStorageWorker(0)
	require.ErrorIs(t, err, errInvalidStorageWork)
	_, err = newStorageWorker(-1)
	require.ErrorIs(t, err, errInvalidStorageWork)
	worker := testStorageWorker(t, 1)
	budget := newStorageWorkBudget(1, 1)
	require.ErrorIs(t, worker.trySubmit(storageWork{}, budget), errInvalidStorageWork)
	work := storageWork{run: func() (any, error) { return nil, nil }}
	require.ErrorIs(t, worker.trySubmit(work, nil), errStorageWorkInvalidReservation)
	var absent *storageWorker
	require.ErrorIs(t, absent.trySubmit(work, budget), errInvalidStorageWork)
	usedBytes, usedItems := budget.usage()
	require.Zero(t, usedBytes)
	require.Zero(t, usedItems)
}
