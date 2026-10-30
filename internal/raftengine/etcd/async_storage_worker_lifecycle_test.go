package etcd

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const storageLifecycleTimeout = 5 * time.Second

type storageLifecycleHarness struct {
	worker *storageWorker
	gates  []func()
	actors []<-chan struct{}
}

func newStorageLifecycleHarness(t *testing.T, capacity int) *storageLifecycleHarness {
	t.Helper()
	worker, err := newStorageWorker(capacity)
	require.NoError(t, err)
	h := &storageLifecycleHarness{worker: worker}
	t.Cleanup(func() {
		// Release every barrier before joining producers or draining the worker.
		for _, release := range h.gates {
			release()
		}
		h.spawn(worker.quiesce)
		h.join(t)
		for _, completion := range storageLifecycleDrain(t, worker) {
			completion.Release()
		}
		storageLifecycleReceive(t, worker.done)
	})
	return h
}

func (h *storageLifecycleHarness) gate() (<-chan struct{}, func()) {
	gate := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(gate) }) }
	h.gates = append(h.gates, release)
	return gate, release
}

func (h *storageLifecycleHarness) spawn(run func()) {
	done := make(chan struct{})
	h.actors = append(h.actors, done)
	go func() {
		defer close(done)
		run()
	}()
}

func (h *storageLifecycleHarness) join(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(storageLifecycleTimeout)
	defer timer.Stop()
	for _, done := range h.actors {
		select {
		case <-done:
		case <-timer.C:
			t.Fatal("storage lifecycle actor did not finish")
		}
	}
}

func storageLifecycleReceive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	timer := time.NewTimer(storageLifecycleTimeout)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatal("storage lifecycle barrier timed out")
		var zero T
		return zero
	}
}

func storageLifecycleCompletion(t *testing.T, worker *storageWorker) storageWorkCompletion {
	t.Helper()
	timer := time.NewTimer(storageLifecycleTimeout)
	defer timer.Stop()
	select {
	case completion, ok := <-worker.completions:
		require.True(t, ok, "completion stream closed early")
		t.Cleanup(completion.Release)
		return completion
	case <-timer.C:
		t.Fatal("storage lifecycle completion timed out")
		return storageWorkCompletion{}
	}
}

func storageLifecycleDrain(t *testing.T, worker *storageWorker) []storageWorkCompletion {
	t.Helper()
	timer := time.NewTimer(storageLifecycleTimeout)
	defer timer.Stop()
	var completions []storageWorkCompletion
	for {
		select {
		case completion, ok := <-worker.completions:
			if !ok {
				return completions
			}
			t.Cleanup(completion.Release)
			completions = append(completions, completion)
		case <-timer.C:
			t.Fatal("storage lifecycle completion stream did not close")
			return nil
		}
	}
}

func (h *storageLifecycleHarness) submit(t *testing.T, work storageWork, budget *storageWorkBudget) error {
	t.Helper()
	result := make(chan error, 1)
	h.spawn(func() { result <- h.worker.trySubmit(work, budget) })
	return storageLifecycleReceive(t, result)
}

func requireStorageLifecycleUsage(t *testing.T, budget *storageWorkBudget, bytes uint64, items int) {
	t.Helper()
	usedBytes, usedItems := budget.usage()
	require.Equal(t, bytes, usedBytes)
	require.Equal(t, items, usedItems)
}

type storageLifecycleSubmission struct {
	token int
	err   error
}

func (h *storageLifecycleHarness) raceSubmissions(
	t *testing.T, work func(int) storageWork, budget *storageWorkBudget,
	submitters int, quiesce bool, releaseHead func(),
) <-chan storageLifecycleSubmission {
	t.Helper()
	start, releaseStart := h.gate()
	results := make(chan storageLifecycleSubmission, submitters)
	racers := submitters + 1
	if quiesce {
		racers += 8
	}
	ready := make(chan struct{}, racers)
	for i := range submitters {
		token := i + 2
		h.spawn(func() {
			ready <- struct{}{}
			<-start
			results <- storageLifecycleSubmission{token: token, err: h.worker.trySubmit(work(token), budget)}
		})
	}
	if quiesce {
		for range 8 {
			h.spawn(func() {
				ready <- struct{}{}
				<-start
				h.worker.quiesce()
				h.worker.quiesce()
			})
		}
	}
	h.spawn(func() {
		ready <- struct{}{}
		<-start
		releaseHead()
	})
	for range racers {
		storageLifecycleReceive(t, ready)
	}
	releaseStart()
	h.join(t)
	return results
}

func requireStorageLifecycleAcceptedCompletions(
	t *testing.T, completions []storageWorkCompletion, accepted map[int]bool, fatal bool, failure error,
) {
	t.Helper()
	require.Len(t, completions, len(accepted))
	seen := make(map[int]bool)
	for _, completion := range completions {
		token, ok := completion.token.(int)
		require.True(t, ok)
		require.True(t, accepted[token], "completion for rejected token %d", token)
		require.False(t, seen[token], "duplicate completion for token %d", token)
		seen[token] = true
		if fatal {
			require.ErrorIs(t, completion.err, failure)
			require.Nil(t, completion.value)
		} else {
			require.NoError(t, completion.err)
			require.Equal(t, token, completion.value)
		}
	}
	require.Equal(t, accepted, seen)
}

func runStorageLifecycleFailure(mode string, failure error) (any, error) {
	switch mode {
	case "panic":
		panic("failure after successful prefix")
	case "panic_nil":
		panic(nil)
	default:
		return "partial result must not escape", failure
	}
}

func TestStorageWorkerLifecycleCompletionSaturation(t *testing.T) {
	t.Parallel()
	h := newStorageLifecycleHarness(t, 1)
	budget := newStorageWorkBudget(10, 10)
	firstGate, releaseFirst := h.gate()
	secondGate, releaseSecond := h.gate()
	firstEntered := make(chan struct{})
	secondEntered := make(chan struct{})
	secondReturned := make(chan struct{})
	thirdEntered := make(chan struct{})
	require.NoError(t, h.submit(t, storageWork{token: 0, retainedBytes: 1, run: func() (any, error) {
		close(firstEntered)
		<-firstGate
		return 0, nil
	}}, budget))
	storageLifecycleReceive(t, firstEntered)
	require.NoError(t, h.submit(t, storageWork{token: 1, retainedBytes: 1, run: func() (any, error) {
		close(secondEntered)
		<-secondGate
		close(secondReturned)
		return 1, nil
	}}, budget))
	releaseFirst()
	storageLifecycleReceive(t, secondEntered)
	require.Equal(t, cap(h.worker.completions), len(h.worker.completions))
	releaseSecond()
	storageLifecycleReceive(t, secondReturned)
	// No consumer has received token 0, so token 1 cannot be delivered yet.
	require.NoError(t, h.submit(t, storageWork{token: 2, retainedBytes: 1, run: func() (any, error) {
		close(thirdEntered)
		return 2, nil
	}}, budget))
	require.ErrorIs(t, h.submit(t, storageWork{token: 3, retainedBytes: 1, run: func() (any, error) {
		return 3, nil
	}}, budget), errStorageWorkBackpressure)
	h.spawn(h.worker.quiesce)
	h.join(t)
	select {
	case <-thirdEntered:
		t.Fatal("worker ran past an undeliverable completion")
	case <-h.worker.done:
		t.Fatal("worker finished before the consumer resumed")
	default:
	}
	requireStorageLifecycleUsage(t, budget, 3, 3)
	completions := storageLifecycleDrain(t, h.worker)
	storageLifecycleReceive(t, h.worker.done)
	require.Len(t, completions, 3)
	for i, completion := range completions {
		require.Equal(t, i, completion.token)
		require.Equal(t, i, completion.value)
		require.NoError(t, completion.err)
	}
	requireStorageLifecycleUsage(t, budget, 3, 3)
	for _, completion := range completions {
		completion.Release()
	}
	requireStorageLifecycleUsage(t, budget, 0, 0)
}

func TestStorageWorkerLifecycleConcurrentSubmitQuiesceFatal(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		quiesce bool
		fatal   bool
	}{
		{name: "quiesce", quiesce: true},
		{name: "fatal", fatal: true},
		{name: "quiesce_and_fatal", quiesce: true, fatal: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			const submitters = 32
			h := newStorageLifecycleHarness(t, submitters+2)
			budget := newStorageWorkBudget(submitters+2, submitters+2)
			headGate, releaseHead := h.gate()
			entered := make(chan struct{})
			failure := errors.New("lifecycle disk failure")
			var calls atomic.Int32
			require.NoError(t, h.submit(t, storageWork{token: 0, retainedBytes: 1, run: func() (any, error) {
				calls.Add(1)
				close(entered)
				<-headGate
				if test.fatal {
					return nil, failure
				}
				return 0, nil
			}}, budget))
			storageLifecycleReceive(t, entered)
			work := func(token int) storageWork {
				return storageWork{token: token, retainedBytes: 1, run: func() (any, error) {
					calls.Add(1)
					return token, nil
				}}
			}
			require.NoError(t, h.submit(t, work(1), budget))
			results := h.raceSubmissions(t, work, budget, submitters, test.quiesce, releaseHead)
			accepted := map[int]bool{0: true, 1: true}
			for range submitters {
				result := storageLifecycleReceive(t, results)
				if result.err == nil {
					accepted[result.token] = true
					continue
				}
				require.True(t, errors.Is(result.err, errStorageWorkerClosed) ||
					(test.fatal && errors.Is(result.err, failure)), "unexpected rejection: %v", result.err)
			}
			requireStorageLifecycleUsage(t, budget, uint64(len(accepted)), len(accepted))
			completions := storageLifecycleDrain(t, h.worker)
			storageLifecycleReceive(t, h.worker.done)
			requireStorageLifecycleAcceptedCompletions(t, completions, accepted, test.fatal, failure)
			if test.fatal {
				require.Equal(t, int32(1), calls.Load(), "queued work ran after fatal prefix")
				require.ErrorIs(t, h.submit(t, work(submitters+2), budget), failure)
			} else {
				require.Equal(t, int32(len(accepted)), calls.Load())
				require.ErrorIs(t, h.submit(t, work(submitters+2), budget), errStorageWorkerClosed)
			}
			requireStorageLifecycleUsage(t, budget, uint64(len(accepted)), len(accepted))
			for _, completion := range completions {
				completion.Release()
				completion.Release()
			}
			requireStorageLifecycleUsage(t, budget, 0, 0)
		})
	}
}

func TestStorageWorkerLifecycleFailureAfterSuccessfulPrefix(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"partial_value_error", "panic", "panic_nil"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			h := newStorageLifecycleHarness(t, 4)
			budget := newStorageWorkBudget(5, 5)
			gate, release := h.gate()
			entered := make(chan struct{})
			failure := errors.New("failure after successful prefix")
			var calls atomic.Int32
			for token := range 5 {
				require.NoError(t, h.submit(t, storageWork{token: token, retainedBytes: 1, run: func() (any, error) {
					calls.Add(1)
					if token != 2 {
						return token + 10, nil
					}
					close(entered)
					<-gate
					return runStorageLifecycleFailure(mode, failure)
				}}, budget))
				if token == 2 {
					storageLifecycleReceive(t, entered)
				}
			}
			var completions []storageWorkCompletion
			for token := range 2 {
				completion := storageLifecycleCompletion(t, h.worker)
				require.Equal(t, token, completion.token)
				require.Equal(t, token+10, completion.value)
				require.NoError(t, completion.err)
				completions = append(completions, completion)
			}
			requireStorageLifecycleUsage(t, budget, 5, 5)
			release()
			failed := storageLifecycleDrain(t, h.worker)
			storageLifecycleReceive(t, h.worker.done)
			require.Len(t, failed, 3)
			want := failure
			if mode != "partial_value_error" {
				want = errStorageWorkerPanic
			}
			for i, completion := range failed {
				require.Equal(t, i+2, completion.token)
				require.ErrorIs(t, completion.err, want)
				require.Same(t, failed[0].err, completion.err, "later tasks must preserve the first failure")
				require.Nil(t, completion.value)
			}
			require.Equal(t, int32(3), calls.Load())
			requireStorageLifecycleUsage(t, budget, 5, 5)
			completions = append(completions, failed...)
			for _, completion := range completions {
				completion.Release()
			}
			requireStorageLifecycleUsage(t, budget, 0, 0)
		})
	}
}

func TestStorageWorkerLifecycleIndependentBudgetsFIFO(t *testing.T) {
	t.Parallel()
	h := newStorageLifecycleHarness(t, 6)
	bulk := newStorageWorkBudget(4, 2)
	control := newStorageWorkBudget(3, 3)
	gate, release := h.gate()
	entered := make(chan struct{})
	work := func(token int, bytes uint64) storageWork {
		return storageWork{token: token, retainedBytes: bytes, run: func() (any, error) {
			if token == 0 {
				close(entered)
				<-gate
			}
			return token, nil
		}}
	}
	require.NoError(t, h.submit(t, work(0, 2), bulk))
	storageLifecycleReceive(t, entered)
	require.NoError(t, h.submit(t, work(1, 1), control))
	require.NoError(t, h.submit(t, work(2, 2), bulk))
	require.ErrorIs(t, h.submit(t, work(90, 2), bulk), errStorageWorkBackpressure)
	requireStorageLifecycleUsage(t, bulk, 4, 2)
	requireStorageLifecycleUsage(t, control, 1, 1)
	require.NoError(t, h.submit(t, work(3, 1), control))
	require.NoError(t, h.submit(t, work(4, 1), control))
	require.ErrorIs(t, h.submit(t, work(91, 1), control), errStorageWorkBackpressure)
	requireStorageLifecycleUsage(t, bulk, 4, 2)
	requireStorageLifecycleUsage(t, control, 3, 3)
	h.spawn(h.worker.quiesce)
	h.join(t)
	release()
	completions := storageLifecycleDrain(t, h.worker)
	storageLifecycleReceive(t, h.worker.done)
	require.Len(t, completions, 5)
	for i, completion := range completions {
		require.Equal(t, i, completion.token)
		require.Equal(t, i, completion.value)
		require.NoError(t, completion.err)
	}
	requireStorageLifecycleUsage(t, bulk, 4, 2)
	requireStorageLifecycleUsage(t, control, 3, 3)
	completions[0].Release()
	completions[2].Release()
	requireStorageLifecycleUsage(t, bulk, 0, 0)
	requireStorageLifecycleUsage(t, control, 3, 3)
	for _, i := range []int{1, 3, 4} {
		completions[i].Release()
	}
	requireStorageLifecycleUsage(t, bulk, 0, 0)
	requireStorageLifecycleUsage(t, control, 0, 0)
}
