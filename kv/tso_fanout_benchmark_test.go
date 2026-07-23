package kv

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkTSOWriteFanout models 16 concurrent write coordinators sharing one
// process-local BatchAllocator. Each logical operation stamps every fan-out
// leg, while a refill pays a 1 ms remote leader plus Raft commit delay.
func BenchmarkTSOWriteFanout(b *testing.B) {
	const (
		writers = 16
		tsoRTT  = time.Millisecond
	)
	for _, fanout := range []int{1, 3, 8} {
		for _, batchSize := range []int{1, 64, 256} {
			name := fmt.Sprintf("writers=%d/fanout=%d/batch=%d/rtt=1ms", writers, fanout, batchSize)
			b.Run(name, func(b *testing.B) {
				runTSOWriteFanoutBenchmark(b, writers, fanout, batchSize, tsoRTT)
			})
		}
	}
}

func runTSOWriteFanoutBenchmark(b *testing.B, writers, fanout, batchSize int, rtt time.Duration) {
	backend := &benchmarkTSOAllocator{rtt: rtt}
	allocator, err := NewBatchAllocator(backend, batchSize)
	if err != nil {
		b.Fatal(err)
	}
	latencies := make([]int64, b.N)
	var next atomic.Uint64
	var firstErr atomic.Pointer[benchmarkTSOError]
	ctx := context.Background()
	b.ResetTimer()
	started := time.Now()
	var wg sync.WaitGroup
	for range writers {
		wg.Add(1)
		go runTSOWriteFanoutWorker(ctx, allocator, fanout, latencies, &next, &firstErr, &wg)
	}
	wg.Wait()
	elapsed := time.Since(started)
	b.StopTimer()
	reportTSOWriteFanoutMetrics(b, fanout, latencies, backend, elapsed, firstErr.Load())
}

func runTSOWriteFanoutWorker(
	ctx context.Context,
	allocator TimestampAllocator,
	fanout int,
	latencies []int64,
	next *atomic.Uint64,
	firstErr *atomic.Pointer[benchmarkTSOError],
	wg *sync.WaitGroup,
) {
	defer wg.Done()
	for {
		index := next.Add(1) - 1
		if index >= uint64(len(latencies)) { //nolint:gosec // benchmark sizes are non-negative.
			return
		}
		opStarted := time.Now()
		for range fanout {
			if _, err := allocator.Next(ctx); err != nil {
				firstErr.CompareAndSwap(nil, &benchmarkTSOError{err: err})
				return
			}
		}
		latencies[index] = time.Since(opStarted).Nanoseconds()
	}
}

func reportTSOWriteFanoutMetrics(
	b *testing.B,
	fanout int,
	latencies []int64,
	backend *benchmarkTSOAllocator,
	elapsed time.Duration,
	recorded *benchmarkTSOError,
) {
	if recorded != nil {
		b.Fatal(recorded.err)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	b.ReportMetric(percentileDuration(latencies, 50).Seconds()*1000, "p50-ms")
	b.ReportMetric(percentileDuration(latencies, 99).Seconds()*1000, "p99-ms")
	b.ReportMetric(float64(backend.refills.Load())/float64(b.N), "refills/op")
	b.ReportMetric(float64(b.N*fanout)/elapsed.Seconds(), "writes/s")
}

func percentileDuration(sorted []int64, percentile int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := (len(sorted)*percentile + 99) / 100
	if index > 0 {
		index--
	}
	return time.Duration(sorted[index])
}

type benchmarkTSOError struct {
	err error
}

type benchmarkTSOAllocator struct {
	rtt     time.Duration
	next    atomic.Uint64
	refills atomic.Uint64
}

func (a *benchmarkTSOAllocator) Next(ctx context.Context) (uint64, error) {
	return a.NextBatch(ctx, 1)
}

func (a *benchmarkTSOAllocator) NextBatch(ctx context.Context, n int) (uint64, error) {
	timer := time.NewTimer(a.rtt)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
	}
	a.refills.Add(1)
	end := a.next.Add(uint64(n))    //nolint:gosec // benchmark batch sizes are positive.
	return end - uint64(n) + 1, nil //nolint:gosec // benchmark batch sizes are positive.
}

func (a *benchmarkTSOAllocator) IsLeader() bool { return false }

func (a *benchmarkTSOAllocator) RunLeaseRenewal(ctx context.Context) {
	<-ctx.Done()
}
