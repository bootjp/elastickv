package etcd

import "testing"

// Measures actor/admission overhead only, not disk or FSM throughput.
func BenchmarkStorageWorkerRoundTrip(b *testing.B) {
	worker, err := newStorageWorker(1)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		worker.quiesce()
		for completion := range worker.completions {
			completion.Release()
		}
		<-worker.done
	})
	budget := newStorageWorkBudget(1, 1)
	work := storageWork{run: func() (any, error) { return nil, nil }}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if submitErr := worker.trySubmit(work, budget); submitErr != nil {
			b.Fatal(submitErr)
		}
		completion := <-worker.completions
		completion.Release()
		if completion.err != nil {
			b.Fatal(completion.err)
		}
	}
}
