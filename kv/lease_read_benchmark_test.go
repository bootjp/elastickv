package kv

import (
	"context"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/monoclock"
)

// BenchmarkLeaseRead measures the Coordinate.LeaseRead fast path, where
// the engine-driven lease anchor (LastQuorumAck + State==Leader) is
// fresh so the read is served from the cached AppliedIndex without a
// LinearizableRead round-trip. The LeaseProvider capability is resolved
// once at construction (NewCoordinatorWithEngine) and cached on
// Coordinate.lp, so this hot loop performs a single nil check rather
// than an interface type assertion per call.
func BenchmarkLeaseRead(b *testing.B) {
	eng := &fakeLeaseEngine{applied: 4242, leaseDur: time.Hour}
	eng.setQuorumAck(monoclock.Now())
	c := NewCoordinatorWithEngine(nil, eng)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := c.LeaseRead(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	// Guard the benchmark against silently exercising the slow path: a
	// single LinearizableRead would invalidate the "assertion is off the
	// hot path" claim because the slow path dominates the measurement.
	if got := eng.linearizableCalls.Load(); got != 0 {
		b.Fatalf("expected the lease fast path on every iteration, but LinearizableRead ran %d times", got)
	}
}

// BenchmarkGroupLeaseRead measures the sharded groupLeaseRead fast path
// (via ShardedCoordinator.LeaseRead on the default group). The
// LeaseProvider capability is resolved once per group in
// NewShardedCoordinator and cached on ShardGroup.lp, so the hot loop is
// a single nil check rather than a per-call interface type assertion.
func BenchmarkGroupLeaseRead(b *testing.B) {
	eng := newShardedLeaseEngine(7777)
	eng.setQuorumAck(monoclock.Now())

	distEngine := distribution.NewEngine()
	distEngine.UpdateRoute([]byte("a"), nil, 1)
	coord := NewShardedCoordinator(distEngine, map[uint64]*ShardGroup{
		1: {Engine: eng, Txn: &recordingTransactional{}},
	}, 1, NewHLC(), nil)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := coord.LeaseRead(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if got := eng.linearizableCalls.Load(); got != 0 {
		b.Fatalf("expected the lease fast path on every iteration, but LinearizableRead ran %d times", got)
	}
}
