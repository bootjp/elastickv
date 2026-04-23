package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

// BenchmarkApplyMutations_SyncMode measures per-op latency and
// throughput of the FSM commit path under each
// ELASTICKV_FSM_SYNC_MODE value. The benchmark is write-heavy and
// serial: each iteration issues one ApplyMutations on a fresh (key,
// commitTS) pair with a single Put mutation, exercising the single-
// fsync hot path.
//
// Run with:
//
//	go test ./store -run='^$' -bench='BenchmarkApplyMutations_SyncMode' -benchtime=2s -benchmem
//
// The sync/nosync ratio (not absolute numbers, which are disk-
// dependent) is the signal of interest. On a laptop SSD, nosync
// typically runs 10-50x faster per op; the exact multiplier reflects
// how cheap the platform's fsync is on a freshly-created WAL file.
func BenchmarkApplyMutations_SyncMode(b *testing.B) {
	cases := []struct {
		name string
		opts *pebble.WriteOptions
	}{
		{name: "sync", opts: pebble.Sync},
		{name: "nosync", opts: pebble.NoSync},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			setBenchFSMApplyWriteOpts(b, tc.opts)

			dir := b.TempDir()
			s, err := NewPebbleStore(dir)
			if err != nil {
				b.Fatalf("NewPebbleStore: %v", err)
			}
			defer s.Close()

			ctx := context.Background()
			val := make([]byte, 64)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := []byte(fmt.Sprintf("bench-%010d", i))
				muts := []*KVPairMutation{{Op: OpTypePut, Key: key, Value: val}}
				// startTS must be strictly < commitTS and distinct across
				// iterations to avoid MVCC write-conflict. Guard the
				// int -> uint64 conversion to satisfy gosec G115; i is
				// non-negative here by construction (loop from 0) but
				// the linter cannot prove it.
				if i < 0 {
					b.Fatalf("unexpected negative iteration counter: %d", i)
				}
				startTS := uint64(i) * 2
				commitTS := startTS + 1
				if err := s.ApplyMutations(ctx, muts, nil, startTS, commitTS); err != nil {
					b.Fatalf("ApplyMutations: %v", err)
				}
			}
		})
	}
}

// setBenchFSMApplyWriteOpts is the benchmark counterpart to
// setFSMApplyWriteOptsForTest. It accepts *testing.B (which does not
// satisfy the testing.TB-only t.Helper() pattern used by the test
// helper) so the benchmark can swap the package-level write options
// around each sub-run.
func setBenchFSMApplyWriteOpts(b *testing.B, opts *pebble.WriteOptions) {
	b.Helper()
	prev := fsmApplyWriteOpts
	fsmApplyWriteOpts = opts
	b.Cleanup(func() { fsmApplyWriteOpts = prev })
}
