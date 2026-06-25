package store

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

// BenchmarkApplyMutationsRaft_SyncMode measures per-op latency and
// throughput of the raft-apply FSM commit path under each
// ELASTICKV_FSM_SYNC_MODE value. The benchmark is write-heavy and
// serial: each iteration issues one ApplyMutationsRaft on a fresh (key,
// commitTS) pair with a single Put mutation, exercising the single-
// fsync hot path.
//
// Run with:
//
//	go test ./store -run='^$' -bench='BenchmarkApplyMutationsRaft_SyncMode' -benchtime=2s -benchmem
//
// The sync/nosync ratio (not absolute numbers, which are disk-
// dependent) is the signal of interest. On a laptop SSD, nosync
// typically runs 10-50x faster per op; the exact multiplier reflects
// how cheap the platform's fsync is on a freshly-created WAL file.
func BenchmarkApplyMutationsRaft_SyncMode(b *testing.B) {
	cases := []struct {
		name string
		opts *pebble.WriteOptions
	}{
		{name: "sync", opts: pebble.Sync},
		{name: "nosync", opts: pebble.NoSync},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			dir := b.TempDir()
			s, err := NewPebbleStore(dir)
			if err != nil {
				b.Fatalf("NewPebbleStore: %v", err)
			}
			defer s.Close()
			ps, ok := s.(*pebbleStore)
			if !ok {
				b.Fatalf("NewPebbleStore returned non-*pebbleStore type: %T", s)
			}
			ps.fsmApplyWriteOpts = tc.opts
			if tc.opts == pebble.NoSync {
				ps.fsmApplySyncModeLabel = fsmSyncModeNoSync
			} else {
				ps.fsmApplySyncModeLabel = fsmSyncModeSync
			}

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
				if err := s.ApplyMutationsRaft(ctx, muts, nil, startTS, commitTS); err != nil {
					b.Fatalf("ApplyMutationsRaft: %v", err)
				}
			}
		})
	}
}
