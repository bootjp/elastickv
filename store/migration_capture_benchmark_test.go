package store

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

// This is a store-entry diagnostic, not an online-Split throughput benchmark.
// Both cases commit 16 point writes in one synced, indexed entry.
func BenchmarkMigrationCaptureEntry(b *testing.B) {
	for _, shadow := range []bool{false, true} {
		name := "inactive"
		if shadow {
			name = "shadow"
		}
		b.Run(name, func(b *testing.B) { benchmarkMigrationCaptureEntry(b, shadow) })
	}
}

func benchmarkMigrationCaptureEntry(b *testing.B, shadow bool) {
	s, err := NewPebbleStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := s.Close(); err != nil {
			b.Error(err)
		}
	})
	applier, ok := s.(RaftEntryApplier)
	if !ok {
		b.Fatal("store must support indexed atomic apply")
	}
	mutations := make([]*KVPairMutation, 16)
	for i := range mutations {
		mutations[i] = &KVPairMutation{Op: OpTypePut, Key: fmt.Appendf(nil, "key/%02d", i), Value: bytes.Repeat([]byte("x"), 1024)}
	}
	requests := []MigrationCaptureRequest{{Mutations: mutations, Fenced: true}}
	id := MigrationCaptureIdentity{SourceGroupID: 1, JobID: 9, Generation: 1, Lineage: [32]byte{1}}
	b.SetBytes(16 * 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		id.Index++
		requests[0].StartTS, requests[0].CommitTS = id.Index*2, id.Index*2+1
		if shadow {
			_, err = ApplyMigrationCaptureEntry(b.Context(), s, id, requests, MigrationCaptureLimits{})
		} else {
			err = applier.ApplyRaftEntry(b.Context(), id.Index, func(ctx context.Context) error {
				return s.ApplyMutationsRaftAtFenced(ctx, mutations, nil, requests[0].StartTS, requests[0].CommitTS, id.Index)
			})
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
