package kv

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/stretchr/testify/require"
)

type benchmarkClaimCommitter struct {
	allocator *RaftTSOAllocator
	batches   atomic.Uint64
}

func (c *benchmarkClaimCommitter) commitClaimBatch(ctx context.Context, term uint64, records []TSOClaimRecord) error {
	if err := c.allocator.commitClaimBatch(ctx, term, records); err != nil {
		return err
	}
	c.batches.Add(1)
	return nil
}

// Native single-node WAL consensus, excluding grant reservation and credential
// preparation. This is NOT an end-to-end production or multi-node RPC benchmark.
// Use a fixed iteration count within the no-GC ledger capacity, e.g. 128x.
func BenchmarkTSOConcreteClaimNative(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		name := "singleton"
		if parallel {
			name = "parallel"
		}
		b.Run(name, func(b *testing.B) {
			benchmarkTSOClaimNative(b, parallel)
		})
	}
}

func benchmarkTSOClaimNative(b *testing.B, parallel bool) {
	if b.N > maxTSOClaimRecords {
		b.Fatalf("iterations %d exceed no-GC capacity %d; use a fixed benchtime", b.N, maxTSOClaimRecords)
	}
	clock := NewHLC()
	state := NewTSOStateMachine(clock)
	result := raftenginetest.NewSingleNode(b, newSingleRaftFactory(), state)
	allocator, err := newTestRaftTSOAllocator(&ShardGroup{Engine: result.Engine, TSOState: state}, clock)
	require.NoError(b, err)
	for _, payload := range [][]byte{
		marshalHLCLeaseRenew(time.Now().Add(testTSOFutureCeiling).UnixMilli()),
		marshalTSOCutover(), marshalTSOPhaseD(1),
	} {
		require.NoError(b, proposeTSOClaim(b.Context(), result.Engine, payload))
	}
	g, err := allocator.reserveClaimGrant(b.Context(), TSOClaimOwner{1}, max(b.N, 1), 0)
	require.NoError(b, err)
	candidates := make([]TSOClaimProof, b.N)
	for i := range b.N {
		candidates[i] = benchmarkTSOClaimCandidate(b, g, i)
	}
	committer := &benchmarkClaimCommitter{allocator: allocator}
	combiner := newTSOClaimCombiner(b.Context(), committer)
	defer combiner.close()
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		var next atomic.Uint64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				i := next.Add(1) - 1
				if _, err := combiner.claim(b.Context(), g.Term, candidates[i]); err != nil {
					b.Error(err)
				}
			}
		})
	} else {
		for i := range b.N {
			if _, err := combiner.claim(b.Context(), g.Term, candidates[i]); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/float64(committer.batches.Load()), "claims/batch")
}

func benchmarkTSOClaimCandidate(b *testing.B, g TSOClaimGrant, i int) TSOClaimProof {
	b.Helper()
	c, err := NewTSOClaimCredential()
	require.NoError(b, err)
	binding := claimTestBinding(1)
	binary.BigEndian.PutUint64(binding.Operation[:], uint64(i+1))
	return TSOClaimProof{Credential: c, Record: TSOClaimRecord{
		ID: c.ID, Grant: g.ID, Timestamp: g.Base + uint64(i), Binding: binding,
		SecretHash: claimCredentialHash(c),
	}}
}
