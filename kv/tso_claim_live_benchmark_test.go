package kv

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/stretchr/testify/require"
)

type publicClaimBenchmarkEngine struct {
	raftengine.Engine
	batches atomic.Uint64
}

func (e *publicClaimBenchmarkEngine) Propose(ctx context.Context, payload []byte) (*raftengine.ProposalResult, error) {
	result, err := e.Engine.Propose(ctx, payload)
	if err == nil && bytes.HasPrefix(payload, []byte(tsoClaimIntentEnvelope)) {
		e.batches.Add(1)
	}
	return result, err
}

// Measures the real public BatchAllocator.Next -> ClaimTimestamp -> native
// single-node WAL path, including signing, lookup fences and grant creation.
// Pinned-key test authority excludes TLS/registry provisioning and network RPC.
func BenchmarkTSOProductionPublicNextNative(b *testing.B) {
	for _, parallel := range []bool{false, true} {
		name := "singleton"
		if parallel {
			name = "parallel"
		}
		b.Run(name, func(b *testing.B) { benchmarkTSOPublicNext(b, parallel) })
	}
}

func benchmarkTSOPublicNext(b *testing.B, parallel bool) {
	if b.N > maxTSOClaimRecords {
		b.Fatalf("iterations %d exceed retained claim capacity %d; use a fixed benchtime", b.N, maxTSOClaimRecords)
	}
	clock := NewHLC()
	state := NewTSOStateMachine(clock)
	result := raftenginetest.NewSingleNode(b, newSingleRaftFactory(), state)
	engine := &publicClaimBenchmarkEngine{Engine: result.Engine}
	identity, err := NewTSOClaimIdentity(bytes.Repeat([]byte{31}, ed25519.SeedSize))
	require.NoError(b, err)
	allocator, err := NewRaftTSOAllocator(&ShardGroup{Engine: engine, TSOState: state}, clock,
		WithTSOCutoverFloorProvider(&recordingTSOFloorProvider{}), WithTSOPhaseDActivationGate(&testPhaseDGate{}),
		WithRaftTSOClaimIdentity(identity), WithTSOClaimAuthority(pinnedClaimSigner{key: identity.PublicKey()}))
	require.NoError(b, err)
	for _, payload := range [][]byte{
		marshalHLCLeaseRenew(time.Now().Add(testTSOFutureCeiling).UnixMilli()), marshalTSOCutover(), marshalTSOPhaseD(1),
	} {
		require.NoError(b, proposeTSOClaim(b.Context(), engine, payload))
	}
	batch, err := NewBatchAllocator(allocator, 256)
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	if parallel {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if _, err := batch.Next(b.Context()); err != nil {
					b.Error(err)
				}
			}
		})
	} else {
		for range b.N {
			if _, err := batch.Next(b.Context()); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/float64(engine.batches.Load()), "claims/batch")
}
