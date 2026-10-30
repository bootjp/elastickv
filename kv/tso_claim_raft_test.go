package kv

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/raftengine/raftenginetest"
	"github.com/stretchr/testify/require"
)

func claimRecordingEngine(t *testing.T, alloc *RaftTSOAllocator) *recordingTSOEngine {
	t.Helper()
	engine, ok := alloc.group.Engine.(*recordingTSOEngine)
	require.True(t, ok)
	return engine
}

func TestTSOClaimGrantMinimumIsOnlyAReservation(t *testing.T) {
	alloc, state := newSlotClaimTSOFixture(t)
	g, err := alloc.reserveClaimGrant(t.Context(), TSOClaimOwner{1}, 16, 0)
	require.NoError(t, err)
	next, err := alloc.reserveClaimGrant(t.Context(), g.Owner, 16, g.Base+6)
	require.NoError(t, err)
	require.Greater(t, next.Base, g.end())
	for offset := uint64(0); offset < uint64(g.Count); offset++ {
		unissued := claimTestRecord(g, byte(10+offset), offset)
		require.ErrorIs(t, state.validateClaim(unissued, unissued.Record.Binding, TSOClaimOnePhase), ErrTSOClaimUnknown)
	}
}

func TestTSOClaimCommitBeforeReplyAndLostReplyReconciliation(t *testing.T) {
	alloc, state := newSlotClaimTSOFixture(t)
	g, err := alloc.reserveClaimGrant(t.Context(), TSOClaimOwner{1}, 16, 0)
	require.NoError(t, err)
	p := claimTestRecord(g, 10, 0)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engine := claimRecordingEngine(t, alloc)
	engine.afterPropose = func(payload []byte) {
		if bytes.HasPrefix(payload, []byte(tsoClaimBatchEnvelope)) {
			require.NoError(t, state.validateClaim(p, p.Record.Binding, TSOClaimPrimary), "record is applied before any successful reply")
			forged := p
			forged.Credential.Secret[0]++
			require.ErrorIs(t, state.validateClaim(forged, p.Record.Binding, TSOClaimPrimary), ErrTSOClaimInvalid,
				"a committed-but-not-replied slot is not public authority")
			cancel()
		}
	}
	require.ErrorIs(t, alloc.commitClaimBatch(ctx, g.Term, []TSOClaimRecord{p.Record}), ErrTSOClaimUnknown)
	engine.afterPropose = nil
	recovered, err := alloc.reconcileClaim(t.Context(), p.Credential, p.Record.Binding)
	require.NoError(t, err)
	require.Equal(t, p, recovered)
	require.NoError(t, alloc.commitClaimBatch(t.Context(), g.Term, []TSOClaimRecord{p.Record}))
	rebound := p.Record
	rebound.Timestamp++
	require.ErrorIs(t, alloc.commitClaimBatch(t.Context(), g.Term, []TSOClaimRecord{rebound}), ErrTSOClaimConflict)
}

func TestTSOClaimCommitRequiresMatchingAppliedRecord(t *testing.T) {
	alloc, _ := newSlotClaimTSOFixture(t)
	g, err := alloc.reserveClaimGrant(t.Context(), TSOClaimOwner{1}, 16, 0)
	require.NoError(t, err)
	p := claimTestRecord(g, 10, 0)
	engine := claimRecordingEngine(t, alloc)
	engine.apply = nil
	require.ErrorIs(t, alloc.commitClaimBatch(t.Context(), g.Term, []TSOClaimRecord{p.Record}), ErrTSOClaimUnknown)
	_, err = alloc.reconcileClaim(t.Context(), p.Credential, p.Record.Binding)
	require.ErrorIs(t, err, ErrTSOClaimUnknown)
}

func TestTSOClaimNativeWALReopenAndNewTerm(t *testing.T) {
	const electionBudget = 5 * time.Second
	dir := t.TempDir()
	factory := newSingleRaftFactory()
	open := func() (*RaftTSOAllocator, *raftengine.FactoryResult) {
		clock := NewHLC()
		state := NewTSOStateMachine(clock)
		result, err := factory.Create(raftengine.FactoryConfig{
			LocalID: "n1", LocalAddress: "127.0.0.1:0", DataDir: dir,
			Bootstrap: true, StateMachine: state,
		})
		require.NoError(t, err)
		raftenginetest.WaitForLeader(t, electionBudget, result.Engine)
		alloc, err := newTestRaftTSOAllocator(&ShardGroup{Engine: result.Engine, TSOState: state}, clock)
		require.NoError(t, err)
		return alloc, result
	}
	alloc, first := open()
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, first.Engine.Close())
		}
	})
	for _, payload := range [][]byte{
		marshalHLCLeaseRenew(time.Now().Add(testTSOFutureCeiling).UnixMilli()),
		marshalTSOCutover(), marshalTSOPhaseD(1),
	} {
		require.NoError(t, proposeTSOClaim(t.Context(), first.Engine, payload))
	}
	g, err := alloc.reserveClaimGrant(t.Context(), TSOClaimOwner{1}, 16, 0)
	require.NoError(t, err)
	p := claimTestRecord(g, 10, 0)
	require.NoError(t, alloc.commitClaimBatch(t.Context(), g.Term, []TSOClaimRecord{p.Record}))
	require.NoError(t, first.Engine.Close())
	closed = true
	reopened, second := open()
	t.Cleanup(func() { require.NoError(t, second.Engine.Close()) })
	recovered, err := reopened.reconcileClaim(t.Context(), p.Credential, p.Record.Binding)
	require.NoError(t, err)
	require.Equal(t, p, recovered)
	newGrant, err := reopened.reserveClaimGrant(t.Context(), g.Owner, 16, 0)
	require.NoError(t, err)
	require.Greater(t, newGrant.Term, g.Term)
	oldUnclaimed := claimTestRecord(g, 11, 1)
	require.ErrorIs(t, reopened.commitClaimBatch(t.Context(), newGrant.Term, []TSOClaimRecord{oldUnclaimed.Record}), ErrTSOClaimClosed)
	require.NoError(t, reopened.validateTimestampClaim(t.Context(), p, p.Record.Binding, TSOClaimResolution))
	// A persisted claim retries in the current term without rebinding it.
	require.NoError(t, reopened.commitClaimBatch(t.Context(), newGrant.Term, []TSOClaimRecord{p.Record}))
}

func TestTSOClaimCombinerParallelUniqueness(t *testing.T) {
	alloc, state := newSlotClaimTSOFixture(t)
	const workers, perWorker = 8, 64
	g, err := alloc.reserveClaimGrant(t.Context(), TSOClaimOwner{1}, workers*perWorker, 0)
	require.NoError(t, err)
	combiner := newTSOClaimCombiner(t.Context(), alloc)
	defer combiner.close()
	proofs := make([]TSOClaimProof, workers*perWorker)
	failures := make([]error, workers*perWorker)
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for j := range perWorker {
				i := worker*perWorker + j
				candidate := claimTestRecord(g, 10, uint64(i))
				binary.BigEndian.PutUint64(candidate.Record.ID[:], uint64(i+1))
				candidate.Credential.ID = candidate.Record.ID
				binary.BigEndian.PutUint64(candidate.Record.Binding.Operation[:], uint64(i+1))
				proofs[i], failures[i] = combiner.claim(t.Context(), g.Term, candidate)
			}
		})
	}
	wg.Wait()
	seen := make(map[uint64]bool, len(proofs))
	for i, proof := range proofs {
		require.NoError(t, failures[i], "every request must succeed")
		require.False(t, seen[proof.Record.Timestamp])
		seen[proof.Record.Timestamp] = true
		require.NoError(t, state.validateClaim(proof, proof.Record.Binding, TSOClaimOnePhase))
	}
	require.Len(t, seen, workers*perWorker)
	for _, payload := range claimRecordingEngine(t, alloc).proposedPayloads() {
		if !bytes.HasPrefix(payload, []byte(tsoClaimBatchEnvelope)) {
			continue
		}
		require.LessOrEqual(t, len(payload), maxTSOClaimBatchBytes)
		_, records, err := decodeTSOClaimBatch(payload)
		require.NoError(t, err)
		require.LessOrEqual(t, len(records), MaxTSOClaimsPerBatch)
	}
}
