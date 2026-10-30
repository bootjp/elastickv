package kv

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func claimTestBinding(n byte) TSOClaimBinding {
	return TSOClaimBinding{
		Owner: TSOClaimOwner{1}, Operation: TSOOperationID{n},
		Intent: TSOIntentDigest{n}, Role: TSOClaimCommit,
	}
}

func claimTestRecord(g TSOClaimGrant, n byte, offset uint64) TSOClaimProof {
	c := TSOClaimCredential{ID: TSOClaimID{n}, Secret: [32]byte{n}}
	return TSOClaimProof{Credential: c, Record: TSOClaimRecord{
		ID: c.ID, Grant: g.ID, Timestamp: g.Base + offset,
		Binding: claimTestBinding(n), SecretHash: sha256.Sum256(c.Secret[:]),
	}}
}

func claimTestGrant(t *testing.T, state *TSOStateMachine, term uint64) TSOClaimGrant {
	t.Helper()
	g := TSOClaimGrant{
		ID: TSOClaimID{byte(term)}, Owner: TSOClaimOwner{1}, Term: term,
		Base: max(state.AllocationFloor(), state.PhaseDFloor()) + 1, Count: 16,
	}
	payload, err := marshalTSOClaimGrant(g)
	require.NoError(t, err)
	require.Nil(t, state.Apply(payload))
	return g
}

func applyTestClaims(t *testing.T, state *TSOStateMachine, term uint64, records ...TSOClaimRecord) any {
	t.Helper()
	payload, err := marshalTSOClaimBatch(term, records)
	require.NoError(t, err)
	return state.Apply(payload)
}

func claimTestSnapshot(t *testing.T, state *TSOStateMachine) []byte {
	t.Helper()
	snap, err := state.Snapshot()
	require.NoError(t, err)
	defer snap.Close()
	var out bytes.Buffer
	_, err = snap.WriteTo(&out)
	require.NoError(t, err)
	return out.Bytes()
}

func requireClaimApplyError(t *testing.T, response any, want error) {
	t.Helper()
	applyErr, ok := response.(error)
	require.True(t, ok, "expected apply error, got %T", response)
	require.ErrorIs(t, applyErr, want)
}

// These use the production FSM, not a distributed Raft fault campaign.
func TestTSOConcreteClaimRejectsUnissuedAndSkippedSlots(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	g := claimTestGrant(t, state, 1)
	first := claimTestRecord(g, 10, 0)
	later := claimTestRecord(g, 11, 7)
	require.ErrorIs(t, state.validateClaim(first, first.Record.Binding, TSOClaimOnePhase), ErrTSOClaimUnknown)
	require.Nil(t, applyTestClaims(t, state, 1, first.Record, later.Record))
	require.NoError(t, state.validateClaim(first, first.Record.Binding, TSOClaimOnePhase))
	require.NoError(t, state.validateClaim(later, later.Record.Binding, TSOClaimOnePhase))
	for offset := uint64(1); offset < 7; offset++ {
		unissued := claimTestRecord(g, byte(20+offset), offset)
		require.ErrorIs(t, state.validateClaim(unissued, unissued.Record.Binding, TSOClaimOnePhase), ErrTSOClaimUnknown)
	}
}

func TestTSOConcreteClaimSurvivesSnapshotAndClosesOldGrant(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	g := claimTestGrant(t, state, 1)
	first := claimTestRecord(g, 10, 0)
	before := claimTestSnapshot(t, state)
	require.Nil(t, applyTestClaims(t, state, 1, first.Record))
	after := claimTestSnapshot(t, state)
	require.NotEqual(t, before, after, "claim apply MUST change durable state")
	claimTestGrant(t, state, 2)
	afterClose := claimTestSnapshot(t, state)
	restored := NewTSOStateMachine(NewHLC())
	require.NoError(t, restored.Restore(bytes.NewReader(afterClose)))
	require.NoError(t, restored.validateClaim(first, first.Record.Binding, TSOClaimSecondary))
	require.Nil(t, applyTestClaims(t, restored, 2, first.Record), "already committed claim retry remains valid")
	unissued := claimTestRecord(g, 11, 1)
	response := applyTestClaims(t, restored, 2, unissued.Record)
	requireClaimApplyError(t, response, ErrTSOClaimClosed)
	require.Equal(t, afterClose, claimTestSnapshot(t, restored))
}

func TestTSOConcreteClaimProofCannotAuthorizeAnotherOperation(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	g := claimTestGrant(t, state, 1)
	p := claimTestRecord(g, 10, 0)
	require.Nil(t, applyTestClaims(t, state, 1, p.Record))
	start := claimTestRecord(g, 11, 1)
	start.Record.Binding = p.Record.Binding
	start.Record.Binding.Role = TSOClaimStart
	require.Nil(t, applyTestClaims(t, state, 1, start.Record))
	for _, phase := range []TSOClaimPhase{TSOClaimRead, TSOClaimPrepare} {
		require.NoError(t, state.validateClaim(start, start.Record.Binding, phase))
	}
	require.ErrorIs(t, state.validateClaim(start, start.Record.Binding, TSOClaimOnePhase), ErrTSOClaimInvalid)
	for _, phase := range []TSOClaimPhase{TSOClaimOnePhase, TSOClaimPrepareEvidence, TSOClaimPrimary, TSOClaimSecondary, TSOClaimResolution} {
		require.NoError(t, state.validateClaim(p, p.Record.Binding, phase), "txn phases reuse, not consume, the claim")
	}
	for _, mutate := range []func(*TSOClaimProof){
		func(p *TSOClaimProof) { p.Credential.Secret[0]++ },
		func(p *TSOClaimProof) { p.Record.Binding.Owner[0]++ },
		func(p *TSOClaimProof) { p.Record.Binding.Operation[0]++ },
		func(p *TSOClaimProof) { p.Record.Binding.Intent[0]++ },
		func(p *TSOClaimProof) { p.Record.Timestamp++ },
		func(p *TSOClaimProof) { p.Record.Grant[0]++ },
	} {
		forged := p
		mutate(&forged)
		require.Error(t, state.validateClaim(forged, p.Record.Binding, TSOClaimOnePhase))
		require.Error(t, state.validateClaim(forged, forged.Record.Binding, TSOClaimOnePhase), "even self-described bindings cannot rewrite the ledger")
	}
	require.ErrorIs(t, state.validateClaim(p, p.Record.Binding, TSOClaimPrepare), ErrTSOClaimInvalid)
	require.ErrorIs(t, state.validateClaim(p, p.Record.Binding, TSOClaimPhase(255)), ErrTSOClaimInvalid)
}

func TestTSOConcreteClaimBatchIsAtomicAndCannotRebind(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	g := claimTestGrant(t, state, 1)
	p := claimTestRecord(g, 10, 0)
	conflict := claimTestRecord(g, 11, 0)
	before := claimTestSnapshot(t, state)
	response := applyTestClaims(t, state, 1, p.Record, conflict.Record)
	requireClaimApplyError(t, response, ErrTSOClaimConflict)
	require.Equal(t, before, claimTestSnapshot(t, state))
	require.Nil(t, applyTestClaims(t, state, 1, p.Record))
	for _, change := range []func(*TSOClaimRecord){
		func(r *TSOClaimRecord) { r.Timestamp++ },
		func(r *TSOClaimRecord) { r.SecretHash[0]++ },
		func(r *TSOClaimRecord) { r.Binding.Intent[0]++ },
		func(r *TSOClaimRecord) { r.ID[0]++; r.Timestamp++ },
	} {
		rebound := p.Record
		change(&rebound)
		response = applyTestClaims(t, state, 1, rebound)
		requireClaimApplyError(t, response, ErrTSOClaimConflict)
	}
}

func TestTSOConcreteClaimBatchStrictDecoder(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	g := claimTestGrant(t, state, 1)
	p := claimTestRecord(g, 10, 0)
	valid, err := marshalTSOClaimBatch(1, []TSOClaimRecord{p.Record})
	require.NoError(t, err)
	oversized := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(oversized[len(tsoClaimBatchEnvelope)+8:], MaxTSOClaimsPerBatch+1)
	for _, payload := range [][]byte{valid[:len(valid)-1], append(append([]byte(nil), valid...), 0), oversized} {
		_, _, err := decodeTSOClaimBatch(payload)
		require.Error(t, err)
	}
	response := applyTestClaims(t, state, 1, p.Record, p.Record)
	requireClaimApplyError(t, response, ErrTSOClaimConflict)
}

func TestTSOClaimRetentionCapacityNeverEvictsClaims(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	g := TSOClaimGrant{ID: TSOClaimID{1}, Owner: TSOClaimOwner{1}, Term: 1, Base: 2, Count: maxTSOClaimRecords}
	payload, err := marshalTSOClaimGrant(g)
	require.NoError(t, err)
	require.Nil(t, state.Apply(payload))
	for start := 0; start < maxTSOClaimRecords; start += MaxTSOClaimsPerBatch {
		records := make([]TSOClaimRecord, MaxTSOClaimsPerBatch)
		for j := range records {
			i := start + j
			r := claimTestRecord(g, 10, uint64(i)).Record
			binary.BigEndian.PutUint64(r.ID[:], uint64(i+1))
			binary.BigEndian.PutUint64(r.Binding.Operation[:], uint64(i+1))
			records[j] = r
		}
		require.Nil(t, applyTestClaims(t, state, 1, records...))
	}
	require.Len(t, state.claims.Claims, maxTSOClaimRecords)
	first := state.claims.Claims[TSOClaimID{0, 0, 0, 0, 0, 0, 0, 1}]
	require.Nil(t, applyTestClaims(t, state, 1, first), "idempotent replay remains available at capacity")
	newGrant := claimTestGrant(t, state, 2)
	next := claimTestRecord(newGrant, 12, 0)
	requireClaimApplyError(t, applyTestClaims(t, state, 2, next.Record), ErrTSOClaimCapacity)
	require.Equal(t, first, state.claims.Claims[first.ID])
	require.Len(t, state.claims.Claims, maxTSOClaimRecords)
}

func TestTSOClaimGrantOverflowAndCapacity(t *testing.T) {
	_, state := newSlotClaimTSOFixture(t)
	invalid := TSOClaimGrant{ID: TSOClaimID{1}, Owner: TSOClaimOwner{1}, Term: 1, Base: ^uint64(0), Count: 2}
	_, err := marshalTSOClaimGrant(invalid)
	require.ErrorIs(t, err, ErrTSOClaimInvalid)
	for i := range maxTSOClaimGrants {
		g := TSOClaimGrant{Owner: TSOClaimOwner{1}, Term: 1, Base: max(state.AllocationFloor(), state.PhaseDFloor()) + 1, Count: 1}
		binary.BigEndian.PutUint64(g.ID[:], uint64(i+1))
		payload, err := marshalTSOClaimGrant(g)
		require.NoError(t, err)
		require.Nil(t, state.Apply(payload))
	}
	before := claimTestSnapshot(t, state)
	g := TSOClaimGrant{ID: TSOClaimID{255}, Owner: TSOClaimOwner{1}, Term: 2, Base: state.AllocationFloor() + 1, Count: 1}
	payload, err := marshalTSOClaimGrant(g)
	require.NoError(t, err)
	requireClaimApplyError(t, state.Apply(payload), ErrTSOClaimCapacity)
	require.Equal(t, before, claimTestSnapshot(t, state), "capacity rejection must not close grants")
}
