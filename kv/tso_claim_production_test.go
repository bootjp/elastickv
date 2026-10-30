package kv

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type pinnedClaimSigner struct{ key [ed25519.PublicKeySize]byte }

func (p pinnedClaimSigner) AuthorizeTimestampClaimLookup(ctx context.Context) error { return ctx.Err() }

func (p pinnedClaimSigner) AuthorizeTimestampClaim(ctx context.Context, key [ed25519.PublicKeySize]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if p.key != key {
		return ErrTSOClaimAuthorityRequired
	}
	return nil
}

func newProductionClaimFixture(t *testing.T) (*RaftTSOAllocator, *BatchAllocator, *TSOStateMachine) {
	t.Helper()
	old, state := newSlotClaimTSOFixture(t)
	seed := bytes.Repeat([]byte{31}, ed25519.SeedSize)
	identity, err := NewTSOClaimIdentity(seed)
	require.NoError(t, err)
	allocator, err := NewRaftTSOAllocator(old.group, old.clock,
		WithTSOCutoverFloorProvider(&recordingTSOFloorProvider{}),
		WithTSOPhaseDActivationGate(&testPhaseDGate{}),
		WithRaftTSOClaimIdentity(identity), WithTSOClaimAuthority(pinnedClaimSigner{key: identity.publicKey()}))
	require.NoError(t, err)
	batch, err := NewBatchAllocator(allocator, 16)
	require.NoError(t, err)
	return allocator, batch, state
}

func TestTSOProductionClaimsNextNextAfterUnissuedAndSnapshot(t *testing.T) {
	allocator, batch, state := newProductionClaimFixture(t)
	first, err := batch.Next(t.Context())
	require.NoError(t, err)
	require.GreaterOrEqual(t, state.AllocationFloor(), first+15)
	require.ErrorIs(t, allocator.ValidateDurableTimestamp(t.Context(), first+1), ErrTSOTimestampInvalid)
	before := claimTestSnapshot(t, state)
	later, err := batch.NextAfter(t.Context(), first+6)
	require.NoError(t, err)
	require.Equal(t, first+7, later)
	for slot := first + 1; slot < later; slot++ {
		require.ErrorIs(t, allocator.ValidateDurableTimestamp(t.Context(), slot), ErrTSOTimestampInvalid)
	}
	require.NoError(t, allocator.ValidateDurableTimestamp(t.Context(), first))
	require.NoError(t, allocator.ValidateDurableTimestamp(t.Context(), later))
	require.NotEqual(t, before, claimTestSnapshot(t, state))
	restored := NewTSOStateMachine(NewHLC())
	require.NoError(t, restored.Restore(bytes.NewReader(claimTestSnapshot(t, state))))
	require.Len(t, restored.claims.Claims, 2, "reservations/skipped offsets must not become claims on restore")
}

func TestTSOProductionClaimsRawCoordinatorAndAdmission(t *testing.T) {
	allocator, batch, _ := newProductionClaimFixture(t)
	st := store.NewMVCCStore()
	dataEngine, closeData := newSingleRaft(t, "claim-data", NewKvFSMWithHLC(st, NewHLC()))
	defer closeData()
	manager := NewTransactionWithProposer(dataEngine)
	defer manager.Close()
	coordinator := NewCoordinatorWithEngine(manager, dataEngine, WithTSOAllocator(batch), WithCoordinateTimestampClaimGroup(1), WithTimestampAdmissionStore(st))
	defer coordinator.Close()
	group := &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("claimed"), Value: []byte("value")}}}
	response, err := coordinator.Dispatch(t.Context(), group)
	require.NoError(t, err)
	require.NotZero(t, response.CommitTS)
	stored, err := st.GetAt(t.Context(), []byte("claimed"), response.CommitTS)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), stored)
	request := rawClaimRequestForGroup(group, 1)
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), batch, request))
	require.Equal(t, response.CommitTS, request.Ts, "same actual operation must resolve its existing claim")
	forged := proto.CloneOf(request)
	forged.Mutations[0].Value = []byte("foreign operation")
	require.ErrorIs(t, StampTimestampAdmission(t.Context(), batch, st, []*pb.Request{request, forged}), ErrTSOClaimInvalid,
		"equal uint64 cannot skip validation of a different request")
	_, err = manager.Commit(t.Context(), []*pb.Request{forged})
	require.ErrorIs(t, err, ErrTSOClaimInvalid)
	bare := proto.CloneOf(request)
	bare.TimestampClaim = nil
	require.ErrorIs(t, StampRequestTimestampClaim(t.Context(), batch, bare), ErrTSOClaimInvalid)
	require.NoError(t, allocator.ValidateDurableTimestamp(t.Context(), bare.Ts), "membership is diagnostic, not write authority")
}

func TestTSOProductionClaimsUnknownReplyReconstructsDurableAttempt(t *testing.T) {
	allocator, batch, state := newProductionClaimFixture(t)
	request := &pb.Request{TimestampClaimGroupId: 1, Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value")}}}
	require.NoError(t, EnsureRequestTimestampIntent(request))
	// The request ID and node seed, not a process-local cache, reconstruct
	// the same credential after an unknown issuance response.
	retained := proto.CloneOf(request)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	engine := claimRecordingEngine(t, allocator)
	engine.afterPropose = func(data []byte) {
		if bytes.HasPrefix(data, []byte(tsoClaimIntentEnvelope)) {
			cancel()
		}
	}
	require.ErrorIs(t, IssueRequestTimestampClaim(ctx, batch, request), ErrTSOClaimUnknown)
	engine.afterPropose = nil
	identity, err := NewTSOClaimIdentity(bytes.Repeat([]byte{31}, ed25519.SeedSize))
	require.NoError(t, err)
	restarted, err := NewBatchAllocator(allocator, 16, WithBatchTSOClaimIdentity(identity))
	require.NoError(t, err)
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), restarted, retained))
	require.Len(t, state.claims.Claims, 1)
	require.NoError(t, ValidateRequestTimestampClaim(t.Context(), restarted, retained))
	foreign := proto.CloneOf(retained)
	foreign.TimestampClaim = nil
	foreign.Ts = 0
	foreign.Mutations[0].Value = []byte("different")
	foreign.TimestampIntent.Participants[0].Mutations, err = claimMutationEvidenceList(foreign.Mutations)
	require.NoError(t, err)
	err = IssueRequestTimestampClaim(t.Context(), restarted, foreign)
	require.True(t, errors.Is(err, ErrTSOClaimConflict), "%v", err)
}

func TestTSOProductionClaimsOnePhaseKeepsReadWatermark(t *testing.T) {
	allocator, batch, _ := newProductionClaimFixture(t)
	st := store.NewMVCCStore()
	engine, closeData := newSingleRaft(t, "claim-one-phase", NewKvFSMWithHLC(st, NewHLC()))
	defer closeData()
	manager := NewTransactionWithProposer(engine)
	defer manager.Close()
	coordinator := NewCoordinatorWithEngine(manager, engine, WithTSOAllocator(batch), WithCoordinateTimestampClaimGroup(1), WithTimestampAdmissionStore(st))
	defer coordinator.Close()
	group := &OperationGroup[OP]{IsTxn: true, StartTS: 1, Elems: []*Elem[OP]{{Op: Put, Key: []byte("key"), Value: []byte("value")}}}
	response, err := coordinator.Dispatch(t.Context(), group)
	require.NoError(t, err)
	require.Equal(t, uint64(1), group.StartTS)
	require.Greater(t, response.CommitTS, group.StartTS)
	require.ErrorIs(t, allocator.ValidateDurableTimestamp(t.Context(), group.StartTS), ErrTSOTimestampPrePhaseD,
		"the read watermark is not being fabricated into a durable write claim")
	value, err := st.GetAt(t.Context(), []byte("key"), response.CommitTS)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
}

func newClaimedShardedFixture(t *testing.T) (*ShardedCoordinator, *ShardStore, map[uint64]*ShardGroup, *RaftTSOAllocator, *TSOStateMachine) {
	t.Helper()
	allocator, batch, state := newProductionClaimFixture(t)
	router := distribution.NewEngine()
	applyComposed1Snapshot(t, router, 7, []distribution.RouteDescriptor{{RouteID: 1, Start: []byte("a"), End: []byte("m"), GroupID: 1}, {RouteID: 2, Start: []byte("m"), GroupID: 2}})
	groups := map[uint64]*ShardGroup{0: allocator.group}
	for _, id := range []uint64{1, 2} {
		st := store.NewMVCCStore()
		fsm := NewKvFSMWithHLC(st, NewHLC(), WithRouteHistory(WrapDistributionEngine(router), id))
		engine, closeNode := newSingleRaft(t, "claimed-data-"+string(rune('0'+id)), fsm)
		t.Cleanup(closeNode)
		for _, control := range [][]byte{EncodePrepareEvidenceFence(1), EncodePrepareEvidenceSeal(), EncodePrepareEvidenceRelease(1)} {
			_, err := engine.Propose(t.Context(), control)
			require.NoError(t, err)
		}
		groups[id] = &ShardGroup{Engine: engine, Store: st, Txn: NewLeaderProxyWithEngine(engine)}
	}
	shards := NewShardStore(router, groups)
	coordinator := NewShardedCoordinator(router, groups, 1, NewHLC(), shards).WithTSOAllocator(batch).WithTimestampGroup(0).WithAllShardGroups(1, 2)
	return coordinator, shards, groups, allocator, state
}

func TestTSOProductionClaimsDistributedPrepareCommitAndPhaseReuse(t *testing.T) {
	coordinator, _, groups, _, state := newClaimedShardedFixture(t)
	operation := &OperationGroup[OP]{IsTxn: true, RequireAllParticipants: true,
		Elems: []*Elem[OP]{{Op: Put, Key: []byte("b"), Value: []byte("primary")}, {Op: Put, Key: []byte("x"), Value: []byte("secondary")}}}
	response, err := coordinator.Dispatch(t.Context(), operation)
	require.NoError(t, err)
	require.Greater(t, response.CommitTS, operation.StartTS)
	for _, id := range []uint64{1, 2} {
		_, _, err := readTxnClaimEvidence(t.Context(), groups[id].Store, []byte("b"), operation.StartTS)
		require.NoError(t, err, "prepare evidence must remain durable after lock cleanup")
		key := []byte("b")
		if id == 2 {
			key = []byte("x")
		}
		value, err := groups[id].Store.GetAt(t.Context(), key, response.CommitTS)
		require.NoError(t, err)
		require.NotEmpty(t, value)
		require.NoError(t, applyTxnResolution(t.Context(), groups[id], pb.Phase_COMMIT, operation.StartTS, response.CommitTS, []byte("b"), [][]byte{key}), "same phase retries must not consume the claim")
	}
	require.Len(t, state.claims.Claims, 2, "one read observation and one shared transaction commit claim")
}

func TestTSOProductionClaimsShardedRawAndPrefixBroadcast(t *testing.T) {
	coordinator, _, groups, _, _ := newClaimedShardedFixture(t)
	write := &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("b/key"), Value: []byte("primary")}, {Op: Put, Key: []byte("x/key"), Value: []byte("secondary")}}}
	_, err := coordinator.Dispatch(t.Context(), write)
	require.NoError(t, err)
	prefixes := &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: DelPrefix, Key: []byte("b/")}, {Op: DelPrefix, Key: []byte("x/")}}}
	_, err = coordinator.Dispatch(t.Context(), prefixes)
	require.NoError(t, err)
	for id, key := range map[uint64][]byte{1: []byte("b/key"), 2: []byte("x/key")} {
		_, err := groups[id].Store.GetAt(t.Context(), key, ^uint64(0))
		require.ErrorIs(t, err, store.ErrKeyNotFound)
	}
	requests, err := coordinator.claimedPrefixRequests(t.Context(), prefixes)
	require.NoError(t, err)
	forged := proto.CloneOf(requests[0])
	forged.Mutations[0].Key = []byte("foreign/")
	require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), coordinator.tsAllocator, forged), ErrTSOClaimInvalid)
	forged = proto.CloneOf(requests[0])
	forged.TimestampClaimPrefixIndices = []uint32{2}
	require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), coordinator.tsAllocator, forged), ErrTSOClaimInvalid)
}

func TestTSOProductionClaimsPreparedEvidenceResolutionBinding(t *testing.T) {
	coordinator, _, groups, _, _ := newClaimedShardedFixture(t)
	request := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 1, ObservedRouteVersion: 7, TimestampClaimGroupId: 1,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("b"), LockTTLms: defaultTxnLockTTLms})},
			{Op: pb.Op_PUT, Key: []byte("b"), Value: make([]byte, 9), CommitTsValueOffset: 1},
		}}
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), coordinator.tsAllocator, request))
	proof, err := DecodeRequestTimestampClaim(request.TimestampClaim)
	require.NoError(t, err)
	commitTS := proof.Proof.Record.Timestamp
	require.Equal(t, commitTS, binary.BigEndian.Uint64(request.Mutations[1].Value[1:]))
	_, err = groups[1].Txn.Commit(t.Context(), []*pb.Request{request})
	require.NoError(t, err, "PREPARE must durably retain its proof at the commit claim, not at the read watermark")
	operation := &OperationGroup[OP]{IsTxn: true, StartTS: 1, CommitTS: commitTS, ObservedRouteVersion: 7,
		TimestampIntent: request.TimestampIntent, TimestampClaim: request.TimestampClaim}
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT, pb.Phase_COMMIT} {
		resolution, err := claimedResolutionRequest(operation, 1, []byte("b"), phase)
		require.NoError(t, err)
		require.NoError(t, ValidateClaimedTxnResolution(t.Context(), coordinator.tsAllocator, groups[1].Store, resolution))
		foreign := proto.CloneOf(resolution)
		foreign.Mutations[1].Key = []byte("c")
		require.ErrorIs(t, ValidateClaimedTxnResolution(t.Context(), coordinator.tsAllocator, groups[1].Store, foreign), ErrTSOClaimInvalid)
		for name, change := range map[string]func(*pb.Request){
			"unbound read alias":   func(r *pb.Request) { r.ReadKeys = [][]byte{[]byte("unbound-read")} },
			"unbound fence bypass": func(r *pb.Request) { r.WriteFenceBypassKeys = [][]byte{[]byte("b")} },
			"unknown metadata":     func(r *pb.Request) { r.Mutations[0].ProtoReflect().SetUnknown([]byte{0x30, 1}) },
		} {
			t.Run(phase.String()+"/"+name, func(t *testing.T) {
				forged := proto.CloneOf(resolution)
				change(forged)
				require.ErrorIs(t, ValidateClaimedTxnResolution(t.Context(), coordinator.tsAllocator, groups[1].Store, forged), ErrTSOClaimInvalid)
			})
		}
	}
	forged := proto.CloneOf(request)
	binary.BigEndian.PutUint64(forged.Mutations[1].Value[1:], commitTS+1)
	require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), coordinator.tsAllocator, forged), ErrTSOClaimInvalid)
	forged = proto.CloneOf(request)
	forged.TimestampClaim[len(forged.TimestampClaim)-1] ^= 1
	require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), coordinator.tsAllocator, forged), ErrTSOClaimInvalid)
}
