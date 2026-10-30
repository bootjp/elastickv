package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTSOClaimRejectsCreationPhaseSubstitution(t *testing.T) {
	for _, initial := range []pb.Phase{pb.Phase_PREPARE, pb.Phase_NONE} {
		t.Run(initial.String(), func(t *testing.T) {
			_, allocator, _ := newProductionClaimFixture(t)
			start, err := allocator.Next(t.Context())
			require.NoError(t, err)
			request := &pb.Request{IsTxn: true, Phase: initial, Ts: start, TimestampClaimGroupId: 1,
				Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("key"), LockTTLms: defaultTxnLockTTLms})},
					{Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value")},
				}}
			require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
			require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, request), "the original creation remains valid")
			forged := proto.CloneOf(request)
			meta, _, err := extractTxnMeta(forged.Mutations)
			require.NoError(t, err)
			if initial == pb.Phase_PREPARE {
				forged.Phase = pb.Phase_NONE
				meta.CommitTS, meta.PrepareCommitTS = meta.PrepareCommitTS, 0
			} else {
				forged.Phase = pb.Phase_PREPARE
				meta.PrepareCommitTS, meta.CommitTS = meta.CommitTS, 0
			}
			forged.Mutations[0].Value = EncodeTxnMeta(meta)
			require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid,
				"the same timestamp proof must not authorize another transaction mode")
		})
	}
}

func newClaimModeRequest(t *testing.T, allocator TimestampAllocator, isTxn bool, phase pb.Phase) *pb.Request {
	t.Helper()
	request := &pb.Request{IsTxn: isTxn, Phase: phase, TimestampClaimGroupId: 1,
		Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value")}}}
	if isTxn {
		start, err := allocator.Next(t.Context())
		require.NoError(t, err)
		request.Ts = start
		request.Mutations = append([]*pb.Mutation{{Op: pb.Op_PUT, Key: []byte(txnMetaPrefix),
			Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("key"), LockTTLms: defaultTxnLockTTLms})}}, request.Mutations...)
	}
	return request
}

func TestTSOClaimRejectsInvalidCreationMode(t *testing.T) {
	const unknownPhase pb.Phase = 127
	for _, test := range []struct {
		name  string
		isTxn bool
		phase pb.Phase
	}{
		{"raw prepare", false, pb.Phase_PREPARE},
		{"raw commit", false, pb.Phase_COMMIT},
		{"raw abort", false, pb.Phase_ABORT},
		{"raw unknown", false, unknownPhase},
		{"txn commit creation", true, pb.Phase_COMMIT},
		{"txn abort creation", true, pb.Phase_ABORT},
		{"txn unknown", true, unknownPhase},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, allocator, state := newProductionClaimFixture(t)
			request := newClaimModeRequest(t, allocator, test.isTxn, test.phase)
			before := claimTestSnapshot(t, state)
			require.ErrorIs(t, IssueRequestTimestampClaim(t.Context(), allocator, request), ErrTSOClaimInvalid)
			require.Empty(t, request.TimestampClaim)
			require.Equal(t, before, claimTestSnapshot(t, state), "invalid creation must not issue another claim")
		})
	}
}

func TestTSOClaimInitialPhaseIsSigned(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE} {
		t.Run(phase.String(), func(t *testing.T) {
			_, allocator, _ := newProductionClaimFixture(t)
			request := newClaimModeRequest(t, allocator, true, phase)
			require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
			forged := proto.CloneOf(request)
			meta, _, err := extractTxnMeta(forged.Mutations)
			require.NoError(t, err)
			if phase == pb.Phase_PREPARE {
				forged.Phase = pb.Phase_NONE
				meta.CommitTS, meta.PrepareCommitTS = meta.PrepareCommitTS, 0
			} else {
				forged.Phase = pb.Phase_PREPARE
				meta.PrepareCommitTS, meta.CommitTS = meta.CommitTS, 0
			}
			forged.TimestampIntent.InitialPhase = forged.Phase
			forged.Mutations[0].Value = EncodeTxnMeta(meta)
			require.NoError(t, ValidateRequestTimestampIntent(forged), "a coherent local projection cannot replace the signed mode")
			require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid)
		})
	}
}

func TestTSOClaimRejectsUnknownOrLegacyManifestMode(t *testing.T) {
	_, allocator, _ := newProductionClaimFixture(t)
	request := newClaimModeRequest(t, allocator, false, pb.Phase_NONE)
	require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
	for name, change := range map[string]func(*pb.Request){
		"unknown request phase":  func(r *pb.Request) { r.Phase = pb.Phase(127) },
		"unknown manifest phase": func(r *pb.Request) { r.TimestampIntent.InitialPhase = pb.Phase(127) },
		"raw manifest prepare":   func(r *pb.Request) { r.TimestampIntent.InitialPhase = pb.Phase_PREPARE },
		"old full value version": func(r *pb.Request) { r.TimestampIntent.ManifestVersion = 1 },
		"old unbound mode version": func(r *pb.Request) {
			r.TimestampIntent.ManifestVersion = 2
		},
	} {
		t.Run(name, func(t *testing.T) {
			forged := proto.CloneOf(request)
			change(forged)
			require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid)
		})
	}
}

func TestTSOClaimRejectsIrrelevantPersistenceSlot(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_NONE, pb.Phase_PREPARE} {
		t.Run(phase.String(), func(t *testing.T) {
			_, allocator, _ := newProductionClaimFixture(t)
			request := newClaimModeRequest(t, allocator, true, phase)
			require.NoError(t, IssueRequestTimestampClaim(t.Context(), allocator, request))
			require.NoError(t, ValidateRequestTimestampClaim(t.Context(), allocator, request))
			for _, dual := range []bool{false, true} {
				forged := proto.CloneOf(request)
				meta, _, err := extractTxnMeta(forged.Mutations)
				require.NoError(t, err)
				if phase == pb.Phase_PREPARE {
					meta.CommitTS = meta.PrepareCommitTS
					if !dual {
						meta.PrepareCommitTS = 0
					}
				} else {
					meta.PrepareCommitTS = meta.CommitTS
					if !dual {
						meta.CommitTS = 0
					}
				}
				forged.Mutations[0].Value = EncodeTxnMeta(meta)
				require.ErrorIs(t, ValidateRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid)
				forged.TimestampClaim, forged.TimestampIntent = nil, nil
				require.ErrorIs(t, IssueRequestTimestampClaim(t.Context(), allocator, forged), ErrTSOClaimInvalid,
					"the irrelevant slot must also be rejected before new claim issuance")
			}
		})
	}
}

func TestTSOClaimRejectsMixedParticipantCreationPhases(t *testing.T) {
	_, allocator, _ := newProductionClaimFixture(t)
	first := newClaimModeRequest(t, allocator, true, pb.Phase_PREPARE)
	second := proto.CloneOf(first)
	second.Phase, second.TimestampClaimGroupId = pb.Phase_NONE, 2
	_, err := bindClaimToParticipantRequests(t.Context(), allocator, []*pb.Request{first, second}, &OperationGroup[OP]{})
	require.ErrorIs(t, err, ErrTSOClaimInvalid, "all participants must share the signed initial mode")
}
