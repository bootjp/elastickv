package adapter

import (
	"strconv"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func releasedAdmissionStore(t *testing.T, floor uint64) store.MVCCStore {
	t.Helper()
	st := store.NewMVCCStore()
	fsm := kv.NewKvFSMWithHLC(st, kv.NewHLC())
	require.Nil(t, fsm.Apply(kv.EncodePrepareEvidenceFence(floor)))
	require.Nil(t, fsm.Apply(kv.EncodePrepareEvidenceSeal()))
	require.Nil(t, fsm.Apply(kv.EncodePrepareEvidenceRelease(floor)))
	return st
}

func TestInternalProofPrepareRetainsOldReadSnapshotAfterGraceWindow(t *testing.T) {
	t.Parallel()
	const afterGraceTS uint64 = (26 * 60 * 60 * 1000) << kv.HLCLogicalBits
	for _, commitTS := range []uint64{60, 150} {
		t.Run(strconv.FormatUint(commitTS, 10), func(t *testing.T) {
			alloc := &phaseDForwardAllocator{floor: 100, ceiling: 200, next: afterGraceTS}
			nextBefore := alloc.next
			txn := &forwardObserverTxn{}
			internal := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil,
				WithInternalTimestampAllocator(alloc), WithInternalStore(releasedAdmissionStore(t, 100)))
			_, err := internal.Forward(t.Context(), &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{{
				IsTxn: true, Phase: pb.Phase_PREPARE, Ts: 50, Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("primary"), PrepareCommitTS: commitTS})},
					{Op: pb.Op_PUT, Key: []byte("primary"), Value: []byte("value")},
				},
			}}})
			if commitTS < 100 {
				require.ErrorIs(t, err, kv.ErrTSOTimestampPrePhaseD)
				require.Empty(t, txn.reqs)
			} else {
				require.NoError(t, err)
				require.Len(t, txn.reqs, 1)
				require.Equal(t, uint64(50), txn.reqs[0].Ts)
				require.Equal(t, uint64(100), txn.reqs[0].GetTimestampAdmissionFloor())
			}
			require.Equal(t, nextBefore, alloc.next, "a new proven commit does not need the legacy creation window")
		})
	}
}

type laggingAdmissionAllocator struct{ phaseDForwardAllocator }

func (*laggingAdmissionAllocator) PhaseDActive() bool   { return false }
func (*laggingAdmissionAllocator) PhaseDRequired() bool { return false }

func TestInternalAdmissionDoesNotTrustStaleModeOrIncomingEpoch(t *testing.T) {
	t.Parallel()
	for _, timestamp := range []uint64{120, 150} {
		t.Run(strconv.FormatUint(timestamp, 10), func(t *testing.T) {
			allocator := &laggingAdmissionAllocator{phaseDForwardAllocator{floor: 100, ceiling: 140}}
			txn := &forwardObserverTxn{}
			internal := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil,
				WithInternalTimestampAllocator(allocator), WithInternalStore(releasedAdmissionStore(t, 100)))
			_, err := internal.Forward(t.Context(), &pb.ForwardRequest{Requests: []*pb.Request{{
				Ts: timestamp, TimestampAdmissionFloor: 100,
				Mutations: []*pb.Mutation{{Op: pb.Op_PUT, Key: []byte("key"), Value: []byte("value")}},
			}}})
			if timestamp > 140 {
				require.ErrorIs(t, err, kv.ErrTSOTimestampInvalid)
				require.Empty(t, txn.reqs)
				return
			}
			require.NoError(t, err)
			require.Greater(t, allocator.validateCalls, 0)
			require.Equal(t, uint64(100), txn.reqs[0].GetTimestampAdmissionFloor())
		})
	}
}
