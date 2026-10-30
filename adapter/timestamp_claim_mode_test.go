package adapter

import (
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestTimestampClaimPreparedProofCannotPublishOnePhase(t *testing.T) {
	for _, phase := range []pb.Phase{pb.Phase_COMMIT, pb.Phase_ABORT} {
		t.Run(phase.String(), func(t *testing.T) {
			allocator := newNativeAdapterClaimAllocator(t)
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			engine, stop := newSingleRaftForDistributionE2E(t, "claim-mode-data", kv.NewKvFSMWithHLC(st, kv.NewHLC()))
			t.Cleanup(stop)
			manager := kv.NewTransactionWithProposer(engine)
			t.Cleanup(manager.Close)
			_, err := engine.Propose(t.Context(), kv.EncodePrepareEvidenceFence(1))
			require.NoError(t, err)
			server := NewInternalWithEngine(manager, engine, nil, nil,
				WithInternalTimestampAllocator(allocator), WithInternalStore(st), WithInternalTimestampClaimGroup(1))
			start, err := allocator.Next(t.Context())
			require.NoError(t, err)
			meta := kv.TxnMeta{PrimaryKey: []byte("phase-key"), LockTTLms: uint64(time.Minute / time.Millisecond)}
			request := &pb.Request{IsTxn: true, Phase: pb.Phase_PREPARE, Ts: start, TimestampClaimGroupId: 1,
				Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(meta)},
					{Op: pb.Op_PUT, Key: meta.PrimaryKey, Value: []byte("prepared-value")},
				}}
			require.NoError(t, kv.IssueRequestTimestampClaim(t.Context(), allocator, request))
			proof, err := kv.DecodeRequestTimestampClaim(request.TimestampClaim)
			require.NoError(t, err)
			commit := proof.Proof.Record.Timestamp
			forged := proto.CloneOf(request)
			forged.Phase = pb.Phase_NONE
			meta.CommitTS = commit
			forged.Mutations[0].Value = kv.EncodeTxnMeta(meta)
			_, err = server.Forward(t.Context(), &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{forged}})
			assert.ErrorIs(t, err, kv.ErrTSOClaimInvalid, "PREPARE proof cannot authorize early 1PC through actual Forward admission")
			_, err = st.GetAt(t.Context(), meta.PrimaryKey, ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound, "the actual FSM must not publish the unprepared value")

			response, err := server.Forward(t.Context(), &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{request}})
			require.NoError(t, err, "the original PREPARE remains valid")
			require.True(t, response.Success)
			_, err = st.GetAt(t.Context(), meta.PrimaryKey, ^uint64(0))
			require.ErrorIs(t, err, store.ErrKeyNotFound, "PREPARE alone must not publish a user value")
			resolution := proto.CloneOf(request)
			resolution.Phase = phase
			resolution.Mutations = []*pb.Mutation{
				{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(meta)},
				{Op: pb.Op_PUT, Key: meta.PrimaryKey},
			}
			dual := proto.CloneOf(resolution)
			dualMeta := meta
			dualMeta.PrepareCommitTS = commit
			dual.Mutations[0].Value = kv.EncodeTxnMeta(dualMeta)
			_, err = server.Forward(t.Context(), &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{dual}})
			require.ErrorIs(t, err, kv.ErrTSOClaimInvalid, "resolution cannot carry an irrelevant PREPARE timestamp slot")
			for range 2 {
				response, err = server.Forward(t.Context(), &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{resolution}})
				require.NoError(t, err, "retained PREPARE authorizes its resolution and idempotent replay")
				require.True(t, response.Success)
			}
			got, err := st.GetAt(t.Context(), meta.PrimaryKey, ^uint64(0))
			if phase == pb.Phase_COMMIT {
				require.NoError(t, err)
				require.Equal(t, []byte("prepared-value"), got)
			} else {
				require.ErrorIs(t, err, store.ErrKeyNotFound)
			}
		})
	}
}
