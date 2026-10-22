package adapter

import (
	"context"
	"testing"

	"github.com/bootjp/elastickv/kv"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type internalDrainResolutionAllocator struct {
	phaseDForwardAllocator
	bound  uint64
	checks int
}

func (a *internalDrainResolutionAllocator) ValidateDurableTimestamp(context.Context, uint64) error {
	return kv.ErrTSOPhaseDInactive
}

func (a *internalDrainResolutionAllocator) ValidateLegacyResolutionTimestamps(_ context.Context, start, commit uint64) error {
	a.checks++
	if start == 0 || commit <= start || commit > a.bound {
		return kv.ErrTSOTimestampInvalid
	}
	return nil
}

func TestInternalPendingResolutionDrainsUnprovenPrimaryWithoutRetimestamping(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		phase    pb.Phase
		commit   uint64
		prepared bool
		wantErr  bool
	}{
		{name: "old first primary commit", phase: pb.Phase_COMMIT, commit: 60, prepared: true},
		{name: "old abort", phase: pb.Phase_ABORT, commit: 60},
		{name: "no existing intent", phase: pb.Phase_COMMIT, commit: 60, wantErr: true},
		{name: "above pending barrier", phase: pb.Phase_COMMIT, commit: 101, prepared: true, wantErr: true},
		{name: "one phase gets no pending exception", phase: pb.Phase_NONE, commit: 60, prepared: true, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			allocator := &internalDrainResolutionAllocator{bound: 100}
			txn := &forwardObserverTxn{}
			internal := NewInternalWithEngine(txn, forwardObserverLeader{}, nil, nil,
				WithInternalTimestampAllocator(allocator),
				WithInternalPrimaryCommitEvidence(primaryCommitEvidenceFunc(func(context.Context, []byte, uint64) (kv.PrimaryCommitEvidence, error) {
					return kv.PrimaryCommitEvidence{Readable: true, PrimaryPrepared: test.prepared}, nil
				})))
			response, err := internal.Forward(t.Context(), &pb.ForwardRequest{IsTxn: true, Requests: []*pb.Request{{
				IsTxn: true, Phase: test.phase, Ts: 50, Mutations: []*pb.Mutation{
					{Op: pb.Op_PUT, Key: []byte(kv.TxnMetaPrefix), Value: kv.EncodeTxnMeta(kv.TxnMeta{PrimaryKey: []byte("primary"), CommitTS: test.commit})},
					{Op: pb.Op_PUT, Key: []byte("primary")},
				},
			}}})
			if test.wantErr {
				require.Error(t, err)
				require.Empty(t, txn.reqs)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.commit, response.GetCommitTs())
			require.Equal(t, 1, allocator.checks)
			require.Len(t, txn.reqs, 1)
		})
	}
}
