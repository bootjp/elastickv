package kv

import (
	"context"
	"fmt"
	"testing"

	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type termRecordingProposer struct {
	fakeProposer
	term  uint64
	calls int
}

func (p *termRecordingProposer) ProposeIfTerm(_ context.Context, term uint64, _ []byte) (*raftengine.ProposalResult, error) {
	p.term = term
	p.calls++
	return nil, raftengine.ErrStaleAdmissionTerm
}

func TestApplyRequestsUsesTermFenceForBoundCreation(t *testing.T) {
	t.Parallel()
	request := &pb.Request{TimestampBindingOperationId: append([]byte{1}, make([]byte, 15)...),
		TimestampBindingAdmissionTerm: 7}
	proposer := &termRecordingProposer{}
	_, _, err := applyRequests(t.Context(), proposer, []*pb.Request{request}, nil)
	require.ErrorIs(t, err, raftengine.ErrStaleAdmissionTerm)
	require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.Equal(t, uint64(7), proposer.term)
	require.Equal(t, 1, proposer.calls)
	require.Zero(t, proposer.fakeProposer.calls.Load())

	request.TimestampBindingAdmissionTerm = 0
	_, _, err = applyRequests(t.Context(), proposer, []*pb.Request{request}, nil)
	require.ErrorIs(t, err, raftengine.ErrStaleAdmissionTerm)
	require.Equal(t, 1, proposer.calls)
}

func TestRawBatchKeepsBoundCreationSeparate(t *testing.T) {
	t.Parallel()
	bound := &pb.Request{TimestampBindingOperationId: append([]byte{1}, make([]byte, 15)...),
		TimestampBindingAdmissionTerm: 7}
	plain := &pb.Request{Mutations: []*pb.Mutation{{Key: []byte("plain")}}}
	for _, reqs := range [][]*pb.Request{{bound, plain}, {plain, bound}} {
		manager := &TransactionManager{rawPending: []*rawCommitItem{
			{reqs: []*pb.Request{reqs[0]}},
			{reqs: []*pb.Request{reqs[1]}},
		}}
		require.Len(t, manager.takeRawBatch(), 1)
		require.Len(t, manager.takeRawBatch(), 1)
	}
}

func TestApplyRequestsDoesNotCertifyFailedProposalAsAbort(t *testing.T) {
	t.Parallel()

	proposer := &fakeProposer{err: context.DeadlineExceeded}
	_, _, err := applyRequests(context.Background(), proposer, []*pb.Request{{
		Mutations: []*pb.Mutation{{Key: []byte("k"), Value: []byte("v")}},
	}}, nil)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.EqualValues(t, 1, proposer.calls.Load())
}

func TestUnknownTransactionFailureDoesNotExposeCause(t *testing.T) {
	t.Parallel()
	cause := errors.New("private-key: write conflict")
	proposer := &fakeProposer{err: cause}
	_, _, err := applyRequests(context.Background(), proposer, []*pb.Request{{IsTxn: true, Phase: pb.Phase_NONE}}, nil)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, cause)
	require.NotContains(t, err.Error(), "private-key")
	require.NotContains(t, fmt.Sprintf("%+v", errors.WithStack(err)), "private-key")
}

func TestApplyRequestsKeepsPreAppendLeaderRejectionsRetryable(t *testing.T) {
	t.Parallel()
	for _, leaderErr := range []error{raftengine.ErrNotLeader, raftengine.ErrLeadershipTransferInProgress} {
		proposer := &fakeProposer{err: leaderErr}
		_, _, err := applyRequests(context.Background(), proposer, []*pb.Request{{
			Mutations: []*pb.Mutation{{Key: []byte("k"), Value: []byte("v")}},
		}}, nil)
		require.ErrorIs(t, err, leaderErr)
		require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
		require.True(t, isTransientLeaderError(err))
	}
}

func TestTransactionManagerDoesNotAbortUnknownTxnOutcome(t *testing.T) {
	t.Parallel()
	for _, phase := range []pb.Phase{pb.Phase_PREPARE, pb.Phase_COMMIT} {
		t.Run(phase.String(), func(t *testing.T) {
			t.Parallel()
			proposer := &fakeProposer{err: context.DeadlineExceeded}
			tm := NewTransactionWithProposer(proposer)
			_, err := tm.Commit(context.Background(), []*pb.Request{outcomeTestTxnRequest(phase)})
			require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.EqualValues(t, 1, proposer.calls.Load(), "an uncertain proposal must not trigger ABORT")
		})
	}
}

func TestTransactionManagerDoesNotAbortPreAppendRejection(t *testing.T) {
	t.Parallel()
	for _, phase := range []pb.Phase{pb.Phase_PREPARE, pb.Phase_COMMIT} {
		t.Run(phase.String(), func(t *testing.T) {
			t.Parallel()
			proposer := &fakeProposer{err: raftengine.ErrNotLeader}
			tm := NewTransactionWithProposer(proposer)
			_, err := tm.Commit(context.Background(), []*pb.Request{outcomeTestTxnRequest(phase)})
			require.ErrorIs(t, err, raftengine.ErrNotLeader)
			require.EqualValues(t, 1, proposer.calls.Load(), "a participant rejection must not issue an unproven ABORT")
		})
	}
}

func TestTransactionManagerKeepsApplyVerdictSeparateFromLogicalOutcome(t *testing.T) {
	t.Parallel()
	cause := store.NewWriteConflictError([]byte("private-key"))
	for _, tc := range []struct {
		name       string
		phase      pb.Phase
		index      uint64
		definitive bool
	}{
		{name: "one phase applied", phase: pb.Phase_NONE, index: 7, definitive: true},
		{name: "one phase missing index", phase: pb.Phase_NONE, index: 0},
		{name: "prepare participant rejected", phase: pb.Phase_PREPARE, index: 7, definitive: true},
		{name: "commit applied", phase: pb.Phase_COMMIT, index: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			proposer := &fakeProposer{resp: &raftengine.ProposalResult{CommitIndex: tc.index, Response: cause}}
			tm := NewTransactionWithProposer(proposer)
			_, err := tm.Commit(context.Background(), []*pb.Request{outcomeTestTxnRequest(tc.phase)})
			require.ErrorIs(t, err, store.ErrWriteConflict)
			require.NotContains(t, err.Error(), "private-key")
			require.EqualValues(t, 1, proposer.calls.Load(), "uncertain outcomes must not trigger ABORT")
			var rejection *AppliedRequestRejection
			if tc.definitive {
				require.ErrorAs(t, err, &rejection)
				require.Equal(t, tc.index, rejection.CommitIndex())
				require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)
			} else {
				require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
				require.False(t, errors.As(err, &rejection))
			}
		})
	}
}

func outcomeTestTxnRequest(phase pb.Phase) *pb.Request {
	return &pb.Request{
		IsTxn: true,
		Phase: phase,
		Ts:    100,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte(TxnMetaPrefix), Value: EncodeTxnMeta(TxnMeta{PrimaryKey: []byte("k")})},
			{Op: pb.Op_PUT, Key: []byte("k"), Value: []byte("v")},
		},
	}
}
