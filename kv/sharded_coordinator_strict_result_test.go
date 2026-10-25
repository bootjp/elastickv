package kv

import (
	"context"
	"errors"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

func TestCommitSecondaryTxnsRequiresEveryAcknowledgement(t *testing.T) {
	t.Parallel()

	failure := errors.New("secondary unavailable")
	secondary := &scriptedTransactional{errs: map[uint64]error{0: failure, 1: failure, 2: failure}}
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{
		2: {Txn: secondary},
	}}
	grouped := map[uint64][]*pb.Mutation{2: {{Key: []byte("secondary")}}}
	_, err := coord.commitSecondaryTxns(context.Background(), 10, 1, []byte("primary"), grouped,
		[]uint64{1, 2}, 20, 7, 0, nil, true)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, failure)
	require.EqualValues(t, txnSecondaryCommitRetryAttempts, secondary.commits.Load())
}

func TestCommitSecondaryTxnsRequiresPresentParticipant(t *testing.T) {
	t.Parallel()
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{}}
	_, err := coord.commitSecondaryTxns(context.Background(), 10, 1, []byte("primary"), nil,
		[]uint64{1, 2}, 20, 7, 0, nil, true)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
}

func TestCommitSecondaryTxnsRequiresNonNilDecision(t *testing.T) {
	t.Parallel()
	coord := &ShardedCoordinator{groups: map[uint64]*ShardGroup{2: {Txn: nilDecisionTransactional{}}}}
	_, err := coord.commitSecondaryTxns(context.Background(), 10, 1, []byte("primary"),
		map[uint64][]*pb.Mutation{2: {{Key: []byte("secondary")}}},
		[]uint64{1, 2}, 20, 7, 0, nil, true)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
}

type nilDecisionTransactional struct{}

func (nilDecisionTransactional) Commit(context.Context, []*pb.Request) (*TransactionResponse, error) {
	return nil, nil
}

func (nilDecisionTransactional) Abort(context.Context, []*pb.Request) (*TransactionResponse, error) {
	return nil, nil
}

func TestStrictSecondaryRouteShiftNeverStartsANewTransaction(t *testing.T) {
	t.Parallel()
	router := distribution.NewEngine()
	router.UpdateRoute([]byte("a"), []byte("m"), 1)
	router.UpdateRoute([]byte("m"), nil, 2)
	primary := &recordingTransactional{}
	secondaryErrors := []error{nil}
	for range txnSecondaryCommitRetryAttempts {
		secondaryErrors = append(secondaryErrors, ErrComposed1Violation)
	}
	secondary := &recordingTransactional{errs: secondaryErrors}
	coord := NewShardedCoordinator(router, map[uint64]*ShardGroup{
		1: {Txn: primary}, 2: {Txn: secondary},
	}, 1, NewHLC(), nil)
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn: true, RequireAllParticipants: true,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("record")},
			{Op: Put, Key: []byte("x"), Value: []byte("effect")},
		},
	})
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.Len(t, primary.requests, 2)
	require.Len(t, secondary.requests, 1+txnSecondaryCommitRetryAttempts)
}
