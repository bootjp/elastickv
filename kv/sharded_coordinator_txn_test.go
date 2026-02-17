package kv

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type recordingTransactional struct {
	mu        sync.Mutex
	requests  []*pb.Request
	responses []*TransactionResponse
	errs      []error
}

func (s *recordingTransactional) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(reqs) != 1 {
		return nil, errors.New("unexpected request batch size")
	}
	s.requests = append(s.requests, cloneTxnRequest(reqs[0]))
	call := len(s.requests) - 1
	if call < len(s.errs) && s.errs[call] != nil {
		return nil, s.errs[call]
	}
	if call < len(s.responses) && s.responses[call] != nil {
		return s.responses[call], nil
	}
	return &TransactionResponse{}, nil
}

func (s *recordingTransactional) Abort(_ []*pb.Request) (*TransactionResponse, error) {
	return &TransactionResponse{}, nil
}

func cloneTxnRequest(req *pb.Request) *pb.Request {
	if req == nil {
		return nil
	}
	cloned := proto.Clone(req)
	request, ok := cloned.(*pb.Request)
	if !ok {
		return nil
	}
	return request
}

func requestTxnMeta(t *testing.T, req *pb.Request) TxnMeta {
	t.Helper()
	require.NotNil(t, req)
	require.NotEmpty(t, req.Mutations)
	require.Equal(t, []byte(txnMetaPrefix), req.Mutations[0].Key)
	meta, err := DecodeTxnMeta(req.Mutations[0].Value)
	require.NoError(t, err)
	return meta
}

func TestShardedCoordinatorDispatchTxn_RejectsMissingPrimaryKey(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{}, 0, NewHLC(), nil)
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn: true,
		Elems: []*Elem[OP]{
			{Op: Put, Key: nil, Value: []byte("v")},
		},
	})
	require.ErrorIs(t, err, ErrTxnPrimaryKeyRequired)
}

func TestShardedCoordinatorDispatchTxn_CrossShardPhasesAndCommitIndex(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{
		responses: []*TransactionResponse{
			{CommitIndex: 3},
			{CommitIndex: 11},
		},
	}
	g2Txn := &recordingTransactional{
		responses: []*TransactionResponse{
			{CommitIndex: 5},
			{CommitIndex: 27},
		},
	}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)

	startTS := uint64(10)
	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: startTS,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(27), resp.CommitIndex)

	require.Len(t, g1Txn.requests, 2)
	require.Len(t, g2Txn.requests, 2)

	g1Prepare := g1Txn.requests[0]
	g2Prepare := g2Txn.requests[0]
	g1Commit := g1Txn.requests[1]
	g2Commit := g2Txn.requests[1]

	require.Equal(t, pb.Phase_PREPARE, g1Prepare.Phase)
	require.Equal(t, pb.Phase_PREPARE, g2Prepare.Phase)
	require.Equal(t, startTS, g1Prepare.Ts)
	require.Equal(t, startTS, g2Prepare.Ts)
	require.Len(t, g1Prepare.Mutations, 2)
	require.Len(t, g2Prepare.Mutations, 2)
	require.Equal(t, []byte("b"), g1Prepare.Mutations[1].Key)
	require.Equal(t, []byte("v1"), g1Prepare.Mutations[1].Value)
	require.Equal(t, []byte("x"), g2Prepare.Mutations[1].Key)
	require.Equal(t, []byte("v2"), g2Prepare.Mutations[1].Value)

	prepareMeta1 := requestTxnMeta(t, g1Prepare)
	prepareMeta2 := requestTxnMeta(t, g2Prepare)
	require.Equal(t, []byte("b"), prepareMeta1.PrimaryKey)
	require.Equal(t, []byte("b"), prepareMeta2.PrimaryKey)
	require.Equal(t, defaultTxnLockTTLms, prepareMeta1.LockTTLms)
	require.Equal(t, defaultTxnLockTTLms, prepareMeta2.LockTTLms)
	require.Zero(t, prepareMeta1.CommitTS)
	require.Zero(t, prepareMeta2.CommitTS)

	require.Equal(t, pb.Phase_COMMIT, g1Commit.Phase)
	require.Equal(t, pb.Phase_COMMIT, g2Commit.Phase)
	require.Equal(t, startTS, g1Commit.Ts)
	require.Equal(t, startTS, g2Commit.Ts)
	require.Len(t, g1Commit.Mutations, 2)
	require.Len(t, g2Commit.Mutations, 2)
	require.Equal(t, pb.Op_PUT, g1Commit.Mutations[1].Op)
	require.Equal(t, pb.Op_PUT, g2Commit.Mutations[1].Op)
	require.Equal(t, []byte("b"), g1Commit.Mutations[1].Key)
	require.Equal(t, []byte("x"), g2Commit.Mutations[1].Key)

	commitMeta1 := requestTxnMeta(t, g1Commit)
	commitMeta2 := requestTxnMeta(t, g2Commit)
	require.Equal(t, []byte("b"), commitMeta1.PrimaryKey)
	require.Equal(t, []byte("b"), commitMeta2.PrimaryKey)
	require.Zero(t, commitMeta1.LockTTLms)
	require.Zero(t, commitMeta2.LockTTLms)
	require.Greater(t, commitMeta1.CommitTS, startTS)
	require.Equal(t, commitMeta1.CommitTS, commitMeta2.CommitTS)
}

func TestCommitSecondaryWithRetry_RetriesAndSucceeds(t *testing.T) {
	t.Parallel()

	transientErr := errors.New("transient")
	txn := &recordingTransactional{
		errs: []error{
			transientErr,
			transientErr,
		},
		responses: []*TransactionResponse{
			nil,
			nil,
			{CommitIndex: 99},
		},
	}

	resp, err := commitSecondaryWithRetry(&ShardGroup{Txn: txn}, &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    7,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("x")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(99), resp.CommitIndex)
	require.Len(t, txn.requests, txnSecondaryCommitRetryAttempts)
}

func TestCommitSecondaryWithRetry_ExhaustsRetries(t *testing.T) {
	t.Parallel()

	failErr := errors.New("always-fail")
	txn := &recordingTransactional{
		errs: []error{
			failErr,
			failErr,
			failErr,
		},
	}

	_, err := commitSecondaryWithRetry(&ShardGroup{Txn: txn}, &pb.Request{
		IsTxn: true,
		Phase: pb.Phase_COMMIT,
		Ts:    9,
		Mutations: []*pb.Mutation{
			{Op: pb.Op_PUT, Key: []byte("x")},
		},
	})
	require.Error(t, err)
	require.Len(t, txn.requests, txnSecondaryCommitRetryAttempts)
}
