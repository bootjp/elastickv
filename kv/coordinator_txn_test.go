package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type stubTransactional struct {
	commits int
	reqs    [][]*pb.Request
}

func (s *stubTransactional) Commit(reqs []*pb.Request) (*TransactionResponse, error) {
	s.commits++
	s.reqs = append(s.reqs, reqs)
	return &TransactionResponse{}, nil
}

func (s *stubTransactional) Abort(_ []*pb.Request) (*TransactionResponse, error) {
	return &TransactionResponse{}, nil
}

func TestCoordinateDispatchTxn_RejectsNonMonotonicCommitTS(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	c := &Coordinate{
		transactionManager: tx,
		clock:              NewHLC(),
	}

	startTS := ^uint64(0)
	c.clock.Observe(startTS)

	_, err := c.dispatchTxn([]*Elem[OP]{
		{Op: Put, Key: []byte("k"), Value: []byte("v")},
	}, startTS, 0)
	require.ErrorIs(t, err, ErrTxnCommitTSRequired)
	require.Equal(t, 0, tx.commits)
}

func TestCoordinateDispatchTxn_RejectsMissingPrimaryKey(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	c := &Coordinate{
		transactionManager: tx,
		clock:              NewHLC(),
	}

	_, err := c.dispatchTxn([]*Elem[OP]{
		{Op: Put, Key: nil, Value: []byte("v")},
	}, 1, 0)
	require.ErrorIs(t, err, ErrTxnPrimaryKeyRequired)
	require.Equal(t, 0, tx.commits)
}

func TestCoordinateDispatchTxn_UsesOnePhaseRequest(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	c := &Coordinate{
		transactionManager: tx,
		clock:              NewHLC(),
	}

	startTS := uint64(10)
	_, err := c.dispatchTxn([]*Elem[OP]{
		{Op: Put, Key: []byte("b"), Value: []byte("v1")},
		{Op: Del, Key: []byte("x")},
	}, startTS, 0)
	require.NoError(t, err)
	require.Equal(t, 1, tx.commits)
	require.Len(t, tx.reqs, 1)
	require.Len(t, tx.reqs[0], 1)

	req := tx.reqs[0][0]
	require.NotNil(t, req)
	require.True(t, req.IsTxn)
	require.Equal(t, pb.Phase_NONE, req.Phase)
	require.Equal(t, startTS, req.Ts)
	require.Len(t, req.Mutations, 3)
	require.Equal(t, []byte(txnMetaPrefix), req.Mutations[0].Key)
	require.Equal(t, []byte("b"), req.Mutations[1].Key)
	require.Equal(t, []byte("v1"), req.Mutations[1].Value)
	require.Equal(t, pb.Op_DEL, req.Mutations[2].Op)
	require.Equal(t, []byte("x"), req.Mutations[2].Key)

	meta, err := DecodeTxnMeta(req.Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, []byte("b"), meta.PrimaryKey)
	require.Greater(t, meta.CommitTS, startTS)
}

func TestCoordinateDispatchTxn_UsesProvidedCommitTS(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	c := &Coordinate{
		transactionManager: tx,
		clock:              NewHLC(),
	}

	startTS := uint64(10)
	commitTS := uint64(25)
	_, err := c.dispatchTxn([]*Elem[OP]{
		{Op: Put, Key: []byte("k"), Value: []byte("v")},
	}, startTS, commitTS)
	require.NoError(t, err)
	require.Len(t, tx.reqs, 1)
	require.Len(t, tx.reqs[0], 1)

	meta, err := DecodeTxnMeta(tx.reqs[0][0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, commitTS, meta.CommitTS)
}
