package kv

import (
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type stubTransactional struct {
	commits int
}

func (s *stubTransactional) Commit(_ []*pb.Request) (*TransactionResponse, error) {
	s.commits++
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
	}, startTS)
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
	}, 1)
	require.ErrorIs(t, err, ErrTxnPrimaryKeyRequired)
	require.Equal(t, 0, tx.commits)
}
