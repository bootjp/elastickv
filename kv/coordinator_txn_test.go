package kv

import (
	"context"
	"encoding/binary"
	"testing"

	pb "github.com/bootjp/elastickv/proto"
	"github.com/stretchr/testify/require"
)

type stubTransactional struct {
	commits int
	reqs    [][]*pb.Request
}

func (s *stubTransactional) Commit(_ context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	s.commits++
	s.reqs = append(s.reqs, reqs)
	return &TransactionResponse{}, nil
}

func (s *stubTransactional) Abort(_ context.Context, _ []*pb.Request) (*TransactionResponse, error) {
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

	_, err := c.dispatchTxn(context.Background(), []*Elem[OP]{
		{Op: Put, Key: []byte("k"), Value: []byte("v")},
	}, nil, startTS, 0, 0, 0)
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

	_, err := c.dispatchTxn(context.Background(), []*Elem[OP]{
		{Op: Put, Key: nil, Value: []byte("v")},
	}, nil, 1, 0, 0, 0)
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
	_, err := c.dispatchTxn(context.Background(), []*Elem[OP]{
		{Op: Put, Key: []byte("b"), Value: []byte("v1")},
		{Op: Del, Key: []byte("x")},
	}, nil, startTS, 0, 0, 0)
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
	_, err := c.dispatchTxn(context.Background(), []*Elem[OP]{
		{Op: Put, Key: []byte("k"), Value: []byte("v")},
	}, nil, startTS, commitTS, 0, 0)
	require.NoError(t, err)
	require.Len(t, tx.reqs, 1)
	require.Len(t, tx.reqs[0], 1)

	meta, err := DecodeTxnMeta(tx.reqs[0][0].Mutations[0].Value)
	require.NoError(t, err)
	require.Equal(t, commitTS, meta.CommitTS)
}

func TestCoordinateDispatchTxn_StampsCommitTSValueOffsetWithoutMutatingElem(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	c := &Coordinate{
		transactionManager: tx,
		clock:              NewHLC(),
	}

	startTS := uint64(10)
	commitTS := uint64(25)
	value := make([]byte, 16)
	resp, err := c.dispatchTxn(context.Background(), []*Elem[OP]{
		{Op: Put, Key: []byte("k"), Value: value, CommitTSValueOffset: 4},
	}, nil, startTS, commitTS, 0, 0)
	require.NoError(t, err)
	require.Equal(t, commitTS, resp.CommitTS)
	require.Len(t, tx.reqs, 1)

	got := tx.reqs[0][0].Mutations[1].Value
	require.Equal(t, commitTS, binary.BigEndian.Uint64(got[4:12]))
	require.Zero(t, binary.BigEndian.Uint64(value[4:12]))
}

func TestCoordinateDispatchTxn_PassesReadKeysToRaftEntry(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	c := &Coordinate{
		transactionManager: tx,
		clock:              NewHLC(),
	}

	readKeys := [][]byte{[]byte("rk1"), []byte("rk2")}
	_, err := c.dispatchTxn(context.Background(), []*Elem[OP]{
		{Op: Put, Key: []byte("k"), Value: []byte("v")},
	}, readKeys, 10, 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, tx.reqs, 1)
	require.Len(t, tx.reqs[0], 1)
	require.Equal(t, readKeys, tx.reqs[0][0].ReadKeys)
}

// ReadKeys inclusion in single-shard Raft entries is tested in
// TestShardedCoordinatorDispatchTxn_SingleShardIncludesReadKeysInRaftEntry.

// TestCoordinateDispatchTxn_PassesObservedRouteVersionToRaftEntry verifies
// that ObservedRouteVersion set on the OperationGroup at Dispatch survives
// through the dispatchTxn boundary into pb.Request.ObservedRouteVersion.
//
// This is the M1 round-trip witness for the Composed-1 plumbing per
// docs/design/2026_05_29_implemented_composed1_cross_group_commit_guard.md
// §M1: at this milestone the FSM ignores the field, so the only assertion
// is that the value survives the encoder.  M3 will add the FSM apply-time
// gate that *uses* this value.
func TestCoordinateDispatchTxn_PassesObservedRouteVersionToRaftEntry(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	c := &Coordinate{
		transactionManager: tx,
		clock:              NewHLC(),
	}

	const pinnedVer = uint64(42)
	_, err := c.dispatchTxn(context.Background(), []*Elem[OP]{
		{Op: Put, Key: []byte("k"), Value: []byte("v")},
	}, nil, 10, 25, 0, pinnedVer)
	require.NoError(t, err)
	require.Len(t, tx.reqs, 1)
	require.Len(t, tx.reqs[0], 1)
	require.Equal(t, pinnedVer, tx.reqs[0][0].ObservedRouteVersion,
		"the ObservedRouteVersion value supplied to dispatchTxn must round-trip into the pb.Request the FSM will apply")
}

// TestCoordinateDispatchTxn_UnpinnedObservedRouteVersionStaysZero documents
// the behaviour-neutral default: every existing caller pre-M1 passed no
// ObservedRouteVersion, so dispatchTxn invoked with 0 must produce
// pb.Request.ObservedRouteVersion == 0.  The M3 FSM gate will treat zero
// as "unpinned" and skip the Composed-1 check, so this is the
// legacy-compatible default the rest of the codebase relies on.
func TestCoordinateDispatchTxn_UnpinnedObservedRouteVersionStaysZero(t *testing.T) {
	t.Parallel()

	tx := &stubTransactional{}
	c := &Coordinate{
		transactionManager: tx,
		clock:              NewHLC(),
	}

	_, err := c.dispatchTxn(context.Background(), []*Elem[OP]{
		{Op: Put, Key: []byte("k"), Value: []byte("v")},
	}, nil, 10, 25, 0, 0)
	require.NoError(t, err)
	require.Len(t, tx.reqs, 1)
	require.Len(t, tx.reqs[0], 1)
	require.Equal(t, uint64(0), tx.reqs[0][0].ObservedRouteVersion,
		"dispatchTxn with observedRouteVersion=0 must produce pb.Request.ObservedRouteVersion == 0 for legacy-compatible behaviour")
}
