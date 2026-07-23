package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type recordingTransactional struct {
	mu        sync.Mutex
	requests  []*pb.Request
	responses []*TransactionResponse
	errs      []error
}

func (s *recordingTransactional) Commit(_ context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
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

func (s *recordingTransactional) Abort(_ context.Context, _ []*pb.Request) (*TransactionResponse, error) {
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

func TestShardedCoordinatorDelPrefixBroadcast_UsesConfiguredAllShardGroups(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	tsoTxn := &recordingTransactional{}
	dataTxn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		0: {Txn: tsoTxn},
		1: {Txn: dataTxn},
	}, 1, NewHLC(), nil).WithAllShardGroups(1)

	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: DelPrefix, Key: []byte("tenant/")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Empty(t, tsoTxn.requests)
	require.Len(t, dataTxn.requests, 1)
	require.Equal(t, pb.Op_DEL_PREFIX, dataTxn.requests[0].Mutations[0].Op)
	require.Equal(t, []byte("tenant/"), dataTxn.requests[0].Mutations[0].Key)
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
	value1 := make([]byte, 16)
	value2 := make([]byte, 16)
	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: startTS,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: value1, CommitTSValueOffset: 4},
			{Op: Put, Key: []byte("x"), Value: value2, CommitTSValueOffset: 4},
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
	require.Equal(t, []byte("x"), g2Prepare.Mutations[1].Key)
	require.Zero(t, g1Prepare.Mutations[1].CommitTsValueOffset)
	require.Zero(t, g2Prepare.Mutations[1].CommitTsValueOffset)

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
	require.Equal(t, commitMeta1.CommitTS, binary.BigEndian.Uint64(g1Prepare.Mutations[1].Value[4:12]))
	require.Equal(t, commitMeta1.CommitTS, binary.BigEndian.Uint64(g2Prepare.Mutations[1].Value[4:12]))
	require.Zero(t, g1Commit.Mutations[1].CommitTsValueOffset)
	require.Zero(t, g2Commit.Mutations[1].CommitTsValueOffset)
	require.Zero(t, binary.BigEndian.Uint64(value1[4:12]))
	require.Zero(t, binary.BigEndian.Uint64(value2[4:12]))
}

func TestShardedCoordinatorDispatchTxn_PhaseDRejectsInvalidCallerStartTSBeforeProposal(t *testing.T) {
	t.Parallel()
	coord, g1Txn, g2Txn, alloc := newPhaseDCrossShardCoordinator(t, ErrTSOTimestampInvalid)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.ErrorIs(t, err, ErrTSOTimestampInvalid)
	require.Equal(t, uint64(1), alloc.validateCalls.Load())
	require.Empty(t, g1Txn.requests)
	require.Empty(t, g2Txn.requests)
}

func TestShardedCoordinatorDispatchTxn_PhaseDAcceptsValidatedCallerStartTS(t *testing.T) {
	t.Parallel()
	coord, g1Txn, g2Txn, alloc := newPhaseDCrossShardCoordinator(t, nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 100,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), alloc.validateCalls.Load())
	require.Len(t, g1Txn.requests, 2)
	require.Len(t, g2Txn.requests, 2)
	require.Equal(t, uint64(100), g1Txn.requests[0].Ts)
	require.Equal(t, uint64(100), g2Txn.requests[0].Ts)
}

func TestShardedCoordinatorDispatchTxn_PhaseDAcceptsBoundAppliedWatermark(t *testing.T) {
	t.Parallel()
	prePhaseDErr := errors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD)
	coord, g1Txn, g2Txn, alloc := newPhaseDCrossShardCoordinator(t, prePhaseDErr)

	readTS, err := BeginReadTimestampThrough(context.Background(), coord, 10, "vouch applied watermark")
	require.NoError(t, err)
	require.Equal(t, uint64(10), readTS.Timestamp())
	require.Equal(t, uint64(1), alloc.validateCalls.Load())

	request := func() *OperationGroup[OP] {
		return &OperationGroup[OP]{
			IsTxn:   true,
			StartTS: readTS.Timestamp(),
			Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("b"), Value: []byte("v1")},
				{Op: Put, Key: []byte("x"), Value: []byte("v2")},
			},
		}
	}

	_, err = coord.Dispatch(context.Background(), request())
	require.ErrorIs(t, err, ErrTSOTimestampPrePhaseD, "a numeric timestamp without the bound capability must not steal a voucher")
	require.Equal(t, uint64(2), alloc.validateCalls.Load())
	require.Empty(t, g1Txn.requests)
	require.Empty(t, g2Txn.requests)

	ctx := readTS.WithDispatchVoucher(context.Background())
	_, err = DispatchWithReadTimestamp(ctx, coord, request())
	require.NoError(t, err)
	require.Equal(t, uint64(2), alloc.validateCalls.Load(), "bound voucher must bypass numeric Phase-D validation")
	require.Len(t, g1Txn.requests, 2)
	require.Len(t, g2Txn.requests, 2)

	_, err = coord.Dispatch(context.Background(), request())
	require.ErrorIs(t, err, ErrTSOTimestampPrePhaseD)
	require.Equal(t, uint64(3), alloc.validateCalls.Load(), "voucher must be bound and single-use")
}

func TestDispatchWithReadTimestampVouchesEveryBoundDispatch(t *testing.T) {
	t.Parallel()
	prePhaseDErr := errors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD)
	coord, g1Txn, g2Txn, alloc := newPhaseDCrossShardCoordinator(t, prePhaseDErr)

	readTimestamp, err := BeginReadTimestampThrough(context.Background(), coord, 10, "vouch reused applied watermark")
	require.NoError(t, err)
	ctx := readTimestamp.WithDispatchVoucher(context.Background())
	request := func(startTS uint64) *OperationGroup[OP] {
		return &OperationGroup[OP]{
			IsTxn:   true,
			StartTS: startTS,
			Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("b"), Value: []byte("v1")},
				{Op: Put, Key: []byte("x"), Value: []byte("v2")},
			},
		}
	}

	for range 2 {
		_, err = DispatchWithReadTimestamp(ctx, coord, request(readTimestamp.Timestamp()))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(1), alloc.validateCalls.Load(), "each dispatch must consume a reserved voucher")
	require.Len(t, g1Txn.requests, 4)
	require.Len(t, g2Txn.requests, 4)

	_, err = DispatchWithReadTimestamp(ctx, coord, request(readTimestamp.Timestamp()+1))
	require.ErrorIs(t, err, ErrTSOTimestampInvalid, "the bound capability must not authorize another timestamp")

	_, err = coord.Dispatch(context.Background(), request(readTimestamp.Timestamp()))
	require.ErrorIs(t, err, ErrTSOTimestampPrePhaseD, "no unused voucher may remain after the bound dispatches")
	require.Equal(t, uint64(2), alloc.validateCalls.Load())
}

func TestDispatchWithReadTimestampRevokesVoucherWhenOuterGateRejects(t *testing.T) {
	t.Parallel()
	prePhaseDErr := errors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD)
	coord, _, _, _ := newPhaseDCrossShardCoordinator(t, prePhaseDErr)
	gateErr := errors.New("startup gate rejected dispatch")
	gated := phaseDGateCoordinator{inner: coord, err: gateErr}

	readTimestamp, err := BeginReadTimestampThrough(context.Background(), gated, 10, "vouch gated applied watermark")
	require.NoError(t, err)
	_, err = DispatchWithReadTimestamp(
		readTimestamp.WithDispatchVoucher(context.Background()),
		gated,
		&OperationGroup[OP]{
			IsTxn:   true,
			StartTS: readTimestamp.Timestamp(),
			Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("b"), Value: []byte("v1")},
				{Op: Put, Key: []byte("x"), Value: []byte("v2")},
			},
		},
	)
	require.ErrorIs(t, err, gateErr)

	coord.appliedReadVoucherMu.Lock()
	defer coord.appliedReadVoucherMu.Unlock()
	require.Empty(t, coord.appliedReadVouchers)
}

func TestReadTimestampVoucherBindingShadowsParentCapability(t *testing.T) {
	prePhaseDErr := errors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD)
	coord, _, _, alloc := newPhaseDCrossShardCoordinator(t, prePhaseDErr)

	oldRead, err := BeginReadTimestampThrough(context.Background(), coord, 10, "vouch old applied watermark")
	require.NoError(t, err)
	ctx := oldRead.WithDispatchVoucher(context.Background())

	alloc.validateErr = nil
	currentRead, err := BeginReadTimestampThrough(ctx, coord, 100, "validate current durable watermark")
	require.NoError(t, err)
	ctx = currentRead.WithDispatchVoucher(ctx)

	_, err = DispatchWithReadTimestamp(ctx, coord, &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: currentRead.Timestamp(),
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.NoError(t, err, "the current timestamp must shadow the parent capability")
}

func TestShardedCoordinatorDispatchTxn_PhaseDPreservesSingleShardCallerStartTS(t *testing.T) {
	t.Parallel()
	coord, g1Txn, _, alloc := newPhaseDCrossShardCoordinator(t, ErrTSOTimestampInvalid)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
		},
	})
	require.NoError(t, err)
	require.Zero(t, alloc.validateCalls.Load())
	require.Len(t, g1Txn.requests, 1)
}

func TestShardedCoordinatorDispatchTxn_PrePhaseDPreservesCrossShardCallerStartTS(t *testing.T) {
	t.Parallel()
	coord, g1Txn, g2Txn, alloc := newPhaseDCrossShardCoordinator(t, ErrTSOTimestampInvalid)
	coord.WithTSOCutoverState(NewTSOStateMachine(NewHLC()))
	alloc.phaseDActive = false
	alloc.phaseDRequired = false

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.NoError(t, err)
	require.Zero(t, alloc.validateCalls.Load())
	require.Len(t, g1Txn.requests, 2)
	require.Len(t, g2Txn.requests, 2)
}

func TestShardedCoordinatorDispatchTxn_PhaseDActivationValidatesBeforeLocalMarker(t *testing.T) {
	t.Parallel()
	coord, g1Txn, g2Txn, alloc := newPhaseDCrossShardCoordinator(t, ErrTSOTimestampInvalid)
	coord.WithTSOCutoverState(NewTSOStateMachine(NewHLC()))
	alloc.phaseDActive = false

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.ErrorIs(t, err, ErrTSOTimestampInvalid)
	require.Equal(t, uint64(1), alloc.validateCalls.Load())
	require.Empty(t, g1Txn.requests)
	require.Empty(t, g2Txn.requests)
}

func newPhaseDCrossShardCoordinator(
	t *testing.T,
	validateErr error,
) (*ShardedCoordinator, *recordingTransactional, *recordingTransactional, *phaseDTestAllocator) {
	t.Helper()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	g1Txn := &recordingTransactional{}
	g2Txn := &recordingTransactional{}
	alloc := &phaseDTestAllocator{
		next:           200,
		phaseDActive:   true,
		phaseDRequired: true,
		validateErr:    validateErr,
	}
	state := NewTSOStateMachine(NewHLC())
	require.Nil(t, state.Apply(marshalTSOCutover()))
	require.Nil(t, state.Apply(marshalTSOPhaseD(0)))
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil).
		WithTSOAllocator(alloc).
		WithTSOCutoverState(state)
	return coord, g1Txn, g2Txn, alloc
}

type phaseDGateCoordinator struct {
	inner *ShardedCoordinator
	err   error
}

func (c phaseDGateCoordinator) Dispatch(context.Context, *OperationGroup[OP]) (*CoordinateResponse, error) {
	return nil, c.err
}

func (c phaseDGateCoordinator) IsLeader() bool { return c.inner.IsLeader() }

func (c phaseDGateCoordinator) VerifyLeader(ctx context.Context) error {
	return c.inner.VerifyLeader(ctx)
}

func (c phaseDGateCoordinator) LinearizableRead(ctx context.Context) (uint64, error) {
	return c.inner.LinearizableRead(ctx)
}

func (c phaseDGateCoordinator) RaftLeader() string { return c.inner.RaftLeader() }

func (c phaseDGateCoordinator) IsLeaderForKey(key []byte) bool {
	return c.inner.IsLeaderForKey(key)
}

func (c phaseDGateCoordinator) VerifyLeaderForKey(ctx context.Context, key []byte) error {
	return c.inner.VerifyLeaderForKey(ctx, key)
}

func (c phaseDGateCoordinator) RaftLeaderForKey(key []byte) string {
	return c.inner.RaftLeaderForKey(key)
}

func (c phaseDGateCoordinator) Clock() *HLC { return c.inner.Clock() }

func (c phaseDGateCoordinator) TimestampAllocator() TimestampAllocator {
	return c.inner.TimestampAllocator()
}

func (c phaseDGateCoordinator) VouchAppliedReadTimestamp(timestamp uint64, ref AppliedReadTimestampVoucherRef) error {
	return c.inner.VouchAppliedReadTimestamp(timestamp, ref)
}

func (c phaseDGateCoordinator) RevokeAppliedReadTimestamp(timestamp uint64, ref AppliedReadTimestampVoucherRef) {
	c.inner.RevokeAppliedReadTimestamp(timestamp, ref)
}

func TestShardedCoordinatorDispatchTxn_SingleShardUsesOnePhase(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	g1Txn := &recordingTransactional{
		responses: []*TransactionResponse{
			{CommitIndex: 17},
		},
	}

	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
	}, 1, NewHLC(), nil)

	startTS := uint64(10)
	resp, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: startTS,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("c"), Value: []byte("v2")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(17), resp.CommitIndex)

	require.Len(t, g1Txn.requests, 1)
	req := g1Txn.requests[0]
	require.Equal(t, pb.Phase_NONE, req.Phase)
	require.Equal(t, startTS, req.Ts)
	require.Len(t, req.Mutations, 3)
	require.Equal(t, []byte("b"), req.Mutations[1].Key)
	require.Equal(t, []byte("c"), req.Mutations[2].Key)

	meta := requestTxnMeta(t, req)
	require.Equal(t, []byte("b"), meta.PrimaryKey)
	require.Zero(t, meta.LockTTLms)
	require.Greater(t, meta.CommitTS, startTS)
}

func TestShardedCoordinatorDispatchTxn_UsesProvidedCommitTS(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{}
	g2Txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)

	startTS := uint64(10)
	commitTS := uint64(25)
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  startTS,
		CommitTS: commitTS,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.NoError(t, err)
	require.Len(t, g1Txn.requests, 2)
	require.Len(t, g2Txn.requests, 2)

	commitMeta1 := requestTxnMeta(t, g1Txn.requests[1])
	commitMeta2 := requestTxnMeta(t, g2Txn.requests[1])
	require.Equal(t, commitTS, commitMeta1.CommitTS)
	require.Equal(t, commitTS, commitMeta2.CommitTS)
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

	resp, err := commitSecondaryWithRetry(context.Background(), &ShardGroup{Txn: txn}, &pb.Request{
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

	_, err := commitSecondaryWithRetry(context.Background(), &ShardGroup{Txn: txn}, &pb.Request{
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

// ---------------------------------------------------------------------------
// groupReadKeysByShardID
// ---------------------------------------------------------------------------

func TestGroupReadKeysByShardID_NilReturnsNil(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {}}, 1, NewHLC(), nil)
	grouped, err := coord.groupReadKeysByShardID(nil)
	require.NoError(t, err)
	require.Nil(t, grouped)
}

func TestGroupReadKeysByShardID_EmptyReturnsNil(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {}}, 1, NewHLC(), nil)
	grouped, err := coord.groupReadKeysByShardID([][]byte{})
	require.NoError(t, err)
	require.Nil(t, grouped)
}

func TestGroupReadKeysByShardID_GroupsByShardID(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {}, 2: {}}, 1, NewHLC(), nil)

	grouped, err := coord.groupReadKeysByShardID([][]byte{
		[]byte("b"), // shard 1
		[]byte("c"), // shard 1
		[]byte("x"), // shard 2
	})
	require.NoError(t, err)
	require.Len(t, grouped, 2)
	require.Len(t, grouped[1], 2)
	require.Equal(t, []byte("b"), grouped[1][0])
	require.Equal(t, []byte("c"), grouped[1][1])
	require.Len(t, grouped[2], 1)
	require.Equal(t, []byte("x"), grouped[2][0])
}

// TestGroupReadKeysByShardID_FailsClosedOnUnroutable pins the
// codex round-2 P1 fix on PR #715: a read key the resolver cannot
// route (recognised-but-unresolved partition key during drift, or
// any key outside the engine's range cover) MUST surface as an
// error so the transaction aborts before any prewrite. Silently
// skipping unroutable keys would let OCC validation run with an
// incomplete read set and break SSI — a concurrent write to that
// key could commit alongside a stale read.
//
// This test was previously TestGroupReadKeysByShardID_SkipsUnroutableKeys
// and pinned the BUGGY skip-silently behaviour. Renamed and rewritten
// to pin the new fail-closed contract.
func TestGroupReadKeysByShardID_FailsClosedOnUnroutable(t *testing.T) {
	t.Parallel()
	// Only route "a"-"m" to shard 1. Keys outside this range are unroutable.
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {}}, 1, NewHLC(), nil)

	grouped, err := coord.groupReadKeysByShardID([][]byte{
		[]byte("b"),   // routable → shard 1
		[]byte("zzz"), // unroutable → MUST surface as error
	})
	require.Error(t, err,
		"unroutable read key MUST fail closed — silently skipping "+
			"would drop the key from OCC validation and break SSI")
	require.Nil(t, grouped)
	require.ErrorIs(t, err, ErrInvalidRequest)
}

// ---------------------------------------------------------------------------
// validateReadOnlyShards
// ---------------------------------------------------------------------------

// stubMVCCStore wraps a real MVCCStore to inject controlled LatestCommitTS.
type stubMVCCStore struct {
	store.MVCCStore
	latestTS  map[string]uint64
	returnErr error
}

func (s *stubMVCCStore) LatestCommitTS(_ context.Context, key []byte) (uint64, bool, error) {
	if s.returnErr != nil {
		return 0, false, s.returnErr
	}
	ts, ok := s.latestTS[string(key)]
	return ts, ok, nil
}

// noopEngine satisfies raftengine.Engine for unit tests.
// LinearizableRead returns immediately (simulates an already-up-to-date FSM).
type noopEngine struct{}

func (noopEngine) Propose(_ context.Context, _ []byte) (*raftengine.ProposalResult, error) {
	return &raftengine.ProposalResult{}, nil
}
func (e noopEngine) ProposeAdmin(ctx context.Context, data []byte) (*raftengine.ProposalResult, error) {
	return e.Propose(ctx, data)
}
func (noopEngine) State() raftengine.State                            { return raftengine.StateLeader }
func (noopEngine) Leader() raftengine.LeaderInfo                      { return raftengine.LeaderInfo{} }
func (noopEngine) VerifyLeader(_ context.Context) error               { return nil }
func (noopEngine) LinearizableRead(_ context.Context) (uint64, error) { return 0, nil }
func (noopEngine) Status() raftengine.Status                          { return raftengine.Status{} }
func (noopEngine) Configuration(_ context.Context) (raftengine.Configuration, error) {
	return raftengine.Configuration{}, nil
}
func (noopEngine) Close() error { return nil }

func TestValidateReadOnlyShards_DetectsConflictOnReadOnlyShard(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	readOnlyStore := &stubMVCCStore{latestTS: map[string]uint64{
		"x": 20, // committed at TS=20
	}}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {},
		2: {Store: readOnlyStore, Engine: noopEngine{}},
	}, 1, NewHLC(), nil)

	groupedReadKeys := map[uint64][][]byte{
		2: {[]byte("x")},
	}
	// shard 2 is read-only (not in writeGIDs), key "x" committed at 20 > startTS 10
	err := coord.validateReadOnlyShards(context.Background(), groupedReadKeys, []uint64{1}, 10)
	require.Error(t, err)
	require.ErrorIs(t, err, store.ErrWriteConflict)
}

func TestValidateReadOnlyShards_SkipsWriteShards(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	// shard 1 has a conflicting key, but it's a write shard — should be skipped
	writeStore := &stubMVCCStore{latestTS: map[string]uint64{
		"b": 20,
	}}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Store: writeStore, Engine: noopEngine{}},
		2: {},
	}, 1, NewHLC(), nil)

	groupedReadKeys := map[uint64][][]byte{
		1: {[]byte("b")}, // write shard → skipped
	}
	err := coord.validateReadOnlyShards(context.Background(), groupedReadKeys, []uint64{1}, 10)
	require.NoError(t, err)
}

func TestValidateReadOnlyShards_NoConflictWhenKeyUnchanged(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	readOnlyStore := &stubMVCCStore{latestTS: map[string]uint64{
		"x": 5, // committed at TS=5 <= startTS=10
	}}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {},
		2: {Store: readOnlyStore, Engine: noopEngine{}},
	}, 1, NewHLC(), nil)

	groupedReadKeys := map[uint64][][]byte{
		2: {[]byte("x")},
	}
	err := coord.validateReadOnlyShards(context.Background(), groupedReadKeys, []uint64{1}, 10)
	require.NoError(t, err)
}

func TestValidateReadOnlyShards_PropagatesStoreError(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	storeErr := errors.New("disk I/O error")
	readOnlyStore := &stubMVCCStore{returnErr: storeErr}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {},
		2: {Store: readOnlyStore, Engine: noopEngine{}},
	}, 1, NewHLC(), nil)

	groupedReadKeys := map[uint64][][]byte{
		2: {[]byte("x")},
	}
	err := coord.validateReadOnlyShards(context.Background(), groupedReadKeys, []uint64{1}, 10)
	require.Error(t, err)
	require.ErrorIs(t, err, storeErr)
}

func TestValidateReadOnlyShards_EmptyGroupedReadKeys(t *testing.T) {
	t.Parallel()
	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {}}, 1, NewHLC(), nil)
	err := coord.validateReadOnlyShards(context.Background(), nil, []uint64{1}, 10)
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Cross-shard: readKeys routed to PREPARE per shard
// ---------------------------------------------------------------------------

func TestShardedCoordinatorDispatchTxn_ReadKeysRoutedToPrepareByShard(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1) // shard 1: a-m
	engine.UpdateRoute([]byte("m"), nil, 2)         // shard 2: m+

	g1Txn := &recordingTransactional{}
	g2Txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: 10,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")}, // shard 1
			{Op: Put, Key: []byte("x"), Value: []byte("v2")}, // shard 2
		},
		ReadKeys: [][]byte{
			[]byte("c"), // shard 1 read key
			[]byte("y"), // shard 2 read key
		},
	})
	require.NoError(t, err)

	// PREPARE for shard 1 should have readKey "c"
	g1Prepare := g1Txn.requests[0]
	require.Equal(t, pb.Phase_PREPARE, g1Prepare.Phase)
	require.Equal(t, [][]byte{[]byte("c")}, g1Prepare.ReadKeys)

	// PREPARE for shard 2 should have readKey "y"
	g2Prepare := g2Txn.requests[0]
	require.Equal(t, pb.Phase_PREPARE, g2Prepare.Phase)
	require.Equal(t, [][]byte{[]byte("y")}, g2Prepare.ReadKeys)
}

func TestShardedCoordinatorDispatchTxn_SingleShardIncludesReadKeysInRaftEntry(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

	g1Txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  10,
		Elems:    []*Elem[OP]{{Op: Put, Key: []byte("k"), Value: []byte("v")}},
		ReadKeys: [][]byte{[]byte("rk1"), []byte("rk2")},
	})
	require.NoError(t, err)
	require.Len(t, g1Txn.requests, 1)
	// Single-shard: readKeys must be included in the Raft log entry so the
	// FSM can validate read-write conflicts atomically under applyMu,
	// eliminating the TOCTOU window that exists between the adapter's
	// pre-Raft validateReadSet call and FSM application.
	require.Equal(t, [][]byte{[]byte("rk1"), []byte("rk2")}, g1Txn.requests[0].ReadKeys)
}

// TestShardedCoordinatorDispatchTxn_CrossShardPropagatesObservedRouteVersion
// is the gemini-critical regression from PR #881.  Contract:
// every PREPARE and COMMIT envelope across the 2PC paths
// (prewriteTxn / commitPrimaryTxn / commitSecondaryTxns) must
// carry OperationGroup.ObservedRouteVersion so the M3 gate fires
// on every cross-shard txn.
//
// History: an earlier round in PR #900 (d8487672) attempted to
// drop the gate on secondary commits to avoid a "fail-closed gate
// + best-effort swallow" silent partial commit (codex P1 on
// 6202b964).  codex P1 on d8487672 (PR #900) showed that dropping
// the gate replaces one silent partial commit with another — the
// write lands on a stale owner that is no longer reachable by
// readers on the new owner.  The correct fix is to KEEP the gate
// active everywhere AND surface secondary Composed-1 errors as a
// distinct fatal sentinel
// (ErrTxnSecondaryRouteShiftedAfterPrimaryCommit) rather than
// either swallowing or dropping the gate.  See
// TestShardedCoordinator_SurfacesFatalErrorOn2PCSecondaryComposed1
// for the fatal-error contract.
//
// With the fatal-surface fix in place, this test reverts to the
// original PR #881 contract: every 2PC envelope on every shard
// carries the pinned version.
func TestShardedCoordinatorDispatchTxn_CrossShardPropagatesObservedRouteVersion(t *testing.T) {
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

	const pinnedVer = uint64(42)
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:                true,
		StartTS:              10,
		ObservedRouteVersion: pinnedVer,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})
	require.NoError(t, err)
	require.Len(t, g1Txn.requests, 2, "g1 must see PREPARE + COMMIT")
	require.Len(t, g2Txn.requests, 2, "g2 must see PREPARE + COMMIT")

	for _, req := range append(g1Txn.requests, g2Txn.requests...) {
		require.Equal(t, pinnedVer, req.ObservedRouteVersion,
			"multi-shard 2PC envelope (phase=%s) must carry "+
				"OperationGroup.ObservedRouteVersion; pre-fix this "+
				"silently dropped to 0 and bypassed the M3 Composed-1 "+
				"apply-time gate for every cross-shard txn",
			req.Phase)
	}
}
