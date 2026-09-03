package kv

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/keyviz"
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
	onCommit  func(call int, req *pb.Request)
}

func (s *recordingTransactional) Commit(_ context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(reqs) != 1 {
		return nil, errors.New("unexpected request batch size")
	}
	s.requests = append(s.requests, cloneTxnRequest(reqs[0]))
	call := len(s.requests) - 1
	if s.onCommit != nil {
		s.onCommit(call, s.requests[call])
	}
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

func TestShardedCoordinatorValidateReadKeysOnShard_UsesStagedVisibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  []byte("a"),
				End:                    []byte("z"),
				GroupID:                1,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
		},
	}))
	st := store.NewMVCCStore()
	readKey := []byte("k")
	require.NoError(t, st.PutAt(ctx, distribution.MigrationStagedDataKey(9, readKey), []byte("staged"), 20, 0))
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Engine: stubLeaderEngine{}, Store: st},
	}, 1, NewHLC(), nil)

	err := coord.validateReadKeysOnShard(ctx, 1, [][]byte{readKey}, 10)
	require.ErrorIs(t, err, store.ErrWriteConflict)
}

func TestShardedCoordinatorDispatchTxn_AddsStagedReadKeyAlias(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  []byte("a"),
				End:                    []byte("z"),
				GroupID:                1,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
		},
	}))
	txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	readKey := []byte("k")
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  10,
		CommitTS: 101,
		Elems:    []*Elem[OP]{{Op: Put, Key: []byte("m"), Value: []byte("write")}},
		ReadKeys: [][]byte{readKey},
	})
	require.NoError(t, err)
	require.Len(t, txn.requests, 1)
	require.Equal(t, [][]byte{
		readKey,
		distribution.MigrationStagedDataKey(9, readKey),
		distribution.MigrationStagedDataKey(9, []byte("m")),
	}, txn.requests[0].ReadKeys)
}

func TestShardedCoordinatorDispatchTxn_AddsStagedWriteKeyAlias(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  []byte("a"),
				End:                    []byte("z"),
				GroupID:                1,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
		},
	}))
	txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: txn},
	}, 1, NewHLC(), nil)

	writeKey := []byte("k")
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  10,
		CommitTS: 101,
		Elems:    []*Elem[OP]{{Op: Put, Key: writeKey, Value: []byte("write")}},
	})
	require.NoError(t, err)
	require.Len(t, txn.requests, 1)
	require.Equal(t, [][]byte{
		distribution.MigrationStagedDataKey(9, writeKey),
	}, txn.requests[0].ReadKeys)
}

func TestShardedCoordinatorPrewrite_AddsStagedWriteKeyAlias(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{
				RouteID:                1,
				Start:                  []byte("a"),
				End:                    []byte("m"),
				GroupID:                1,
				State:                  distribution.RouteStateActive,
				StagedVisibilityActive: true,
				MigrationJobID:         9,
			},
			{RouteID: 2, Start: []byte("m"), End: []byte("z"), GroupID: 2, State: distribution.RouteStateActive},
		},
	}))
	g1Txn := &recordingTransactional{}
	g2Txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)

	writeKey := []byte("b")
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  10,
		CommitTS: 101,
		Elems: []*Elem[OP]{
			{Op: Put, Key: writeKey, Value: []byte("write-b")},
			{Op: Put, Key: []byte("x"), Value: []byte("write-x")},
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, g1Txn.requests)
	require.NotEmpty(t, g2Txn.requests)
	require.Equal(t, [][]byte{
		distribution.MigrationStagedDataKey(9, writeKey),
	}, g1Txn.requests[0].ReadKeys)
	require.Empty(t, g2Txn.requests[0].ReadKeys)
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

func TestShardedCoordinatorGroupMutationsUsesExplicitElemGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {},
		2: {},
	}, 1, NewHLC(), nil)

	grouped, gids, err := coord.groupMutations([]*Elem[OP]{
		{Op: Del, Key: []byte("a-key"), GroupID: 2},
	}, keyviz.Label(""))
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, gids)
	require.Len(t, grouped[2], 1)
	require.Equal(t, []byte("a-key"), grouped[2][0].Key)
}

func TestShardedCoordinatorDispatchTxn_CommitPrimaryUsesPinnedGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 1)

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
	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:   true,
		StartTS: startTS,
		Elems: []*Elem[OP]{
			{Op: Del, Key: []byte("a-key"), GroupID: 2},
			{Op: Put, Key: []byte("z-key"), Value: []byte("v"), GroupID: 1},
		},
	})
	require.NoError(t, err)
	require.Len(t, g1Txn.requests, 2)
	require.Len(t, g2Txn.requests, 2)

	g1Commit := g1Txn.requests[1]
	g2Commit := g2Txn.requests[1]
	require.Equal(t, [][]byte{[]byte("z-key")}, g1Txn.requests[0].WriteFenceBypassKeys)
	require.Equal(t, [][]byte{[]byte("z-key")}, g1Commit.WriteFenceBypassKeys)
	require.Equal(t, [][]byte{[]byte("a-key")}, g2Txn.requests[0].WriteFenceBypassKeys)
	require.Equal(t, [][]byte{[]byte("a-key")}, g2Commit.WriteFenceBypassKeys)
	require.Equal(t, pb.Phase_COMMIT, g1Commit.Phase)
	require.Equal(t, pb.Phase_COMMIT, g2Commit.Phase)
	require.Equal(t, []byte("z-key"), g1Commit.Mutations[1].Key)
	require.Equal(t, pb.Op_PUT, g1Commit.Mutations[1].Op)
	require.Equal(t, []byte("a-key"), g2Commit.Mutations[1].Key)
	require.Equal(t, pb.Op_PUT, g2Commit.Mutations[1].Op)

	primaryCommitMeta := requestTxnMeta(t, g2Commit)
	require.Equal(t, []byte("a-key"), primaryCommitMeta.PrimaryKey)
	require.Greater(t, primaryCommitMeta.CommitTS, startTS)
}

func TestShardedCoordinatorPinnedWritesBypassLogicalRouteFence(t *testing.T) {
	t.Parallel()

	for _, isTxn := range []bool{false, true} {
		t.Run(map[bool]string{false: "raw", true: "txn"}[isTxn], func(t *testing.T) {
			t.Parallel()

			engine := distribution.NewEngine()
			require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
				Version: 1,
				Routes: []distribution.RouteDescriptor{
					{RouteID: 1, Start: []byte(""), End: []byte("m"), GroupID: 1, State: distribution.RouteStateActive},
					{RouteID: 2, Start: []byte("m"), End: nil, GroupID: 2, State: distribution.RouteStateWriteFenced},
				},
			}))

			g1Txn := &recordingTransactional{}
			coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
				1: {Txn: g1Txn},
				2: {Txn: &recordingTransactional{}},
			}, 1, NewHLC(), nil)
			key := []byte("z-key")
			reqs := &OperationGroup[OP]{
				IsTxn: isTxn,
				Elems: []*Elem[OP]{{Op: Del, Key: key, GroupID: 1}},
			}
			if isTxn {
				reqs.StartTS = 10
			}

			_, err := coord.Dispatch(context.Background(), reqs)
			require.NoError(t, err)
			require.Len(t, g1Txn.requests, 1)
			require.Equal(t, [][]byte{key}, g1Txn.requests[0].WriteFenceBypassKeys)
			require.Equal(t, key, g1Txn.requests[0].Mutations[len(g1Txn.requests[0].Mutations)-1].Key)
		})
	}
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

func TestShardedCoordinatorDispatchNonTxn_RejectsRouteWriteTimestampFloor(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			{RouteID: 1, Start: []byte(""), GroupID: 1, State: distribution.RouteStateActive, MinWriteTSExclusive: ^uint64(0)},
		},
	}))

	g1Txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v")},
		},
	})
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.Empty(t, g1Txn.requests)
}

func TestNewShardedCoordinatorCopiesGroupMap(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	group := &ShardGroup{}
	groups := map[uint64]*ShardGroup{1: group}

	coord := NewShardedCoordinator(engine, groups, 1, NewHLC(), nil)
	delete(groups, 1)
	groups[2] = &ShardGroup{}

	require.Same(t, group, coord.groups[1])
	_, ok := coord.groups[2]
	require.False(t, ok)
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

func TestShardedCoordinatorDispatchTxn_PhaseDRejectsInvalidCallerCommitTS(t *testing.T) {
	t.Parallel()

	coord, g1Txn, g2Txn, alloc := newPhaseDCrossShardCoordinator(t, ErrTSOTimestampInvalid)
	readTimestamp := ReadTimestamp{timestamp: 99, voucher: newAppliedReadDispatchVoucher()}
	_, err := DispatchWithReadTimestamp(readTimestamp.WithDispatchVoucher(context.Background()), coord, &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  readTimestamp.Timestamp(),
		CommitTS: 101,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("b"), Value: []byte("v1")},
			{Op: Put, Key: []byte("x"), Value: []byte("v2")},
		},
	})

	require.ErrorIs(t, err, ErrTSOTimestampInvalid)
	require.Equal(t, uint64(1), alloc.validateCalls.Load())
	require.Equal(t, uint64(101), alloc.validated.Load())
	require.Zero(t, coord.Clock().Current(), "invalid caller CommitTS must be rejected before HLC Observe")
	require.Empty(t, g1Txn.requests)
	require.Empty(t, g2Txn.requests)
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

func TestDispatchWithReadTimestampUsesDistinctRefsForOverlappingDispatches(t *testing.T) {
	t.Parallel()

	coord := newOverlappingReadVoucherCoordinator()
	readTimestamp := ReadTimestamp{
		timestamp: 10,
		voucher:   newAppliedReadDispatchVoucher(),
	}
	ctx := readTimestamp.WithDispatchVoucher(context.Background())
	request := func() *OperationGroup[OP] {
		return &OperationGroup[OP]{
			IsTxn:   true,
			StartTS: readTimestamp.Timestamp(),
			Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("b"), Value: []byte("v1")},
				{Op: Put, Key: []byte("x"), Value: []byte("v2")},
			},
		}
	}

	firstErr := make(chan error, 1)
	go func() {
		_, err := DispatchWithReadTimestamp(ctx, coord, request())
		firstErr <- err
		close(coord.firstDone)
	}()
	requireChannelClosed(t, coord.firstEntered)

	secondErr := make(chan error, 1)
	go func() {
		_, err := DispatchWithReadTimestamp(ctx, coord, request())
		secondErr <- err
	}()
	requireChannelClosed(t, coord.secondEntered)

	close(coord.releaseFirst)
	require.NoError(t, <-firstErr)
	require.NoError(t, <-secondErr)
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

type overlappingReadVoucherCoordinator struct {
	mu            sync.Mutex
	clock         *HLC
	vouchers      map[appliedReadVoucherKey]uint64
	dispatchCalls int
	firstEntered  chan struct{}
	secondEntered chan struct{}
	releaseFirst  chan struct{}
	firstDone     chan struct{}
}

func newOverlappingReadVoucherCoordinator() *overlappingReadVoucherCoordinator {
	return &overlappingReadVoucherCoordinator{
		clock:         NewHLC(),
		vouchers:      make(map[appliedReadVoucherKey]uint64),
		firstEntered:  make(chan struct{}),
		secondEntered: make(chan struct{}),
		releaseFirst:  make(chan struct{}),
		firstDone:     make(chan struct{}),
	}
}

func (c *overlappingReadVoucherCoordinator) Dispatch(ctx context.Context, req *OperationGroup[OP]) (*CoordinateResponse, error) {
	ref, ok := appliedReadTimestampVoucherRefFromContext(ctx, req.StartTS)
	if !ok {
		return nil, errors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD)
	}
	c.mu.Lock()
	c.dispatchCalls++
	call := c.dispatchCalls
	c.mu.Unlock()
	switch call {
	case 1:
		close(c.firstEntered)
		<-c.releaseFirst
	case 2:
		close(c.secondEntered)
		<-c.firstDone
	default:
		return nil, errors.New("unexpected dispatch")
	}
	if !c.consumeAppliedReadTimestampVoucher(req.StartTS, ref) {
		return nil, errors.Join(ErrTSOTimestampInvalid, ErrTSOTimestampPrePhaseD)
	}
	return &CoordinateResponse{}, nil
}

func (c *overlappingReadVoucherCoordinator) VouchAppliedReadTimestamp(timestamp uint64, ref AppliedReadTimestampVoucherRef) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.vouchers[appliedReadVoucherKey{timestamp: timestamp, ref: ref}]++
	return nil
}

func (c *overlappingReadVoucherCoordinator) RevokeAppliedReadTimestamp(timestamp uint64, ref AppliedReadTimestampVoucherRef) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := appliedReadVoucherKey{timestamp: timestamp, ref: ref}
	uses := c.vouchers[key]
	if uses <= 1 {
		delete(c.vouchers, key)
		return
	}
	c.vouchers[key] = uses - 1
}

func (c *overlappingReadVoucherCoordinator) consumeAppliedReadTimestampVoucher(timestamp uint64, ref AppliedReadTimestampVoucherRef) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := appliedReadVoucherKey{timestamp: timestamp, ref: ref}
	uses := c.vouchers[key]
	if uses == 0 {
		return false
	}
	if uses == 1 {
		delete(c.vouchers, key)
		return true
	}
	c.vouchers[key] = uses - 1
	return true
}

func (c *overlappingReadVoucherCoordinator) IsLeader() bool { return true }

func (c *overlappingReadVoucherCoordinator) VerifyLeader(context.Context) error { return nil }

func (c *overlappingReadVoucherCoordinator) LinearizableRead(context.Context) (uint64, error) {
	return 0, nil
}

func (c *overlappingReadVoucherCoordinator) RaftLeader() string { return "" }

func (c *overlappingReadVoucherCoordinator) IsLeaderForKey([]byte) bool { return true }

func (c *overlappingReadVoucherCoordinator) VerifyLeaderForKey(context.Context, []byte) error {
	return nil
}

func (c *overlappingReadVoucherCoordinator) RaftLeaderForKey([]byte) string { return "" }

func (c *overlappingReadVoucherCoordinator) Clock() *HLC { return c.clock }

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

func TestShardedCoordinatorDispatchTxn_RejectsMigrationTimestampFloor(t *testing.T) {
	t.Parallel()

	g1Txn := &recordingTransactional{}
	coord := NewShardedCoordinator(newMigrationFloorEngine(t, 100), map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
	}, 1, NewHLC(), nil)

	_, err := coord.Dispatch(context.Background(), &OperationGroup[OP]{
		IsTxn:    true,
		StartTS:  90,
		CommitTS: 100,
		Elems: []*Elem[OP]{
			{Op: Put, Key: []byte("z"), Value: []byte("v")},
		},
	})
	require.ErrorIs(t, err, ErrRouteWriteTimestampTooLow)
	require.Empty(t, g1Txn.requests, "coordinator must reject before preparing a floor-violating txn")
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

func TestGroupReadKeysByShardID_RoutesS3BucketAuxiliaryToStagedOwner(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-a"
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  s3BucketAuxiliaryStagedRoutes(bucket, 1, 2),
	}))
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {},
		2: {},
	}, 1, NewHLC(), nil)

	key := s3keys.BucketMetaKey(bucket)
	grouped, err := coord.groupReadKeysByShardID([][]byte{key})
	require.NoError(t, err)
	require.Empty(t, grouped[1])
	require.Equal(t, [][]byte{
		key,
		distribution.MigrationStagedDataKey(9, key),
	}, grouped[2])
}

func TestGroupReadKeysByShardID_RoutesS3BucketAuxiliaryToPromotedOwner(t *testing.T) {
	t.Parallel()

	const bucket = "bucket-a"
	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes:  s3BucketAuxiliaryPromotedRoutes(),
	}))
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {},
		2: {},
	}, 1, NewHLC(), nil)

	key := s3keys.BucketMetaKey(bucket)
	grouped, err := coord.groupReadKeysByShardID([][]byte{key})
	require.NoError(t, err)
	require.Empty(t, grouped[1])
	require.Equal(t, [][]byte{key}, grouped[2])
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
func (noopEngine) SnapshotEvery() uint64 { return 10_000 }
func (noopEngine) Close() error          { return nil }

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

func TestShardedCoordinatorCommitPrimaryUsesPinnedMutationGroup(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte(""), nil, 2)

	g1Txn := &recordingTransactional{responses: []*TransactionResponse{{CommitIndex: 7}}}
	g2Txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 2, NewHLC(), nil)

	primaryKey := []byte("!lst|meta|d|pinned")
	grouped := map[uint64][]*pb.Mutation{
		1: {{Op: pb.Op_DEL, Key: primaryKey}},
		2: {{Op: pb.Op_PUT, Key: []byte("z"), Value: []byte("v")}},
	}
	primaryGid, commitIndex, err := coord.commitPrimaryTxn(context.Background(), 10, primaryKey, grouped, []uint64{1, 2}, 20, 0, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), primaryGid)
	require.Equal(t, uint64(7), commitIndex)
	require.Len(t, g1Txn.requests, 1)
	require.Empty(t, g2Txn.requests)
	require.Equal(t, pb.Phase_COMMIT, g1Txn.requests[0].Phase)
	require.Len(t, g1Txn.requests[0].Mutations, 2)
	require.Equal(t, primaryKey, g1Txn.requests[0].Mutations[1].Key)
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

// In partition-resolved keyspaces such as HT-FIFO SQS, routeKey collapses a
// concrete partition key onto the global SQS route, so that route's write floor
// is not the key's floor. rejectWriteFencedPointKey already exempts these keys;
// the timestamp-floor precheck must match, or it rejects writes the fence
// precheck deliberately lets through.
func TestShardedCoordinatorFloorPrecheckSkipsResolverOwnedKeys(t *testing.T) {
	t.Parallel()

	const partitionKey = "!sqs|msg|data|p|queue|7"

	engine := distribution.NewEngine()
	require.NoError(t, engine.ApplySnapshot(distribution.CatalogSnapshot{
		Version: 1,
		Routes: []distribution.RouteDescriptor{
			// The route routeKey() collapses partition keys onto, carrying a floor.
			{RouteID: 1, Start: []byte(""), End: nil, GroupID: 1, State: distribution.RouteStateActive, MinWriteTSExclusive: 100},
		},
	}))
	c := NewShardedCoordinator(engine, map[uint64]*ShardGroup{1: {}}, 1, NewHLC(), nil)

	// Without a resolver the floor applies, which is what makes the exemption
	// below meaningful rather than vacuous.
	require.ErrorIs(t,
		c.rejectWriteTimestampFloorPointKey([]byte(partitionKey), 100),
		ErrRouteWriteTimestampTooLow)

	c.WithPartitionResolver(&fakePartitionResolver{
		routes:           map[string]uint64{partitionKey: 1},
		recognisedPrefix: []byte("!sqs|msg|data|p|"),
	})

	require.NoError(t,
		c.rejectWriteTimestampFloorPointKey([]byte(partitionKey), 100),
		"a resolver-owned key must not be judged by the route routeKey collapses it onto")

	// A key the resolver does not own still gets the floor.
	require.ErrorIs(t,
		c.rejectWriteTimestampFloorPointKey([]byte("ordinary-key"), 100),
		ErrRouteWriteTimestampTooLow)
}
