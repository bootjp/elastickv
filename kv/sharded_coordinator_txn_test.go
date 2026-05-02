package kv

import (
	"context"
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
