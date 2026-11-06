package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// blockingPrepareTransactional parks the first PREPARE it receives until
// release is closed, without holding its request lock, so a second
// transaction can run while the first one is still between phases.
type blockingPrepareTransactional struct {
	mu       sync.Mutex
	requests []*pb.Request
	blocked  bool
	entered  chan struct{}
	release  chan struct{}
}

func newBlockingPrepareTransactional() *blockingPrepareTransactional {
	return &blockingPrepareTransactional{entered: make(chan struct{}), release: make(chan struct{})}
}

func (s *blockingPrepareTransactional) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	s.mu.Lock()
	for _, req := range reqs {
		s.requests = append(s.requests, cloneTxnRequest(req))
	}
	block := !s.blocked && len(reqs) == 1 && reqs[0].Phase == pb.Phase_PREPARE
	if block {
		s.blocked = true
	}
	s.mu.Unlock()
	if block {
		close(s.entered)
		select {
		case <-s.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &TransactionResponse{CommitIndex: 1}, nil
}

func (s *blockingPrepareTransactional) Abort(context.Context, []*pb.Request) (*TransactionResponse, error) {
	return &TransactionResponse{}, nil
}

func (s *blockingPrepareTransactional) phaseCount(phase pb.Phase) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, req := range s.requests {
		if req.Phase == phase {
			n++
		}
	}
	return n
}

// Two distinct cross-shard transactions that pick the same primary key and
// read the same snapshot share the FSM transaction identity
// (primaryKey, startTS). The FSM treats a second PREPARE with that identity
// as an idempotent replay and overwrites the first transaction's intent, and
// the loser's COMMIT then fails with ErrTxnInvalidMeta, which the client sees
// as an ambiguous outcome (EIO through FUSE). The coordinator must instead
// reject the colliding transaction with a definitive write conflict before it
// prepares anything, so the caller can retry at a fresh snapshot.
func TestShardedCoordinatorDispatchTxn_RejectsConcurrentSameIdentity(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := newBlockingPrepareTransactional()
	g2Txn := &recordingTransactional{}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)

	const startTS = uint64(10)
	request := func(value byte) *OperationGroup[OP] {
		return &OperationGroup[OP]{
			IsTxn:   true,
			StartTS: startTS,
			Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("b"), Value: []byte{value}},
				{Op: Put, Key: []byte("x"), Value: []byte{value}},
			},
		}
	}

	var releaseOnce sync.Once
	releaseFirst := func() { releaseOnce.Do(func() { close(g1Txn.release) }) }
	t.Cleanup(releaseFirst)

	firstErr := make(chan error, 1)
	go func() {
		_, err := coord.Dispatch(context.Background(), request(1))
		firstErr <- err
	}()
	select {
	case <-g1Txn.entered:
	case err := <-firstErr:
		t.Fatalf("first dispatch returned before PREPARE: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("first PREPARE did not start")
	}

	_, err := coord.Dispatch(context.Background(), request(2))
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.NotErrorIs(t, err, ErrTransactionOutcomeUnknown)

	releaseFirst()
	require.NoError(t, <-firstErr)
	require.Equal(t, 1, g1Txn.phaseCount(pb.Phase_PREPARE),
		"the colliding transaction must not prepare under the in-flight identity")

	// Once the first transaction finished, the identity is free again.
	_, err = coord.Dispatch(context.Background(), request(3))
	require.NoError(t, err)
}

func TestShardedCoordinatorDispatchTxn_RetainsIdentityAfterUncertainPrepare(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{}
	g2Txn := &recordingTransactional{errs: []error{context.DeadlineExceeded, context.DeadlineExceeded}}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)
	request := func(startTS uint64) *OperationGroup[OP] {
		return &OperationGroup[OP]{
			IsTxn:   true,
			StartTS: startTS,
			Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("b"), Value: []byte("first")},
				{Op: Put, Key: []byte("x"), Value: []byte("first")},
			},
		}
	}

	_, err := coord.Dispatch(context.Background(), request(10))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, g2Txn.requests, 1)

	_, err = coord.Dispatch(context.Background(), request(10))
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.Len(t, g2Txn.requests, 1, "an unresolved identity must not prepare again")

	_, err = coord.Dispatch(context.Background(), request(11))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Len(t, g2Txn.requests, 2, "a fresh snapshot must remain available")
}

func TestShardedCoordinatorDispatchTxn_RetainsIdentityAfterFailedAbort(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{errs: []error{nil, context.DeadlineExceeded}}
	g2Txn := &failingTransactional{err: store.NewWriteConflictError([]byte("x"))}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)
	request := func(startTS uint64) *OperationGroup[OP] {
		return &OperationGroup[OP]{
			IsTxn:   true,
			StartTS: startTS,
			Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("b"), Value: []byte("value")},
				{Op: Put, Key: []byte("x"), Value: []byte("value")},
			},
		}
	}

	_, err := coord.Dispatch(context.Background(), request(10))
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.Len(t, g1Txn.requests, 2, "the first PREPARE and failed ABORT must be recorded")

	_, err = coord.Dispatch(context.Background(), request(10))
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.Len(t, g1Txn.requests, 2, "the unresolved lock must block reuse of its identity")

	_, err = coord.Dispatch(context.Background(), request(11))
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.Len(t, g1Txn.requests, 4, "a fresh snapshot must still reach PREPARE")
}

func TestShardedCoordinatorDispatchTxn_ReleasesIdentityAfterSuccessfulAbort(t *testing.T) {
	t.Parallel()

	engine := distribution.NewEngine()
	engine.UpdateRoute([]byte("a"), []byte("m"), 1)
	engine.UpdateRoute([]byte("m"), nil, 2)

	g1Txn := &recordingTransactional{}
	g2Txn := &failingTransactional{err: store.NewWriteConflictError([]byte("x"))}
	coord := NewShardedCoordinator(engine, map[uint64]*ShardGroup{
		1: {Txn: g1Txn},
		2: {Txn: g2Txn},
	}, 1, NewHLC(), nil)
	request := func() *OperationGroup[OP] {
		return &OperationGroup[OP]{
			IsTxn:   true,
			StartTS: 10,
			Elems: []*Elem[OP]{
				{Op: Put, Key: []byte("b"), Value: []byte("value")},
				{Op: Put, Key: []byte("x"), Value: []byte("value")},
			},
		}
	}

	_, err := coord.Dispatch(context.Background(), request())
	require.ErrorIs(t, err, store.ErrWriteConflict)
	_, err = coord.Dispatch(context.Background(), request())
	require.ErrorIs(t, err, store.ErrWriteConflict)
	require.Len(t, g1Txn.requests, 4, "a definitive rejection and completed cleanup must release the identity")
}
