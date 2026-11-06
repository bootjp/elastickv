package kv

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bootjp/elastickv/internal/monoclock"
	"github.com/bootjp/elastickv/internal/raftengine"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type operationHandleKey struct{}

func TestOperationAdmissionResultBoundary(t *testing.T) {
	t.Parallel()
	revoked := errors.New("ownership revoked")
	for _, original := range []error{nil, ErrTransactionOutcomeUnknown} {
		t.Run("finish retains outcome", func(t *testing.T) {
			finished := 0
			hook := func(ctx context.Context, _ uint64, _ AdmissionPurpose) (context.Context, func(error) error, error) {
				return ctx, func(err error) error {
					finished++
					require.Equal(t, original, err)
					return revoked
				}, nil
			}
			result, err := admittedOperation(context.Background(), hook, 1, AdmissionData, func(context.Context) (uint64, error) {
				return 42, original
			})
			require.Zero(t, result)
			require.ErrorIs(t, err, revoked)
			require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
			if original != nil {
				require.ErrorIs(t, err, original)
			}
			require.Equal(t, 1, finished)
		})
	}
	result, err := admittedOperation(context.Background(), nil, 1, AdmissionData, func(context.Context) (uint64, error) {
		return 42, ErrTransactionOutcomeUnknown
	})
	require.Equal(t, uint64(42), result)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
}

func TestOperationAdmissionRejectsBeforeWork(t *testing.T) {
	t.Parallel()
	denied := errors.New("data not ready")
	hook := func(context.Context, uint64, AdmissionPurpose) (context.Context, func(error) error, error) {
		return nil, nil, denied
	}
	c := NewCoordinatorWithEngine(nil, nil, WithOperationAdmission(hook))
	_, err := c.Dispatch(context.Background(), &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("k")}}})
	require.ErrorIs(t, err, denied)
	_, err = c.Next(context.Background())
	require.ErrorIs(t, err, denied)
	require.ErrorIs(t, c.ProposeHLCLease(context.Background(), 1), denied)
	sc := &ShardedCoordinator{operationAdmission: hook}
	_, err = sc.Dispatch(context.Background(), &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("k")}}})
	require.ErrorIs(t, err, denied)
	_, err = sc.NextAfter(context.Background(), 1)
	require.ErrorIs(t, err, denied)
	lr := &LockResolver{operationAdmission: hook}
	require.ErrorIs(t, lr.resolveGroupLocks(context.Background(), 2, nil), denied)
	compactor := NewFSMCompactor(nil, WithFSMCompactorOperationAdmission(hook))
	require.ErrorIs(t, compactor.compactRuntime(context.Background(), FSMCompactRuntime{GroupID: 2}), denied)
}

func TestOperationAdmissionIncompleteOwnership(t *testing.T) {
	t.Parallel()
	for _, nilContext := range []bool{false, true} {
		t.Run("incomplete ownership", func(t *testing.T) {
			finished := false
			hook := func(ctx context.Context, _ uint64, _ AdmissionPurpose) (context.Context, func(error) error, error) {
				if nilContext {
					return nil, func(err error) error { finished = true; return err }, nil
				}
				return ctx, nil, nil
			}
			_, err := admittedOperation(context.Background(), hook, 1, AdmissionData, func(context.Context) (uint64, error) {
				t.Fatal("incomplete ownership reached work")
				return 0, nil
			})
			require.Error(t, err)
			require.Equal(t, nilContext, finished)
		})
	}
}

func TestOperationAdmissionFinishOnPartialEntryAndPanic(t *testing.T) {
	t.Parallel()
	entryErr := errors.New("partial entry failed")
	for _, failEntry := range []bool{false, true} {
		t.Run("owner release", func(t *testing.T) {
			finished := 0
			hook := func(ctx context.Context, _ uint64, _ AdmissionPurpose) (context.Context, func(error) error, error) {
				finish := func(err error) error {
					finished++
					require.Error(t, err)
					return err
				}
				if failEntry {
					return ctx, finish, entryErr
				}
				return ctx, finish, nil
			}
			run := func() {
				_, err := admittedOperation(context.Background(), hook, 1, AdmissionData, func(context.Context) (uint64, error) {
					panic("operation panic")
				})
				require.ErrorIs(t, err, entryErr)
			}
			if failEntry {
				run()
			} else {
				require.Panics(t, run)
			}
			require.Equal(t, 1, finished)
		})
	}
}

type operationContextTxn struct {
	scriptedTransactional
	t *testing.T
}

func (tx *operationContextTxn) Commit(ctx context.Context, reqs []*pb.Request) (*TransactionResponse, error) {
	tx.t.Helper()
	require.Equal(tx.t, true, ctx.Value(operationHandleKey{}))
	return tx.scriptedTransactional.Commit(ctx, reqs)
}

func TestOperationAdmissionDataContextReachesTransaction(t *testing.T) {
	t.Parallel()
	hook := func(ctx context.Context, _ uint64, _ AdmissionPurpose) (context.Context, func(error) error, error) {
		return context.WithValue(ctx, operationHandleKey{}, true), func(err error) error { return err }, nil
	}
	txm := &operationContextTxn{t: t}
	c := NewCoordinatorWithEngine(txm, newStubLeaderEngine(), WithOperationAdmission(hook))
	_, err := c.Dispatch(context.Background(), &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("k")}}})
	require.NoError(t, err)
	txn := &leaseRefreshingTxn{inner: txm, g: &ShardGroup{}, coord: &ShardedCoordinator{operationAdmission: hook}}
	_, err = txn.Commit(context.Background(), nil)
	require.NoError(t, err)
}

type operationLockScanStore struct {
	store.MVCCStore
	t       *testing.T
	started chan struct{}
}

func (s *operationLockScanStore) ScanAt(ctx context.Context, _, _ []byte, _ int, _ uint64) ([]*store.KVPair, error) {
	s.t.Helper()
	require.Equal(s.t, true, ctx.Value(operationHandleKey{}))
	close(s.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestOperationAdmissionLockScanCancellationFinishes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, finished, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	hook := func(ctx context.Context, _ uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		require.Equal(t, AdmissionMaintenance, purpose)
		return context.WithValue(ctx, operationHandleKey{}, true), func(err error) error {
			require.ErrorIs(t, err, context.Canceled)
			close(finished)
			return err
		}, nil
	}
	lr := NewLockResolver(nil, nil, nil, WithLockResolverContext(ctx), WithLockResolverOperationAdmission(hook))
	t.Cleanup(lr.Close)
	group := &ShardGroup{Engine: newStubLeaderEngine(), Store: &operationLockScanStore{t: t, started: started}}
	go func() {
		defer close(done)
		err := lr.resolveGroupLocks(ctx, 7, group)
		require.ErrorIs(t, err, context.Canceled)
	}()
	requireOperationSignal(t, started)
	cancel()
	requireOperationSignal(t, done)
	requireOperationSignal(t, finished)
}

func TestOperationAdmissionDispatchRetryInheritsHandle(t *testing.T) {
	t.Parallel()
	var minted, entered, finished int
	handle := new(int)
	hook := func(ctx context.Context, group uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		require.Equal(t, uint64(coordinateSingleGroupID), group)
		require.Contains(t, []AdmissionPurpose{AdmissionData, AdmissionAllocator}, purpose)
		entered++
		if ctx.Value(operationHandleKey{}) == nil {
			minted++
			ctx = context.WithValue(ctx, operationHandleKey{}, handle)
		}
		require.Same(t, handle, ctx.Value(operationHandleKey{}))
		return ctx, func(err error) error { finished++; return err }, nil
	}
	txm := &scriptedTransactional{errs: map[uint64]error{0: raftengine.ErrNotLeader}}
	c := NewCoordinatorWithEngine(txm, newStubLeaderEngine(), WithOperationAdmission(hook))
	resp, err := c.Dispatch(context.Background(), &OperationGroup[OP]{Elems: []*Elem[OP]{{Op: Put, Key: []byte("k")}}})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, uint64(2), txm.commits.Load())
	require.Equal(t, 1, minted)
	require.Equal(t, entered, finished)
}

func TestOperationAdmissionAllocatorContextAndResult(t *testing.T) {
	t.Parallel()
	revoked := errors.New("allocator revoked")
	for _, sharded := range []bool{false, true} {
		t.Run("allocator", func(t *testing.T) {
			hook := func(ctx context.Context, _ uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
				require.Equal(t, AdmissionAllocator, purpose)
				return context.WithValue(ctx, operationHandleKey{}, true), func(error) error { return revoked }, nil
			}
			alloc := operationContextAllocator{t: t}
			var next func(context.Context) (uint64, error)
			if sharded {
				c := &ShardedCoordinator{tsAllocator: alloc, operationAdmission: hook}
				next = c.Next
			} else {
				c := NewCoordinatorWithEngine(nil, nil, WithTSOAllocator(alloc), WithOperationAdmission(hook))
				next = c.Next
			}
			ts, err := next(context.Background())
			require.Zero(t, ts)
			require.ErrorIs(t, err, revoked)
		})
	}
}

type operationContextAllocator struct{ t *testing.T }

func (a operationContextAllocator) Next(ctx context.Context) (uint64, error) {
	a.t.Helper()
	require.Equal(a.t, true, ctx.Value(operationHandleKey{}))
	return 42, nil
}

func TestOperationAdmissionHLCContextAndLeaseValidation(t *testing.T) {
	t.Parallel()
	revoked := errors.New("hlc revoked")
	engine := &fakeLeaseEngine{leaseDur: time.Hour}
	engine.proposeCtxHook = func(ctx context.Context) {
		require.Equal(t, true, ctx.Value(operationHandleKey{}))
	}
	hook := func(ctx context.Context, group uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		require.Equal(t, AdmissionHLC, purpose)
		require.Contains(t, []uint64{coordinateSingleGroupID, 7}, group)
		return context.WithValue(ctx, operationHandleKey{}, true), func(error) error { return revoked }, nil
	}
	c := NewCoordinatorWithEngine(nil, engine, WithOperationAdmission(hook))
	t.Cleanup(func() { _ = c.Close() })
	require.ErrorIs(t, c.ProposeHLCLease(context.Background(), 42), revoked)
	require.False(t, c.lease.valid(monoclock.Now()))
	group := &ShardGroup{Engine: engine}
	sc := &ShardedCoordinator{operationAdmission: hook}
	require.ErrorIs(t, sc.proposeHLCLeaseForGroup(context.Background(), 7, group, 42), revoked)
	require.False(t, group.lease.valid(monoclock.Now()))
}

func TestOperationAdmissionHLCLoopsJoinChildren(t *testing.T) {
	t.Parallel()
	for _, sharded := range []bool{false, true} {
		t.Run("join", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started, release, finished, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var calls atomic.Uint64
			engine := newStubLeaderEngine()
			engine.ProposeFunc = func(ctx context.Context, _ []byte) (*raftengine.ProposalResult, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-ctx.Done()
				<-release
				return nil, ctx.Err()
			}
			hook := func(ctx context.Context, _ uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
				require.Equal(t, AdmissionHLC, purpose)
				return ctx, func(err error) error {
					select {
					case <-finished:
					default:
						close(finished)
					}
					return err
				}, nil
			}
			var run func(context.Context)
			if sharded {
				sc := &ShardedCoordinator{groups: map[uint64]*ShardGroup{7: {Engine: engine}}, operationAdmission: hook}
				run = sc.RunHLCLeaseRenewal
			} else {
				c := NewCoordinatorWithEngine(nil, engine, WithOperationAdmission(hook))
				run = c.RunHLCLeaseRenewal
			}
			go func() { run(ctx); close(done) }()
			requireOperationSignal(t, started)
			cancel()
			select {
			case <-done:
				t.Error("outer loop returned before child finished")
			case <-time.After(20 * time.Millisecond):
			}
			close(release)
			requireOperationSignal(t, done)
			requireOperationSignal(t, finished)
		})
	}
}

func requireOperationSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not signal completion")
	}
}

type operationCompactionStore struct {
	deadlineCapturingStore
	compact func(context.Context) error
}

func (s *operationCompactionStore) Compact(ctx context.Context, _ uint64) error {
	return s.compact(ctx)
}

func TestOperationAdmissionCompactionJoinsBeforeFinish(t *testing.T) {
	t.Parallel()
	revoked := errors.New("maintenance revoked")
	workDone := false
	st := &operationCompactionStore{
		deadlineCapturingStore: deadlineCapturingStore{lastCommitTS: 42},
		compact: func(ctx context.Context) error {
			require.Equal(t, true, ctx.Value(operationHandleKey{}))
			workDone = true
			return ErrTransactionOutcomeUnknown
		},
	}
	hook := func(ctx context.Context, group uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		require.Equal(t, uint64(7), group)
		require.Equal(t, AdmissionMaintenance, purpose)
		return context.WithValue(ctx, operationHandleKey{}, true), func(err error) error {
			require.True(t, workDone)
			require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
			return revoked
		}, nil
	}
	c := NewFSMCompactor([]FSMCompactRuntime{{GroupID: 7, StatusReader: fakeRaftStatus{}, Store: st}}, WithFSMCompactorOperationAdmission(hook))
	err := c.SyncOnce(context.Background())
	require.ErrorIs(t, err, revoked)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
}

func TestOperationAdmissionLockResolverRootCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	lr := NewLockResolver(nil, nil, slog.Default(), WithLockResolverContext(ctx))
	cancel()
	select {
	case <-lr.done:
	case <-time.After(time.Second):
		t.Fatal("root cancellation did not stop resolver")
	}
	lr.Close()
}

func TestOperationAdmissionShardCommitAndAbort(t *testing.T) {
	t.Parallel()
	revoked := errors.New("data revoked")
	var entered, finished int
	hook := func(ctx context.Context, group uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		require.Equal(t, uint64(7), group)
		require.Equal(t, AdmissionData, purpose)
		entered++
		return ctx, func(error) error { finished++; return revoked }, nil
	}
	txm := &scriptedTransactional{}
	txn := &leaseRefreshingTxn{inner: txm, g: &ShardGroup{}, groupID: 7, coord: &ShardedCoordinator{operationAdmission: hook}}
	for _, run := range []func(context.Context, []*pb.Request) (*TransactionResponse, error){txn.Commit, txn.Abort} {
		resp, err := run(context.Background(), nil)
		require.Nil(t, resp)
		require.ErrorIs(t, err, revoked)
	}
	require.Equal(t, 2, entered)
	require.Equal(t, entered, finished)
}

func TestOperationAdmissionSecondaryPreservesUnknown(t *testing.T) {
	t.Parallel()
	txm := &scriptedTransactional{errs: map[uint64]error{0: ErrTransactionOutcomeUnknown}}
	_, err := commitSecondaryWithRetry(context.Background(), &ShardGroup{Txn: txm}, &pb.Request{}, true)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	require.Equal(t, uint64(1), txm.commits.Load())
	revoked := errors.New("secondary admission revoked")
	c := &ShardedCoordinator{operationAdmission: func(context.Context, uint64, AdmissionPurpose) (context.Context, func(error) error, error) {
		return nil, nil, revoked
	}}
	err = c.secondaryCommitFailure(revoked, 1, 10, 20, []byte("k"), false)
	require.ErrorIs(t, err, revoked)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
	err = c.combineBroadcastErrors(revoked, ErrTransactionOutcomeUnknown)
	require.ErrorIs(t, err, revoked)
	require.ErrorIs(t, err, ErrTransactionOutcomeUnknown)
}

var _ store.MVCCStore = (*operationCompactionStore)(nil)

type operationCompactReceiver interface {
	Compact(context.Context, uint64) error
	SetOperationAdmission(OperationAdmission)
}

type operationDirectCompactStore struct {
	store.MVCCStore
	run func(context.Context, uint64) error
}

func (s *operationDirectCompactStore) Compact(ctx context.Context, minTS uint64) error {
	return s.run(ctx, minTS)
}

type operationCompactFactory struct {
	name  string
	group uint64
	new   func(store.MVCCStore) operationCompactReceiver
}

func operationCompactFactories() []operationCompactFactory {
	return []operationCompactFactory{
		{name: "sharded", group: 7, new: func(st store.MVCCStore) operationCompactReceiver {
			return NewShardStore(nil, map[uint64]*ShardGroup{7: {Store: st}})
		}},
		{name: "leader_routed", group: coordinateSingleGroupID, new: func(st store.MVCCStore) operationCompactReceiver {
			return NewLeaderRoutedStore(st, nil)
		}},
	}
}

func TestOperationAdmissionDirectCompactBoundaries(t *testing.T) {
	t.Parallel()
	denied, revoked := errors.New("data not ready"), errors.New("ownership revoked")
	diskErr := errors.New("compact disk failure")
	scenarios := []operationDirectCompactCase{
		{name: "success", finishes: 1},
		{name: "denied", entryErr: denied, want: []error{denied}},
		{name: "io_error", ioErr: diskErr, want: []error{diskErr}, finishes: 1},
		{name: "finish_error", finishErr: revoked, want: []error{revoked}, finishes: 1},
		{name: "unknown", ioErr: ErrTransactionOutcomeUnknown, finishErr: revoked, want: []error{ErrTransactionOutcomeUnknown, revoked}, finishes: 1},
		{name: "panic", panics: true, finishes: 1},
	}
	for _, factory := range operationCompactFactories() {
		t.Run(factory.name, func(t *testing.T) {
			for _, scenario := range scenarios {
				t.Run(scenario.name, func(t *testing.T) { testOperationDirectCompactBoundary(t, factory, scenario) })
			}
		})
	}
}

type operationDirectCompactCase struct {
	name      string
	entryErr  error
	ioErr     error
	finishErr error
	want      []error
	panics    bool
	finishes  int
}

func testOperationDirectCompactBoundary(t *testing.T, factory operationCompactFactory, scenario operationDirectCompactCase) {
	t.Helper()
	called, finished := false, 0
	handle := new(int)
	ctx := context.WithValue(context.Background(), operationHandleKey{}, handle)
	st := &operationDirectCompactStore{run: func(owned context.Context, minTS uint64) error {
		called = true
		require.Same(t, handle, owned.Value(operationHandleKey{}))
		require.Equal(t, uint64(42), minTS)
		if scenario.panics {
			panic("compact panic")
		}
		return scenario.ioErr
	}}
	receiver := factory.new(st)
	receiver.SetOperationAdmission(func(input context.Context, group uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		require.Same(t, ctx, input)
		require.Equal(t, factory.group, group)
		require.Equal(t, AdmissionMaintenance, purpose)
		require.False(t, called, "DataReady admission must precede disk I/O")
		if scenario.entryErr != nil {
			return nil, nil, scenario.entryErr
		}
		return input, func(err error) error {
			finished++
			require.True(t, called)
			if scenario.panics {
				require.Error(t, err)
			} else {
				require.Equal(t, scenario.ioErr, err)
			}
			return scenario.finishErr
		}, nil
	})
	if scenario.panics {
		require.Panics(t, func() { _ = receiver.Compact(ctx, 42) })
		require.Equal(t, 1, finished)
		return
	}
	err := receiver.Compact(ctx, 42)
	for _, want := range scenario.want {
		require.ErrorIs(t, err, want)
	}
	if len(scenario.want) == 0 {
		require.NoError(t, err)
	}
	require.Equal(t, scenario.entryErr == nil, called)
	require.Equal(t, scenario.finishes, finished)
}

type operationCompactGroupKey struct{}

func TestOperationAdmissionDirectCompactGroupsInheritInput(t *testing.T) {
	t.Parallel()
	handle := new(int)
	ctx := context.WithValue(context.Background(), operationHandleKey{}, handle)
	started, finished := map[uint64]int{}, map[uint64]int{}
	groups := map[uint64]*ShardGroup{0: nil, 3: {}}
	for _, gid := range []uint64{2, 7} {
		groups[gid] = &ShardGroup{Store: &operationDirectCompactStore{run: func(owned context.Context, minTS uint64) error {
			require.Same(t, handle, owned.Value(operationHandleKey{}))
			require.Equal(t, gid, owned.Value(operationCompactGroupKey{}))
			require.Equal(t, uint64(42), minTS)
			started[gid]++
			return nil
		}}}
	}
	receiver := NewShardStore(nil, groups)
	receiver.SetOperationAdmission(func(input context.Context, gid uint64, purpose AdmissionPurpose) (context.Context, func(error) error, error) {
		require.Same(t, ctx, input)
		require.Nil(t, input.Value(operationCompactGroupKey{}), "must not reuse another group's attempt context")
		require.Equal(t, AdmissionMaintenance, purpose)
		return context.WithValue(input, operationCompactGroupKey{}, gid), func(err error) error {
			finished[gid]++
			require.Equal(t, 1, started[gid])
			return err
		}, nil
	})
	require.NoError(t, receiver.Compact(ctx, 42))
	require.Equal(t, map[uint64]int{2: 1, 7: 1}, started)
	require.Equal(t, started, finished)
}

func TestOperationAdmissionDirectCompactLegacy(t *testing.T) {
	t.Parallel()
	for _, factory := range operationCompactFactories() {
		t.Run(factory.name, func(t *testing.T) {
			called := false
			input := context.WithValue(context.Background(), operationHandleKey{}, new(int))
			st := &operationDirectCompactStore{run: func(ctx context.Context, minTS uint64) error {
				called = true
				require.Same(t, input, ctx)
				require.Equal(t, uint64(42), minTS)
				return ErrTransactionOutcomeUnknown
			}}
			require.ErrorIs(t, factory.new(st).Compact(input, 42), ErrTransactionOutcomeUnknown)
			require.True(t, called)
		})
	}
	var absent *LeaderRoutedStore
	require.ErrorIs(t, absent.Compact(context.Background(), 42), store.ErrNotSupported)
	require.ErrorIs(t, NewLeaderRoutedStore(nil, nil).Compact(context.Background(), 42), store.ErrNotSupported)
}

func TestOperationAdmissionDirectCompactWaitsForIO(t *testing.T) {
	t.Parallel()
	for _, factory := range operationCompactFactories() {
		t.Run(factory.name, func(t *testing.T) { testOperationDirectCompactWaitsForIO(t, factory) })
	}
}

func testOperationDirectCompactWaitsForIO(t *testing.T, factory operationCompactFactory) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ioDone, finished, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	st := &operationDirectCompactStore{run: func(owned context.Context, _ uint64) error {
		close(started)
		<-owned.Done()
		close(cancelled)
		<-release
		close(ioDone)
		return owned.Err()
	}}
	receiver := factory.new(st)
	receiver.SetOperationAdmission(func(input context.Context, _ uint64, _ AdmissionPurpose) (context.Context, func(error) error, error) {
		return input, func(err error) error {
			requireOperationSignal(t, ioDone)
			close(finished)
			return err
		}, nil
	})
	go func() { done <- receiver.Compact(ctx, 42) }()
	requireOperationSignal(t, started)
	cancel()
	requireOperationSignal(t, cancelled)
	select {
	case <-finished:
		t.Error("finish released owner before disk I/O returned")
	default:
	}
	close(release)
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("compaction did not finish")
	}
	requireOperationSignal(t, finished)
}
