package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const fifoAttemptMessageCount = 5

type fifoAttemptCoordinator struct {
	kv.Coordinator
	firstErr error
	requests []*kv.OperationGroup[kv.OP]
}

func (c *fifoAttemptCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.requests = append(c.requests, req)
	if len(c.requests) == 1 && c.firstErr != nil {
		return nil, c.firstErr
	}
	return c.Coordinator.Dispatch(ctx, req)
}

type fifoAttemptStore struct {
	store.MVCCStore
	outerStarts [][]byte
	outerTS     []uint64
	failGetKey  []byte
	failLimit   int
	fault       error
}

func (s *fifoAttemptStore) GetAt(ctx context.Context, key []byte, ts uint64) ([]byte, error) {
	if s.fault != nil && bytes.Equal(key, s.failGetKey) {
		return nil, s.fault
	}
	return s.MVCCStore.GetAt(ctx, key, ts)
}

func (s *fifoAttemptStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	if limit == fifoProgressPageSize {
		s.outerStarts = append(s.outerStarts, bytes.Clone(start))
		s.outerTS = append(s.outerTS, ts)
	}
	if s.fault != nil && limit == s.failLimit {
		return nil, s.fault
	}
	return s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
}

type fifoAttemptFixture struct {
	*fifoProgressFixture
	coord   *fifoAttemptCoordinator
	scanned *fifoAttemptStore
}

func newFIFOAttemptFixture(t *testing.T, partitions uint32) *fifoAttemptFixture {
	t.Helper()
	// The shared fixture uses a real MVCC store and LocalCoordinator with
	// OCCTransactions, ValidateReadKeys and AdvanceCommitTS enabled.
	f := newFIFOProgressFixture(t, partitions, fifoAttemptMessageCount, false)
	coord := &fifoAttemptCoordinator{Coordinator: f.srv.coordinator}
	scanned := &fifoAttemptStore{MVCCStore: f.st}
	f.srv.coordinator, f.srv.store = coord, scanned
	require.NotContains(t, f.ids[:fifoProgressPageSize], f.head)
	return &fifoAttemptFixture{fifoProgressFixture: f, coord: coord, scanned: scanned}
}

func (f *fifoAttemptFixture) scan(t *testing.T, visibility int64) ([]map[string]any, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return f.srv.scanAndDeliverPartition(ctx, f.meta.Name, f.meta, f.partition, f.st.LastCommitTS(),
		time.Now().Add(3*time.Second), fifoProgressPageSize, sqsReceiveOptions{
			Max: 10, VisibilityTimeout: visibility,
		})
}

func (f *fifoAttemptFixture) requirePagedSnapshot(t *testing.T, readTS uint64) {
	t.Helper()
	const expectedPages = (fifoAttemptMessageCount + fifoProgressPageSize - 1) / fifoProgressPageSize
	require.Len(t, f.scanned.outerStarts, expectedPages, "must visit the head's later outer page")
	for i, ts := range f.scanned.outerTS {
		require.Equal(t, readTS, ts)
		if i > 0 {
			require.Positive(t, bytes.Compare(f.scanned.outerStarts[i], f.scanned.outerStarts[i-1]))
		}
	}
}

func (f *fifoAttemptFixture) requireHeadAttempt(t *testing.T, req *kv.OperationGroup[kv.OP], readTS uint64) {
	t.Helper()
	require.True(t, req.IsTxn)
	require.Equal(t, readTS, req.StartTS)
	require.Contains(t, req.ReadKeys, f.visKey(f.head))
	require.Contains(t, req.ReadKeys, f.dataKey(f.head))
	require.Contains(t, req.ReadKeys, sqsMsgGroupKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, "g"))
	require.Equal(t, kv.Del, req.Elems[0].Op)
	require.Equal(t, f.visKey(f.head), req.Elems[0].Key)
	require.Equal(t, kv.Put, req.Elems[2].Op)
	require.Equal(t, f.dataKey(f.head), req.Elems[2].Key)
}

func TestSQSFIFOOwnedHeadAfterForeignSuccessorPages(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		t.Run(fmt.Sprintf("partitions_%d", partitions), func(t *testing.T) {
			f := newFIFOAttemptFixture(t, partitions)
			lock, err := encodeFifoGroupLock(&sqsFifoGroupLock{
				MessageID: f.head, AcquiredAtMs: f.visibleAt, VisibleAtMs: f.visibleAt,
			})
			require.NoError(t, err)
			lockKey := sqsMsgGroupKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, "g")
			require.NoError(t, f.st.PutAt(context.Background(), lockKey, lock, 10, 0))
			readTS := f.st.LastCommitTS()

			messages, err := f.scan(t, 60)
			require.NoError(t, err)
			require.Len(t, messages, 1, "foreign-lock skips must not suppress the later lock owner")
			f.requirePagedSnapshot(t, readTS)
			require.Len(t, f.coord.requests, 1)
			f.requireHeadAttempt(t, f.coord.requests[0], readTS)
			f.requireHeadDelivered(t, messages[0])
			for _, id := range f.ids[:len(f.ids)-1] {
				f.requireUnchanged(t, id)
			}
		})
	}
}

func TestSQSFIFOAttemptZeroVisibilityOncePerSnapshot(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		t.Run(fmt.Sprintf("partitions_%d", partitions), func(t *testing.T) {
			f := newFIFOAttemptFixture(t, partitions)
			readTS := f.st.LastCommitTS()
			messages, err := f.scan(t, 0)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			f.requirePagedSnapshot(t, readTS)
			// OCC alone could hide duplicate attempts as write conflicts.
			// Assert dispatch count as well as the returned batch length.
			require.Len(t, f.coord.requests, 1, "one group must be attempted only once across outer pages")
			f.requireHeadAttempt(t, f.coord.requests[0], readTS)
			f.requireHeadDelivered(t, messages[0])
			for _, id := range f.ids[:len(f.ids)-1] {
				f.requireUnchanged(t, id)
			}
		})
	}
}

func TestSQSFIFOAttemptRaceRetriesOnFreshReceive(t *testing.T) {
	for _, race := range []struct {
		name string
		err  error
	}{
		{name: "write_conflict", err: store.ErrWriteConflict},
		{name: "txn_locked", err: kv.ErrTxnLocked},
	} {
		t.Run(race.name, func(t *testing.T) {
			for _, partitions := range []uint32{1, 4} {
				t.Run(fmt.Sprintf("partitions_%d", partitions), func(t *testing.T) {
					runFIFOAttemptRace(t, partitions, race.err)
				})
			}
		})
	}
}

func runFIFOAttemptRace(t *testing.T, partitions uint32, race error) {
	t.Helper()
	f := newFIFOAttemptFixture(t, partitions)
	f.coord.firstErr = race
	readTS := f.st.LastCommitTS()
	messages, err := f.scan(t, 60)
	require.NoError(t, err)
	require.Empty(t, messages, "an expected first-head race permits a short receive")
	f.requirePagedSnapshot(t, readTS)
	require.Len(t, f.coord.requests, 1, "later candidates must not retry a failed head in this snapshot")
	f.requireHeadAttempt(t, f.coord.requests[0], readTS)
	for _, id := range f.ids {
		f.requireUnchanged(t, id)
	}
	lock, err := f.srv.loadFifoGroupLock(context.Background(), f.meta.Name, f.meta, f.partition, f.meta.Generation, "g", readTS)
	require.NoError(t, err)
	require.Nil(t, lock)
	require.Equal(t, readTS, f.st.LastCommitTS())
	f.scanned.outerStarts, f.scanned.outerTS = nil, nil

	// No intervening send, write or changed snapshot: only the receive's
	// local attempted set is fresh, so the same head must be retried.
	messages, err = f.scan(t, 60)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	f.requirePagedSnapshot(t, readTS)
	require.Len(t, f.coord.requests, 2)
	f.requireHeadAttempt(t, f.coord.requests[1], readTS)
	f.requireHeadDelivered(t, messages[0])
}

func TestSQSFIFOAttemptDispatchErrorsPropagate(t *testing.T) {
	for _, fault := range []struct {
		name string
		err  error
	}{
		{name: "operational", err: errors.New("delivery coordinator unavailable")},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "route_fence", err: kv.ErrRouteWriteFenced},
		{name: "unknown_outcome", err: kv.ErrTransactionOutcomeUnknown},
		{name: "fence_with_race", err: errors.Join(kv.ErrRouteWriteFenced, kv.ErrTxnLocked)},
		{name: "unknown_with_race", err: errors.Join(kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)},
	} {
		t.Run(fault.name, func(t *testing.T) {
			f := newFIFOAttemptFixture(t, 4)
			f.coord.firstErr = fault.err
			readTS := f.st.LastCommitTS()
			messages, err := f.scan(t, 60)
			require.ErrorIs(t, err, fault.err, "must not convert a delivery failure into an empty success")
			require.Empty(t, messages)
			require.Len(t, f.coord.requests, 1)
			f.requireHeadAttempt(t, f.coord.requests[0], readTS)
			for _, id := range f.ids {
				f.requireUnchanged(t, id)
			}
		})
	}
}

func TestSQSFIFOAttemptStoreErrorsPropagate(t *testing.T) {
	for _, location := range []string{"outer_scan", "tie_scan", "candidate_load", "head_load", "group_lock"} {
		t.Run(location, func(t *testing.T) {
			f := newFIFOAttemptFixture(t, 4)
			fault := errors.New("receive store unavailable: " + location)
			f.scanned.fault = fault
			switch location {
			case "outer_scan":
				f.scanned.failLimit = fifoProgressPageSize
			case "tie_scan":
				f.scanned.failLimit = sqsFifoTieScanPage
			case "candidate_load":
				f.scanned.failGetKey = f.dataKey(f.ids[0])
			case "head_load":
				f.scanned.failGetKey = f.dataKey(f.head)
			case "group_lock":
				f.scanned.failGetKey = sqsMsgGroupKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, "g")
			}
			messages, err := f.scan(t, 60)
			require.ErrorIs(t, err, fault, "must preserve the original storage failure")
			require.Empty(t, messages)
			require.Empty(t, f.coord.requests)
			for _, id := range f.ids {
				f.requireUnchanged(t, id)
			}
		})
	}
}

func TestSQSFIFOAttemptReceiveRaceClassification(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil"},
		{name: "operational", err: errors.New("coordinator unavailable")},
		{name: "deadline", err: context.DeadlineExceeded},
		{name: "conflict", err: store.ErrWriteConflict, want: true},
		{name: "locked", err: kv.ErrTxnLocked, want: true},
		{name: "wrapped_conflict", err: fmt.Errorf("receive: %w", store.ErrWriteConflict), want: true},
		{name: "wrapped_locked", err: fmt.Errorf("receive: %w", kv.ErrTxnLocked), want: true},
		{name: "joined_races", err: errors.Join(store.ErrWriteConflict, kv.ErrTxnLocked), want: true},
		{name: "unknown", err: kv.ErrTransactionOutcomeUnknown},
		{name: "unknown_conflict", err: errors.Join(kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)},
		{name: "unknown_locked", err: errors.Join(kv.ErrTxnLocked, kv.ErrTransactionOutcomeUnknown)},
		{name: "wrapped_unknown_conflict", err: fmt.Errorf("%w: %w", kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)},
		{name: "fence", err: kv.ErrRouteWriteFenced},
		{name: "fence_conflict", err: errors.Join(kv.ErrRouteWriteFenced, store.ErrWriteConflict)},
		{name: "fence_locked", err: errors.Join(kv.ErrTxnLocked, kv.ErrRouteWriteFenced)},
		{name: "grpc_unknown_fence_conflict", err: errors.Join(store.ErrWriteConflict, status.Error(codes.Unknown, kv.ErrRouteWriteFenced.Error()))},
		{name: "grpc_aborted_fence_conflict", err: errors.Join(status.Error(codes.Aborted, kv.ErrRouteWriteFenced.Error()), store.ErrWriteConflict)},
		{name: "grpc_precondition_fence_locked", err: errors.Join(kv.ErrTxnLocked, status.Error(codes.FailedPrecondition, kv.ErrRouteWriteFenced.Error()))},
		{name: "unknown_fence_conflict", err: errors.Join(kv.ErrTransactionOutcomeUnknown, kv.ErrRouteWriteFenced, store.ErrWriteConflict)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, isIgnorableSQSReceiveRace(tc.err))
		})
	}
}
