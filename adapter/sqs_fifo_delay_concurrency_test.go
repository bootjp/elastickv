package adapter

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSQSFIFODelayConcurrentConfiguration(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 4, 0)
	id := sendDelayTestMessage(t, srv, "A", "g")
	gate := &fifoDelayBarrierCoordinator{Coordinator: srv.coordinator, entered: make(chan struct{}), release: make(chan struct{}), match: func(req *kv.OperationGroup[kv.OP]) bool {
		return !delayDataPut(req)
	}}
	srv.coordinator = gate
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- srv.setQueueAttributesCore(ctx, fifoDelayTestQueue, map[string]string{"DelaySeconds": "900"})
	}()
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(gate.release) }); cancel() })
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("configuration never reached its commit")
	}
	require.NoError(t, srv.setQueueAttributesCore(ctx, fifoDelayTestQueue, map[string]string{"DelaySeconds": "300", "VisibilityTimeout": "120"}))
	unblock.Do(func() { close(gate.release) })
	require.NoError(t, <-done)
	meta, _, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, srv.nextTxnReadTS(ctx))
	require.NoError(t, err)
	require.EqualValues(t, 900, meta.DelaySeconds)
	require.EqualValues(t, 120, meta.VisibilityTimeoutSeconds, "the retried writer must not clobber unrelated attributes")
	require.True(t, meta.FifoOrderIndexed)
	require.Nil(t, meta.DelayReindex)
	rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
	require.Equal(t, rec.SendTimestampMillis+900_000, rec.VisibleAtMillis)
}

type fifoDelayFaultCoordinator struct {
	kv.Coordinator
	fault error
	apply bool
	calls int
}

func (c *fifoDelayFaultCoordinator) RaftMembers(ctx context.Context) ([]kv.RaftMember, error) {
	return (kv.CoordinatorForwarder{Inner: c.Coordinator}).RaftMembers(ctx)
}

func (c *fifoDelayFaultCoordinator) RaftMembersForKey(ctx context.Context, key []byte) ([]kv.RaftMember, error) {
	return (kv.CoordinatorForwarder{Inner: c.Coordinator}).RaftMembersForKey(ctx, key)
}

func (c *fifoDelayFaultCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.calls++
	if c.apply {
		if _, err := c.Coordinator.Dispatch(ctx, req); err != nil {
			return nil, err
		}
	}
	return nil, c.fault
}

func TestSQSFIFODelayUnknownOutcomeRecovery(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := newDelayTestServer(t, st)
			createDelayTestQueue(t, srv, 1, 0)
			id := sendDelayTestMessage(t, srv, "A", "g")
			beginDelayTestUpdate(t, srv, 900)
			fault := &fifoDelayFaultCoordinator{Coordinator: srv.coordinator, apply: applied, fault: errors.Join(kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)}
			srv.coordinator = fault
			err := srv.completeFifoDelayReindex(context.Background(), fifoDelayTestQueue)
			require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
			require.Equal(t, 1, fault.calls, "an unknown outcome must not be blindly retried even if it wraps a conflict")
			srv = newDelayTestServer(t, st)
			require.NoError(t, srv.completeFifoDelayReindex(context.Background(), fifoDelayTestQueue))
			rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
			require.Equal(t, rec.SendTimestampMillis+900_000, rec.VisibleAtMillis)
			meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
			require.NoError(t, err)
			prefix := sqsFifoOrderPrefix(meta, meta.Name, partitionFor(meta, "g"), meta.Generation, "g")
			page, err := st.ScanAt(context.Background(), prefix, prefixScanEnd(prefix), 10, st.LastCommitTS())
			require.NoError(t, err)
			require.Len(t, page, 1)
			require.Equal(t, id, string(page[0].Value))
		})
	}
}

func TestSQSFIFODelayInitialFenceUnknownOutcome(t *testing.T) {
	for _, applied := range []bool{false, true} {
		t.Run(fmt.Sprint(applied), func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := newDelayTestServer(t, st)
			createDelayTestQueue(t, srv, 1, 0)
			id := sendDelayTestMessage(t, srv, "A", "g")
			fault := &fifoDelayFaultCoordinator{Coordinator: srv.coordinator, apply: applied, fault: errors.Join(kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)}
			srv.coordinator = fault
			err := srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "900"})
			require.ErrorIs(t, err, kv.ErrTransactionOutcomeUnknown)
			require.Equal(t, 1, fault.calls)
			srv = newDelayTestServer(t, st)
			require.NoError(t, srv.completeFifoDelayReindex(context.Background(), fifoDelayTestQueue))
			rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
			want := rec.SendTimestampMillis
			if applied {
				want += 900_000
			}
			require.Equal(t, want, rec.VisibleAtMillis)
		})
	}
}

func TestSQSFIFODelayDeleteDuringBackfill(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 1, 0)
	first := sendDelayTestMessage(t, srv, "A", "g")
	second := sendDelayTestMessage(t, srv, "B", "g")
	messages, err := srv.scanAndDeliverOnce(context.Background(), fifoDelayTestQueue, sqsReceiveOptions{Max: 1, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	handle, err := decodeClientReceiptHandle(delayNativeString(t, messages[0], "ReceiptHandle"))
	require.NoError(t, err)
	// Activate the index while preserving the received head, then start a
	// second reconciliation so deletion races an already-built membership.
	require.NoError(t, srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "900"}))
	beginDelayTestUpdate(t, srv, 0)
	require.NoError(t, srv.changeMessageVisibilityCore(context.Background(), fifoDelayTestQueue, handle, 0))
	require.NoError(t, srv.deleteMessageCore(context.Background(), fifoDelayTestQueue, handle))
	require.NoError(t, srv.completeFifoDelayReindex(context.Background(), fifoDelayTestQueue))
	messages, err = srv.scanAndDeliverOnce(context.Background(), fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, second, messages[0]["MessageId"])
	meta, _, err := srv.loadQueueMetaAt(context.Background(), fifoDelayTestQueue, srv.nextTxnReadTS(context.Background()))
	require.NoError(t, err)
	prefix := sqsFifoOrderPrefix(meta, meta.Name, 0, meta.Generation, "g")
	page, err := st.ScanAt(context.Background(), prefix, prefixScanEnd(prefix), 10, st.LastCommitTS())
	require.NoError(t, err)
	require.Len(t, page, 1)
	require.NotEqual(t, first, string(page[0].Value))
}

func TestSQSFIFOIndexedHeadExpiryRemovesMembership(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		for _, reaper := range []bool{false, true} {
			t.Run(fmt.Sprintf("partitions_%d/reaper_%t", partitions, reaper), func(t *testing.T) {
				st := store.NewMVCCStore()
				t.Cleanup(func() { require.NoError(t, st.Close()) })
				srv := newDelayTestServer(t, st)
				createDelayTestQueue(t, srv, partitions, 0)
				first := sendDelayTestMessage(t, srv, "A", "g")
				second := sendDelayTestMessage(t, srv, "B", "g")
				require.NoError(t, srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"DelaySeconds": "0"}))
				ctx := context.Background()
				stamp, err := srv.beginTxnReadTimestamp(ctx, "expiry test")
				require.NoError(t, err)
				ctx = stamp.WithDispatchVoucher(ctx)
				meta, _, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, stamp.Timestamp())
				require.NoError(t, err)
				rec := loadDelayTestRecord(t, srv, fifoDelayTestQueue, first, "g")
				partition := partitionFor(meta, "g")
				dataKey := sqsMsgDataKeyDispatch(meta, meta.Name, partition, meta.Generation, first)
				visKey := sqsMsgVisKeyDispatch(meta, meta.Name, partition, meta.Generation, rec.VisibleAtMillis, first)
				if reaper {
					ageKey := sqsMsgByAgeKeyDispatch(meta, meta.Name, partition, meta.Generation, rec.SendTimestampMillis, first)
					req, err := srv.buildReapOps(ctx, meta.Name, meta, partition, meta.Generation, ageKey, dataKey, rec, stamp.Timestamp())
					require.NoError(t, err)
					_, err = kv.DispatchWithReadTimestamp(ctx, srv.coordinator, req)
					require.NoError(t, err)
				} else {
					require.NoError(t, srv.expireMessage(ctx, meta.Name, meta, partition, meta.Generation, visKey, dataKey, rec, stamp.Timestamp()))
				}
				_, err = st.GetAt(ctx, sqsFifoOrderKey(meta, meta.Name, partition, meta.Generation, rec), st.LastCommitTS())
				require.ErrorIs(t, err, store.ErrKeyNotFound)
				messages, err := srv.scanAndDeliverOnce(context.Background(), meta.Name, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
				require.NoError(t, err)
				require.Len(t, messages, 1)
				require.Equal(t, second, messages[0]["MessageId"])
			})
		}
	}
}

func TestSQSFIFOOrderPrefixDoesNotAlias(t *testing.T) {
	meta := &sqsQueueMeta{Name: fifoDelayTestQueue, Generation: 1, PartitionCount: 4}
	for _, group := range []string{"g", "gg", "g\x00", "g|", "g/", ""} {
		prefix := sqsFifoOrderPrefix(meta, meta.Name, 0, meta.Generation, group)
		for _, other := range []string{"g", "gg", "g\x00", "g|", "g/", ""} {
			if group == other {
				continue
			}
			require.False(t, bytes.HasPrefix(sqsMsgGroupKeyDispatch(meta, meta.Name, 0, meta.Generation, other), prefix))
			require.False(t, bytes.HasPrefix(sqsFifoOrderPrefix(meta, meta.Name, 0, meta.Generation, other), prefix))
		}
	}
}

func TestSQSFIFODelayReadFailureDoesNotAdvance(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 1, 0)
	id := sendDelayTestMessage(t, srv, "A", "g")
	beginDelayTestUpdate(t, srv, 900)
	ctx := context.Background()
	before, _, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, srv.nextTxnReadTS(ctx))
	require.NoError(t, err)
	committed := st.LastCommitTS()
	fault := errors.New("injected data read failure")
	srv.store = &fifoAttemptStore{MVCCStore: st, failGetKey: sqsMsgDataKeyDispatch(before, before.Name, 0, before.Generation, id), fault: fault}
	_, err = srv.advanceFifoDelayReindex(ctx, fifoDelayTestQueue)
	require.ErrorIs(t, err, fault)
	require.Equal(t, committed, st.LastCommitTS(), "no partial page or cursor commit is allowed on a read failure")
	after, _, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, srv.nextTxnReadTS(ctx))
	require.NoError(t, err)
	require.Equal(t, before.DelayReindex, after.DelayReindex)
	srv.store = st
	require.NoError(t, srv.completeFifoDelayReindex(ctx, fifoDelayTestQueue))
}

func TestSQSFIFODelayDeleteRecreateCleansMembership(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		t.Run(fmt.Sprint(partitions), func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := newDelayTestServer(t, st)
			createDelayTestQueue(t, srv, partitions, 0)
			for i := 0; i < sqsFifoDelayPageSize+1; i++ {
				sendDelayTestMessage(t, srv, fmt.Sprint(i), "g")
			}
			ctx := context.Background()
			require.NoError(t, srv.setQueueAttributesCore(ctx, fifoDelayTestQueue, map[string]string{"DelaySeconds": "0"}))
			old, _, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, srv.nextTxnReadTS(ctx))
			require.NoError(t, err)
			beginDelayTestUpdate(t, srv, 900)
			done, err := srv.advanceFifoDelayReindex(ctx, fifoDelayTestQueue)
			require.NoError(t, err)
			require.False(t, done)
			require.NoError(t, srv.deleteQueueCore(ctx, fifoDelayTestQueue))
			createDelayTestQueue(t, srv, partitions, 0)
			id := sendDelayTestMessage(t, srv, "new", "g")
			require.NoError(t, srv.completeFifoDelayReindex(ctx, fifoDelayTestQueue))
			require.NoError(t, srv.reapAllQueues(ctx))
			prefix := sqsFifoOrderPrefix(old, old.Name, partitionFor(old, "g"), old.Generation, "g")
			page, err := st.ScanAt(ctx, prefix, prefixScanEnd(prefix), 100, st.LastCommitTS())
			require.NoError(t, err)
			require.Empty(t, page, "tombstone cleanup must remove sequence members as well as locks")
			messages, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, id, messages[0]["MessageId"])
		})
	}
}
