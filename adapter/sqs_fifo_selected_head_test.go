package adapter

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

type selectedHeadCoordinator struct {
	kv.Coordinator
	requests []*kv.OperationGroup[kv.OP]
}

func (c *selectedHeadCoordinator) Dispatch(ctx context.Context, req *kv.OperationGroup[kv.OP]) (*kv.CoordinateResponse, error) {
	c.requests = append(c.requests, req)
	return c.Coordinator.Dispatch(ctx, req)
}

func newSelectedHeadFixture(t *testing.T, partitions uint32, count int) (*fifoProgressFixture, *selectedHeadCoordinator) {
	t.Helper()
	f := newFIFOProgressFixture(t, partitions, count, false)
	coord := &selectedHeadCoordinator{Coordinator: f.srv.coordinator}
	f.srv.coordinator = coord
	return f, coord
}

func selectedHeadScan(t *testing.T, f *fifoProgressFixture, readTS uint64) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	messages, err := f.srv.scanAndDeliverPartition(ctx, f.meta.Name, f.meta, f.partition, readTS,
		time.Now().Add(sqsVisScanWallClockBudget), fifoProgressPageSize,
		sqsReceiveOptions{Max: 10, VisibilityTimeout: 60, WaitSeconds: 0})
	require.NoError(t, err)
	return messages
}

func selectedHeadLoad(t *testing.T, st store.MVCCStore, key []byte, ts uint64) *sqsMessageRecord {
	t.Helper()
	raw, err := st.GetAt(context.Background(), key, ts)
	require.NoError(t, err)
	rec, err := decodeSQSMessageRecord(raw)
	require.NoError(t, err)
	return rec
}

func selectedHeadKeys(meta *sqsQueueMeta, partition uint32, rec *sqsMessageRecord) [][]byte {
	return [][]byte{
		sqsMsgDataKeyDispatch(meta, meta.Name, partition, meta.Generation, rec.MessageID),
		sqsMsgVisKeyDispatch(meta, meta.Name, partition, meta.Generation, rec.VisibleAtMillis, rec.MessageID),
		sqsMsgByAgeKeyDispatch(meta, meta.Name, partition, meta.Generation, rec.SendTimestampMillis, rec.MessageID),
	}
}

func selectedHeadPut(t *testing.T, f *fifoProgressFixture, rec *sqsMessageRecord, partition uint32, ts uint64) [][]byte {
	t.Helper()
	raw, err := encodeSQSMessageRecord(rec)
	require.NoError(t, err)
	keys := selectedHeadKeys(f.meta, partition, rec)
	for i, key := range keys {
		value := []byte(rec.MessageID)
		if i == 0 {
			value = raw
		}
		require.NoError(t, f.st.PutAt(context.Background(), key, value, ts, 0))
	}
	return keys
}

func selectedHeadPrepareCleanup(t *testing.T, f *fifoProgressFixture, redrive, owned bool) *sqsMessageRecord {
	t.Helper()
	rec := selectedHeadLoad(t, f.st, f.dataKey(f.head), 10)
	if redrive {
		rec.ReceiveCount, rec.FirstReceiveMillis = 2, f.visibleAt
		var err error
		rec.CurrentReceiptToken, err = newReceiptToken()
		require.NoError(t, err)
	} else {
		rec.SendTimestampMillis = f.visibleAt - int64((2*time.Hour)/time.Millisecond)
		require.NoError(t, f.st.DeleteAt(context.Background(), f.ageKey(f.head), 20))
	}
	selectedHeadPut(t, f, rec, f.partition, 20)
	if owned {
		lock, err := encodeFifoGroupLock(&sqsFifoGroupLock{MessageID: f.head, AcquiredAtMs: f.visibleAt, VisibleAtMs: f.visibleAt})
		require.NoError(t, err)
		key := sqsMsgGroupKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, "g")
		require.NoError(t, f.st.PutAt(context.Background(), key, lock, 20, 0))
	}
	return rec
}

func selectedHeadPrepareDLQ(t *testing.T, f *fifoProgressFixture) *fifoProgressFixture {
	t.Helper()
	meta := *f.meta
	meta.Name, meta.Generation, meta.PartitionCount = "selected-head-dlq.fifo", 11, 8
	raw, err := encodeSQSQueueMeta(&meta)
	require.NoError(t, err)
	ctx := context.Background()
	require.NoError(t, f.st.PutAt(ctx, sqsQueueMetaKey(meta.Name), raw, 20, 0))
	require.NoError(t, f.st.PutAt(ctx, sqsQueueGenKey(meta.Name), []byte("11"), 20, 0))
	require.NoError(t, f.st.PutAt(ctx, sqsQueueSeqKey(meta.Name), []byte("41"), 20, 0))
	f.meta.RedrivePolicy = fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":2}`, f.srv.queueArn(meta.Name))
	raw, err = encodeSQSQueueMeta(f.meta)
	require.NoError(t, err)
	require.NoError(t, f.st.PutAt(ctx, sqsQueueMetaKey(f.meta.Name), raw, 20, 0))
	return &fifoProgressFixture{st: f.st, srv: f.srv, meta: &meta, partition: partitionFor(&meta, "g"), head: f.head}
}

func requireSelectedHeadCleanup(t *testing.T, f *fifoProgressFixture, rec *sqsMessageRecord, readTS uint64, owned bool) {
	t.Helper()
	ctx := context.Background()
	commitTS := f.st.LastCommitTS()
	require.Greater(t, commitTS, readTS)
	keys := selectedHeadKeys(f.meta, f.partition, rec)
	groupKey := sqsMsgGroupKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, "g")
	if owned {
		keys = append(keys, groupKey)
	}
	for _, key := range keys {
		_, err := f.st.GetAt(ctx, key, commitTS)
		require.ErrorIs(t, err, store.ErrKeyNotFound, "cleanup must remove the selected head key %q", key)
		ts, exists, err := f.st.LatestCommitTS(ctx, key)
		require.NoError(t, err)
		require.True(t, exists, "cleanup must persist a tombstone")
		require.Equal(t, commitTS, ts, "head cleanup must be atomic")
	}
	_, err := f.st.GetAt(ctx, groupKey, commitTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound, "cleanup must not acquire the successor's group lock")
	old := selectedHeadLoad(t, f.st, f.dataKey(f.head), readTS)
	require.Equal(t, rec, old, "the receive snapshot must retain the original selected head")
	for _, id := range f.ids {
		if id != f.head {
			f.requireUnchanged(t, id)
		}
	}
}

func requireSelectedHeadRotation(t *testing.T, f *fifoProgressFixture, message map[string]any, id string, sequence uint64) {
	t.Helper()
	require.Equal(t, id, message[sqsFieldMessageID])
	require.Equal(t, "body-of-"+id, message["Body"])
	attrs, ok := message["Attributes"].(map[string]string)
	require.True(t, ok)
	require.Equal(t, strconv.FormatUint(sequence, 10), attrs["SequenceNumber"])
	receipt, ok := message["ReceiptHandle"].(string)
	require.True(t, ok)
	handle, err := decodeReceiptHandle(receipt)
	require.NoError(t, err)
	require.Equal(t, id, handle.MessageIDHex)
	require.Equal(t, f.partition, handle.Partition)
	require.Equal(t, f.meta.Generation, handle.QueueGeneration)
	latest := f.st.LastCommitTS()
	rec := selectedHeadLoad(t, f.st, f.dataKey(id), latest)
	require.Equal(t, id, rec.MessageID)
	require.Equal(t, sequence, rec.SequenceNumber)
	require.Equal(t, f.meta.Generation, rec.QueueGeneration)
	require.Equal(t, int64(1), rec.ReceiveCount)
	require.Equal(t, handle.ReceiptToken, rec.CurrentReceiptToken)
	vis := sqsMsgVisKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, rec.VisibleAtMillis, id)
	value, err := f.st.GetAt(context.Background(), vis, latest)
	require.NoError(t, err)
	require.Equal(t, id, string(value))
	lock, err := f.srv.loadFifoGroupLock(context.Background(), f.meta.Name, f.meta, f.partition, f.meta.Generation, "g", latest)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, id, lock.MessageID)
	require.Equal(t, rec.VisibleAtMillis, lock.VisibleAtMs)
}

func requireSelectedHeadDLQ(t *testing.T, src, dlq *fifoProgressFixture, before *sqsMessageRecord, commitTS uint64) {
	t.Helper()
	rec := selectedHeadLoad(t, dlq.st, dlq.dataKey(src.head), commitTS)
	require.Equal(t, before.Body, rec.Body)
	require.Equal(t, before.MD5OfBody, rec.MD5OfBody)
	require.Equal(t, before.MessageID, rec.MessageID)
	require.Equal(t, before.MessageGroupId, rec.MessageGroupId)
	require.Equal(t, dlq.meta.Generation, rec.QueueGeneration)
	require.Equal(t, uint64(42), rec.SequenceNumber)
	require.Zero(t, rec.ReceiveCount)
	require.Zero(t, rec.FirstReceiveMillis)
	require.NotEmpty(t, rec.CurrentReceiptToken)
	require.NotEqual(t, before.CurrentReceiptToken, rec.CurrentReceiptToken)
	require.Equal(t, src.srv.queueArn(src.meta.Name), rec.DeadLetterSourceArn)
	require.Equal(t, src.head, rec.MessageDeduplicationId)
	for _, key := range selectedHeadKeys(dlq.meta, dlq.partition, rec) {
		ts, exists, err := dlq.st.LatestCommitTS(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, commitTS, ts, "source cleanup and all DLQ indices must share a commit")
	}
	seq, err := dlq.st.GetAt(context.Background(), sqsQueueSeqKey(dlq.meta.Name), commitTS)
	require.NoError(t, err)
	require.Equal(t, "42", string(seq))
	// The successor must never be copied into the DLQ by head substitution.
	_, err = dlq.st.GetAt(context.Background(), dlq.dataKey(src.ids[0]), commitTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	dlq.visibleAt = rec.VisibleAtMillis
}

func runSelectedHeadCleanup(t *testing.T, partitions uint32, redrive, owned bool) {
	t.Helper()
	f, coord := newSelectedHeadFixture(t, partitions, 4)
	var dlq *fifoProgressFixture
	if redrive {
		dlq = selectedHeadPrepareDLQ(t, f)
	}
	rec := selectedHeadPrepareCleanup(t, f, redrive, owned)
	readTS := f.st.LastCommitTS()
	var messages []map[string]any
	if owned {
		// An owned lock forbids successor substitution; the later outer
		// page must instead clean its actual owner and release the lock.
		messages = selectedHeadScan(t, f, readTS)
	} else {
		// Expire the budget after page one: cleanup cannot accidentally
		// pass merely by eventually visiting the head's original page.
		messages = f.receive(t)
	}
	require.Empty(t, messages)
	require.Len(t, coord.requests, 1, "one receive must not repeatedly process its selected group")
	require.Equal(t, readTS, coord.requests[0].StartTS)
	requireSelectedHeadCleanup(t, f, rec, readTS, owned)
	if redrive {
		requireSelectedHeadDLQ(t, f, dlq, rec, f.st.LastCommitTS())
	}
	// A fresh snapshot and fresh attempt-local group set expose the successor.
	fresh := selectedHeadScan(t, f, f.st.LastCommitTS())
	require.Len(t, fresh, 1)
	requireSelectedHeadRotation(t, f, fresh[0], f.ids[0], 2)
	require.Len(t, coord.requests, 2)
	if redrive {
		moved := selectedHeadScan(t, dlq, f.st.LastCommitTS())
		require.Len(t, moved, 1)
		requireSelectedHeadRotation(t, dlq, moved[0], f.head, 42)
		require.Len(t, coord.requests, 3)
	}
}

func TestSQSSelectedHeadRetentionExpiry(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		for _, owned := range []bool{false, true} {
			t.Run(fmt.Sprintf("partitions_%d/owned_%t", partitions, owned), func(t *testing.T) {
				runSelectedHeadCleanup(t, partitions, false, owned)
			})
		}
	}
}

func TestSQSSelectedHeadRedrive(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		for _, owned := range []bool{false, true} {
			t.Run(fmt.Sprintf("partitions_%d/owned_%t", partitions, owned), func(t *testing.T) {
				runSelectedHeadCleanup(t, partitions, true, owned)
			})
		}
	}
}

func TestSQSSelectedHeadCleanupJoinedErrors(t *testing.T) {
	for _, fault := range []struct {
		name string
		err  error
	}{
		{name: "unknown_with_conflict", err: errors.Join(kv.ErrTransactionOutcomeUnknown, store.ErrWriteConflict)},
		{name: "fence_with_locked", err: errors.Join(kv.ErrRouteWriteFenced, kv.ErrTxnLocked)},
	} {
		for _, redrive := range []bool{false, true} {
			for _, partitions := range []uint32{1, 4} {
				t.Run(fmt.Sprintf("%s/redrive_%t/partitions_%d", fault.name, redrive, partitions), func(t *testing.T) {
					runSelectedHeadCleanupFault(t, partitions, redrive, fault.err)
				})
			}
		}
	}
}

func runSelectedHeadCleanupFault(t *testing.T, partitions uint32, redrive bool, fault error) {
	t.Helper()
	f, applied := newSelectedHeadFixture(t, partitions, 4)
	var dlq *fifoProgressFixture
	if redrive {
		dlq = selectedHeadPrepareDLQ(t, f)
	}
	rec := selectedHeadPrepareCleanup(t, f, redrive, false)
	headKeys := selectedHeadKeys(f.meta, f.partition, rec)
	saved := selectedHeadSaveKeys(t, f.st, headKeys)
	// Inject before apply; subsequent successful calls would use the real
	// OCC coordinator. An ambiguous/fenced first result must abort receive.
	failing := &fifoAttemptCoordinator{Coordinator: f.srv.coordinator, firstErr: fault}
	f.srv.coordinator = failing
	readTS := f.st.LastCommitTS()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	messages, err := f.srv.scanAndDeliverPartition(ctx, f.meta.Name, f.meta, f.partition, readTS,
		time.Now().Add(sqsVisScanWallClockBudget), fifoProgressPageSize,
		sqsReceiveOptions{Max: 10, VisibilityTimeout: 60, WaitSeconds: 0})
	require.ErrorIs(t, err, fault, "joined unknown/fence must propagate instead of becoming an empty successful receive")
	require.Empty(t, messages)
	require.Len(t, failing.requests, 1, "must not retry cleanup or rotate a successor after an ambiguous/fenced result")
	require.Empty(t, applied.requests, "fault injection must precede real OCC application")
	req := failing.requests[0]
	require.True(t, req.IsTxn)
	require.Equal(t, readTS, req.StartTS)
	deleted := make([][]byte, 0, len(headKeys))
	for _, elem := range req.Elems {
		if elem.Op == kv.Del {
			deleted = append(deleted, elem.Key)
		}
	}
	require.ElementsMatch(t, headKeys, deleted, "cleanup must delete exactly the selected head's keys, never its scanned successor")
	requireSelectedHeadSavedKeys(t, f.st, saved)
	require.Equal(t, readTS, f.st.LastCommitTS())
	for _, id := range f.ids[:len(f.ids)-1] {
		f.requireUnchanged(t, id)
	}
	groupKey := sqsMsgGroupKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, "g")
	_, err = f.st.GetAt(ctx, groupKey, readTS)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	if dlq != nil {
		_, err = dlq.st.GetAt(ctx, dlq.dataKey(f.head), dlq.st.LastCommitTS())
		require.ErrorIs(t, err, store.ErrKeyNotFound)
		seq, err := dlq.st.GetAt(ctx, sqsQueueSeqKey(dlq.meta.Name), dlq.st.LastCommitTS())
		require.NoError(t, err)
		require.Equal(t, "41", string(seq), "failed redrive must not advance the DLQ sequence")
	}
}

func TestSQSSelectedHeadSingleRotationPerSnapshot(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		t.Run(fmt.Sprintf("partitions_%d", partitions), func(t *testing.T) {
			f, coord := newSelectedHeadFixture(t, partitions, 33)
			messages := selectedHeadScan(t, f, f.st.LastCommitTS())
			require.Len(t, messages, 1, "Max=10 must not rotate the same selected head on later pages of the old snapshot")
			f.requireHeadDelivered(t, messages[0])
			require.Len(t, coord.requests, 1)
			committed := f.st.LastCommitTS()
			require.Empty(t, selectedHeadScan(t, f, committed), "the persisted owned lock must keep successors invisible on a fresh receive")
			require.Len(t, coord.requests, 1)
			require.Equal(t, committed, f.st.LastCommitTS())
			for _, id := range f.ids {
				if id != f.head {
					f.requireUnchanged(t, id)
				}
			}
		})
	}
}

type selectedHeadSavedKey struct {
	key   []byte
	value []byte
	ts    uint64
}

func selectedHeadSaveKeys(t *testing.T, st store.MVCCStore, keys [][]byte) []selectedHeadSavedKey {
	t.Helper()
	saved := make([]selectedHeadSavedKey, 0, len(keys))
	for _, key := range keys {
		value, err := st.GetAt(context.Background(), key, st.LastCommitTS())
		require.NoError(t, err)
		ts, exists, err := st.LatestCommitTS(context.Background(), key)
		require.NoError(t, err)
		require.True(t, exists)
		saved = append(saved, selectedHeadSavedKey{key: bytes.Clone(key), value: bytes.Clone(value), ts: ts})
	}
	return saved
}

func requireSelectedHeadSavedKeys(t *testing.T, st store.MVCCStore, saved []selectedHeadSavedKey) {
	t.Helper()
	for _, entry := range saved {
		value, err := st.GetAt(context.Background(), entry.key, st.LastCommitTS())
		require.NoError(t, err)
		require.Equal(t, entry.value, value, "selection must not mutate unrelated or stale data")
		ts, exists, err := st.LatestCommitTS(context.Background(), entry.key)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, entry.ts, ts)
	}
}

func selectedHeadAddDecoys(t *testing.T, f *fifoProgressFixture) []selectedHeadSavedKey {
	t.Helper()
	base := selectedHeadLoad(t, f.st, f.dataKey(f.head), 10)
	base.SequenceNumber = 0
	base.MessageID, base.MessageGroupId = strings.Repeat("a", 32), "other-group"
	keys := selectedHeadPut(t, f, base, f.partition, 20)
	base.MessageID, base.MessageGroupId = strings.Repeat("c", 32), "g"
	base.VisibleAtMillis = f.visibleAt + 60_000
	keys = append(keys, selectedHeadPut(t, f, base, f.partition, 20)...)
	staleVis := sqsMsgVisKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, f.visibleAt, base.MessageID)
	require.NoError(t, f.st.PutAt(context.Background(), staleVis, []byte(base.MessageID), 20, 0))
	keys = append(keys, staleVis)
	missing := strings.Repeat("b", 32)
	missingVis := f.visKey(missing)
	require.NoError(t, f.st.PutAt(context.Background(), missingVis, []byte(missing), 20, 0))
	keys = append(keys, missingVis)
	if f.meta.PartitionCount > 1 {
		// A lower sequence stored under another partition must never be
		// substituted, even when its group and millisecond match.
		base.MessageID, base.VisibleAtMillis = strings.Repeat("e", 32), f.visibleAt
		keys = append(keys, selectedHeadPut(t, f, base, (f.partition+1)%f.meta.PartitionCount, 20)...)
	}
	base.MessageID, base.VisibleAtMillis = strings.Repeat("d", 32), f.visibleAt
	keys = append(keys, selectedHeadPut(t, f, base, f.partition, 30)...)
	return selectedHeadSaveKeys(t, f.st, keys)
}

func TestSQSSelectedHeadSelectionFilters(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		t.Run(fmt.Sprintf("partitions_%d", partitions), func(t *testing.T) {
			f, coord := newSelectedHeadFixture(t, partitions, 4)
			saved := selectedHeadAddDecoys(t, f)
			// The last decoy committed at 30, after this receive's snapshot.
			const readTS = uint64(20)
			cand := sqsMsgCandidate{visKey: f.visKey(f.ids[0]), messageID: f.ids[0], partition: f.partition}
			var state sqsFifoReceiveState
			message, skip, err := f.srv.tryDeliverCandidate(context.Background(), f.meta.Name, f.meta, cand, readTS,
				sqsReceiveOptions{Max: 1, VisibilityTimeout: 60}, nil, &state)
			require.NoError(t, err)
			require.False(t, skip)
			f.requireHeadDelivered(t, message)
			require.Len(t, coord.requests, 1)
			require.Equal(t, readTS, coord.requests[0].StartTS)
			requireSelectedHeadSavedKeys(t, f.st, saved)
			for _, id := range f.ids[:len(f.ids)-1] {
				f.requireUnchanged(t, id)
			}
		})
	}
}
