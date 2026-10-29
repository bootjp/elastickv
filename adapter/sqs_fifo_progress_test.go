package adapter

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv/kvtest"
	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

const fifoProgressPageSize = 2

// Delay only the first outer page, AFTER its real snapshot scan. Tie scans
// still read the real store, including the head outside the admitted page.
type fifoProgressDeadlineStore struct {
	store.MVCCStore
	deadline   time.Time
	entered    chan []*store.KVPair
	release    chan struct{}
	outerScans int
	tieScans   int
	firstStart []byte
	firstTS    uint64
	expired    bool
}

func (s *fifoProgressDeadlineStore) ScanAt(ctx context.Context, start, end []byte, limit int, ts uint64) ([]*store.KVPair, error) {
	page, err := s.MVCCStore.ScanAt(ctx, start, end, limit, ts)
	if err != nil {
		return nil, err
	}
	if limit != fifoProgressPageSize {
		s.tieScans++
		return page, nil
	}
	s.outerScans++
	if s.outerScans != 1 {
		return page, nil
	}
	s.firstStart, s.firstTS = bytes.Clone(start), ts
	s.entered <- page
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	// This is the exact deadline passed to scanAndDeliverPartition, not a
	// sleep guessed from elapsed time or a replacement for the global clock.
	timer := time.NewTimer(time.Until(s.deadline))
	defer timer.Stop()
	select {
	case <-timer.C:
		s.expired = time.Now().After(s.deadline)
		return page, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type fifoProgressFixture struct {
	st        store.MVCCStore
	srv       *SQSServer
	meta      *sqsQueueMeta
	partition uint32
	visibleAt int64
	ids       []string
	head      string
	original  map[string][]byte
}

func newFIFOProgressFixture(t *testing.T, partitions uint32, count int, headFirst bool) *fifoProgressFixture {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	meta := &sqsQueueMeta{
		Name: "fifo-progress.fifo", Generation: 7, IsFIFO: true,
		PartitionCount: partitions, MessageRetentionSeconds: 3600,
		FifoThroughputLimit: htfifoThroughputPerMessageGroupID,
		DeduplicationScope:  htfifoDedupeScopeMessageGroup,
	}
	f := &fifoProgressFixture{
		st: st, meta: meta, partition: partitionFor(meta, "g"),
		visibleAt: time.Now().Add(-time.Second).UnixMilli(),
		ids:       make([]string, count),
		original:  make(map[string][]byte),
	}
	for i := range f.ids {
		f.ids[i] = fmt.Sprintf("%032x", i)
	}
	// MessageId byte order is NOT visibility-key order: the segment is
	// base64 encoded. Sort and verify the actual dispatched keys instead.
	sort.Slice(f.ids, func(i, j int) bool { return bytes.Compare(f.visKey(f.ids[i]), f.visKey(f.ids[j])) < 0 })
	f.head = f.ids[len(f.ids)-1]
	if headFirst {
		f.head = f.ids[0]
	}
	f.seed(t)
	f.requireIndexOrder(t)
	// Atomic OCC application, including read-set validation: successful
	// delivery must have real data/visibility/group-lock effects.
	coord := kvtest.NewLocalCoordinator(st, kvtest.LocalOptions{
		OCC: kvtest.OCCTransactions, ValidateReadKeys: true, AdvanceCommitTS: true,
	})
	f.srv = &SQSServer{store: st, coordinator: coord}
	return f
}

func (f *fifoProgressFixture) seed(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	metaBytes, err := encodeSQSQueueMeta(f.meta)
	require.NoError(t, err)
	require.NoError(t, f.st.PutAt(ctx, sqsQueueMetaKey(f.meta.Name), metaBytes, 10, 0))
	require.NoError(t, f.st.PutAt(ctx, sqsQueueGenKey(f.meta.Name), []byte("7"), 10, 0))
	for i, id := range f.ids {
		seq := uint64(i + 2)
		if id == f.head {
			seq = 1
		}
		body := []byte("body-of-" + id)
		rec := &sqsMessageRecord{
			MessageID: id, Body: body, MD5OfBody: sqsMD5Hex(body),
			SendTimestampMillis: f.visibleAt, AvailableAtMillis: f.visibleAt,
			VisibleAtMillis: f.visibleAt, QueueGeneration: f.meta.Generation,
			MessageGroupId: "g", SequenceNumber: seq,
		}
		raw, err := encodeSQSMessageRecord(rec)
		require.NoError(t, err)
		f.original[id] = raw
		require.NoError(t, f.st.PutAt(ctx, f.dataKey(id), raw, 10, 0))
		require.NoError(t, f.st.PutAt(ctx, f.visKey(id), []byte(id), 10, 0))
		require.NoError(t, f.st.PutAt(ctx, f.ageKey(id), []byte(id), 10, 0))
	}
}

func (f *fifoProgressFixture) requireIndexOrder(t *testing.T) {
	t.Helper()
	start, end := sqsMsgVisScanBoundsDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, f.visibleAt)
	indexed, err := f.st.ScanAt(context.Background(), start, end, len(f.ids)+1, 10)
	require.NoError(t, err)
	require.Len(t, indexed, len(f.ids))
	for i, entry := range indexed {
		require.Equal(t, f.visKey(f.ids[i]), entry.Key)
		require.Equal(t, f.ids[i], string(entry.Value))
		if i > 0 {
			require.Negative(t, bytes.Compare(indexed[i-1].Key, entry.Key), "actual encoded visibility keys must be strictly increasing")
		}
	}
	t.Logf("actual base64 visibility-key order: %v; sequence-1 head=%s partition=%d generation=%d", f.ids, f.head, f.partition, f.meta.Generation)
}

func (f *fifoProgressFixture) dataKey(id string) []byte {
	return sqsMsgDataKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, id)
}

func (f *fifoProgressFixture) visKey(id string) []byte {
	return sqsMsgVisKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, f.visibleAt, id)
}

func (f *fifoProgressFixture) ageKey(id string) []byte {
	return sqsMsgByAgeKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, f.visibleAt, id)
}

func (f *fifoProgressFixture) receive(t *testing.T) []map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	readTS := f.st.LastCommitTS()
	deadline := time.Now().Add(sqsVisScanWallClockBudget)
	gated := &fifoProgressDeadlineStore{
		MVCCStore: f.st, deadline: deadline,
		entered: make(chan []*store.KVPair, 1), release: make(chan struct{}),
	}
	f.srv.store = gated
	type result struct {
		messages []map[string]any
		err      error
	}
	returned, joined := make(chan result, 1), make(chan struct{})
	go func() {
		defer close(joined)
		messages, err := f.srv.scanAndDeliverPartition(ctx, f.meta.Name, f.meta, f.partition, readTS, deadline, fifoProgressPageSize,
			sqsReceiveOptions{Max: 1, VisibilityTimeout: 60, WaitSeconds: 0})
		returned <- result{messages, err}
	}()
	defer func() {
		cancel()
		<-joined
		f.srv.store = f.st
	}()
	select {
	case page := <-gated.entered:
		require.Len(t, page, fifoProgressPageSize)
		for i, entry := range page {
			require.Equal(t, f.ids[i], string(entry.Value), "every receive restarts at the same initial page")
		}
		close(gated.release)
	case <-ctx.Done():
		t.Fatal("first real outer scan never reached the deadline gate")
	}
	var got result
	select {
	case got = <-returned:
		<-joined
	case <-ctx.Done():
		t.Fatal("receive did not finish within three seconds")
	}
	require.NoError(t, got.err)
	require.True(t, gated.expired, "the admitted first page must return after this call's exact deadline")
	require.Equal(t, 1, gated.outerScans, "a second outer page must not be needed to resolve an admitted bucket's head")
	require.Positive(t, gated.tieScans, "must exercise real same-millisecond head discovery")
	start, _ := sqsMsgVisScanBoundsDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, f.visibleAt)
	require.Equal(t, start, gated.firstStart)
	require.Equal(t, readTS, gated.firstTS)
	t.Logf("returned=%d outer_pages=%d tie_scans=%d deadline_expired=%t snapshot=%d", len(got.messages), gated.outerScans, gated.tieScans, gated.expired, readTS)
	return got.messages
}

func (f *fifoProgressFixture) requireUnchanged(t *testing.T, id string) {
	t.Helper()
	ctx := context.Background()
	for _, key := range [][]byte{f.dataKey(id), f.visKey(id), f.ageKey(id)} {
		ts, exists, err := f.st.LatestCommitTS(ctx, key)
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, uint64(10), ts, "no premature write for message %s at key %q", id, key)
	}
	raw, err := f.st.GetAt(ctx, f.dataKey(id), f.st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, f.original[id], raw)
}

func (f *fifoProgressFixture) requireHeadDelivered(t *testing.T, message map[string]any) {
	t.Helper()
	ctx := context.Background()
	require.Equal(t, f.head, message[sqsFieldMessageID])
	require.Equal(t, "body-of-"+f.head, message["Body"])
	attrs, ok := message["Attributes"].(map[string]string)
	require.True(t, ok, "delivery must contain string-valued system attributes")
	require.Equal(t, "1", attrs["SequenceNumber"])
	receipt, ok := message["ReceiptHandle"].(string)
	require.True(t, ok, "delivery must contain a string receipt handle")
	handle, err := decodeReceiptHandle(receipt)
	require.NoError(t, err)
	require.Equal(t, f.head, handle.MessageIDHex)
	require.Equal(t, f.partition, handle.Partition)
	require.Equal(t, f.meta.Generation, handle.QueueGeneration)
	latest := f.st.LastCommitTS()
	require.Greater(t, latest, uint64(10), "successful delivery must really commit")
	raw, err := f.st.GetAt(ctx, f.dataKey(f.head), latest)
	require.NoError(t, err)
	rec, err := decodeSQSMessageRecord(raw)
	require.NoError(t, err)
	require.Equal(t, f.head, rec.MessageID)
	require.Equal(t, int64(1), rec.ReceiveCount)
	require.Equal(t, handle.ReceiptToken, rec.CurrentReceiptToken)
	require.Greater(t, rec.VisibleAtMillis, f.visibleAt)
	_, err = f.st.GetAt(ctx, f.visKey(f.head), latest)
	require.ErrorIs(t, err, store.ErrKeyNotFound)
	newVis := sqsMsgVisKeyDispatch(f.meta, f.meta.Name, f.partition, f.meta.Generation, rec.VisibleAtMillis, f.head)
	id, err := f.st.GetAt(ctx, newVis, latest)
	require.NoError(t, err)
	require.Equal(t, f.head, string(id))
	lock, err := f.srv.loadFifoGroupLock(ctx, f.meta.Name, f.meta, f.partition, f.meta.Generation, "g", latest)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, f.head, lock.MessageID)
	require.Equal(t, rec.VisibleAtMillis, lock.VisibleAtMs)
	old, err := f.st.GetAt(ctx, f.dataKey(f.head), 10)
	require.NoError(t, err)
	require.Equal(t, f.original[f.head], old, "delivery must preserve the receive's old snapshot")
}

func TestSQSReceiveFIFOProgressAcrossExpiredPageBudget(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		for _, count := range []int{4, 33} {
			for _, headFirst := range []bool{true, false} {
				t.Run(fmt.Sprintf("partitions_%d/count_%d/head_first_%t", partitions, count, headFirst), func(t *testing.T) {
					runFIFOProgressCase(t, partitions, count, headFirst)
				})
			}
		}
	}
}

func runFIFOProgressCase(t *testing.T, partitions uint32, count int, headFirst bool) {
	t.Helper()
	f := newFIFOProgressFixture(t, partitions, count, headFirst)
	if !headFirst {
		require.Equal(t, f.ids[len(f.ids)-1], f.head, "the head must sort last by its actual encoded visibility key")
		require.NotContains(t, f.ids[:fifoProgressPageSize], f.head)
		if count == 33 {
			require.Greater(t, len(f.ids)-1, sqsFifoTieScanPage, "the head must be beyond the first tie-scan page")
		}
	}
	// No sends, deletes, or clock changes between receives. A fixed
	// resolver must deliver the head as work admitted by page one.
	for attempt := 1; attempt <= 3; attempt++ {
		messages := f.receive(t)
		for _, id := range f.ids {
			if id != f.head {
				f.requireUnchanged(t, id)
			}
		}
		if len(messages) == 1 {
			f.requireHeadDelivered(t, messages[0])
			return
		}
		f.requireUnchanged(t, f.head)
		lock, err := f.srv.loadFifoGroupLock(context.Background(), f.meta.Name, f.meta, f.partition, f.meta.Generation, "g", f.st.LastCommitTS())
		require.NoError(t, err)
		require.Nil(t, lock)
		t.Errorf("receive %d: live sequence-1 head %s must be reachable from the admitted same-millisecond bucket; got %d messages after the page budget expired", attempt, f.head, len(messages))
	}
}
