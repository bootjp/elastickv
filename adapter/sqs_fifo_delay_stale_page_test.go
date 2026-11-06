package adapter

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

func TestSQSFIFODelayStalePageCannotUndoLifecycle(t *testing.T) {
	for _, partitions := range []uint32{1, 4} {
		for _, action := range []string{"purge", "delete_recreate", "delete_message"} {
			t.Run(fmt.Sprintf("partitions_%d/%s", partitions, action), func(t *testing.T) {
				testFifoDelayStalePage(t, partitions, action)
			})
		}
	}
}

func testFifoDelayStalePage(t *testing.T, partitions uint32, action string) {
	t.Helper()
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, partitions, 0)
	first := sendDelayTestMessage(t, srv, "A", "g")
	var handle *decodedReceiptHandle
	if action == "delete_message" {
		messages, err := srv.scanAndDeliverOnce(t.Context(), fifoDelayTestQueue, sqsReceiveOptions{Max: 1, VisibilityTimeout: 60})
		require.NoError(t, err)
		require.Len(t, messages, 1)
		handle, err = decodeClientReceiptHandle(delayNativeString(t, messages[0], "ReceiptHandle"))
		require.NoError(t, err)
		sendDelayTestMessage(t, srv, "B", "g")
	}
	oldRecord := loadDelayTestRecord(t, srv, fifoDelayTestQueue, first, "g")
	beginDelayTestUpdate(t, srv, 900)
	oldMeta := seekDelayTestPartition(t, srv, "g")
	gate := &fifoDelayBarrierCoordinator{
		Coordinator: srv.coordinator, match: delayDataPut,
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	srv.coordinator = gate
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	result := make(chan error, 1)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		_, err := srv.advanceFifoDelayReindex(ctx, fifoDelayTestQueue)
		result <- err
	}()
	var unblock sync.Once
	t.Cleanup(func() {
		unblock.Do(func() { close(gate.release) })
		cancel()
		<-joined
	})
	select {
	case <-gate.entered:
	case <-ctx.Done():
		t.Fatal("reindex page never reached its commit boundary")
	}

	freshID := applyDelayTestLifecycle(t, ctx, srv, partitions, action, handle)
	before, exists, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, srv.nextTxnReadTS(ctx))
	require.NoError(t, err)
	require.True(t, exists)
	unblock.Do(func() { close(gate.release) })
	require.ErrorIs(t, <-result, store.ErrWriteConflict, "a built page must lose its OCC check after the concurrent lifecycle operation")
	<-joined
	after, exists, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, srv.nextTxnReadTS(ctx))
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, before, after, "the stale page cannot restore its old generation, cursor or attributes")
	oldKey := sqsFifoOrderKey(oldMeta, oldMeta.Name, partitionFor(oldMeta, "g"), oldMeta.Generation, oldRecord)
	_, err = st.GetAt(ctx, oldKey, st.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound, "an abandoned page must not publish stale membership")
	dataKey := sqsMsgDataKeyDispatch(oldMeta, oldMeta.Name, partitionFor(oldMeta, "g"), oldMeta.Generation, first)
	raw, err := st.GetAt(ctx, dataKey, st.LastCommitTS())
	if action == "delete_message" {
		require.ErrorIs(t, err, store.ErrKeyNotFound, "the stale page must not resurrect the deleted receipt owner")
	} else {
		require.NoError(t, err)
		record, decodeErr := decodeSQSMessageRecord(raw)
		require.NoError(t, decodeErr)
		require.Equal(t, oldRecord, record, "purged generations may await cleanup, but an abandoned page cannot rewrite them")
	}
	require.NoError(t, srv.setQueueAttributesCore(ctx, fifoDelayTestQueue, map[string]string{"DelaySeconds": "0"}))
	messages, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1, "the current queue must remain usable after rejecting the stale page")
	if action == "delete_message" {
		require.Equal(t, "B", messages[0]["Body"])
	} else {
		require.Equal(t, freshID, messages[0]["MessageId"])
	}
}

func seekDelayTestPartition(t *testing.T, srv *SQSServer, group string) *sqsQueueMeta {
	t.Helper()
	for {
		ctx := t.Context()
		meta, exists, err := srv.loadQueueMetaAt(ctx, fifoDelayTestQueue, srv.nextTxnReadTS(ctx))
		require.NoError(t, err)
		require.True(t, exists)
		require.NotNil(t, meta.DelayReindex)
		if meta.DelayReindex.Partition == partitionFor(meta, group) {
			return meta
		}
		require.Less(t, meta.DelayReindex.Partition, partitionFor(meta, group))
		done, err := srv.advanceFifoDelayReindex(ctx, fifoDelayTestQueue)
		require.NoError(t, err)
		require.False(t, done)
	}
}

func applyDelayTestLifecycle(t *testing.T, ctx context.Context, srv *SQSServer, partitions uint32, action string, handle *decodedReceiptHandle) string {
	t.Helper()
	switch action {
	case "purge":
		require.NoError(t, srv.purgeQueueCore(ctx, fifoDelayTestQueue))
	case "delete_recreate":
		require.NoError(t, srv.deleteQueueCore(ctx, fifoDelayTestQueue))
		createDelayTestQueue(t, srv, partitions, 0)
	case "delete_message":
		require.NoError(t, srv.deleteMessageCore(ctx, fifoDelayTestQueue, handle))
		return ""
	default:
		t.Fatalf("unexpected lifecycle action: %s", action)
	}
	return sendDelayTestMessage(t, srv, "fresh", "g")
}
