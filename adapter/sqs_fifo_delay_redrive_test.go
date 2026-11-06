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

const fifoDelayDLQ = "delay-dead.fifo"

func createDelayDLQ(t *testing.T, srv *SQSServer) {
	t.Helper()
	_, err := srv.createQueueCore(context.Background(), &sqsCreateQueueInput{QueueName: fifoDelayDLQ, Attributes: map[string]string{
		"FifoQueue": "true", "PartitionCount": "4", "FifoThroughputLimit": "perMessageGroupId", "DeduplicationScope": "messageGroup",
	}})
	require.NoError(t, err)
	policy := fmt.Sprintf(`{"deadLetterTargetArn":%q,"maxReceiveCount":1}`, srv.queueArn(fifoDelayDLQ))
	require.NoError(t, srv.setQueueAttributesCore(context.Background(), fifoDelayTestQueue, map[string]string{"RedrivePolicy": policy}))
}

func TestSQSFIFODelayFencesStaleRedrive(t *testing.T) {
	for _, fenceSource := range []bool{false, true} {
		t.Run(fmt.Sprint(fenceSource), func(t *testing.T) {
			st := store.NewMVCCStore()
			t.Cleanup(func() { require.NoError(t, st.Close()) })
			srv := newDelayTestServer(t, st)
			createDelayTestQueue(t, srv, 4, 0)
			createDelayDLQ(t, srv)
			id := sendDelayTestMessage(t, srv, "poison", "g")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			messages, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 1, VisibilityTimeout: 0})
			require.NoError(t, err)
			require.Len(t, messages, 1)
			gate := &fifoDelayBarrierCoordinator{Coordinator: srv.coordinator, match: delayDataPut, entered: make(chan struct{}), release: make(chan struct{})}
			srv.coordinator = gate
			done, finished := make(chan error, 1), make(chan struct{})
			go func() {
				defer close(finished)
				_, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 1, VisibilityTimeout: 60})
				done <- err
			}()
			var unblock sync.Once
			t.Cleanup(func() {
				unblock.Do(func() { close(gate.release) })
				cancel()
				<-finished
			})
			select {
			case <-gate.entered:
			case <-ctx.Done():
				t.Fatal("redrive never reached its commit")
			}
			queue := fifoDelayDLQ
			if fenceSource {
				queue = fifoDelayTestQueue
			}
			_, _, _, _, started, err := srv.trySetQueueAttributesOnce(ctx, queue, map[string]string{"DelaySeconds": "900"})
			require.NoError(t, err)
			require.True(t, started)
			unblock.Do(func() { close(gate.release) })
			require.NoError(t, <-done, "the known OCC abort may return a short receive but must not enqueue")
			src := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, "g")
			require.EqualValues(t, 1, src.ReceiveCount)
			dlq, _, err := srv.loadQueueMetaAt(ctx, fifoDelayDLQ, srv.nextTxnReadTS(ctx))
			require.NoError(t, err)
			_, err = st.GetAt(ctx, sqsMsgDataKeyDispatch(dlq, fifoDelayDLQ, partitionFor(dlq, "g"), dlq.Generation, id), st.LastCommitTS())
			require.ErrorIs(t, err, store.ErrKeyNotFound)
			require.NoError(t, srv.completeFifoDelayReindex(ctx, queue))
		})
	}
}

func delayGroupOnPartition(meta *sqsQueueMeta, partition uint32) string {
	for i := 0; ; i++ {
		group := fmt.Sprintf("group-%d", i)
		if partitionFor(meta, group) == partition {
			return group
		}
	}
}

func TestSQSFIFODelayRedriveCannotEnterCompletedBackfillPartition(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 4, 0)
	createDelayDLQ(t, srv)
	ctx := context.Background()
	dlq, _, err := srv.loadQueueMetaAt(ctx, fifoDelayDLQ, srv.nextTxnReadTS(ctx))
	require.NoError(t, err)
	group := delayGroupOnPartition(dlq, 0)
	id := sendDelayTestMessage(t, srv, "poison", group)
	messages, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 1, VisibilityTimeout: 0})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	_, _, _, _, done, err := srv.trySetQueueAttributesOnce(ctx, fifoDelayDLQ, map[string]string{"DelaySeconds": "900"})
	require.NoError(t, err)
	require.True(t, done)
	done, err = srv.advanceFifoDelayReindex(ctx, fifoDelayDLQ)
	require.NoError(t, err)
	require.False(t, done)
	dlq, _, err = srv.loadQueueMetaAt(ctx, fifoDelayDLQ, srv.nextTxnReadTS(ctx))
	require.NoError(t, err)
	require.False(t, dlq.FifoOrderIndexed)
	require.NotNil(t, dlq.DelayReindex)
	require.EqualValues(t, 1, dlq.DelayReindex.Partition, "partition 0 must be durably behind the cursor before the redrive attempt")
	_, err = srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 1, VisibilityTimeout: 60})
	requireDelayUnavailable(t, err)
	src := loadDelayTestRecord(t, srv, fifoDelayTestQueue, id, group)
	require.EqualValues(t, 1, src.ReceiveCount, "a rejected redrive must retain source data and its receipt state")
	_, err = st.GetAt(ctx, sqsMsgDataKeyDispatch(dlq, fifoDelayDLQ, 0, dlq.Generation, id), st.LastCommitTS())
	require.ErrorIs(t, err, store.ErrKeyNotFound, "no unindexed enqueue may appear behind the backfill cursor")
	require.NoError(t, srv.completeFifoDelayReindex(ctx, fifoDelayDLQ))
	messages, err = srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 1, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Empty(t, messages)
	dlq, _, err = srv.loadQueueMetaAt(ctx, fifoDelayDLQ, srv.nextTxnReadTS(ctx))
	require.NoError(t, err)
	require.True(t, dlq.FifoOrderIndexed)
	rec := loadDelayTestRecord(t, srv, fifoDelayDLQ, id, group)
	member, err := st.GetAt(ctx, sqsFifoOrderKey(dlq, fifoDelayDLQ, 0, dlq.Generation, rec), st.LastCommitTS())
	require.NoError(t, err)
	require.Equal(t, id, string(member))
}

func TestSQSFIFOIndexedDelayedHeadPrecedesImmediateRedrive(t *testing.T) {
	st := store.NewMVCCStore()
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	srv := newDelayTestServer(t, st)
	createDelayTestQueue(t, srv, 4, 0)
	createDelayDLQ(t, srv)
	ctx := context.Background()
	out, err := srv.sendMessageCore(ctx, fifoDelayDLQ, sqsSendMessageInput{MessageBody: "A", MessageGroupId: "g", MessageDeduplicationId: "A"})
	require.NoError(t, err)
	first := out["MessageId"]
	require.NoError(t, srv.setQueueAttributesCore(ctx, fifoDelayDLQ, map[string]string{"DelaySeconds": "900"}))
	second := sendDelayTestMessage(t, srv, "B", "g")
	for range 2 {
		_, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 0})
		require.NoError(t, err)
	}
	b := loadDelayTestRecord(t, srv, fifoDelayDLQ, second, "g")
	a := loadDelayTestRecord(t, srv, fifoDelayDLQ, first, "g")
	require.Less(t, a.SequenceNumber, b.SequenceNumber)
	require.Less(t, b.VisibleAtMillis, a.VisibleAtMillis, "redrive retains its preexisting immediate-delivery semantics")
	messages, err := srv.scanAndDeliverOnce(ctx, fifoDelayDLQ, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Empty(t, messages, "the visible successor must not overtake its delayed group head")
	other := sendDelayTestMessage(t, srv, "C", "other")
	for range 2 {
		_, err := srv.scanAndDeliverOnce(ctx, fifoDelayTestQueue, sqsReceiveOptions{Max: 10, VisibilityTimeout: 0})
		require.NoError(t, err)
	}
	messages, err = srv.scanAndDeliverOnce(ctx, fifoDelayDLQ, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, other, messages[0]["MessageId"], "an unrelated visible group must progress past the delayed group")
	require.NoError(t, srv.setQueueAttributesCore(ctx, fifoDelayDLQ, map[string]string{"DelaySeconds": "0"}))
	messages, err = srv.scanAndDeliverOnce(ctx, fifoDelayDLQ, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, first, messages[0]["MessageId"])
	handle, err := decodeClientReceiptHandle(delayNativeString(t, messages[0], "ReceiptHandle"))
	require.NoError(t, err)
	require.NoError(t, srv.deleteMessageCore(ctx, fifoDelayDLQ, handle))
	messages, err = srv.scanAndDeliverOnce(ctx, fifoDelayDLQ, sqsReceiveOptions{Max: 10, VisibilityTimeout: 60})
	require.NoError(t, err)
	require.Len(t, messages, 1)
	require.Equal(t, second, messages[0]["MessageId"])
}
