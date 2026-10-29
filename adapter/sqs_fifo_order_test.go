package adapter

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/bootjp/elastickv/store"
	"github.com/stretchr/testify/require"
)

// putFifoTieRecord stores a visible FIFO message (data record and
// visibility entry) at visibleAt, bypassing the send path so the test
// controls the MessageId and therefore the visibility-key order.
func putFifoTieRecord(t *testing.T, st store.MVCCStore, meta *sqsQueueMeta, id, group string, seq uint64, visibleAt int64) *sqsMessageRecord {
	t.Helper()
	ctx := context.Background()
	rec := &sqsMessageRecord{
		MessageID:         id,
		Body:              []byte(id),
		AvailableAtMillis: visibleAt,
		VisibleAtMillis:   visibleAt,
		QueueGeneration:   meta.Generation,
		MessageGroupId:    group,
		SequenceNumber:    seq,
	}
	body, err := encodeSQSMessageRecord(rec)
	require.NoError(t, err)
	require.NoError(t, st.PutAt(ctx, sqsMsgDataKeyDispatch(meta, meta.Name, 0, meta.Generation, id), body, 10, 0))
	require.NoError(t, st.PutAt(ctx, sqsMsgVisKeyDispatch(meta, meta.Name, 0, meta.Generation, visibleAt, id), []byte(id), 10, 0))
	return rec
}

// TestClassifyFifoCandidate_SameMillisecondKeepsSendOrder pins that a FIFO
// candidate cannot take a free group lock while an earlier send of its
// group shares its visible-at millisecond. The later send is given the
// MessageId that sorts first in the visibility index, which is the order
// the receive scan visits them in.
func TestClassifyFifoCandidate_SameMillisecondKeepsSendOrder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	st := store.NewMVCCStore()
	srv := &SQSServer{store: st}
	meta := &sqsQueueMeta{Name: "q.fifo", Generation: 1, IsFIFO: true}
	const visibleAt = int64(1_700_000_000_000)

	firstID := strings.Repeat("f", 32)
	laterID := strings.Repeat("0", 32)
	otherGroupID := strings.Repeat("1", 32)
	first := putFifoTieRecord(t, st, meta, firstID, "g", 1, visibleAt)
	later := putFifoTieRecord(t, st, meta, laterID, "g", 2, visibleAt)
	other := putFifoTieRecord(t, st, meta, otherGroupID, "h", 3, visibleAt)
	// A later send of group g alone in the next millisecond has no tie.
	next := putFifoTieRecord(t, st, meta, strings.Repeat("2", 32), "g", 4, visibleAt+1)

	// A stale visibility entry in the same millisecond whose record has
	// since moved to another visible-at time is not an earlier send.
	staleID := strings.Repeat("3", 32)
	putFifoTieRecord(t, st, meta, staleID, "s", 1, visibleAt+60_000)
	require.NoError(t, st.PutAt(ctx, sqsMsgVisKeyDispatch(meta, meta.Name, 0, meta.Generation, visibleAt, staleID), []byte(staleID), 10, 0))
	staleLater := putFifoTieRecord(t, st, meta, strings.Repeat("4", 32), "s", 2, visibleAt)

	laterVis := sqsMsgVisKeyDispatch(meta, meta.Name, 0, meta.Generation, visibleAt, laterID)
	firstVis := sqsMsgVisKeyDispatch(meta, meta.Name, 0, meta.Generation, visibleAt, firstID)
	require.Negative(t, bytes.Compare(laterVis, firstVis), "fixture: the later send must sort first in the visibility index")

	for _, tc := range []struct {
		name string
		rec  *sqsMessageRecord
		want fifoCandidateLockState
	}{
		{name: "later send behind an earlier same-millisecond send", rec: later, want: fifoLockSkip},
		{name: "earliest send of the group", rec: first, want: fifoLockAcquire},
		{name: "other group in the same millisecond", rec: other, want: fifoLockAcquire},
		{name: "send alone in its millisecond", rec: next, want: fifoLockAcquire},
		{name: "stale same-millisecond entry of the group", rec: staleLater, want: fifoLockAcquire},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cand := sqsMsgCandidate{
				visKey:    sqsMsgVisKeyDispatch(meta, meta.Name, 0, meta.Generation, tc.rec.VisibleAtMillis, tc.rec.MessageID),
				messageID: tc.rec.MessageID,
			}
			got, _, err := srv.classifyFifoCandidate(ctx, meta.Name, meta, cand, tc.rec, 20)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestSQSServer_FifoBatchDeliversInSendOrder sends one group's messages
// in a single SendMessageBatch, so several of them share a visible-at
// millisecond, and requires receive + delete to return them in send
// order.
func TestSQSServer_FifoBatchDeliversInSendOrder(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "fifo-batch-order.fifo",
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	url, _ := out["QueueUrl"].(string)

	const count = 10
	entries := make([]map[string]any, 0, count)
	for i := range count {
		entries = append(entries, map[string]any{
			"Id":                     fmt.Sprintf("e%d", i),
			"MessageBody":            fmt.Sprintf("m%d", i),
			"MessageGroupId":         "g",
			"MessageDeduplicationId": fmt.Sprintf("d%d", i),
		})
	}
	status, body := callSQS(t, node, sqsSendMessageBatchTarget, map[string]any{
		"QueueUrl": url,
		"Entries":  entries,
	})
	require.Equal(t, http.StatusOK, status, body)
	successful, _ := body["Successful"].([]any)
	require.Len(t, successful, count)

	for i := range count {
		_, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
			"QueueUrl":            url,
			"MaxNumberOfMessages": 10,
			"VisibilityTimeout":   60,
		})
		msgs, _ := out["Messages"].([]any)
		require.Len(t, msgs, 1, "the group lock must expose exactly the head (receive %d)", i)
		m, _ := msgs[0].(map[string]any)
		require.Equal(t, fmt.Sprintf("m%d", i), m["Body"], "receive %d out of send order", i)
		receipt, _ := m["ReceiptHandle"].(string)
		deleteMessageOK(t, node, url, receipt)
	}
}
