package adapter

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func loadDelayTestRecord(t *testing.T, srv *SQSServer, name, id, group string) *sqsMessageRecord {
	t.Helper()
	ctx := context.Background()
	ts := srv.nextTxnReadTS(ctx)
	meta, exists, err := srv.loadQueueMetaAt(ctx, name, ts)
	require.NoError(t, err)
	require.True(t, exists)
	data, err := srv.store.GetAt(ctx, sqsMsgDataKeyDispatch(meta, name, partitionFor(meta, group), meta.Generation, id), ts)
	require.NoError(t, err)
	rec, err := decodeSQSMessageRecord(data)
	require.NoError(t, err)
	return rec
}

func delayNativeString(t *testing.T, out map[string]any, field string) string {
	t.Helper()
	value, ok := out[field].(string)
	require.True(t, ok, "expected string field %s", field)
	return value
}

func delayNativeMessage(t *testing.T, value any) map[string]any {
	t.Helper()
	message, ok := value.(map[string]any)
	require.True(t, ok)
	return message
}

func TestSQSFIFORetroactiveDelayNative(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	for _, mode := range []struct {
		name       string
		partitions int
		throughput string
	}{
		{"legacy", 1, ""}, {"ht", 4, "perMessageGroupId"}, {"per_queue", 4, "perQueue"},
	} {
		t.Run(mode.name, func(t *testing.T) {
			name := "retro-" + mode.name + ".fifo"
			attrs := map[string]string{"FifoQueue": "true", "DelaySeconds": "0"}
			if mode.partitions > 1 {
				attrs["PartitionCount"], attrs["FifoThroughputLimit"], attrs["DeduplicationScope"] = fmt.Sprint(mode.partitions), mode.throughput, "messageGroup"
			}
			status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{"QueueName": name, "Attributes": attrs})
			require.Equal(t, http.StatusOK, status, out)
			url := delayNativeString(t, out, "QueueUrl")
			send := func(body, group string) string {
				t.Helper()
				status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{"QueueUrl": url, "MessageBody": body, "MessageGroupId": group, "MessageDeduplicationId": body})
				require.Equal(t, http.StatusOK, status, out)
				return delayNativeString(t, out, "MessageId")
			}
			receive := func() []any {
				t.Helper()
				status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{"QueueUrl": url, "MaxNumberOfMessages": 10, "VisibilityTimeout": 900, "WaitTimeSeconds": 0})
				require.Equal(t, http.StatusOK, status, out)
				messages, _ := out["Messages"].([]any)
				return messages
			}
			set := func(delay string) {
				t.Helper()
				status, out := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{"QueueUrl": url, "Attributes": map[string]string{"DelaySeconds": delay}})
				require.Equal(t, http.StatusOK, status, out)
			}
			inflight := send("inflight", "busy")
			require.Len(t, receive(), 1)
			original := loadDelayTestRecord(t, node.sqsServer, name, inflight, "busy")
			first := send("A", "g")
			set("900")
			rec := loadDelayTestRecord(t, node.sqsServer, name, first, "g")
			require.Equal(t, rec.SendTimestampMillis+900_000, rec.VisibleAtMillis)
			require.Equal(t, rec.VisibleAtMillis, rec.AvailableAtMillis)
			require.Empty(t, receive(), "increasing delay applies to messages already eligible but never received")
			second := send("B", "g")
			other := send("C", "other")
			set("0")
			require.Equal(t, original, loadDelayTestRecord(t, node.sqsServer, name, inflight, "busy"), "delay changes must not change receipt state or inflight visibility")
			messages := receive()
			require.Len(t, messages, 2, "an inflight group must not prevent other groups from progressing")
			ids := make([]any, 0, len(messages))
			var firstHandle string
			for _, value := range messages {
				message := delayNativeMessage(t, value)
				ids = append(ids, message["MessageId"])
				if message["MessageId"] == first {
					firstHandle = delayNativeString(t, message, "ReceiptHandle")
				}
			}
			require.ElementsMatch(t, []any{first, other}, ids)
			require.Empty(t, receive(), "B remains behind undeleted A")
			status, out = callSQS(t, node, sqsDeleteMessageTarget, map[string]any{"QueueUrl": url, "ReceiptHandle": firstHandle})
			require.Equal(t, http.StatusOK, status, out)
			messages = receive()
			require.Len(t, messages, 1)
			require.Equal(t, second, delayNativeMessage(t, messages[0])["MessageId"])
		})
	}
}

func TestSQSStandardDelayChangeIsNotRetroactiveNative(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	for _, initial := range []string{"0", "900"} {
		t.Run(initial, func(t *testing.T) {
			name := "standard-delay-" + initial
			status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{"QueueName": name, "Attributes": map[string]string{"DelaySeconds": initial}})
			require.Equal(t, http.StatusOK, status, out)
			url := delayNativeString(t, out, "QueueUrl")
			send := func(body string) string {
				t.Helper()
				status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{"QueueUrl": url, "MessageBody": body})
				require.Equal(t, http.StatusOK, status, out)
				return delayNativeString(t, out, "MessageId")
			}
			first := send("A")
			before := loadDelayTestRecord(t, node.sqsServer, name, first, "")
			updated := "900"
			if initial == "900" {
				updated = "0"
			}
			status, out = callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{"QueueUrl": url, "Attributes": map[string]string{"DelaySeconds": updated}})
			require.Equal(t, http.StatusOK, status, out)
			require.Equal(t, before, loadDelayTestRecord(t, node.sqsServer, name, first, ""))
			second := send("B")
			status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{"QueueUrl": url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 0})
			require.Equal(t, http.StatusOK, status, out)
			messages, ok := out["Messages"].([]any)
			require.True(t, ok)
			require.Len(t, messages, 1)
			want := first
			if initial == "900" {
				want = second
			}
			require.Equal(t, want, delayNativeMessage(t, messages[0])["MessageId"])
			require.Greater(t, time.Now().UnixMilli(), before.SendTimestampMillis)
		})
	}
}

func TestSQSFIFODelayNativeMembershipCoverage(t *testing.T) {
	nodes, _, _ := createNode(t, 3)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	const name = "native-member-delay.fifo"
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": name, "Attributes": map[string]string{"FifoQueue": "true", "DelaySeconds": "0"},
	})
	require.Equal(t, http.StatusOK, status, out)
	url := delayNativeString(t, out, "QueueUrl")
	status, out = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": url, "MessageBody": "A", "MessageGroupId": "g", "MessageDeduplicationId": "A",
	})
	require.Equal(t, http.StatusOK, status, out)
	id := delayNativeString(t, out, "MessageId")
	status, _ = callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url, "Attributes": map[string]string{"DelaySeconds": "900"},
	})
	require.Equal(t, http.StatusServiceUnavailable, status, "a real multi-member configuration must not treat an empty SQS map as standalone")
	leaders := make(map[string]string, len(nodes))
	for _, peer := range nodes {
		leaders[peer.raftAddress] = peer.sqsAddress
	}
	node.sqsServer.leaderSQS = leaders
	for _, delay := range []string{"900", "0"} {
		status, out = callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
			"QueueUrl": url, "Attributes": map[string]string{"DelaySeconds": delay},
		})
		require.Equal(t, http.StatusOK, status, out)
	}
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl": url, "MaxNumberOfMessages": 10, "WaitTimeSeconds": 0,
	})
	require.Equal(t, http.StatusOK, status, out)
	messages, ok := out["Messages"].([]any)
	require.True(t, ok)
	require.Len(t, messages, 1)
	require.Equal(t, id, delayNativeMessage(t, messages[0])["MessageId"])
}
