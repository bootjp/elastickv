package adapter

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Public API calls on the native Raft/SQS fixture, with no injected records,
// fake clock, or wait for the original 900-second delay.
func TestSQSFIFODelayChangeDoesNotOvertakeHead(t *testing.T) {
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	for _, partitions := range []uint32{1, 4} {
		for _, scenario := range []struct {
			name         string
			initialDelay string
			changeDelay  bool
		}{
			{name: "immediate_control", initialDelay: "0"},
			{name: "delayed_control", initialDelay: "900"},
			{name: "delay_reduction", initialDelay: "900", changeDelay: true},
		} {
			t.Run(fmt.Sprintf("partitions_%d/%s", partitions, scenario.name), func(t *testing.T) {
				queueName := fmt.Sprintf("delay-repro-%d-%s.fifo", partitions, scenario.name)
				attrs := map[string]string{
					"FifoQueue": "true", "DelaySeconds": scenario.initialDelay,
				}
				if partitions > 1 {
					attrs["PartitionCount"] = fmt.Sprint(partitions)
					attrs["FifoThroughputLimit"] = "perMessageGroupId"
					attrs["DeduplicationScope"] = "messageGroup"
				}
				status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
					"QueueName": queueName, "Attributes": attrs,
				})
				require.Equal(t, http.StatusOK, status, out)
				queueURL, ok := out["QueueUrl"].(string)
				require.True(t, ok)
				require.NotEmpty(t, queueURL)

				send := func(body, dedup string) string {
					t.Helper()
					status, result := callSQS(t, node, sqsSendMessageTarget, map[string]any{
						"QueueUrl": queueURL, "MessageGroupId": "g",
						"MessageBody": body, "MessageDeduplicationId": dedup,
					})
					require.Equal(t, http.StatusOK, status, result)
					id, valid := result["MessageId"].(string)
					require.True(t, valid)
					require.NotEmpty(t, id)
					return id
				}
				firstID := send("A", "a")
				if scenario.changeDelay {
					before := loadDelayTestRecord(t, node.sqsServer, queueName, firstID, "g")
					require.Equal(t, before.SendTimestampMillis+900_000, before.VisibleAtMillis)
					require.Greater(t, before.VisibleAtMillis-time.Now().UnixMilli(), int64(time.Minute/time.Millisecond))
					status, result := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
						"QueueUrl": queueURL, "Attributes": map[string]string{"DelaySeconds": "0"},
					})
					require.Equal(t, http.StatusOK, status, result)
				}
				secondID := send("B", "b")

				srv := node.sqsServer
				ctx := context.Background()
				readTS := srv.nextTxnReadTS(ctx)
				meta, exists, err := srv.loadQueueMetaAt(ctx, queueName, readTS)
				require.NoError(t, err)
				require.True(t, exists)
				partition := partitionFor(meta, "g")
				load := func(id string) *sqsMessageRecord {
					t.Helper()
					raw, getErr := srv.store.GetAt(ctx, sqsMsgDataKeyDispatch(meta, queueName, partition, meta.Generation, id), readTS)
					require.NoError(t, getErr)
					record, decodeErr := decodeSQSMessageRecord(raw)
					require.NoError(t, decodeErr)
					return record
				}
				first, second := load(firstID), load(secondID)
				require.Less(t, first.SequenceNumber, second.SequenceNumber)
				lock, err := srv.loadFifoGroupLock(ctx, queueName, meta, partition, meta.Generation, "g", readTS)
				require.NoError(t, err)
				require.Nil(t, lock, "no prior receive or deletion may mask the send-order test")
				if scenario.changeDelay {
					require.Equal(t, first.SendTimestampMillis, first.VisibleAtMillis,
						"reducing queue delay must rewrite the persisted head deadline")
					require.Equal(t, first.VisibleAtMillis, first.AvailableAtMillis)
					require.LessOrEqual(t, second.VisibleAtMillis, time.Now().UnixMilli())
				}

				status, result := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
					"QueueUrl": queueURL, "MaxNumberOfMessages": 10,
					"VisibilityTimeout": 60, "WaitTimeSeconds": 0,
				})
				require.Equal(t, http.StatusOK, status, result)
				messages, _ := result["Messages"].([]any)
				if scenario.initialDelay == "900" && !scenario.changeDelay {
					require.Empty(t, messages)
					return
				}
				require.NotEmpty(t, messages, "eligible A must be delivered, not merely hidden to avoid an inversion")
				message, valid := messages[0].(map[string]any)
				require.True(t, valid)
				require.Equal(t, firstID, message["MessageId"], "later B must not overtake undeleted A in the same group")
				require.Equal(t, "A", message["Body"])
			})
		}
	}
}
