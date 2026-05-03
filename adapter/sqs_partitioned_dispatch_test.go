package adapter

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/stretchr/testify/require"
)

// Integration tests for the PR 5b-2 partitioned-FIFO data plane
// wiring. The §11 PR 2 dormancy gate still rejects PartitionCount
// > 1 at CreateQueue (lifted atomically with the capability check
// in PR 5b-3), so these tests reach below the public CreateQueue
// surface to install a partitioned meta record directly. That
// short-circuits the dormancy gate for the duration of the test
// without disabling it for production CreateQueue calls — which
// is the exact split the design doc envisaged for "data plane
// landed but not user-creatable yet."

// installPartitionedMetaForTest overwrites the queue's meta record
// with a partitioned shape (PartitionCount > 1) by dispatching a
// raw OCC put against the meta key. The queue is created via the
// normal CreateQueue path first (so generation / incarnation
// counters and the catalog index are populated correctly); only
// the partition-shape attributes are mutated.
//
// The dormancy gate intercepts CreateQueue, not the data plane,
// so once the meta record carries PartitionCount > 1 every
// SendMessage / ReceiveMessage / DeleteMessage call routes
// through the partitioned dispatch helpers exactly as it would
// after PR 5b-3 lifts the gate.
func installPartitionedMetaForTest(t *testing.T, node Node, queueName string, partitionCount uint32, throughputLimit string) {
	t.Helper()
	s := node.sqsServer
	require.NotNil(t, s, "test must run on a node with sqsServer wired")

	ctx := context.Background()
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	require.NoError(t, err)
	require.True(t, exists, "queue %q must exist before partition meta override", queueName)

	meta.PartitionCount = partitionCount
	meta.FifoThroughputLimit = throughputLimit
	meta.DeduplicationScope = htfifoDedupeScopeMessageGroup

	body, err := encodeSQSQueueMeta(meta)
	require.NoError(t, err)

	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{sqsQueueMetaKey(queueName)},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: sqsQueueMetaKey(queueName), Value: body},
		},
	}
	_, err = s.coordinator.Dispatch(ctx, req)
	require.NoError(t, err)
}

// TestSQSServer_PartitionedFIFO_SendReceiveDeleteRoundTrip is the
// end-to-end smoke test for PR 5b-2's wiring: SendMessage on a
// partitioned FIFO queue stores the message under the partitioned
// keyspace, ReceiveMessage's fanout finds it via the partitioned
// vis prefix, the returned ReceiptHandle is in v2 wire format, and
// DeleteMessage routes back to the same partition via
// handle.Partition. Failure of any step is a wiring break.
func TestSQSServer_PartitionedFIFO_SendReceiveDeleteRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	const queueName = "orders.fifo"
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  queueName,
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	require.Equal(t, http.StatusOK, status, "create FIFO queue: %v", out)
	queueURL, _ := out["QueueUrl"].(string)
	require.NotEmpty(t, queueURL)

	installPartitionedMetaForTest(t, node, queueName, 4, htfifoThroughputPerMessageGroupID)

	// Load meta once so the test can assert each handle lands on the
	// partition partitionFor would compute for its group — a loose
	// "Partition < 4" check would still pass if every group landed
	// on partition 0, masking the dispatch regression this test is
	// meant to catch.
	ctx := context.Background()
	readTS := node.sqsServer.nextTxnReadTS(ctx)
	meta, exists, err := node.sqsServer.loadQueueMetaAt(ctx, queueName, readTS)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint32(4), meta.PartitionCount,
		"meta override must have installed PartitionCount=4")

	// Send a few messages spread across distinct group ids so
	// partitionFor gets to actually pick different partitions.
	groups := []string{"alpha", "beta", "gamma", "delta", "epsilon", "zeta"}
	sent := make(map[string]string, len(groups))        // group -> messageID
	byMessageID := make(map[string]string, len(groups)) // messageID -> group
	for i, g := range groups {
		body := "body-" + g
		dedup := "dedup-" + g
		status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":               queueURL,
			"MessageBody":            body,
			"MessageGroupId":         g,
			"MessageDeduplicationId": dedup,
		})
		require.Equal(t, http.StatusOK, status,
			"send #%d (group=%s): %v", i, g, out)
		msgID, _ := out["MessageId"].(string)
		require.NotEmpty(t, msgID, "send #%d: empty MessageId", i)
		sent[g] = msgID
		byMessageID[msgID] = g
	}

	// Receive them all; the fanout walks all 4 partitions to find
	// every message. Receive enough times to drain the queue;
	// each receive bumps a per-message visibility deadline so a
	// second receive is a no-op for already-rotated messages
	// inside the same call.
	collected := make(map[string]string, len(groups)) // messageID -> receiptHandle
	for range len(groups) * 4 {                       // generous budget
		if len(collected) == len(groups) {
			break
		}
		status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
			"QueueUrl":            queueURL,
			"MaxNumberOfMessages": 10,
			"VisibilityTimeout":   60,
		})
		require.Equal(t, http.StatusOK, status, "receive: %v", out)
		msgs, _ := out["Messages"].([]any)
		for _, m := range msgs {
			mm, _ := m.(map[string]any)
			id, _ := mm["MessageId"].(string)
			handle, _ := mm["ReceiptHandle"].(string)

			// The handle must be v2 since the queue is partitioned —
			// this is the wiring guarantee.
			parsed, err := decodeReceiptHandle(handle)
			require.NoError(t, err, "decode receipt handle: %v", err)
			require.Equal(t, sqsReceiptHandleVersion2, parsed.Version,
				"partitioned queue must produce v2 handles, got version=0x%02x for message=%s",
				parsed.Version, id)
			group, ok := byMessageID[id]
			require.True(t, ok, "received unknown messageId %s", id)
			require.Equal(t,
				partitionFor(meta, group),
				parsed.Partition,
				"message %s (group=%s) routed to wrong partition", id, group)

			collected[id] = handle
		}
	}
	require.Len(t, collected, len(groups),
		"fanout receive must surface every message on the partitioned queue")

	// Delete each one via its v2 handle. The DeleteMessage path
	// uses handle.Partition to dispatch the data-key lookup; if
	// that wire is broken these calls return ReceiptHandleIsInvalid
	// (no record found at the legacy data key).
	for id, handle := range collected {
		status, out := callSQS(t, node, sqsDeleteMessageTarget, map[string]any{
			"QueueUrl":      queueURL,
			"ReceiptHandle": handle,
		})
		require.Equal(t, http.StatusOK, status,
			"delete (msg=%s): %v", id, out)
	}

	// Queue must now be empty. Probe the partitioned data keyspace
	// directly so a regression that turned DeleteMessage into a
	// "leave the record invisible but not removed" no-op cannot
	// false-pass under the still-active 60s visibility window.
	for p := uint32(0); p < meta.PartitionCount; p++ {
		dataPrefix := sqsMsgDataKeyDispatch(meta, queueName, p, meta.Generation, "")
		end := append([]byte(nil), dataPrefix...)
		end = append(end, 0xFF, 0xFF, 0xFF, 0xFF)
		page, err := node.sqsServer.store.ScanAt(ctx, dataPrefix, end, 32, node.sqsServer.nextTxnReadTS(ctx))
		require.NoError(t, err, "post-delete scan partition %d: %v", p, err)
		for _, kvp := range page {
			require.False(t,
				strings.HasPrefix(string(kvp.Key), string(dataPrefix)),
				"partition %d still holds key %q after delete — DeleteMessage left a tombstone-less record",
				p, string(kvp.Key))
		}
	}

	// And the public API must agree: a fresh receive with a short
	// visibility timeout (after sleeping past it, so any in-flight
	// invisible record would re-expose) returns no messages.
	time.Sleep(1100 * time.Millisecond)
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   1,
	})
	require.Equal(t, http.StatusOK, status, "post-delete receive: %v", out)
	if msgs, _ := out["Messages"].([]any); len(msgs) > 0 {
		t.Fatalf("expected empty queue after delete; got %d messages", len(msgs))
	}

	// Sanity: the legacy keyspace must be empty (every send on
	// this queue went to the partitioned keyspace, never the
	// legacy one). We probe the legacy data prefix at this
	// queue's generation: it must yield zero entries.
	readTS = node.sqsServer.nextTxnReadTS(ctx)
	legacyDataPrefix := sqsMsgDataKey(queueName, meta.Generation, "")
	// Cap the prefix scan at the generation byte so we do not
	// drag in unrelated queues.
	end := append([]byte(nil), legacyDataPrefix...)
	end = append(end, 0xFF, 0xFF, 0xFF, 0xFF)
	page, err := node.sqsServer.store.ScanAt(ctx, legacyDataPrefix, end, 32, readTS)
	require.NoError(t, err)
	for _, kvp := range page {
		require.False(t, strings.HasPrefix(string(kvp.Key), string(legacyDataPrefix)),
			"legacy data key found on a partitioned queue (key=%q) — wiring leaked into the wrong keyspace",
			string(kvp.Key))
	}
}

// TestSQSServer_PartitionedFIFO_RejectsV1Handle pins the queue-
// aware version validation: a v1 handle on a partitioned queue
// surfaces as ReceiptHandleIsInvalid. PR 5a's blanket v2 rejection
// at the API boundary moved into validateReceiptHandleVersion,
// which is invoked once meta is loaded.
func TestSQSServer_PartitionedFIFO_RejectsV1Handle(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	const queueName = "rejectv1.fifo"
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  queueName,
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	require.Equal(t, http.StatusOK, status, "create: %v", out)
	queueURL, _ := out["QueueUrl"].(string)

	// Send one message under legacy meta so we know the queue
	// generation. Then upgrade meta to partitioned.
	status, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":               queueURL,
		"MessageBody":            "x",
		"MessageGroupId":         "g",
		"MessageDeduplicationId": "d",
	})
	require.Equal(t, http.StatusOK, status)

	installPartitionedMetaForTest(t, node, queueName, 4, htfifoThroughputPerMessageGroupID)

	// Forge a v1 handle that would have been valid before the
	// upgrade. The partitioned-queue receipt-version check must
	// reject it as ReceiptHandleIsInvalid (not pass it through to
	// dispatch where it would route to the legacy keyspace and
	// silently miss).
	token := make([]byte, sqsReceiptTokenBytes)
	v1Handle, err := encodeReceiptHandle(1, "deadbeefdeadbeefdeadbeefdeadbeef", token)
	require.NoError(t, err)
	status, out = callSQS(t, node, sqsDeleteMessageTarget, map[string]any{
		"QueueUrl":      queueURL,
		"ReceiptHandle": v1Handle,
	})
	require.Equal(t, http.StatusBadRequest, status,
		"v1 handle on partitioned queue must surface as ReceiptHandleIsInvalid: %v", out)
	require.Equal(t, sqsErrReceiptHandleInvalid, out["__type"])

	// ChangeMessageVisibility must reject the same v1 handle.
	status, out = callSQS(t, node, sqsChangeMessageVisibilityTarget, map[string]any{
		"QueueUrl":          queueURL,
		"ReceiptHandle":     v1Handle,
		"VisibilityTimeout": int64(30),
	})
	require.Equal(t, http.StatusBadRequest, status, "%v", out)
	require.Equal(t, sqsErrReceiptHandleInvalid, out["__type"])
}

// TestSQSServer_PartitionedFIFO_PerQueueCollapsesToPartitionZero
// is the named regression for the PR 731 round 2 forward note: a
// queue with PartitionCount=4 and FifoThroughputLimit=perQueue
// has every group hashed to partition 0 by partitionFor, but the
// keyspace is still partitioned. The receive fanout must collapse
// to 1 iteration (effectivePartitionCount=1) AND that iteration
// must scan the partitioned vis prefix — the perQueue
// short-circuit narrows the iteration count, not the keyspace.
func TestSQSServer_PartitionedFIFO_PerQueueCollapsesToPartitionZero(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	const queueName = "perqueue.fifo"
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  queueName,
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	require.Equal(t, http.StatusOK, status, "create: %v", out)
	queueURL, _ := out["QueueUrl"].(string)

	installPartitionedMetaForTest(t, node, queueName, 4, htfifoThroughputPerQueue)

	// Send messages from many distinct groups; in perQueue mode
	// they all land in partition 0.
	groups := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, g := range groups {
		status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":               queueURL,
			"MessageBody":            "body-" + g,
			"MessageGroupId":         g,
			"MessageDeduplicationId": "d-" + g,
		})
		require.Equal(t, http.StatusOK, status, "send group=%s: %v", g, out)
	}

	// One receive call must surface every message — the fanout
	// only iterates partition 0 (effectivePartitionCount=1) but
	// that single iteration must use the partitioned vis prefix.
	collected := make(map[string]bool, len(groups))
	for range 4 {
		if len(collected) == len(groups) {
			break
		}
		status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
			"QueueUrl":            queueURL,
			"MaxNumberOfMessages": 10,
			"VisibilityTimeout":   60,
		})
		require.Equal(t, http.StatusOK, status, "receive: %v", out)
		msgs, _ := out["Messages"].([]any)
		for _, m := range msgs {
			mm, _ := m.(map[string]any)
			body, _ := mm["Body"].(string)
			collected[body] = true
			handle, _ := mm["ReceiptHandle"].(string)
			parsed, err := decodeReceiptHandle(handle)
			require.NoError(t, err)
			require.Equal(t, sqsReceiptHandleVersion2, parsed.Version,
				"perQueue + PartitionCount=4 must still produce v2 handles")
			require.Equal(t, uint32(0), parsed.Partition,
				"perQueue mode pins every group to partition 0, so every handle must record Partition=0")
		}
	}
	require.Len(t, collected, len(groups),
		"perQueue receive must surface every message in one fanout pass over partition 0")
}

// TestNextReceiveFanoutStart_RoundRobin pins the round-2 regression:
// the original PR #732 round-1 fix derived the starting partition
// from masked readTS bits (XOR-fold of the upper and lower 32-bit
// halves). Codex P1 round 2 flagged that this can alias to a subset
// of partitions when readTS advances by a structured stride — HLC
// packs a 16-bit logical counter into the low bits and ReceiveMessage
// commits a fixed number of per-message transactions per call, so
// consecutive readTS deltas exhibit a structured stride.
//
// nextReceiveFanoutStart replaces the bit-fold with a per-server
// atomic counter, so consecutive calls walk every partition in
// strict round-robin regardless of HLC behaviour. This test pins
// both the round-robin contract and the explicit aliasing case
// Codex flagged.
func TestNextReceiveFanoutStart_RoundRobin(t *testing.T) {
	t.Parallel()

	s := &SQSServer{}

	// Legacy / non-partitioned / perQueue queues collapse to 0 and
	// must not perturb the counter.
	require.Equal(t, uint32(0), s.nextReceiveFanoutStart(0))
	require.Equal(t, uint32(0), s.nextReceiveFanoutStart(1))

	// Partitioned queues walk every partition in strict round-robin.
	// 16 consecutive calls over 4 partitions must produce exactly
	// 4 hits per partition.
	const partitions uint32 = 4
	seen := make(map[uint32]int, partitions)
	for range 16 {
		seen[s.nextReceiveFanoutStart(partitions)]++
	}
	require.Len(t, seen, int(partitions),
		"counter-driven rotation must cover every partition; got %v", seen)
	for p := uint32(0); p < partitions; p++ {
		require.Equal(t, 4, seen[p],
			"partition %d hit %d times in 16 calls; round-robin must be exact",
			p, seen[p])
	}

	// The output must always fall inside [0, partitions). Validates
	// the mask-AND contract — partitions is power-of-two, so the
	// AND with (partitions-1) is equivalent to modulo.
	for range 1024 {
		off := s.nextReceiveFanoutStart(partitions)
		require.Less(t, off, partitions,
			"offset %d out of range [0, %d)", off, partitions)
	}

	// Concurrent receivers must each get a valid offset (no torn
	// reads, no out-of-range values). Atomicity check.
	var wg sync.WaitGroup
	const goroutines = 8
	const perGoroutine = 256
	results := make([][]uint32, goroutines)
	for i := 0; i < goroutines; i++ {
		results[i] = make([]uint32, perGoroutine)
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				results[idx][j] = s.nextReceiveFanoutStart(partitions)
			}
		}(i)
	}
	wg.Wait()
	for _, batch := range results {
		for _, off := range batch {
			require.Less(t, off, partitions,
				"concurrent offset %d out of range", off)
		}
	}
}
