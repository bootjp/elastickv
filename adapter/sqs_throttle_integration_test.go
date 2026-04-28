package adapter

import (
	"net/http"
	"strconv"
	"testing"
)

// TestSQSServer_Throttle_DefaultOff_AllowsUnboundedSends pins the
// default-off contract: a queue without any Throttle* attributes
// accepts arbitrary send volume. The hot path for unconfigured queues
// must never reject — anything else would change steady-state
// behaviour for every existing deployment that hasn't opted in.
func TestSQSServer_Throttle_DefaultOff_AllowsUnboundedSends(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateQueue(t, node, "throttle-off")
	for i := range 50 {
		status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    url,
			"MessageBody": "msg-" + strconv.Itoa(i),
		})
		if status != http.StatusOK {
			t.Fatalf("default-off send #%d: status %d", i+1, status)
		}
	}
}

// TestSQSServer_Throttle_SendBucketRejectsAfterCapacity is the §6
// item 2 end-to-end test: configure SendCapacity=10 RefillPerSec=1,
// send 10 → 200, send 11th immediately → 400 Throttling with
// Retry-After. Pins the wire-level contract: the 400 carries the
// AWS-shaped error envelope and the Retry-After header.
func TestSQSServer_Throttle_SendBucketRejectsAfterCapacity(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateQueue(t, node, "throttle-send")
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        "10",
		"ThrottleSendRefillPerSecond": "1",
	})
	for i := range 10 {
		status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    url,
			"MessageBody": "msg-" + strconv.Itoa(i),
		})
		if status != http.StatusOK {
			t.Fatalf("send #%d: status %d (expected 200)", i+1, status)
		}
	}
	// 11th must reject with Throttling + Retry-After.
	resp := postSQSRequest(t, "http://"+node.sqsAddress+"/", sqsSendMessageTarget, `{"QueueUrl":"`+url+`","MessageBody":"overflow"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("11th send: status %d (expected 400)", resp.StatusCode)
	}
	if got := resp.Header.Get("x-amzn-ErrorType"); got != sqsErrThrottling {
		t.Fatalf("11th send: x-amzn-ErrorType=%q (expected Throttling)", got)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "1" {
		t.Fatalf("11th send: Retry-After=%q (expected 1)", ra)
	}
}

// TestSQSServer_Throttle_RecvBucketRejectsAfterCapacity is the
// receive-side mirror of the send test. ReceiveMessage charges 1
// from Recv regardless of MaxNumberOfMessages, so 11 calls drain a
// capacity-10 bucket. Empty queue is fine — the throttle check sits
// before any catalog read.
func TestSQSServer_Throttle_RecvBucketRejectsAfterCapacity(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateQueue(t, node, "throttle-recv")
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleRecvCapacity":        "10",
		"ThrottleRecvRefillPerSecond": "1",
	})
	for i := range 10 {
		status, _ := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{"QueueUrl": url})
		if status != http.StatusOK {
			t.Fatalf("recv #%d: status %d (expected 200)", i+1, status)
		}
	}
	resp := postSQSRequest(t, "http://"+node.sqsAddress+"/", sqsReceiveMessageTarget, `{"QueueUrl":"`+url+`"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("11th recv: status %d (expected 400)", resp.StatusCode)
	}
	if got := resp.Header.Get("x-amzn-ErrorType"); got != sqsErrThrottling {
		t.Fatalf("11th recv: x-amzn-ErrorType=%q", got)
	}
}

// TestSQSServer_Throttle_BatchChargesByEntryCount pins the §3.3
// charging table for SendMessageBatch: 10 entries → 10 tokens. With
// SendCapacity=10 a single 10-entry batch drains the bucket and the
// next single SendMessage rejects.
func TestSQSServer_Throttle_BatchChargesByEntryCount(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateQueue(t, node, "throttle-batch")
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        "10",
		"ThrottleSendRefillPerSecond": "1",
	})
	entries := make([]map[string]any, 10)
	for i := range entries {
		entries[i] = map[string]any{
			"Id":          "id" + strconv.Itoa(i),
			"MessageBody": "body-" + strconv.Itoa(i),
		}
	}
	status, _ := callSQS(t, node, sqsSendMessageBatchTarget, map[string]any{
		"QueueUrl": url,
		"Entries":  entries,
	})
	if status != http.StatusOK {
		t.Fatalf("10-entry batch: status %d (expected 200)", status)
	}
	// Bucket now empty. Single send must reject.
	resp := postSQSRequest(t, "http://"+node.sqsAddress+"/", sqsSendMessageTarget, `{"QueueUrl":"`+url+`","MessageBody":"overflow"}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("post-batch single send: status %d (expected 400)", resp.StatusCode)
	}
}

// TestSQSServer_Throttle_SetQueueAttributesInvalidatesBucket pins the
// §3.1 cache-invalidation contract for SetQueueAttributes: a raise of
// the limit must take effect on the very next request, not after the
// 1h idle-evict sweep. Without invalidation the test's final send
// would still reject under the old (now-empty) bucket.
func TestSQSServer_Throttle_SetQueueAttributesInvalidatesBucket(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateQueue(t, node, "throttle-invalidate")
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        "10",
		"ThrottleSendRefillPerSecond": "1",
	})
	// Drain.
	for range 10 {
		status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl": url, "MessageBody": "drain",
		})
		if status != http.StatusOK {
			t.Fatalf("drain send: status %d", status)
		}
	}
	// Sanity-check exhaustion.
	status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": url, "MessageBody": "should-throttle",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected throttle, got %d", status)
	}
	// Raise capacity and refill.
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        "20",
		"ThrottleSendRefillPerSecond": "20",
	})
	// Immediate send must succeed — a fresh bucket starts at capacity.
	status, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": url, "MessageBody": "post-set",
	})
	if status != http.StatusOK {
		t.Fatalf("post-SetQueueAttributes send: status %d (expected 200; bucket invalidation broken)", status)
	}
}

// TestSQSServer_Throttle_DeleteQueueInvalidatesBucket pins the §3.1
// cache-invalidation contract for DeleteQueue: a same-name recreate
// gets a fresh bucket, not the stale balance from the previous
// incarnation. Without invalidation the post-recreate send would
// inherit the drained bucket and reject.
func TestSQSServer_Throttle_DeleteQueueInvalidatesBucket(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	queue := "throttle-recreate"
	url := mustCreateQueue(t, node, queue)
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        "10",
		"ThrottleSendRefillPerSecond": "1",
	})
	for range 10 {
		_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl": url, "MessageBody": "drain",
		})
	}
	// Delete and recreate.
	if status, _ := callSQS(t, node, sqsDeleteQueueTarget, map[string]any{"QueueUrl": url}); status != http.StatusOK {
		t.Fatalf("delete: %d", status)
	}
	url2 := mustCreateQueue(t, node, queue)
	mustSetQueueAttributes(t, node, url2, map[string]string{
		"ThrottleSendCapacity":        "10",
		"ThrottleSendRefillPerSecond": "1",
	})
	// First send on the recreated queue must succeed (full-capacity bucket).
	status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": url2, "MessageBody": "fresh",
	})
	if status != http.StatusOK {
		t.Fatalf("post-recreate send: status %d (bucket invalidation on DeleteQueue broken)", status)
	}
}

// TestSQSServer_Throttle_GetQueueAttributesRoundTrip pins the §6 item
// 4 contract: SetQueueAttributes(Throttle*) followed by
// GetQueueAttributes("All") returns the same values. SDKs use the
// round-trip to confirm the config landed; a missing field on
// GetQueueAttributes would make operators think the SetQueueAttributes
// call silently failed.
func TestSQSServer_Throttle_GetQueueAttributesRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateQueue(t, node, "throttle-roundtrip")
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        "100",
		"ThrottleSendRefillPerSecond": "50",
		"ThrottleRecvCapacity":        "20",
		"ThrottleRecvRefillPerSecond": "5",
	})
	status, out := callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("GetQueueAttributes: status %d", status)
	}
	attrs, _ := out["Attributes"].(map[string]any)
	if attrs == nil {
		t.Fatalf("missing Attributes in response: %v", out)
	}
	expect := map[string]string{
		"ThrottleSendCapacity":        "100",
		"ThrottleSendRefillPerSecond": "50",
		"ThrottleRecvCapacity":        "20",
		"ThrottleRecvRefillPerSecond": "5",
	}
	for k, want := range expect {
		got, _ := attrs[k].(string)
		if got != want {
			t.Fatalf("attr %s: got %q want %q (full attrs: %v)", k, got, want, attrs)
		}
	}
}

// TestSQSServer_Throttle_RejectsCapacityBelowBatchMin pins the §3.2
// validator's batch-floor rule: SendCapacity < 10 makes every full
// batch permanently unserviceable. The SetQueueAttributes call is
// rejected with InvalidAttributeValue rather than landing.
func TestSQSServer_Throttle_RejectsCapacityBelowBatchMin(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateQueue(t, node, "throttle-bad-cap")
	status, out := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"ThrottleSendCapacity":        "5",
			"ThrottleSendRefillPerSecond": "1",
		},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected 400 for SendCapacity=5 (< batch min 10), got %d body %v", status, out)
	}
	if got, _ := out["__type"].(string); got != sqsErrInvalidAttributeValue {
		t.Fatalf("expected __type=InvalidAttributeValue, got %q", got)
	}
}

// --- helpers ---

func mustCreateQueue(t *testing.T, node Node, name string) string {
	t.Helper()
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{"QueueName": name})
	if status != http.StatusOK {
		t.Fatalf("createQueue %q: status %d body %v", name, status, out)
	}
	url, _ := out["QueueUrl"].(string)
	if url == "" {
		t.Fatalf("createQueue %q: missing QueueUrl", name)
	}
	return url
}

// TestSQSServer_Throttle_NoOpSetQueueAttributesPreservesBucket pins
// the Codex P1 fix on PR #664 round 9. Earlier code invalidated the
// throttle bucket whenever any Throttle* attribute appeared in a
// SetQueueAttributes request — including same-value writes. A caller
// could therefore force the bucket back to full capacity by
// re-submitting their own current throttle config in a tight loop,
// effectively bypassing the rate limit. The fix gates the
// invalidation on a real value change (snapshot before applyAttributes,
// throttleConfigEqual after), so a no-op SetQueueAttributes leaves
// the in-memory bucket alone.
func TestSQSServer_Throttle_NoOpSetQueueAttributesPreservesBucket(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	url := mustCreateQueue(t, node, "throttle-noop-set")
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        "10",
		"ThrottleSendRefillPerSecond": "1",
	})
	// Drain the bucket so the next charge would only succeed if the
	// bucket was reset to a fresh full-capacity replacement.
	for range 10 {
		status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl": url, "MessageBody": "drain",
		})
		if status != http.StatusOK {
			t.Fatalf("drain send: status %d", status)
		}
	}
	// Sanity-check: drained bucket rejects.
	status, _ := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": url, "MessageBody": "should-throttle",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("expected throttle, got %d", status)
	}
	// Re-submit identical Throttle* values. Old code invalidated on
	// key presence and the next send would have been allowed against
	// a fresh full bucket.
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        "10",
		"ThrottleSendRefillPerSecond": "1",
	})
	// Bucket must still be drained — no-op SetQueueAttributes must not
	// reset the rate limiter.
	status, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": url, "MessageBody": "post-noop-set",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("post-no-op SetQueueAttributes send: status %d (expected 400; "+
			"no-op invalidate-bypass regression)", status)
	}
}

func mustSetQueueAttributes(t *testing.T, node Node, url string, attrs map[string]string) {
	t.Helper()
	status, out := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl":   url,
		"Attributes": attrs,
	})
	if status != http.StatusOK {
		t.Fatalf("setQueueAttributes(%v): status %d body %v", attrs, status, out)
	}
}
