package adapter

import (
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// === SQS throttle test refill-rate guardrail ============================
//
// The throttle refill rate in this file is a recurring CI-flake vector.
// Every test that (a) sets a `ThrottleSend/RecvRefillPerSecond` AND
// (b) drains the bucket then asserts a "throttled" response is racing
// its own wall clock — under `-race` on slow CI runners each request
// elapses ~100-250 ms (Raft propose+apply), so an N-write drain plus
// the assertion send takes ~N×150 ms ± noise. At Refill=1/sec the
// bucket accumulates ≥1 token over that window and the "throttled"
// assertion fires against a refilled bucket, returning 200 instead
// of the expected 400.
//
// Three commits have fixed exactly this in three different tests:
//   54c6cd56  NoOpSetQueueAttributesPreservesBucket  (PR #819)
//   204b7294  SetQueueAttributesInvalidatesBucket    (PR #890)
//   <THIS>    structural fix across the remaining drain-pattern tests
//
// Rule for new tests in this file:
//
//   • If the test drains a bucket and asserts a 400 throttle, the
//     refill MUST be `slowRefillRate` (1 token per 100 s). Use the
//     helpers `newSendThrottleAttrs` / `newRecvThrottleAttrs`.
//   • If the test does NOT drain (config round-trip, validator
//     rejection, post-set fresh-bucket exercise), the refill rate is
//     part of the test contract and can be any value.
//
// Helpers below encode the rule so a future test writer who reaches
// for `mustSetQueueAttributes(... "ThrottleSendRefillPerSecond": "1" ...)`
// at least has to think about why they didn't use `newSendThrottleAttrs`.

const (
	// slowRefillRate is 1 token per 100 seconds — slow enough that no
	// realistic test-window wall-clock can accumulate to a whole
	// token. The throttle-config validator accepts fractional
	// `float64` (see `sqs_catalog.go` `SendRefillPerSecond float64`)
	// and `0.01 != 0` keeps `IsEmpty()` returning false, so
	// throttling stays enabled.
	slowRefillRate = "0.01"

	// drainBucketCapacity is the canonical capacity for the
	// drain-pattern tests in this file. 10 is the §3.2 validator's
	// batch-floor minimum (SendCapacity ≥ 10 required so the §3.3
	// charging table's 10-entry batch can succeed), so it doubles
	// as the smallest value that can drain via either single or
	// batch sends. A future test needing a different capacity
	// should not use these helpers — the helpers exist to encode
	// the *one* canonical shape for drain-then-assert.
	drainBucketCapacity = "10"
)

// newSendThrottleAttrs returns an attribute map for a drain-then-assert
// send-side throttle test. Both fields are locked: capacity to
// drainBucketCapacity (the validator's batch-floor minimum), refill
// to slowRefillRate (immune to the wall-clock race). Tests that
// genuinely need different values are out of scope for this helper
// and should construct the map literally.
func newSendThrottleAttrs() map[string]string {
	return map[string]string{
		"ThrottleSendCapacity":        drainBucketCapacity,
		"ThrottleSendRefillPerSecond": slowRefillRate,
	}
}

// newRecvThrottleAttrs is the receive-side mirror of
// newSendThrottleAttrs. ReceiveMessage charges 1 from
// ThrottleRecvCapacity regardless of MaxNumberOfMessages, so the
// same drain-then-assert race applies.
func newRecvThrottleAttrs() map[string]string {
	return map[string]string{
		"ThrottleRecvCapacity":        drainBucketCapacity,
		"ThrottleRecvRefillPerSecond": slowRefillRate,
	}
}

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
	// Drain-then-assert pattern — must use slowRefillRate (see file-head
	// guardrail). Capacity=10 plays the same role as before.
	mustSetQueueAttributes(t, node, url, newSendThrottleAttrs())
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
	// Retry-After is `ceil((1 - currentTokens) / refillRate)` per
	// sqs_throttle.go computeRetryAfter (§3.4). With refillRate=0.01
	// and a freshly drained bucket the integer result is in
	// [1, 100]: the floor comes from the function's `secs < 1`
	// guard; the ceiling is the (requested=1)/(refill=0.01) limit.
	// The exact value depends on `currentTokens` at the moment the
	// rejected charge fires, which is racing the test's own
	// drain-loop wall clock (each drain send refills 0.01 × elapsed
	// tokens) — asserting "100" exactly would just re-introduce the
	// wall-clock flake at the assertion layer. Pin the range
	// instead so the §3.4 formula's contract (positive integer,
	// bounded by 1/refillRate) is still verified without re-racing.
	ra := resp.Header.Get("Retry-After")
	raSecs, raErr := strconv.Atoi(ra)
	if raErr != nil {
		t.Fatalf("11th send: Retry-After=%q is not an integer: %v", ra, raErr)
	}
	if raSecs < 1 || raSecs > 100 {
		t.Fatalf("11th send: Retry-After=%d out of [1,100] (slowRefillRate=0.01 ⇒ ceil(1/0.01)=100; floor from sqs_throttle.go computeRetryAfter)", raSecs)
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
	// Drain-then-assert pattern — use the receive-side helper to keep
	// the refill rate below any wall-clock window the test can elapse.
	mustSetQueueAttributes(t, node, url, newRecvThrottleAttrs())
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
	// Drain-then-assert pattern (the batch fully drains; the next single
	// send must reject) — use slow refill so the inter-call gap cannot
	// refill a token.
	mustSetQueueAttributes(t, node, url, newSendThrottleAttrs())
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
	// Drain-then-sanity-throttle pattern in the first phase, then
	// raise Capacity/Refill in the second phase. The first phase
	// must use slowRefillRate per the file-head guardrail; the
	// second phase below sets the explicit raised values it is
	// testing for.
	mustSetQueueAttributes(t, node, url, newSendThrottleAttrs())
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

func TestSQSServer_Throttle_SetQueueAttributesSyncsDisabledMetrics(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	observer := &recordingSQSThrottleObserver{}
	node.sqsServer.throttleObserver = observer

	url := mustCreateQueue(t, node, "throttle-metric-sync")
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleSendCapacity":        drainBucketCapacity,
		"ThrottleSendRefillPerSecond": slowRefillRate,
		"ThrottleRecvCapacity":        drainBucketCapacity,
		"ThrottleRecvRefillPerSecond": slowRefillRate,
	})
	require.Contains(t, observer.syncs, throttleSyncReport{
		queue:   "throttle-metric-sync",
		enabled: []string{SQSThrottleActionSend, SQSThrottleActionReceive},
	})

	observer.syncs = nil
	observer.forgotten = nil
	mustSetQueueAttributes(t, node, url, map[string]string{
		"ThrottleRecvCapacity":        "0",
		"ThrottleRecvRefillPerSecond": "0",
	})

	require.Contains(t, observer.syncs, throttleSyncReport{
		queue:   "throttle-metric-sync",
		enabled: []string{SQSThrottleActionSend},
	})
	require.NotContains(t, observer.forgotten, throttleForgetReport{queue: "throttle-metric-sync", action: SQSThrottleActionSend})
	require.Contains(t, observer.forgotten, throttleForgetReport{queue: "throttle-metric-sync", action: SQSThrottleActionReceive})
	require.Contains(t, observer.forgotten, throttleForgetReport{queue: "throttle-metric-sync", action: SQSThrottleActionDefault})
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
	// Use slowRefillRate for consistency with the rest of the file. This
	// test does not assert throttle after the drain (the drain loop
	// ignores status), so the value does not affect correctness — but
	// uniformity reduces "why is this one different?" cognitive load.
	mustSetQueueAttributes(t, node, url, newSendThrottleAttrs())
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
	mustSetQueueAttributes(t, node, url2, newSendThrottleAttrs())
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
// the no-op gate. Earlier code invalidated the
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
	// Drain-then-assert + re-set-identical-attrs + drain-assert. The
	// helper handles the file-head slowRefillRate rule. The two
	// mustSetQueueAttributes calls below share the same value so the
	// "no-op" intent is structurally visible — a future drift would
	// be obviously wrong (Gemini medium on PR #819).
	throttleAttrs := newSendThrottleAttrs()
	mustSetQueueAttributes(t, node, url, throttleAttrs)
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
	mustSetQueueAttributes(t, node, url, throttleAttrs)
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
