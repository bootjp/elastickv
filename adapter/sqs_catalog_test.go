package adapter

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"

	json "github.com/goccy/go-json"
)

// callSQS routes a JSON-protocol request to the given node's SQS endpoint.
// The helper exists so tests read like "createQueue → 200 with a URL" rather
// than having to hand-build X-Amz-Target envelopes every time.
func callSQS(t *testing.T, node Node, target string, in any) (int, map[string]any) {
	t.Helper()
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		"http://"+node.sqsAddress+"/", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", sqsContentTypeJSON)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode %q: %v", string(raw), err)
		}
	}
	return resp.StatusCode, out
}

func sqsLeaderNode(t *testing.T, nodes []Node) Node {
	t.Helper()
	for _, n := range nodes {
		if n.engine != nil && n.engine.Leader().Address == n.raftAddress {
			return n
		}
	}
	return nodes[0]
}

func TestSQSServer_CatalogCreateGetList(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	// CreateQueue: 200 with a QueueUrl that ends in the queue name.
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "orders",
	})
	if status != http.StatusOK {
		t.Fatalf("create: status %d body %v", status, out)
	}
	url, _ := out["QueueUrl"].(string)
	if !strings.HasSuffix(url, "/orders") {
		t.Fatalf("QueueUrl %q does not end in /orders", url)
	}

	// GetQueueUrl returns the same URL.
	status, out = callSQS(t, node, sqsGetQueueUrlTarget, map[string]any{
		"QueueName": "orders",
	})
	if status != http.StatusOK {
		t.Fatalf("getQueueUrl: status %d body %v", status, out)
	}
	if got, _ := out["QueueUrl"].(string); got != url {
		t.Fatalf("GetQueueUrl=%q want %q", got, url)
	}

	// ListQueues sees it.
	status, out = callSQS(t, node, sqsListQueuesTarget, map[string]any{})
	if status != http.StatusOK {
		t.Fatalf("list: status %d body %v", status, out)
	}
	urls, _ := out["QueueUrls"].([]any)
	foundList := false
	for _, u := range urls {
		if s, _ := u.(string); s == url {
			foundList = true
			break
		}
	}
	if !foundList {
		t.Fatalf("ListQueues did not include %q; got %v", url, urls)
	}
}

func TestSQSServer_CatalogCreateIsIdempotent(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	in := map[string]any{
		"QueueName": "idempotent",
		"Attributes": map[string]string{
			"VisibilityTimeout": "60",
		},
	}
	status1, out1 := callSQS(t, node, sqsCreateQueueTarget, in)
	if status1 != http.StatusOK {
		t.Fatalf("first create: %d %v", status1, out1)
	}
	// Second call with the same attributes must succeed with the same URL.
	status2, out2 := callSQS(t, node, sqsCreateQueueTarget, in)
	if status2 != http.StatusOK {
		t.Fatalf("second create (same attrs): %d %v", status2, out2)
	}
	if out1["QueueUrl"] != out2["QueueUrl"] {
		t.Fatalf("idempotent create returned different URLs: %v vs %v", out1, out2)
	}

	// Third call with differing attributes must fail with QueueNameExists.
	changed := map[string]any{
		"QueueName":  "idempotent",
		"Attributes": map[string]string{"VisibilityTimeout": "120"},
	}
	status3, out3 := callSQS(t, node, sqsCreateQueueTarget, changed)
	if status3 != http.StatusBadRequest {
		t.Fatalf("differing-attrs create: got %d want 400; body %v", status3, out3)
	}
	if got, _ := out3["__type"].(string); got != sqsErrQueueNameExists {
		t.Fatalf("differing-attrs error type: got %q want %q", got, sqsErrQueueNameExists)
	}
}

func TestSQSServer_CatalogGetAndSetAttributes(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "attrs",
	})
	if status != http.StatusOK {
		t.Fatalf("create: %d %v", status, out)
	}
	url, _ := out["QueueUrl"].(string)

	status, out = callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("getAttrs: %d %v", status, out)
	}
	attrs, _ := out["Attributes"].(map[string]any)
	if attrs["VisibilityTimeout"] != "30" {
		t.Fatalf("default VisibilityTimeout = %v, want 30", attrs["VisibilityTimeout"])
	}

	status, out = callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"VisibilityTimeout": "90",
			"DelaySeconds":      "5",
		},
	})
	if status != http.StatusOK {
		t.Fatalf("setAttrs: %d %v", status, out)
	}

	_, out = callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"VisibilityTimeout", "DelaySeconds"},
	})
	attrs, _ = out["Attributes"].(map[string]any)
	if attrs["VisibilityTimeout"] != "90" || attrs["DelaySeconds"] != "5" {
		t.Fatalf("updated attrs = %v, want VisibilityTimeout=90 DelaySeconds=5", attrs)
	}
}

func TestSQSServer_CatalogDelete(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "deleteme",
	})
	url, _ := out["QueueUrl"].(string)

	status, out := callSQS(t, node, sqsDeleteQueueTarget, map[string]any{
		"QueueUrl": url,
	})
	if status != http.StatusOK {
		t.Fatalf("delete: %d %v", status, out)
	}

	// GetQueueUrl after delete returns NonExistentQueue.
	status, out = callSQS(t, node, sqsGetQueueUrlTarget, map[string]any{
		"QueueName": "deleteme",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("getQueueUrl after delete: got %d want 400; body %v", status, out)
	}
	if got, _ := out["__type"].(string); got != sqsErrQueueDoesNotExist {
		t.Fatalf("error type: got %q want %q", got, sqsErrQueueDoesNotExist)
	}

	// DeleteQueue on an unknown queue also returns NonExistentQueue.
	status, _ = callSQS(t, node, sqsDeleteQueueTarget, map[string]any{
		"QueueUrl": url,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("second delete: got %d want 400", status)
	}
}

func TestSQSServer_CatalogFIFOValidation(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	// FIFO name with FifoQueue=false is rejected.
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "bad.fifo",
		"Attributes": map[string]string{"FifoQueue": "false"},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("mismatch name/FifoQueue: got %d want 400; body %v", status, out)
	}

	// Non-FIFO name with FifoQueue=true is rejected.
	status, out = callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "plain",
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("plain name + FifoQueue=true: got %d want 400; body %v", status, out)
	}

	// Valid FIFO succeeds and the attribute is echoed back.
	status, out = callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "events.fifo",
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	if status != http.StatusOK {
		t.Fatalf("FIFO create: %d %v", status, out)
	}
	url, _ := out["QueueUrl"].(string)
	_, out = callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"FifoQueue"},
	})
	attrs, _ := out["Attributes"].(map[string]any)
	if attrs["FifoQueue"] != "true" {
		t.Fatalf("FIFO flag not persisted: %v", attrs)
	}
}

func TestSQSServer_CatalogKeyEncoding(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"", "a", "hello world", "queue.fifo", strings.Repeat("x", 80)} {
		encoded := encodeSQSSegment(name)
		decoded, err := decodeSQSSegment(encoded)
		if err != nil {
			t.Fatalf("decode %q: %v", name, err)
		}
		if decoded != name {
			t.Fatalf("round-trip %q -> %q -> %q", name, encoded, decoded)
		}
	}

	// queueNameFromMetaKey round-trips sqsQueueMetaKey.
	name := "round.trip.fifo"
	key := sqsQueueMetaKey(name)
	got, ok := queueNameFromMetaKey(key)
	if !ok || got != name {
		t.Fatalf("queueNameFromMetaKey(sqsQueueMetaKey(%q)) = (%q, %v), want (%q, true)", name, got, ok, name)
	}

	// Unknown prefixes are rejected.
	if _, ok := queueNameFromMetaKey([]byte("random")); ok {
		t.Fatal("queueNameFromMetaKey should reject non-catalog keys")
	}
}

func TestSQSServer_GetQueueAttributesOmittedReturnsEmpty(t *testing.T) {
	t.Parallel()
	// AWS semantics: an omitted AttributeNames list returns NO
	// attributes, not every attribute. Callers that want "everything"
	// must pass ["All"] explicitly. Over-returning metadata on
	// omission (our previous behavior) would hand operators data
	// they did not ask for and differ from real SQS.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	url := createSQSQueueForTest(t, node, "attrs-empty-select")

	// No AttributeNames → empty Attributes map.
	status, out := callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
	})
	if status != http.StatusOK {
		t.Fatalf("getAttrs omitted: %d %v", status, out)
	}
	attrs, _ := out["Attributes"].(map[string]any)
	if len(attrs) != 0 {
		t.Fatalf("omitted AttributeNames should return empty map; got %v", attrs)
	}

	// Explicit All → every attribute.
	status, out = callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("getAttrs All: %d %v", status, out)
	}
	attrs, _ = out["Attributes"].(map[string]any)
	if attrs["VisibilityTimeout"] == nil {
		t.Fatalf("All should include VisibilityTimeout; got %v", attrs)
	}
}

func TestSQSServer_GetQueueAttributesApproxCounters(t *testing.T) {
	t.Parallel()
	// AWS exposes three Approximate* counters on GetQueueAttributes:
	// - ApproximateNumberOfMessages (visible right now)
	// - ApproximateNumberOfMessagesNotVisible (in-flight after receive)
	// - ApproximateNumberOfMessagesDelayed (sent with DelaySeconds, not yet eligible)
	// Pre-Phase-3.A the adapter returned none of them. This test pins
	// (a) that they appear under the All selector, (b) that they
	// classify the three states correctly, and (c) that they appear
	// only when explicitly requested or via All.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	url := createSQSQueueForTest(t, node, "approx-counters")

	// Send 3 ready messages and one delayed message.
	for i := range 3 {
		_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    url,
			"MessageBody": "ready-" + strconv.Itoa(i),
		})
	}
	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":     url,
		"MessageBody":  "delayed",
		"DelaySeconds": 60,
	})

	// Receive 1 → in-flight.
	_, _ = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            url,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   60,
	})

	status, out := callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("getAttrs All: %d %v", status, out)
	}
	attrs, _ := out["Attributes"].(map[string]any)
	if got := attrs["ApproximateNumberOfMessages"]; got != "2" {
		t.Errorf("ApproximateNumberOfMessages = %v, want 2 (3 sent - 1 in-flight)", got)
	}
	if got := attrs["ApproximateNumberOfMessagesNotVisible"]; got != "1" {
		t.Errorf("ApproximateNumberOfMessagesNotVisible = %v, want 1", got)
	}
	if got := attrs["ApproximateNumberOfMessagesDelayed"]; got != "1" {
		t.Errorf("ApproximateNumberOfMessagesDelayed = %v, want 1", got)
	}
}

func TestSQSServer_GetQueueAttributesApproxCountersOnlyWhenSelected(t *testing.T) {
	t.Parallel()
	// The Approximate* counters trigger a visibility-index scan, so
	// they must NOT be returned for callers that asked for unrelated
	// attributes — both for cost and for AWS-shape parity (an
	// explicit-name request only returns the listed names).
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	url := createSQSQueueForTest(t, node, "approx-isolation")
	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    url,
		"MessageBody": "x",
	})

	// Only VisibilityTimeout: counters must be absent.
	status, out := callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"VisibilityTimeout"},
	})
	if status != http.StatusOK {
		t.Fatalf("getAttrs VisibilityTimeout: %d %v", status, out)
	}
	attrs, _ := out["Attributes"].(map[string]any)
	if _, present := attrs["ApproximateNumberOfMessages"]; present {
		t.Errorf("counters leaked into VisibilityTimeout-only request: %v", attrs)
	}
	if got := attrs["VisibilityTimeout"]; got == nil {
		t.Errorf("VisibilityTimeout missing: %v", attrs)
	}

	// Only ApproximateNumberOfMessages: counter present, nothing else.
	status, out = callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"ApproximateNumberOfMessages"},
	})
	if status != http.StatusOK {
		t.Fatalf("getAttrs ApproximateNumberOfMessages: %d %v", status, out)
	}
	attrs, _ = out["Attributes"].(map[string]any)
	if got := attrs["ApproximateNumberOfMessages"]; got != "1" {
		t.Errorf("ApproximateNumberOfMessages = %v, want 1", got)
	}
	if _, present := attrs["VisibilityTimeout"]; present {
		t.Errorf("VisibilityTimeout leaked into counter-only request: %v", attrs)
	}
}

func TestSQSServer_SetQueueAttributesRequiresAttributes(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	url := createSQSQueueForTest(t, node, "setattrs-required")

	status, out := callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("setAttrs without Attributes: got %d want 400 (%v)", status, out)
	}
	if got, _ := out["__type"].(string); got != sqsErrMissingParameter {
		t.Fatalf("error type: %q want %q", got, sqsErrMissingParameter)
	}

	// Empty Attributes map is also treated as omitted.
	status, out = callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl":   url,
		"Attributes": map[string]string{},
	})
	if status != http.StatusBadRequest {
		t.Fatalf("setAttrs empty Attributes: got %d want 400 (%v)", status, out)
	}
}

func TestSQSServer_CreateQueueRejectsRedrivePolicy(t *testing.T) {
	t.Parallel()
	// Milestone 1 does not enforce DLQ redrive on the receive path, so
	// accepting RedrivePolicy would silently advertise a feature the
	// adapter can't deliver — poison messages would redeliver
	// indefinitely instead of moving to the DLQ. Until the Milestone-2
	// receive-side DLQ move lands, reject the attribute loudly.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "with-redrive",
		"Attributes": map[string]string{
			"RedrivePolicy": `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq","maxReceiveCount":"5"}`,
		},
	})
	if status != http.StatusNotImplemented {
		t.Fatalf("CreateQueue with RedrivePolicy: got %d want 501 (%v)", status, out)
	}
	if got, _ := out["__type"].(string); got != sqsErrNotImplemented {
		t.Fatalf("error type: %q want %q", got, sqsErrNotImplemented)
	}

	// SetQueueAttributes rejects the same attribute on an existing
	// queue.
	url := createSQSQueueForTest(t, node, "no-redrive")
	status, out = callSQS(t, node, sqsSetQueueAttributesTarget, map[string]any{
		"QueueUrl": url,
		"Attributes": map[string]string{
			"RedrivePolicy": `{"maxReceiveCount":"3"}`,
		},
	})
	if status != http.StatusNotImplemented {
		t.Fatalf("SetQueueAttributes with RedrivePolicy: got %d want 501 (%v)", status, out)
	}
}

func TestSQSServer_ReceiveMessageRejectsOutOfRangeMax(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	url := createSQSQueueForTest(t, node, "max-range")

	for _, bad := range []int{0, -1, 11, 100} {
		status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
			"QueueUrl":            url,
			"MaxNumberOfMessages": bad,
		})
		if status != http.StatusBadRequest {
			t.Fatalf("MaxNumberOfMessages=%d: got %d want 400 (%v)", bad, status, out)
		}
		if got, _ := out["__type"].(string); got != sqsErrInvalidAttributeValue {
			t.Fatalf("MaxNumberOfMessages=%d error type: %q want %q", bad, got, sqsErrInvalidAttributeValue)
		}
	}

	// Omitted → defaults to 1, succeeds (empty queue returns 0 messages).
	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl": url,
	})
	if status != http.StatusOK {
		t.Fatalf("omitted MaxNumberOfMessages: %d %v", status, out)
	}

	// In-range → succeeds.
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            url,
		"MaxNumberOfMessages": 10,
	})
	if status != http.StatusOK {
		t.Fatalf("MaxNumberOfMessages=10: %d %v", status, out)
	}
}
