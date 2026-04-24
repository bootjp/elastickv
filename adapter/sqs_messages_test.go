package adapter

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// createSQSQueueForTest is a small helper so every message-path test does
// not have to repeat the "createQueue -> pull URL" dance.
func createSQSQueueForTest(t *testing.T, node Node, name string) string {
	t.Helper()
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{"QueueName": name})
	if status != http.StatusOK {
		t.Fatalf("createQueue %q: %d %v", name, status, out)
	}
	url, _ := out["QueueUrl"].(string)
	if url == "" {
		t.Fatalf("createQueue %q: empty URL", name)
	}
	return url
}

func TestSQSServer_SendReceiveDelete(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "work")

	msgID := sendOneMessage(t, node, queueURL, "hello")
	receipt := receiveOneMessage(t, node, queueURL, msgID, "hello")
	expectNoMessagesVisible(t, node, queueURL, "after-receive")
	deleteMessageOK(t, node, queueURL, receipt)
	expectNoMessagesVisible(t, node, queueURL, "after-delete")
}

// sendOneMessage sends a single message and returns the assigned MessageId.
// It asserts the response carries an MD5 over the body that matches what
// sqsMD5Hex would compute locally.
func sendOneMessage(t *testing.T, node Node, queueURL, body string) string {
	t.Helper()
	status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": body,
	})
	if status != http.StatusOK {
		t.Fatalf("send: %d %v", status, out)
	}
	msgID, _ := out["MessageId"].(string)
	if msgID == "" {
		t.Fatalf("no MessageId in %v", out)
	}
	if out["MD5OfMessageBody"] != sqsMD5Hex([]byte(body)) {
		t.Fatalf("md5 mismatch: got %v want %q", out["MD5OfMessageBody"], sqsMD5Hex([]byte(body)))
	}
	return msgID
}

func receiveOneMessage(t *testing.T, node Node, queueURL, wantID, wantBody string) string {
	t.Helper()
	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   60,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1 (%v)", len(msgs), out)
	}
	m, _ := msgs[0].(map[string]any)
	if m["Body"] != wantBody {
		t.Fatalf("Body=%v want %q", m["Body"], wantBody)
	}
	if m["MessageId"] != wantID {
		t.Fatalf("MessageId=%v want %q", m["MessageId"], wantID)
	}
	receipt, _ := m["ReceiptHandle"].(string)
	if receipt == "" {
		t.Fatalf("no ReceiptHandle in %v", m)
	}
	return receipt
}

func expectNoMessagesVisible(t *testing.T, node Node, queueURL, tag string) {
	t.Helper()
	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
	})
	if status != http.StatusOK {
		t.Fatalf("%s receive: %d %v", tag, status, out)
	}
	if msgs, _ := out["Messages"].([]any); len(msgs) != 0 {
		t.Fatalf("%s: got %d messages, want 0", tag, len(msgs))
	}
}

func deleteMessageOK(t *testing.T, node Node, queueURL, receipt string) {
	t.Helper()
	status, out := callSQS(t, node, sqsDeleteMessageTarget, map[string]any{
		"QueueUrl":      queueURL,
		"ReceiptHandle": receipt,
	})
	if status != http.StatusOK {
		t.Fatalf("delete: %d %v", status, out)
	}
}

func TestSQSServer_DeleteWithStaleReceiptIsIdempotentNoOp(t *testing.T) {
	t.Parallel()
	// AWS SQS semantics: DeleteMessage with a stale receipt handle (token
	// rotated under our feet, or record already gone) must return 200
	// success without deleting. SDK retry paths and batch workers rely on
	// this so a retry after a visibility-expiry re-delivery does not fail
	// loudly. Structurally malformed handles are still an error;
	// token-only mismatches are not.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "stale-receipt")

	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "x",
	})
	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   60,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) == 0 {
		t.Fatalf("no messages received")
	}
	goodHandle, _ := msgs[0].(map[string]any)["ReceiptHandle"].(string)

	// Flip a byte of the token so the stored token != handle token.
	decoded, err := decodeReceiptHandle(goodHandle)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	decoded.ReceiptToken[0] ^= 0xff
	staleHandle, err := encodeReceiptHandle(decoded.QueueGeneration, decoded.MessageIDHex, decoded.ReceiptToken)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	// Delete with the stale handle must succeed (no-op) per AWS.
	status, out = callSQS(t, node, sqsDeleteMessageTarget, map[string]any{
		"QueueUrl":      queueURL,
		"ReceiptHandle": staleHandle,
	})
	if status != http.StatusOK {
		t.Fatalf("delete with stale receipt: status=%d body=%v", status, out)
	}

	// The real handle must still work — the stale delete must not have
	// removed the in-flight message out from under the original consumer.
	status, out = callSQS(t, node, sqsDeleteMessageTarget, map[string]any{
		"QueueUrl":      queueURL,
		"ReceiptHandle": goodHandle,
	})
	if status != http.StatusOK {
		t.Fatalf("delete with good receipt after stale no-op: %d %v", status, out)
	}

	// A structurally malformed handle still errors out — only token
	// mismatches are the idempotent no-op case.
	status, out = callSQS(t, node, sqsDeleteMessageTarget, map[string]any{
		"QueueUrl":      queueURL,
		"ReceiptHandle": "not-base64-!!!",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("malformed handle: status=%d body=%v", status, out)
	}
	if out["__type"] != sqsErrReceiptHandleInvalid {
		t.Fatalf("error type for malformed handle: %q want %q", out["__type"], sqsErrReceiptHandleInvalid)
	}
}

func TestSQSServer_ReceiveBatchRespectsMaxMessages(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "batch")

	const sent = 5
	for i := 0; i < sent; i++ {
		_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "m-" + strconv.Itoa(i),
		})
	}

	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 3,
		"VisibilityTimeout":   60,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want 3 (%v)", len(msgs), out)
	}

	// A second receive picks up the remaining two.
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   60,
	})
	if status != http.StatusOK {
		t.Fatalf("receive#2: %d %v", status, out)
	}
	msgs, _ = out["Messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("got %d messages on second receive, want 2", len(msgs))
	}
}

func TestSQSServer_DelaySecondsDefersDelivery(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "delayed")

	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":     queueURL,
		"MessageBody":  "later",
		"DelaySeconds": 2,
	})

	// Immediate receive must return nothing.
	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	if msgs, _ := out["Messages"].([]any); len(msgs) != 0 {
		t.Fatalf("expected 0 messages before delay elapsed, got %d", len(msgs))
	}

	// After the delay, the message becomes visible.
	time.Sleep(2100 * time.Millisecond)
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   60,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message after delay, got %d (%v)", len(msgs), out)
	}
}

func TestSQSServer_VisibilityTimeoutExpiryMakesMessageVisibleAgain(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "revisible")

	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "retry-me",
	})

	// First receive with a very short visibility so this test doesn't
	// sit idle for 30 seconds.
	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   1,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("first receive: got %d messages want 1", len(msgs))
	}
	first, _ := msgs[0].(map[string]any)
	firstAttrs, _ := first["Attributes"].(map[string]any)
	if firstAttrs["ApproximateReceiveCount"] != "1" {
		t.Fatalf("first receive count = %v, want 1", firstAttrs)
	}

	time.Sleep(1200 * time.Millisecond)

	// Second receive should see the same message with ReceiveCount=2.
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   60,
	})
	if status != http.StatusOK {
		t.Fatalf("second receive: %d %v", status, out)
	}
	msgs, _ = out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("second receive: got %d messages want 1", len(msgs))
	}
	second, _ := msgs[0].(map[string]any)
	if first["MessageId"] != second["MessageId"] {
		t.Fatalf("second receive returned a different message: %v vs %v", first, second)
	}
	secondAttrs, _ := second["Attributes"].(map[string]any)
	if secondAttrs["ApproximateReceiveCount"] != "2" {
		t.Fatalf("second receive count = %v, want 2", secondAttrs)
	}
}

func TestSQSServer_ChangeMessageVisibility(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "chgvis")

	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "bumpy",
	})
	status, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   1,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("no message received")
	}
	receipt, _ := msgs[0].(map[string]any)["ReceiptHandle"].(string)

	// Extend visibility to 60s.
	status, out = callSQS(t, node, sqsChangeMessageVisibilityTarget, map[string]any{
		"QueueUrl":          queueURL,
		"ReceiptHandle":     receipt,
		"VisibilityTimeout": 60,
	})
	if status != http.StatusOK {
		t.Fatalf("change visibility: %d %v", status, out)
	}

	// After the original 1s expiry, the message must still be hidden.
	time.Sleep(1200 * time.Millisecond)
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	if msgs, _ := out["Messages"].([]any); len(msgs) != 0 {
		t.Fatalf("expected 0 messages after visibility extended, got %d", len(msgs))
	}
}

func TestSQSServer_ReceiptHandleCodecRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		gen uint64
		id  string
	}{
		{0, "00000000000000000000000000000000"},
		{1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
		{42, "deadbeefdeadbeefdeadbeefdeadbeef"},
	} {
		token := make([]byte, sqsReceiptTokenBytes)
		for i := range token {
			token[i] = byte(i)
		}
		h, err := encodeReceiptHandle(tc.gen, tc.id, token)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		back, err := decodeReceiptHandle(h)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if back.QueueGeneration != tc.gen || back.MessageIDHex != tc.id {
			t.Fatalf("round-trip mismatch: %+v vs %d/%s", back, tc.gen, tc.id)
		}
		if hex.EncodeToString(back.ReceiptToken) != hex.EncodeToString(token) {
			t.Fatalf("token round-trip mismatch: %x vs %x", back.ReceiptToken, token)
		}
	}

	// A garbage handle must fail to decode, not crash.
	if _, err := decodeReceiptHandle("!!!"); err == nil {
		t.Fatal("expected decode error for garbage handle")
	}
}
