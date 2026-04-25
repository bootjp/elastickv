package adapter

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestSQSServer_PurgeQueueRemovesMessagesAndRateLimits(t *testing.T) {
	t.Parallel()
	// PurgeQueue must (a) bump the queue generation so previously sent
	// messages are unreachable on the new generation and (b) reject a
	// follow-up purge issued within AWS's 60-second cooldown.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "purge-target")

	for i := range 3 {
		_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "msg-" + strconv.Itoa(i),
		})
	}

	status, out := callSQS(t, node, sqsPurgeQueueTarget, map[string]any{
		"QueueUrl": queueURL,
	})
	if status != http.StatusOK {
		t.Fatalf("purge: %d %v", status, out)
	}

	// After purge, the queue is empty for the new generation.
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 10,
	})
	if status != http.StatusOK {
		t.Fatalf("receive after purge: %d %v", status, out)
	}
	if msgs, _ := out["Messages"].([]any); len(msgs) != 0 {
		t.Fatalf("expected 0 messages after purge, got %d (%v)", len(msgs), msgs)
	}

	// Second purge inside the 60-second cooldown must fail.
	status, out = callSQS(t, node, sqsPurgeQueueTarget, map[string]any{
		"QueueUrl": queueURL,
	})
	if status != http.StatusBadRequest {
		t.Fatalf("rapid purge: got %d want 400 (%v)", status, out)
	}
	if got, _ := out["__type"].(string); got != sqsErrPurgeInProgress {
		t.Fatalf("error type: %q want %q", got, sqsErrPurgeInProgress)
	}

	// New sends still work after a purge — the queue still exists.
	status, out = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "after-purge",
	})
	if status != http.StatusOK {
		t.Fatalf("post-purge send: %d %v", status, out)
	}
}

func TestSQSServer_PurgeQueueOnMissingQueue(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	status, out := callSQS(t, node, sqsPurgeQueueTarget, map[string]any{
		"QueueUrl": "http://" + node.sqsAddress + "/no-such-queue",
	})
	if status != http.StatusBadRequest {
		t.Fatalf("purge missing: %d %v", status, out)
	}
	if got, _ := out["__type"].(string); got != sqsErrQueueDoesNotExist {
		t.Fatalf("error type: %q want %q", got, sqsErrQueueDoesNotExist)
	}
}

func TestSQSServer_SendMessageBatchHappyPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "batch-send")

	entries := make([]map[string]any, 0, 3)
	for i := range 3 {
		entries = append(entries, map[string]any{
			"Id":          "e" + strconv.Itoa(i),
			"MessageBody": "body-" + strconv.Itoa(i),
		})
	}
	status, out := callSQS(t, node, sqsSendMessageBatchTarget, map[string]any{
		"QueueUrl": queueURL,
		"Entries":  entries,
	})
	if status != http.StatusOK {
		t.Fatalf("send batch: %d %v", status, out)
	}
	successful, _ := out["Successful"].([]any)
	if len(successful) != 3 {
		t.Fatalf("expected 3 successful, got %d (%v)", len(successful), out)
	}
	failed, _ := out["Failed"].([]any)
	if len(failed) != 0 {
		t.Fatalf("expected 0 failed, got %v", failed)
	}

	// Confirm the messages are deliverable.
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   60,
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 received, got %d", len(msgs))
	}
}

func TestSQSServer_SendMessageBatchPartialFailure(t *testing.T) {
	t.Parallel()
	// One entry has an empty body (rejected as InvalidParameterValue),
	// two are valid. AWS reports per-entry success/failure rather than
	// failing the whole batch — verify that contract holds.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "batch-mixed")

	entries := []map[string]any{
		{"Id": "ok-1", "MessageBody": "yes"},
		{"Id": "bad-1", "MessageBody": ""}, // empty body is per-entry failure
		{"Id": "ok-2", "MessageBody": "yes-2"},
	}
	status, out := callSQS(t, node, sqsSendMessageBatchTarget, map[string]any{
		"QueueUrl": queueURL,
		"Entries":  entries,
	})
	if status != http.StatusOK {
		t.Fatalf("send batch: %d %v", status, out)
	}
	successful, _ := out["Successful"].([]any)
	if len(successful) != 2 {
		t.Fatalf("expected 2 successful, got %d (%v)", len(successful), successful)
	}
	failed, _ := out["Failed"].([]any)
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed, got %d (%v)", len(failed), failed)
	}
	bad, _ := failed[0].(map[string]any)
	if bad["Id"] != "bad-1" {
		t.Fatalf("failed entry Id = %v, want bad-1", bad["Id"])
	}
	if bad["SenderFault"] != true {
		t.Fatalf("SenderFault = %v, want true", bad["SenderFault"])
	}
}

func TestSQSServer_SendMessageBatchRejectsEmptyAndOversize(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "batch-shape")

	// Empty entries list → EmptyBatchRequest.
	status, out := callSQS(t, node, sqsSendMessageBatchTarget, map[string]any{
		"QueueUrl": queueURL,
		"Entries":  []map[string]any{},
	})
	if status != http.StatusBadRequest || out["__type"] != sqsErrEmptyBatchRequest {
		t.Fatalf("empty batch: status=%d body=%v", status, out)
	}

	// More than 10 entries → TooManyEntriesInBatchRequest.
	bigEntries := make([]map[string]any, 0, 11)
	for i := range 11 {
		bigEntries = append(bigEntries, map[string]any{
			"Id": "e" + strconv.Itoa(i), "MessageBody": "x",
		})
	}
	status, out = callSQS(t, node, sqsSendMessageBatchTarget, map[string]any{
		"QueueUrl": queueURL,
		"Entries":  bigEntries,
	})
	if status != http.StatusBadRequest || out["__type"] != sqsErrTooManyEntriesInBatchRequest {
		t.Fatalf("too many entries: status=%d body=%v", status, out)
	}

	// Duplicate Ids → BatchEntryIdsNotDistinct.
	status, out = callSQS(t, node, sqsSendMessageBatchTarget, map[string]any{
		"QueueUrl": queueURL,
		"Entries": []map[string]any{
			{"Id": "dup", "MessageBody": "a"},
			{"Id": "dup", "MessageBody": "b"},
		},
	})
	if status != http.StatusBadRequest || out["__type"] != sqsErrBatchEntryIdsNotDistinct {
		t.Fatalf("dup ids: status=%d body=%v", status, out)
	}
}

func TestSQSServer_DeleteMessageBatch(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "batch-delete")

	for i := range 3 {
		_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "d-" + strconv.Itoa(i),
		})
	}
	_, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   60,
	})
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 3 {
		t.Fatalf("expected 3 received, got %d", len(msgs))
	}
	entries := make([]map[string]any, 0, len(msgs))
	for i, m := range msgs {
		mm, _ := m.(map[string]any)
		entries = append(entries, map[string]any{
			"Id":            "d" + strconv.Itoa(i),
			"ReceiptHandle": mm["ReceiptHandle"],
		})
	}
	// Add a malformed handle entry — must fail per-entry, not the whole batch.
	entries = append(entries, map[string]any{
		"Id":            "bad-handle",
		"ReceiptHandle": "not-base64-!!!",
	})
	status, out := callSQS(t, node, sqsDeleteMessageBatchTarget, map[string]any{
		"QueueUrl": queueURL,
		"Entries":  entries,
	})
	if status != http.StatusOK {
		t.Fatalf("delete batch: %d %v", status, out)
	}
	successful, _ := out["Successful"].([]any)
	if len(successful) != 3 {
		t.Fatalf("expected 3 successful, got %d (%v)", len(successful), successful)
	}
	failed, _ := out["Failed"].([]any)
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed, got %v", failed)
	}
	bad, _ := failed[0].(map[string]any)
	if bad["Id"] != "bad-handle" {
		t.Fatalf("failed Id = %v, want bad-handle", bad["Id"])
	}
}

func TestSQSServer_ChangeMessageVisibilityBatch(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "batch-chgvis")

	for i := range 2 {
		_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "c-" + strconv.Itoa(i),
		})
	}
	_, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   1,
	})
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 2 {
		t.Fatalf("expected 2 received, got %d", len(msgs))
	}
	entries := make([]map[string]any, 0, len(msgs))
	for i, m := range msgs {
		mm, _ := m.(map[string]any)
		entries = append(entries, map[string]any{
			"Id":                "v" + strconv.Itoa(i),
			"ReceiptHandle":     mm["ReceiptHandle"],
			"VisibilityTimeout": 60,
		})
	}
	// Add an entry with a bad VisibilityTimeout — must fail per-entry.
	entries = append(entries, map[string]any{
		"Id":                "bad",
		"ReceiptHandle":     "ignored",
		"VisibilityTimeout": -1,
	})
	status, out := callSQS(t, node, sqsChangeMessageVisibilityBatchTgt, map[string]any{
		"QueueUrl": queueURL,
		"Entries":  entries,
	})
	if status != http.StatusOK {
		t.Fatalf("change vis batch: %d %v", status, out)
	}
	successful, _ := out["Successful"].([]any)
	if len(successful) != 2 {
		t.Fatalf("expected 2 successful, got %d (%v)", len(successful), successful)
	}
	failed, _ := out["Failed"].([]any)
	if len(failed) != 1 {
		t.Fatalf("expected 1 failed, got %d", len(failed))
	}

	// After the original 1s expires, the messages must still be hidden
	// thanks to the new 60s visibility set by the batch call.
	time.Sleep(1200 * time.Millisecond)
	_, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 10,
	})
	if msgs, _ := out["Messages"].([]any); len(msgs) != 0 {
		t.Fatalf("expected 0 messages after visibility extension, got %d", len(msgs))
	}
}

func TestSQSServer_TagQueueRoundTrip(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "tagged")

	// Initial ListQueueTags returns an empty map.
	status, out := callSQS(t, node, sqsListQueueTagsTarget, map[string]any{
		"QueueUrl": queueURL,
	})
	if status != http.StatusOK {
		t.Fatalf("list tags initial: %d %v", status, out)
	}
	tags, _ := out["Tags"].(map[string]any)
	if len(tags) != 0 {
		t.Fatalf("expected no tags, got %v", tags)
	}

	// TagQueue stores two tags.
	status, out = callSQS(t, node, sqsTagQueueTarget, map[string]any{
		"QueueUrl": queueURL,
		"Tags":     map[string]string{"team": "platform", "env": "test"},
	})
	if status != http.StatusOK {
		t.Fatalf("tag: %d %v", status, out)
	}
	_, out = callSQS(t, node, sqsListQueueTagsTarget, map[string]any{
		"QueueUrl": queueURL,
	})
	tags, _ = out["Tags"].(map[string]any)
	if tags["team"] != "platform" || tags["env"] != "test" {
		t.Fatalf("after tag: %v", tags)
	}

	// UntagQueue drops one tag, leaves the other.
	status, out = callSQS(t, node, sqsUntagQueueTarget, map[string]any{
		"QueueUrl": queueURL,
		"TagKeys":  []string{"env"},
	})
	if status != http.StatusOK {
		t.Fatalf("untag: %d %v", status, out)
	}
	_, out = callSQS(t, node, sqsListQueueTagsTarget, map[string]any{
		"QueueUrl": queueURL,
	})
	tags, _ = out["Tags"].(map[string]any)
	if _, present := tags["env"]; present {
		t.Fatalf("env should be removed, got %v", tags)
	}
	if tags["team"] != "platform" {
		t.Fatalf("team should remain, got %v", tags)
	}
}

func TestSQSServer_GetQueueAttributesApproximateCounters(t *testing.T) {
	t.Parallel()
	// Three buckets must be reflected by a single GetQueueAttributes call:
	//   - visible    : sent and currently deliverable
	//   - delayed    : sent with DelaySeconds > 0 and not yet available
	//   - not visible: delivered to a consumer and within the visibility
	//                  window
	// QueueArn / CreatedTimestamp / LastModifiedTimestamp must also come
	// back so dashboards have something to render. Counts are approximate
	// per AWS, but the snapshot we read should be coherent.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "approx")

	// One visible message.
	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": queueURL, "MessageBody": "v",
	})
	// One delayed message.
	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": queueURL, "MessageBody": "d", "DelaySeconds": 60,
	})
	// One in-flight message: send, then receive with a long visibility
	// timeout so it stays not-visible for the duration of this test.
	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl": queueURL, "MessageBody": "i",
	})
	_, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   600,
	})
	if msgs, _ := out["Messages"].([]any); len(msgs) != 1 {
		t.Fatalf("expected 1 received, got %d", len(msgs))
	}

	status, body := callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       queueURL,
		"AttributeNames": []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("getAttrs: %d %v", status, body)
	}
	attrs, _ := body["Attributes"].(map[string]any)
	assertApproxCounterAttrs(t, attrs)

	// When the caller does not request any Approximate* attribute, the
	// scan must be skipped — verify by asking for a single non-counter
	// attribute and confirming the counters are absent from the response.
	_, body = callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       queueURL,
		"AttributeNames": []string{"VisibilityTimeout"},
	})
	attrs, _ = body["Attributes"].(map[string]any)
	if _, present := attrs["ApproximateNumberOfMessages"]; present {
		t.Fatalf("counter included for non-counter selection: %v", attrs)
	}
}

func TestSQSServer_MessageAttributesCanonicalMD5(t *testing.T) {
	t.Parallel()
	// Cross-check md5OfAttributesHex against the AWS-published wire
	// format using a hand-rolled reference encoder. AWS SDKs verify
	// MD5OfMessageAttributes; if our hash drifts every SDK send fails
	// with MessageAttributeMD5Mismatch.
	attrs := map[string]sqsMessageAttributeValue{
		"City":  {DataType: "String", StringValue: "Anytown"},
		"Order": {DataType: "Number", StringValue: "12345"},
		"Blob":  {DataType: "Binary", BinaryValue: []byte{0xde, 0xad, 0xbe, 0xef}},
	}
	want := referenceCanonicalMD5(attrs)
	got := md5OfAttributesHex(attrs)
	if got != want {
		t.Fatalf("canonical md5 mismatch:\n  got:  %s\n  want: %s", got, want)
	}
	if md5OfAttributesHex(nil) != "" {
		t.Fatalf("empty attrs must hash to empty string, got %q", md5OfAttributesHex(nil))
	}
	if md5OfAttributesHex(map[string]sqsMessageAttributeValue{}) != "" {
		t.Fatalf("empty map must hash to empty string")
	}
}

// referenceCanonicalMD5 reimplements the AWS canonical algorithm in a
// way that does not share code with md5OfAttributesHex. If the two
// disagree the SDK hash is wrong.
func referenceCanonicalMD5(attrs map[string]sqsMessageAttributeValue) string {
	if len(attrs) == 0 {
		return ""
	}
	names := make([]string, 0, len(attrs))
	for k := range attrs {
		names = append(names, k)
	}
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j-1] > names[j]; j-- {
			names[j-1], names[j] = names[j], names[j-1]
		}
	}
	var buf bytes.Buffer
	writeLen := func(s string) {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], safeUint32Len(len(s)))
		buf.Write(l[:])
	}
	writeLenBytes := func(p []byte) {
		var l [4]byte
		binary.BigEndian.PutUint32(l[:], safeUint32Len(len(p)))
		buf.Write(l[:])
	}
	for _, name := range names {
		v := attrs[name]
		writeLen(name)
		buf.WriteString(name)
		writeLen(v.DataType)
		buf.WriteString(v.DataType)
		switch v.DataType {
		case "Binary":
			buf.WriteByte(0x02)
			writeLenBytes(v.BinaryValue)
			buf.Write(v.BinaryValue)
		default:
			buf.WriteByte(0x01)
			writeLen(v.StringValue)
			buf.WriteString(v.StringValue)
		}
	}
	return hexMD5(buf.Bytes())
}

func hexMD5(p []byte) string {
	h := sqsMD5Hex(p)
	if _, err := hex.DecodeString(h); err != nil {
		return ""
	}
	return h
}

func TestSQSServer_SendMessageWithMessageAttributes(t *testing.T) {
	t.Parallel()
	// SendMessage must accept MessageAttributes, return the AWS-canonical
	// MD5 in MD5OfMessageAttributes, and a subsequent ReceiveMessage with
	// MessageAttributeNames=["All"] must echo the attributes back along
	// with the same MD5.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "msg-attrs")

	attrs := map[string]any{
		"City":  map[string]any{"DataType": "String", "StringValue": "Tokyo"},
		"Order": map[string]any{"DataType": "Number", "StringValue": "42"},
	}
	status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":          queueURL,
		"MessageBody":       "hi",
		"MessageAttributes": attrs,
	})
	if status != http.StatusOK {
		t.Fatalf("send: %d %v", status, out)
	}
	expectedMD5 := md5OfAttributesHex(map[string]sqsMessageAttributeValue{
		"City":  {DataType: "String", StringValue: "Tokyo"},
		"Order": {DataType: "Number", StringValue: "42"},
	})
	if got, _ := out["MD5OfMessageAttributes"].(string); got != expectedMD5 {
		t.Fatalf("MD5OfMessageAttributes = %q, want %q", got, expectedMD5)
	}

	// Receive with "All" must echo attributes + matching MD5.
	status, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":              queueURL,
		"MaxNumberOfMessages":   1,
		"VisibilityTimeout":     60,
		"MessageAttributeNames": []string{"All"},
	})
	if status != http.StatusOK {
		t.Fatalf("receive: %d %v", status, out)
	}
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	m, _ := msgs[0].(map[string]any)
	if got, _ := m["MD5OfMessageAttributes"].(string); got != expectedMD5 {
		t.Fatalf("Receive MD5 = %q, want %q", got, expectedMD5)
	}
	echoed, _ := m["MessageAttributes"].(map[string]any)
	if len(echoed) != 2 {
		t.Fatalf("expected 2 echoed attributes, got %v", echoed)
	}
	city, _ := echoed["City"].(map[string]any)
	if city["StringValue"] != "Tokyo" || city["DataType"] != "String" {
		t.Fatalf("City attribute = %v", city)
	}
}

func TestSQSServer_SendMessageRejectsMalformedAttributes(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "bad-attrs")

	// Missing DataType.
	status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "x",
		"MessageAttributes": map[string]any{
			"X": map[string]any{"StringValue": "value"},
		},
	})
	if status != http.StatusBadRequest || out["__type"] != sqsErrInvalidAttributeValue {
		t.Fatalf("missing DataType: status=%d body=%v", status, out)
	}

	// Unknown DataType.
	status, out = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "x",
		"MessageAttributes": map[string]any{
			"X": map[string]any{"DataType": "Bogus", "StringValue": "v"},
		},
	})
	if status != http.StatusBadRequest || out["__type"] != sqsErrInvalidAttributeValue {
		t.Fatalf("bad DataType: status=%d body=%v", status, out)
	}

	// String type with empty StringValue.
	status, out = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "x",
		"MessageAttributes": map[string]any{
			"X": map[string]any{"DataType": "String", "StringValue": ""},
		},
	})
	if status != http.StatusBadRequest || out["__type"] != sqsErrInvalidAttributeValue {
		t.Fatalf("empty StringValue: status=%d body=%v", status, out)
	}
}

func TestSQSServer_DLQRedriveOnMaxReceiveCount(t *testing.T) {
	t.Parallel()
	// A message received maxReceiveCount times must be moved to the
	// DLQ on the next receive instead of being delivered. The DLQ
	// receives the message body and a DeadLetterQueueSourceArn
	// attribute, and the source queue stops surfacing it.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	dlqURL := createSQSQueueForTest(t, node, "dlq-target")
	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq-target","maxReceiveCount":2}`
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "redrive-src",
		"Attributes": map[string]string{
			"RedrivePolicy": policy,
		},
	})
	if status != http.StatusOK {
		t.Fatalf("create source: %d %v", status, out)
	}
	srcURL, _ := out["QueueUrl"].(string)

	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    srcURL,
		"MessageBody": "poison",
	})

	// First two receives deliver the message normally (count 1, 2).
	for i := range 2 {
		_, out := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
			"QueueUrl":            srcURL,
			"MaxNumberOfMessages": 1,
			"VisibilityTimeout":   1,
		})
		msgs, _ := out["Messages"].([]any)
		if len(msgs) != 1 {
			t.Fatalf("receive #%d expected 1 msg, got %d (%v)", i, len(msgs), out)
		}
		// Wait past the visibility window so the next receive can pick
		// it up again.
		time.Sleep(1100 * time.Millisecond)
	}

	// Third receive triggers the redrive — source returns 0 messages.
	_, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            srcURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   1,
	})
	if msgs, _ := out["Messages"].([]any); len(msgs) != 0 {
		t.Fatalf("source still returning poison message after redrive: %v", msgs)
	}

	// DLQ now has the moved message.
	_, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            dlqURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   60,
	})
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("DLQ expected 1 moved message, got %d (%v)", len(msgs), out)
	}
	moved, _ := msgs[0].(map[string]any)
	if moved["Body"] != "poison" {
		t.Fatalf("DLQ message body = %v, want poison", moved["Body"])
	}
	// ApproximateReceiveCount on the DLQ side starts at 1 (this single
	// receive); the source's count is not carried over.
	movedAttrs, _ := moved["Attributes"].(map[string]any)
	if movedAttrs["ApproximateReceiveCount"] != "1" {
		t.Fatalf("DLQ message ApproximateReceiveCount = %v, want 1", movedAttrs["ApproximateReceiveCount"])
	}
}

func TestSQSServer_FifoSequenceNumberMonotonic(t *testing.T) {
	t.Parallel()
	// Two FIFO sends must come back with strictly increasing
	// SequenceNumber, and ReceiveMessage must echo the same number on
	// the corresponding message.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "fifo-seq.fifo",
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	url, _ := out["QueueUrl"].(string)

	first := sendFifoMessage(t, node, url, "g1", "d1", "a")
	second := sendFifoMessage(t, node, url, "g1", "d2", "b")
	if first >= second {
		t.Fatalf("FIFO SequenceNumbers not increasing: first=%d second=%d", first, second)
	}

	_, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            url,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   60,
	})
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected 1 received, got %d", len(msgs))
	}
	m, _ := msgs[0].(map[string]any)
	attrs, _ := m["Attributes"].(map[string]any)
	seqStr, _ := attrs["SequenceNumber"].(string)
	got, _ := strconv.ParseUint(seqStr, 10, 64)
	if got != first {
		t.Fatalf("Receive SequenceNumber=%d, want first send's %d", got, first)
	}
}

// assertApproxCounterAttrs verifies the Approximate counters and the
// catalog metadata fields produced by GetQueueAttributes for the
// hand-built (visible=1, not-visible=1, delayed=1) fixture.
func assertApproxCounterAttrs(t *testing.T, attrs map[string]any) {
	t.Helper()
	if attrs["ApproximateNumberOfMessages"] != "1" {
		t.Fatalf("Visible counter = %v, want 1 (%v)", attrs["ApproximateNumberOfMessages"], attrs)
	}
	if attrs["ApproximateNumberOfMessagesNotVisible"] != "1" {
		t.Fatalf("NotVisible counter = %v, want 1", attrs["ApproximateNumberOfMessagesNotVisible"])
	}
	if attrs["ApproximateNumberOfMessagesDelayed"] != "1" {
		t.Fatalf("Delayed counter = %v, want 1", attrs["ApproximateNumberOfMessagesDelayed"])
	}
	if got, _ := attrs["QueueArn"].(string); got == "" || got[:11] != "arn:aws:sqs" {
		t.Fatalf("QueueArn malformed: %v", attrs["QueueArn"])
	}
	if attrs["CreatedTimestamp"] == nil || attrs["LastModifiedTimestamp"] == nil {
		t.Fatalf("expected created/modified timestamps, got %v", attrs)
	}
}

func sendFifoMessage(t *testing.T, node Node, url, groupID, dedupID, body string) uint64 {
	t.Helper()
	status, out := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":               url,
		"MessageBody":            body,
		"MessageGroupId":         groupID,
		"MessageDeduplicationId": dedupID,
	})
	if status != http.StatusOK {
		t.Fatalf("FIFO send: %d %v", status, out)
	}
	seqStr, _ := out["SequenceNumber"].(string)
	seq, err := strconv.ParseUint(seqStr, 10, 64)
	if err != nil {
		t.Fatalf("SequenceNumber not parseable: %v", out["SequenceNumber"])
	}
	return seq
}

func TestSQSServer_FifoDedupBlocksDuplicateSend(t *testing.T) {
	t.Parallel()
	// A second FIFO send with the same MessageDeduplicationId inside
	// the dedup window must come back with the original MessageId and
	// not write a new copy on the queue.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "fifo-dedup.fifo",
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	url, _ := out["QueueUrl"].(string)

	status, first := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":               url,
		"MessageBody":            "x",
		"MessageGroupId":         "g",
		"MessageDeduplicationId": "same",
	})
	if status != http.StatusOK {
		t.Fatalf("first send: %d %v", status, first)
	}
	status, second := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":               url,
		"MessageBody":            "different-body-but-same-dedup",
		"MessageGroupId":         "g",
		"MessageDeduplicationId": "same",
	})
	if status != http.StatusOK {
		t.Fatalf("dedup send: %d %v", status, second)
	}
	if first["MessageId"] != second["MessageId"] {
		t.Fatalf("dedup hit must reuse original MessageId: %v vs %v", first, second)
	}

	// Only one message should be deliverable.
	_, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            url,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   60,
	})
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("dedup window must collapse send to 1 message; got %d", len(msgs))
	}
	m, _ := msgs[0].(map[string]any)
	if m["Body"] != "x" {
		t.Fatalf("dedup must keep the original body, got %v", m["Body"])
	}
}

func TestSQSServer_FifoContentBasedDedup(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "fifo-cbd.fifo",
		"Attributes": map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	url, _ := out["QueueUrl"].(string)

	status, first := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":       url,
		"MessageBody":    "same-body",
		"MessageGroupId": "g",
	})
	if status != http.StatusOK {
		t.Fatalf("first cbd send: %d %v", status, first)
	}
	status, second := callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":       url,
		"MessageBody":    "same-body",
		"MessageGroupId": "g",
	})
	if status != http.StatusOK {
		t.Fatalf("dup cbd send: %d %v", status, second)
	}
	if first["MessageId"] != second["MessageId"] {
		t.Fatalf("ContentBasedDeduplication must hash body to the same dedup id: %v vs %v", first, second)
	}
}

func TestSQSServer_FifoGroupLockHoldsAcrossVisibilityExpiry(t *testing.T) {
	t.Parallel()
	// Two FIFO messages in the same group: the first receive must pull
	// message A (the head). Even after A's visibility window expires
	// (without a delete), the next receive must re-deliver A — never
	// jump ahead to B — because the group lock pins itself to the head
	// across visibility-timeout transitions.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "fifo-grouplock.fifo",
		"Attributes": map[string]string{"FifoQueue": "true"},
	})
	url, _ := out["QueueUrl"].(string)

	_ = sendFifoMessage(t, node, url, "g", "a", "first")
	_ = sendFifoMessage(t, node, url, "g", "b", "second")

	// First receive: must be the head.
	_, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            url,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   1,
	})
	msgs, _ := out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected exactly 1 head message, got %d (group lock should hide successor)", len(msgs))
	}
	first, _ := msgs[0].(map[string]any)
	if first["Body"] != "first" {
		t.Fatalf("FIFO head must be 'first', got %v", first["Body"])
	}

	// Wait past the visibility window; group lock must keep the head
	// pinned so the next receive re-delivers A, not B.
	time.Sleep(1200 * time.Millisecond)
	_, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            url,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   60,
	})
	msgs, _ = out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected re-delivery of head, got %d messages", len(msgs))
	}
	again, _ := msgs[0].(map[string]any)
	if again["Body"] != "first" {
		t.Fatalf("FIFO redelivery must stay on head; got %v", again["Body"])
	}
	if again["MessageId"] != first["MessageId"] {
		t.Fatalf("FIFO redelivery must reuse the same MessageId")
	}

	// Delete the head; the next receive can finally pick up the second.
	receiptHandle, _ := again["ReceiptHandle"].(string)
	deleteMessageOK(t, node, url, receiptHandle)
	_, out = callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            url,
		"MaxNumberOfMessages": 10,
		"VisibilityTimeout":   60,
	})
	msgs, _ = out["Messages"].([]any)
	if len(msgs) != 1 {
		t.Fatalf("expected successor after head delete, got %d", len(msgs))
	}
	tail, _ := msgs[0].(map[string]any)
	if tail["Body"] != "second" {
		t.Fatalf("expected successor body 'second', got %v", tail["Body"])
	}
}

func TestSQSServer_RetentionReaperRemovesOldMessage(t *testing.T) {
	t.Parallel()
	// Send a message, backdate it past retention, then drive one reaper
	// pass and confirm the data, vis, and byage entries are gone. The
	// reaper must succeed without going through ReceiveMessage — that
	// is the whole reason byage exists, since a message stuck in
	// flight could otherwise live forever past retention.
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName": "reap-target",
		"Attributes": map[string]string{
			"MessageRetentionPeriod": "60",
		},
	})
	url, _ := out["QueueUrl"].(string)
	_, _ = callSQS(t, node, sqsSendMessageTarget, map[string]any{
		"QueueUrl":    url,
		"MessageBody": "to-be-reaped",
	})

	// Backdate so retention has elapsed.
	backdateSQSMessageForTest(t, nodes[0], "reap-target", 120*time.Second)

	// Drive one reaper pass directly so the test does not have to
	// wait the natural 30 s tick.
	srv := node.sqsServer
	if err := srv.reapAllQueues(t.Context()); err != nil {
		t.Fatalf("reapAllQueues: %v", err)
	}

	// Receive must come back empty — the data record is gone, the
	// scan path will not find it via the visibility index either.
	status, body := callSQS(t, node, sqsReceiveMessageTarget, map[string]any{
		"QueueUrl":            url,
		"MaxNumberOfMessages": 1,
	})
	if status != http.StatusOK {
		t.Fatalf("receive after reap: %d %v", status, body)
	}
	if msgs, _ := body["Messages"].([]any); len(msgs) != 0 {
		t.Fatalf("reaper left a message behind: %v", msgs)
	}

	// ApproximateNumberOfMessages must reflect the empty queue too.
	_, body = callSQS(t, node, sqsGetQueueAttributesTarget, map[string]any{
		"QueueUrl":       url,
		"AttributeNames": []string{"ApproximateNumberOfMessages"},
	})
	attrs, _ := body["Attributes"].(map[string]any)
	if attrs["ApproximateNumberOfMessages"] != "0" {
		t.Fatalf("approx counter after reap = %v, want 0", attrs)
	}
}

func TestSQSServer_TagQueueRequiresTags(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	queueURL := createSQSQueueForTest(t, node, "tag-required")

	// Missing Tags is a MissingParameter, not a silent no-op.
	status, out := callSQS(t, node, sqsTagQueueTarget, map[string]any{
		"QueueUrl": queueURL,
	})
	if status != http.StatusBadRequest || out["__type"] != sqsErrMissingParameter {
		t.Fatalf("tag without Tags: %d %v", status, out)
	}
	status, out = callSQS(t, node, sqsUntagQueueTarget, map[string]any{
		"QueueUrl": queueURL,
	})
	if status != http.StatusBadRequest || out["__type"] != sqsErrMissingParameter {
		t.Fatalf("untag without TagKeys: %d %v", status, out)
	}
}
