package backup

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
)

func newSQSEncoder(t *testing.T) (*SQSEncoder, string) {
	t.Helper()
	root := t.TempDir()
	return NewSQSEncoder(root), root
}

func encodeQueueMetaValue(t *testing.T, m map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := append([]byte{}, storedSQSMetaMagic...)
	return append(out, body...)
}

func encodeMessageValue(t *testing.T, m map[string]any) []byte {
	t.Helper()
	body, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	out := append([]byte{}, storedSQSMsgMagic...)
	return append(out, body...)
}

func readQueueJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	b, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

// floatField fetches a JSON-decoded numeric field as float64, asserting it
// exists. JSON unmarshals all numbers to float64 by default; this wrapper
// keeps the type-assertion in one place and gives lint a structured failure
// to point at if the assumption breaks.
func floatField(t *testing.T, m map[string]any, key string) float64 {
	t.Helper()
	v, ok := m[key]
	if !ok {
		t.Fatalf("field %q missing in %+v", key, m)
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("field %q = %T(%v), want float64", key, v, v)
	}
	return f
}

func readMessagesJSONL(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer f.Close()
	var out []map[string]any
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		var rec map[string]any
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatalf("unmarshal %q: %v", sc.Text(), err)
		}
		out = append(out, rec)
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	return out
}

func TestSQS_QueueMetaRoundTrip(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	key := EncodeQueueMetaKey("orders-fifo.fifo")
	val := encodeQueueMetaValue(t, map[string]any{
		"name":                         "orders-fifo.fifo",
		"is_fifo":                      true,
		"content_based_dedup":          false,
		"visibility_timeout_seconds":   30,
		"message_retention_seconds":    345600,
		"delay_seconds":                0,
		"receive_message_wait_seconds": 0,
		"maximum_message_size":         262144,
	})
	if err := enc.HandleQueueMeta(key, val); err != nil {
		t.Fatalf("HandleQueueMeta: %v", err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	got := readQueueJSON(t, filepath.Join(root, "sqs", "orders-fifo.fifo", "_queue.json"))
	if got["name"] != "orders-fifo.fifo" {
		t.Fatalf("name = %v", got["name"])
	}
	if got["fifo"] != true {
		t.Fatalf("fifo = %v", got["fifo"])
	}
	if floatField(t, got, "visibility_timeout_seconds") != 30 {
		t.Fatalf("visibility_timeout_seconds = %v", got["visibility_timeout_seconds"])
	}
	if floatField(t, got, "format_version") != 1 {
		t.Fatalf("format_version = %v", got["format_version"])
	}
}

func TestSQS_MessagesSortedByTimestampSeqMessageID(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	queue := "orders-fifo.fifo"
	if err := enc.HandleQueueMeta(EncodeQueueMetaKey(queue), encodeQueueMetaValue(t, map[string]any{
		"name": queue, "is_fifo": true, "visibility_timeout_seconds": 30, "message_retention_seconds": 60,
	})); err != nil {
		t.Fatal(err)
	}
	// Insert in scrambled order; after Finalize the JSONL must be sorted
	// by (send_ts, seq, msg_id).
	send := func(msgID string, sendMs int64, seq uint64, body string) {
		t.Helper()
		key := EncodeMsgDataKey(queue, 7, msgID)
		val := encodeMessageValue(t, map[string]any{
			"message_id":            msgID,
			"body":                  []byte(body),
			"send_timestamp_millis": sendMs,
			"available_at_millis":   sendMs,
			"queue_generation":      7,
			"sequence_number":       seq,
		})
		if err := enc.HandleMessageData(key, val); err != nil {
			t.Fatalf("HandleMessageData: %v", err)
		}
	}
	send("msg-c", 100, 3, "c")
	send("msg-a", 90, 1, "a")
	send("msg-b", 100, 2, "b")
	send("msg-tieA", 200, 5, "tA")
	send("msg-tieB", 200, 5, "tB")
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	recs := readMessagesJSONL(t, filepath.Join(root, "sqs", queue, "messages.jsonl"))
	wantOrder := []string{"msg-a", "msg-b", "msg-c", "msg-tieA", "msg-tieB"}
	if len(recs) != len(wantOrder) {
		t.Fatalf("len = %d want %d", len(recs), len(wantOrder))
	}
	for i, w := range wantOrder {
		if recs[i]["message_id"] != w {
			t.Fatalf("recs[%d].message_id = %v, want %v", i, recs[i]["message_id"], w)
		}
	}
}

func TestSQS_DefaultZeroesVisibilityState(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	queue := "q"
	if err := enc.HandleQueueMeta(EncodeQueueMetaKey(queue), encodeQueueMetaValue(t, map[string]any{
		"name": queue, "visibility_timeout_seconds": 30, "message_retention_seconds": 60,
	})); err != nil {
		t.Fatal(err)
	}
	val := encodeMessageValue(t, map[string]any{
		"message_id":            "m1",
		"body":                  []byte("payload"),
		"send_timestamp_millis": 1000,
		"queue_generation":      1,
		"visible_at_millis":     5000, // populated mid-flight
		"current_receipt_token": []byte{0x01, 0x02},
		"receive_count":         3,
		"first_receive_millis":  4500,
	})
	if err := enc.HandleMessageData(EncodeMsgDataKey(queue, 1, "m1"), val); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	recs := readMessagesJSONL(t, filepath.Join(root, "sqs", queue, "messages.jsonl"))
	if len(recs) != 1 {
		t.Fatalf("recs = %d", len(recs))
	}
	r := recs[0]
	if floatField(t, r, "visible_at_millis") != 0 {
		t.Fatalf("visible_at_millis = %v want 0", r["visible_at_millis"])
	}
	if _, present := r["current_receipt_token"]; present {
		t.Fatalf("current_receipt_token must be omitted on default zeroing, got %v", r["current_receipt_token"])
	}
	if floatField(t, r, "receive_count") != 0 {
		t.Fatalf("receive_count = %v want 0", r["receive_count"])
	}
}

func TestSQS_PreserveVisibilityKeepsLiveFields(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	enc.WithPreserveVisibility(true)
	queue := "q"
	if err := enc.HandleQueueMeta(EncodeQueueMetaKey(queue), encodeQueueMetaValue(t, map[string]any{
		"name": queue, "visibility_timeout_seconds": 30, "message_retention_seconds": 60,
	})); err != nil {
		t.Fatal(err)
	}
	val := encodeMessageValue(t, map[string]any{
		"message_id":            "m1",
		"body":                  []byte("payload"),
		"send_timestamp_millis": 1000,
		"queue_generation":      1,
		"visible_at_millis":     5000,
		"receive_count":         3,
		"first_receive_millis":  4500,
	})
	if err := enc.HandleMessageData(EncodeMsgDataKey(queue, 1, "m1"), val); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	recs := readMessagesJSONL(t, filepath.Join(root, "sqs", queue, "messages.jsonl"))
	if floatField(t, recs[0], "visible_at_millis") != 5000 {
		t.Fatalf("visible_at_millis = %v want 5000", recs[0]["visible_at_millis"])
	}
	if floatField(t, recs[0], "receive_count") != 3 {
		t.Fatalf("receive_count = %v want 3", recs[0]["receive_count"])
	}
}

func TestSQS_OrphanMessagesEmitWarning(t *testing.T) {
	t.Parallel()
	enc, _ := newSQSEncoder(t)
	var events []string
	enc.WithWarnSink(func(event string, fields ...any) {
		events = append(events, event)
	})
	// Message arrives before the queue meta record (typical lex order)
	// AND no meta record arrives at all (e.g., the queue was deleted
	// before the snapshot was taken). The encoder buffers, then warns.
	val := encodeMessageValue(t, map[string]any{
		"message_id":            "stranded",
		"body":                  []byte("orphan"),
		"send_timestamp_millis": 1,
		"queue_generation":      1,
	})
	if err := enc.HandleMessageData(EncodeMsgDataKey("ghost-queue", 1, "stranded"), val); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0] != "sqs_orphan_messages" {
		t.Fatalf("events = %v want [sqs_orphan_messages]", events)
	}
}

func TestSQS_RejectsValueWithoutMagicPrefix(t *testing.T) {
	t.Parallel()
	enc, _ := newSQSEncoder(t)
	t.Run("queue-meta", func(t *testing.T) {
		err := enc.HandleQueueMeta(EncodeQueueMetaKey("q"), []byte(`{"name":"q"}`))
		if !errors.Is(err, ErrSQSInvalidQueueMeta) {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("message", func(t *testing.T) {
		err := enc.HandleMessageData(EncodeMsgDataKey("q", 1, "m"), []byte(`{"message_id":"m"}`))
		if !errors.Is(err, ErrSQSInvalidMessage) {
			t.Fatalf("err=%v", err)
		}
	})
}

func TestSQS_RejectsTrailingGarbageAfterMagic(t *testing.T) {
	t.Parallel()
	enc, _ := newSQSEncoder(t)
	bad := append([]byte{}, storedSQSMsgMagic...)
	bad = append(bad, []byte("not json")...)
	err := enc.HandleMessageData(EncodeMsgDataKey("q", 1, "m"), bad)
	if !errors.Is(err, ErrSQSInvalidMessage) {
		t.Fatalf("err=%v", err)
	}
}

func TestSQS_RejectsKeyWithWrongPrefix(t *testing.T) {
	t.Parallel()
	enc, _ := newSQSEncoder(t)
	err := enc.HandleQueueMeta([]byte("!unknown|prefix|q"), encodeQueueMetaValue(t, map[string]any{}))
	if !errors.Is(err, ErrSQSMalformedKey) {
		t.Fatalf("err=%v", err)
	}
}

func TestSQS_PeekMsgDataKeyParsesComponents(t *testing.T) {
	t.Parallel()
	key := EncodeMsgDataKey("orders", 7, "msg-id")
	got, err := peekMsgDataKey(key)
	if err != nil {
		t.Fatalf("peekMsgDataKey: %v", err)
	}
	if got.Encoded != "b3JkZXJz" { // base64url("orders")
		t.Fatalf("encoded queue = %q", got.Encoded)
	}
	if len(got.GenRaw) != genBytes {
		t.Fatalf("gen length = %d", len(got.GenRaw))
	}
	if got.GenRaw[7] != 7 { // BE: high bytes zero, low byte = 7
		t.Fatalf("gen low byte = %x", got.GenRaw[7])
	}
	if got.MsgID != "bXNnLWlk" { // base64url("msg-id")
		t.Fatalf("msg id = %q", got.MsgID)
	}
}

func TestSQS_IncludeSideRecordsBuffersBetweenFinalize(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	enc.WithIncludeSideRecords(true)
	queue := "q"
	if err := enc.HandleQueueMeta(EncodeQueueMetaKey(queue), encodeQueueMetaValue(t, map[string]any{
		"name": queue, "visibility_timeout_seconds": 30, "message_retention_seconds": 60,
	})); err != nil {
		t.Fatal(err)
	}
	val := encodeMessageValue(t, map[string]any{
		"message_id": "m", "body": []byte("v"), "send_timestamp_millis": 1, "queue_generation": 1,
	})
	if err := enc.HandleMessageData(EncodeMsgDataKey(queue, 1, "m"), val); err != nil {
		t.Fatal(err)
	}
	// Synthesise a fake vis side-record. parseSQSGenericKey only looks
	// at the encoded queue prefix, so a payload-shaped tail is fine.
	visKey := append([]byte(SQSMsgVisPrefix), []byte("cQ")...) // base64url("q")
	visKey = append(visKey, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)
	if err := enc.HandleSideRecord(SQSMsgVisPrefix, visKey, []byte("ignored-payload")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	side := filepath.Join(root, "sqs", queue, "_internals", "side_records.jsonl")
	if _, err := os.Stat(side); err != nil {
		t.Fatalf("expected side file: %v", err)
	}
}

func TestSQS_ParseMessageDataKey_RejectsEmptyMsgIDSegment(t *testing.T) {
	t.Parallel()
	// Synthesise a key whose msg-id segment is empty: prefix +
	// base64url("q") + 8-byte gen, nothing after. AWS SQS message
	// IDs are non-empty by construction; an empty trailer cannot be
	// a legitimate snapshot record.
	key := append([]byte(SQSMsgDataPrefix), []byte("cQ")...)
	key = append(key, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)
	if _, err := parseSQSMessageDataKey(key); !errors.Is(err, ErrSQSMalformedKey) {
		t.Fatalf("err=%v want ErrSQSMalformedKey for empty msg-id", err)
	}
}

func TestSQS_ParseGenericKey_RejectsTrailerlessKey(t *testing.T) {
	t.Parallel()
	// Side-record key whose entire suffix is base64url-clean (no
	// trailer bytes). Must surface as malformed rather than treating
	// the whole tail as the queue segment.
	key := append([]byte(SQSMsgVisPrefix), []byte("cQQQ")...)
	if _, err := parseSQSGenericKey(key, SQSMsgVisPrefix); !errors.Is(err, ErrSQSMalformedKey) {
		t.Fatalf("err=%v want ErrSQSMalformedKey for trailerless side-record key", err)
	}
}

func TestSQS_SideRecordsFlushedEvenWhenZeroMessages(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	enc.WithIncludeSideRecords(true)
	queue := "purged"
	if err := enc.HandleQueueMeta(EncodeQueueMetaKey(queue), encodeQueueMetaValue(t, map[string]any{
		"name": queue, "visibility_timeout_seconds": 30, "message_retention_seconds": 60,
	})); err != nil {
		t.Fatal(err)
	}
	// Side record only, no message-data records — purged queue scenario.
	visKey := append([]byte(SQSMsgVisPrefix), []byte("cHVyZ2Vk")...) // base64url("purged")
	visKey = append(visKey, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)
	if err := enc.HandleSideRecord(SQSMsgVisPrefix, visKey, []byte("opaque")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(root, "sqs", queue, "_internals", "side_records.jsonl")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected side-records file even with zero messages: %v", err)
	}
}

// TestSQS_OrphanQueueSideRecordsPreserved is the regression for Codex
// P2 round 8: when a queue's !sqs|queue|meta record is missing (e.g.
// after DeleteQueue left tombstones but the meta row was removed) and
// --include-sqs-side-records is on, side records were silently dropped
// alongside any orphan messages. The opt-in contract is the opposite:
// side records exist precisely so deletion-era state is recoverable.
// Now those records flush to a `<encoded>.orphan` directory while the
// orphan-messages warning fires.
func TestSQS_OrphanQueueSideRecordsPreserved(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	enc.WithIncludeSideRecords(true)
	var events []string
	enc.WithWarnSink(func(event string, _ ...any) { events = append(events, event) })
	// Side record arrives without a meta row first (deletion-era).
	encQueue := "ZGVsZXRlZA" // base64url("deleted")
	visKey := append([]byte(SQSMsgVisPrefix), []byte(encQueue)...)
	visKey = append(visKey, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)
	if err := enc.HandleSideRecord(SQSMsgVisPrefix, visKey, []byte("vis-record")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 || events[0] != "sqs_orphan_messages" {
		t.Fatalf("expected sqs_orphan_messages warning, got %v", events)
	}
	want := filepath.Join(root, "sqs", encQueue+".orphan", "_internals", "side_records.jsonl")
	if _, err := os.Stat(want); err != nil {
		t.Fatalf("expected orphan side-records file at %s: %v", want, err)
	}
}

// TestSQS_OrphanQueueSideRecordsSuppressedWhenOptOut asserts that the
// orphan-side-records branch is gated on --include-sqs-side-records:
// without the flag, the warning still fires but no .orphan dir is
// created (consistent with the default-off contract for side records).
func TestSQS_OrphanQueueSideRecordsSuppressedWhenOptOut(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	// includeSideRecords is off by default — HandleSideRecord drops
	// the record at intake, so the buffer is empty by Finalize. We
	// exercise the path anyway to confirm no .orphan dir is created.
	encQueue := "b3B0LW91dA" // base64url("opt-out")
	visKey := append([]byte(SQSMsgVisPrefix), []byte(encQueue)...)
	visKey = append(visKey, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02)
	if err := enc.HandleSideRecord(SQSMsgVisPrefix, visKey, []byte("vis")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "sqs", encQueue+".orphan")); !os.IsNotExist(err) {
		t.Fatalf("orphan dir created without --include-sqs-side-records: stat err=%v", err)
	}
}

func TestSQS_DefaultDoesNotEmitInternals(t *testing.T) {
	t.Parallel()
	enc, root := newSQSEncoder(t)
	queue := "q"
	if err := enc.HandleQueueMeta(EncodeQueueMetaKey(queue), encodeQueueMetaValue(t, map[string]any{
		"name": queue, "visibility_timeout_seconds": 30, "message_retention_seconds": 60,
	})); err != nil {
		t.Fatal(err)
	}
	visKey := append([]byte(SQSMsgVisPrefix), []byte("cQ")...)
	visKey = append(visKey, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01)
	if err := enc.HandleSideRecord(SQSMsgVisPrefix, visKey, []byte("ignored")); err != nil {
		t.Fatal(err)
	}
	if err := enc.Finalize(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "sqs", queue, "_internals")); !os.IsNotExist(err) {
		t.Fatalf("expected no _internals dir without --include-sqs-side-records, stat err=%v", err)
	}
}
