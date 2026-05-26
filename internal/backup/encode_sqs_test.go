package backup

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/cockroachdb/errors"
)

const sqsEncTS uint64 = 0x0001_8F1A_2B3C_00EE

// writeSQSQueue writes a _queue.json and messages.jsonl under
// <root>/sqs/<EncodeSegment(queue)>/.
func writeSQSQueue(t *testing.T, root, queue string, queueJSON []byte, msgLines [][]byte) {
	t.Helper()
	dir := filepath.Join(root, "sqs", EncodeSegment([]byte(queue)))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "_queue.json"), queueJSON, 0o600); err != nil {
		t.Fatalf("WriteFile _queue.json: %v", err)
	}
	if len(msgLines) > 0 {
		if err := os.WriteFile(filepath.Join(dir, "messages.jsonl"), bytes.Join(msgLines, []byte("\n")), 0o600); err != nil {
			t.Fatalf("WriteFile messages.jsonl: %v", err)
		}
	}
}

// encodeSQSTree runs the SQS reverse encoder over inRoot and returns the
// EKVPBBL1 bytes.
func encodeSQSTree(t *testing.T, inRoot string) []byte {
	t.Helper()
	b := newSnapshotBuilder(sqsEncTS)
	if err := NewSQSRecordEncoder(inRoot).Encode(b); err != nil {
		t.Fatalf("SQSRecordEncoder.Encode: %v", err)
	}
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	return buf.Bytes()
}

// decodeSQSAndRead decodes fsm and returns the queue's _queue.json and its
// messages.jsonl records.
func decodeSQSAndRead(t *testing.T, fsm []byte, queue string) (sqsQueueMetaPublic, []sqsMessageRecord) {
	t.Helper()
	out := t.TempDir()
	if _, err := DecodeSnapshot(bytes.NewReader(fsm), DecodeOptions{
		OutRoot:  out,
		Adapters: AdapterSet{SQS: true},
	}); err != nil {
		t.Fatalf("DecodeSnapshot: %v", err)
	}
	dir := filepath.Join(out, "sqs", EncodeSegment([]byte(queue)))
	metaData, err := os.ReadFile(filepath.Join(dir, "_queue.json"))
	if err != nil {
		t.Fatalf("read decoded _queue.json: %v", err)
	}
	var meta sqsQueueMetaPublic
	if err := json.Unmarshal(metaData, &meta); err != nil {
		t.Fatalf("unmarshal decoded _queue.json: %v", err)
	}
	return meta, readDecodedSQSMessages(t, dir)
}

// readDecodedSQSMessages reads a decoded messages.jsonl (absent = empty
// queue), failing the test on any open/scan/parse error.
func readDecodedSQSMessages(t *testing.T, dir string) []sqsMessageRecord {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, "messages.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("open decoded messages.jsonl: %v", err)
	}
	defer func() { _ = f.Close() }()
	var msgs []sqsMessageRecord
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := sc.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var rec sqsMessageRecord
		if jerr := json.Unmarshal(line, &rec); jerr != nil {
			t.Fatalf("unmarshal decoded message: %v", jerr)
		}
		msgs = append(msgs, rec)
	}
	if serr := sc.Err(); serr != nil {
		t.Fatalf("scan decoded messages.jsonl: %v", serr)
	}
	return msgs
}

// TestSQSEncodeStandardQueueRoundTrip pins the gold-standard round-trip
// for a standard (non-FIFO) queue with two messages.
func TestSQSEncodeStandardQueueRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "orders"
	writeSQSQueue(t, in, queue, []byte(`{"format_version":1,"name":"orders",`+
		`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			[]byte(`{"message_id":"m1","body":"hello","send_timestamp_millis":1000,"available_at_millis":1000}`),
			[]byte(`{"message_id":"m2","body":"world","send_timestamp_millis":2000,"available_at_millis":2000}`),
		})

	meta, msgs := decodeSQSAndRead(t, encodeSQSTree(t, in), queue)
	if meta.Name != queue || meta.VisibilityTimeoutSeconds != 30 || meta.MessageRetentionSeconds != 345600 {
		t.Fatalf("decoded meta = %+v", meta)
	}
	got := map[string]string{}
	for _, m := range msgs {
		got[m.MessageID] = string(m.Body)
	}
	if len(got) != 2 || got["m1"] != "hello" || got["m2"] != "world" {
		t.Fatalf("decoded messages = %#v", got)
	}
}

// TestSQSEncodeFifoSeqCounter pins that a FIFO queue emits a
// !sqs|queue|seq| counter = max(sequence_number)+1, and the message bodies
// round-trip.
func TestSQSEncodeFifoSeqCounter(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "orders.fifo"
	writeSQSQueue(t, in, queue, []byte(`{"format_version":1,"name":"orders.fifo","fifo":true,`+
		`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			[]byte(`{"message_id":"m1","body":"a","send_timestamp_millis":1000,"available_at_millis":1000,"message_group_id":"g","sequence_number":5}`),
			[]byte(`{"message_id":"m2","body":"b","send_timestamp_millis":2000,"available_at_millis":2000,"message_group_id":"g","sequence_number":9}`),
		})

	entries, _, err := DecodeLiveEntries(bytes.NewReader(encodeSQSTree(t, in)))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	seq := sqsFindEntry(entries, sqsQueueSeqKeyBytes(queue))
	if seq == nil {
		t.Fatal("no !sqs|queue|seq| counter emitted for FIFO queue")
	}
	if got := string(seq.UserValue); got != "10" {
		t.Fatalf("seq counter = %q, want \"10\" (max 9 + 1)", got)
	}
	gen := sqsFindEntry(entries, sqsQueueGenKeyBytes(queue))
	if gen == nil || string(gen.UserValue) != strconv.FormatUint(sqsRestoreGeneration, 10) {
		t.Fatalf("gen counter = %v, want %d", gen, sqsRestoreGeneration)
	}
}

// sqsFindEntry returns the first live entry with the given user key, or nil.
func sqsFindEntry(entries []RoundTripEntry, key []byte) *RoundTripEntry {
	for i := range entries {
		if bytes.Equal(entries[i].UserKey, key) {
			return &entries[i]
		}
	}
	return nil
}

// TestSQSEncodeBinaryBodyRoundTrip pins that a non-UTF-8 message body
// (stored as a {base64:...} envelope in messages.jsonl) round-trips.
func TestSQSEncodeBinaryBodyRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "bin"
	// body bytes {0xFF,0x00,0xFE} are not valid UTF-8 → base64url envelope.
	writeSQSQueue(t, in, queue, []byte(`{"format_version":1,"name":"bin",`+
		`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			[]byte(`{"message_id":"b1","body":{"base64":"_wD-"},"send_timestamp_millis":1000,"available_at_millis":1000}`),
		})

	_, msgs := decodeSQSAndRead(t, encodeSQSTree(t, in), queue)
	if len(msgs) != 1 {
		t.Fatalf("decoded %d messages, want 1", len(msgs))
	}
	if !bytes.Equal([]byte(msgs[0].Body), []byte{0xFF, 0x00, 0xFE}) {
		t.Fatalf("binary body round-trip = %x", []byte(msgs[0].Body))
	}
}

// TestSQSEncodeRejectsPartitioned pins fail-closed for an HT-FIFO queue.
func TestSQSEncodeRejectsPartitioned(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "ht.fifo", []byte(`{"format_version":1,"name":"ht.fifo","fifo":true,`+
		`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0,"partition_count":4}`), nil)
	b := newSnapshotBuilder(sqsEncTS)
	if err := NewSQSRecordEncoder(in).Encode(b); !errors.Is(err, ErrSQSEncodeUnsupportedPartitioned) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeUnsupportedPartitioned", err)
	}
}

// TestSQSEncodeRejectsNonRegularQueueMeta pins the file guard: a
// _queue.json that is a directory is refused with ErrSQSEncodeNotRegular.
func TestSQSEncodeRejectsNonRegularQueueMeta(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	if err := os.MkdirAll(filepath.Join(in, "sqs", EncodeSegment([]byte("q")), "_queue.json"), 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	b := newSnapshotBuilder(sqsEncTS)
	if err := NewSQSRecordEncoder(in).Encode(b); !errors.Is(err, ErrSQSEncodeNotRegular) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeNotRegular", err)
	}
}

// TestSQSEncodeRejectsEmptyMessageID pins fail-closed on a message lacking
// a message_id (which would otherwise key on the empty string).
func TestSQSEncodeRejectsEmptyMessageID(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "q-emptyid"
	writeSQSQueue(t, in, queue, []byte(`{"format_version":1,"name":"q-emptyid",`+
		`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{[]byte(`{"message_id":"","body":"x","send_timestamp_millis":1,"available_at_millis":1}`)})
	b := newSnapshotBuilder(sqsEncTS)
	if err := NewSQSRecordEncoder(in).Encode(b); !errors.Is(err, ErrSQSEncodeInvalidMessage) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidMessage", err)
	}
}

// TestSQSEncodeRejectsMaxSequence pins that a FIFO message at the uint64
// sequence ceiling fails closed rather than wrapping the seq counter to 0.
func TestSQSEncodeRejectsMaxSequence(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "max.fifo"
	writeSQSQueue(t, in, queue, []byte(`{"format_version":1,"name":"max.fifo","fifo":true,`+
		`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			[]byte(`{"message_id":"m1","body":"a","send_timestamp_millis":1,"available_at_millis":1,"message_group_id":"g","sequence_number":18446744073709551615}`),
		})
	b := newSnapshotBuilder(sqsEncTS)
	if err := NewSQSRecordEncoder(in).Encode(b); !errors.Is(err, ErrSQSEncodeInvalidMessage) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidMessage", err)
	}
}

// TestSQSEncodeRejectsMalformedBodyObject pins that a non-string body
// object that is not a {base64:...} envelope fails closed rather than
// silently restoring an empty body.
func TestSQSEncodeRejectsMalformedBodyObject(t *testing.T) {
	t.Parallel()
	// Note: a JSON `null` body is not covered — encoding/json skips
	// UnmarshalJSON for null, leaving an empty body (lenient, and
	// unreachable from a decoder-produced dump).
	for _, body := range []string{`{}`, `{"oops":"x"}`} {
		in := t.TempDir()
		const queue = "q"
		writeSQSQueue(t, in, queue, []byte(`{"format_version":1,"name":"q",`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
			[][]byte{[]byte(`{"message_id":"m1","body":` + body + `,"send_timestamp_millis":1,"available_at_millis":1}`)})
		b := newSnapshotBuilder(sqsEncTS)
		if err := NewSQSRecordEncoder(in).Encode(b); !errors.Is(err, ErrSQSEncodeInvalidMessage) {
			t.Fatalf("body %s: Encode err = %v, want ErrSQSEncodeInvalidMessage", body, err)
		}
	}
}

// TestSQSEncodeMissingDirIsNoop pins that an absent sqs/ dir is a no-op.
func TestSQSEncodeMissingDirIsNoop(t *testing.T) {
	t.Parallel()
	b := newSnapshotBuilder(sqsEncTS)
	if err := NewSQSRecordEncoder(t.TempDir()).Encode(b); err != nil {
		t.Fatalf("Encode on empty tree: %v", err)
	}
	if b.Len() != 0 {
		t.Fatalf("entries = %d, want 0", b.Len())
	}
}

// TestSQSEncodeRejectsUnknownFormatVersion pins the _queue.json format gate.
func TestSQSEncodeRejectsUnknownFormatVersion(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "q", []byte(`{"format_version":99,"name":"q"}`), nil)
	b := newSnapshotBuilder(sqsEncTS)
	if err := NewSQSRecordEncoder(in).Encode(b); !errors.Is(err, ErrSQSEncodeInvalidQueue) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidQueue", err)
	}
}
