package backup

import (
	"bytes"
	"encoding/json"
	"testing"
)

// liveSQSEntries encodes inRoot and returns the decoded live
// (userKey, userValue) records — used to inspect derived vis / byage /
// dedup rows directly, since the M5-1 decoder ignores side records and a
// directory round-trip cannot observe them. Mirrors liveDDBEntries.
func liveSQSEntries(t *testing.T, inRoot string) []RoundTripEntry {
	t.Helper()
	b := newSnapshotBuilder(sqsEncTS)
	if err := NewSQSRecordEncoder(inRoot).Encode(b); err != nil {
		t.Fatalf("SQSRecordEncoder.Encode: %v", err)
	}
	var buf bytes.Buffer
	if _, err := b.WriteTo(&buf); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	entries, _, err := DecodeLiveEntries(&buf)
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	return entries
}

// splitSQSEntries groups encoded entries by family for assertion. The five
// categories the M5-2 derivation rules touch are data / vis / byage /
// dedup / group; queue meta / gen / seq are ignored.
func splitSQSEntries(entries []RoundTripEntry) (data, vis, byage, dedup, group []RoundTripEntry) {
	for _, e := range entries {
		switch {
		case bytes.HasPrefix(e.UserKey, []byte(SQSMsgDataPrefix)):
			data = append(data, e)
		case bytes.HasPrefix(e.UserKey, []byte(SQSMsgVisPrefix)):
			vis = append(vis, e)
		case bytes.HasPrefix(e.UserKey, []byte(SQSMsgByAgePrefix)):
			byage = append(byage, e)
		case bytes.HasPrefix(e.UserKey, []byte(SQSMsgDedupPrefix)):
			dedup = append(dedup, e)
		case bytes.HasPrefix(e.UserKey, []byte(SQSMsgGroupPrefix)):
			group = append(group, e)
		}
	}
	return data, vis, byage, dedup, group
}

// TestSQSEncodeSideRecordsCrossCheckClassic pins byte-identical key + value
// output between this package and the live adapter constructors for a
// classic FIFO queue carrying varied dedup state. It is the §"Encoder
// cross-check" pattern the parent design mandates and the M5-2 design
// doc lists as gold-standard. Partitioned variant deferred to M5-3.
func TestSQSEncodeSideRecordsCrossCheckClassic(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "billing-fifo"
	const fixedSendTs = int64(1_700_000_000_000)
	const fixedAvailTs = int64(1_700_000_000_500)
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"billing-fifo","fifo":true,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000500,"sequence_number":1,"message_group_id":"g1","message_deduplication_id":"d1"}`),
			[]byte(`{"format_version":1,"message_id":"m2","body":"b","send_timestamp_millis":1700000000100,"available_at_millis":1700000000600,"sequence_number":2,"message_group_id":"g1","message_deduplication_id":"d2"}`),
		},
	)

	entries := liveSQSEntries(t, in)
	data, vis, byage, dedup, group := splitSQSEntries(entries)
	if len(data) != 2 || len(vis) != 2 || len(byage) != 2 || len(dedup) != 2 {
		t.Fatalf("families = data=%d vis=%d byage=%d dedup=%d, want 2/2/2/2", len(data), len(vis), len(byage), len(dedup))
	}
	if len(group) != 0 {
		t.Fatalf("group rows = %d, want 0 (group MUST NOT be emitted — see design doc / gemini critical #885)", len(group))
	}

	// Direct constructor cross-check: every emitted vis/byage/dedup key
	// must equal the byte-string the adapter's send path would write
	// for the same (queue, gen, ts, msgID, dedup-id) tuple.
	wantKeys := map[string][]byte{
		"vis[m1]":   sqsMsgVisKeyBytes(queue, sqsRestoreGeneration, fixedAvailTs, "m1"),
		"vis[m2]":   sqsMsgVisKeyBytes(queue, sqsRestoreGeneration, fixedAvailTs+100, "m2"),
		"byage[m1]": sqsMsgByAgeKeyBytes(queue, sqsRestoreGeneration, fixedSendTs, "m1"),
		"byage[m2]": sqsMsgByAgeKeyBytes(queue, sqsRestoreGeneration, fixedSendTs+100, "m2"),
		"dedup[d1]": sqsMsgDedupKeyBytes(queue, sqsRestoreGeneration, "d1"),
		"dedup[d2]": sqsMsgDedupKeyBytes(queue, sqsRestoreGeneration, "d2"),
	}
	keyToValue := indexEntriesByKey(append(append(vis, byage...), dedup...))
	for label, want := range wantKeys {
		if _, ok := keyToValue[string(want)]; !ok {
			t.Errorf("missing %s key: %x", label, want)
		}
	}

	// Value cross-check: vis + byage values equal []byte(messageID).
	for _, c := range []struct{ label, msgID string }{
		{"vis[m1]", "m1"}, {"vis[m2]", "m2"}, {"byage[m1]", "m1"}, {"byage[m2]", "m2"},
	} {
		got := keyToValue[string(wantKeys[c.label])]
		if !bytes.Equal(got, []byte(c.msgID)) {
			t.Errorf("%s value = %x, want %q", c.label, got, c.msgID)
		}
	}

	// Dedup value cross-check: sqsFifoDedupRecord JSON with all four
	// fields the live writer (sqs_fifo.go:219-223) produces.
	wantD1 := sqsFifoDedupRecord{
		MessageID: "m1", SendTimestampMs: fixedSendTs,
		ExpiresAtMillis: fixedSendTs + sqsFifoDedupWindowMillis, OriginalSequence: 1,
	}
	assertDedupValue(t, "d1", keyToValue[string(wantKeys["dedup[d1]"])], wantD1)
}

// indexEntriesByKey collapses a slice of RoundTripEntry to a key→value map
// for direct lookup. Test helper kept terse for cyclomatic-complexity room.
func indexEntriesByKey(entries []RoundTripEntry) map[string][]byte {
	out := make(map[string][]byte, len(entries))
	for _, e := range entries {
		out[string(e.UserKey)] = e.UserValue
	}
	return out
}

// assertDedupValue compares an emitted dedup row's value against the
// sqsFifoDedupRecord the live writer would have produced for the same
// fixture, fatal-fail on JSON parse error and t.Errorf on field mismatch.
func assertDedupValue(t *testing.T, label string, got []byte, want sqsFifoDedupRecord) {
	t.Helper()
	var rec sqsFifoDedupRecord
	if err := json.Unmarshal(got, &rec); err != nil {
		t.Fatalf("unmarshal dedup[%s]: %v", label, err)
	}
	if rec != want {
		t.Errorf("dedup[%s] = %+v, want %+v", label, rec, want)
	}
}

// TestSQSEncodeSideRecordsStandardQueueOmitsFIFOFamilies pins that a
// non-FIFO queue emits vis + byage but NEVER dedup or group, matching
// adapter/sqs_keys.go semantics (dedup keys make no sense for standard
// queues and would poison the keyspace).
func TestSQSEncodeSideRecordsStandardQueueOmitsFIFOFamilies(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "standard-q"
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"standard-q","fifo":false,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":0,"message_deduplication_id":"d-set-but-ignored"}`),
		},
	)

	_, vis, byage, dedup, group := splitSQSEntries(liveSQSEntries(t, in))
	if len(vis) != 1 || len(byage) != 1 {
		t.Fatalf("vis=%d byage=%d, want 1/1 (standard queue still gets these)", len(vis), len(byage))
	}
	if len(dedup) != 0 {
		t.Errorf("dedup rows on a standard queue = %d, want 0 (would poison the keyspace)", len(dedup))
	}
	if len(group) != 0 {
		t.Errorf("group rows on a standard queue = %d, want 0", len(group))
	}
}

// TestSQSEncodeSideRecordsEmptyDedupOmitsDedupRow pins that a FIFO
// message with an empty message_dedup_id gets vis + byage but no
// dedup row. The live writer only inserts dedup when the producer
// supplied a dedup-id, so the encoder must mirror that gate.
func TestSQSEncodeSideRecordsEmptyDedupOmitsDedupRow(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "fifo-empty-dedup"
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"fifo-empty-dedup","fifo":true,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":1,"message_group_id":"g1","message_deduplication_id":""}`),
		},
	)

	_, vis, byage, dedup, _ := splitSQSEntries(liveSQSEntries(t, in))
	if len(vis) != 1 || len(byage) != 1 {
		t.Fatalf("vis=%d byage=%d, want 1/1", len(vis), len(byage))
	}
	if len(dedup) != 0 {
		t.Errorf("dedup rows for empty dedup-id = %d, want 0", len(dedup))
	}
}

// TestSQSEncodeSideRecordsNoGroupRows is the regression pin for gemini
// critical #885: a FIFO queue with multiple distinct message_group_ids
// MUST produce zero !sqs|msg|group| rows. Any row would falsely indicate
// a held group lock and permanently block receives for that group (the
// live loadFifoGroupLock returns the parsed lock whenever the key exists;
// only missing-key returns nil).
func TestSQSEncodeSideRecordsNoGroupRows(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const queue = "fifo-multi-group"
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"fifo-multi-group","fifo":true,"partition_count":1,"generation":1}`),
		[][]byte{
			[]byte(`{"format_version":1,"message_id":"m1","body":"a","send_timestamp_millis":1700000000000,"available_at_millis":1700000000000,"sequence_number":1,"message_group_id":"g1","message_deduplication_id":"d1"}`),
			[]byte(`{"format_version":1,"message_id":"m2","body":"b","send_timestamp_millis":1700000000100,"available_at_millis":1700000000100,"sequence_number":2,"message_group_id":"g2","message_deduplication_id":"d2"}`),
			[]byte(`{"format_version":1,"message_id":"m3","body":"c","send_timestamp_millis":1700000000200,"available_at_millis":1700000000200,"sequence_number":3,"message_group_id":"g3","message_deduplication_id":"d3"}`),
		},
	)

	_, _, _, _, group := splitSQSEntries(liveSQSEntries(t, in))
	if len(group) != 0 {
		t.Fatalf("group rows = %d (keys=%x), want 0 — emission would block all groups post-restore (gemini critical #885)", len(group), groupKeysHex(group))
	}
}

func groupKeysHex(rows []RoundTripEntry) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, string(r.UserKey))
	}
	return out
}
