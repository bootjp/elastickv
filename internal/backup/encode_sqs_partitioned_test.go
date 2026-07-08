package backup

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/errors"
)

// TestSQSEncodePartitionedMsgDataKeyByteShape pins
// sqsPartitionedMsgDataKeyBytes against the wire format mandated by
// adapter/sqs_keys.go:sqsPartitionedMsgDataKey:
//
//	prefix + base64url(queue) + '|' + BE-u32(partition) + BE-u64(gen) +
//	base64url(messageID).
//
// internal/backup cannot import adapter to cross-check directly (M3b-3
// circular-dep ban; same constraint sqsFifoDedupWindowMillis lives
// under), so this is a manually-assembled byte-equality assertion.
func TestSQSEncodePartitionedMsgDataKeyByteShape(t *testing.T) {
	t.Parallel()
	const (
		queue  = "q1"
		msgID  = "msg-001"
		partN  = uint32(3)
		genCnt = uint64(7)
	)
	want := []byte(SQSPartitionedMsgDataPrefix)
	want = append(want, encodeSQSSegment(queue)...)
	want = append(want, sqsPartitionedQueueTerminator)
	want = binary.BigEndian.AppendUint32(want, partN)
	want = binary.BigEndian.AppendUint64(want, genCnt)
	want = append(want, encodeSQSSegment(msgID)...)

	got := sqsPartitionedMsgDataKeyBytes(queue, partN, genCnt, msgID)
	if !bytes.Equal(got, want) {
		t.Fatalf("partitioned data key mismatch\ngot:  %x\nwant: %x", got, want)
	}
}

// TestSQSEncodePartitionedMsgVisKeyByteShape pins
// sqsPartitionedMsgVisKeyBytes: prefix + base64url(queue) + '|' +
// BE-u32(partition) + BE-u64(gen) + BE-u64(visibleAt) +
// base64url(messageID).
func TestSQSEncodePartitionedMsgVisKeyByteShape(t *testing.T) {
	t.Parallel()
	const (
		queue  = "qvis"
		msgID  = "mid"
		partN  = uint32(0)
		genCnt = uint64(1)
		visMs  = int64(1_700_000_000_000)
	)
	want := []byte(SQSPartitionedMsgVisPrefix)
	want = append(want, encodeSQSSegment(queue)...)
	want = append(want, sqsPartitionedQueueTerminator)
	want = binary.BigEndian.AppendUint32(want, partN)
	want = binary.BigEndian.AppendUint64(want, genCnt)
	want = binary.BigEndian.AppendUint64(want, uint64(visMs))
	want = append(want, encodeSQSSegment(msgID)...)

	got := sqsPartitionedMsgVisKeyBytes(queue, partN, genCnt, visMs, msgID)
	if !bytes.Equal(got, want) {
		t.Fatalf("partitioned vis key mismatch\ngot:  %x\nwant: %x", got, want)
	}
}

// TestSQSEncodePartitionedMsgByAgeKeyByteShape pins
// sqsPartitionedMsgByAgeKeyBytes: prefix + base64url(queue) + '|' +
// BE-u32(partition) + BE-u64(gen) + BE-u64(sendTs) +
// base64url(messageID).
func TestSQSEncodePartitionedMsgByAgeKeyByteShape(t *testing.T) {
	t.Parallel()
	const (
		queue  = "qage"
		msgID  = "mid-age"
		partN  = uint32(15)
		genCnt = uint64(1)
		sendMs = int64(1_600_000_000_000)
	)
	want := []byte(SQSPartitionedMsgByAgePrefix)
	want = append(want, encodeSQSSegment(queue)...)
	want = append(want, sqsPartitionedQueueTerminator)
	want = binary.BigEndian.AppendUint32(want, partN)
	want = binary.BigEndian.AppendUint64(want, genCnt)
	want = binary.BigEndian.AppendUint64(want, uint64(sendMs))
	want = append(want, encodeSQSSegment(msgID)...)

	got := sqsPartitionedMsgByAgeKeyBytes(queue, partN, genCnt, sendMs, msgID)
	if !bytes.Equal(got, want) {
		t.Fatalf("partitioned byage key mismatch\ngot:  %x\nwant: %x", got, want)
	}
}

// TestSQSEncodePartitionedMsgDedupKeyByteShape pins
// sqsPartitionedMsgDedupKeyBytes including the second '|' between
// the variable-length group and dedup segments (CodeRabbit major
// PR #732 round 6 / M5-3 design doc §line 19): prefix +
// base64url(queue) + '|' + BE-u32(partition) +
// BE-u64(sqsRestoreGeneration) + base64url(groupID) + '|' +
// base64url(dedupID). gen is hardcoded to sqsRestoreGeneration in
// the constructor (mirroring the classic sqsMsgDedupKeyBytes pattern;
// satisfies unparam).
func TestSQSEncodePartitionedMsgDedupKeyByteShape(t *testing.T) {
	t.Parallel()
	const (
		queue   = "qdedup"
		groupID = "group-A"
		dedupID = "dedup-001"
		partN   = uint32(2)
	)
	want := []byte(SQSPartitionedMsgDedupPrefix)
	want = append(want, encodeSQSSegment(queue)...)
	want = append(want, sqsPartitionedQueueTerminator)
	want = binary.BigEndian.AppendUint32(want, partN)
	want = binary.BigEndian.AppendUint64(want, sqsRestoreGeneration)
	want = append(want, encodeSQSSegment(groupID)...)
	want = append(want, sqsPartitionedQueueTerminator)
	want = append(want, encodeSQSSegment(dedupID)...)

	got := sqsPartitionedMsgDedupKeyBytes(queue, partN, groupID, dedupID)
	if !bytes.Equal(got, want) {
		t.Fatalf("partitioned dedup key mismatch\ngot:  %x\nwant: %x", got, want)
	}

	// Also pin that the group+dedup terminator is present at the
	// expected offset so a future change that drops it (regressing
	// the FNV-collision class) fails this test.
	offset := len(SQSPartitionedMsgDedupPrefix) +
		len(encodeSQSSegment(queue)) + 1 + // queue + '|'
		4 + 8 + // partition u32 + gen u64
		len(encodeSQSSegment(groupID))
	if got[offset] != sqsPartitionedQueueTerminator {
		t.Fatalf("dedup key byte at offset %d = %x, want '|'", offset, got[offset])
	}
}

// TestSQSEncodeRejectsMissingPartitionOnPartitionedQueue pins gate 1
// of validatePartitioning: a partitioned queue (PartitionCount > 1)
// with a message whose Partition field is nil (legacy pre-M5-3 dump
// shape) fails closed with ErrSQSEncodeMissingPartition. Without
// this gate the message would route to the classic key keyspace
// (addMessage's `partition != nil` dispatch falls to
// sqsMsgDataKeyBytes) and become invisible to the live readers
// scanning the partitioned keyspace (codex P1 v914 v2).
func TestSQSEncodeRejectsMissingPartitionOnPartitionedQueue(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "p.fifo",
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2}`),
		[][]byte{
			// No "partition" field — legacy dump shape.
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000,` +
				`"message_group_id":"g1"}`),
		})
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodeMissingPartition) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeMissingPartition", err)
	}
}

// TestSQSEncodeRejectsOutOfRangePartition pins gate 2: a partitioned
// queue with a message whose *Partition >= PartitionCount fails
// closed with ErrSQSEncodeOutOfRangePartition. Such a dump is
// internally inconsistent (operator hand-edit or M5-3 decoder bug).
func TestSQSEncodeRejectsOutOfRangePartition(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "p.fifo",
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2}`),
		[][]byte{
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000,` +
				`"message_group_id":"g1","partition":5}`),
		})
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodeOutOfRangePartition) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeOutOfRangePartition", err)
	}
}

// TestSQSEncodeRejectsNonzeroPartitionOnPerQueueHTFIFO pins gate 3
// (codex P2 v914 v4). For a partitioned queue with
// FifoThroughputLimit == "perQueue", the live partitionFor
// (adapter/sqs_partitioning.go:71-72) forces every group to
// partition 0; ReceiveMessage only scans the partition-0 lane.
// Accepting *Partition != 0 would restore messages onto |p|1|... or
// |p|N|... lanes the live receive never visits.
func TestSQSEncodeRejectsNonzeroPartitionOnPerQueueHTFIFO(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "p.fifo",
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2,"fifo_throughput_limit":"perQueue"}`),
		[][]byte{
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000,` +
				`"message_group_id":"g1","partition":1}`),
		})
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodePartitionRoutingMismatch) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodePartitionRoutingMismatch", err)
	}
}

// TestSQSEncodeRejectsNonZeroPartitionOnClassicQueue pins gate 4:
// classic queue (PartitionCount <= 1) with a message whose
// Partition is non-nil and non-zero fails closed with the existing
// ErrSQSEncodeInvalidMessage sentinel. Classic keyspace has no
// partition concept; the dump is internally inconsistent.
func TestSQSEncodeRejectsNonZeroPartitionOnClassicQueue(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "classic",
		[]byte(`{"format_version":1,"name":"classic",`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000,"partition":2}`),
		})
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodeInvalidMessage) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidMessage", err)
	}
}

// TestSQSEncodeGateUsesRawPartitionCount is the regression for codex
// P2 v914 v7. The four validation gates MUST use raw
// meta.PartitionCount > 1 as the partitioned-queue predicate, never
// effectivePartitionCount. A perQueue queue with PartitionCount=2
// collapses effectivePartitionCount to 1; if the missing-partition
// gate used the effective count, a message with Partition==nil
// would slip past validation, then addMessage's `partition != nil`
// dispatch would emit the classic-shape data key against a
// partitioned-keyspace queue — invisible on first read.
//
// This test would silently pass if the gate predicate were changed
// to effectivePartitionCount(&meta) > 1.
func TestSQSEncodeGateUsesRawPartitionCount(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "p.fifo",
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2,"fifo_throughput_limit":"perQueue"}`),
		[][]byte{
			// partition_count=2 + perQueue: effectivePartitionCount==1.
			// No "partition" field. Raw-PartitionCount gate must fire
			// before the perQueue routing gate has a chance to inspect
			// a nil Partition.
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000,` +
				`"message_group_id":"g1"}`),
		})
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodeMissingPartition) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeMissingPartition (codex P2 v914 v7 — gate must use raw PartitionCount)", err)
	}
}

// TestSQSEncodePartitionedDedupKeepsLatestOnExpiredCollision pins
// codex P2 #929. Two FIFO partitioned messages with the SAME
// (group, dedupID) tuple but sent more than the dedup window
// (5 minutes) apart end up with the same dedup-row key. The live
// keyspace only holds one row at a time (the later send overwrites
// the prior expired one), but the dump captures BOTH message data
// records. Naive per-message dedup emission would collide on
// snapshotBuilder.Add and abort the restore. pickDedupWinners must
// keep only the LATEST send's dedup row.
//
// "g1" hashes to partition 1 (FNV-1a-mod-2). Both messages share
// group_id="g1" + dedup_id="d" but differ in send_timestamp by 10
// minutes (well beyond sqsFifoDedupWindowMillis = 5 min).
func TestSQSEncodePartitionedDedupKeepsLatestOnExpiredCollision(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "p.fifo",
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2,"fifo_throughput_limit":"perMessageGroupId"}`),
		[][]byte{
			// Older send (send_ts = 1_000_000).
			[]byte(`{"message_id":"m-old","body":"first","send_timestamp_millis":1000000,` +
				`"available_at_millis":1000000,"message_group_id":"g1",` +
				`"message_deduplication_id":"d","sequence_number":1,"partition":1}`),
			// Newer send 10 min later (send_ts = 1_600_000); dedup
			// window already expired for the older send.
			[]byte(`{"message_id":"m-new","body":"second","send_timestamp_millis":1600000,` +
				`"available_at_millis":1600000,"message_group_id":"g1",` +
				`"message_deduplication_id":"d","sequence_number":2,"partition":1}`),
		})
	fsm := encodeSQSTree(t, in)
	entries, _, err := DecodeLiveEntries(bytes.NewReader(fsm))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	// Exactly ONE dedup row, and it must encode m-new (the latest).
	var dedupCount int
	var dedupVal []byte
	for _, e := range entries {
		if bytes.HasPrefix(e.UserKey, []byte(SQSPartitionedMsgDedupPrefix)) {
			dedupCount++
			dedupVal = e.UserValue
		}
	}
	if dedupCount != 1 {
		t.Fatalf("dedup-row count = %d, want 1 (only the latest send keeps the row)", dedupCount)
	}
	if !bytes.Contains(dedupVal, []byte(`"message_id":"m-new"`)) {
		t.Fatalf("dedup row contents = %s, want winner = m-new", dedupVal)
	}
	// Both DATA records still emitted.
	dataCount := 0
	for _, e := range entries {
		if bytes.HasPrefix(e.UserKey, []byte(SQSPartitionedMsgDataPrefix)) {
			dataCount++
		}
	}
	if dataCount != 2 {
		t.Fatalf("data-record count = %d, want 2 (both messages still emitted)", dataCount)
	}
}

// TestSQSEncodeClassicDedupKeepsLatestOnExpiredCollision pins the
// same fix for classic FIFO queues. Codex P2 #929 framed the issue
// around partitioned queues, but the same key-collision class
// exists for classic queues (where the dedup key is just
// (queue, dedupID) — no partition / group disambiguation).
func TestSQSEncodeClassicDedupKeepsLatestOnExpiredCollision(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "classic",
		[]byte(`{"format_version":1,"name":"classic","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			[]byte(`{"message_id":"m-old","body":"first","send_timestamp_millis":1000000,` +
				`"available_at_millis":1000000,"message_group_id":"g",` +
				`"message_deduplication_id":"d","sequence_number":1}`),
			[]byte(`{"message_id":"m-new","body":"second","send_timestamp_millis":1600000,` +
				`"available_at_millis":1600000,"message_group_id":"g",` +
				`"message_deduplication_id":"d","sequence_number":2}`),
		})
	fsm := encodeSQSTree(t, in)
	entries, _, err := DecodeLiveEntries(bytes.NewReader(fsm))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	var dedupCount int
	var dedupVal []byte
	for _, e := range entries {
		if bytes.HasPrefix(e.UserKey, []byte(SQSMsgDedupPrefix)) {
			dedupCount++
			dedupVal = e.UserValue
		}
	}
	if dedupCount != 1 {
		t.Fatalf("classic dedup-row count = %d, want 1", dedupCount)
	}
	if !bytes.Contains(dedupVal, []byte(`"message_id":"m-new"`)) {
		t.Fatalf("classic dedup row contents = %s, want winner = m-new", dedupVal)
	}
}

// TestSQSEncodeRejectsNonPowerOfTwoPartitionCount pins coderabbit
// Major #929. partitionForGroup masks with (PartitionCount - 1)
// which assumes a power of two. A malformed dump like
// partition_count=3 would mask via (h & 0b10) and route messages
// inconsistently. readQueueMeta must fail closed.
func TestSQSEncodeRejectsNonPowerOfTwoPartitionCount(t *testing.T) {
	t.Parallel()
	for _, n := range []uint32{3, 5, 6, 7, 9, 10} {
		t.Run(fmt.Sprintf("count=%d", n), func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			writeSQSQueue(t, in, "bad.fifo",
				[]byte(fmt.Sprintf(`{"format_version":1,"name":"bad.fifo","fifo":true,`+
					`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
					`"delay_seconds":0,"partition_count":%d,"fifo_throughput_limit":"perMessageGroupId"}`, n)),
				nil)
			b := newSnapshotBuilder(sqsEncTS)
			err := NewSQSRecordEncoder(in).Encode(b)
			if !errors.Is(err, ErrSQSEncodeInvalidQueue) {
				t.Fatalf("partition_count=%d: err = %v, want ErrSQSEncodeInvalidQueue", n, err)
			}
		})
	}
}

// TestSQSEncodeAcceptsPowerOfTwoPartitionCount confirms the
// power-of-two guard does not reject legitimate values.
func TestSQSEncodeAcceptsPowerOfTwoPartitionCount(t *testing.T) {
	t.Parallel()
	for _, n := range []uint32{2, 4, 8, 16, 32} {
		t.Run(fmt.Sprintf("count=%d", n), func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			writeSQSQueue(t, in, "ok.fifo",
				[]byte(fmt.Sprintf(`{"format_version":1,"name":"ok.fifo","fifo":true,`+
					`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
					`"delay_seconds":0,"partition_count":%d,"fifo_throughput_limit":"perQueue"}`, n)),
				nil)
			b := newSnapshotBuilder(sqsEncTS)
			if err := NewSQSRecordEncoder(in).Encode(b); err != nil {
				t.Fatalf("partition_count=%d: unexpected err = %v", n, err)
			}
		})
	}
}

func TestSQSEncodeRejectsTooLargePartitionCount(t *testing.T) {
	t.Parallel()
	for _, n := range []uint32{64, 128} {
		t.Run(fmt.Sprintf("count=%d", n), func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			writeSQSQueue(t, in, "too-large.fifo",
				[]byte(fmt.Sprintf(`{"format_version":1,"name":"too-large.fifo","fifo":true,`+
					`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
					`"delay_seconds":0,"partition_count":%d,"fifo_throughput_limit":"perMessageGroupId"}`, n)),
				nil)
			b := newSnapshotBuilder(sqsEncTS)
			err := NewSQSRecordEncoder(in).Encode(b)
			if !errors.Is(err, ErrSQSEncodeInvalidQueue) {
				t.Fatalf("partition_count=%d: err = %v, want ErrSQSEncodeInvalidQueue", n, err)
			}
		})
	}
}

func TestSQSEncodeRejectsPartitionedStandardQueue(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "standard",
		[]byte(`{"format_version":1,"name":"standard","fifo":false,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2}`),
		nil)
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodeInvalidQueue) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidQueue", err)
	}
}

func TestSQSEncodeRejectsQueueScopedDedupOnPartitionedFIFO(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "queue-scope.fifo",
		[]byte(`{"format_version":1,"name":"queue-scope.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2,"deduplication_scope":"queue"}`),
		nil)
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodeInvalidQueue) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidQueue", err)
	}
}

func TestSQSEncodeRejectsStandardQueueWithFIFOOnlyAttrs(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		body string
	}{
		{
			name: "throughput limit",
			body: `"fifo_throughput_limit":"perQueue"`,
		},
		{
			name: "dedup scope",
			body: `"deduplication_scope":"messageGroup"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			writeSQSQueue(t, in, "standard",
				[]byte(`{"format_version":1,"name":"standard","fifo":false,`+
					`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
					`"delay_seconds":0,`+tc.body+`}`),
				nil)
			b := newSnapshotBuilder(sqsEncTS)
			err := NewSQSRecordEncoder(in).Encode(b)
			if !errors.Is(err, ErrSQSEncodeInvalidQueue) {
				t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidQueue", err)
			}
		})
	}
}

func TestSQSEncodeRejectsPerMessageGroupLimitWithoutPartitions(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "single.fifo",
		[]byte(`{"format_version":1,"name":"single.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":1,"fifo_throughput_limit":"perMessageGroupId"}`),
		nil)
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodeInvalidQueue) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidQueue", err)
	}
}

func TestSQSEncodeRejectsUnknownHTFIFOAttributeValues(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		body string
	}{
		{
			name: "unknown throughput",
			body: `"fifo_throughput_limit":"perShard"`,
		},
		{
			name: "unknown dedup scope",
			body: `"deduplication_scope":"queueGroup"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			in := t.TempDir()
			writeSQSQueue(t, in, "bad.fifo",
				[]byte(`{"format_version":1,"name":"bad.fifo","fifo":true,`+
					`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
					`"delay_seconds":0,"partition_count":2,`+tc.body+`}`),
				nil)
			b := newSnapshotBuilder(sqsEncTS)
			err := NewSQSRecordEncoder(in).Encode(b)
			if !errors.Is(err, ErrSQSEncodeInvalidQueue) {
				t.Fatalf("Encode err = %v, want ErrSQSEncodeInvalidQueue", err)
			}
		})
	}
}

// TestSQSEncodeClassicDedupKeepsLatestAcrossGroups pins codex P2
// #929 (round 2): for classic FIFO queues the live dedup key is
// (queue, generation, dedupID) — NO group / partition. Two retained
// messages in DIFFERENT MessageGroupIds but the SAME dedupID
// (after the 5-minute window expired) must collapse to one winner;
// otherwise both groups emit the same classic dedup key and
// snapshotBuilder.Add fails.
//
// Earlier pickDedupWinners keyed on (partition, group, dedupID) for
// both shapes; this test would have produced 2 dedup rows and
// aborted the restore. The classic-collapse keying produces 1 row
// for the latest send and 2 data records.
func TestSQSEncodeClassicDedupKeepsLatestAcrossGroups(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "classic",
		[]byte(`{"format_version":1,"name":"classic","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			[]byte(`{"message_id":"m-grpA","body":"a","send_timestamp_millis":1000000,` +
				`"available_at_millis":1000000,"message_group_id":"groupA",` +
				`"message_deduplication_id":"d","sequence_number":1}`),
			// Different group, same dedup, 10 min later.
			[]byte(`{"message_id":"m-grpB","body":"b","send_timestamp_millis":1600000,` +
				`"available_at_millis":1600000,"message_group_id":"groupB",` +
				`"message_deduplication_id":"d","sequence_number":2}`),
		})
	fsm := encodeSQSTree(t, in)
	entries, _, err := DecodeLiveEntries(bytes.NewReader(fsm))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	var dedupCount int
	var dedupVal []byte
	for _, e := range entries {
		if bytes.HasPrefix(e.UserKey, []byte(SQSMsgDedupPrefix)) {
			dedupCount++
			dedupVal = e.UserValue
		}
	}
	if dedupCount != 1 {
		t.Fatalf("classic cross-group dedup-row count = %d, want 1", dedupCount)
	}
	if !bytes.Contains(dedupVal, []byte(`"message_id":"m-grpB"`)) {
		t.Fatalf("classic cross-group dedup row = %s, want winner = m-grpB", dedupVal)
	}
	// Both DATA records emitted (data key includes message_id).
	dataCount := 0
	for _, e := range entries {
		if bytes.HasPrefix(e.UserKey, []byte(SQSMsgDataPrefix)) &&
			!bytes.HasPrefix(e.UserKey, []byte(SQSPartitionedMsgDataPrefix)) {
			dataCount++
		}
	}
	if dataCount != 2 {
		t.Fatalf("classic cross-group data-record count = %d, want 2", dataCount)
	}
}

// TestSQSEncodeRejectsHashMismatchOnPerMessageGroupId pins gate 5
// (codex P2 #929). For a perMessageGroupId HT-FIFO queue, the
// partition value MUST match partitionFor(MessageGroupID). An
// in-range but wrong partition would split a FIFO group across two
// partition-scoped group-lock keyspaces and break FIFO order on
// first read.
//
// "g0" hashes (FNV-1a-mod-2) to partition 0; pairing it with
// partition=1 violates the invariant.
func TestSQSEncodeRejectsHashMismatchOnPerMessageGroupId(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "p.fifo",
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2,"fifo_throughput_limit":"perMessageGroupId"}`),
		[][]byte{
			// group_id="g0" hashes to partition 0; partition=1 is
			// in-range but inconsistent.
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000,` +
				`"message_group_id":"g0","partition":1}`),
		})
	b := newSnapshotBuilder(sqsEncTS)
	err := NewSQSRecordEncoder(in).Encode(b)
	if !errors.Is(err, ErrSQSEncodePartitionHashMismatch) {
		t.Fatalf("Encode err = %v, want ErrSQSEncodePartitionHashMismatch", err)
	}
}

// TestSQSEncodePartitionForGroup_LiveAdapterParity verifies the
// internal partitionForGroup mirror matches adapter/sqs_partitioning.go
// for representative inputs. We cannot import adapter (M3b-3 ban),
// so this is a parity-by-spec check against the FNV-1a constants.
func TestSQSEncodePartitionForGroup_LiveAdapterParity(t *testing.T) {
	t.Parallel()
	cases := []struct {
		meta  *sqsQueueMetaPublic
		group string
		want  uint32
	}{
		// Classic queue: always 0.
		{&sqsQueueMetaPublic{PartitionCount: 1}, "anything", 0},
		// perQueue: always 0 regardless of PartitionCount.
		{&sqsQueueMetaPublic{PartitionCount: 4, FifoThroughputLimit: sqsFifoThroughputPerQueue}, "g0", 0},
		// perMessageGroupId, empty group: 0.
		{&sqsQueueMetaPublic{PartitionCount: 4, FifoThroughputLimit: "perMessageGroupId"}, "", 0},
		// perMessageGroupId, FNV-1a-mod-2 of "g0" = 0, "g1" = 1.
		{&sqsQueueMetaPublic{PartitionCount: 2, FifoThroughputLimit: "perMessageGroupId"}, "g0", 0},
		{&sqsQueueMetaPublic{PartitionCount: 2, FifoThroughputLimit: "perMessageGroupId"}, "g1", 1},
	}
	for _, c := range cases {
		got := partitionForGroup(c.meta, c.group)
		if got != c.want {
			t.Errorf("partitionForGroup(meta{PC=%d,limit=%q}, %q) = %d, want %d",
				c.meta.PartitionCount, c.meta.FifoThroughputLimit, c.group, got, c.want)
		}
	}
}

// TestSQSEncodeClassicQueueWithExplicitPartitionZeroUsesClassicKeys is
// the regression for codex P1 / gemini critical (PR #929). A classic
// queue dump with an explicit `"partition": 0` is allowed past gate 4
// (which only rejects *Partition != 0) — but the dispatch MUST still
// pick classic-shape keys because the live reader for a classic queue
// only scans the classic keyspace.
//
// Before the (isPartitioned bool, partition uint32) signature, the
// `partition != nil` branch incorrectly chose partitioned keys here,
// making restored messages invisible. This test would fail (silently
// emit |p|0|... keys) under that buggy dispatch.
func TestSQSEncodeClassicQueueWithExplicitPartitionZeroUsesClassicKeys(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "classic",
		[]byte(`{"format_version":1,"name":"classic",`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			// PartitionCount<=1 (classic), but message has explicit
			// "partition": 0. Gate 4 must NOT fire (*Partition == 0),
			// and addMessage must use classic keys.
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000,"partition":0}`),
		})
	fsm := encodeSQSTree(t, in)
	entries, _, err := DecodeLiveEntries(bytes.NewReader(fsm))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	// No partitioned-shape data/vis/byage/dedup keys allowed.
	for _, e := range entries {
		for _, p := range []string{
			SQSPartitionedMsgDataPrefix,
			SQSPartitionedMsgVisPrefix,
			SQSPartitionedMsgByAgePrefix,
			SQSPartitionedMsgDedupPrefix,
		} {
			if bytes.HasPrefix(e.UserKey, []byte(p)) {
				t.Errorf("partitioned-shape key on classic queue with explicit partition=0: %q (prefix %q)", e.UserKey, p)
			}
		}
	}
	// At least one classic data key must exist.
	found := false
	for _, e := range entries {
		if bytes.HasPrefix(e.UserKey, []byte(SQSMsgDataPrefix)) &&
			!bytes.HasPrefix(e.UserKey, []byte(SQSPartitionedMsgDataPrefix)) {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("no classic data record found")
	}
}

// TestSQSEncodeLegacyDumpsWithoutPartitionStillRoundTrip pins that a
// pre-M5-3 messages.jsonl (no "partition" field on any line) still
// round-trips through the M5-3 encoder unchanged when the queue is
// classic (PartitionCount <= 1). The classic constructor branch is
// selected by addMessage when Partition == nil.
func TestSQSEncodeLegacyDumpsWithoutPartitionStillRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "legacy",
		[]byte(`{"format_version":1,"name":"legacy",`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,"delay_seconds":0}`),
		[][]byte{
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000}`),
			[]byte(`{"message_id":"m2","body":"world",` +
				`"send_timestamp_millis":2000,"available_at_millis":2000}`),
		})
	_, msgs := decodeSQSAndRead(t, encodeSQSTree(t, in), "legacy")
	if len(msgs) != 2 {
		t.Fatalf("round-tripped %d messages, want 2", len(msgs))
	}
	for i := range msgs {
		if msgs[i].Partition != nil {
			t.Fatalf("msg[%d].Partition = %v, want nil for classic round-trip", i, *msgs[i].Partition)
		}
	}
}

// TestSQSEncodePartitionedQueueRoundTrip verifies the partitioned
// data + side records are emitted with the |p| prefix and the
// partition is preserved end-to-end through encode → decode.
func TestSQSEncodePartitionedQueueRoundTrip(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	writeSQSQueue(t, in, "p.fifo",
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2,"fifo_throughput_limit":"perMessageGroupId"}`),
		[][]byte{
			[]byte(`{"message_id":"m0a","body":"p0-a","send_timestamp_millis":1000,` +
				`"available_at_millis":1000,"message_group_id":"g0","message_deduplication_id":"d0a",` +
				`"sequence_number":1,"partition":0}`),
			[]byte(`{"message_id":"m1a","body":"p1-a","send_timestamp_millis":1100,` +
				`"available_at_millis":1100,"message_group_id":"g1","message_deduplication_id":"d1a",` +
				`"sequence_number":2,"partition":1}`),
			[]byte(`{"message_id":"m1b","body":"p1-b","send_timestamp_millis":1200,` +
				`"available_at_millis":1200,"message_group_id":"g1","message_deduplication_id":"d1b",` +
				`"sequence_number":3,"partition":1}`),
		})

	fsm := encodeSQSTree(t, in)
	entries, _, err := DecodeLiveEntries(bytes.NewReader(fsm))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	// All data/vis/byage/dedup keys for this queue must be the
	// partitioned shape (|p|).
	wantPrefixes := []string{
		SQSPartitionedMsgDataPrefix,
		SQSPartitionedMsgVisPrefix,
		SQSPartitionedMsgByAgePrefix,
		SQSPartitionedMsgDedupPrefix,
	}
	for _, prefix := range wantPrefixes {
		found := 0
		for _, e := range entries {
			if bytes.HasPrefix(e.UserKey, []byte(prefix)) {
				found++
			}
		}
		if found == 0 {
			t.Errorf("no entries with prefix %q (3-message partitioned queue should emit ≥1)", prefix)
		}
	}
	// No classic-shape data/vis/byage/dedup keys for this queue.
	for _, classic := range []string{SQSMsgDataPrefix, SQSMsgVisPrefix, SQSMsgByAgePrefix, SQSMsgDedupPrefix} {
		for _, e := range entries {
			if !bytes.HasPrefix(e.UserKey, []byte(classic)) {
				continue
			}
			// Skip if it's actually a partitioned key (the partitioned
			// prefix starts with the classic prefix + "p|").
			classicAndDiscriminator := classic + sqsPartitionedDiscriminator
			if bytes.HasPrefix(e.UserKey, []byte(classicAndDiscriminator)) {
				continue
			}
			t.Errorf("classic-shape key on a partitioned queue: %q", e.UserKey)
		}
	}
}

// TestSQSEncodePartitionedDedupBuildsGroupSegment pins the
// partitioned dedup row's <group-seg>+|+<dedup-seg> shape: the
// MessageGroupID is base64-encoded as part of the key and a
// terminator '|' separates it from the dedupID segment (CodeRabbit
// major PR #732 round 6).
func TestSQSEncodePartitionedDedupBuildsGroupSegment(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	const (
		queue   = "p.fifo"
		groupID = "groupA"
		dedupID = "dedup-1"
	)
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2,"fifo_throughput_limit":"perMessageGroupId"}`),
		[][]byte{
			[]byte(`{"message_id":"m1","body":"hello",` +
				`"send_timestamp_millis":1000,"available_at_millis":1000,` +
				`"message_group_id":"` + groupID + `",` +
				`"message_deduplication_id":"` + dedupID + `",` +
				`"sequence_number":1,"partition":1}`),
		})

	fsm := encodeSQSTree(t, in)
	entries, _, err := DecodeLiveEntries(bytes.NewReader(fsm))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	// Find the dedup entry and compare to the manually-built key.
	want := sqsPartitionedMsgDedupKeyBytes(queue, 1, groupID, dedupID)
	var got []byte
	for _, e := range entries {
		if bytes.HasPrefix(e.UserKey, []byte(SQSPartitionedMsgDedupPrefix)) {
			got = e.UserKey
			break
		}
	}
	if got == nil {
		t.Fatalf("no partitioned dedup entry emitted")
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("partitioned dedup key mismatch\ngot:  %x\nwant: %x", got, want)
	}
}

// TestSQSEncodePartitionedSideRecordsByteCrossCheck verifies the
// three partitioned side-record bytes emitted by addSideRecords
// equal what sqsPartitionedMsg{Vis,ByAge,Dedup}KeyBytes produce
// directly. This is the in-package cross-check the M5-2 precedent
// at TestSQSEncodeSideRecordsCrossCheckClassic uses for the classic
// path (the adapter's unexported sqsPartitionedMsg* constructors
// cannot be invoked from this package).
func TestSQSEncodePartitionedSideRecordsByteCrossCheck(t *testing.T) {
	t.Parallel()
	in := t.TempDir()
	// groupID "g1" hashes (FNV-1a-mod-2) to partition 1; pair it with
	// partition=1 so gate 5 (partition-hash consistency for
	// perMessageGroupId) is satisfied.
	const (
		queue   = "p.fifo"
		groupID = "g1"
		dedupID = "d0"
		msgID   = "mid-001"
		sendMs  = int64(2000)
		availMs = int64(2000)
		partN   = uint32(1)
	)
	writeSQSQueue(t, in, queue,
		[]byte(`{"format_version":1,"name":"p.fifo","fifo":true,`+
			`"visibility_timeout_seconds":30,"message_retention_seconds":345600,`+
			`"delay_seconds":0,"partition_count":2,"fifo_throughput_limit":"perMessageGroupId"}`),
		[][]byte{
			[]byte(`{"message_id":"mid-001","body":"x","send_timestamp_millis":2000,` +
				`"available_at_millis":2000,"message_group_id":"g1",` +
				`"message_deduplication_id":"d0","sequence_number":1,"partition":1}`),
		})
	entries, _, err := DecodeLiveEntries(bytes.NewReader(encodeSQSTree(t, in)))
	if err != nil {
		t.Fatalf("DecodeLiveEntries: %v", err)
	}
	wantVis := sqsPartitionedMsgVisKeyBytes(queue, partN, sqsRestoreGeneration, availMs, msgID)
	wantByAge := sqsPartitionedMsgByAgeKeyBytes(queue, partN, sqsRestoreGeneration, sendMs, msgID)
	wantDedup := sqsPartitionedMsgDedupKeyBytes(queue, partN, groupID, dedupID)
	var gotVis, gotByAge, gotDedup []byte
	for _, e := range entries {
		switch {
		case bytes.HasPrefix(e.UserKey, []byte(SQSPartitionedMsgVisPrefix)):
			gotVis = e.UserKey
		case bytes.HasPrefix(e.UserKey, []byte(SQSPartitionedMsgByAgePrefix)):
			gotByAge = e.UserKey
		case bytes.HasPrefix(e.UserKey, []byte(SQSPartitionedMsgDedupPrefix)):
			gotDedup = e.UserKey
		}
	}
	if !bytes.Equal(gotVis, wantVis) {
		t.Errorf("partitioned vis mismatch\ngot:  %x\nwant: %x", gotVis, wantVis)
	}
	if !bytes.Equal(gotByAge, wantByAge) {
		t.Errorf("partitioned byage mismatch\ngot:  %x\nwant: %x", gotByAge, wantByAge)
	}
	if !bytes.Equal(gotDedup, wantDedup) {
		t.Errorf("partitioned dedup mismatch\ngot:  %x\nwant: %x", gotDedup, wantDedup)
	}
}

// TestSQSEncodePartitionedSortStableAcrossPartitions pins the
// 4-field sort: messages within the same partition with identical
// send_timestamp_millis are ordered by sequence_number, and
// messages from different partitions group together by partition
// first.
func TestSQSEncodePartitionedSortStableAcrossPartitions(t *testing.T) {
	t.Parallel()
	// Two messages on partition 1 with identical send_ts but
	// different sequence_number, plus one on partition 0 with
	// later send_ts.
	records := []sqsMessageRecord{
		{MessageID: "m1-b", SendTimestampMillis: 1000, SequenceNumber: 2, Partition: u32ptr(1)},
		{MessageID: "m0", SendTimestampMillis: 2000, SequenceNumber: 1, Partition: u32ptr(0)},
		{MessageID: "m1-a", SendTimestampMillis: 1000, SequenceNumber: 1, Partition: u32ptr(1)},
	}
	sortMessagesForPartitionedEmit(records)
	wantOrder := []string{"m0", "m1-a", "m1-b"}
	for i, r := range records {
		if r.MessageID != wantOrder[i] {
			t.Errorf("records[%d].MessageID = %q, want %q", i, r.MessageID, wantOrder[i])
		}
	}
}

func u32ptr(v uint32) *uint32 { return &v }

// TestSQSEffectivePartitionCount pins the local mirror of
// adapter/sqs_keys_dispatch.go:effectivePartitionCount: nil meta,
// PartitionCount<=1, and perQueue all collapse to 1; otherwise the
// declared PartitionCount is returned.
func TestSQSEffectivePartitionCount(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		meta *sqsQueueMetaPublic
		want uint32
	}{
		{"nil meta", nil, 1},
		{"PartitionCount=0", &sqsQueueMetaPublic{PartitionCount: 0}, 1},
		{"PartitionCount=1", &sqsQueueMetaPublic{PartitionCount: 1}, 1},
		{"PartitionCount=4 perQueue", &sqsQueueMetaPublic{PartitionCount: 4, FifoThroughputLimit: sqsFifoThroughputPerQueue}, 1},
		{"PartitionCount=4 perGroup", &sqsQueueMetaPublic{PartitionCount: 4, FifoThroughputLimit: "perMessageGroupId"}, 4},
		{"PartitionCount=4 unset throughput", &sqsQueueMetaPublic{PartitionCount: 4}, 4},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := effectivePartitionCount(c.meta); got != c.want {
				t.Fatalf("effectivePartitionCount(%+v) = %d, want %d", c.meta, got, c.want)
			}
		})
	}
}
