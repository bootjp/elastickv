package backup

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// TestSQSEncodePartitionedMsgDataKeyByteShape pins
// sqsPartitionedMsgDataKeyBytes against the wire format mandated by
// adapter/sqs_keys.go:sqsPartitionedMsgDataKey:
//   prefix + base64url(queue) + '|' + BE-u32(partition) + BE-u64(gen) +
//   base64url(messageID).
// internal/backup cannot import adapter to cross-check directly (M3b-3
// circular-dep ban; same constraint sqsFifoDedupWindowMillis lives
// under), so this is a manually-assembled byte-equality assertion.
func TestSQSEncodePartitionedMsgDataKeyByteShape(t *testing.T) {
	t.Parallel()
	const (
		queue   = "q1"
		msgID   = "msg-001"
		partN   = uint32(3)
		genCnt  = uint64(7)
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
		queue   = "qvis"
		msgID   = "mid"
		partN   = uint32(0)
		genCnt  = uint64(1)
		visMs   = int64(1_700_000_000_000)
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
		queue   = "qage"
		msgID   = "mid-age"
		partN   = uint32(15)
		genCnt  = uint64(1)
		sendMs  = int64(1_600_000_000_000)
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
// base64url(queue) + '|' + BE-u32(partition) + BE-u64(gen) +
// base64url(groupID) + '|' + base64url(dedupID).
func TestSQSEncodePartitionedMsgDedupKeyByteShape(t *testing.T) {
	t.Parallel()
	const (
		queue   = "qdedup"
		groupID = "group-A"
		dedupID = "dedup-001"
		partN   = uint32(2)
		genCnt  = uint64(1)
	)
	want := []byte(SQSPartitionedMsgDedupPrefix)
	want = append(want, encodeSQSSegment(queue)...)
	want = append(want, sqsPartitionedQueueTerminator)
	want = binary.BigEndian.AppendUint32(want, partN)
	want = binary.BigEndian.AppendUint64(want, genCnt)
	want = append(want, encodeSQSSegment(groupID)...)
	want = append(want, sqsPartitionedQueueTerminator)
	want = append(want, encodeSQSSegment(dedupID)...)

	got := sqsPartitionedMsgDedupKeyBytes(queue, partN, genCnt, groupID, dedupID)
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
