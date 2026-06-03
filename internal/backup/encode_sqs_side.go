package backup

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"

	"github.com/cockroachdb/errors"
)

// sqsFifoDedupWindowMillis mirrors adapter/sqs_fifo.go (5 minutes). Restored
// dedup rows that fall outside this window from the backup's send timestamp
// will be expired the next time the live FIFO send path inspects them.
const sqsFifoDedupWindowMillis int64 = 5 * 60 * 1000

// sqsSideKeyAllocBytes mirrors the live adapter's sqsKeyCapLarge tuning
// (adapter/sqs_messages.go:68): a 64-byte tail after the prefix is large
// enough to hold the BE-u64 generation + visibleAt + base64url(messageID)
// for typical queue / message IDs without forcing a re-allocation.
const sqsSideKeyAllocBytes = 64

// sqsFifoDedupRecord mirrors the live struct at adapter/sqs_fifo.go:25.
// Duplicated here (rather than imported) so the encoder package can run
// without a circular dependency on adapter, matching the pattern M3b-3
// used for DynamoDB GSI helpers.
type sqsFifoDedupRecord struct {
	MessageID        string `json:"message_id"`
	SendTimestampMs  int64  `json:"send_timestamp_ms"`
	ExpiresAtMillis  int64  `json:"expires_at_millis"`
	OriginalSequence uint64 `json:"original_sequence,omitempty"`
}

// sqsMsgVisKeyBytes reproduces adapter/sqs_messages.go sqsMsgVisKey:
// prefix + base64url(queue) + BE-u64(gen) + BE-u64(visibleAt) +
// base64url(messageID). Negative visibleAt clamps to zero, matching the
// live uint64MaxZero helper.
func sqsMsgVisKeyBytes(queueName string, gen uint64, visibleAtMillis int64, messageID string) []byte {
	out := make([]byte, 0, len(SQSMsgVisPrefix)+sqsSideKeyAllocBytes)
	out = append(out, SQSMsgVisPrefix...)
	out = append(out, encodeSQSSegment(queueName)...)
	out = binary.BigEndian.AppendUint64(out, gen)
	out = binary.BigEndian.AppendUint64(out, sqsClampNonNegativeMillis(visibleAtMillis))
	return append(out, encodeSQSSegment(messageID)...)
}

// sqsMsgByAgeKeyBytes reproduces adapter/sqs_keys.go sqsMsgByAgeKey:
// prefix + base64url(queue) + BE-u64(gen) + BE-u64(sendTs) +
// base64url(messageID). Negative sendTs clamps to zero.
func sqsMsgByAgeKeyBytes(queueName string, gen uint64, sendTimestampMs int64, messageID string) []byte {
	out := make([]byte, 0, len(SQSMsgByAgePrefix)+sqsSideKeyAllocBytes)
	out = append(out, SQSMsgByAgePrefix...)
	out = append(out, encodeSQSSegment(queueName)...)
	out = binary.BigEndian.AppendUint64(out, gen)
	out = binary.BigEndian.AppendUint64(out, sqsClampNonNegativeMillis(sendTimestampMs))
	return append(out, encodeSQSSegment(messageID)...)
}

// sqsMsgDedupKeyBytes reproduces adapter/sqs_keys.go sqsMsgDedupKey:
// prefix + base64url(queue) + BE-u64(sqsRestoreGeneration) +
// base64url(dedupID). The live adapter signature accepts a variable gen,
// but every M5-2 call site uses sqsRestoreGeneration (a fresh restore
// has exactly one live generation, with no superseded counters to
// reference), so the parameter is hardcoded here to satisfy unparam.
func sqsMsgDedupKeyBytes(queueName, dedupID string) []byte {
	out := make([]byte, 0, len(SQSMsgDedupPrefix)+sqsSideKeyAllocBytes)
	out = append(out, SQSMsgDedupPrefix...)
	out = append(out, encodeSQSSegment(queueName)...)
	out = binary.BigEndian.AppendUint64(out, sqsRestoreGeneration)
	return append(out, encodeSQSSegment(dedupID)...)
}

// sqsPartitionedMsgVisKeyBytes reproduces
// adapter/sqs_keys.go:sqsPartitionedMsgVisKey: prefix +
// base64url(queue) + '|' + BE-u32(partition) + BE-u64(gen) +
// BE-u64(visibleAt) + base64url(messageID). Mirrored locally per
// M3b-3 (internal/backup cannot import adapter).
func sqsPartitionedMsgVisKeyBytes(queueName string, partition uint32, gen uint64, visibleAtMillis int64, messageID string) []byte {
	out := make([]byte, 0, len(SQSPartitionedMsgVisPrefix)+sqsSideKeyAllocBytes)
	out = append(out, SQSPartitionedMsgVisPrefix...)
	out = append(out, encodeSQSSegment(queueName)...)
	out = append(out, sqsPartitionedQueueTerminator)
	out = binary.BigEndian.AppendUint32(out, partition)
	out = binary.BigEndian.AppendUint64(out, gen)
	out = binary.BigEndian.AppendUint64(out, sqsClampNonNegativeMillis(visibleAtMillis))
	return append(out, encodeSQSSegment(messageID)...)
}

// sqsPartitionedMsgByAgeKeyBytes reproduces
// adapter/sqs_keys.go:sqsPartitionedMsgByAgeKey: prefix +
// base64url(queue) + '|' + BE-u32(partition) + BE-u64(gen) +
// BE-u64(sendTs) + base64url(messageID).
func sqsPartitionedMsgByAgeKeyBytes(queueName string, partition uint32, gen uint64, sendTimestampMs int64, messageID string) []byte {
	out := make([]byte, 0, len(SQSPartitionedMsgByAgePrefix)+sqsSideKeyAllocBytes)
	out = append(out, SQSPartitionedMsgByAgePrefix...)
	out = append(out, encodeSQSSegment(queueName)...)
	out = append(out, sqsPartitionedQueueTerminator)
	out = binary.BigEndian.AppendUint32(out, partition)
	out = binary.BigEndian.AppendUint64(out, gen)
	out = binary.BigEndian.AppendUint64(out, sqsClampNonNegativeMillis(sendTimestampMs))
	return append(out, encodeSQSSegment(messageID)...)
}

// sqsPartitionedMsgDedupKeyBytes reproduces
// adapter/sqs_keys.go:sqsPartitionedMsgDedupKey: prefix +
// base64url(queue) + '|' + BE-u32(partition) + BE-u64(gen) +
// base64url(groupID) + '|' + base64url(dedupID). Note the second
// '|' between the variable-length group and dedup segments — without
// it distinct (groupID, dedupID) pairs can FNV-collapse onto the
// same key (CodeRabbit major PR #732 round 6; design doc §line 19).
func sqsPartitionedMsgDedupKeyBytes(queueName string, partition uint32, gen uint64, groupID, dedupID string) []byte {
	out := make([]byte, 0, len(SQSPartitionedMsgDedupPrefix)+sqsSideKeyAllocBytes)
	out = append(out, SQSPartitionedMsgDedupPrefix...)
	out = append(out, encodeSQSSegment(queueName)...)
	out = append(out, sqsPartitionedQueueTerminator)
	out = binary.BigEndian.AppendUint32(out, partition)
	out = binary.BigEndian.AppendUint64(out, gen)
	out = append(out, encodeSQSSegment(groupID)...)
	out = append(out, sqsPartitionedQueueTerminator)
	return append(out, encodeSQSSegment(dedupID)...)
}

// sqsClampNonNegativeMillis mirrors adapter/sqs_messages.go uint64MaxZero:
// wall-clock millis should never be negative, but a negative int64 would
// silently overflow under a direct uint64() cast and produce a far-future
// key, so clamp to zero defensively.
func sqsClampNonNegativeMillis(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

// encodeFifoDedupRecordBytes mirrors adapter/sqs_fifo.go encodeFifoDedupRecord:
// straight json.Marshal of the four-field struct. Wrapped with WithStack so
// callers get a uniform stack-trace at the error site.
func encodeFifoDedupRecordBytes(r *sqsFifoDedupRecord) ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b, nil
}

// resolveDedupID mirrors adapter/sqs_fifo.go resolveFifoDedupID exactly: an
// explicit MessageDeduplicationId wins; otherwise on ContentBasedDeduplication
// queues the dedup-id is sha256(body) hex-encoded; otherwise empty (= no row).
//
// This is the critical CBD-correctness path: the live adapter writes only the
// USER-SUPPLIED MessageDeduplicationId into the stored sqsStoredMessage
// (sqs_messages.go:680), NOT the resolved one. So a CBD-FIFO message round-
// trips through the dump as message_deduplication_id="", and a naive
// non-empty-only gate would silently lose dedup protection on restore for
// every CBD queue. We have to redo the live derivation at restore time.
func resolveDedupID(rec *sqsMessageRecord, meta *sqsQueueMetaPublic) string {
	if rec.MessageDedupID != "" {
		return rec.MessageDedupID
	}
	if meta.ContentBasedDeduplication {
		sum := sha256.Sum256(rec.Body)
		return hex.EncodeToString(sum[:])
	}
	return ""
}

// addSideRecords emits the vis + byage + (conditional) dedup rows that the
// live adapter would have written alongside the !sqs|msg|data| record M5-1
// already stages. No !sqs|msg|group| rows are emitted at any time — see
// docs/design/2026_05_30_proposed_sqs_side_record_derivation.md "Families"
// table for why (loadFifoGroupLock treats key presence alone as "lock held";
// any value would permanently block every group post-restore).
//
// Emission rules:
//   - vis:   always emitted, visibleAt = rec.AvailableAtMillis. For delayed
//     messages captured before their delay expired this is in the future;
//     the live ReceiveMessage path honors this exactly as it would for a
//     fresh send (the message is invisible until the scheduled time).
//   - byage: always emitted, sendTs = rec.SendTimestampMillis (required by
//     the reaper to honor MessageRetentionPeriod after restore).
//   - dedup: FIFO + resolveDedupID(rec, meta) non-empty. ExpiresAtMillis =
//     SendTimestampMs + sqsFifoDedupWindowMillis. CBD queues get a SHA-256
//     derived dedup-id (matches adapter/sqs_fifo.go resolveFifoDedupID).
func (e *SQSRecordEncoder) addSideRecords(b *snapshotBuilder, queueName string, meta *sqsQueueMetaPublic, rec *sqsMessageRecord) error {
	msgIDBytes := []byte(rec.MessageID)

	visKey := sqsMsgVisKeyBytes(queueName, sqsRestoreGeneration, rec.AvailableAtMillis, rec.MessageID)
	if err := b.Add(visKey, msgIDBytes, 0); err != nil {
		return err
	}

	byAgeKey := sqsMsgByAgeKeyBytes(queueName, sqsRestoreGeneration, rec.SendTimestampMillis, rec.MessageID)
	if err := b.Add(byAgeKey, msgIDBytes, 0); err != nil {
		return err
	}

	if !meta.FIFO {
		return nil
	}
	dedupID := resolveDedupID(rec, meta)
	if dedupID == "" {
		return nil
	}
	dedupRec := &sqsFifoDedupRecord{
		MessageID:        rec.MessageID,
		SendTimestampMs:  rec.SendTimestampMillis,
		ExpiresAtMillis:  rec.SendTimestampMillis + sqsFifoDedupWindowMillis,
		OriginalSequence: rec.SequenceNumber,
	}
	val, err := encodeFifoDedupRecordBytes(dedupRec)
	if err != nil {
		return err
	}
	dedupKey := sqsMsgDedupKeyBytes(queueName, dedupID)
	return b.Add(dedupKey, val, 0)
}
