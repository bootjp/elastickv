package backup

import (
	"encoding/binary"
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
// prefix + base64url(queue) + BE-u64(gen) + base64url(dedupID).
func sqsMsgDedupKeyBytes(queueName string, gen uint64, dedupID string) []byte {
	out := make([]byte, 0, len(SQSMsgDedupPrefix)+sqsSideKeyAllocBytes)
	out = append(out, SQSMsgDedupPrefix...)
	out = append(out, encodeSQSSegment(queueName)...)
	out = binary.BigEndian.AppendUint64(out, gen)
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

// addSideRecords emits the vis + byage + (conditional) dedup rows that the
// live adapter would have written alongside the !sqs|msg|data| record M5-1
// already stages. No !sqs|msg|group| rows are emitted at any time — see
// docs/design/2026_05_30_proposed_sqs_side_record_derivation.md "Families"
// table for why (loadFifoGroupLock treats key presence alone as "lock held";
// any value would permanently block every group post-restore).
//
// Emission rules:
//   - vis:   always emitted, visibleAt = rec.AvailableAtMillis (= "visible now").
//   - byage: always emitted, sendTs = rec.SendTimestampMillis (required by the
//     reaper to honor MessageRetentionPeriod after restore).
//   - dedup: FIFO + non-empty MessageDedupID only; ExpiresAtMillis =
//     SendTimestampMs + sqsFifoDedupWindowMillis.
func (e *SQSRecordEncoder) addSideRecords(b *snapshotBuilder, queueName string, meta *sqsQueueMetaPublic, rec *sqsMessageRecord) error {
	if rec.MessageID == "" {
		return errors.Wrap(ErrSQSEncodeInvalidMessage, "side records require non-empty message_id")
	}
	msgIDBytes := []byte(rec.MessageID)

	visKey := sqsMsgVisKeyBytes(queueName, sqsRestoreGeneration, rec.AvailableAtMillis, rec.MessageID)
	if err := b.Add(visKey, msgIDBytes, 0); err != nil {
		return err
	}

	byAgeKey := sqsMsgByAgeKeyBytes(queueName, sqsRestoreGeneration, rec.SendTimestampMillis, rec.MessageID)
	if err := b.Add(byAgeKey, msgIDBytes, 0); err != nil {
		return err
	}

	if !meta.FIFO || rec.MessageDedupID == "" {
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
	dedupKey := sqsMsgDedupKeyBytes(queueName, sqsRestoreGeneration, rec.MessageDedupID)
	return b.Add(dedupKey, val, 0)
}
