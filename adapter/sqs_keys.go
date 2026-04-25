package adapter

import (
	"encoding/base64"
	"strings"

	"github.com/cockroachdb/errors"
)

// SQS keyspace prefixes. Kept in sync with the naming in
// docs/design/2026_04_24_proposed_sqs_compatible_adapter.md.
const (
	// SqsQueueMetaPrefix prefixes queue-metadata records.
	SqsQueueMetaPrefix = "!sqs|queue|meta|"
	// SqsQueueGenPrefix prefixes the per-queue monotonic generation counter.
	// Bumped on DeleteQueue / PurgeQueue so keys from an older incarnation of
	// the same queue name cannot leak into a newly created queue.
	SqsQueueGenPrefix = "!sqs|queue|gen|"
	// SqsQueueSeqPrefix prefixes the per-queue FIFO sequence counter. Bumped
	// on every FIFO send and embedded in the message record so consumers can
	// reconstruct the producer's strict total order.
	SqsQueueSeqPrefix = "!sqs|queue|seq|"
	// SqsMsgDedupPrefix prefixes FIFO deduplication records. Each entry
	// stores the original message id and the dedup-window expiry; the
	// receive path is unaware of these — they only gate sends.
	SqsMsgDedupPrefix = "!sqs|msg|dedup|"
	// SqsMsgGroupPrefix prefixes the FIFO group-lock records. The lock is
	// held by at most one message per group, persists across visibility
	// expiries, and is only released on DeleteMessage / DLQ redrive /
	// retention expiry.
	SqsMsgGroupPrefix = "!sqs|msg|group|"
	// SqsMsgByAgePrefix prefixes the send-age index. Each entry is
	// keyed by (queue, gen, send_timestamp, message_id) so the reaper
	// can find every record whose retention deadline has elapsed with
	// one bounded scan, without having to load every message body.
	SqsMsgByAgePrefix = "!sqs|msg|byage|"
)

func sqsQueueMetaKey(queueName string) []byte {
	return []byte(SqsQueueMetaPrefix + encodeSQSSegment(queueName))
}

func sqsQueueGenKey(queueName string) []byte {
	return []byte(SqsQueueGenPrefix + encodeSQSSegment(queueName))
}

func sqsQueueSeqKey(queueName string) []byte {
	return []byte(SqsQueueSeqPrefix + encodeSQSSegment(queueName))
}

func sqsMsgDedupKey(queueName string, gen uint64, dedupID string) []byte {
	buf := make([]byte, 0, len(SqsMsgDedupPrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsMsgDedupPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	buf = append(buf, encodeSQSSegment(dedupID)...)
	return buf
}

func sqsMsgGroupKey(queueName string, gen uint64, groupID string) []byte {
	buf := make([]byte, 0, len(SqsMsgGroupPrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsMsgGroupPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	buf = append(buf, encodeSQSSegment(groupID)...)
	return buf
}

func sqsMsgByAgeKey(queueName string, gen uint64, sendTimestampMs int64, messageID string) []byte {
	buf := make([]byte, 0, len(SqsMsgByAgePrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsMsgByAgePrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	buf = appendU64(buf, uint64MaxZero(sendTimestampMs))
	buf = append(buf, encodeSQSSegment(messageID)...)
	return buf
}

func sqsMsgByAgePrefixForQueue(queueName string, gen uint64) []byte {
	buf := make([]byte, 0, len(SqsMsgByAgePrefix)+sqsKeyCapSmall)
	buf = append(buf, SqsMsgByAgePrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	return buf
}

// encodeSQSSegment emits a printable, byte-ordered-unique representation of a
// queue name. Base64 raw URL encoding matches the encoding the DynamoDB
// adapter uses for table segments (see encodeDynamoSegment) so that operators
// reading raw keys from Pebble see the same shape across both adapters.
func encodeSQSSegment(v string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(v))
}

func decodeSQSSegment(v string) (string, error) {
	b, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return string(b), nil
}

// queueNameFromMetaKey pulls the queue name out of a !sqs|queue|meta|<seg>
// key. The second return reports success so callers can skip keys that were
// not written by this adapter.
func queueNameFromMetaKey(key []byte) (string, bool) {
	enc, ok := strings.CutPrefix(string(key), SqsQueueMetaPrefix)
	if !ok || enc == "" {
		return "", false
	}
	name, err := decodeSQSSegment(enc)
	if err != nil {
		return "", false
	}
	return name, true
}
