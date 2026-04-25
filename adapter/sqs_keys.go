package adapter

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
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
	// SqsQueueTombstonePrefix prefixes a "queue was deleted" marker.
	// DeleteQueue writes one (queue, gen) tombstone alongside the meta
	// delete, and the reaper enumerates these markers to clean up
	// orphan data / vis / byage / dedup / group keys whose meta row no
	// longer exists. The tombstone is itself deleted once the reaper
	// confirms no message-keyspace state remains for that (queue, gen).
	SqsQueueTombstonePrefix = "!sqs|queue|tombstone|"
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

func sqsQueueTombstoneKey(queueName string, gen uint64) []byte {
	buf := make([]byte, 0, len(SqsQueueTombstonePrefix)+sqsKeyCapSmall)
	buf = append(buf, SqsQueueTombstonePrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	return buf
}

// sqsGenerationSuffixLen is the byte length of the trailing big-endian
// uint64 generation segment in tombstone and byage keys.
const sqsGenerationSuffixLen = 8

// parseSqsQueueTombstoneKey reverses sqsQueueTombstoneKey. The
// generation is fixed at the last 8 bytes of the key, so the queue
// name segment is everything between the prefix and that suffix —
// no delimiter needed.
func parseSqsQueueTombstoneKey(key []byte) (queueName string, gen uint64, ok bool) {
	if !bytes.HasPrefix(key, []byte(SqsQueueTombstonePrefix)) {
		return "", 0, false
	}
	rest := key[len(SqsQueueTombstonePrefix):]
	if len(rest) < sqsGenerationSuffixLen {
		return "", 0, false
	}
	encQueue := rest[:len(rest)-sqsGenerationSuffixLen]
	gen = binary.BigEndian.Uint64(rest[len(rest)-sqsGenerationSuffixLen:])
	name, err := decodeSQSSegment(string(encQueue))
	if err != nil {
		return "", 0, false
	}
	return name, gen, true
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

// sqsMsgByAgePrefixAllGenerations returns the prefix for every byage
// entry under (queue, *) — i.e. across every queue generation, alive
// and superseded. The reaper uses this to surface orphan records left
// over by PurgeQueue / DeleteQueue, which bump the generation counter
// instead of cleaning up old keys.
func sqsMsgByAgePrefixAllGenerations(queueName string) []byte {
	buf := make([]byte, 0, len(SqsMsgByAgePrefix)+sqsKeyCapSmall)
	buf = append(buf, SqsMsgByAgePrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	return buf
}

// sqsMsgByAgeRecord is the parsed shape of a byage key. Generation,
// send timestamp, and message id all live in the key (the value is
// just the message id again so a missing-data scan does not have to
// open the data record). Returns ok=false when the key does not match
// the expected shape, so the reaper can skip junk without looping.
type sqsMsgByAgeRecord struct {
	Generation      uint64
	SendTimestampMs int64
	MessageID       string
}

// sqsByAgeKeyHeaderLen is the byte length of the (gen, ts) prefix that
// follows the queue segment in a byage key — two big-endian uint64s.
const sqsByAgeKeyHeaderLen = 16

func parseSqsMsgByAgeKey(key []byte, queueName string) (sqsMsgByAgeRecord, bool) {
	expected := sqsMsgByAgePrefixAllGenerations(queueName)
	if !bytes.HasPrefix(key, expected) {
		return sqsMsgByAgeRecord{}, false
	}
	rest := key[len(expected):]
	if len(rest) < sqsByAgeKeyHeaderLen {
		return sqsMsgByAgeRecord{}, false
	}
	gen := binary.BigEndian.Uint64(rest[:8])
	tsRaw := binary.BigEndian.Uint64(rest[8:sqsByAgeKeyHeaderLen])
	msgIDEnc := string(rest[sqsByAgeKeyHeaderLen:])
	msgID, err := decodeSQSSegment(msgIDEnc)
	if err != nil {
		return sqsMsgByAgeRecord{}, false
	}
	// Wall-clock millis fits in int63; the only way tsRaw exceeds
	// math.MaxInt64 is if the caller wrote a key with a uint64 that
	// the rest of the adapter would never produce. Treat that as
	// malformed.
	if tsRaw > 1<<63-1 {
		return sqsMsgByAgeRecord{}, false
	}
	return sqsMsgByAgeRecord{
		Generation:      gen,
		SendTimestampMs: int64(tsRaw),
		MessageID:       msgID,
	}, true
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
