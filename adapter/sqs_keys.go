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
	// SqsQueueTombstonePrefix prefixes a generation-orphan marker.
	// DeleteQueue and PurgeQueue each write one (queue, gen) tombstone
	// in the same OCC transaction that supersedes that generation —
	// DeleteQueue tombstones the gen it removes the meta row at, and
	// PurgeQueue tombstones the pre-bump gen so the reaper can find
	// pre-purge orphans even if the queue is deleted before the next
	// reaper tick. The reaper enumerates these markers to clean up
	// orphan data / vis / byage / dedup / group keys for superseded
	// generations. The tombstone is itself deleted once the reaper
	// confirms no message-keyspace state remains for that (queue, gen).
	SqsQueueTombstonePrefix = "!sqs|queue|tombstone|"
)

// HT-FIFO partitioned-keyspace discriminator. Per the §3.1 design in
// docs/design/2026_04_26_proposed_sqs_split_queue_fifo.md, partitioned
// FIFO queues live in a separate keyspace so the legacy single-
// partition layout can stay byte-identical on disk:
//
//	legacy:      !sqs|msg|<family>|<queue>|<gen>|<rest>
//	partitioned: !sqs|msg|<family>|p|<queue>|<partition>|<gen>|<rest>
//
// The literal "p|" segment is the discriminator. validateQueueName
// rejects "|" in queue names, so a legacy "!sqs|msg|data|<queue>|..."
// can never collide with a partitioned "!sqs|msg|data|p|<queue>|..."
// — the queue-name segment is base64-raw-URL-encoded (see
// encodeSQSSegment) and cannot start with the literal ASCII byte 'p'
// followed by '|'.
//
// Each partitioned constructor terminates the variable-length
// queue-name segment with a '|' before the fixed-width partition
// uint32. Without that delimiter, a prefix scan for queue "q" would
// also match queue "q1" because base64("q") is a strict byte prefix
// of base64("q1"). The discriminator inserts the '|' into the prefix
// itself; the per-constructor terminator inserts it after the queue.
const sqsPartitionedDiscriminator = "p|"

// sqsPartitionedQueueTerminator is appended after the encoded queue
// name in every partitioned key. It mirrors the role the fixed-width
// generation suffix plays in tombstone keys: a hard end-of-segment
// marker that prevents queue-name prefix collisions during scans.
// '|' is safe by construction — validateQueueName rejects raw '|',
// and base64.RawURLEncoding never emits '|' (it uses A-Z, a-z, 0-9,
// '-', '_').
const sqsPartitionedQueueTerminator = '|'

// SqsPartitionedMsg*Prefix mirrors each legacy SqsMsg*Prefix with the
// partitioned-keyspace discriminator inserted. Defined as full string
// constants (rather than runtime concatenation in each constructor)
// so the byte-layout invariant is asserted by the type system: a
// future rename of the discriminator must touch the constants here,
// not 6+ scattered string concatenations.
const (
	SqsPartitionedMsgDataPrefix  = "!sqs|msg|data|" + sqsPartitionedDiscriminator
	SqsPartitionedMsgVisPrefix   = "!sqs|msg|vis|" + sqsPartitionedDiscriminator
	SqsPartitionedMsgDedupPrefix = "!sqs|msg|dedup|" + sqsPartitionedDiscriminator
	SqsPartitionedMsgGroupPrefix = "!sqs|msg|group|" + sqsPartitionedDiscriminator
	SqsPartitionedMsgByAgePrefix = "!sqs|msg|byage|" + sqsPartitionedDiscriminator
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

// ---------------------- HT-FIFO partitioned keyspace ----------------------
//
// The constructors below mirror the legacy sqsMsg*Key family with a
// partition uint32 inserted between the queue segment and the
// generation. The legacy keyspace is unchanged on disk, so existing
// queues and Standard queues stay byte-identical — these helpers are
// reachable only when meta.PartitionCount > 1, and the §11 PR 2
// dormancy gate currently rejects that at CreateQueue. The data plane
// dispatch lands together with the gate-lift in PR 5.
//
// Each helper appends the partition as a fixed-width big-endian
// uint32 so prefix scans `!sqs|msg|<family>|p|<queue>|<partition>|`
// can pick exactly one partition's keys without touching its
// neighbours.

// sqsPartitionedMsgDataKey builds the data-record key for a
// partitioned FIFO queue.
func sqsPartitionedMsgDataKey(queueName string, partition uint32, gen uint64, messageID string) []byte {
	buf := make([]byte, 0, len(SqsPartitionedMsgDataPrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsPartitionedMsgDataPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = append(buf, sqsPartitionedQueueTerminator)
	buf = appendU32(buf, partition)
	buf = appendU64(buf, gen)
	buf = append(buf, encodeSQSSegment(messageID)...)
	return buf
}

// sqsPartitionedMsgVisKey builds the visibility-index key for a
// partitioned FIFO queue.
func sqsPartitionedMsgVisKey(queueName string, partition uint32, gen uint64, visibleAtMillis int64, messageID string) []byte {
	buf := make([]byte, 0, len(SqsPartitionedMsgVisPrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsPartitionedMsgVisPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = append(buf, sqsPartitionedQueueTerminator)
	buf = appendU32(buf, partition)
	buf = appendU64(buf, gen)
	buf = appendU64(buf, uint64MaxZero(visibleAtMillis))
	buf = append(buf, encodeSQSSegment(messageID)...)
	return buf
}

// sqsPartitionedMsgVisPrefixForQueue returns the prefix of every
// vis-index key for a single (queue, partition, gen) triple. The
// partition fan-out scans this prefix on each partition independently.
func sqsPartitionedMsgVisPrefixForQueue(queueName string, partition uint32, gen uint64) []byte {
	buf := make([]byte, 0, len(SqsPartitionedMsgVisPrefix)+sqsKeyCapSmall)
	buf = append(buf, SqsPartitionedMsgVisPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = append(buf, sqsPartitionedQueueTerminator)
	buf = appendU32(buf, partition)
	buf = appendU64(buf, gen)
	return buf
}

// sqsPartitionedMsgDedupKey builds the FIFO dedup key for a
// partitioned queue. The dedup window is per-partition by design
// (DeduplicationScope=messageGroup with PartitionCount>1) — the
// validator in adapter/sqs_partitioning.go rejects the queue-scoped
// scope on partitioned queues, so this key shape is always reachable
// from the same partition that ran the dedup check.
func sqsPartitionedMsgDedupKey(queueName string, partition uint32, gen uint64, dedupID string) []byte {
	buf := make([]byte, 0, len(SqsPartitionedMsgDedupPrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsPartitionedMsgDedupPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = append(buf, sqsPartitionedQueueTerminator)
	buf = appendU32(buf, partition)
	buf = appendU64(buf, gen)
	buf = append(buf, encodeSQSSegment(dedupID)...)
	return buf
}

// sqsPartitionedMsgGroupKey builds the FIFO group-lock key for a
// partitioned queue. partitionFor maps a MessageGroupId to one
// partition, so the group lock for any given group lives on exactly
// one partition — there is no cross-partition group-lock invariant
// to maintain.
func sqsPartitionedMsgGroupKey(queueName string, partition uint32, gen uint64, groupID string) []byte {
	buf := make([]byte, 0, len(SqsPartitionedMsgGroupPrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsPartitionedMsgGroupPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = append(buf, sqsPartitionedQueueTerminator)
	buf = appendU32(buf, partition)
	buf = appendU64(buf, gen)
	buf = append(buf, encodeSQSSegment(groupID)...)
	return buf
}

// sqsPartitionedMsgByAgeKey builds the send-age index key for a
// partitioned queue. The reaper enumerates both the legacy and
// partitioned byage prefixes when reaping a queue (see
// sqsMsgByAgePrefixesForQueue) so a queue that is partitioned today
// — or, hypothetically, that gains partitions across a future
// migration — does not strand its old data.
func sqsPartitionedMsgByAgeKey(queueName string, partition uint32, gen uint64, sendTimestampMs int64, messageID string) []byte {
	buf := make([]byte, 0, len(SqsPartitionedMsgByAgePrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsPartitionedMsgByAgePrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = append(buf, sqsPartitionedQueueTerminator)
	buf = appendU32(buf, partition)
	buf = appendU64(buf, gen)
	buf = appendU64(buf, uint64MaxZero(sendTimestampMs))
	buf = append(buf, encodeSQSSegment(messageID)...)
	return buf
}

// sqsPartitionedMsgByAgePrefixForQueueAllPartitions returns the
// prefix shared by every partitioned byage entry for one queue
// (across all partitions and all generations). The reaper uses it
// alongside the legacy prefix to enumerate orphan records on a
// partitioned queue.
func sqsPartitionedMsgByAgePrefixForQueueAllPartitions(queueName string) []byte {
	buf := make([]byte, 0, len(SqsPartitionedMsgByAgePrefix)+sqsKeyCapSmall)
	buf = append(buf, SqsPartitionedMsgByAgePrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = append(buf, sqsPartitionedQueueTerminator)
	return buf
}

// sqsMsgByAgePrefixesForQueue returns the {legacy, partitioned}
// prefix pair for a queue's byage records. The reaper iterates both:
// a queue created before HT-FIFO landed has only legacy entries; a
// partitioned queue created after PR 5 has only partitioned entries;
// no queue has both today, but enumerating both keeps the reaper
// future-proof against an offline-rebuild migration that produces a
// mixed-prefix queue.
func sqsMsgByAgePrefixesForQueue(queueName string) [][]byte {
	return [][]byte{
		sqsMsgByAgePrefixAllGenerations(queueName),
		sqsPartitionedMsgByAgePrefixForQueueAllPartitions(queueName),
	}
}

// sqsPartitionedByAgeKeyHeaderLen is the byte length of the
// (partition, gen, ts) header that follows the queue segment in a
// partitioned byage key — one big-endian uint32 plus two big-endian
// uint64s.
const sqsPartitionedByAgeKeyHeaderLen = 4 + 8 + 8

// parseSqsPartitionedMsgByAgeKey reverses sqsPartitionedMsgByAgeKey.
// Returns ok=false when the key does not match the expected partitioned
// shape. The reaper tries this parser when parseSqsMsgByAgeKey fails,
// so it can handle a queue with both legacy and partitioned entries
// (today only one or the other applies, but the dual-parse keeps the
// reaper safe against future migrations).
func parseSqsPartitionedMsgByAgeKey(key []byte, queueName string) (sqsPartitionedMsgByAgeRecord, bool) {
	expected := sqsPartitionedMsgByAgePrefixForQueueAllPartitions(queueName)
	if !bytes.HasPrefix(key, expected) {
		return sqsPartitionedMsgByAgeRecord{}, false
	}
	rest := key[len(expected):]
	if len(rest) < sqsPartitionedByAgeKeyHeaderLen {
		return sqsPartitionedMsgByAgeRecord{}, false
	}
	partition := binary.BigEndian.Uint32(rest[:4])
	gen := binary.BigEndian.Uint64(rest[4:12])
	tsRaw := binary.BigEndian.Uint64(rest[12:sqsPartitionedByAgeKeyHeaderLen])
	msgIDEnc := string(rest[sqsPartitionedByAgeKeyHeaderLen:])
	msgID, err := decodeSQSSegment(msgIDEnc)
	if err != nil {
		return sqsPartitionedMsgByAgeRecord{}, false
	}
	// Wall-clock millis fits in int63 — see parseSqsMsgByAgeKey for
	// the same bound.
	if tsRaw > 1<<63-1 {
		return sqsPartitionedMsgByAgeRecord{}, false
	}
	return sqsPartitionedMsgByAgeRecord{
		Partition:       partition,
		Generation:      gen,
		SendTimestampMs: int64(tsRaw),
		MessageID:       msgID,
	}, true
}

// sqsPartitionedMsgByAgeRecord is the parsed shape of a partitioned
// byage key. Mirrors sqsMsgByAgeRecord with partition added.
type sqsPartitionedMsgByAgeRecord struct {
	Partition       uint32
	Generation      uint64
	SendTimestampMs int64
	MessageID       string
}

// appendU32 mirrors appendU64 for the partition segment.
func appendU32(dst []byte, v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return append(dst, buf[:]...)
}
