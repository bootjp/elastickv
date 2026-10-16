package adapter

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"

	logicalbackup "github.com/bootjp/elastickv/internal/backup"
	"github.com/stretchr/testify/require"
)

// SQS mirrors pinned here (internal/backup copy -> live counterpart):
//
//   - sqs.go prefix constants and key parsers (classic and "p|" partitioned
//     families) -> sqs_keys.go / sqs_messages.go key builders.
//   - encode_sqs.go sqsStoredQueueMeta + queue meta/gen/seq keys ->
//     encodeSQSQueueMeta, sqsQueueMetaKey, sqsQueueGenKey, sqsQueueSeqKey.
//   - encode_sqs.go sqsStoredMessage + sqsMsgDataKeyBytes /
//     sqsPartitionedMsgDataKeyBytes -> encodeSQSMessageRecord +
//     sqsMsgDataKeyDispatch.
//   - encode_sqs.go partitionForGroup -> partitionFor (the restore rejects a
//     dump whose partition disagrees with its own hash, so a drift fails the
//     round trip).
//   - encode_sqs_side.go vis / byage / dedup key builders, resolveDedupID and
//     encodeFifoDedupRecordBytes -> sqsMsgVisKeyDispatch,
//     sqsMsgByAgeKeyDispatch, sqsMsgDedupKeyDispatch, resolveFifoDedupID and
//     encodeFifoDedupRecord.
//
// Families a restore cannot reproduce by design, so the live side is built in
// the documented restored state (encode_sqs.go, encode_sqs_side.go):
//
//   - Generation: the dump carries none; the restore stamps generation 1, the
//     generation CreateQueue allocates on a fresh cluster.
//   - Queue meta: incarnation, created-at stamps, tags and throttle config are
//     not in the dump and restore as zero values, so they stay unset.
//   - Visibility: the receipt token is random per send and the dump zeroes
//     visibility state, so every message restores never-received and fully
//     visible (visible_at 0, no token, receive count 0). The vis index row
//     still sits at available_at, as a fresh send writes it.
//   - FIFO dedup row: SendMessage stamps send_timestamp_ms from a second
//     clock read right after the record's; the dump keeps only the record's,
//     so the live side is the common case where both reads share a
//     millisecond.
//   - FIFO group-lock rows are written on receive, never on send, and the
//     restore never emits them, so no fixture has one.

// xcheckSQSGeneration is the generation CreateQueue allocates on a fresh
// cluster and the generation every restored queue carries.
const xcheckSQSGeneration uint64 = 1

// xcheckSQSSendBaseMs is the send timestamp of the first fixture message.
const xcheckSQSSendBaseMs int64 = 1_760_000_000_000

// xcheckSQSMaxDelaySeconds is the longest per-message delay SendMessage accepts.
const xcheckSQSMaxDelaySeconds int64 = sqsMaxDelaySeconds

// xcheckSQSQueue is one queue fixture: the CreateQueue attributes and the
// messages SendMessage writes into it, in send order.
type xcheckSQSQueue struct {
	name     string
	queue    string
	attrs    map[string]string
	messages []xcheckSQSMessage
	// wantPartitions is the number of distinct partitions the messages
	// land on (0 for the classic keyspace), pinning that the fixture
	// spreads message groups across partitions.
	wantPartitions int
}

// xcheckSQSMessage is one SendMessage call.
type xcheckSQSMessage struct {
	body       string
	attributes map[string]sqsMessageAttributeValue
	groupID    string
	dedupID    string
	delay      *int64
}

func TestBackupCrossCheckSQS(t *testing.T) {
	t.Parallel()
	queues := xcheckSQSQueues()
	xcheckSQSRequireFamilies(t, queues)
	for _, tc := range queues {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			meta, live := xcheckSQSLiveRecords(t, tc)
			require.Equal(t, tc.wantPartitions, xcheckSQSPartitionCount(meta, tc.messages), "partitions used")

			_, restored := xcheckRoundTrip(t, logicalbackup.AdapterSet{SQS: true}, live)
			xcheckRequireIdentical(t, live, restored)
		})
	}
}

// xcheckSQSLiveRecords returns the records the live CreateQueue and
// SendMessage paths write for a queue fixture, plus the queue meta.
func xcheckSQSLiveRecords(t *testing.T, tc xcheckSQSQueue) (*sqsQueueMeta, []xcheckPair) {
	t.Helper()
	require.NoError(t, validateQueueName(tc.queue))
	meta, err := parseAttributesIntoMeta(tc.queue, tc.attrs)
	require.NoError(t, err)
	meta.Generation = xcheckSQSGeneration
	metaValue, err := encodeSQSQueueMeta(meta)
	require.NoError(t, err)
	live := []xcheckPair{
		{key: sqsQueueMetaKey(tc.queue), value: metaValue},
		{key: sqsQueueGenKey(tc.queue), value: []byte(strconv.FormatUint(meta.Generation, 10))},
	}
	var seq uint64
	for i, m := range tc.messages {
		if meta.IsFIFO {
			seq++ // sendFifoMessage: nextSeq = prevSeq + 1
		}
		live = append(live, xcheckSQSSend(t, meta, xcheckSQSMessageID(tc.queue, i), xcheckSQSSendBaseMs+int64(i), m, seq)...)
	}
	if seq > 0 {
		// Each FIFO send overwrites the counter with its own sequence number.
		live = append(live, xcheckPair{key: sqsQueueSeqKey(tc.queue), value: []byte(strconv.FormatUint(seq, 10))})
	}
	return meta, live
}

// xcheckSQSSend returns the records one SendMessage writes: the data,
// visibility and by-age rows and, on FIFO queues, the dedup row. seq is the
// FIFO sequence number (0 on standard queues).
func xcheckSQSSend(t *testing.T, meta *sqsQueueMeta, messageID string, sendTs int64, m xcheckSQSMessage, seq uint64) []xcheckPair {
	t.Helper()
	in := sqsSendMessageInput{
		MessageBody:            m.body,
		DelaySeconds:           m.delay,
		MessageAttributes:      m.attributes,
		MessageGroupId:         m.groupID,
		MessageDeduplicationId: m.dedupID,
	}
	require.NoError(t, validateMessageAttributes(in.MessageAttributes))
	require.NoError(t, validateSendFIFOParams(meta, in))
	delay, err := resolveSendDelay(meta, in.DelaySeconds)
	require.NoError(t, err)
	rec, _, err := buildSendRecordWithIdentity(meta, in, sqsSendIdentity{
		messageID:         messageID,
		sendTsMillis:      sendTs,
		availableAtMillis: sendTs + delay*sqsMillisPerSecond,
	})
	require.NoError(t, err)
	rec.SequenceNumber = seq
	xcheckSQSRestoredVisibility(rec)
	data, err := encodeSQSMessageRecord(rec)
	require.NoError(t, err)

	// partitionFor is 0 on standard and single-partition queues, the
	// constant partition sendMessage uses for them.
	partition := partitionFor(meta, in.MessageGroupId)
	gen := meta.Generation
	out := []xcheckPair{
		{key: sqsMsgDataKeyDispatch(meta, meta.Name, partition, gen, rec.MessageID), value: data},
		{key: sqsMsgVisKeyDispatch(meta, meta.Name, partition, gen, rec.AvailableAtMillis, rec.MessageID), value: []byte(rec.MessageID)},
		{key: sqsMsgByAgeKeyDispatch(meta, meta.Name, partition, gen, rec.SendTimestampMillis, rec.MessageID), value: []byte(rec.MessageID)},
	}
	if !meta.IsFIFO {
		return out
	}
	dedup, err := encodeFifoDedupRecord(&sqsFifoDedupRecord{
		MessageID:        rec.MessageID,
		SendTimestampMs:  rec.SendTimestampMillis,
		ExpiresAtMillis:  rec.SendTimestampMillis + sqsFifoDedupWindowMillis,
		OriginalSequence: seq,
	})
	require.NoError(t, err)
	dedupKey := sqsMsgDedupKeyDispatch(meta, meta.Name, partition, gen, in.MessageGroupId, resolveFifoDedupID(meta, in))
	return append(out, xcheckPair{key: dedupKey, value: dedup})
}

// xcheckSQSRestoredVisibility puts a freshly built record into the
// visibility state a restore produces: never received and fully visible.
func xcheckSQSRestoredVisibility(rec *sqsMessageRecord) {
	rec.VisibleAtMillis = 0
	rec.CurrentReceiptToken = nil
	rec.ReceiveCount = 0
	rec.FirstReceiveMillis = 0
}

// xcheckSQSPartitionCount counts the distinct partitions the live
// partitionFor assigns the fixture's message groups; 0 for queues on the
// classic keyspace.
func xcheckSQSPartitionCount(meta *sqsQueueMeta, messages []xcheckSQSMessage) int {
	if meta.PartitionCount <= 1 {
		return 0
	}
	partitions := make(map[uint32]struct{})
	for _, m := range messages {
		partitions[partitionFor(meta, m.groupID)] = struct{}{}
	}
	return len(partitions)
}

// xcheckSQSMessageID derives a stable 32-hex-digit message ID, the shape
// newMessageIDHex mints.
func xcheckSQSMessageID(queue string, i int) string {
	return sqsMD5Hex([]byte(fmt.Sprintf("%s/%d", queue, i)))
}

func xcheckSQSQueues() []xcheckSQSQueue {
	return []xcheckSQSQueue{
		xcheckSQSStandardQueue(),
		xcheckSQSDeadLetterQueue(),
		xcheckSQSClassicFIFOQueue(),
		xcheckSQSPartitionedFIFOQueue(),
		xcheckSQSPerQueueFIFOQueue(),
	}
}

// xcheckSQSStandardQueue is a standard queue with non-default attributes, a
// redrive policy, message attributes of every type, a non-UTF-8 body, and
// queue-default, zero and maximum send delays.
func xcheckSQSStandardQueue() xcheckSQSQueue {
	noDelay := int64(0)
	maxDelay := xcheckSQSMaxDelaySeconds
	return xcheckSQSQueue{
		name:  "standard queue",
		queue: "xcheck-standard",
		attrs: map[string]string{
			"VisibilityTimeout":             "45",
			"MessageRetentionPeriod":        "86400",
			"DelaySeconds":                  "3",
			"ReceiveMessageWaitTimeSeconds": "2",
			"MaximumMessageSize":            "2048",
			"RedrivePolicy":                 `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:xcheck-dlq","maxReceiveCount":"5"}`,
		},
		messages: []xcheckSQSMessage{
			{
				body: "hello <&> \"world\"  ",
				attributes: map[string]sqsMessageAttributeValue{
					"str":    {DataType: "String", StringValue: "x<&>y   ünïcødé"},
					"num":    {DataType: "Number", StringValue: "1.50"},
					"bin":    {DataType: "Binary", BinaryValue: []byte{0x00, 0xFF, 0x7F}},
					"custom": {DataType: "String.custom", StringValue: "v"},
				},
			},
			{body: "\xff\xfe binary \x00 body", delay: &noDelay},
			{body: "delayed", delay: &maxDelay},
		},
	}
}

// xcheckSQSDeadLetterQueue is an empty queue carrying a redrive-allow
// policy: meta and generation rows only.
func xcheckSQSDeadLetterQueue() xcheckSQSQueue {
	return xcheckSQSQueue{
		name:  "empty dead-letter queue",
		queue: "xcheck-dlq",
		attrs: map[string]string{
			"RedriveAllowPolicy": `{"redrivePermission":"byQueue","sourceQueueArns":["arn:aws:sqs:us-east-1:000000000000:xcheck-standard"]}`,
		},
	}
}

// xcheckSQSClassicFIFOQueue is a single-partition FIFO queue with
// content-based deduplication and a queue-level delay, mixing explicit and
// body-derived deduplication IDs.
func xcheckSQSClassicFIFOQueue() xcheckSQSQueue {
	return xcheckSQSQueue{
		name:  "classic FIFO queue",
		queue: "xcheck-classic.fifo",
		attrs: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
			"VisibilityTimeout":         "60",
			"DelaySeconds":              "5",
		},
		messages: []xcheckSQSMessage{
			{body: "alpha 1", groupID: "g-alpha", dedupID: "order-1"},
			{body: "alpha 2 (content dedup)", groupID: "g-alpha"},
			{body: "beta 1", groupID: "g-beta", dedupID: "order-2"},
			{
				body: "beta 2 ✓ (content dedup)", groupID: "g-beta",
				attributes: map[string]sqsMessageAttributeValue{"trace": {DataType: "String", StringValue: "t-1"}},
			},
		},
	}
}

// xcheckSQSPartitionedFIFOQueue is an HT-FIFO queue that hashes message
// groups across four partitions. Two groups reuse one deduplication ID,
// which the partitioned dedup key keeps apart.
func xcheckSQSPartitionedFIFOQueue() xcheckSQSQueue {
	groups := []string{"orders-1", "orders-2", "orders-3", "orders-4", "orders-5", "orders-6", "orders-7", "orders-8"}
	messages := make([]xcheckSQSMessage, 0, len(groups)+2)
	for _, g := range groups {
		messages = append(messages, xcheckSQSMessage{body: "payload for " + g, groupID: g, dedupID: "first-" + g})
	}
	messages = append(messages,
		xcheckSQSMessage{body: "second in orders-1", groupID: "orders-1", dedupID: "shared-dedup"},
		xcheckSQSMessage{body: "second in orders-2", groupID: "orders-2", dedupID: "shared-dedup"},
	)
	return xcheckSQSQueue{
		name:  "partitioned FIFO queue, perMessageGroupId",
		queue: "xcheck-htfifo.fifo",
		attrs: map[string]string{
			"FifoQueue":           "true",
			"PartitionCount":      "4",
			"FifoThroughputLimit": "perMessageGroupId",
			"DeduplicationScope":  "messageGroup",
		},
		messages:       messages,
		wantPartitions: 4,
	}
}

// xcheckSQSPerQueueFIFOQueue is a partitioned FIFO queue in perQueue mode:
// every group routes to partition 0 of the partitioned keyspace.
func xcheckSQSPerQueueFIFOQueue() xcheckSQSQueue {
	return xcheckSQSQueue{
		name:  "partitioned FIFO queue, perQueue",
		queue: "xcheck-perqueue.fifo",
		attrs: map[string]string{
			"FifoQueue":                 "true",
			"PartitionCount":            "2",
			"FifoThroughputLimit":       "perQueue",
			"ContentBasedDeduplication": "true",
		},
		messages: []xcheckSQSMessage{
			{body: "first", groupID: "tenant-a"},
			{body: "second", groupID: "tenant-b"},
			{body: "third", groupID: "tenant-a", dedupID: "explicit"},
		},
		wantPartitions: 1,
	}
}

// xcheckSQSRequireFamilies pins that the fixtures write every SQS key family
// the restore rebuilds, in the classic and in the partitioned keyspace.
func xcheckSQSRequireFamilies(t *testing.T, queues []xcheckSQSQueue) {
	t.Helper()
	var all []xcheckPair
	for _, q := range queues {
		_, live := xcheckSQSLiveRecords(t, q)
		all = append(all, live...)
	}
	for _, prefix := range []string{
		SqsQueueMetaPrefix, SqsQueueGenPrefix, SqsQueueSeqPrefix,
		SqsMsgDataPrefix, SqsMsgVisPrefix, SqsMsgByAgePrefix, SqsMsgDedupPrefix,
		SqsPartitionedMsgDataPrefix, SqsPartitionedMsgVisPrefix, SqsPartitionedMsgByAgePrefix, SqsPartitionedMsgDedupPrefix,
	} {
		require.True(t, xcheckSQSHasFamily(all, prefix), "no fixture writes the %q family", prefix)
	}
}

// xcheckSQSHasFamily reports whether any live record belongs to the key
// family that starts with prefix. A classic message prefix is also a prefix
// of its partitioned twin, so partitioned keys do not count for it.
func xcheckSQSHasFamily(live []xcheckPair, prefix string) bool {
	partitioned := []byte(prefix + sqsPartitionedDiscriminator)
	for _, p := range live {
		if bytes.HasPrefix(p.key, []byte(prefix)) && !bytes.HasPrefix(p.key, partitioned) {
			return true
		}
	}
	return false
}
