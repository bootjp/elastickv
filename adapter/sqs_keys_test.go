package adapter

import (
	"bytes"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSqsPartitionedMsgKeys_DistinctFromLegacy pins the §3.1
// non-overlap guarantee: a partitioned key for any (queue, partition,
// gen, …) tuple must never share a byte sequence with a legacy key
// for the same (queue, gen, …). The "p|" discriminator after the
// family prefix is what makes this true; this test fails if the
// constant ever loses the trailing "|" or the discriminator changes
// shape such that a partition value (a fixed-width uint32) could
// align with a base64-encoded queue segment.
func TestSqsPartitionedMsgKeys_DistinctFromLegacy(t *testing.T) {
	t.Parallel()
	const (
		queue     = "orders.fifo"
		gen       = uint64(7)
		partition = uint32(3)
		msgID     = "0123456789abcdef"
		groupID   = "user-42"
		dedupID   = "dedup-token"
		ts        = int64(1700000000000)
	)
	cases := []struct {
		name        string
		legacy      []byte
		partitioned []byte
	}{
		{
			name:        "data",
			legacy:      sqsMsgDataKey(queue, gen, msgID),
			partitioned: sqsPartitionedMsgDataKey(queue, partition, gen, msgID),
		},
		{
			name:        "vis",
			legacy:      sqsMsgVisKey(queue, gen, ts, msgID),
			partitioned: sqsPartitionedMsgVisKey(queue, partition, gen, ts, msgID),
		},
		{
			name:        "dedup",
			legacy:      sqsMsgDedupKey(queue, gen, dedupID),
			partitioned: sqsPartitionedMsgDedupKey(queue, partition, gen, dedupID),
		},
		{
			name:        "group",
			legacy:      sqsMsgGroupKey(queue, gen, groupID),
			partitioned: sqsPartitionedMsgGroupKey(queue, partition, gen, groupID),
		},
		{
			name:        "byage",
			legacy:      sqsMsgByAgeKey(queue, gen, ts, msgID),
			partitioned: sqsPartitionedMsgByAgeKey(queue, partition, gen, ts, msgID),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NotEqual(t, tc.legacy, tc.partitioned,
				"legacy and partitioned keys must be byte-distinct")
			// Neither key may be a prefix of the other — if the
			// partitioned key were a prefix of the legacy, a scan
			// of the partitioned prefix would also match the
			// legacy key (and vice versa), which would let the
			// reaper or a partition-scoped scan accidentally
			// surface keys from the wrong keyspace.
			require.False(t, bytes.HasPrefix(tc.legacy, tc.partitioned),
				"legacy key must not start with partitioned key bytes")
			require.False(t, bytes.HasPrefix(tc.partitioned, tc.legacy),
				"partitioned key must not start with legacy key bytes")
			// The partitioned key must contain the "p|"
			// discriminator immediately after the family prefix —
			// asserts the constant did not lose its trailing "|".
			require.True(t, bytes.Contains(tc.partitioned, []byte("|p|")),
				"partitioned key must carry the p| discriminator")
		})
	}
}

// TestSqsPartitionedMsgKeys_PartitionsAreDistinct pins the per-
// partition isolation contract: two keys that differ only in the
// partition value must produce different bytes, and one cannot be a
// prefix of the other. Without this, a scan of partition k's prefix
// would surface partition k+1's data when the encoded partition
// happens to share a prefix.
func TestSqsPartitionedMsgKeys_PartitionsAreDistinct(t *testing.T) {
	t.Parallel()
	const (
		gen   = uint64(1)
		msgID = "msg-id"
	)
	a := sqsPartitionedMsgDataKey("orders.fifo", 0, gen, msgID)
	b := sqsPartitionedMsgDataKey("orders.fifo", 1, gen, msgID)
	require.NotEqual(t, a, b, "partition 0 and partition 1 must produce different keys")
	require.False(t, bytes.HasPrefix(a, b))
	require.False(t, bytes.HasPrefix(b, a))
	// Different generations within the same partition also distinct.
	c := sqsPartitionedMsgDataKey("orders.fifo", 0, gen+1, msgID)
	require.NotEqual(t, a, c, "different generations must produce different keys")
	// Different queues at the same (partition, gen) also distinct —
	// asserts the queueName segment actually participates in the key
	// (otherwise two queues would collide on the same partition).
	d := sqsPartitionedMsgDataKey("events.fifo", 0, gen, msgID)
	require.NotEqual(t, a, d, "different queue names must produce different keys")
}

// TestSqsPartitionedMsgKeys_QueueNamePrefixIsolation pins the
// queue-name terminator contract: a queue whose encoded name is a
// strict byte prefix of another queue's encoded name must NOT have
// its prefix match the longer queue's keys. base64.RawURLEncoding is
// variable-length and aligned on 3-byte input groups, so for an
// input length that is a multiple of 3, appending one extra byte
// extends (without changing) the encoded prefix: base64("queue") =
// "cXVldWU" is a strict byte prefix of base64("queue1") = "cXVldWUx".
// Without the trailing '|' terminator after the queue segment, a
// reaper scan of "queue"'s prefix would surface "queue1"'s entries
// (and vice versa for the all-partitions byage prefix). This test
// would have caught the missing-delimiter bug from gemini's review.
func TestSqsPartitionedMsgKeys_QueueNamePrefixIsolation(t *testing.T) {
	t.Parallel()
	const (
		shortQ    = "queue"  // base64 → "cXVldWU"
		longQ     = "queue1" // base64 → "cXVldWUx" (strict superstring)
		gen       = uint64(1)
		partition = uint32(0)
		// Distinct from the ts used in other tests so unparam is
		// satisfied that vis-key callers exercise more than one
		// timestamp value across the suite.
		ts    = int64(1234567890)
		msgID = "id"
	)
	// Confirm the precondition: base64(shortQ) is a strict byte
	// prefix of base64(longQ). If this ever stops being true, the
	// test still holds the contract but loses its motivating example.
	require.True(t,
		bytes.HasPrefix([]byte(encodeSQSSegment(longQ)), []byte(encodeSQSSegment(shortQ))),
		"sanity: base64(%q) must be a byte prefix of base64(%q)", shortQ, longQ)
	require.NotEqual(t, encodeSQSSegment(shortQ), encodeSQSSegment(longQ),
		"sanity: encodings must differ in length")

	// Per-(queue, partition) vis prefix must not leak across queue
	// names that share an encoded prefix.
	shortVisPrefix := sqsPartitionedMsgVisPrefixForQueue(shortQ, partition, gen)
	longVisKey := sqsPartitionedMsgVisKey(longQ, partition, gen, ts, msgID)
	require.False(t, bytes.HasPrefix(longVisKey, shortVisPrefix),
		"vis prefix for queue %q must not match keys for queue %q", shortQ, longQ)

	// All-partitions byage prefix must not leak across queue names
	// — this is what the reaper scans, so a leak would let the
	// reaper for the shorter queue enumerate (and potentially
	// delete) the longer queue's orphan records.
	shortByagePrefix := sqsPartitionedMsgByAgePrefixForQueueAllPartitions(shortQ)
	longByageKey := sqsPartitionedMsgByAgeKey(longQ, partition, gen, ts, msgID)
	require.False(t, bytes.HasPrefix(longByageKey, shortByagePrefix),
		"byage prefix for queue %q must not match keys for queue %q", shortQ, longQ)

	// Spot-check the data and group families too — the same
	// terminator argument applies to every partitioned constructor.
	shortDataKey := sqsPartitionedMsgDataKey(shortQ, partition, gen, msgID)
	longDataKey := sqsPartitionedMsgDataKey(longQ, partition, gen, msgID)
	require.False(t, bytes.HasPrefix(longDataKey, shortDataKey),
		"data key for %q must not be a prefix of %q's", shortQ, longQ)
	shortGroupKey := sqsPartitionedMsgGroupKey(shortQ, partition, gen, "g")
	longGroupKey := sqsPartitionedMsgGroupKey(longQ, partition, gen, "g")
	require.False(t, bytes.HasPrefix(longGroupKey, shortGroupKey),
		"group key for %q must not be a prefix of %q's", shortQ, longQ)
}

// TestSqsPartitionedMsgKeys_Deterministic pins the determinism
// contract: the same inputs always produce the same key bytes. The
// FIFO group lock and dedup lookups depend on byte-exact equality
// across processes, so this is not just a tidiness check — a non-
// deterministic key would silently corrupt the contract.
func TestSqsPartitionedMsgKeys_Deterministic(t *testing.T) {
	t.Parallel()
	const (
		queue     = "orders.fifo"
		partition = uint32(5)
		gen       = uint64(99)
		msgID     = "deterministic-id"
	)
	for range 16 {
		a := sqsPartitionedMsgDataKey(queue, partition, gen, msgID)
		b := sqsPartitionedMsgDataKey(queue, partition, gen, msgID)
		require.Equal(t, a, b)
	}
}

// TestSqsPartitionedMsgVisPrefixForQueue_BoundsScanToOnePartition
// pins that scanning a partition's vis prefix never matches another
// partition's keys. ReceiveMessage's per-partition fan-out
// (Phase 3.D PR 5) builds one prefix per partition and scans each
// independently; if the prefix were a prefix of another partition's
// keys, fan-out would double-count messages.
func TestSqsPartitionedMsgVisPrefixForQueue_BoundsScanToOnePartition(t *testing.T) {
	t.Parallel()
	const (
		queue = "q.fifo"
		gen   = uint64(3)
		ts    = int64(1700000000000)
		msgID = "m"
	)
	prefix0 := sqsPartitionedMsgVisPrefixForQueue(queue, 0, gen)
	keyP0 := sqsPartitionedMsgVisKey(queue, 0, gen, ts, msgID)
	keyP1 := sqsPartitionedMsgVisKey(queue, 1, gen, ts, msgID)
	require.True(t, bytes.HasPrefix(keyP0, prefix0),
		"partition 0's vis key must match partition 0's scan prefix")
	require.False(t, bytes.HasPrefix(keyP1, prefix0),
		"partition 1's vis key must NOT match partition 0's scan prefix; "+
			"otherwise the per-partition fan-out would double-count messages")
}

// TestParseSqsPartitionedMsgByAgeKey_RoundTrip pins the parser
// against the constructor: every constructed key must parse back to
// its inputs. The reaper depends on this round-trip when it surfaces
// orphan records under a tombstoned generation.
func TestParseSqsPartitionedMsgByAgeKey_RoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		queue     string
		partition uint32
		gen       uint64
		ts        int64
		msgID     string
	}{
		{"q.fifo", 0, 1, 1, "id-0"},
		{"orders.fifo", 7, 42, 1700000000000, "01234567890123456789ab"},
		{"q-with-dash.fifo", 31, 999, 0, "x"},
	}
	for _, tc := range cases {
		t.Run(tc.queue+"/p"+strconv.FormatUint(uint64(tc.partition), 10), func(t *testing.T) {
			t.Parallel()
			key := sqsPartitionedMsgByAgeKey(tc.queue, tc.partition, tc.gen, tc.ts, tc.msgID)
			parsed, ok := parseSqsPartitionedMsgByAgeKey(key, tc.queue)
			require.True(t, ok, "round-trip parse must succeed")
			require.Equal(t, tc.partition, parsed.Partition)
			require.Equal(t, tc.gen, parsed.Generation)
			require.Equal(t, tc.ts, parsed.SendTimestampMs)
			require.Equal(t, tc.msgID, parsed.MessageID)
		})
	}
}

// TestParseSqsPartitionedMsgByAgeKey_RejectsLegacy pins that the
// partitioned parser refuses a legacy byage key — the dual-parse
// pattern in the reaper relies on each parser rejecting the other's
// keyspace so a key is unambiguously routed to one parser.
func TestParseSqsPartitionedMsgByAgeKey_RejectsLegacy(t *testing.T) {
	t.Parallel()
	legacy := sqsMsgByAgeKey("q.fifo", 1, 1700000000000, "id-0")
	_, ok := parseSqsPartitionedMsgByAgeKey(legacy, "q.fifo")
	require.False(t, ok,
		"partitioned parser must reject a legacy byage key; "+
			"the dual-parse contract requires unambiguous routing")
}

// TestParseSqsMsgByAgeKey_RejectsPartitioned pins the converse: the
// legacy parser refuses a partitioned key. A regression here would
// let the reaper mis-decode a partitioned record's partition bytes
// as part of the generation, which would produce a bogus generation
// and either skip live records or operate on a tombstoned cohort
// that no longer exists.
func TestParseSqsMsgByAgeKey_RejectsPartitioned(t *testing.T) {
	t.Parallel()
	partitioned := sqsPartitionedMsgByAgeKey("q.fifo", 3, 1, 1700000000000, "id-0")
	_, ok := parseSqsMsgByAgeKey(partitioned, "q.fifo")
	require.False(t, ok,
		"legacy parser must reject a partitioned byage key")
}

// TestSqsMsgByAgePrefixesForQueue_CoversBothKeyspaces pins the
// reaper-side enumeration helper: the returned slice always contains
// both the legacy and partitioned prefixes, in that order. The
// reaper's Range loop iterates this slice; a regression that drops
// either prefix would silently leak orphan records of that flavour.
func TestSqsMsgByAgePrefixesForQueue_CoversBothKeyspaces(t *testing.T) {
	t.Parallel()
	prefixes := sqsMsgByAgePrefixesForQueue("orders.fifo")
	require.Len(t, prefixes, 2, "must enumerate both legacy and partitioned prefixes")
	require.Equal(t, sqsMsgByAgePrefixAllGenerations("orders.fifo"), prefixes[0])
	require.Equal(t, sqsPartitionedMsgByAgePrefixForQueueAllPartitions("orders.fifo"), prefixes[1])
}

// TestSqsPartitionedMsgPrefixes_TerminatedByQueueSegment pins the
// safety-by-construction argument from §3.1: queue names cannot
// contain "|" (validateQueueName rejects it), so the literal "p|"
// after the family prefix cannot collide with a legacy queue-name
// segment. This test asserts that no legacy key built from a name
// that begins with the literal byte 'p' (followed by base64-encoded
// trailing chars) starts with the partitioned prefix.
func TestSqsPartitionedMsgPrefixes_TerminatedByQueueSegment(t *testing.T) {
	t.Parallel()
	// The discriminator is the literal three bytes "p|" — note '|'
	// is outside the base64-raw-URL alphabet (A-Z, a-z, 0-9, '-',
	// '_'), so an encoded queue segment can never produce 'p'
	// followed immediately by '|'. The family prefix therefore
	// terminates before the partitioned prefix can match.
	legacy := sqsMsgDataKey("p", 1, "id")
	partitionedPrefixOnly := []byte("!sqs|msg|data|p|")
	require.False(t, bytes.HasPrefix(legacy, partitionedPrefixOnly),
		"a legacy key for any queue name must not start with the partitioned prefix; "+
			"the trailing | in the discriminator is what makes this true — "+
			"if the legacy key starts with `!sqs|msg|data|p|...`, the queue "+
			"name's encoded segment would have to start with `p|` which "+
			"base64-raw-URL never produces")
	// Also assert the partitioned prefix constants do not lose the
	// trailing "|".
	for _, p := range []string{
		SqsPartitionedMsgDataPrefix,
		SqsPartitionedMsgVisPrefix,
		SqsPartitionedMsgDedupPrefix,
		SqsPartitionedMsgGroupPrefix,
		SqsPartitionedMsgByAgePrefix,
	} {
		require.True(t, strings.HasSuffix(p, "p|"),
			"partitioned prefix %q must end with the p| discriminator", p)
	}
}
