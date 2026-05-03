package adapter

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSQSKeysDispatch_LegacyMatchesLegacyConstructor pins that
// every dispatch helper, when meta.PartitionCount <= 1, produces
// byte-for-byte the same key as the existing legacy constructor.
// The stage-1 contract is "no behavior change for non-partitioned
// queues" — any drift here would corrupt every existing queue.
func TestSQSKeysDispatch_LegacyMatchesLegacyConstructor(t *testing.T) {
	t.Parallel()
	const (
		queue   = "orders.fifo"
		gen     = uint64(7)
		msgID   = "0123456789abcdef"
		groupID = "user-42"
		dedupID = "dedup-token"
		ts      = int64(1700000000000)
	)
	cases := []struct {
		name       string
		dispatched []byte
		legacy     []byte
	}{
		// meta=nil sub-cases — ratchet against accidentally
		// dropping the nil-guard from any helper.
		{"meta=nil data", sqsMsgDataKeyDispatch(nil, queue, 0, gen, msgID),
			sqsMsgDataKey(queue, gen, msgID)},
		{"meta=nil vis",
			sqsMsgVisKeyDispatch(nil, queue, 0, gen, ts, msgID),
			sqsMsgVisKey(queue, gen, ts, msgID)},
		{"meta=nil dedup",
			sqsMsgDedupKeyDispatch(nil, queue, 0, gen, groupID, dedupID),
			sqsMsgDedupKey(queue, gen, dedupID)},
		{"meta=nil group",
			sqsMsgGroupKeyDispatch(nil, queue, 0, gen, groupID),
			sqsMsgGroupKey(queue, gen, groupID)},
		{"meta=nil byage",
			sqsMsgByAgeKeyDispatch(nil, queue, 0, gen, ts, msgID),
			sqsMsgByAgeKey(queue, gen, ts, msgID)},
		{"meta=nil vis prefix",
			sqsMsgVisPrefixForQueueDispatch(nil, queue, 0, gen),
			sqsMsgVisPrefixForQueue(queue, gen)},
		// meta.PartitionCount=0 sub-cases — ratchet against
		// accidentally dropping the > 1 guard.
		{"meta.PartitionCount=0 data",
			sqsMsgDataKeyDispatch(&sqsQueueMeta{PartitionCount: 0}, queue, 0, gen, msgID),
			sqsMsgDataKey(queue, gen, msgID)},
		{"meta.PartitionCount=0 vis",
			sqsMsgVisKeyDispatch(&sqsQueueMeta{PartitionCount: 0}, queue, 0, gen, ts, msgID),
			sqsMsgVisKey(queue, gen, ts, msgID)},
		{"meta.PartitionCount=0 vis prefix",
			sqsMsgVisPrefixForQueueDispatch(&sqsQueueMeta{PartitionCount: 0}, queue, 0, gen),
			sqsMsgVisPrefixForQueue(queue, gen)},
		// meta.PartitionCount=1 sub-cases — ratchet that the > 1
		// boundary is exclusive: 1 is still the legacy layout.
		{"meta.PartitionCount=1 data",
			sqsMsgDataKeyDispatch(&sqsQueueMeta{PartitionCount: 1}, queue, 0, gen, msgID),
			sqsMsgDataKey(queue, gen, msgID)},
		{"meta.PartitionCount=1 vis",
			sqsMsgVisKeyDispatch(&sqsQueueMeta{PartitionCount: 1}, queue, 0, gen, ts, msgID),
			sqsMsgVisKey(queue, gen, ts, msgID)},
		{"meta.PartitionCount=1 dedup",
			sqsMsgDedupKeyDispatch(&sqsQueueMeta{PartitionCount: 1}, queue, 0, gen, groupID, dedupID),
			sqsMsgDedupKey(queue, gen, dedupID)},
		{"meta.PartitionCount=1 group",
			sqsMsgGroupKeyDispatch(&sqsQueueMeta{PartitionCount: 1}, queue, 0, gen, groupID),
			sqsMsgGroupKey(queue, gen, groupID)},
		{"meta.PartitionCount=1 byage",
			sqsMsgByAgeKeyDispatch(&sqsQueueMeta{PartitionCount: 1}, queue, 0, gen, ts, msgID),
			sqsMsgByAgeKey(queue, gen, ts, msgID)},
		{"meta.PartitionCount=1 vis prefix",
			sqsMsgVisPrefixForQueueDispatch(&sqsQueueMeta{PartitionCount: 1}, queue, 0, gen),
			sqsMsgVisPrefixForQueue(queue, gen)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.legacy, tc.dispatched,
				"dispatched key must be byte-identical to the legacy "+
					"constructor on a non-partitioned queue; otherwise "+
					"existing data on disk becomes unreadable")
		})
	}
}

// TestSQSKeysDispatch_PartitionedMatchesPartitionedConstructor
// pins the converse: for meta.PartitionCount > 1, every dispatch
// helper produces byte-for-byte the same key as the partitioned
// constructor. This is what makes the partitioned key family
// reachable for SendMessage / ReceiveMessage in stage 2 and
// stage 3.
func TestSQSKeysDispatch_PartitionedMatchesPartitionedConstructor(t *testing.T) {
	t.Parallel()
	meta := &sqsQueueMeta{PartitionCount: 4}
	const (
		queue     = "events.fifo"
		gen       = uint64(11)
		partition = uint32(2)
		msgID     = "fedcba9876543210"
		groupID   = "tenant-9"
		dedupID   = "send-token"
		ts        = int64(1701234567890)
	)
	cases := []struct {
		name        string
		dispatched  []byte
		partitioned []byte
	}{
		{"data",
			sqsMsgDataKeyDispatch(meta, queue, partition, gen, msgID),
			sqsPartitionedMsgDataKey(queue, partition, gen, msgID)},
		{"vis",
			sqsMsgVisKeyDispatch(meta, queue, partition, gen, ts, msgID),
			sqsPartitionedMsgVisKey(queue, partition, gen, ts, msgID)},
		{"dedup",
			sqsMsgDedupKeyDispatch(meta, queue, partition, gen, groupID, dedupID),
			sqsPartitionedMsgDedupKey(queue, partition, gen, groupID, dedupID)},
		{"group",
			sqsMsgGroupKeyDispatch(meta, queue, partition, gen, groupID),
			sqsPartitionedMsgGroupKey(queue, partition, gen, groupID)},
		{"byage",
			sqsMsgByAgeKeyDispatch(meta, queue, partition, gen, ts, msgID),
			sqsPartitionedMsgByAgeKey(queue, partition, gen, ts, msgID)},
		{"vis prefix",
			sqsMsgVisPrefixForQueueDispatch(meta, queue, partition, gen),
			sqsPartitionedMsgVisPrefixForQueue(queue, partition, gen)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.partitioned, tc.dispatched,
				"dispatched key must be byte-identical to the "+
					"partitioned constructor on a partitioned queue")
		})
	}
}

// TestSQSKeysDispatch_BoundaryAtPartitionCount2 pins the > 1
// threshold: PartitionCount=2 is the smallest value that selects
// the partitioned keyspace. An off-by-one in the dispatch
// condition (e.g. >= 1 vs > 1) would not be caught by tests that
// only exercise PartitionCount=4.
func TestSQSKeysDispatch_BoundaryAtPartitionCount2(t *testing.T) {
	t.Parallel()
	meta := &sqsQueueMeta{PartitionCount: 2}
	const (
		queue = "boundary.fifo"
		gen   = uint64(1)
		msgID = "id"
	)
	got := sqsMsgDataKeyDispatch(meta, queue, 0, gen, msgID)
	want := sqsPartitionedMsgDataKey(queue, 0, gen, msgID)
	require.Equal(t, want, got,
		"PartitionCount=2 must dispatch to the partitioned "+
			"keyspace; an off-by-one in the > 1 threshold would "+
			"silently put PR 5b's first-partitioned-queue traffic "+
			"on the legacy keyspace")
	// And it must NOT match the legacy constructor.
	legacy := sqsMsgDataKey(queue, gen, msgID)
	require.NotEqual(t, legacy, got,
		"PartitionCount=2 must NOT route to the legacy keyspace")
}

// TestSQSDedupKeyDispatch_PartitionedScopesByMessageGroupId is the
// regression for the round-3 P1 (Codex) on PR #732: with
// DeduplicationScope=messageGroup on a partitioned queue the dedup
// key MUST include MessageGroupId so two distinct groups that
// FNV-collide onto the same partition do NOT share a dedup
// namespace. Without the group segment, a fresh send in group "B"
// reusing group "A"'s dedup-id would be silently acked with group
// "A"'s MessageId — that is a data-loss outcome.
func TestSQSDedupKeyDispatch_PartitionedScopesByMessageGroupId(t *testing.T) {
	t.Parallel()
	meta := &sqsQueueMeta{PartitionCount: 4}
	const (
		queue     = "events.fifo"
		gen       = uint64(11)
		partition = uint32(2)
		dedupID   = "shared-token"
	)
	keyA := sqsMsgDedupKeyDispatch(meta, queue, partition, gen, "groupA", dedupID)
	keyB := sqsMsgDedupKeyDispatch(meta, queue, partition, gen, "groupB", dedupID)
	require.NotEqual(t, keyA, keyB,
		"distinct MessageGroupIds on the same (queue, partition, dedupID) "+
			"must produce distinct dedup keys — otherwise a fresh send in "+
			"groupB is silently dropped as a duplicate of groupA")

	// Same group + same dedupID must round-trip to the same key (the
	// idempotency contract we DO want to keep).
	keyA2 := sqsMsgDedupKeyDispatch(meta, queue, partition, gen, "groupA", dedupID)
	require.Equal(t, keyA, keyA2,
		"same (group, dedupID) must produce the same dedup key — "+
			"AWS idempotent-by-design retries depend on this")

	// Legacy (non-partitioned) path is unaffected: groupID is ignored
	// because there is only one implicit group on a non-partitioned
	// queue and the legacy key shape predates partitioning.
	legacyA := sqsMsgDedupKeyDispatch(nil, queue, 0, gen, "groupA", dedupID)
	legacyB := sqsMsgDedupKeyDispatch(nil, queue, 0, gen, "groupB", dedupID)
	require.Equal(t, legacyA, legacyB,
		"legacy keyspace ignores groupID — preserves the on-disk shape "+
			"for queues created before partitioning landed")
}

// TestSqsPartitionedMsgDedupKey_GroupDedupSeparator pins the round-6
// fix for the CodeRabbit major: encodeSQSSegment uses RawURLEncoding
// (no padding, alphabet [A-Za-z0-9_-]), so when groupID and dedupID
// segments are concatenated WITHOUT a separator, distinct (group,
// dedup) pairs can collapse onto the same byte sequence — most
// trivially when one of the two is empty (("", "abcd") vs ("abcd",
// "") encode identically as base64). Even with non-empty inputs the
// boundary is fragile because the per-segment length depends on the
// input length mod 3. The fix is a single sqsPartitionedQueueTerminator
// '|' between the two segments; '|' is outside RawURLEncoding's
// alphabet so it cannot be produced by either encodeSQSSegment call,
// making the boundary unambiguous regardless of input length.
func TestSqsPartitionedMsgDedupKey_GroupDedupSeparator(t *testing.T) {
	t.Parallel()
	const (
		queue     = "events.fifo"
		gen       = uint64(11)
		partition = uint32(2)
	)

	// (1) The empty-segment collision class. SQS validation
	// rejects empty group / dedup IDs at the public API, but the
	// key constructor must still be unambiguous on its own — a
	// future code path that passes an empty string (intentionally
	// or via a bug) must NOT silently merge keyspaces. Without the
	// terminator: ("", "abcd") and ("abcd", "") produce the same
	// "...QUJDRA" suffix.
	keyEmptyGroup := sqsPartitionedMsgDedupKey(queue, partition, gen, "", "abcd")
	keyEmptyDedup := sqsPartitionedMsgDedupKey(queue, partition, gen, "abcd", "")
	require.NotEqual(t, keyEmptyGroup, keyEmptyDedup,
		"('', 'abcd') and ('abcd', '') must produce distinct keys — "+
			"the terminator is the only thing that disambiguates them")

	// (2) The separator must appear AFTER the encoded groupID,
	// BEFORE the encoded dedupID. Build the expected suffix
	// "<b64(groupID)>|<b64(dedupID)>" and assert the key ends with
	// it. RawURLEncoding's alphabet (A-Z a-z 0-9 - _) excludes '|'
	// so neither segment can contribute one of its own — finding
	// the literal "|" between them therefore proves the round-6
	// terminator is in place. (We can't just count '|' because the
	// SqsPartitionedMsgDedupPrefix constant itself contains '|'
	// separators inside the family-name marker.)
	key := sqsPartitionedMsgDedupKey(queue, partition, gen, "groupA", "dedup-token")
	wantTail := append(append([]byte(encodeSQSSegment("groupA")), '|'), encodeSQSSegment("dedup-token")...)
	require.True(t, bytes.HasSuffix(key, wantTail),
		"key must end with <b64(group)>|<b64(dedup)>; got key=%q want suffix=%q",
		key, wantTail)

	// (3) Round-trip: two non-empty pairs whose b64 lengths align
	// at the segment boundary must still produce distinct keys.
	// Without a terminator, a regression that re-introduces back-
	// to-back encoding could (for some lengths) make these match.
	keyAB := sqsPartitionedMsgDedupKey(queue, partition, gen, "ab", "cd")
	keyABCD := sqsPartitionedMsgDedupKey(queue, partition, gen, "abcd", "")
	require.NotEqual(t, keyAB, keyABCD,
		"('ab', 'cd') and ('abcd', '') must produce distinct keys")

	// (4) Read-write symmetry: the dispatch helper used by
	// loadFifoDedupRecord (read) and sendFifoMessage (write) MUST
	// route to the same constructor so both sides observe the same
	// new format simultaneously — no read/write skew window.
	meta := &sqsQueueMeta{PartitionCount: 4}
	viaDispatch := sqsMsgDedupKeyDispatch(meta, queue, partition, gen, "groupA", "dedup-token")
	require.Equal(t, key, viaDispatch,
		"dispatch helper must produce the same bytes as the underlying "+
			"constructor — round-6 format change must be picked up "+
			"symmetrically on read and write paths")
}

// TestSQSKeysDispatch_LegacyAndPartitionedAreDistinct pins the
// keyspace-isolation invariant at the dispatch level: a legacy
// (PartitionCount=1) key and a partitioned (PartitionCount>1) key
// for the same conceptual record never share a byte sequence.
// This is what makes meta.PartitionCount the routing decision —
// without keyspace distinctness, a single-partition queue and a
// partitioned queue of the same name would collide.
func TestSQSKeysDispatch_LegacyAndPartitionedAreDistinct(t *testing.T) {
	t.Parallel()
	legacyMeta := &sqsQueueMeta{PartitionCount: 1}
	partitionedMeta := &sqsQueueMeta{PartitionCount: 4}
	const (
		queue = "q.fifo"
		gen   = uint64(1)
		msgID = "id"
	)
	legacy := sqsMsgDataKeyDispatch(legacyMeta, queue, 0, gen, msgID)
	partitioned := sqsMsgDataKeyDispatch(partitionedMeta, queue, 0, gen, msgID)
	require.NotEqual(t, legacy, partitioned,
		"legacy and partitioned keys must be byte-distinct")
	require.False(t, bytes.HasPrefix(legacy, partitioned),
		"legacy key must not start with partitioned key bytes")
	require.False(t, bytes.HasPrefix(partitioned, legacy),
		"partitioned key must not start with legacy key bytes")
}

// TestEffectivePartitionCount pins the iteration-count helper
// that ReceiveMessage's fanout uses. Returns 1 for any
// PartitionCount that the rest of the system treats as
// non-partitioned (nil meta, 0, 1) and the explicit count
// otherwise.
func TestEffectivePartitionCount(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint32(1), effectivePartitionCount(nil))
	require.Equal(t, uint32(1), effectivePartitionCount(&sqsQueueMeta{}))
	require.Equal(t, uint32(1), effectivePartitionCount(&sqsQueueMeta{PartitionCount: 1}))
	require.Equal(t, uint32(2), effectivePartitionCount(&sqsQueueMeta{PartitionCount: 2}))
	require.Equal(t, uint32(8), effectivePartitionCount(&sqsQueueMeta{PartitionCount: 8}))
	require.Equal(t, uint32(32), effectivePartitionCount(&sqsQueueMeta{PartitionCount: 32}))
}

// TestEffectivePartitionCount_PerQueueModeCollapsesToOne pins the
// codex P2 round-1 fix on PR #731: when a queue is configured
// with FifoThroughputLimit=perQueue, partitionFor's §3.3 short-
// circuit forces every MessageGroupId to partition 0 regardless
// of PartitionCount. The fanout helper MUST mirror that decision
// — returning the literal PartitionCount would have
// ReceiveMessage scan up to 31 guaranteed-empty partitions on
// every poll for no correctness benefit.
//
// Without this ratchet, a future refactor that drops the
// perQueue branch from effectivePartitionCount would silently
// regress receive performance to "scan 32 empty partitions per
// poll" with no test failure.
func TestEffectivePartitionCount_PerQueueModeCollapsesToOne(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		meta *sqsQueueMeta
		want uint32
	}{
		{
			name: "perQueue + PartitionCount=4 → 1",
			meta: &sqsQueueMeta{
				PartitionCount:      4,
				FifoThroughputLimit: htfifoThroughputPerQueue,
			},
			want: 1,
		},
		{
			name: "perQueue + PartitionCount=32 (max) → 1",
			meta: &sqsQueueMeta{
				PartitionCount:      32,
				FifoThroughputLimit: htfifoThroughputPerQueue,
			},
			want: 1,
		},
		{
			name: "perMessageGroupId + PartitionCount=4 → 4",
			meta: &sqsQueueMeta{
				PartitionCount:      4,
				FifoThroughputLimit: htfifoThroughputPerMessageGroupID,
			},
			want: 4,
		},
		{
			name: "empty FifoThroughputLimit + PartitionCount=4 → 4",
			meta: &sqsQueueMeta{
				PartitionCount:      4,
				FifoThroughputLimit: "",
			},
			want: 4,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, effectivePartitionCount(tc.meta))
		})
	}
}

// TestSQSKeysDispatch_PartitionIgnoredOnLegacy pins the contract
// that the partition argument is ignored when meta.PartitionCount
// <= 1 — calling with partition=0 vs partition=999 against a
// legacy queue produces the same key. Without this, a buggy
// caller passing a stale partition value to a non-partitioned
// queue would corrupt the keyspace.
func TestSQSKeysDispatch_PartitionIgnoredOnLegacy(t *testing.T) {
	t.Parallel()
	legacyMeta := &sqsQueueMeta{PartitionCount: 1}
	const (
		queue = "legacy.fifo"
		gen   = uint64(3)
		msgID = "id"
	)
	zero := sqsMsgDataKeyDispatch(legacyMeta, queue, 0, gen, msgID)
	bogus := sqsMsgDataKeyDispatch(legacyMeta, queue, 999, gen, msgID)
	require.Equal(t, zero, bogus,
		"partition arg must be ignored on a non-partitioned queue; "+
			"otherwise a stale-partition caller could write to a "+
			"different keyspace and silently strand the message")
}
