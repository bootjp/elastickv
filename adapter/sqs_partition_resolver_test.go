package adapter

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestNewSQSPartitionResolver_NilOnEmpty pins the constructor's
// nil-on-empty contract — kv.ShardRouter.WithPartitionResolver(nil)
// is documented as a no-op, so a non-partitioned cluster MUST end
// up with no resolver in the request hot path.
func TestNewSQSPartitionResolver_NilOnEmpty(t *testing.T) {
	t.Parallel()
	require.Nil(t, NewSQSPartitionResolver(nil))
	require.Nil(t, NewSQSPartitionResolver(map[string][]uint64{}))
}

// TestNewSQSPartitionResolver_DefensiveCopy pins that mutating the
// caller's input map after construction does NOT change the
// resolver's view. Without this, a hot-reload of --sqsFifoPartitionMap
// would partially alter the resolver mid-request and produce
// transient mis-routes.
func TestNewSQSPartitionResolver_DefensiveCopy(t *testing.T) {
	t.Parallel()
	input := map[string][]uint64{"q.fifo": {10, 11}}
	r := NewSQSPartitionResolver(input)
	// Mutate after construction.
	input["q.fifo"][0] = 999
	delete(input, "q.fifo")

	key := sqsPartitionedMsgDataKey("q.fifo", 0, 1, "msg")
	gid, ok := r.ResolveGroup(key)
	require.True(t, ok)
	require.Equal(t, uint64(10), gid,
		"resolver must keep its constructor-time view; "+
			"caller mutation cannot leak in")
}

// TestSQSPartitionResolver_ResolveByPartition pins the core dispatch
// contract: a key produced by sqsPartitionedMsg*Key for (queue,
// partition) resolves to the operator-chosen group for that exact
// partition.
func TestSQSPartitionResolver_ResolveByPartition(t *testing.T) {
	t.Parallel()
	r := NewSQSPartitionResolver(map[string][]uint64{
		"orders.fifo": {10, 11, 12, 13},
		"events.fifo": {20, 21},
	})
	cases := []struct {
		name      string
		queue     string
		partition uint32
		wantGroup uint64
	}{
		{"orders p0", "orders.fifo", 0, 10},
		{"orders p1", "orders.fifo", 1, 11},
		{"orders p3", "orders.fifo", 3, 13},
		{"events p0", "events.fifo", 0, 20},
		{"events p1", "events.fifo", 1, 21},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Every family must resolve to the same group for the
			// same (queue, partition) — the reaper / fanout reader
			// / send path all depend on this invariant.
			for familyName, key := range map[string][]byte{
				"data":  sqsPartitionedMsgDataKey(tc.queue, tc.partition, 1, "msg-id"),
				"vis":   sqsPartitionedMsgVisKey(tc.queue, tc.partition, 1, 1700000000000, "msg-id"),
				"dedup": sqsPartitionedMsgDedupKey(tc.queue, tc.partition, 1, "group-id", "dedup-id"),
				"group": sqsPartitionedMsgGroupKey(tc.queue, tc.partition, 1, "group-id"),
				"byage": sqsPartitionedMsgByAgeKey(tc.queue, tc.partition, 1, 1700000000000, "msg-id"),
			} {
				gid, ok := r.ResolveGroup(key)
				require.True(t, ok, "family %s key for (%s, p%d) must resolve",
					familyName, tc.queue, tc.partition)
				require.Equal(t, tc.wantGroup, gid,
					"family %s key for (%s, p%d) resolved to wrong group",
					familyName, tc.queue, tc.partition)
			}
		})
	}
}

// TestSQSPartitionResolver_QueueIsolation pins the queue-name
// terminator contract from the routing layer's perspective. Two
// queues with overlapping encoded prefixes (base64("queue") is a
// strict prefix of base64("queue1")) MUST resolve to their own
// groups, not each other's. The '|' terminator after the queue
// segment is what makes this safe — same invariant
// TestSqsPartitionedMsgKeys_QueueNamePrefixIsolation pins for the
// keyspace itself.
func TestSQSPartitionResolver_QueueIsolation(t *testing.T) {
	t.Parallel()
	r := NewSQSPartitionResolver(map[string][]uint64{
		"queue":  {500},
		"queue1": {600},
	})
	queueKey := sqsPartitionedMsgDataKey("queue", 0, 1, "id")
	queue1Key := sqsPartitionedMsgDataKey("queue1", 0, 1, "id")

	queueGID, ok := r.ResolveGroup(queueKey)
	require.True(t, ok)
	require.Equal(t, uint64(500), queueGID)

	queue1GID, ok := r.ResolveGroup(queue1Key)
	require.True(t, ok)
	require.Equal(t, uint64(600), queue1GID,
		"queue1 keys must NOT resolve to queue's group; "+
			"the '|' terminator after the queue segment is what makes this safe")
}

// TestSQSPartitionResolver_LegacyKeyFallsThrough pins that a non-
// partitioned (legacy) SQS key returns (0, false) — the
// kv.ShardRouter caller then falls through to the byte-range
// engine for default routing. A regression here would route every
// legacy queue's traffic to whichever group the resolver guessed
// from its partition map.
func TestSQSPartitionResolver_LegacyKeyFallsThrough(t *testing.T) {
	t.Parallel()
	r := NewSQSPartitionResolver(map[string][]uint64{
		"orders.fifo": {10, 11},
	})
	cases := []struct {
		name string
		key  []byte
	}{
		{"legacy data", sqsMsgDataKey("orders.fifo", 1, "id")},
		{"legacy vis", sqsMsgVisKey("orders.fifo", 1, 1700000000000, "id")},
		{"legacy dedup", sqsMsgDedupKey("orders.fifo", 1, "dedup")},
		{"legacy group", sqsMsgGroupKey("orders.fifo", 1, "group")},
		{"legacy byage", sqsMsgByAgeKey("orders.fifo", 1, 1700000000000, "id")},
		{"queue meta", sqsQueueMetaKey("orders.fifo")},
		{"non-sqs key", []byte("/some/other/key")},
		{"empty", []byte{}},
		{"nil", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gid, ok := r.ResolveGroup(tc.key)
			require.False(t, ok, "non-partitioned key must NOT resolve")
			require.Equal(t, uint64(0), gid)
		})
	}
}

// TestSQSPartitionResolver_UnknownQueueRecognisedButUnresolved
// pins the round-5 fail-closed contract: a well-formed partitioned
// key for a queue that is NOT in the partition map returns
// ResolveGroup=(0, false) AND RecognisesPartitionedKey=true. The
// router pairs these so the request fails closed — engine
// fall-through would silently route the misconfiguration through
// the SQS catalog default group (codex round-2 P1 on PR #715).
func TestSQSPartitionResolver_UnknownQueueRecognisedButUnresolved(t *testing.T) {
	t.Parallel()
	r := NewSQSPartitionResolver(map[string][]uint64{
		"known.fifo": {10, 11},
	})
	key := sqsPartitionedMsgDataKey("unknown.fifo", 0, 1, "id")
	gid, ok := r.ResolveGroup(key)
	require.False(t, ok)
	require.Equal(t, uint64(0), gid)
	require.True(t, r.RecognisesPartitionedKey(key),
		"unknown-queue partitioned key must still be recognised — "+
			"the router pairs Recognised=true with ok=false to fail "+
			"closed instead of falling through to the engine")
}

// TestSQSPartitionResolver_OutOfRangePartitionRecognisedButUnresolved
// pins the round-5 fail-closed contract for a partition value
// beyond the configured PartitionCount. Same router-side
// fail-closed argument as the unknown-queue case.
func TestSQSPartitionResolver_OutOfRangePartitionRecognisedButUnresolved(t *testing.T) {
	t.Parallel()
	r := NewSQSPartitionResolver(map[string][]uint64{
		"q.fifo": {10, 11},
	})
	// PartitionCount in resolver is 2 (partitions 0 and 1); craft a
	// key for partition 5 to simulate a writer at a different
	// partition count.
	key := sqsPartitionedMsgDataKey("q.fifo", 5, 1, "id")
	gid, ok := r.ResolveGroup(key)
	require.False(t, ok)
	require.Equal(t, uint64(0), gid)
	require.True(t, r.RecognisesPartitionedKey(key),
		"OOR-partition key must still be recognised — the router "+
			"pairs Recognised=true with ok=false to fail closed "+
			"instead of falling through to the engine")
}

// TestSQSPartitionResolver_NilReceiverIsSafe pins defensive
// behaviour — a nil resolver pointer must not panic on
// ResolveGroup. kv.ShardRouter's resolver-first dispatch checks
// `if s.partitionResolver != nil` before calling, but a typed-nil
// can still slip through interface assertions.
func TestSQSPartitionResolver_NilReceiverIsSafe(t *testing.T) {
	t.Parallel()
	var r *SQSPartitionResolver
	gid, ok := r.ResolveGroup([]byte("any-key"))
	require.False(t, ok)
	require.Equal(t, uint64(0), gid)
}

// TestSQSPartitionResolver_RecognisesPartitionedKey pins the
// shape-only predicate the router uses to decide between
// fall-through and fail-closed (codex round-2 P1 on PR #715).
// RecognisesPartitionedKey MUST answer purely on the structural
// shape — partitioned family prefix + queue + '|' terminator +
// be32 partition — independent of whether the queue is in the
// routes map. Otherwise the router could not reliably fail-closed
// for unresolved-but-recognised keys.
func TestSQSPartitionResolver_RecognisesPartitionedKey(t *testing.T) {
	t.Parallel()
	r := NewSQSPartitionResolver(map[string][]uint64{
		"known.fifo": {10, 11},
	})
	cases := []struct {
		name string
		key  []byte
		want bool
	}{
		{
			name: "data family + known queue",
			key:  sqsPartitionedMsgDataKey("known.fifo", 0, 1, "id"),
			want: true,
		},
		{
			name: "vis family + unknown queue (recognised, unresolved)",
			key:  sqsPartitionedMsgVisKey("not-in-routes.fifo", 0, 1, 1, "id"),
			want: true,
		},
		{
			name: "byage family + OOR partition (recognised, unresolved)",
			key:  sqsPartitionedMsgByAgeKey("known.fifo", 99, 1, 1, "id"),
			want: true,
		},
		{
			name: "legacy SQS key — not partitioned",
			key:  sqsMsgDataKey("known.fifo", 1, "id"),
			want: false,
		},
		{
			name: "non-SQS key",
			key:  []byte("/foo/bar"),
			want: false,
		},
		{
			name: "queue meta key",
			key:  sqsQueueMetaKey("known.fifo"),
			want: false,
		},
		{
			name: "empty",
			key:  []byte{},
			want: false,
		},
		{
			name: "nil",
			key:  nil,
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, r.RecognisesPartitionedKey(tc.key))
		})
	}
}

// TestSQSPartitionResolver_RecognisesPartitionedKey_NilReceiver
// pins the typed-nil-safe branch — same as ResolveGroup, but for
// the new predicate.
func TestSQSPartitionResolver_RecognisesPartitionedKey_NilReceiver(t *testing.T) {
	t.Parallel()
	var r *SQSPartitionResolver
	require.False(t, r.RecognisesPartitionedKey([]byte("any-key")))
}

// TestSQSPartitionResolver_RecognisesMalformedPartitionedKey pins
// the prefix-only check (round 5 nit on PR #715): a key with a
// valid partitioned family prefix but a corrupt / truncated queue
// or partition segment MUST still be recognised so the router
// fails closed rather than falling through to engine routing
// (which would silently mis-route to the SQS catalog default
// group via routeKey's !sqs|route|global collapse).
func TestSQSPartitionResolver_RecognisesMalformedPartitionedKey(t *testing.T) {
	t.Parallel()
	r := NewSQSPartitionResolver(map[string][]uint64{
		"q.fifo": {10, 11},
	})
	cases := []struct {
		name string
		key  []byte
	}{
		{
			// Prefix only, nothing after — parsePartitionedSQSKey
			// would fail on the missing '|' terminator, but the
			// shape IS recognised.
			name: "prefix only",
			key:  []byte(SqsPartitionedMsgDataPrefix),
		},
		{
			// Prefix + base64 garbage that decodes invalidly.
			// parsePartitionedSQSKey would fail at decodeSQSSegment.
			name: "prefix + invalid base64",
			key:  []byte(SqsPartitionedMsgDataPrefix + "!!!|"),
		},
		{
			// Prefix + valid queue segment + '|' but no partition
			// bytes (truncated).
			name: "prefix + queue + truncated partition",
			key:  []byte(SqsPartitionedMsgVisPrefix + encodeSQSSegment("q.fifo") + "|"),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.True(t, r.RecognisesPartitionedKey(tc.key),
				"malformed partitioned key MUST be recognised so "+
					"the router fails closed rather than mis-routing "+
					"through the engine's SQS catalog default")
			gid, ok := r.ResolveGroup(tc.key)
			require.False(t, ok,
				"malformed key cannot resolve")
			require.Equal(t, uint64(0), gid)
		})
	}
}

// TestSQSPartitionResolver_PrefixesAlign pins that the resolver's
// family-prefix list matches the constants in sqs_keys.go exactly.
// A future renamed prefix or added family that touches sqs_keys.go
// without also touching sqsResolverFamilyPrefixes would silently
// stop resolving keys for the new family.
func TestSQSPartitionResolver_PrefixesAlign(t *testing.T) {
	t.Parallel()
	want := [][]byte{
		[]byte(SqsPartitionedMsgDataPrefix),
		[]byte(SqsPartitionedMsgVisPrefix),
		[]byte(SqsPartitionedMsgDedupPrefix),
		[]byte(SqsPartitionedMsgGroupPrefix),
		[]byte(SqsPartitionedMsgByAgePrefix),
	}
	require.Equal(t, want, sqsResolverFamilyPrefixes,
		"sqsResolverFamilyPrefixes must mirror the constants in "+
			"sqs_keys.go — a new partitioned family added there must "+
			"be added here too, or the resolver silently stops "+
			"matching keys in that family")
	for _, p := range sqsResolverFamilyPrefixes {
		require.True(t, strings.HasSuffix(string(p), "p|"),
			"every partitioned prefix must end with the p| discriminator")
	}
}
