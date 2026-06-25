package adapter

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Tests for the PR 5b-2 dispatch helpers added on top of the PR 5b-1
// per-key wrappers: encodeReceiptHandleDispatch (handle wire format
// dispatch), validateReceiptHandleVersion (queue-aware version check
// that replaces PR 5a's blanket rejection), and
// sqsMsgVisScanBoundsDispatch (per-partition scan bounds).

const (
	dispatchTestMsgIDHex = "deadbeefdeadbeefdeadbeefdeadbeef"
	dispatchTestQueue    = "orders.fifo"
)

// TestEncodeReceiptHandleDispatch_PicksVersionByPartitionCount pins
// the wire-format dispatch decision: meta.PartitionCount > 1 → v2,
// otherwise v1. The partition argument is only consulted on the v2
// branch (a v1 handle has no partition field).
func TestEncodeReceiptHandleDispatch_PicksVersionByPartitionCount(t *testing.T) {
	t.Parallel()
	token := make([]byte, sqsReceiptTokenBytes)

	// Legacy queue: nil meta → v1.
	h, err := encodeReceiptHandleDispatch(nil, 0, 7, dispatchTestMsgIDHex, token)
	require.NoError(t, err)
	parsed, err := decodeReceiptHandle(h)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleVersion1, parsed.Version,
		"nil meta must dispatch to v1 — the legacy single-partition layout")

	// PartitionCount = 0 → v1 (canonical legacy / unset).
	h, err = encodeReceiptHandleDispatch(&sqsQueueMeta{PartitionCount: 0}, 0, 7, dispatchTestMsgIDHex, token)
	require.NoError(t, err)
	parsed, err = decodeReceiptHandle(h)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleVersion1, parsed.Version)

	// PartitionCount = 1 → v1.
	h, err = encodeReceiptHandleDispatch(&sqsQueueMeta{PartitionCount: 1}, 0, 7, dispatchTestMsgIDHex, token)
	require.NoError(t, err)
	parsed, err = decodeReceiptHandle(h)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleVersion1, parsed.Version)

	// PartitionCount = 4 → v2; partition is preserved.
	h, err = encodeReceiptHandleDispatch(&sqsQueueMeta{PartitionCount: 4}, 3, 7, dispatchTestMsgIDHex, token)
	require.NoError(t, err)
	parsed, err = decodeReceiptHandle(h)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleVersion2, parsed.Version)
	require.Equal(t, uint32(3), parsed.Partition)
	require.Equal(t, uint64(7), parsed.QueueGeneration)
}

// TestEncodeReceiptHandleDispatch_LegacyByteIdenticalToV1 protects
// the byte-identical-output guarantee on legacy queues: the
// dispatch helper's output must equal what encodeReceiptHandle
// would produce directly. Without this, a future refactor that
// added a v1.5 layout via the dispatch path could change the wire
// format on existing deployments without anyone noticing in the
// test suite.
func TestEncodeReceiptHandleDispatch_LegacyByteIdenticalToV1(t *testing.T) {
	t.Parallel()
	token := bytes.Repeat([]byte{0xAB}, sqsReceiptTokenBytes)
	want, err := encodeReceiptHandle(7, dispatchTestMsgIDHex, token)
	require.NoError(t, err)

	for _, meta := range []*sqsQueueMeta{nil, {PartitionCount: 0}, {PartitionCount: 1}} {
		got, err := encodeReceiptHandleDispatch(meta, 0, 7, dispatchTestMsgIDHex, token)
		require.NoError(t, err)
		require.Equal(t, want, got,
			"legacy dispatch (PartitionCount=%d) must be byte-identical to encodeReceiptHandle",
			func() uint32 {
				if meta == nil {
					return 0
				}
				return meta.PartitionCount
			}())
	}
}

// TestValidateReceiptHandleVersion_QueueAwareRules covers the four
// (handle.Version, meta.PartitionCount) cells of the PR 5b-2
// version-validation matrix. The PR 5a blanket rejection moved
// here from decodeClientReceiptHandle, so this test pins the
// dormancy guarantee under the new contract.
func TestValidateReceiptHandleVersion_QueueAwareRules(t *testing.T) {
	t.Parallel()
	v1Handle := &decodedReceiptHandle{Version: sqsReceiptHandleVersion1}
	v2Handle := &decodedReceiptHandle{Version: sqsReceiptHandleVersion2, Partition: 3}

	cases := []struct {
		name      string
		meta      *sqsQueueMeta
		handle    *decodedReceiptHandle
		wantError bool
	}{
		{"nil_meta_v1", nil, v1Handle, false},
		{"nil_meta_v2", nil, v2Handle, true},
		{"legacy_count0_v1", &sqsQueueMeta{PartitionCount: 0}, v1Handle, false},
		{"legacy_count0_v2", &sqsQueueMeta{PartitionCount: 0}, v2Handle, true},
		{"legacy_count1_v1", &sqsQueueMeta{PartitionCount: 1}, v1Handle, false},
		{"legacy_count1_v2", &sqsQueueMeta{PartitionCount: 1}, v2Handle, true},
		{"partitioned4_v1", &sqsQueueMeta{PartitionCount: 4}, v1Handle, true},
		{"partitioned4_v2", &sqsQueueMeta{PartitionCount: 4}, v2Handle, false},
		// perQueue throughput on PartitionCount=4 still requires v2 on
		// the wire — partitionFor collapses every group to partition
		// 0, but the keyspace is still partitioned, and a v1 handle
		// would route through the legacy data-key constructor.
		{"partitioned4_perQueue_v1", &sqsQueueMeta{PartitionCount: 4, FifoThroughputLimit: htfifoThroughputPerQueue}, v1Handle, true},
		{"partitioned4_perQueue_v2", &sqsQueueMeta{PartitionCount: 4, FifoThroughputLimit: htfifoThroughputPerQueue}, v2Handle, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := validateReceiptHandleVersion(tc.meta, tc.handle)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}

// TestValidateReceiptHandleVersion_NilHandle pins the defensive nil
// branch so a typed-nil mistake does not silently pass.
func TestValidateReceiptHandleVersion_NilHandle(t *testing.T) {
	t.Parallel()
	err := validateReceiptHandleVersion(&sqsQueueMeta{PartitionCount: 4}, nil)
	require.Error(t, err)
}

// TestValidateReceiptHandleVersion_RejectsOutOfRangePartition pins
// the round-2 fix: handle.Partition is client-controlled once
// decodeClientReceiptHandle accepts v2, so the queue-aware validator
// must bounds-check it against meta.PartitionCount. Without this an
// out-of-range partition would fall through to sqsMsg*KeyDispatch
// and depend on downstream routing failure semantics rather than
// returning ReceiptHandleIsInvalid at the validation choke point.
func TestValidateReceiptHandleVersion_RejectsOutOfRangePartition(t *testing.T) {
	t.Parallel()
	meta := &sqsQueueMeta{PartitionCount: 4}
	for _, partition := range []uint32{4, 5, 17, 1 << 30} {
		err := validateReceiptHandleVersion(meta, &decodedReceiptHandle{
			Version:   sqsReceiptHandleVersion2,
			Partition: partition,
		})
		require.Error(t, err, "partition=%d on PartitionCount=4 must be rejected", partition)
		require.True(t,
			strings.Contains(err.Error(), "out of range"),
			"error must reference out-of-range partition, got %v", err)
	}
	// Boundary: partition == count-1 is the last legal value.
	err := validateReceiptHandleVersion(meta, &decodedReceiptHandle{
		Version:   sqsReceiptHandleVersion2,
		Partition: 3,
	})
	require.NoError(t, err)
}

// TestSQSMsgVisScanBoundsDispatch_LegacyMatchesLegacy pins that on
// legacy / non-partitioned metas the dispatch helper produces the
// same start/end pair as the legacy sqsMsgVisScanBounds — the
// receive fanout's byte-identical guarantee on the steady-state
// path.
func TestSQSMsgVisScanBoundsDispatch_LegacyMatchesLegacy(t *testing.T) {
	t.Parallel()
	const gen uint64 = 7
	const maxTS int64 = 1_700_000_000_000

	wantStart, wantEnd := sqsMsgVisScanBounds(dispatchTestQueue, gen, maxTS)

	for _, meta := range []*sqsQueueMeta{nil, {PartitionCount: 0}, {PartitionCount: 1}} {
		gotStart, gotEnd := sqsMsgVisScanBoundsDispatch(meta, dispatchTestQueue, 0, gen, maxTS)
		require.Equal(t, wantStart, gotStart, "start must match legacy sqsMsgVisScanBounds")
		require.Equal(t, wantEnd, gotEnd, "end must match legacy sqsMsgVisScanBounds")
	}
}

// TestSQSMsgVisScanBoundsDispatch_PartitionedUsesPartitionedPrefix
// pins that on a partitioned queue the bounds carry the
// partitioned vis prefix — different partitions yield disjoint
// ranges, which is what makes the receive fanout safe (no scan
// overlap, no cross-partition leakage).
func TestSQSMsgVisScanBoundsDispatch_PartitionedUsesPartitionedPrefix(t *testing.T) {
	t.Parallel()
	const gen uint64 = 7
	const maxTS int64 = 1_700_000_000_000
	meta := &sqsQueueMeta{PartitionCount: 4}

	startP0, endP0 := sqsMsgVisScanBoundsDispatch(meta, dispatchTestQueue, 0, gen, maxTS)
	startP1, endP1 := sqsMsgVisScanBoundsDispatch(meta, dispatchTestQueue, 1, gen, maxTS)

	require.NotEqual(t, startP0, startP1,
		"different partitions must produce disjoint scan ranges")
	require.Equal(t, sqsPartitionedMsgVisPrefixForQueue(dispatchTestQueue, 0, gen),
		startP0[:len(sqsPartitionedMsgVisPrefixForQueue(dispatchTestQueue, 0, gen))],
		"partition 0 start must be prefixed by sqsPartitionedMsgVisPrefixForQueue(_, 0, _)")
	require.Equal(t, sqsPartitionedMsgVisPrefixForQueue(dispatchTestQueue, 1, gen),
		startP1[:len(sqsPartitionedMsgVisPrefixForQueue(dispatchTestQueue, 1, gen))],
		"partition 1 start must be prefixed by sqsPartitionedMsgVisPrefixForQueue(_, 1, _)")
	require.True(t, bytes.HasPrefix(startP0, []byte(SqsMsgVisPrefix)),
		"partitioned start key still begins with the SqsMsgVisPrefix marker")
	require.True(t, bytes.HasPrefix(endP0, []byte(SqsMsgVisPrefix)),
		"partitioned end key still begins with the SqsMsgVisPrefix marker")
	require.True(t, bytes.Compare(startP0, endP0) < 0,
		"start must compare strictly less than end")
	require.True(t, bytes.Compare(startP1, endP1) < 0,
		"start must compare strictly less than end")
}

// TestSQSMsgVisScanBoundsDispatch_PerQueueOnPartitionedKeyspace
// pins the PR 731 round 2 forward note: when meta.PartitionCount=4
// and FifoThroughputLimit=perQueue, partitionFor collapses every
// group to partition 0, effectivePartitionCount returns 1, but the
// partition-0 vis prefix is still the partitioned-keyspace one
// (not legacy) because dispatch is keyed on PartitionCount > 1.
//
// Without this invariant a perQueue queue would write to the
// partitioned vis prefix at partition 0 (under sendFifoMessage's
// dispatch helpers) but the receive fanout — collapsing to a
// single iteration — would scan the legacy prefix and miss
// everything.
func TestSQSMsgVisScanBoundsDispatch_PerQueueOnPartitionedKeyspace(t *testing.T) {
	t.Parallel()
	const gen uint64 = 7
	const maxTS int64 = 1_700_000_000_000
	meta := &sqsQueueMeta{
		PartitionCount:      4,
		FifoThroughputLimit: htfifoThroughputPerQueue,
	}

	// effectivePartitionCount collapses to 1 (perQueue short-circuit).
	require.Equal(t, uint32(1), effectivePartitionCount(meta))

	// But the partition-0 scan bounds still use the partitioned
	// prefix — the dispatch helpers route by PartitionCount, not
	// by the throughput-limit short-circuit.
	gotStart, gotEnd := sqsMsgVisScanBoundsDispatch(meta, dispatchTestQueue, 0, gen, maxTS)
	wantPrefix := sqsPartitionedMsgVisPrefixForQueue(dispatchTestQueue, 0, gen)
	require.True(t, bytes.HasPrefix(gotStart, wantPrefix),
		"perQueue + PartitionCount=4 must scan the partitioned "+
			"prefix at partition 0 (where send writes), not the "+
			"legacy prefix — without this the fanout misses everything")
	require.True(t, bytes.HasPrefix(gotEnd, wantPrefix))

	// And the legacy bounds for this same (queue, gen, maxTS) must
	// be byte-distinct from the partitioned partition-0 bounds, so
	// a future refactor that accidentally read the legacy prefix
	// here would be caught by direct byte comparison.
	legacyStart, _ := sqsMsgVisScanBounds(dispatchTestQueue, gen, maxTS)
	require.NotEqual(t, legacyStart, gotStart,
		"perQueue + PartitionCount=4 must NOT collapse to the "+
			"legacy keyspace — that would silently strand send writes")
}

// TestEncodeReceiptHandleDispatch_PerQueueUsesV2 pins that perQueue
// + PartitionCount=4 still produces v2 handles (because the
// keyspace is partitioned). The handle records the partition the
// message was actually stored under — partitionFor returns 0 in
// perQueue mode, so every handle carries Partition=0, and the
// later DeleteMessage / ChangeMessageVisibility routes back to
// partition 0 of the partitioned keyspace.
func TestEncodeReceiptHandleDispatch_PerQueueUsesV2(t *testing.T) {
	t.Parallel()
	token := make([]byte, sqsReceiptTokenBytes)
	meta := &sqsQueueMeta{
		PartitionCount:      4,
		FifoThroughputLimit: htfifoThroughputPerQueue,
	}
	h, err := encodeReceiptHandleDispatch(meta, 0, 7, dispatchTestMsgIDHex, token)
	require.NoError(t, err)
	parsed, err := decodeReceiptHandle(h)
	require.NoError(t, err)
	require.Equal(t, sqsReceiptHandleVersion2, parsed.Version,
		"perQueue + PartitionCount=4 must use the v2 wire format — "+
			"the partition the send wrote to is meaningful even "+
			"when partitionFor collapses every group to partition 0")
	require.Equal(t, uint32(0), parsed.Partition)
}

// TestValidateReceiptHandleVersion_RejectsV2OnNonPartitioned is a
// named regression for the dormancy guarantee under the new
// contract. PR 5a rejected v2 at the public API; PR 5b-2 moves
// that to validateReceiptHandleVersion against the loaded meta.
// In PR 5b-2 the §11 PR 2 dormancy gate still rejects
// PartitionCount > 1 at CreateQueue, so every queue is
// non-partitioned, so every v2 handle still surfaces as
// ReceiptHandleIsInvalid downstream — exactly the PR 5a
// observable behaviour.
func TestValidateReceiptHandleVersion_RejectsV2OnNonPartitioned(t *testing.T) {
	t.Parallel()
	v2 := &decodedReceiptHandle{Version: sqsReceiptHandleVersion2, Partition: 3}
	for _, meta := range []*sqsQueueMeta{nil, {PartitionCount: 0}, {PartitionCount: 1}} {
		err := validateReceiptHandleVersion(meta, v2)
		require.Error(t, err,
			"v2 handle on a non-partitioned queue must be rejected")
		require.True(t, strings.Contains(err.Error(), "version"),
			"error message should reference version mismatch")
	}
}
