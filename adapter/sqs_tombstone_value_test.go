package adapter

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEncodeQueueTombstoneValue_LegacyByteIdenticalForUnpartitioned
// pins that pre-PR-6a tombstone values are byte-identical for
// PartitionCount==0 and PartitionCount==1: a rolling upgrade that
// flips a node from pre-PR-6a to PR 6a sees no on-disk change for
// existing non-partitioned queues. Without this, every DeleteQueue
// during the rollout window would emit a new tombstone value shape
// that the pre-PR-6a binary doesn't expect (it ignores the value
// today, but a future encoding-aware check could trip on it).
func TestEncodeQueueTombstoneValue_LegacyByteIdenticalForUnpartitioned(t *testing.T) {
	t.Parallel()
	require.Equal(t, []byte{1}, encodeQueueTombstoneValue(0),
		"PartitionCount=0 must encode to the legacy single-byte sentinel")
	require.Equal(t, []byte{1}, encodeQueueTombstoneValue(1),
		"PartitionCount=1 must encode to the legacy single-byte sentinel")
}

// TestEncodeQueueTombstoneValue_PartitionedEncodesUint64BE pins the
// new encoding for PartitionCount > 1: an 8-byte big-endian uint64
// the reaper can safely Decode. Roundtrips through
// decodeQueueTombstoneValue.
func TestEncodeQueueTombstoneValue_PartitionedEncodesUint64BE(t *testing.T) {
	t.Parallel()
	for _, pc := range []uint32{2, 4, 8, 16, 32} {
		got := encodeQueueTombstoneValue(pc)
		require.Len(t, got, 8, "PartitionCount=%d must encode to 8 bytes", pc)
		require.Equal(t, uint64(pc), binary.BigEndian.Uint64(got),
			"PartitionCount=%d must encode big-endian", pc)
		require.Equal(t, pc, decodeQueueTombstoneValue(got),
			"encode/decode round-trip must preserve PartitionCount")
	}
}

// TestDecodeQueueTombstoneValue_LegacyValuesFallBackToOne pins the
// fail-safe for any value the reaper observes that doesn't match
// the canonical 8-byte shape: empty value (theoretically impossible
// since tombstones always have a non-empty marker), the legacy
// []byte{1} sentinel from pre-PR-6a writers, and any other length
// (a future encoding revision that this binary doesn't recognise)
// all degrade to PartitionCount=1 — the legacy reaper path. Without
// this, a partial rollback or mid-flight encoding change would
// silently flip the reaper to scan zero partitions and leak the
// queue's data.
func TestDecodeQueueTombstoneValue_LegacyValuesFallBackToOne(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint32(1), decodeQueueTombstoneValue(nil),
		"nil value must fall back to PartitionCount=1")
	require.Equal(t, uint32(1), decodeQueueTombstoneValue([]byte{}),
		"empty value must fall back to PartitionCount=1")
	require.Equal(t, uint32(1), decodeQueueTombstoneValue([]byte{1}),
		"legacy single-byte sentinel must decode to PartitionCount=1")
	require.Equal(t, uint32(1), decodeQueueTombstoneValue([]byte{0, 0, 0, 1}),
		"unrecognised 4-byte value must fall back to PartitionCount=1")
}

// TestDecodeQueueTombstoneValue_OutOfRangeFallsBackToOne pins the
// defensive clamp on canonical 8-byte values whose decoded count is
// outside the [1, htfifoMaxPartitions] range. PartitionCount=0 is
// canonical for "no HT-FIFO" on the read path (effectivePartitionCount
// collapses 0→1) so the reaper treats it the same way; values above
// htfifoMaxPartitions are a corruption / future-format signal — the
// reaper can't know how to iterate them safely, so it falls back to
// the legacy single-partition sweep and surfaces the corruption to
// the operator via the slow leak (better than the alternative of
// iterating bogus partitions).
func TestDecodeQueueTombstoneValue_OutOfRangeFallsBackToOne(t *testing.T) {
	t.Parallel()
	zero := make([]byte, 8)
	require.Equal(t, uint32(1), decodeQueueTombstoneValue(zero),
		"canonical encoding of 0 must decode to PartitionCount=1 (matches effectivePartitionCount)")

	tooBig := make([]byte, 8)
	binary.BigEndian.PutUint64(tooBig, uint64(htfifoMaxPartitions)+1)
	require.Equal(t, uint32(1), decodeQueueTombstoneValue(tooBig),
		"PartitionCount above htfifoMaxPartitions must fall back to 1")

	maxOK := make([]byte, 8)
	binary.BigEndian.PutUint64(maxOK, uint64(htfifoMaxPartitions))
	require.Equal(t, htfifoMaxPartitions, decodeQueueTombstoneValue(maxOK),
		"PartitionCount=htfifoMaxPartitions is in-range and must decode unchanged")
}
