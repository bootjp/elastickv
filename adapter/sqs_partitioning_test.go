package adapter

import (
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- partitionFor unit tests ---

// TestPartitionFor_LegacyZeroOrOneAlwaysPartitionZero pins the
// single-partition compatibility contract: a queue with
// PartitionCount == 0 (the unset state) or 1 routes every group ID
// to partition 0. Without this guarantee an existing single-
// partition queue would re-shuffle messages once PR 5 lands the
// data plane.
func TestPartitionFor_LegacyZeroOrOneAlwaysPartitionZero(t *testing.T) {
	t.Parallel()
	for _, count := range []uint32{0, 1} {
		meta := &sqsQueueMeta{PartitionCount: count}
		for _, gid := range []string{"a", "b", "user-1", "long-group-id-blah"} {
			require.Equal(t, uint32(0), partitionFor(meta, gid),
				"PartitionCount=%d, group=%q must route to 0", count, gid)
		}
	}
}

// TestPartitionFor_PerQueueShortCircuits pins the §3.3 short-circuit:
// FifoThroughputLimit=perQueue collapses every group ID to
// partition 0 regardless of PartitionCount. Operators who want the
// AWS attribute set without the throughput scaling depend on this.
func TestPartitionFor_PerQueueShortCircuits(t *testing.T) {
	t.Parallel()
	meta := &sqsQueueMeta{PartitionCount: 8, FifoThroughputLimit: htfifoThroughputPerQueue}
	for _, gid := range []string{"a", "b", "user-1", "long-group-id"} {
		require.Equal(t, uint32(0), partitionFor(meta, gid))
	}
}

// TestPartitionFor_EmptyMessageGroupIdRoutesZero pins the defensive
// fallback. FIFO send validation rejects empty MessageGroupId so
// this case should never reach the router; the test ensures the
// router doesn't crash if it does.
func TestPartitionFor_EmptyMessageGroupIdRoutesZero(t *testing.T) {
	t.Parallel()
	meta := &sqsQueueMeta{PartitionCount: 8}
	require.Equal(t, uint32(0), partitionFor(meta, ""))
}

// TestPartitionFor_DeterministicAcrossRuns pins the §3.3
// determinism contract: the same group ID always returns the same
// partition. Without it, a consumer that pulls from a partition by
// group ID could see messages re-routed to a different partition on
// a process restart and lose ordering guarantees.
func TestPartitionFor_DeterministicAcrossRuns(t *testing.T) {
	t.Parallel()
	meta := &sqsQueueMeta{PartitionCount: 8}
	gid := "user-1234"
	first := partitionFor(meta, gid)
	for range 100 {
		require.Equal(t, first, partitionFor(meta, gid))
	}
}

// TestPartitionFor_DistributionApproximatelyUniform pins the §9 unit
// test from the design: 100k random group IDs across 8 partitions
// must land within ±5% of equal share. FNV-1a is not a CSPRNG but
// for non-adversarial input the distribution is well-behaved.
func TestPartitionFor_DistributionApproximatelyUniform(t *testing.T) {
	t.Parallel()
	const partitions uint32 = 8
	const sample = 100_000
	meta := &sqsQueueMeta{PartitionCount: partitions}
	hits := make(map[uint32]int, partitions)
	for i := range sample {
		hits[partitionFor(meta, "group-"+strconv.Itoa(i))]++
	}
	expected := sample / int(partitions)
	tolerance := expected / 20 // ±5%
	for p := uint32(0); p < partitions; p++ {
		count := hits[p]
		if count < expected-tolerance || count > expected+tolerance {
			t.Fatalf("partition %d: %d hits, expected within ±%d of %d (full distribution: %v)",
				p, count, tolerance, expected, hits)
		}
	}
}

// TestPartitionFor_PowerOfTwoMaskingMatchesMod is a regression
// guard for the bitwise-mask optimisation in partitionFor. The
// optimisation is equivalent to `% PartitionCount` only when
// PartitionCount is a power of two — the validator enforces this
// at config time, but if a future bug leaks a non-power-of-two
// value through validation, this test will catch the distribution
// bias immediately.
func TestPartitionFor_PowerOfTwoMaskingMatchesMod(t *testing.T) {
	t.Parallel()
	for _, n := range []uint32{2, 4, 8, 16, 32} {
		meta := &sqsQueueMeta{PartitionCount: n}
		for i := range 1000 {
			gid := "g-" + strconv.Itoa(i)
			require.Less(t, partitionFor(meta, gid), n,
				"partitionFor must always be < PartitionCount=%d", n)
		}
	}
}

// --- isPowerOfTwo unit tests ---

func TestIsPowerOfTwo(t *testing.T) {
	t.Parallel()
	cases := []struct {
		n    uint32
		want bool
	}{
		{0, false},
		{1, true},
		{2, true},
		{3, false},
		{4, true},
		{7, false},
		{8, true},
		{16, true},
		{32, true},
		{33, false},
	}
	for _, tc := range cases {
		require.Equal(t, tc.want, isPowerOfTwo(tc.n), "n=%d", tc.n)
	}
}

// --- validatePartitionConfig unit tests ---

// TestValidatePartitionConfig_PowerOfTwo pins the §3.2 rule that
// PartitionCount must be a power of two. The bitwise-mask routing
// in partitionFor depends on this; non-powers would distribute
// unevenly.
func TestValidatePartitionConfig_PowerOfTwo(t *testing.T) {
	t.Parallel()
	bad := []uint32{3, 5, 6, 7, 9, 10, 12, 15}
	for _, n := range bad {
		err := validatePartitionConfig(&sqsQueueMeta{PartitionCount: n, IsFIFO: true})
		require.Error(t, err, "n=%d must reject", n)
	}
	good := []uint32{1, 2, 4, 8, 16, 32}
	for _, n := range good {
		err := validatePartitionConfig(&sqsQueueMeta{PartitionCount: n, IsFIFO: true})
		require.NoError(t, err, "n=%d must be accepted", n)
	}
}

// TestValidatePartitionConfig_RejectsAboveMax pins the §10
// per-queue cap. 64 must reject; 32 must succeed.
func TestValidatePartitionConfig_RejectsAboveMax(t *testing.T) {
	t.Parallel()
	require.Error(t, validatePartitionConfig(&sqsQueueMeta{PartitionCount: 64, IsFIFO: true}))
	require.NoError(t, validatePartitionConfig(&sqsQueueMeta{PartitionCount: 32, IsFIFO: true}))
}

// TestValidatePartitionConfig_StandardQueueRejectsHTFIFOAttrs pins
// the §3.2 FIFO-only rule: HT-FIFO attributes on a non-FIFO queue
// reject with InvalidAttributeValue. Setting them silently on a
// Standard queue would advertise unsupported behaviour.
//
// PartitionCount > 1 is also FIFO-only (Claude review on PR #681
// round 2 caught the gap) — without the guard a Standard queue
// with PartitionCount=2 would slip past the validator after PR 5
// lifts the dormancy gate. PartitionCount 0/1 are still accepted
// on Standard queues because both mean "single-partition layout".
func TestValidatePartitionConfig_StandardQueueRejectsHTFIFOAttrs(t *testing.T) {
	t.Parallel()
	require.Error(t, validatePartitionConfig(&sqsQueueMeta{IsFIFO: false, FifoThroughputLimit: htfifoThroughputPerQueue}))
	require.Error(t, validatePartitionConfig(&sqsQueueMeta{IsFIFO: false, DeduplicationScope: htfifoDedupeScopeMessageGroup}))
	for _, n := range []uint32{2, 4, 8, 16, 32} {
		require.Error(t, validatePartitionConfig(&sqsQueueMeta{IsFIFO: false, PartitionCount: n}),
			"PartitionCount=%d on Standard queue must reject", n)
	}
	require.NoError(t, validatePartitionConfig(&sqsQueueMeta{IsFIFO: false, PartitionCount: 0}))
	require.NoError(t, validatePartitionConfig(&sqsQueueMeta{IsFIFO: false, PartitionCount: 1}))
	require.NoError(t, validatePartitionConfig(&sqsQueueMeta{IsFIFO: true, FifoThroughputLimit: htfifoThroughputPerMessageGroupID, PartitionCount: 8}))
}

// TestValidatePartitionConfig_RejectsQueueScopedDedupOnPartitioned
// pins the §3.2 cross-attribute control-plane gate: queue-scoped
// dedup is incompatible with multi-partition FIFO because the dedup
// key cannot be globally unique across partitions without a cross-
// partition OCC transaction. Rejected as InvalidParameterValue at
// CreateQueue / SetQueueAttributes time so the operator sees the
// error before a single SendMessage.
func TestValidatePartitionConfig_RejectsQueueScopedDedupOnPartitioned(t *testing.T) {
	t.Parallel()
	err := validatePartitionConfig(&sqsQueueMeta{
		IsFIFO:             true,
		PartitionCount:     8,
		DeduplicationScope: htfifoDedupeScopeQueue,
	})
	require.Error(t, err)
	var apiErr *sqsAPIError
	require.True(t, errors.As(err, &apiErr), "expected sqsAPIError, got %T", err)
	require.Equal(t, sqsErrValidation, apiErr.errorType,
		"the cross-attribute rejection must use InvalidParameterValue (incoherent params, sqsErrValidation), not InvalidAttributeValue (malformed individual value)")
	// Single-partition + queue-scoped dedup is fine (legacy behaviour).
	require.NoError(t, validatePartitionConfig(&sqsQueueMeta{
		IsFIFO:             true,
		PartitionCount:     1,
		DeduplicationScope: htfifoDedupeScopeQueue,
	}))
}

// --- validatePartitionDormancyGate unit tests ---

// TestValidatePartitionDormancyGate_RejectsAboveOne pins the §11
// PR 2 dormancy gate: PartitionCount > 1 must reject until PR 5
// lifts the gate. PartitionCount 0 or 1 must pass (both are the
// legacy single-partition layout).
func TestValidatePartitionDormancyGate_RejectsAboveOne(t *testing.T) {
	t.Parallel()
	require.NoError(t, validatePartitionDormancyGate(&sqsQueueMeta{PartitionCount: 0}))
	require.NoError(t, validatePartitionDormancyGate(&sqsQueueMeta{PartitionCount: 1}))
	for _, n := range []uint32{2, 4, 8, 16, 32} {
		err := validatePartitionDormancyGate(&sqsQueueMeta{PartitionCount: n})
		require.Error(t, err, "PartitionCount=%d must reject under the dormancy gate", n)
		require.Contains(t, err.Error(), "not yet enabled",
			"the gate's reason must surface to the operator")
	}
}

// --- validatePartitionImmutability unit tests ---

// TestValidatePartitionImmutability_RejectsAnyChange pins the §3.2
// immutability rule: SetQueueAttributes attempts to change any of
// the three immutable HT-FIFO fields reject with
// InvalidAttributeValue.
func TestValidatePartitionImmutability_RejectsAnyChange(t *testing.T) {
	t.Parallel()
	current := &sqsQueueMeta{
		PartitionCount:      8,
		FifoThroughputLimit: htfifoThroughputPerMessageGroupID,
		DeduplicationScope:  htfifoDedupeScopeMessageGroup,
	}
	cases := []struct {
		name      string
		mutate    func(*sqsQueueMeta)
		mustError bool
	}{
		{"PartitionCount changed", func(m *sqsQueueMeta) { m.PartitionCount = 4 }, true},
		{"FifoThroughputLimit changed", func(m *sqsQueueMeta) { m.FifoThroughputLimit = htfifoThroughputPerQueue }, true},
		{"DeduplicationScope changed", func(m *sqsQueueMeta) { m.DeduplicationScope = htfifoDedupeScopeQueue }, true},
		{"no immutable change (same-value no-op)", func(m *sqsQueueMeta) {}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := *current
			tc.mutate(&req)
			err := validatePartitionImmutability(current, &req)
			if tc.mustError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidatePartitionImmutability_PartitionCountZeroAndOneEquivalent
// pins the Claude Low fix on PR #679 round 6.2 / 6.3. The on-disk
// PartitionCount=0 ("unset") is canonical-equivalent to an explicit
// PartitionCount=1 ("single partition"), so a SetQueueAttributes
// that reaffirms the default ought to be a no-op rather than a hard
// "PartitionCount is immutable" rejection. validatePartitionImmutability
// uses normalisePartitionCount on both sides for exactly this case.
func TestValidatePartitionImmutability_PartitionCountZeroAndOneEquivalent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		current uint32
		req     uint32
		wantErr bool
	}{
		{"stored 0, requested 1 (no-op)", 0, 1, false},
		{"stored 1, requested 0 (no-op)", 1, 0, false},
		{"stored 0, requested 0 (no-op)", 0, 0, false},
		{"stored 1, requested 1 (no-op)", 1, 1, false},
		{"stored 0, requested 2 (real change)", 0, 2, true},
		{"stored 1, requested 2 (real change)", 1, 2, true},
		{"stored 2, requested 1 (real change)", 2, 1, true},
		{"stored 2, requested 0 (real change)", 2, 0, true},
		{"stored 4, requested 8 (real change)", 4, 8, true},
		{"stored 8, requested 8 (no-op)", 8, 8, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cur := &sqsQueueMeta{PartitionCount: tc.current}
			req := &sqsQueueMeta{PartitionCount: tc.req}
			err := validatePartitionImmutability(cur, req)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// --- htfifoAttributesPresent ---

func TestHTFIFOAttributesPresent(t *testing.T) {
	t.Parallel()
	require.False(t, htfifoAttributesPresent(map[string]string{}))
	require.False(t, htfifoAttributesPresent(map[string]string{"VisibilityTimeout": "30"}))
	require.True(t, htfifoAttributesPresent(map[string]string{"PartitionCount": "8"}))
	require.True(t, htfifoAttributesPresent(map[string]string{"FifoThroughputLimit": htfifoThroughputPerMessageGroupID}))
	require.True(t, htfifoAttributesPresent(map[string]string{"DeduplicationScope": htfifoDedupeScopeMessageGroup}))
}

// TestHTFIFOAttributesEqual_PartitionCountZeroAndOneEquivalent pins
// the Codex P2 fix on PR #679 round 6.1: validatePartitionConfig
// documents PartitionCount=0 (unset) and =1 (explicit single
// partition) as semantically identical legacy/single-partition
// routing, so CreateQueue idempotency must treat them as equal —
// otherwise a queue created without PartitionCount (stored as 0) is
// rejected as "different attributes" by a retry that explicitly
// passes PartitionCount=1.
func TestHTFIFOAttributesEqual_PartitionCountZeroAndOneEquivalent(t *testing.T) {
	t.Parallel()
	a := &sqsQueueMeta{PartitionCount: 0}
	b := &sqsQueueMeta{PartitionCount: 1}
	require.True(t, htfifoAttributesEqual(a, b),
		"PartitionCount 0 (unset) and 1 (explicit single partition) must compare equal")
	require.True(t, htfifoAttributesEqual(b, a),
		"equality must be symmetric")
	// Real divergence (>1 vs 0/1) still rejects.
	c := &sqsQueueMeta{PartitionCount: 2}
	require.False(t, htfifoAttributesEqual(a, c),
		"PartitionCount=2 must differ from unset")
	require.False(t, htfifoAttributesEqual(b, c),
		"PartitionCount=2 must differ from explicit 1")
	// Same > 1 value still equal.
	d := &sqsQueueMeta{PartitionCount: 2}
	require.True(t, htfifoAttributesEqual(c, d),
		"identical PartitionCount > 1 must compare equal")
}

func TestNormalisePartitionCount(t *testing.T) {
	t.Parallel()
	require.Equal(t, uint32(1), normalisePartitionCount(0))
	require.Equal(t, uint32(1), normalisePartitionCount(1))
	require.Equal(t, uint32(2), normalisePartitionCount(2))
	require.Equal(t, uint32(8), normalisePartitionCount(8))
}
