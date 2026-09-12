package s3keys_test

import (
	"bytes"
	"testing"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

const planCommitTS = uint64(1_700_000_000_000) << 16

// findMutation returns the planned mutation for key, if any.
func findMutation(t *testing.T, plan []s3keys.ChunkRefRCMutation, key []byte) (s3keys.ChunkRefRCMutation, bool) {
	t.Helper()
	for _, m := range plan {
		if bytes.Equal(m.Key, key) {
			return m, true
		}
	}
	return s3keys.ChunkRefRCMutation{}, false
}

func requireRCValue(t *testing.T, plan []s3keys.ChunkRefRCMutation, sha [32]byte, want s3keys.ChunkRefRC) {
	t.Helper()
	m, ok := findMutation(t, plan, s3keys.ChunkRefRCKey(sha))
	require.True(t, ok, "plan must write the reference-count record")
	require.False(t, m.Delete)
	got, ok := s3keys.DecodeChunkRefRC(m.Value)
	require.True(t, ok)
	require.Equal(t, want, got)
}

// TestPlanFirstReferenceWritesCountWithoutQueueing covers the ordinary
// upload: a blob becomes reachable, so nothing is queued.
func TestPlanFirstReferenceWritesCountWithoutQueueing(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	plan, err := s3keys.PlanChunkRefRCMutations(
		[]s3keys.ChunkRefDelta{{ContentSHA256: sha, Added: 1}}, nil, planCommitTS)
	require.NoError(t, err)
	require.Len(t, plan, 1, "a first reference touches only the count")
	requireRCValue(t, plan, sha, s3keys.ChunkRefRC{Count: 1})
}

// TestPlanLastReferenceRemovalQueuesWithTheCommitTimestamp is the
// eligibility half of §3.5: the same txn that drives the count to zero
// must record WHEN, because a counter resting at zero carries no time
// signal and the grace window would be unimplementable.
func TestPlanLastReferenceRemovalQueuesWithTheCommitTimestamp(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	plan, err := s3keys.PlanChunkRefRCMutations(
		[]s3keys.ChunkRefDelta{{ContentSHA256: sha, Removed: 1}},
		map[[32]byte]s3keys.ChunkRefRC{sha: {Count: 1}},
		planCommitTS)
	require.NoError(t, err)

	queueKey := s3keys.ChunkBlobGCQueueKey(planCommitTS, sha)
	q, ok := findMutation(t, plan, queueKey)
	require.True(t, ok, "dropping to zero must queue the blob")
	require.False(t, q.Delete)

	requireRCValue(t, plan, sha, s3keys.ChunkRefRC{Count: 0, QueuedAtTS: planCommitTS})
}

// TestPlanReReferenceDeletesTheExistingQueueEntry is the clause that
// forced the timestamp into the RC value: §3.5 requires the queue to
// reflect *currently* RC==0, not *ever was* zero, so a txn that makes a
// blob reachable again must remove the entry in the same txn — which it
// can only name because the record carries the timestamp.
func TestPlanReReferenceDeletesTheExistingQueueEntry(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	const queuedAt = uint64(1_699_000_000_000) << 16

	plan, err := s3keys.PlanChunkRefRCMutations(
		[]s3keys.ChunkRefDelta{{ContentSHA256: sha, Added: 1}},
		map[[32]byte]s3keys.ChunkRefRC{sha: {Count: 0, QueuedAtTS: queuedAt}},
		planCommitTS)
	require.NoError(t, err)

	del, ok := findMutation(t, plan, s3keys.ChunkBlobGCQueueKey(queuedAt, sha))
	require.True(t, ok, "re-referencing must delete the stale queue entry")
	require.True(t, del.Delete)
	require.Nil(t, del.Value)

	// The count record no longer claims a queue entry.
	requireRCValue(t, plan, sha, s3keys.ChunkRefRC{Count: 1})
}

// TestPlanKeepsTheOriginalEligibilityTimestamp pins that a blob which
// is already queued and stays at zero does NOT get restamped.
// Restamping would silently restart a grace period that was already
// running, so a blob could never age out under repeated no-op txns.
func TestPlanKeepsTheOriginalEligibilityTimestamp(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	const queuedAt = uint64(1_699_000_000_000) << 16

	// A txn that adds and removes one reference: nets to zero, and the
	// blob was already queued.
	plan, err := s3keys.PlanChunkRefRCMutations(
		[]s3keys.ChunkRefDelta{{ContentSHA256: sha, Added: 1, Removed: 1}},
		map[[32]byte]s3keys.ChunkRefRC{sha: {Count: 0, QueuedAtTS: queuedAt}},
		planCommitTS)
	require.NoError(t, err)

	requireRCValue(t, plan, sha, s3keys.ChunkRefRC{Count: 0, QueuedAtTS: queuedAt})
	_, requeued := findMutation(t, plan, s3keys.ChunkBlobGCQueueKey(planCommitTS, sha))
	require.False(t, requeued, "an already-queued blob must keep its original timestamp")
}

// TestPlanUnderflowFailsClosed pins that a decrement below zero fails
// the txn instead of clamping. Clamping would queue a blob for deletion
// on the strength of a bookkeeping disagreement — a correctness bug
// dressed up as a space reclaim.
func TestPlanUnderflowFailsClosed(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	tests := []struct {
		name    string
		current map[[32]byte]s3keys.ChunkRefRC
		delta   s3keys.ChunkRefDelta
	}{
		{
			name:    "no record at all",
			current: nil,
			delta:   s3keys.ChunkRefDelta{ContentSHA256: sha, Removed: 1},
		},
		{
			name:    "removing more than held",
			current: map[[32]byte]s3keys.ChunkRefRC{sha: {Count: 2}},
			delta:   s3keys.ChunkRefDelta{ContentSHA256: sha, Removed: 3},
		},
		{
			name:    "adds do not cover removes",
			current: map[[32]byte]s3keys.ChunkRefRC{sha: {Count: 1}},
			delta:   s3keys.ChunkRefDelta{ContentSHA256: sha, Added: 1, Removed: 3},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := s3keys.PlanChunkRefRCMutations(
				[]s3keys.ChunkRefDelta{tc.delta}, tc.current, planCommitTS)
			require.Error(t, err)
			require.True(t, errors.Is(err, s3keys.ErrChunkRefRCUnderflow))
		})
	}
}

// TestPlanNetZeroDeltaIsANoOp pins that a txn which neither adds nor
// removes references leaves the record alone — including its queue
// state, since reachability did not change.
func TestPlanNetZeroDeltaIsANoOp(t *testing.T) {
	t.Parallel()

	sha := testSHA("payload")
	plan, err := s3keys.PlanChunkRefRCMutations(
		[]s3keys.ChunkRefDelta{{ContentSHA256: sha}},
		map[[32]byte]s3keys.ChunkRefRC{sha: {Count: 3}},
		planCommitTS)
	require.NoError(t, err)
	require.Empty(t, plan)
}

// TestPlanHandlesDedupAcrossMultipleSHAsInOneTxn covers a multipart
// upload touching several chunks at once: each SHA is planned
// independently and a drop to zero for one must not affect another.
func TestPlanHandlesDedupAcrossMultipleSHAsInOneTxn(t *testing.T) {
	t.Parallel()

	keep := testSHA("still-referenced")
	drop := testSHA("about-to-be-orphaned")

	plan, err := s3keys.PlanChunkRefRCMutations([]s3keys.ChunkRefDelta{
		{ContentSHA256: keep, Added: 1},
		{ContentSHA256: drop, Removed: 1},
	}, map[[32]byte]s3keys.ChunkRefRC{
		keep: {Count: 1},
		drop: {Count: 1},
	}, planCommitTS)
	require.NoError(t, err)

	requireRCValue(t, plan, keep, s3keys.ChunkRefRC{Count: 2})
	requireRCValue(t, plan, drop, s3keys.ChunkRefRC{Count: 0, QueuedAtTS: planCommitTS})

	_, keepQueued := findMutation(t, plan, s3keys.ChunkBlobGCQueueKey(planCommitTS, keep))
	require.False(t, keepQueued, "a still-referenced blob must never be queued")
	_, dropQueued := findMutation(t, plan, s3keys.ChunkBlobGCQueueKey(planCommitTS, drop))
	require.True(t, dropQueued)
}

func TestPlanRequiresACommitTimestamp(t *testing.T) {
	t.Parallel()

	_, err := s3keys.PlanChunkRefRCMutations(
		[]s3keys.ChunkRefDelta{{ContentSHA256: testSHA("payload"), Added: 1}}, nil, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, s3keys.ErrInvalidChunkRefPlan))
}
