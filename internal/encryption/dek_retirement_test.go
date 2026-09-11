package encryption_test

import (
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var retireMembers = []string{"n1", "n2", "n3"}

func readyStorage(node string, minRetained uint64) encryption.StorageRetirementReport {
	return encryption.StorageRetirementReport{
		NodeID:                node,
		RewriteCursorComplete: true,
		ValuesPerDEK:          0,
		MinRetainedTS:         minRetained,
	}
}

func readyRaft(node string, compact, snapshot uint64) encryption.RaftRetirementReport {
	return encryption.RaftRetirementReport{
		NodeID:          node,
		LogCompactIndex: compact,
		SnapshotIndex:   snapshot,
	}
}

// ---------------------------------------------------------------------------
// Storage purpose
// ---------------------------------------------------------------------------

func TestStorageRetirementEligibleWhenEveryCriterionHolds(t *testing.T) {
	t.Parallel()

	d, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", 200), readyStorage("n2", 200), readyStorage("n3", 200),
		}, 100)
	require.NoError(t, err)
	require.True(t, d.Eligible)
	require.Empty(t, d.Blockers)
	require.NoError(t, d.Err(encryption.RetirementPurposeStorage))
}

// TestStorageRetirementRefusesWhileAnyNodeStillHoldsValues pins the
// per-node nature of the criterion: one lagging replica is enough,
// because unloading the DEK makes ITS data unreadable.
func TestStorageRetirementRefusesWhileAnyNodeStillHoldsValues(t *testing.T) {
	t.Parallel()

	behind := readyStorage("n2", 200)
	behind.ValuesPerDEK = 7

	d, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", 200), behind, readyStorage("n3", 200),
		}, 100)
	require.NoError(t, err)
	require.False(t, d.Eligible)
	require.Len(t, d.Blockers, 1)
	require.Contains(t, d.Blockers[0], "n2")
	require.True(t, errors.Is(d.Err(encryption.RetirementPurposeStorage),
		encryption.ErrDEKStillReferenced))
}

func TestStorageRetirementRefusesWhileTheRewriteIsIncomplete(t *testing.T) {
	t.Parallel()

	partial := readyStorage("n3", 200)
	partial.RewriteCursorComplete = false

	d, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", 200), readyStorage("n2", 200), partial,
		}, 100)
	require.NoError(t, err)
	require.False(t, d.Eligible)
}

// TestStorageRetirementRequiresMinRetainedTSStrictlyPastTheCommitTS is
// the boundary §5.4 states as "greater than". Equality still admits a
// snapshot read at exactly that version, so it must not be eligible.
func TestStorageRetirementRequiresMinRetainedTSStrictlyPastTheCommitTS(t *testing.T) {
	t.Parallel()

	const largestCommitTS = uint64(100)

	equal, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", largestCommitTS),
			readyStorage("n2", largestCommitTS),
			readyStorage("n3", largestCommitTS),
		}, largestCommitTS)
	require.NoError(t, err)
	require.False(t, equal.Eligible,
		"minRetainedTS equal to the commit_ts still admits a read at that version")

	past, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", largestCommitTS+1),
			readyStorage("n2", largestCommitTS+1),
			readyStorage("n3", largestCommitTS+1),
		}, largestCommitTS)
	require.NoError(t, err)
	require.True(t, past.Eligible)
}

// ---------------------------------------------------------------------------
// Raft purpose
// ---------------------------------------------------------------------------

func TestRaftRetirementEligibleWhenWALAndSnapshotsHavePassed(t *testing.T) {
	t.Parallel()

	d, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", 500, 400), readyRaft("n2", 500, 400), readyRaft("n3", 500, 400),
		}, 300, 350)
	require.NoError(t, err)
	require.True(t, d.Eligible)
	require.NoError(t, d.Err(encryption.RetirementPurposeRaft))
}

// TestRaftRetirementRefusesWhileTheWALStillHoldsEntries is the failure
// mode §5.4 names: unloading the DEK while the WAL still references it
// causes unknown_key_id apply failures on the next restart or on a
// lagging follower's catch-up.
func TestRaftRetirementRefusesWhileTheWALStillHoldsEntries(t *testing.T) {
	t.Parallel()

	lagging := readyRaft("n2", 250, 400) // compact index below the proposed index

	d, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", 500, 400), lagging, readyRaft("n3", 500, 400),
		}, 300, 350)
	require.NoError(t, err)
	require.False(t, d.Eligible)
	require.True(t, errors.Is(d.Err(encryption.RetirementPurposeRaft),
		encryption.ErrRaftDEKWALStillReferences),
		"the raft path has its own sentinel so the runbook points at the WAL, not the rewrite")
}

// TestRaftRetirementRequiresTheCompactIndexStrictlyPast pins §5.4's
// "strictly greater than": an index EQUAL to the largest proposed one
// means that entry is still un-truncated and would be replayed.
func TestRaftRetirementRequiresTheCompactIndexStrictlyPast(t *testing.T) {
	t.Parallel()

	const proposed = uint64(300)

	equal, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", proposed, 400), readyRaft("n2", proposed, 400), readyRaft("n3", proposed, 400),
		}, proposed, 350)
	require.NoError(t, err)
	require.False(t, equal.Eligible)

	past, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", proposed+1, 400), readyRaft("n2", proposed+1, 400), readyRaft("n3", proposed+1, 400),
		}, proposed, 350)
	require.NoError(t, err)
	require.True(t, past.Eligible)
}

// TestRaftRetirementRefusesASnapshotPredatingTheRotation covers the
// second raft criterion: a node restored from an older snapshot would
// replay entries that still need the retiring DEK.
func TestRaftRetirementRefusesASnapshotPredatingTheRotation(t *testing.T) {
	t.Parallel()

	stale := readyRaft("n3", 500, 100) // snapshot taken before the rotation at 350

	d, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", 500, 400), readyRaft("n2", 500, 400), stale,
		}, 300, 350)
	require.NoError(t, err)
	require.False(t, d.Eligible)
	require.Contains(t, d.Blockers[0], "n3")
}

// ---------------------------------------------------------------------------
// Cluster-wide coverage — a missing node is not a passing node
// ---------------------------------------------------------------------------

func TestRetirementRefusesAPartialReport(t *testing.T) {
	t.Parallel()

	// n3 never reported. Treating silence as success is how a single
	// unreachable replica ends up replaying against an unloaded DEK.
	_, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", 200), readyStorage("n2", 200),
		}, 100)
	require.Error(t, err)
	require.True(t, errors.Is(err, encryption.ErrIncompleteRetirementReport))
	require.ErrorContains(t, err, "n3")

	_, err = encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", 500, 400), readyRaft("n2", 500, 400),
		}, 300, 350)
	require.Error(t, err)
	require.True(t, errors.Is(err, encryption.ErrIncompleteRetirementReport))
}

// TestRetirementRefusesAnEmptyMembership guards the degenerate case:
// "no member reported a problem" is not evidence when nobody was asked.
func TestRetirementRefusesAnEmptyMembership(t *testing.T) {
	t.Parallel()

	_, err := encryption.ClassifyStorageDEKRetirement(nil, nil, 100)
	require.True(t, errors.Is(err, encryption.ErrIncompleteRetirementReport))

	_, err = encryption.ClassifyRaftDEKRetirement(nil, nil, 300, 350)
	require.True(t, errors.Is(err, encryption.ErrIncompleteRetirementReport))
}

// TestRetirementBlockersNameEveryOffendingNode keeps the operator
// output actionable: being told only "not ready" leaves them polling a
// cluster with no idea which replica to wait on.
func TestRetirementBlockersNameEveryOffendingNode(t *testing.T) {
	t.Parallel()

	bad1 := readyStorage("n1", 200)
	bad1.ValuesPerDEK = 3
	bad3 := readyStorage("n3", 200)
	bad3.RewriteCursorComplete = false

	d, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{bad1, readyStorage("n2", 200), bad3}, 100)
	require.NoError(t, err)
	require.False(t, d.Eligible)
	require.Len(t, d.Blockers, 2)
	joined := d.Blockers[0] + d.Blockers[1]
	require.Contains(t, joined, "n1")
	require.Contains(t, joined, "n3")
}

// TestRaftRetirementStaysReachableAcrossSuccessiveRotations is the
// regression test for the criterion's signal.
//
// §5.4's parenthetical says the snapshot check should read the FSM
// snapshot header's raft_envelope_cutover_index. That value is the
// one-shot Phase-2 enablement index: applier.go preserves the original
// across every later rotation and kv/fsm.go copies the unchanged value
// into each new snapshot header, so it stays frozen at the enablement
// index forever. Sourcing the criterion from it makes every
// post-cutover rotation permanently ineligible — no raft DEK could ever
// be retired, which defeats the whole classifier.
//
// SnapshotIndex is the snapshot's own Raft index, so it advances with
// every snapshot install and the criterion is reachable.
func TestRaftRetirementStaysReachableAcrossSuccessiveRotations(t *testing.T) {
	t.Parallel()

	// Phase-2 was enabled at index 50 and never moves again.
	const frozenCutoverIndex = uint64(50)

	// Two rotations have happened since.
	for _, rotationIndex := range []uint64{400, 900} {
		require.Less(t, frozenCutoverIndex, rotationIndex,
			"the frozen cutover index cannot reach rotation %d, so a criterion "+
				"sourced from it is unsatisfiable by construction", rotationIndex)

		// A snapshot taken after the rotation satisfies the criterion.
		snapshotIndex := rotationIndex + 10
		d, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
			[]encryption.RaftRetirementReport{
				readyRaft("n1", rotationIndex+1, snapshotIndex),
				readyRaft("n2", rotationIndex+1, snapshotIndex),
				readyRaft("n3", rotationIndex+1, snapshotIndex),
			}, rotationIndex-1, rotationIndex)
		require.NoError(t, err)
		require.True(t, d.Eligible,
			"rotation %d must become retirable once every node has snapshotted past it; blockers: %v",
			rotationIndex, d.Blockers)

		// Boundary: a snapshot exactly at the rotation index is enough,
		// because a restore from it replays only later entries.
		d, err = encryption.ClassifyRaftDEKRetirement(retireMembers,
			[]encryption.RaftRetirementReport{
				readyRaft("n1", rotationIndex+1, rotationIndex),
				readyRaft("n2", rotationIndex+1, rotationIndex),
				readyRaft("n3", rotationIndex+1, rotationIndex),
			}, rotationIndex-1, rotationIndex)
		require.NoError(t, err)
		require.True(t, d.Eligible, "blockers: %v", d.Blockers)
	}
}

// TestRetirementRefusesDuplicateNodeReports pins the fail-closed
// handling of an ambiguous report set.
//
// Collapsing reports into a map is last-write-wins, so a node that
// reported a blocker and then reported ready would be recorded as ready
// while coverage still looked complete — eligibility would depend on
// the order the reports arrived. Both classifiers must refuse instead.
func TestRetirementRefusesDuplicateNodeReports(t *testing.T) {
	t.Parallel()

	t.Run("storage: a blocker followed by a ready report", func(t *testing.T) {
		t.Parallel()

		blocked := readyStorage("n2", 200)
		blocked.ValuesPerDEK = 7

		_, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
			[]encryption.StorageRetirementReport{
				readyStorage("n1", 200),
				blocked,
				readyStorage("n2", 200), // duplicate: would win and hide the blocker
				readyStorage("n3", 200),
			}, 100)
		require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
	})

	t.Run("raft: a blocker followed by a ready report", func(t *testing.T) {
		t.Parallel()

		blocked := readyRaft("n2", 1, 1000)

		_, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
			[]encryption.RaftRetirementReport{
				readyRaft("n1", 500, 1000),
				blocked,
				readyRaft("n2", 500, 1000), // duplicate: would win and hide the blocker
				readyRaft("n3", 500, 1000),
			}, 100, 900)
		require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
	})
}
