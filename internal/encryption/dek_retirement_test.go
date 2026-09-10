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

func readyRaft(node string, compact, cutover uint64) encryption.RaftRetirementReport {
	return encryption.RaftRetirementReport{
		NodeID:               node,
		LogCompactIndex:      compact,
		SnapshotCutoverIndex: cutover,
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
