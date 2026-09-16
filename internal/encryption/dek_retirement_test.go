package encryption_test

import (
	"testing"

	"github.com/bootjp/elastickv/internal/encryption"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var retireMembers = []string{"n1", "n2", "n3"}

// retiringKeyID / successorKeyID: the reports must name the key under test and
// show a different key active, or the classifier refuses outright.
const (
	retiringKeyID  = uint32(7)
	successorKeyID = uint32(8)
)

// retireGroup is the single Raft group most cases use; multi-group behaviour
// has its own tests.
const retireGroup = uint64(1)

func readyStorage(node string, minRetained uint64) encryption.StorageRetirementReport {
	return encryption.StorageRetirementReport{
		NodeID:                node,
		ReportedKeyID:         retiringKeyID,
		ActiveKeyID:           successorKeyID,
		RewriteCursorComplete: true,
		ValuesPerDEK:          0,
		MinRetainedTS:         minRetained,
	}
}

func boundaries(largestProposed uint64) map[uint64]encryption.RaftGroupBoundary {
	return map[uint64]encryption.RaftGroupBoundary{
		retireGroup: {KeyID: retiringKeyID, LargestProposedIndex: largestProposed},
	}
}

func readyRaft(node string, compact, snapshot uint64) encryption.RaftRetirementReport {
	return encryption.RaftRetirementReport{
		NodeID:        node,
		ReportedKeyID: retiringKeyID,
		ActiveKeyID:   successorKeyID,
		Groups: []encryption.RaftGroupRetirementReport{{
			GroupID:         retireGroup,
			LogCompactIndex: compact,
			SnapshotIndex:   snapshot,
		}},
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
		}, retiringKeyID, 100)
	require.NoError(t, err)
	require.True(t, d.Eligible)
	require.Empty(t, d.Blockers)
	require.NoError(t, d.Err())
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
		}, retiringKeyID, 100)
	require.NoError(t, err)
	require.False(t, d.Eligible)
	require.Len(t, d.Blockers, 1)
	require.Contains(t, d.Blockers[0], "n2")
	require.True(t, errors.Is(d.Err(),
		encryption.ErrDEKStillReferenced))
}

func TestStorageRetirementRefusesWhileTheRewriteIsIncomplete(t *testing.T) {
	t.Parallel()

	partial := readyStorage("n3", 200)
	partial.RewriteCursorComplete = false

	d, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", 200), readyStorage("n2", 200), partial,
		}, retiringKeyID, 100)
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
		}, retiringKeyID, largestCommitTS)
	require.NoError(t, err)
	require.False(t, equal.Eligible,
		"minRetainedTS equal to the commit_ts still admits a read at that version")

	past, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", largestCommitTS+1),
			readyStorage("n2", largestCommitTS+1),
			readyStorage("n3", largestCommitTS+1),
		}, retiringKeyID, largestCommitTS)
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
		}, retiringKeyID, boundaries(300))
	require.NoError(t, err)
	require.True(t, d.Eligible)
	require.NoError(t, d.Err())
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
		}, retiringKeyID, boundaries(300))
	require.NoError(t, err)
	require.False(t, d.Eligible)
	require.True(t, errors.Is(d.Err(),
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
		}, retiringKeyID, boundaries(proposed))
	require.NoError(t, err)
	require.False(t, equal.Eligible)

	past, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", proposed+1, 400), readyRaft("n2", proposed+1, 400), readyRaft("n3", proposed+1, 400),
		}, retiringKeyID, boundaries(proposed))
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
		}, retiringKeyID, boundaries(300))
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
		}, retiringKeyID, 100)
	require.Error(t, err)
	require.True(t, errors.Is(err, encryption.ErrIncompleteRetirementReport))
	require.ErrorContains(t, err, "n3")

	_, err = encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", 500, 400), readyRaft("n2", 500, 400),
		}, retiringKeyID, boundaries(300))
	require.Error(t, err)
	require.True(t, errors.Is(err, encryption.ErrIncompleteRetirementReport))
}

// TestRetirementRefusesAnEmptyMembership guards the degenerate case:
// "no member reported a problem" is not evidence when nobody was asked.
func TestRetirementRefusesAnEmptyMembership(t *testing.T) {
	t.Parallel()

	_, err := encryption.ClassifyStorageDEKRetirement(nil, nil, retiringKeyID, 100)
	require.True(t, errors.Is(err, encryption.ErrIncompleteRetirementReport))

	_, err = encryption.ClassifyRaftDEKRetirement(nil, nil, retiringKeyID, boundaries(300))
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
		[]encryption.StorageRetirementReport{bad1, readyStorage("n2", 200), bad3}, retiringKeyID, 100)
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
			}, retiringKeyID, boundaries(rotationIndex-1))
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
			}, retiringKeyID, boundaries(rotationIndex-1))
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
			}, retiringKeyID, 100)
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
			}, retiringKeyID, boundaries(100))
		require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
	})
}

// ---------------------------------------------------------------------------
// The retiring key must not be the active one
// ---------------------------------------------------------------------------

// TestRetirementRefusesTheStillActiveDEK pins the guard against unloading the
// key still selected for writes.
//
// Every retention criterion can legitimately pass for the ACTIVE key: an empty
// cluster reports a complete rewrite cursor, zero values and an advanced
// retention floor for the key it is still writing under. The classifiers saw
// neither the retiring key nor each node's active key, so they returned
// eligible and a retire command would unload the live key — breaking the next
// write or proposal rather than an old read.
func TestRetirementRefusesTheStillActiveDEK(t *testing.T) {
	t.Parallel()

	t.Run("storage: the retiring key is still active", func(t *testing.T) {
		t.Parallel()

		reports := []encryption.StorageRetirementReport{
			readyStorage("n1", 200), readyStorage("n2", 200), readyStorage("n3", 200),
		}
		// Every criterion passes; only the active key is wrong.
		reports[1].ActiveKeyID = retiringKeyID

		_, err := encryption.ClassifyStorageDEKRetirement(
			retireMembers, reports, retiringKeyID, 100)
		require.ErrorIs(t, err, encryption.ErrRetiringDEKStillActive)
	})

	t.Run("raft: the retiring key is still active", func(t *testing.T) {
		t.Parallel()

		reports := []encryption.RaftRetirementReport{
			readyRaft("n1", 500, 1000), readyRaft("n2", 500, 1000), readyRaft("n3", 500, 1000),
		}
		reports[2].ActiveKeyID = retiringKeyID

		_, err := encryption.ClassifyRaftDEKRetirement(
			retireMembers, reports, retiringKeyID, boundaries(100))
		require.ErrorIs(t, err, encryption.ErrRetiringDEKStillActive)
	})

	t.Run("a node with no successor active is refused", func(t *testing.T) {
		t.Parallel()

		reports := []encryption.StorageRetirementReport{
			readyStorage("n1", 200), readyStorage("n2", 200), readyStorage("n3", 200),
		}
		// Zero means "no key active", which cannot be treated as a successor.
		reports[0].ActiveKeyID = 0

		_, err := encryption.ClassifyStorageDEKRetirement(
			retireMembers, reports, retiringKeyID, 100)
		require.ErrorIs(t, err, encryption.ErrRetiringDEKStillActive)
	})
}

// TestRetirementRefusesAReportForAnotherKey pins the binding between the report
// and the key being retired: a report gathered for a different DEK says nothing
// about this one, and accepting it silently judges the wrong key.
func TestRetirementRefusesAReportForAnotherKey(t *testing.T) {
	t.Parallel()

	reports := []encryption.StorageRetirementReport{
		readyStorage("n1", 200), readyStorage("n2", 200), readyStorage("n3", 200),
	}
	reports[1].ReportedKeyID = retiringKeyID + 99

	_, err := encryption.ClassifyStorageDEKRetirement(
		retireMembers, reports, retiringKeyID, 100)
	require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
}

func TestRetirementRequiresTheRetiringKeyID(t *testing.T) {
	t.Parallel()

	_, err := encryption.ClassifyStorageDEKRetirement(retireMembers, nil, 0, 100)
	require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)

	_, err = encryption.ClassifyRaftDEKRetirement(
		retireMembers, nil, 0, boundaries(100))
	require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
}

// ---------------------------------------------------------------------------
// Raft boundaries are per group
// ---------------------------------------------------------------------------

func multiGroupRaft(node string, groups ...encryption.RaftGroupRetirementReport) encryption.RaftRetirementReport {
	return encryption.RaftRetirementReport{
		NodeID:        node,
		ReportedKeyID: retiringKeyID,
		ActiveKeyID:   successorKeyID,
		Groups:        groups,
	}
}

// TestRaftRetirementChecksEveryGroupAgainstItsOwnBoundary is the multi-shard
// regression.
//
// The raft DEK is cluster-wide — raftEnvelopeRuntime.installFromApply sets the
// same wrap on every attached ShardGroup — but Raft log and snapshot indexes
// live in independent per-group index spaces. Judging the shared DEK from one
// group's indexes left every other group unchecked, so a group whose WAL still
// held old-key entries could not block retirement.
func TestRaftRetirementChecksEveryGroupAgainstItsOwnBoundary(t *testing.T) {
	t.Parallel()

	bounds := map[uint64]encryption.RaftGroupBoundary{
		// A busy group with high indexes...
		1: {KeyID: retiringKeyID, LargestProposedIndex: 10_000},
		// ...and a quiet one whose indexes are far lower.
		2: {KeyID: retiringKeyID, LargestProposedIndex: 40},
	}

	t.Run("every group past its own boundary is eligible", func(t *testing.T) {
		t.Parallel()

		reports := make([]encryption.RaftRetirementReport, 0, len(retireMembers))
		for _, node := range retireMembers {
			reports = append(reports, multiGroupRaft(node,
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000},
				encryption.RaftGroupRetirementReport{GroupID: 2, LogCompactIndex: 41, SnapshotIndex: 40},
			))
		}

		d, err := encryption.ClassifyRaftDEKRetirement(retireMembers, reports, retiringKeyID, bounds)
		require.NoError(t, err)
		require.True(t, d.Eligible,
			"the quiet group must be judged against ITS OWN low boundary, not the busy "+
				"group's: a global minimum would keep it permanently behind; blockers: %v",
			d.Blockers)
	})

	t.Run("a group still behind its boundary blocks retirement", func(t *testing.T) {
		t.Parallel()

		reports := make([]encryption.RaftRetirementReport, 0, len(retireMembers))
		for _, node := range retireMembers {
			reports = append(reports, multiGroupRaft(node,
				// Group 1 is ready...
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000},
				// ...group 2's WAL still holds entries proposed under the key.
				// Its snapshot matches its log start, as an untruncated node's
				// does: the blocker is the log, and exactly one per node.
				encryption.RaftGroupRetirementReport{GroupID: 2, LogCompactIndex: 20, SnapshotIndex: 40},
			))
		}

		d, err := encryption.ClassifyRaftDEKRetirement(retireMembers, reports, retiringKeyID, bounds)
		require.NoError(t, err)
		require.False(t, d.Eligible,
			"an unready group must block the shared DEK even when other groups are ready")
		require.Len(t, d.Blockers, len(retireMembers))
		require.Contains(t, d.Blockers[0], "group 2")
	})

	t.Run("a node that omits a group is an incomplete report", func(t *testing.T) {
		t.Parallel()

		reports := []encryption.RaftRetirementReport{
			multiGroupRaft("n1",
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000},
				encryption.RaftGroupRetirementReport{GroupID: 2, LogCompactIndex: 41, SnapshotIndex: 40}),
			// n2 reports only group 1, so group 2 is unverified on that node.
			multiGroupRaft("n2",
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000}),
			multiGroupRaft("n3",
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000},
				encryption.RaftGroupRetirementReport{GroupID: 2, LogCompactIndex: 41, SnapshotIndex: 40}),
		}

		_, err := encryption.ClassifyRaftDEKRetirement(retireMembers, reports, retiringKeyID, bounds)
		require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
	})

	t.Run("a duplicate group on one node is refused", func(t *testing.T) {
		t.Parallel()

		reports := []encryption.RaftRetirementReport{
			multiGroupRaft("n1",
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 5, SnapshotIndex: 1},
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000}),
			multiGroupRaft("n2",
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000}),
			multiGroupRaft("n3",
				encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000}),
		}

		_, err := encryption.ClassifyRaftDEKRetirement(retireMembers, reports, retiringKeyID,
			map[uint64]encryption.RaftGroupBoundary{1: {KeyID: retiringKeyID, LargestProposedIndex: 10_000}})
		require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
	})
}

func TestRaftRetirementRequiresPerGroupBoundaries(t *testing.T) {
	t.Parallel()

	// No boundaries means no group's old-key high-water mark is known.
	// "Nothing to check" must not read as "safe".
	_, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", 500, 1000), readyRaft("n2", 500, 1000), readyRaft("n3", 500, 1000),
		}, retiringKeyID, nil)
	require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
}

// ---------------------------------------------------------------------------
// The sentinel follows the classifier, not an argument
// ---------------------------------------------------------------------------

// TestRetirementErrSentinelComesFromTheClassifier pins that each blocked
// decision reports its own sentinel.
//
// Err used to pick from a caller-supplied purpose string, defaulting to the
// storage sentinel for anything unrecognised — so a blocked RAFT decision
// passed a misspelled or omitted purpose sent the operator to the rewrite/MVCC
// remediation for what is actually a WAL blocker.
func TestRetirementErrSentinelComesFromTheClassifier(t *testing.T) {
	t.Parallel()

	storage, err := encryption.ClassifyStorageDEKRetirement(retireMembers,
		[]encryption.StorageRetirementReport{
			readyStorage("n1", 1), readyStorage("n2", 1), readyStorage("n3", 1),
		}, retiringKeyID, 100)
	require.NoError(t, err)
	require.False(t, storage.Eligible)
	require.ErrorIs(t, storage.Err(), encryption.ErrDEKStillReferenced)
	require.NotErrorIs(t, storage.Err(), encryption.ErrRaftDEKWALStillReferences)

	raft, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", 10, 1000), readyRaft("n2", 10, 1000), readyRaft("n3", 10, 1000),
		}, retiringKeyID, boundaries(500))
	require.NoError(t, err)
	require.False(t, raft.Eligible)
	require.ErrorIs(t, raft.Err(), encryption.ErrRaftDEKWALStillReferences,
		"a WAL blocker must not be reported as an MVCC one")
	require.NotErrorIs(t, raft.Err(), encryption.ErrDEKStillReferenced)
}

// A decision with no purpose is a wiring bug, and guessing a sentinel would
// point the operator at the wrong remediation.
func TestRetirementErrRefusesToGuessAPurpose(t *testing.T) {
	t.Parallel()

	blocked := encryption.RetirementDecision{Blockers: []string{"n1: something"}}
	require.ErrorIs(t, blocked.Err(), encryption.ErrIncompleteRetirementReport)

	eligible := encryption.RetirementDecision{Eligible: true}
	require.NoError(t, eligible.Err(), "an eligible decision has no error whatever its purpose")
}

// TestRaftRetirementRejectsReportedGroupsWithoutABoundary closes the other
// direction of coverage.
//
// Checking only that every BOUNDARY was reported treats the boundary map as
// the complete group universe. If collection omits group 2 while every node
// reports group 2 with old-key entries still in its log, group 2 is judged by
// nobody -- and the classifier returns eligible for a DEK whose entries are
// still replayable. An incomplete boundary map must not be able to authorize
// unloading the shared key.
func TestRaftRetirementRejectsReportedGroupsWithoutABoundary(t *testing.T) {
	t.Parallel()

	reports := make([]encryption.RaftRetirementReport, 0, len(retireMembers))
	for _, node := range retireMembers {
		reports = append(reports, multiGroupRaft(node,
			encryption.RaftGroupRetirementReport{GroupID: 1, LogCompactIndex: 10_001, SnapshotIndex: 10_000},
			// Reported, and far behind -- but absent from the boundary map.
			encryption.RaftGroupRetirementReport{GroupID: 2, LogCompactIndex: 1, SnapshotIndex: 1},
		))
	}

	_, err := encryption.ClassifyRaftDEKRetirement(retireMembers, reports, retiringKeyID,
		map[uint64]encryption.RaftGroupBoundary{
			1: {KeyID: retiringKeyID, LargestProposedIndex: 10_000},
		})
	require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
	require.ErrorContains(t, err, "no boundary")
}

// TestRaftRetirementRejectsABoundaryForAnotherKey pins the boundary's key
// binding, which the reports already had and the boundaries did not.
//
// Every report can correctly name key 7 while the boundary handed in for a
// group describes key 8. Key 8's lower high-water mark is then trivially
// cleared, the classifier returns eligible, and key 7's entries are still
// replayable. Mixing per-key metric series has to fail closed.
func TestRaftRetirementRejectsABoundaryForAnotherKey(t *testing.T) {
	t.Parallel()

	reports := []encryption.RaftRetirementReport{
		readyRaft("n1", 10_001, 10_000),
		readyRaft("n2", 10_001, 10_000),
		readyRaft("n3", 10_001, 10_000),
	}

	_, err := encryption.ClassifyRaftDEKRetirement(retireMembers, reports, retiringKeyID,
		map[uint64]encryption.RaftGroupBoundary{
			retireGroup: {KeyID: retiringKeyID + 1, LargestProposedIndex: 10},
		})
	require.ErrorIs(t, err, encryption.ErrIncompleteRetirementReport)
	require.ErrorContains(t, err, "not the retiring key")
}

// TestRaftRetirementBlocksASnapshotBelowTheGroupsHighWaterMark states the
// snapshot criterion in the only index space that exists per group.
//
// There is no per-group rotation index to compare against: the rotation is
// proposed once through the DEFAULT group's engine and its apply handler
// installs the new wrapper on every group in memory, so a non-default group
// has no rotation entry of its own. The quantity that does exist per group is
// the largest index proposed under the retiring key, and restoring from a
// snapshot below it replays exactly those entries.
func TestRaftRetirementBlocksASnapshotBelowTheGroupsHighWaterMark(t *testing.T) {
	t.Parallel()

	d, err := encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			// Log start is past the mark, but the snapshot is not: a restore
			// would begin before the last old-key entry.
			readyRaft("n1", 10_001, 9_000),
			readyRaft("n2", 10_001, 10_000),
			readyRaft("n3", 10_001, 10_000),
		}, retiringKeyID, boundaries(10_000))
	require.NoError(t, err)
	require.False(t, d.Eligible)
	require.Len(t, d.Blockers, 1)
	require.Contains(t, d.Blockers[0], "n1")
	require.Contains(t, d.Blockers[0], "high-water mark")

	// Exactly at the mark is enough: a restore from it replays only entries
	// at or after the last old-key one, and that one is in the snapshot.
	d, err = encryption.ClassifyRaftDEKRetirement(retireMembers,
		[]encryption.RaftRetirementReport{
			readyRaft("n1", 10_001, 10_000),
			readyRaft("n2", 10_001, 10_000),
			readyRaft("n3", 10_001, 10_000),
		}, retiringKeyID, boundaries(10_000))
	require.NoError(t, err)
	require.True(t, d.Eligible, "blockers: %v", d.Blockers)
}
