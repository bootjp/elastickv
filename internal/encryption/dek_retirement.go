package encryption

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"
)

// §5.4 DEK retirement eligibility.
//
// Retiring a DEK unloads it. If any ciphertext anywhere still needs it,
// that data becomes unreadable — so the design states plainly that
// there is **no override flag**, because overriding "is silently
// equivalent to 'lose data on the next read or replay.'" This package
// therefore exposes only a classifier: every criterion must be
// satisfied cluster-wide, and there is no way to ask it to say yes
// anyway.
//
// The two purposes have genuinely different criteria because their
// ciphertext lives in different places — Pebble values for storage,
// the etcd raft WAL and snapshots for raft — so retaining MVCC
// versions does not cover the raft case at all.

// ErrDEKStillReferenced reports that a storage DEK cannot be retired
// yet: either the rewrite has not finished, or a snapshot/lease read
// could still legitimately ask for a version written under it.
var ErrDEKStillReferenced = errors.New("encryption: storage DEK is still referenced; refusing to retire")

// ErrRaftDEKWALStillReferences is the §5.4 sentinel for retiring a raft
// DEK early.
//
// Without the WAL guard, unloading a raft DEK while the WAL still holds
// entries encrypted under it causes unknown_key_id apply failures on
// the next restart (replay from disk) or on a lagging follower's
// catch-up — the exact failure mode the writer registry and capability
// gate exist to prevent.
var ErrRaftDEKWALStillReferences = errors.New("encryption: raft WAL still references this DEK; refusing to retire")

// ErrIncompleteRetirementReport reports that eligibility could not be
// established for the whole cluster.
//
// Distinct from "not yet eligible": the criteria are cluster-wide, so a
// missing node is not a node that passed. Treating an absent report as
// satisfied is how a single unreachable replica ends up replaying
// against an unloaded DEK.
var ErrIncompleteRetirementReport = errors.New("encryption: retirement report does not cover every cluster member")

// StorageRetirementReport is one node's view of a storage DEK.
type StorageRetirementReport struct {
	NodeID string
	// RewriteCursorComplete is true when the rewrite job has reached
	// the end of the keyspace on this node.
	RewriteCursorComplete bool
	// ValuesPerDEK is elastickv_encryption_values_per_dek{key_id} on
	// this node; zero is required.
	ValuesPerDEK uint64
	// MinRetainedTS is this node's MVCC retention floor. It must have
	// advanced past every commit_ts written under the retiring DEK,
	// or a snapshot read can still legitimately ask for one of those
	// versions.
	MinRetainedTS uint64
}

// RaftRetirementReport is one node's view of a raft DEK.
type RaftRetirementReport struct {
	NodeID string
	// LogCompactIndex is this node's persisted Raft log start index —
	// the lower bound of un-truncated entries, exposed as
	// etcd_raft_log_compact_index. It must be STRICTLY greater than
	// the largest index ever proposed under the retiring DEK.
	LogCompactIndex uint64
	// SnapshotIndex is the Raft index of this node's last committed
	// snapshot (raftpb.SnapshotMetadata.Index). It must be at least the
	// rotation index: restoring from a snapshot taken BEFORE the
	// rotation replays the entries between the snapshot and the
	// rotation, and those were proposed under the retiring DEK.
	//
	// NOT the sidecar's raft_envelope_cutover_index, which §5.4's
	// parenthetical names. That value is the one-shot Phase-2
	// enablement index: applier.go preserves the original across every
	// later rotation, and kv/fsm.go copies the unchanged value into
	// each new snapshot header. It can therefore never be "past the
	// rotation entry" for any post-cutover rotation, so a criterion
	// built on it classifies the retiring raft DEK ineligible forever
	// and no raft DEK could ever be retired.
	//
	// The cutover index also does not mean what that criterion needs.
	// Per §4.4 the FSM snapshot stream "is ciphertext by construction"
	// from the storage layer and "no additional wrapping is required at
	// the snapshot layer" — a snapshot is not encrypted under any raft
	// DEK, so "taken under the new raft DEK" can only be about which
	// entries a restore would replay. That is exactly what the
	// snapshot's own index expresses, and it advances with every
	// snapshot install.
	SnapshotIndex uint64
}

// RetirementDecision is the classifier's answer.
type RetirementDecision struct {
	// Eligible is true only when every criterion holds on every node.
	Eligible bool
	// Blockers names each node that is not yet ready and why, so an
	// operator can see which replica to wait on rather than being told
	// only that the cluster is not ready.
	Blockers []string
}

// Err returns the sentinel matching this decision, or nil when
// eligible. Callers surface this from `retire-dek`.
func (d RetirementDecision) Err(purpose string) error {
	if d.Eligible {
		return nil
	}
	base := ErrDEKStillReferenced
	if purpose == RetirementPurposeRaft {
		base = ErrRaftDEKWALStillReferences
	}
	return errors.Wrapf(base, "blockers: %v", d.Blockers)
}

// Purposes accepted by the classifier.
const (
	RetirementPurposeStorage = "storage"
	RetirementPurposeRaft    = "raft"
)

// ClassifyStorageDEKRetirement applies the §5.4 storage criteria.
//
// largestCommitTS is the largest commit_ts ever written under the
// retiring DEK. members is the full cluster membership; a report is
// required from each, because "cluster-wide" cannot be established
// from a subset.
func ClassifyStorageDEKRetirement(
	members []string, reports []StorageRetirementReport, largestCommitTS uint64,
) (RetirementDecision, error) {
	byNode := make(map[string]StorageRetirementReport, len(reports))
	for _, r := range reports {
		if _, dup := byNode[r.NodeID]; dup {
			return RetirementDecision{}, duplicateReportErr(r.NodeID)
		}
		byNode[r.NodeID] = r
	}
	if err := requireFullCoverage(members, func(n string) bool {
		_, ok := byNode[n]
		return ok
	}); err != nil {
		return RetirementDecision{}, err
	}

	var blockers []string
	for _, node := range members {
		r := byNode[node]
		if !r.RewriteCursorComplete {
			blockers = append(blockers,
				fmt.Sprintf("%s: rewrite cursor has not reached the end of the keyspace", node))
		}
		if r.ValuesPerDEK != 0 {
			blockers = append(blockers,
				fmt.Sprintf("%s: %d values still encrypted under this DEK", node, r.ValuesPerDEK))
		}
		// Strictly greater: a minRetainedTS EQUAL to the largest
		// commit_ts still admits a read at exactly that version.
		if r.MinRetainedTS <= largestCommitTS {
			blockers = append(blockers,
				fmt.Sprintf("%s: minRetainedTS %d has not advanced past commit_ts %d",
					node, r.MinRetainedTS, largestCommitTS))
		}
	}
	sort.Strings(blockers)
	return RetirementDecision{Eligible: len(blockers) == 0, Blockers: blockers}, nil
}

// ClassifyRaftDEKRetirement applies the §5.4 raft criteria.
//
// largestProposedIndex is the largest log index ever proposed under the
// retiring DEK; rotationIndex is the index of the rotation entry that
// installed its successor.
func ClassifyRaftDEKRetirement(
	members []string, reports []RaftRetirementReport,
	largestProposedIndex, rotationIndex uint64,
) (RetirementDecision, error) {
	byNode := make(map[string]RaftRetirementReport, len(reports))
	for _, r := range reports {
		if _, dup := byNode[r.NodeID]; dup {
			return RetirementDecision{}, duplicateReportErr(r.NodeID)
		}
		byNode[r.NodeID] = r
	}
	if err := requireFullCoverage(members, func(n string) bool {
		_, ok := byNode[n]
		return ok
	}); err != nil {
		return RetirementDecision{}, err
	}

	var blockers []string
	for _, node := range members {
		r := byNode[node]
		// Strictly greater, per §5.4: an index EQUAL to the largest
		// proposed one means that entry is still un-truncated and
		// would be replayed.
		if r.LogCompactIndex <= largestProposedIndex {
			blockers = append(blockers,
				fmt.Sprintf("%s: raft log start index %d has not passed proposed index %d",
					node, r.LogCompactIndex, largestProposedIndex))
		}
		if r.SnapshotIndex < rotationIndex {
			blockers = append(blockers,
				fmt.Sprintf("%s: last snapshot predates the rotation (snapshot %d < rotation %d)",
					node, r.SnapshotIndex, rotationIndex))
		}
	}
	sort.Strings(blockers)
	return RetirementDecision{Eligible: len(blockers) == 0, Blockers: blockers}, nil
}

// duplicateReportErr rejects a report set containing the same node
// twice.
//
// Collapsing duplicates into a map is last-write-wins, so a node that
// reported a blocker and then reported ready would be recorded as
// ready, and coverage would still be complete because the map holds one
// entry per unique node. Eligibility would then depend on the order the
// reports arrived. Since the whole point of this classifier is that
// unloading a DEK with any live reference loses data, an ambiguous
// report set fails closed instead.
func duplicateReportErr(nodeID string) error {
	return errors.Wrapf(ErrIncompleteRetirementReport,
		"node %s reported more than once", nodeID)
}

// requireFullCoverage rejects a report set that does not cover every
// member, and a membership list that is empty.
//
// Duplicates are caught by the callers, not here. This function
// previously compared the number of unique reporting nodes against the
// membership size, which cannot detect a duplicate at all: if every
// member is covered, the unique count equals the membership size
// whether or not a node reported twice, and if a member is missing the
// `missing` check above already fires.
//
// An empty membership is refused rather than treated as trivially
// satisfied: "no members reported a problem" is not evidence when
// nobody was asked.
func requireFullCoverage(members []string, covered func(string) bool) error {
	if len(members) == 0 {
		return errors.Wrap(ErrIncompleteRetirementReport, "cluster membership is empty")
	}
	var missing []string
	for _, node := range members {
		if !covered(node) {
			missing = append(missing, node)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return errors.Wrapf(ErrIncompleteRetirementReport, "no report from %v", missing)
	}
	return nil
}
