package encryption

import (
	"fmt"
	"slices"
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

// ErrRetiringDEKStillActive reports an attempt to retire the DEK that is
// still selected for writes or proposals.
//
// Its own sentinel because it is not "not yet eligible": every retention
// criterion can legitimately pass for the ACTIVE key -- an empty cluster
// reports a complete rewrite cursor, zero values and an advanced retention
// floor for the key it is still writing under -- and unloading it breaks the
// next write or proposal rather than an old read.
var ErrRetiringDEKStillActive = errors.New(
	"encryption: refusing to retire the DEK that is still active")

// StorageRetirementReport is one node's view of a storage DEK.
type StorageRetirementReport struct {
	NodeID string
	// ReportedKeyID is the DEK this report describes. Checked against the
	// key being retired: a report gathered for a different key says nothing
	// about this one, and accepting it silently would judge the wrong DEK.
	ReportedKeyID uint32
	// ActiveKeyID is the storage DEK this node is currently writing under.
	// A successor must be active and must differ from the retiring key.
	ActiveKeyID uint32
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

// RaftGroupRetirementReport is one node's view of ONE Raft group.
//
// Per group, because Raft log and snapshot indexes live in independent
// per-group index spaces while the raft DEK is cluster-wide: the runtime
// installs one wrap on every attached ShardGroup
// (raftEnvelopeRuntime.installFromApply loops over r.groups). Judging the
// shared DEK from a single group's indexes leaves every other group
// unchecked, and aggregating across groups with a minimum is worse still --
// a quiet low-index group stays permanently below a busy group's boundary
// even after all of its old-key entries are gone, so the DEK could never be
// retired.
type RaftGroupRetirementReport struct {
	GroupID uint64
	// LogCompactIndex is this node's persisted Raft log start index for
	// THIS group -- the lower bound of un-truncated entries, exposed as
	// etcd_raft_log_compact_index. It must be STRICTLY greater than the
	// largest index ever proposed under the retiring DEK in this group.
	LogCompactIndex uint64
	// SnapshotIndex is the Raft index of this group's last committed
	// snapshot. See RaftRetirementReport.SnapshotIndex for why it is the
	// snapshot's own index rather than raft_envelope_cutover_index.
	SnapshotIndex uint64
}

// RaftGroupBoundary is the old-key high-water mark for one Raft group.
type RaftGroupBoundary struct {
	// KeyID is the DEK this boundary describes. Checked against the key
	// being retired for the same reason the reports are: without it, a
	// boundary collected for a different DEK -- key 8's lower
	// LargestProposedIndex while key 7 is being retired -- would be accepted
	// by reports that all correctly name key 7, and the classifier would
	// return eligible with key 7 entries still replayable. Mixing per-key
	// metric series has to fail closed.
	KeyID uint32
	// LargestProposedIndex is the largest log index ever proposed under the
	// retiring DEK in this group.
	//
	// There is deliberately no per-group rotation index. The rotation is
	// proposed once, through the DEFAULT group's engine
	// (main_encryption_admin.go), and its apply handler installs the new
	// wrapper on every attached group in memory
	// (raftEnvelopeRuntime.installRotatedRaftDEK). A non-default group has
	// no rotation entry in its own index space at all, so requiring one
	// would force the report collector to invent a number and make the
	// verdict arbitrary. This index is the quantity that does exist per
	// group, and it is the one the snapshot criterion actually needs:
	// restoring from a snapshot below it replays entries proposed under the
	// retiring DEK.
	LargestProposedIndex uint64
}

// RaftRetirementReport is one node's view of a raft DEK.
type RaftRetirementReport struct {
	NodeID string
	// ReportedKeyID is the DEK this report describes, checked against the
	// key being retired.
	ReportedKeyID uint32
	// ActiveKeyID is the raft DEK this node currently proposes under.
	ActiveKeyID uint32
	// Groups is this node's per-group view. Every group the node hosts must
	// be present, because the DEK is shared across all of them.
	Groups []RaftGroupRetirementReport
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
	// Purpose records which classifier produced this decision, so Err can
	// pick the matching sentinel without being told.
	Purpose string
	// Eligible is true only when every criterion holds on every node.
	Eligible bool
	// Blockers names each node that is not yet ready and why, so an
	// operator can see which replica to wait on rather than being told
	// only that the cluster is not ready.
	Blockers []string
}

// Err returns the sentinel matching this decision, or nil when eligible.
// Callers surface this from `retire-dek`.
//
// The sentinel comes from the CLASSIFIER that produced the decision, recorded
// in Purpose, not from an argument. Selecting it from a caller-supplied string
// meant a misspelled or omitted purpose silently reported a blocked raft
// decision as ErrDEKStillReferenced -- sending the operator to the
// rewrite/MVCC remediation for a WAL blocker, and vice versa.
func (d RetirementDecision) Err() error {
	if d.Eligible {
		return nil
	}
	switch d.Purpose {
	case RetirementPurposeStorage:
		return errors.Wrapf(ErrDEKStillReferenced, "blockers: %v", d.Blockers)
	case RetirementPurposeRaft:
		return errors.Wrapf(ErrRaftDEKWALStillReferences, "blockers: %v", d.Blockers)
	default:
		// Unreachable through the classifiers, which always set Purpose. A
		// zero-value decision reaching here is a wiring bug, and guessing a
		// sentinel would point the operator at the wrong remediation.
		return errors.Wrapf(ErrIncompleteRetirementReport,
			"decision has no purpose; blockers: %v", d.Blockers)
	}
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
	members []string, reports []StorageRetirementReport, retiringKeyID uint32, largestCommitTS uint64,
) (RetirementDecision, error) {
	if retiringKeyID == 0 {
		return RetirementDecision{}, errors.Wrap(ErrIncompleteRetirementReport,
			"retiring key id is required")
	}
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
		if err := checkReportKeyBinding(node, r.ReportedKeyID, r.ActiveKeyID, retiringKeyID); err != nil {
			return RetirementDecision{}, err
		}
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
	return RetirementDecision{
		Purpose:  RetirementPurposeStorage,
		Eligible: len(blockers) == 0,
		Blockers: blockers,
	}, nil
}

// ClassifyRaftDEKRetirement applies the §5.4 raft criteria.
//
// largestProposedIndex is the largest log index ever proposed under the
// retiring DEK; rotationIndex is the index of the rotation entry that
// installed its successor.
func ClassifyRaftDEKRetirement(
	members []string, reports []RaftRetirementReport,
	retiringKeyID uint32, boundaries map[uint64]RaftGroupBoundary,
) (RetirementDecision, error) {
	if retiringKeyID == 0 {
		return RetirementDecision{}, errors.Wrap(ErrIncompleteRetirementReport,
			"retiring key id is required")
	}
	if len(boundaries) == 0 {
		// No boundaries means no group's old-key high-water mark is known,
		// so nothing can be verified. "Nothing to check" is not "safe".
		return RetirementDecision{}, errors.Wrap(ErrIncompleteRetirementReport,
			"per-group raft boundaries are required")
	}
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
		if err := checkReportKeyBinding(node, r.ReportedKeyID, r.ActiveKeyID, retiringKeyID); err != nil {
			return RetirementDecision{}, err
		}
		nodeBlockers, err := raftGroupBlockers(node, r.Groups, boundaries, retiringKeyID)
		if err != nil {
			return RetirementDecision{}, err
		}
		blockers = append(blockers, nodeBlockers...)
	}
	sort.Strings(blockers)
	return RetirementDecision{
		Purpose:  RetirementPurposeRaft,
		Eligible: len(blockers) == 0,
		Blockers: blockers,
	}, nil
}

// checkReportKeyBinding rejects a report that does not describe the DEK being
// retired, or that shows the retiring DEK still active.
//
// Both are fatal rather than blockers: a report for another key is not
// evidence about this one, and "still active" is a different operator mistake
// from "not yet eligible" -- every retention criterion can pass for the active
// key, and unloading it breaks the next write rather than an old read.
func checkReportKeyBinding(node string, reported, active, retiring uint32) error {
	if reported != retiring {
		return errors.Wrapf(ErrIncompleteRetirementReport,
			"node %s reported on key %d, not the key being retired (%d)",
			node, reported, retiring)
	}
	if active == retiring {
		return errors.Wrapf(ErrRetiringDEKStillActive,
			"node %s still has key %d active", node, retiring)
	}
	if active == 0 {
		return errors.Wrapf(ErrRetiringDEKStillActive,
			"node %s reports no active successor key", node)
	}
	return nil
}

// raftGroupBlockers applies the §5.4 raft criteria to every group the node
// hosts, against that group's own boundary.
//
// A node that omits a group with a known boundary is an incomplete report, not
// a passing one: the DEK is installed on every group, so an unreported group
// is an unverified one.
// indexReportedGroups turns a node's per-group reports into a map, rejecting a
// node that reported the same group twice: last-write-wins over a duplicate
// would let a group's blocking report be overwritten by a passing one.
func indexReportedGroups(
	node string,
	groups []RaftGroupRetirementReport,
) (map[uint64]RaftGroupRetirementReport, error) {
	byGroup := make(map[uint64]RaftGroupRetirementReport, len(groups))
	for _, g := range groups {
		if _, dup := byGroup[g.GroupID]; dup {
			return nil, errors.Wrapf(ErrIncompleteRetirementReport,
				"node %s reported group %d more than once", node, g.GroupID)
		}
		byGroup[g.GroupID] = g
	}
	return byGroup, nil
}

// requireExactGroupCoverage checks BOTH directions between the reports and the
// boundaries.
//
// A boundary with no report is an unverified group: the DEK is installed on
// every group, so a group nobody reported is one nobody checked. A report with
// no boundary is the same hole seen from the other side -- checking only the
// first treats the boundary map as the complete group universe, so if
// collection omits a group while every node reports it with old-key entries,
// that group is judged by nobody and the classifier returns eligible.
func requireExactGroupCoverage(
	node string,
	byGroup map[uint64]RaftGroupRetirementReport,
	boundaries map[uint64]RaftGroupBoundary,
) error {
	var missing []uint64
	for groupID := range boundaries {
		if _, ok := byGroup[groupID]; !ok {
			missing = append(missing, groupID)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		return errors.Wrapf(ErrIncompleteRetirementReport,
			"node %s did not report groups %v", node, missing)
	}

	var unbounded []uint64
	for groupID := range byGroup {
		if _, ok := boundaries[groupID]; !ok {
			unbounded = append(unbounded, groupID)
		}
	}
	if len(unbounded) > 0 {
		slices.Sort(unbounded)
		return errors.Wrapf(ErrIncompleteRetirementReport,
			"node %s reported groups %v with no boundary", node, unbounded)
	}
	return nil
}

// groupBlockers applies the §5.4 raft criteria to ONE group.
func groupBlockers(
	node string,
	groupID uint64,
	g RaftGroupRetirementReport,
	boundary RaftGroupBoundary,
) []string {
	var blockers []string
	// Strictly greater, per §5.4: an index EQUAL to the largest proposed one
	// means that entry is still un-truncated and would be replayed.
	if g.LogCompactIndex <= boundary.LargestProposedIndex {
		blockers = append(blockers,
			fmt.Sprintf("%s group %d: raft log start index %d has not passed proposed index %d",
				node, groupID, g.LogCompactIndex, boundary.LargestProposedIndex))
	}
	if g.SnapshotIndex < boundary.LargestProposedIndex {
		blockers = append(blockers,
			fmt.Sprintf("%s group %d: last snapshot predates the old-key high-water mark (snapshot %d < proposed %d)",
				node, groupID, g.SnapshotIndex, boundary.LargestProposedIndex))
	}
	return blockers
}

// raftGroupBlockers applies the §5.4 raft criteria to every group the node
// hosts, against that group's own boundary.
func raftGroupBlockers(
	node string,
	groups []RaftGroupRetirementReport,
	boundaries map[uint64]RaftGroupBoundary,
	retiringKeyID uint32,
) ([]string, error) {
	byGroup, err := indexReportedGroups(node, groups)
	if err != nil {
		return nil, err
	}
	if err := requireExactGroupCoverage(node, byGroup, boundaries); err != nil {
		return nil, err
	}

	groupIDs := make([]uint64, 0, len(boundaries))
	for groupID := range boundaries {
		groupIDs = append(groupIDs, groupID)
	}
	slices.Sort(groupIDs)

	var blockers []string
	for _, groupID := range groupIDs {
		boundary := boundaries[groupID]
		if boundary.KeyID != retiringKeyID {
			return nil, errors.Wrapf(ErrIncompleteRetirementReport,
				"group %d boundary describes key %d, not the retiring key %d",
				groupID, boundary.KeyID, retiringKeyID)
		}
		blockers = append(blockers, groupBlockers(node, groupID, byGroup[groupID], boundary)...)
	}
	return blockers, nil
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
