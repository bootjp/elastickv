package etcd

import (
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/confchange"
	"go.etcd.io/raft/v3/raftpb"
	"go.etcd.io/raft/v3/tracker"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const baselineTrackerInflight = 256

func baselineSameConfState(a, b *raftpb.ConfState) bool {
	return slices.Equal(a.GetVoters(), b.GetVoters()) && slices.Equal(a.GetVotersOutgoing(), b.GetVotersOutgoing()) &&
		slices.Equal(a.GetLearners(), b.GetLearners()) && slices.Equal(a.GetLearnersNext(), b.GetLearnersNext()) && a.GetAutoLeave() == b.GetAutoLeave()
}

func selectBaselineSnapshot(dir string, image *baselineWALImage) (*raftpb.Snapshot, error) {
	selected := &raftpb.Snapshot{}
	files, err := os.ReadDir(filepath.Join(dir, snapDirName))
	if err != nil && !os.IsNotExist(err) {
		return nil, errors.WithStack(err)
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".snap") {
			continue
		}
		path := filepath.Join(dir, snapDirName, file.Name())
		candidate, matches, err := readBaselineSnapshotCandidate(path, image)
		if err != nil {
			return nil, err
		}
		if matches && candidate.GetMetadata().GetIndex() > selected.GetMetadata().GetIndex() {
			selected = candidate
		}
	}
	if err := validateSelectedBaselineSnapshot(dir, image, selected); err != nil {
		return nil, err
	}
	return selected, nil
}

func readBaselineSnapshotCandidate(path string, image *baselineWALImage) (*raftpb.Snapshot, bool, error) {
	if err := baselineRegularFile(path); err != nil {
		return nil, false, err
	}
	candidate, err := snap.Read(zap.NewNop(), path)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	matches, err := baselineSnapshotMatches(image, candidate)
	return candidate, matches, err
}

func validateSelectedBaselineSnapshot(dir string, image *baselineWALImage, selected *raftpb.Snapshot) error {
	if selected.GetMetadata().GetIndex() == 0 {
		for _, marker := range image.snapshots {
			if marker.GetIndex() != 0 {
				return errors.Wrap(ErrOfflineBaseline, "no WAL-valid snapshot available")
			}
		}
	}
	if isSnapshotToken(selected.GetData()) {
		token, err := decodeSnapshotToken(selected.GetData())
		if err != nil {
			return err
		}
		if token.Index != selected.GetMetadata().GetIndex() {
			return errors.Wrap(ErrOfflineBaseline, "snapshot token index mismatch")
		}
		if err := verifyFSMSnapshotFileWithToken(fsmSnapPath(filepath.Join(dir, fsmSnapDirName), token.Index), token.CRC32C, true); err != nil {
			return err
		}
	}
	return nil
}

func baselineSnapshotMatches(image *baselineWALImage, candidate *raftpb.Snapshot) (bool, error) {
	for _, marker := range image.snapshots {
		if marker.GetIndex() != candidate.GetMetadata().GetIndex() || marker.GetTerm() != candidate.GetMetadata().GetTerm() {
			continue
		}
		if marker.ConfState != nil && !baselineSameConfState(marker.ConfState, candidate.GetMetadata().GetConfState()) {
			return false, errors.Wrap(ErrOfflineBaseline, "WAL/snapshot ConfState mismatch")
		}
		return true, nil
	}
	return false, nil
}

func baselineConfiguration(o OfflineBaselineOptions, image *baselineWALImage, snapshot *raftpb.Snapshot, proof BaselineStoreProof) (raftbootstrap.Configuration, error) {
	conf, index, err := baselineReplayedConfiguration(image, snapshot, proof)
	if err != nil {
		return raftbootstrap.Configuration{}, err
	}
	if err := validateBaselinePeers(o, image, conf); err != nil {
		return raftbootstrap.Configuration{}, err
	}
	return raftbootstrap.Configuration{Index: index, Members: slices.Clone(o.Expected.Configuration.Members), State: raftbootstrap.ConfState{
		Voters: conf.GetVoters(), VotersOutgoing: conf.GetVotersOutgoing(), Learners: conf.GetLearners(), LearnersNext: conf.GetLearnersNext(), AutoLeave: conf.GetAutoLeave(),
	}}, nil
}

func baselineReplayedConfiguration(image *baselineWALImage, snapshot *raftpb.Snapshot, proof BaselineStoreProof) (*raftpb.ConfState, uint64, error) {
	state := snapshot.GetMetadata().GetConfState()
	if state == nil {
		state = &raftpb.ConfState{}
	}
	p := tracker.MakeProgressTracker(baselineTrackerInflight, 0)
	configuration, progress, err := confchange.Restore(confchange.Changer{Tracker: p, LastIndex: image.lastIndex()}, state)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	p.Config, p.Progress = configuration, progress
	index, err := baselineSnapshotConfigurationIndex(image, snapshot, proof)
	if err != nil {
		return nil, 0, err
	}
	index, err = baselineReplayConfigurationTail(image, snapshot, &p, index)
	if err != nil {
		return nil, 0, err
	}
	if index == 0 || (proof.ConfigurationIndex != 0 && index != proof.ConfigurationIndex) {
		return nil, 0, errors.Wrap(ErrOfflineBaseline, "actual configuration index unverifiable or inconsistent")
	}
	return p.ConfState(), index, nil
}

func baselineReplayConfigurationTail(image *baselineWALImage, snapshot *raftpb.Snapshot, p *tracker.ProgressTracker, index uint64) (uint64, error) {
	for entryIndex := snapshot.GetMetadata().GetIndex() + 1; entryIndex <= image.hardState.GetCommit(); entryIndex++ {
		entry := image.entries[entryIndex]
		if entry == nil {
			return 0, errors.Wrap(ErrOfflineBaseline, "missing configuration replay entry")
		}
		change, err := baselineConfChange(entry)
		if err != nil {
			return 0, err
		}
		if change == nil {
			continue
		}
		if err := baselineApplyConfChange(p, change, image.lastIndex()); err != nil {
			return 0, err
		}
		index = entryIndex
	}
	return index, nil
}

func validateBaselinePeers(o OfflineBaselineOptions, image *baselineWALImage, conf *raftpb.ConfState) error {
	peers, present, err := loadPersistedPeersState(o.DataDir)
	if err != nil {
		return err
	}
	if !present || peers.Index > image.hardState.GetCommit() {
		return errors.Wrap(ErrOfflineBaseline, "missing or ahead peers metadata")
	}
	if err := validateConfState(*conf, peers.Peers); err != nil {
		return err
	}
	if len(peers.Peers) != len(o.Expected.Configuration.Members) {
		return errors.Wrap(ErrOfflineBaseline, "peer roster mismatch")
	}
	for _, member := range o.Expected.Configuration.Members {
		if !baselinePeerMatches(peers.Peers, member) {
			return errors.Wrap(ErrOfflineBaseline, "peer address mismatch")
		}
	}
	return nil
}

func baselinePeerMatches(peers []Peer, member raftbootstrap.Member) bool {
	for _, peer := range peers {
		if peer.NodeID == member.ID && peer.Address == member.Address {
			return true
		}
	}
	return false
}

func baselineSnapshotConfigurationIndex(image *baselineWALImage, snapshot *raftpb.Snapshot, proof BaselineStoreProof) (uint64, error) {
	m := snapshot.GetMetadata().GetIndex()
	if m == 0 {
		return 0, nil
	}
	// An ordered store row is independent evidence, not a snapshot-index guess.
	n := proof.ConfigurationIndex
	if n == 0 || n > m {
		if baselineTailChangesConfiguration(image, m) {
			return 0, nil
		}
		// A complete retained prefix can independently recover the snapshot epoch.
		var err error
		n, err = baselineFullPrefixConfiguration(image, snapshot)
		if err != nil {
			return 0, err
		}
	}
	return n, validateBaselineSnapshotEpoch(image, m, n)
}

func baselineTailChangesConfiguration(image *baselineWALImage, snapshotIndex uint64) bool {
	for index, entry := range image.entries {
		if index <= snapshotIndex || index > image.hardState.GetCommit() {
			continue
		}
		if entry.GetType() == raftpb.EntryConfChange || entry.GetType() == raftpb.EntryConfChangeV2 {
			return true
		}
	}
	return false
}

func validateBaselineSnapshotEpoch(image *baselineWALImage, m, n uint64) error {
	for i, entry := range image.entries {
		if i > m {
			continue
		}
		change, err := baselineConfChange(entry)
		if err != nil {
			return err
		}
		if change != nil && i > n {
			return errors.Wrap(ErrOfflineBaseline, "store configuration epoch behind retained ConfChange")
		}
		if i == n && change == nil {
			return errors.Wrap(ErrOfflineBaseline, "store epoch points at ordinary data")
		}
	}
	return nil
}

func baselineFullPrefixConfiguration(image *baselineWALImage, snapshot *raftpb.Snapshot) (uint64, error) {
	p := tracker.MakeProgressTracker(baselineTrackerInflight, 0)
	var n uint64
	start := uint64(1)
	if image.genesis != nil {
		config, progress, err := confchange.Restore(confchange.Changer{Tracker: p, LastIndex: image.lastIndex()}, image.genesis)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		p.Config, p.Progress = config, progress
		n, start = 1, 2
	}
	return baselineProvePrefix(image, snapshot, &p, n, start)
}

func baselineProvePrefix(image *baselineWALImage, snapshot *raftpb.Snapshot, p *tracker.ProgressTracker, n, start uint64) (uint64, error) {
	for i := start; i <= snapshot.GetMetadata().GetIndex(); i++ {
		entry := image.entries[i]
		if entry == nil {
			return 0, errors.Wrap(ErrOfflineBaseline, "missing ordered namespace proof and incomplete retained configuration history")
		}
		change, err := baselineConfChange(entry)
		if err != nil {
			return 0, err
		}
		if change != nil {
			if err := baselineApplyConfChange(p, change, image.lastIndex()); err != nil {
				return 0, err
			}
			n = i
		}
	}
	if n == 0 || !baselineSameConfState(p.ConfState(), snapshot.GetMetadata().GetConfState()) {
		return 0, errors.Wrap(ErrOfflineBaseline, "snapshot configuration not proven by complete history")
	}
	return n, nil
}

func baselineConfChange(entry *raftpb.Entry) (*raftpb.ConfChangeV2, error) {
	switch entry.GetType() {
	case raftpb.EntryNormal:
		return nil, nil
	case raftpb.EntryConfChange:
		cc := &raftpb.ConfChange{}
		if err := proto.Unmarshal(entry.Data, cc); err != nil {
			return nil, errors.WithStack(err)
		}
		return cc.AsV2(), nil
	case raftpb.EntryConfChangeV2:
		cc := &raftpb.ConfChangeV2{}
		if err := proto.Unmarshal(entry.Data, cc); err != nil {
			return nil, errors.WithStack(err)
		}
		return cc, nil
	default:
		return nil, errors.Wrap(ErrOfflineBaseline, "unknown retained entry type")
	}
}

func baselineApplyConfChange(p *tracker.ProgressTracker, cc *raftpb.ConfChangeV2, last uint64) error {
	c := confchange.Changer{Tracker: *p, LastIndex: last}
	var config tracker.Config
	var progress tracker.ProgressMap
	var err error
	if cc.LeaveJoint() {
		config, progress, err = c.LeaveJoint()
	} else if auto, joint := cc.EnterJoint(); joint {
		config, progress, err = c.EnterJoint(auto, cc.Changes...)
	} else {
		config, progress, err = c.Simple(cc.Changes...)
	}
	if err != nil {
		return errors.WithStack(err)
	}
	p.Config, p.Progress = config, progress
	return nil
}
