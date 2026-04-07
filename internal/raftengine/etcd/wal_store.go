package etcd

import (
	"bytes"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	etcdraft "go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	walDirName  = "wal"
	snapDirName = "snap"
)

var (
	errBootstrapRequired = errors.New("etcd raft persistent state is missing and bootstrap is disabled")
)

type diskState struct {
	Storage   *etcdraft.MemoryStorage
	Persist   etcdstorage.Storage
	LocalSnap raftpb.Snapshot
}

func openDiskState(cfg OpenConfig, peers []Peer) (*diskState, error) {
	logger := zap.NewNop()
	walDir := filepath.Join(cfg.DataDir, walDirName)
	snapDir := filepath.Join(cfg.DataDir, snapDirName)

	if err := os.MkdirAll(cfg.DataDir, defaultDirPerm); err != nil {
		return nil, errors.WithStack(err)
	}
	if err := os.MkdirAll(snapDir, defaultDirPerm); err != nil {
		return nil, errors.WithStack(err)
	}

	if wal.Exist(walDir) {
		return loadWalState(logger, walDir, snapDir, cfg.StateMachine, peers)
	}

	legacy, legacyErr := loadLegacyOrSplitState(cfg.DataDir)
	if legacyErr == nil {
		return migrateLegacyState(logger, walDir, snapDir, cfg.StateMachine, legacy, peers)
	}
	if legacyErr != nil && !os.IsNotExist(errors.UnwrapAll(legacyErr)) {
		return nil, legacyErr
	}

	if len(peers) == 1 {
		return bootstrapWalState(logger, walDir, snapDir, cfg.StateMachine, peers)
	}
	if !cfg.Bootstrap {
		return nil, errors.WithStack(errBootstrapRequired)
	}
	return bootstrapWalState(logger, walDir, snapDir, cfg.StateMachine, peers)
}

func bootstrapWalState(logger *zap.Logger, walDir string, snapDir string, fsm StateMachine, peers []Peer) (*diskState, error) {
	snapshotData, err := stateMachineSnapshotBytes(fsm)
	if err != nil {
		return nil, err
	}
	boot := bootstrapStateForPeers(peers, snapshotData)
	return persistBootState(logger, walDir, snapDir, fsm, boot)
}

func loadWalState(logger *zap.Logger, walDir string, snapDir string, fsm StateMachine, peers []Peer) (*diskState, error) {
	snapshotter := snap.New(logger, snapDir)
	snapshot, err := loadPersistedSnapshot(logger, walDir, snapshotter)
	if err != nil {
		return nil, err
	}
	if err := restoreSnapshotState(fsm, snapshot, peers); err != nil {
		return nil, err
	}

	w, err := wal.Open(logger, walDir, walSnapshotFor(snapshot))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	_, hardState, entries, err := w.ReadAll()
	if err != nil {
		_ = w.Close()
		return nil, errors.WithStack(err)
	}

	storage, err := newMemoryStorage(persistedState{
		HardState: hardState,
		Snapshot:  snapshot,
		Entries:   entries,
	})
	if err != nil {
		_ = w.Close()
		return nil, err
	}

	return &diskState{
		Storage:   storage,
		Persist:   etcdstorage.NewStorage(logger, w, snapshotter),
		LocalSnap: snapshot,
	}, nil
}

func loadPersistedSnapshot(logger *zap.Logger, walDir string, snapshotter *snap.Snapshotter) (raftpb.Snapshot, error) {
	walSnaps, err := wal.ValidSnapshotEntries(logger, walDir)
	if err != nil {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	loaded, err := snapshotter.LoadNewestAvailable(walSnaps)
	switch {
	case err == nil:
		return *loaded, nil
	case errors.Is(err, snap.ErrNoSnapshot):
		return raftpb.Snapshot{}, nil
	default:
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
}

func restoreSnapshotState(fsm StateMachine, snapshot raftpb.Snapshot, peers []Peer) error {
	if !etcdraft.IsEmptySnap(snapshot) && len(snapshot.Data) > 0 && fsm != nil {
		if err := fsm.Restore(bytes.NewReader(snapshot.Data)); err != nil {
			return errors.WithStack(err)
		}
	}
	return validateConfState(snapshot.Metadata.ConfState, peers)
}

func walSnapshotFor(snapshot raftpb.Snapshot) walpb.Snapshot {
	return walpb.Snapshot{
		Index:     snapshot.Metadata.Index,
		Term:      snapshot.Metadata.Term,
		ConfState: &snapshot.Metadata.ConfState,
	}
}

func migrateLegacyState(logger *zap.Logger, walDir string, snapDir string, fsm StateMachine, state persistedState, peers []Peer) (*diskState, error) {
	if !etcdraft.IsEmptySnap(state.Snapshot) && len(state.Snapshot.Data) > 0 {
		if err := fsm.Restore(bytes.NewReader(state.Snapshot.Data)); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	if err := validateConfState(state.Snapshot.Metadata.ConfState, peers); err != nil {
		return nil, err
	}
	return persistBootState(logger, walDir, snapDir, fsm, state)
}

func persistBootState(logger *zap.Logger, walDir string, snapDir string, fsm StateMachine, state persistedState) (*diskState, error) {
	if err := ensureWalDirs(walDir, snapDir); err != nil {
		return nil, err
	}
	if wal.Exist(walDir) {
		return loadWalState(logger, walDir, snapDir, fsm, nil)
	}

	w, err := wal.Create(logger, walDir, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	snapshotter := snap.New(logger, snapDir)
	persist := etcdstorage.NewStorage(logger, w, snapshotter)

	if err := saveBootstrapState(persist, state); err != nil {
		_ = persist.Close()
		return nil, err
	}

	storage, err := newMemoryStorage(state)
	if err != nil {
		_ = persist.Close()
		return nil, err
	}
	return &diskState{
		Storage:   storage,
		Persist:   persist,
		LocalSnap: state.Snapshot,
	}, nil
}

func ensureWalDirs(walDir string, snapDir string) error {
	if err := os.MkdirAll(filepath.Dir(walDir), defaultDirPerm); err != nil {
		return errors.WithStack(err)
	}
	if err := os.MkdirAll(snapDir, defaultDirPerm); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func saveBootstrapState(persist etcdstorage.Storage, state persistedState) error {
	if !etcdraft.IsEmptySnap(state.Snapshot) {
		if err := persist.SaveSnap(state.Snapshot); err != nil {
			return errors.WithStack(err)
		}
	}
	if !etcdraft.IsEmptyHardState(state.HardState) || len(state.Entries) > 0 {
		if err := persist.Save(state.HardState, state.Entries); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func stateMachineSnapshotBytes(fsm StateMachine) (data []byte, err error) {
	snapshot, err := fsm.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var buf bytes.Buffer
	if _, err := snapshot.WriteTo(&buf); err != nil {
		closeErr := errors.WithStack(snapshot.Close())
		return nil, errors.WithStack(errors.CombineErrors(errors.WithStack(err), closeErr))
	}
	if err := errors.WithStack(snapshot.Close()); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func bootstrapStateForPeers(peers []Peer, snapshotData []byte) persistedState {
	confState := confStateForPeers(peers)
	return persistedState{
		HardState: raftpb.HardState{
			Term:   1,
			Commit: 1,
		},
		Snapshot: raftpb.Snapshot{
			Data: snapshotData,
			Metadata: raftpb.SnapshotMetadata{
				ConfState: confState,
				Index:     1,
				Term:      1,
			},
		},
	}
}

func validateConfState(conf raftpb.ConfState, peers []Peer) error {
	if len(peers) == 0 {
		return nil
	}
	expected := confStateForPeers(peers)
	if len(conf.Voters) != len(expected.Voters) {
		return errors.Wrapf(errClusterMismatch, "expected %d voters got %d", len(expected.Voters), len(conf.Voters))
	}
	for i, voter := range conf.Voters {
		if voter != expected.Voters[i] {
			return errors.Wrapf(errClusterMismatch, "voter[%d]=%d expected %d", i, voter, expected.Voters[i])
		}
	}
	if len(conf.VotersOutgoing) > 0 || len(conf.Learners) > 0 || len(conf.LearnersNext) > 0 || conf.AutoLeave {
		return errors.Wrap(errClusterMismatch, "joint consensus state is not supported")
	}
	return nil
}

func loadLegacyOrSplitState(dataDir string) (persistedState, error) {
	state, err := loadStateFiles(dataDir)
	if err == nil {
		payload, readErr := os.ReadFile(snapshotDataFilePath(dataDir))
		if readErr == nil {
			state.Snapshot.Data = payload
		} else if !os.IsNotExist(errors.UnwrapAll(readErr)) {
			return persistedState{}, errors.WithStack(readErr)
		}
		return state, nil
	}
	if !os.IsNotExist(errors.UnwrapAll(err)) {
		return persistedState{}, err
	}
	return loadStateFile(stateFilePath(dataDir))
}

func persistReadyToWAL(persist etcdstorage.Storage, rd etcdraft.Ready) error {
	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		if err := persist.SaveSnap(rd.Snapshot); err != nil {
			return errors.WithStack(err)
		}
	}
	if !etcdraft.IsEmptyHardState(rd.HardState) || len(rd.Entries) > 0 {
		if err := persist.Save(rd.HardState, rd.Entries); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func persistLocalSnapshot(storage *etcdraft.MemoryStorage, persist etcdstorage.Storage, fsm StateMachine, applied uint64) (raftpb.Snapshot, error) {
	payload, err := stateMachineSnapshotBytes(fsm)
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	_, confState, err := storage.InitialState()
	if err != nil {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	snapshot, err := storage.CreateSnapshot(applied, &confState, payload)
	if err != nil {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	if err := persist.SaveSnap(snapshot); err != nil {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	if err := storage.Compact(applied); err != nil && !errors.Is(err, etcdraft.ErrCompacted) {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	if err := persist.Release(snapshot); err != nil {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	return snapshot, nil
}

func closePersist(persist etcdstorage.Storage) error {
	if persist == nil {
		return nil
	}
	return errors.WithStack(persist.Close())
}
