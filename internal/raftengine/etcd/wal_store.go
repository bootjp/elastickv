package etcd

import (
	"bytes"
	"io"
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

type diskState struct {
	Storage   *etcdraft.MemoryStorage
	Persist   etcdstorage.Storage
	LocalSnap raftpb.Snapshot
}

func openDiskState(cfg OpenConfig, peers []Peer) (*diskState, error) {
	logger := zap.NewNop()
	walDir := filepath.Join(cfg.DataDir, walDirName)
	snapDir := filepath.Join(cfg.DataDir, snapDirName)
	fsmSnapDir := filepath.Join(cfg.DataDir, fsmSnapDirName)

	if err := prepareDataDirs(cfg.DataDir, snapDir, fsmSnapDir); err != nil {
		return nil, err
	}

	if wal.Exist(walDir) {
		return loadWalState(logger, walDir, snapDir, fsmSnapDir, cfg.StateMachine)
	}

	legacy, legacyErr := loadLegacyOrSplitState(cfg.DataDir)
	if legacyErr == nil {
		return migrateLegacyState(logger, walDir, snapDir, fsmSnapDir, cfg.StateMachine, legacy)
	}
	if !os.IsNotExist(errors.UnwrapAll(legacyErr)) {
		return nil, legacyErr
	}

	return bootstrapNewCluster(logger, walDir, snapDir, fsmSnapDir, cfg, peers)
}

func prepareDataDirs(dataDir, snapDir, fsmSnapDir string) error {
	if err := os.MkdirAll(dataDir, defaultDirPerm); err != nil {
		return errors.WithStack(err)
	}
	if err := os.MkdirAll(snapDir, defaultDirPerm); err != nil {
		return errors.WithStack(err)
	}
	if err := cleanupStaleSnapshotSpools(dataDir); err != nil {
		return errors.Wrap(err, "cleanup stale snapshot spools")
	}
	// Startup CRC verification is disabled by default: for GiB-scale snapshots
	// reading the entire file to compute a checksum can add seconds to recovery
	// time. Orphan removal (index-based) still runs unconditionally. CRC
	// integrity is enforced at restore time by openAndRestoreFSMSnapshot.
	if err := cleanupStaleFSMSnaps(snapDir, fsmSnapDir, true); err != nil {
		return errors.Wrap(err, "cleanup stale fsm snapshots")
	}
	return nil
}

func bootstrapNewCluster(logger *zap.Logger, walDir, snapDir, fsmSnapDir string, cfg OpenConfig, peers []Peer) (*diskState, error) {
	if len(peers) == 1 {
		return bootstrapWalState(logger, walDir, snapDir, fsmSnapDir, cfg.StateMachine, peers)
	}
	if !cfg.Bootstrap {
		return joinWalState(logger, walDir, snapDir, fsmSnapDir)
	}
	return bootstrapWalState(logger, walDir, snapDir, fsmSnapDir, cfg.StateMachine, peers)
}

func joinWalState(logger *zap.Logger, walDir, snapDir, fsmSnapDir string) (*diskState, error) {
	return persistBootState(logger, walDir, snapDir, fsmSnapDir, nil, persistedState{})
}

func bootstrapWalState(logger *zap.Logger, walDir, snapDir, fsmSnapDir string, fsm StateMachine, peers []Peer) (*diskState, error) {
	// Bootstrap snapshot always has index 1.
	const bootstrapIndex = uint64(1)
	snapshot, err := fsm.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	crc32c, writeErr := writeFSMSnapshotFile(snapshot, fsmSnapDir, bootstrapIndex)
	closeErr := snapshot.Close()
	if writeErr != nil {
		return nil, writeErr
	}
	if closeErr != nil {
		return nil, errors.WithStack(closeErr)
	}
	token := encodeSnapshotToken(bootstrapIndex, crc32c)
	boot := bootstrapStateForPeers(peers, token)
	return persistBootState(logger, walDir, snapDir, fsmSnapDir, fsm, boot)
}

func loadWalState(logger *zap.Logger, walDir, snapDir, fsmSnapDir string, fsm StateMachine) (*diskState, error) {
	snapshotter := snap.New(logger, snapDir)
	snapshot, err := loadPersistedSnapshot(logger, walDir, snapshotter)
	if err != nil {
		return nil, err
	}
	if err := restoreSnapshotState(fsm, snapshot, fsmSnapDir); err != nil {
		return nil, err
	}

	w, hardState, entries, err := openAndReadWAL(logger, walDir, walSnapshotFor(snapshot))
	if err != nil {
		return nil, err
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

// openAndReadWAL opens the WAL at walDir and returns its hard state and
// entries. On io.ErrUnexpectedEOF from ReadAll — which happens when the
// kernel OOM-killer SIGKILLs the process mid-WAL-write, leaving the last
// preallocated segment with a partial trailing record — it invokes
// wal.Repair to truncate the partial record and retries the open once.
// This is the same recovery path etcd's own server uses on boot. CRC
// mismatches and other errors propagate unchanged: those are real
// corruption, not in-flight-write truncation.
func openAndReadWAL(logger *zap.Logger, walDir string, walSnap walpb.Snapshot) (*wal.WAL, raftpb.HardState, []raftpb.Entry, error) {
	repaired := false
	for {
		w, err := wal.Open(logger, walDir, walSnap)
		if err != nil {
			return nil, raftpb.HardState{}, nil, errors.WithStack(err)
		}
		_, hardState, entries, err := w.ReadAll()
		if err == nil {
			return w, hardState, entries, nil
		}
		_ = w.Close()
		if repaired || !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, raftpb.HardState{}, nil, errors.WithStack(err)
		}
		logger.Warn("WAL tail truncated, repairing",
			zap.String("dir", walDir),
			zap.Error(err),
		)
		if !wal.Repair(logger, walDir) {
			return nil, raftpb.HardState{}, nil, errors.Wrap(err, "WAL unrepairable")
		}
		repaired = true
	}
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

func restoreSnapshotState(fsm StateMachine, snapshot raftpb.Snapshot, fsmSnapDir string) error {
	if etcdraft.IsEmptySnap(snapshot) || len(snapshot.Data) == 0 || fsm == nil {
		return nil
	}
	if isSnapshotToken(snapshot.Data) {
		tok, err := decodeSnapshotToken(snapshot.Data)
		if err != nil {
			return err
		}
		return openAndRestoreFSMSnapshot(fsm, fsmSnapPath(fsmSnapDir, tok.Index), tok.CRC32C)
	}
	// Legacy format: full FSM payload embedded in snapshot.Data.
	return errors.WithStack(fsm.Restore(bytes.NewReader(snapshot.Data)))
}

func walSnapshotFor(snapshot raftpb.Snapshot) walpb.Snapshot {
	return walpb.Snapshot{
		Index:     snapshot.Metadata.Index,
		Term:      snapshot.Metadata.Term,
		ConfState: &snapshot.Metadata.ConfState,
	}
}

func migrateLegacyState(logger *zap.Logger, walDir, snapDir, fsmSnapDir string, fsm StateMachine, state persistedState) (*diskState, error) {
	if !etcdraft.IsEmptySnap(state.Snapshot) && len(state.Snapshot.Data) > 0 {
		token, err := restoreAndOffloadLegacySnapshot(fsm, fsmSnapDir, state.Snapshot)
		if err != nil {
			return nil, err
		}
		state.Snapshot.Data = token
	}
	return persistBootState(logger, walDir, snapDir, fsmSnapDir, fsm, state)
}

// restoreAndOffloadLegacySnapshot restores the FSM from a legacy full-payload
// snapshot and, when fsmSnapDir is non-empty, eagerly converts the payload to
// the disk-offloaded EKVT token format so the WAL no longer carries GiB-scale
// blobs after the first startup following a HashiCorp → etcd migration.
// Returns the token bytes (or the original Data slice when fsmSnapDir is empty).
func restoreAndOffloadLegacySnapshot(fsm StateMachine, fsmSnapDir string, snap raftpb.Snapshot) ([]byte, error) {
	if err := fsm.Restore(bytes.NewReader(snap.Data)); err != nil {
		return nil, errors.WithStack(err)
	}
	if fsmSnapDir == "" {
		return snap.Data, nil
	}
	index := snap.Metadata.Index
	fsmSnap, err := fsm.Snapshot()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	crc32c, writeErr := writeFSMSnapshotFile(fsmSnap, fsmSnapDir, index)
	closeErr := fsmSnap.Close()
	if writeErr != nil {
		return nil, writeErr
	}
	if closeErr != nil {
		return nil, errors.WithStack(closeErr)
	}
	return encodeSnapshotToken(index, crc32c), nil
}

func persistBootState(logger *zap.Logger, walDir, snapDir, fsmSnapDir string, fsm StateMachine, state persistedState) (*diskState, error) {
	if err := ensureWalDirs(walDir, snapDir); err != nil {
		return nil, err
	}
	if wal.Exist(walDir) {
		return loadWalState(logger, walDir, snapDir, fsmSnapDir, fsm)
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

// snapshotBytesAndClose materializes a Snapshot as []byte via disk spooling.
// Retained for the legacy migration path (migrateLegacyState) where the old
// format is still used. New code should call writeFSMSnapshotFile instead.
func snapshotBytesAndClose(snapshot Snapshot, spoolDir string) (data []byte, err error) {
	defer func() {
		err = errors.CombineErrors(err, errors.WithStack(snapshot.Close()))
	}()
	return snapshotBytes(snapshot, spoolDir)
}

func snapshotBytes(snapshot Snapshot, spoolDir string) (data []byte, err error) {
	spool, err := newSnapshotSpool(spoolDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.CombineErrors(err, spool.Close())
	}()

	if _, err := snapshot.WriteTo(spool); err != nil {
		return nil, errors.WithStack(err)
	}
	// The current raftpb snapshot APIs still require a materialized []byte, but
	// keeping the intermediate WriteTo path on disk avoids growing a second
	// in-memory buffer while the FSM snapshot is being serialized.
	data, err = spool.Bytes()
	if err != nil {
		return nil, err
	}
	return data, nil
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
	if err := cleanupReplaceTempFiles(dataDir); err != nil {
		return persistedState{}, err
	}
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

func persistLocalSnapshotPayload(storage *etcdraft.MemoryStorage, persist etcdstorage.Storage, applied uint64, payload []byte) (raftpb.Snapshot, error) {
	snapshot, err := buildLocalSnapshot(storage, applied, payload)
	if err != nil {
		return raftpb.Snapshot{}, err
	}
	if err := persist.SaveSnap(snapshot); err != nil {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	if _, err := storage.CreateSnapshot(applied, &snapshot.Metadata.ConfState, payload); err != nil {
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

// defaultMaxSnapFiles is the number of .snap/.fsm file pairs to retain.
// etcd itself purges old snap files via fileutil.PurgeFile; the elastickv
// etcd engine must do this explicitly, coordinating with .fsm files.
const defaultMaxSnapFiles = 3

func buildLocalSnapshot(storage *etcdraft.MemoryStorage, applied uint64, payload []byte) (raftpb.Snapshot, error) {
	_, confState, err := storage.InitialState()
	if err != nil {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	term, err := storage.Term(applied)
	if err != nil {
		return raftpb.Snapshot{}, errors.WithStack(err)
	}
	return raftpb.Snapshot{
		Data: payload,
		Metadata: raftpb.SnapshotMetadata{
			ConfState: confState,
			Index:     applied,
			Term:      term,
		},
	}, nil
}

func closePersist(persist etcdstorage.Storage) error {
	if persist == nil {
		return nil
	}
	return errors.WithStack(persist.Close())
}
