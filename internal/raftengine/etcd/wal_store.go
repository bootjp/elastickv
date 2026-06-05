package etcd

import (
	"bytes"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"github.com/bootjp/elastickv/internal/raftengine"
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
		return loadWalState(logger, walDir, snapDir, fsmSnapDir, cfg.StateMachine, cfg.ColdStartObserver)
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
	// receiveSnapshotStream places its in-flight spool file inside
	// fsmSnapDir (not dataDir) to keep the FinalizeAsFSMFile rename
	// intra-filesystem, so a crash mid-receive can leak an
	// elastickv-etcd-snapshot-* into fsmSnapDir. Sweep that dir on
	// startup the same way we sweep dataDir.
	if err := cleanupStaleSnapshotSpools(fsmSnapDir); err != nil {
		return errors.Wrap(err, "cleanup stale snapshot spools (fsm-snap dir)")
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

func loadWalState(logger *zap.Logger, walDir, snapDir, fsmSnapDir string, fsm StateMachine, obs raftengine.ColdStartObserver) (*diskState, error) {
	// Scope the repair retry tightly to WAL-only reads: both
	// loadPersistedSnapshot (scans WAL via wal.ValidSnapshotEntries)
	// and openAndReadWAL's ReadAll can surface io.ErrUnexpectedEOF
	// when the kernel OOM-killer SIGKILLed the process mid-WAL-write.
	// wal.Repair truncates the partial trailing record once and is
	// idempotent. FSM snapshot restore is kept out of this retry —
	// a truncated .fsm payload surfacing ErrUnexpectedEOF is a
	// different failure mode (the FSM snapshotter has its own
	// on-disk CRC footer) and wal.Repair does not address it;
	// running repair in that case would dirty a perfectly-good WAL.
	snapshotter := snap.New(logger, snapDir)
	snapshot, err := loadPersistedSnapshotWithRepair(logger, walDir, snapshotter)
	if err != nil {
		return nil, err
	}
	if err := restoreSnapshotState(fsm, snapshot, fsmSnapDir, obs, logger); err != nil {
		return nil, err
	}

	w, hardState, entries, err := openAndReadWALWithRepair(logger, walDir, walSnapshotFor(snapshot))
	if err != nil {
		return nil, err
	}

	storage, err := newMemoryStorage(persistedState{
		HardState: hardState,
		Snapshot:  snapshot,
		Entries:   entries,
	})
	if err != nil {
		if closeErr := w.Close(); closeErr != nil {
			logger.Warn("WAL close failed after storage init error",
				zap.String("dir", walDir),
				zap.Error(closeErr),
			)
		}
		return nil, err
	}

	return &diskState{
		Storage:   storage,
		Persist:   etcdstorage.NewStorage(logger, w, snapshotter),
		LocalSnap: snapshot,
	}, nil
}

// loadPersistedSnapshotWithRepair wraps loadPersistedSnapshot with one
// wal.Repair attempt on io.ErrUnexpectedEOF. The caller passes in a
// shared snapshotter so loadWalState does not instantiate snap.New
// twice per open.
func loadPersistedSnapshotWithRepair(logger *zap.Logger, walDir string, snapshotter *snap.Snapshotter) (raftpb.Snapshot, error) {
	snapshot, err := loadPersistedSnapshot(logger, walDir, snapshotter)
	if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		return snapshot, err
	}
	logger.Warn("WAL tail truncated during snapshot scan, repairing",
		zap.String("dir", walDir),
		zap.Error(err),
	)
	if !wal.Repair(logger, walDir) {
		return raftpb.Snapshot{}, errors.Wrap(err, "WAL unrepairable")
	}
	snapshot, err = loadPersistedSnapshot(logger, walDir, snapshotter)
	if err != nil {
		return raftpb.Snapshot{}, errors.Wrap(err, "WAL unrepairable after repair")
	}
	return snapshot, nil
}

// openAndReadWALWithRepair wraps openAndReadWAL with one wal.Repair
// attempt on io.ErrUnexpectedEOF.
func openAndReadWALWithRepair(logger *zap.Logger, walDir string, walSnap walpb.Snapshot) (*wal.WAL, raftpb.HardState, []raftpb.Entry, error) {
	w, hs, ents, err := openAndReadWAL(logger, walDir, walSnap)
	if err == nil || !errors.Is(err, io.ErrUnexpectedEOF) {
		return w, hs, ents, err
	}
	logger.Warn("WAL tail truncated during ReadAll, repairing",
		zap.String("dir", walDir),
		zap.Error(err),
	)
	if !wal.Repair(logger, walDir) {
		return nil, raftpb.HardState{}, nil, errors.Wrap(err, "WAL unrepairable")
	}
	w, hs, ents, err = openAndReadWAL(logger, walDir, walSnap)
	if err != nil {
		return nil, raftpb.HardState{}, nil, errors.Wrap(err, "WAL unrepairable after repair")
	}
	return w, hs, ents, nil
}

// openAndReadWAL opens the WAL at walDir and runs ReadAll. io.ErrUnexpectedEOF
// and other errors propagate upward; the retry/repair is handled once at
// loadWalState so ValidSnapshotEntries and ReadAll share a single repair
// pass and the "WAL tail truncated" log is emitted at most once.
func openAndReadWAL(logger *zap.Logger, walDir string, walSnap walpb.Snapshot) (*wal.WAL, raftpb.HardState, []raftpb.Entry, error) {
	w, err := wal.Open(logger, walDir, walSnap)
	if err != nil {
		return nil, raftpb.HardState{}, nil, errors.WithStack(err)
	}
	_, hardState, entries, err := w.ReadAll()
	if err != nil {
		if closeErr := w.Close(); closeErr != nil {
			logger.Warn("WAL close failed after ReadAll error",
				zap.String("dir", walDir),
				zap.Error(closeErr),
			)
		}
		return nil, raftpb.HardState{}, nil, errors.WithStack(err)
	}
	return w, hardState, entries, nil
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

func restoreSnapshotState(fsm StateMachine, snapshot raftpb.Snapshot, fsmSnapDir string, obs raftengine.ColdStartObserver, logger *zap.Logger) error {
	if etcdraft.IsEmptySnap(snapshot) || len(snapshot.Data) == 0 || fsm == nil {
		return nil
	}
	if isSnapshotToken(snapshot.Data) {
		tok, err := decodeSnapshotToken(snapshot.Data)
		if err != nil {
			return err
		}
		// Branch 3 (PR #910 design §4): skip the multi-GiB body
		// restore when the on-disk FSM is already at least as fresh
		// as the snapshot pointer. The skip path still consumes the
		// v1/v2 header so the HLC ceiling + Stage 8a cutover are
		// preserved (PR #910 §5), and runs the same three-step CRC
		// verification (size + footer-vs-tokenCRC + full-body-CRC)
		// as openAndRestoreFSMSnapshot before mutating any FSM state.
		decision, have := decideSkipOutcome(fsm, tok.Index)
		reportColdStart(obs, logger, decision, tok.Index, have)
		if decision == coldStartSkip {
			return applyHeaderStateOnSkip(fsm, fsmSnapPath(fsmSnapDir, tok.Index), tok.CRC32C)
		}
		return openAndRestoreFSMSnapshot(fsm, fsmSnapPath(fsmSnapDir, tok.Index), tok.CRC32C)
	}
	// Legacy format: full FSM payload embedded in snapshot.Data.
	return errors.WithStack(fsm.Restore(bytes.NewReader(snapshot.Data)))
}

// coldStartDecision enumerates the three outcomes the skip gate
// distinguishes. Used together with ColdStartObserver labels to
// keep the metrics + log emitter centralised.
type coldStartDecision int

const (
	coldStartSkip coldStartDecision = iota
	coldStartExecute
	coldStartFallbackNotReader
	coldStartFallbackMissingMeta
	coldStartFallbackReadErr
)

func (d coldStartDecision) fallbackReason() string {
	switch d { //nolint:exhaustive // skip / execute return "" via default
	case coldStartFallbackNotReader:
		return "not_reader"
	case coldStartFallbackMissingMeta:
		return "missing_meta"
	case coldStartFallbackReadErr:
		return "read_err"
	default:
		return ""
	}
}

// decideSkipOutcome reads the FSM's durable applied index and
// classifies into one of the five outcomes. Returns (decision,
// haveIndex). haveIndex is meaningful only for skip / execute
// outcomes; the three fallback outcomes leave it at 0 because the
// store could not authoritatively report a value.
func decideSkipOutcome(fsm StateMachine, want uint64) (coldStartDecision, uint64) {
	r, ok := fsm.(raftengine.AppliedIndexReader)
	if !ok {
		return coldStartFallbackNotReader, 0
	}
	have, present, err := r.LastAppliedIndex()
	switch {
	case err != nil:
		return coldStartFallbackReadErr, 0
	case !present:
		return coldStartFallbackMissingMeta, 0
	case have < want:
		return coldStartExecute, have
	default:
		return coldStartSkip, have
	}
}

// reportColdStart dispatches the outcome to the observer + the
// engine logger. nil observer / nil logger no-op individually.
func reportColdStart(obs raftengine.ColdStartObserver, logger *zap.Logger, d coldStartDecision, snapIndex, have uint64) {
	switch d { //nolint:exhaustive // default groups the three fallback variants
	case coldStartSkip:
		if obs != nil {
			obs.RestoreSkipped(snapIndex, have)
		}
		if logger != nil {
			logger.Info("restoreSnapshotState skipped",
				zap.Uint64("fsm_applied", have),
				zap.Uint64("snapshot_index", snapIndex),
				zap.Uint64("gap_ahead", have-snapIndex),
			)
		}
	case coldStartExecute:
		if obs != nil {
			obs.RestoreExecuted(snapIndex, have)
		}
		if logger != nil {
			logger.Info("restoreSnapshotState executed (FSM behind snapshot)",
				zap.Uint64("fsm_applied", have),
				zap.Uint64("snapshot_index", snapIndex),
				zap.Uint64("gap_behind", snapIndex-have),
			)
		}
	default:
		// Fallback variants: the strictly-additive policy. We could
		// not even attempt the skip; the full restore runs.
		reason := d.fallbackReason()
		if obs != nil {
			obs.RestoreFallback(snapIndex, reason)
		}
		if logger != nil {
			logger.Info("restoreSnapshotState fallback to full restore",
				zap.Uint64("snapshot_index", snapIndex),
				zap.String("reason", reason),
			)
		}
	}
}

// applyHeaderStateOnSkip mirrors openAndRestoreFSMSnapshot's safety
// contract (size + footer-vs-tokenCRC + full-body-CRC) but applies
// only the header side-effects (HLC ceiling + Stage 8a cutover)
// instead of running the body restore. The body bytes are read for
// CRC coverage but discarded -- fsm.db already holds equivalent
// state, which is precisely the reason we're skipping the restore.
//
// FSMs that do not implement raftengine.SnapshotHeaderApplier
// silently no-op the apply phase -- the FSM has no header state to
// carry forward, and the CRC verification still runs (with no
// observable side-effect on success). On any verification failure
// the typed error propagates and FSM state stays untouched.
//
// See PR #910 design §5 round-7 (two-phase seam) + round-6
// (three-step CRC mirroring openAndRestoreFSMSnapshot).
func applyHeaderStateOnSkip(fsm StateMachine, snapPath string, tokenCRC uint32) error {
	file, err := os.Open(snapPath)
	if err != nil {
		return statFSMFileError(err)
	}
	defer func() { _ = file.Close() }()

	info, err := file.Stat()
	if err != nil {
		return errors.WithStack(err)
	}
	footer, err := verifyFSMSnapshotPrefix(file, info.Size(), snapPath, tokenCRC)
	if err != nil {
		return err
	}

	// Step 3: full-body CRC. Wrap the payload in a crc32 TeeReader
	// and hand it to the FSM's ParseSnapshotHeader for header parse
	// + drain. Every payload byte flows through h, matching
	// restoreAndComputeCRC's boundary in openAndRestoreFSMSnapshot.
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}
	payloadSize := info.Size() - fsmFooterSize
	h := crc32.New(crc32cTable)
	tee := io.TeeReader(io.LimitReader(file, payloadSize), h)

	setter, hasSetter := fsm.(raftengine.SnapshotHeaderApplier)
	ceiling, cutover, err := readSnapshotHeaderOrDrain(setter, hasSetter, tee)
	if err != nil {
		return err
	}

	if h.Sum32() != footer {
		return errors.Wrapf(ErrFSMSnapshotFileCRC,
			"path=%s footer=%08x computed=%08x", snapPath, footer, h.Sum32())
	}

	// All three checks passed; apply side-effects (pure assignment
	// in the FSM). Skipped silently when the FSM does not expose
	// the seam.
	if hasSetter {
		setter.ApplySnapshotHeader(ceiling, cutover)
	}
	return nil
}

// verifyFSMSnapshotPrefix runs the first two cheap checks of
// openAndRestoreFSMSnapshot's three-step contract: size and
// footer-vs-tokenCRC. Returns the on-disk footer value (caller
// reuses it for the step-3 full-body CRC compare). Typed errors
// surface unchanged.
func verifyFSMSnapshotPrefix(file *os.File, fileSize int64, snapPath string, tokenCRC uint32) (uint32, error) {
	if fileSize < fsmMinFileSize {
		return 0, errors.Wrapf(ErrFSMSnapshotTooSmall,
			"file too small: %d bytes (minimum %d)", fileSize, fsmMinFileSize)
	}
	footer, err := readFSMFooter(file, fileSize)
	if err != nil {
		return 0, err
	}
	if footer != tokenCRC {
		return 0, errors.Wrapf(ErrFSMSnapshotTokenCRC,
			"path=%s footer=%08x token=%08x", snapPath, footer, tokenCRC)
	}
	return footer, nil
}

// readSnapshotHeaderOrDrain branches on whether the FSM exposes the
// SnapshotHeaderApplier seam: when present, delegate to
// ParseSnapshotHeader (which parses the header AND drains the rest);
// otherwise drain the entire payload through the tee'd reader so the
// CRC pass covers every byte. The (ceiling, cutover) tuple is zero
// in the no-seam case -- the caller's ApplySnapshotHeader branch
// short-circuits on hasSetter, so the zero values are inert.
func readSnapshotHeaderOrDrain(setter raftengine.SnapshotHeaderApplier, hasSetter bool, tee io.Reader) (uint64, uint64, error) {
	if hasSetter {
		ceiling, cutover, err := setter.ParseSnapshotHeader(tee)
		if err != nil {
			return 0, 0, errors.WithStack(err)
		}
		return ceiling, cutover, nil
	}
	if _, err := io.Copy(io.Discard, tee); err != nil {
		return 0, 0, errors.WithStack(err)
	}
	return 0, 0, nil
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
		// Recursive load after bootstrap-style setup: no observer
		// needed because the engine has not handed one to us
		// (bootstrap path runs before OpenConfig wiring reaches
		// this point) and a fresh-bootstrap restore will be a no-op
		// anyway (the WAL was just created).
		return loadWalState(logger, walDir, snapDir, fsmSnapDir, fsm, nil)
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
	// Joint consensus markers are still rejected: learner add/promote
	// uses the simple V1 single-step ConfChange path which never sets
	// these. See docs/design/2026_04_26_proposed_raft_learner.md §4.2.
	if len(conf.VotersOutgoing) > 0 || len(conf.LearnersNext) > 0 || conf.AutoLeave {
		return errors.Wrap(errClusterMismatch, "joint consensus state is not supported")
	}
	expectedVoters, learnerSet := splitPeersBySuffrage(peers)
	if err := validateConfStateVoters(conf, expectedVoters); err != nil {
		return err
	}
	return validateConfStateLearners(conf, learnerSet)
}

// validateConfStateVoters checks that conf.Voters matches the voter
// peers element-wise. The element-wise comparison relies on the
// well-established voter ordering invariant: persistConfigState
// writes voters in ConfState.Voters order and normalizePeers
// preserves it.
func validateConfStateVoters(conf raftpb.ConfState, expectedVoters []uint64) error {
	if len(conf.Voters) != len(expectedVoters) {
		return errors.Wrapf(errClusterMismatch, "expected %d voters got %d", len(expectedVoters), len(conf.Voters))
	}
	for i, voter := range conf.Voters {
		if voter != expectedVoters[i] {
			return errors.Wrapf(errClusterMismatch, "voter[%d]=%d expected %d", i, voter, expectedVoters[i])
		}
	}
	return nil
}

// validateConfStateLearners is set-based, not element-wise. There is
// no prior writer-side ordering invariant for learners, so we do not
// pin one in the reader. A set comparison rejects both
// same-count-member-divergence and conf-learner-not-in-peers cases —
// see docs/design/2026_04_26_proposed_raft_learner.md §4.2 edit 3.
func validateConfStateLearners(conf raftpb.ConfState, expected map[uint64]struct{}) error {
	if len(conf.Learners) != len(expected) {
		return errors.Wrapf(errClusterMismatch, "expected %d learners got %d", len(expected), len(conf.Learners))
	}
	for _, nodeID := range conf.Learners {
		if _, ok := expected[nodeID]; !ok {
			return errors.Wrapf(errClusterMismatch, "learner %d not present in peers", nodeID)
		}
	}
	return nil
}

// splitPeersBySuffrage partitions a peers list into the ordered voter
// node-ID slice (preserving input order, which encodes the
// well-established voter ordering invariant) and a learner set
// (unordered).
func splitPeersBySuffrage(peers []Peer) ([]uint64, map[uint64]struct{}) {
	voters := make([]uint64, 0, len(peers))
	var learners map[uint64]struct{}
	for _, peer := range peers {
		if peer.Suffrage == SuffrageLearner {
			if learners == nil {
				learners = make(map[uint64]struct{})
			}
			learners[peer.NodeID] = struct{}{}
			continue
		}
		voters = append(voters, peer.NodeID)
	}
	return voters, learners
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
