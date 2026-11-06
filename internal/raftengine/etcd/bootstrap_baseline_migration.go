package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type OfflineBaselineMigration struct {
	Directory, BackupDir, PreparedWALDir string
	Phase                                string
}

type baselineMigrationJournal struct {
	phase                                     byte
	manifest, source, backup, newWAL          raftbootstrap.Digest
	preparedAux, preparedData, transformation raftbootstrap.Digest
}

const baselineJournalSize = 8 + 1 + 7*32 + 32
const (
	baselinePreparing        byte = 1
	baselineStaging          byte = 2
	baselinePrepared         byte = 3
	baselineBound            byte = 4
	baselinePrivateDirectory      = 0o700
	baselinePrivateFile           = 0o600
)

// PrepareOfflineBaseline does not swap the source WAL. It produces a durable,
// independently reverified replacement and retains a full source-directory backup.
func PrepareOfflineBaseline(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest) (OfflineBaselineMigration, error) {
	if err := raftbootstrap.VerifyBaselineManifest(manifest, o.Expected); err != nil {
		return OfflineBaselineMigration{}, errors.WithStack(err)
	}
	inspection, err := inspectOfflineBaseline(ctx, o, nil)
	if err != nil {
		return OfflineBaselineMigration{}, err
	}
	if err := matchBaselineLocalEvidence(inspection.public.Evidence, manifest, o.MemberID); err != nil {
		return OfflineBaselineMigration{}, err
	}
	paths := baselineMigrationPaths(o)
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	if err != nil {
		return OfflineBaselineMigration{}, errors.WithStack(err)
	}
	journal := baselineMigrationJournal{phase: baselinePreparing, manifest: digest, source: inspection.public.Evidence.NativeStateDigest}
	journal.transformation, err = baselineTransformationDigest(o)
	if err != nil {
		return OfflineBaselineMigration{}, err
	}
	if err := initializeBaselineJournal(paths.Directory, journal); err != nil {
		return OfflineBaselineMigration{}, err
	}
	return prepareBaselineReplacement(ctx, o, manifest, paths, journal, inspection)
}

// ResumeOfflineBaseline resumes backup/preparation or atomically exchanges the
// verified WAL directories. Both old WAL and full backup are retained permanently.
func ResumeOfflineBaseline(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest) (OfflineBaselineMigration, error) {
	if err := raftbootstrap.VerifyBaselineManifest(manifest, o.Expected); err != nil {
		return OfflineBaselineMigration{}, errors.WithStack(err)
	}
	paths := baselineMigrationPaths(o)
	if _, err := verifyBaselineOwnership(o); err != nil {
		return OfflineBaselineMigration{}, err
	}
	journal, err := readValidatedBaselineJournal(o, manifest, paths.Directory)
	if err != nil {
		return resumeBaselineInitialJournal(ctx, o, manifest, paths.Directory, err)
	}
	if journal.phase == baselinePreparing {
		inspection, err := inspectOfflineBaseline(ctx, o, nil)
		if err != nil {
			return OfflineBaselineMigration{}, err
		}
		if inspection.public.Evidence.NativeStateDigest != journal.source {
			return OfflineBaselineMigration{}, errors.Wrap(ErrOfflineBaseline, "source changed during interrupted prepare")
		}
		return prepareBaselineReplacement(ctx, o, manifest, paths, journal, inspection)
	}
	if err := verifyCompleteBaselineBackup(ctx, o, manifest, paths, journal); err != nil {
		return paths, err
	}
	if journal.phase == baselineStaging {
		return stageBaselinePreparation(ctx, o, manifest, paths, journal)
	}
	return exchangeBaselineWAL(ctx, o, manifest, paths, journal)
}

func resumeBaselineInitialJournal(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, dir string, journalErr error) (OfflineBaselineMigration, error) {
	if !os.IsNotExist(errors.UnwrapAll(journalErr)) {
		return OfflineBaselineMigration{}, journalErr
	}
	// Only an existing initial directory is resumable. Prepare rechecks
	// signed source evidence before touching any interrupted journal.
	if err := baselineRegularDirectory(dir); err != nil {
		return OfflineBaselineMigration{}, err
	}
	return PrepareOfflineBaseline(ctx, o, manifest)
}

func readValidatedBaselineJournal(o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, dir string) (baselineMigrationJournal, error) {
	journal, err := readBaselineJournal(dir)
	if err != nil {
		return journal, err
	}
	digest, err := raftbootstrap.BaselineManifestDigest(manifest)
	if err != nil || journal.manifest != digest {
		return journal, errors.Wrap(ErrOfflineBaseline, "foreign migration journal")
	}
	transformation, err := baselineTransformationDigest(o)
	if err != nil || transformation != journal.transformation {
		return journal, errors.Wrap(ErrOfflineBaseline, "preparation transformation changed")
	}
	return journal, nil
}

func baselineMigrationPaths(o OfflineBaselineOptions) OfflineBaselineMigration {
	dir := filepath.Clean(o.DataDir) + ".baseline-" + hex.EncodeToString(o.Expected.OperationID[:])
	return OfflineBaselineMigration{Directory: dir, BackupDir: filepath.Join(dir, "backup"), PreparedWALDir: filepath.Join(dir, "replacement-wal")}
}

func matchBaselineLocalEvidence(local raftbootstrap.BaselineMemberEvidence, manifest raftbootstrap.BaselineManifest, member uint64) error {
	for _, evidence := range manifest.Members {
		if evidence.MemberID != member {
			continue
		}
		if baselineEvidenceMatches(evidence, local) {
			return nil
		}
		return errors.Wrap(ErrOfflineBaseline, "signed local evidence does not match offline source")
	}
	return errors.Wrap(ErrOfflineBaseline, "no signed local evidence")
}

func baselineEvidenceMatches(a, b raftbootstrap.BaselineMemberEvidence) bool {
	return a.NativeStateDigest == b.NativeStateDigest && a.ConfigurationDigest == b.ConfigurationDigest && a.DataDigest == b.DataDigest &&
		a.AppliedIndex == b.AppliedIndex && a.CommitIndex == b.CommitIndex && a.CommitTerm == b.CommitTerm && a.LastLogIndex == b.LastLogIndex && a.Watermark == b.Watermark
}

func prepareBaselineReplacement(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, paths OfflineBaselineMigration, journal baselineMigrationJournal, original *baselineInspection) (OfflineBaselineMigration, error) {
	if err := matchBaselineLocalEvidence(original.public.Evidence, manifest, o.MemberID); err != nil {
		return paths, err
	}
	backupDigest, err := createBaselineBackup(ctx, o.DataDir, paths.BackupDir)
	if err != nil {
		return paths, err
	}
	replacement, err := buildBaselineReplacement(o, paths.PreparedWALDir, journal.manifest, original.wal)
	if err != nil {
		return paths, err
	}
	// Re-read the local source AND Store after backup/build; a signed probe is
	// not permission to migrate a changed source.
	current, err := inspectOfflineBaseline(ctx, o, nil)
	if err != nil {
		return paths, err
	}
	if current.public.Evidence.NativeStateDigest != journal.source {
		return paths, errors.Wrap(ErrOfflineBaseline, "source changed during prepare")
	}
	journal.phase, journal.backup, journal.newWAL = baselineStaging, backupDigest, replacement.fileDigest
	if err := writeBaselineJournal(paths.Directory, journal); err != nil {
		return paths, err
	}
	return stageBaselinePreparation(ctx, o, manifest, paths, journal)
}

func createBaselineBackup(ctx context.Context, source, backup string) (raftbootstrap.Digest, error) {
	var zero raftbootstrap.Digest
	if err := copyBaselineTree(ctx, source, backup); err != nil {
		return zero, err
	}
	backupDigest, err := baselineTreeDigest(ctx, backup, "")
	if err != nil {
		return zero, err
	}
	sourceDigest, err := baselineTreeDigest(ctx, source, "")
	if err != nil || backupDigest != sourceDigest {
		return zero, errors.Wrap(ErrOfflineBaseline, "backup/source mismatch")
	}
	return backupDigest, baselineSyncDirectory(filepath.Dir(backup))
}

func buildBaselineReplacement(o OfflineBaselineOptions, dir string, preparation raftbootstrap.Digest, original *baselineWALImage) (*baselineWALImage, error) {
	identity, err := baselineDestinationIdentity(o, preparation)
	if err != nil {
		return nil, err
	}
	if _, err := os.Lstat(dir); err == nil {
		// Only this exact journal's disposable replacement is rebuilt. Never
		// remove or rewrite the source WAL or the full backup.
		if err := baselineRegularDirectory(dir); err != nil {
			return nil, err
		}
		if err := os.RemoveAll(dir); err != nil {
			return nil, errors.WithStack(err)
		}
	} else if !os.IsNotExist(err) {
		return nil, errors.WithStack(err)
	}
	if err := writeBaselineReplacement(dir, original, identity); err != nil {
		return nil, err
	}
	replacement, err := readBaselineWAL(dir)
	if err != nil {
		return nil, err
	}
	if err := verifyWALIdentity(replacement.metadata, identity); err != nil {
		return nil, err
	}
	if !original.equivalent(replacement) {
		return nil, errors.Wrap(ErrOfflineBaseline, "replacement changed retained WAL state")
	}
	return replacement, nil
}

func baselineDestinationIdentity(o OfflineBaselineOptions, preparation raftbootstrap.Digest) (*WALIdentity, error) {
	if preparation == (raftbootstrap.Digest{}) {
		return nil, errors.Wrap(ErrOfflineBaseline, "full preparation manifest digest required")
	}
	digest, err := raftbootstrap.ConfigurationDigest(o.Expected.Configuration)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &WALIdentity{Identity: o.Expected.Identity, NodeID: o.MemberID, BaselineDigest: digest, PreparationDigest: preparation}, nil
}

func writeBaselineReplacement(dir string, image *baselineWALImage, identity *WALIdentity) (retErr error) {
	metadata, err := encodeWALIdentity(identity)
	if err != nil {
		return err
	}
	w, err := wal.Create(zap.NewNop(), dir, metadata)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { retErr = errors.CombineErrors(retErr, w.Close()) }()
	stream := newBaselineWALImage()
	stream.recordSink = func(record *walpb.Record) error { return writeBaselineRecord(w, record) }
	var crc uint32
	for _, path := range image.segmentPaths {
		crc, err = readBaselineSegment(path, stream, crc)
		if err != nil {
			return err
		}
	}
	if !image.equivalent(stream) {
		return errors.Wrap(ErrOfflineBaseline, "source WAL changed while streaming replacement")
	}
	return errors.WithStack(w.Sync())
}

func writeBaselineRecord(w *wal.WAL, record *walpb.Record) error {
	switch record.GetType() {
	case wal.EntryType:
		entry := &raftpb.Entry{}
		if err := proto.Unmarshal(record.Data, entry); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(w.Save(&raftpb.HardState{}, []*raftpb.Entry{entry}))
	case wal.StateType:
		state := &raftpb.HardState{}
		if err := proto.Unmarshal(record.Data, state); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(w.Save(state, nil))
	case wal.SnapshotType:
		snapshot := &walpb.Snapshot{}
		if err := proto.Unmarshal(record.Data, snapshot); err != nil {
			return errors.WithStack(err)
		}
		return errors.WithStack(w.SaveSnapshot(snapshot))
	case wal.MetadataType:
		// This is the only intentionally replaced record.
	default:
		return errors.Wrap(ErrOfflineBaseline, "unexpected replacement record")
	}
	return nil
}

func exchangeBaselineWAL(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, paths OfflineBaselineMigration, journal baselineMigrationJournal) (OfflineBaselineMigration, error) {
	backup, err := baselineTreeDigest(ctx, paths.BackupDir, "")
	if err != nil || backup != journal.backup {
		return paths, errors.Wrap(ErrOfflineBaseline, "backup changed")
	}
	liveDir := filepath.Join(o.DataDir, walDirName)
	live, err := readBaselineWAL(liveDir)
	if err != nil {
		return paths, err
	}
	identity, err := baselineDestinationIdentity(o, journal.manifest)
	if err != nil {
		return paths, err
	}
	if live.fileDigest == journal.newWAL {
		return finishBoundBaseline(ctx, o, manifest, paths, journal, identity)
	}
	if err := verifyBaselineExchange(ctx, o, manifest, paths, journal, live, identity); err != nil {
		return paths, err
	}
	if err := swapBaselineWAL(o, paths); err != nil {
		return paths, err
	}
	// This same branch is used after a crash immediately following exchange.
	return exchangeBaselineWAL(ctx, o, manifest, paths, journal)
}

func finishBoundBaseline(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, paths OfflineBaselineMigration, journal baselineMigrationJournal, identity *WALIdentity) (OfflineBaselineMigration, error) {
	if err := verifyBaselineExchangeBarrier(o, identity); err != nil {
		return paths, err
	}
	if _, _, err := verifyBaselinePreparedTarget(ctx, o, manifest, paths, journal, identity); err != nil {
		return paths, err
	}
	if err := baselineSyncDirectory(o.DataDir); err != nil {
		return paths, err
	}
	if err := baselineSyncDirectory(paths.Directory); err != nil {
		return paths, err
	}
	journal.phase = baselineBound
	if err := writeBaselineJournal(paths.Directory, journal); err != nil {
		return paths, err
	}
	paths.Phase = "bound"
	return paths, nil
}

func verifyBaselineExchange(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, paths OfflineBaselineMigration, journal baselineMigrationJournal, live *baselineWALImage, identity *WALIdentity) error {
	if err := verifyBaselineExchangeBarrier(o, identity); err != nil {
		return err
	}
	if journal.phase != baselinePrepared {
		return errors.Wrap(ErrOfflineBaseline, "bound migration lost destination WAL")
	}
	current, _, err := verifyBaselinePreparedTarget(ctx, o, manifest, paths, journal, nil)
	if err != nil {
		return err
	}
	if current.auxDigest != journal.preparedAux {
		return errors.Wrap(ErrOfflineBaseline, "prepared source changed before swap")
	}
	replacement, err := readBaselineWAL(paths.PreparedWALDir)
	if err != nil {
		return err
	}
	if replacement.fileDigest != journal.newWAL || !live.equivalent(replacement) {
		return errors.Wrap(ErrOfflineBaseline, "replacement changed before swap")
	}
	return verifyWALIdentity(replacement.metadata, identity)
}

func verifyBaselineExchangeBarrier(o OfflineBaselineOptions, identity *WALIdentity) error {
	if o.Preparation == nil {
		return errors.Wrap(ErrOfflineBaseline, "v2 exchange requires exact preparation and durable format barrier")
	}
	_, err := verifyBaselineRequiredIntent(o, identity)
	return err
}

func swapBaselineWAL(o OfflineBaselineOptions, paths OfflineBaselineMigration) error {
	ownership, err := verifyBaselineOwnership(o)
	if err != nil {
		return err
	}
	if err := ownership.retainWAL(paths.PreparedWALDir); err != nil {
		return err
	}
	if err := ownership.exchangeWAL(filepath.Join(o.DataDir, walDirName), paths.PreparedWALDir); err != nil {
		return err
	}
	if err := baselineSyncDirectory(o.DataDir); err != nil {
		return err
	}
	if err := baselineSyncDirectory(paths.Directory); err != nil {
		return err
	}
	return nil
}

func initializeBaselineJournal(dir string, journal baselineMigrationJournal) error {
	if err := os.Mkdir(dir, baselinePrivateDirectory); err != nil && !os.IsExist(err) {
		return errors.WithStack(err)
	}
	if err := validateBaselineInitialDirectory(dir, encodeBaselineJournal(journal)); err != nil {
		return err
	}
	if err := baselineSyncDirectory(filepath.Dir(dir)); err != nil {
		return err
	}
	// Unlink only the validated initial temporary name. Do not truncate an
	// existing inode, which could also be linked outside the migration directory.
	if err := os.Remove(filepath.Join(dir, "journal.tmp")); err != nil && !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	return writeBaselineJournal(dir, journal)
}

func validateBaselineInitialDirectory(dir string, expected []byte) error {
	if err := baselineRegularDirectory(dir); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(entries) == 0 {
		return nil
	}
	if len(entries) != 1 || entries[0].Name() != "journal.tmp" {
		return errors.Wrap(ErrOfflineBaseline, "initial migration directory contains unexpected files; use resume for a published journal")
	}
	path := filepath.Join(dir, "journal.tmp")
	if err := baselineRegularFile(path); err != nil {
		return err
	}
	file, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	data, readErr := io.ReadAll(io.LimitReader(file, baselineJournalSize+1))
	if err := errors.CombineErrors(readErr, file.Close()); err != nil {
		return errors.WithStack(err)
	}
	if !bytes.HasPrefix(expected, data) {
		return errors.Wrap(ErrOfflineBaseline, "interrupted initial journal differs from reverified source or manifest")
	}
	return nil
}

func encodeBaselineJournal(journal baselineMigrationJournal) []byte {
	data := append([]byte("EKVBLJ02"), journal.phase)
	for _, digest := range []raftbootstrap.Digest{journal.manifest, journal.source, journal.backup, journal.newWAL, journal.preparedAux, journal.preparedData, journal.transformation} {
		data = append(data, digest[:]...)
	}
	digest := sha256.Sum256(data)
	return append(data, digest[:]...)
}

func writeBaselineJournal(dir string, journal baselineMigrationJournal) error {
	data := encodeBaselineJournal(journal)
	file, err := os.OpenFile(filepath.Join(dir, "journal.tmp"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, baselinePrivateFile)
	if err != nil {
		return errors.WithStack(err)
	}
	_, writeErr := file.Write(data)
	if writeErr == nil {
		writeErr = file.Sync()
	}
	closeErr := file.Close()
	if err := errors.CombineErrors(writeErr, closeErr); err != nil {
		return errors.WithStack(err)
	}
	if err := os.Rename(filepath.Join(dir, "journal.tmp"), filepath.Join(dir, "journal")); err != nil {
		return errors.WithStack(err)
	}
	return baselineSyncDirectory(dir)
}

func readBaselineJournal(dir string) (baselineMigrationJournal, error) {
	var journal baselineMigrationJournal
	if err := baselineRegularDirectory(dir); err != nil {
		return journal, err
	}
	if err := baselineRegularFile(filepath.Join(dir, "journal")); err != nil {
		return journal, err
	}
	data, err := os.ReadFile(filepath.Join(dir, "journal"))
	if err != nil {
		return journal, errors.WithStack(err)
	}
	if len(data) != baselineJournalSize || string(data[:8]) != "EKVBLJ02" {
		return journal, errors.Wrap(ErrOfflineBaseline, "invalid migration journal")
	}
	digest := sha256.Sum256(data[:len(data)-32])
	if !bytes.Equal(digest[:], data[len(data)-32:]) || data[8] < baselinePreparing || data[8] > baselineBound {
		return journal, errors.Wrap(ErrOfflineBaseline, "corrupt migration journal")
	}
	journal.phase = data[8]
	for i, target := range []*raftbootstrap.Digest{&journal.manifest, &journal.source, &journal.backup, &journal.newWAL, &journal.preparedAux, &journal.preparedData, &journal.transformation} {
		copy(target[:], data[9+i*32:9+(i+1)*32])
	}
	return journal, nil
}

func baselineSyncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer f.Close()
	return errors.WithStack(f.Sync())
}

func copyBaselineTree(ctx context.Context, source, destination string) error {
	var directories []string
	err := filepath.WalkDir(source, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errors.WithStack(walkErr)
		}
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return errors.WithStack(err)
		}
		target := filepath.Join(destination, rel)
		if entry.IsDir() {
			if err := os.MkdirAll(target, baselinePrivateDirectory); err != nil {
				return errors.WithStack(err)
			}
			directories = append(directories, target)
			return baselineRegularDirectory(target)
		}
		return copyBaselineFile(path, target)
	})
	if err != nil {
		return errors.WithStack(err)
	}
	for i := len(directories) - 1; i >= 0; i-- {
		if err := baselineSyncDirectory(directories[i]); err != nil {
			return err
		}
	}
	return nil
}

func copyBaselineFile(source, destination string) error {
	if err := baselineRegularFile(source); err != nil {
		return err
	}
	if _, err := os.Lstat(destination); err == nil {
		if err := baselineRegularFile(destination); err != nil {
			return err
		}
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	src, err := os.Open(source)
	if err != nil {
		return errors.WithStack(err)
	}
	defer src.Close()
	dst, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, baselinePrivateFile)
	if err != nil {
		return errors.WithStack(err)
	}
	_, copyErr := io.Copy(dst, src)
	if copyErr == nil {
		copyErr = dst.Sync()
	}
	closeErr := dst.Close()
	if err := errors.CombineErrors(copyErr, closeErr); err != nil {
		return errors.WithStack(err)
	}
	return baselineSyncDirectory(filepath.Dir(destination))
}
