package etcd

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/bootjp/elastickv/internal/raftbootstrap"
	"github.com/cockroachdb/errors"
)

type BaselinePreparationContext struct {
	DataDir, BackupDir string
	Manifest           raftbootstrap.BaselineManifest
}

type BaselinePreparedStoreProof struct {
	// Baseline is recomputed from the preserved original image and an exact
	// source/target logical diff, NOT obtained by ignoring unknown target keys.
	Baseline BaselineStoreProof
	// PreparedDataDigest hashes the complete target, including new metadata.
	PreparedDataDigest raftbootstrap.Digest
}

type BaselinePreparationCallbacks struct {
	// Exact relative Store roots / marker files owned by the transformation.
	// WAL, snapshot, FSM snapshot and peer files cannot be excluded. The
	// verifier must validate every logical record/file in these paths against
	// the original backup and this precise manifest/preparation ID.
	MutablePaths []string
	// Must be durable and idempotent. On restart it may accept only the original
	// image or an exact prefix of its own transformation, never arbitrary edits.
	StagePreparation    func(context.Context, BaselinePreparationContext) error
	VerifyPreparedStore func(context.Context, BaselinePreparationContext) (BaselinePreparedStoreProof, error)
}

func baselineTransformationDigest(o OfflineBaselineOptions) (raftbootstrap.Digest, error) {
	h := sha256.New()
	_, _ = h.Write([]byte("elastickv/offline-baseline/preparation-paths/v1\x00"))
	if o.Crypto != nil {
		if err := validateOfflineBaselineCrypto(o.Crypto); err != nil {
			return raftbootstrap.Digest{}, err
		}
		_, _ = h.Write([]byte("explicit-offline-crypto/v1\x00"))
		_, _ = h.Write(baselineAppendUint64(nil, o.Crypto.RaftEnvelopeCutoverIndex))
	}
	if o.Preparation == nil {
		var digest raftbootstrap.Digest
		copy(digest[:], h.Sum(nil))
		return digest, nil
	}
	if o.Preparation.StagePreparation == nil || o.Preparation.VerifyPreparedStore == nil {
		return raftbootstrap.Digest{}, errors.Wrap(ErrOfflineBaseline, "both preparation callbacks required")
	}
	paths := slices.Clone(o.Preparation.MutablePaths)
	slices.Sort(paths)
	for i, path := range paths {
		if err := validateBaselineMutablePath(path); err != nil {
			return raftbootstrap.Digest{}, err
		}
		if i > 0 && paths[i-1] == path {
			return raftbootstrap.Digest{}, errors.Wrap(ErrOfflineBaseline, "duplicate mutable preparation path")
		}
		frame := baselineAppendUint64(nil, uint64(len(path)))
		_, _ = h.Write(append(frame, path...))
	}
	var digest raftbootstrap.Digest
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func validateBaselineMutablePath(path string) error {
	if path == "" || path == "." || filepath.IsAbs(path) || filepath.Clean(path) != path || strings.HasPrefix(path, "..") {
		return errors.Wrap(ErrOfflineBaseline, "invalid mutable preparation path")
	}
	for _, immutable := range []string{walDirName, snapDirName, fsmSnapDirName, peersFileName} {
		if path == immutable || strings.HasPrefix(path, immutable+string(filepath.Separator)) || strings.HasPrefix(immutable, path+string(filepath.Separator)) {
			return errors.Wrap(ErrOfflineBaseline, "native recovery files cannot be preparation-mutable")
		}
	}
	return nil
}

func baselineImmutablePreparationDigest(ctx context.Context, root string, preparation *BaselinePreparationCallbacks) (raftbootstrap.Digest, error) {
	h := sha256.New()
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return errors.WithStack(walkErr)
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return errors.WithStack(err)
		}
		if baselinePreparationPathMutable(rel, preparation) {
			if entry.Type()&os.ModeSymlink != 0 {
				return errors.Wrap(ErrOfflineBaseline, "symlink preparation path")
			}
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		return baselineHashTreeEntry(ctx, h, root, walDirName, path, entry, nil)
	})
	if err != nil {
		return raftbootstrap.Digest{}, errors.WithStack(err)
	}
	var digest raftbootstrap.Digest
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

func baselinePreparationPathMutable(path string, preparation *BaselinePreparationCallbacks) bool {
	if preparation == nil {
		return false
	}
	return slices.Contains(preparation.MutablePaths, path)
}

func verifyBaselineImmutablePreparation(ctx context.Context, o OfflineBaselineOptions, paths OfflineBaselineMigration) error {
	source, err := baselineImmutablePreparationDigest(ctx, paths.BackupDir, o.Preparation)
	if err != nil {
		return err
	}
	target, err := baselineImmutablePreparationDigest(ctx, o.DataDir, o.Preparation)
	if err != nil {
		return err
	}
	if source != target {
		return errors.Wrap(ErrOfflineBaseline, "unexpected non-preparation source mutation")
	}
	return nil
}

func verifyBaselinePreparedTarget(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, paths OfflineBaselineMigration, journal baselineMigrationJournal, identity *WALIdentity) (*baselineInspection, raftbootstrap.Digest, error) {
	var zero raftbootstrap.Digest
	if err := verifyBaselineImmutablePreparation(ctx, o, paths); err != nil {
		return nil, zero, err
	}
	if o.Preparation == nil {
		inspection, err := inspectOfflineBaseline(ctx, o, identity)
		return inspection, o.Expected.DataDigest, err
	}
	proof, err := o.Preparation.VerifyPreparedStore(ctx, BaselinePreparationContext{DataDir: o.DataDir, BackupDir: paths.BackupDir, Manifest: manifest})
	if err != nil {
		return nil, zero, errors.WithStack(err)
	}
	if proof.PreparedDataDigest == zero {
		return nil, zero, errors.Wrap(ErrOfflineBaseline, "missing complete prepared image digest")
	}
	copyOptions := o
	copyOptions.ProbeStore = func(context.Context) (BaselineStoreProof, error) { return proof.Baseline, nil }
	inspection, err := inspectOfflineBaseline(ctx, copyOptions, identity)
	if err != nil {
		return nil, zero, err
	}
	if journal.phase >= baselinePrepared && (journal.preparedAux != inspection.auxDigest || journal.preparedData != proof.PreparedDataDigest) {
		return nil, zero, errors.Wrap(ErrOfflineBaseline, "prepared target changed after verification")
	}
	return inspection, proof.PreparedDataDigest, nil
}

func stageBaselinePreparation(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, paths OfflineBaselineMigration, journal baselineMigrationJournal) (OfflineBaselineMigration, error) {
	if err := verifyBaselineImmutablePreparation(ctx, o, paths); err != nil {
		return paths, err
	}
	if o.Preparation != nil {
		if err := o.Preparation.StagePreparation(ctx, BaselinePreparationContext{DataDir: o.DataDir, BackupDir: paths.BackupDir, Manifest: manifest}); err != nil {
			return paths, errors.WithStack(err)
		}
	}
	target, data, err := verifyBaselinePreparedTarget(ctx, o, manifest, paths, journal, nil)
	if err != nil {
		return paths, err
	}
	original, err := readBaselineWAL(filepath.Join(paths.BackupDir, walDirName))
	if err != nil {
		return paths, err
	}
	if target.wal.fileDigest != original.fileDigest {
		return paths, errors.Wrap(ErrOfflineBaseline, "preparation changed original WAL")
	}
	journal.phase, journal.preparedAux, journal.preparedData = baselinePrepared, target.auxDigest, data
	if err := writeBaselineJournal(paths.Directory, journal); err != nil {
		return paths, err
	}
	paths.Phase = "prepared"
	return paths, nil
}

func verifyCompleteBaselineBackup(ctx context.Context, o OfflineBaselineOptions, manifest raftbootstrap.BaselineManifest, paths OfflineBaselineMigration, journal baselineMigrationJournal) error {
	digest, err := baselineTreeDigest(ctx, paths.BackupDir, "")
	if err != nil {
		return err
	}
	if digest != journal.backup {
		return errors.Wrap(ErrOfflineBaseline, "original backup changed")
	}
	image, err := readBaselineWAL(filepath.Join(paths.BackupDir, walDirName))
	if err != nil {
		return err
	}
	aux, err := baselineTreeDigest(ctx, paths.BackupDir, walDirName)
	if err != nil {
		return err
	}
	h := sha256.New()
	_, _ = h.Write([]byte("elastickv/offline-baseline/native/v1\x00"))
	_, _ = h.Write(image.fileDigest[:])
	_, _ = h.Write(aux[:])
	var native raftbootstrap.Digest
	copy(native[:], h.Sum(nil))
	if native != journal.source {
		return errors.Wrap(ErrOfflineBaseline, "original native source proof changed")
	}
	for _, evidence := range manifest.Members {
		if evidence.MemberID == o.MemberID && evidence.NativeStateDigest == native {
			return nil
		}
	}
	return errors.Wrap(ErrOfflineBaseline, "backup is not the signed original member image")
}
