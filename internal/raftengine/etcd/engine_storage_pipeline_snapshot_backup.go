package etcd

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

const pipelineSnapshotBackupSuffix = ".normalize-backup"

// Native SaveSnap writes and may remove the final .snap pathname on failure.
// Keep the prior selected snapshot at a durable alternate pathname until the
// replacement and its WAL record are durable. Startup rolls back any leftover
// backup before orphan cleanup or WAL snapshot selection.
func persistPipelineRecoveredSnapshot(persist interface{ SaveSnap(*raftpb.Snapshot) error }, n *pipelineSnapshotNormalization) error {
	if n == nil || n.snapshot == nil || n.path == "" {
		return errors.WithStack(errInvalidStorageWork)
	}
	backup, err := reservePipelineSnapshotBackup(n.path)
	if err != nil {
		return err
	}
	dir := filepath.Dir(n.path)
	if err := persist.SaveSnap(n.snapshot); err != nil {
		return errors.WithStack(err)
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	if err := os.Remove(backup); err != nil {
		return errors.WithStack(err)
	}
	return syncDir(dir)
}

func reservePipelineSnapshotBackup(path string) (string, error) {
	backup := path + pipelineSnapshotBackupSuffix
	if _, err := os.Lstat(backup); err == nil {
		return "", errors.Wrap(errInvalidStorageWork, "snapshot normalization backup already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", errors.WithStack(err)
	}
	if err := os.Rename(path, backup); err != nil {
		return "", errors.WithStack(err)
	}
	return backup, syncDir(filepath.Dir(path))
}

func recoverPipelineSnapshotBackups(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, pipelineSnapshotBackupSuffix) {
			continue
		}
		original := strings.TrimSuffix(name, pipelineSnapshotBackupSuffix)
		if !validPipelineSnapshotName(original) || !entry.Type().IsRegular() {
			return errors.Wrapf(errInvalidStorageWork, "invalid snapshot normalization backup %q", name)
		}
		backup := filepath.Join(dir, name)
		if err := validatePipelineSnapshotBackup(backup, original); err != nil {
			return errors.Wrap(err, "invalid snapshot normalization backup")
		}
		if err := os.Rename(backup, filepath.Join(dir, original)); err != nil {
			return errors.WithStack(err)
		}
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
}

func validatePipelineSnapshotBackup(path, name string) (err error) {
	file, err := os.Open(path)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { err = errors.CombineErrors(err, file.Close()) }()
	data, err := verifiedPipelineRaftFile(file)
	if err != nil {
		return err
	}
	inner, err := scanPipelineWireFile(file, data)
	if err != nil {
		return err
	}
	metadata, err := readPipelineRaftMetadata(file, inner.bytes[2])
	if err != nil {
		return err
	}
	term, _ := strconv.ParseUint(name[:16], 16, 64)
	index, _ := strconv.ParseUint(name[17:33], 16, 64)
	if metadata.GetTerm() != term || metadata.GetIndex() != index {
		return errors.WithStack(errInvalidStorageWork)
	}
	return nil
}

func validPipelineSnapshotName(name string) bool {
	if len(name) != 16+1+16+len(".snap") || name[16] != '-' || !strings.HasSuffix(name, ".snap") {
		return false
	}
	if _, err := strconv.ParseUint(name[:16], 16, 64); err != nil {
		return false
	}
	_, err := strconv.ParseUint(name[17:33], 16, 64)
	return err == nil
}
