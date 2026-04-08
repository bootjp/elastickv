package etcd

import (
	"os"
	"path/filepath"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
)

const migrationTempSuffix = ".migrating"

type MigrationStats struct {
	SnapshotBytes int64
	Peers         int
}

func MigrateFSMStore(storePath string, destDataDir string, peers []Peer) (*MigrationStats, error) {
	destDataDir, tempDir, err := prepareMigrationDest(storePath, destDataDir, peers)
	if err != nil {
		return nil, err
	}
	snapshotData, snapshotBytes, err := readStoreSnapshot(storePath)
	if err != nil {
		return nil, err
	}
	if err := seedMigrationDir(tempDir, peers, snapshotData); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	if err := finalizeMigrationDir(tempDir, destDataDir); err != nil {
		return nil, err
	}
	return &MigrationStats{
		SnapshotBytes: snapshotBytes,
		Peers:         len(peers),
	}, nil
}

func prepareMigrationDest(storePath string, destDataDir string, peers []Peer) (string, string, error) {
	switch {
	case storePath == "":
		return "", "", errors.WithStack(errors.New("source FSM store path is required"))
	case destDataDir == "":
		return "", "", errors.WithStack(errors.New("destination data dir is required"))
	case len(peers) == 0:
		return "", "", errors.WithStack(errors.New("at least one peer is required"))
	}

	destDataDir = filepath.Clean(destDataDir)
	if err := ensureMigrationPathAbsent(destDataDir, "destination"); err != nil {
		return "", "", err
	}
	tempDir := destDataDir + migrationTempSuffix
	if err := ensureMigrationPathAbsent(tempDir, "temporary destination"); err != nil {
		return "", "", err
	}
	return destDataDir, tempDir, nil
}

func ensureMigrationPathAbsent(path string, kind string) error {
	if _, err := os.Stat(path); err == nil {
		return errors.WithStack(errors.Newf("%s already exists: %s", kind, path))
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	return nil
}

func readStoreSnapshot(storePath string) ([]byte, int64, error) {
	source, err := store.NewPebbleStore(storePath)
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	defer source.Close()

	snapshot, err := source.Snapshot()
	if err != nil {
		return nil, 0, errors.WithStack(err)
	}
	data, err := snapshotBytes(snapshot, storePath)
	if err != nil {
		return nil, 0, err
	}
	return data, int64(len(data)), nil
}

func seedMigrationDir(tempDir string, peers []Peer, snapshotData []byte) error {
	state := bootstrapStateForPeers(peers, snapshotData)
	logger := zap.NewNop()
	disk, err := persistBootState(logger, filepath.Join(tempDir, walDirName), filepath.Join(tempDir, snapDirName), nil, state)
	if err != nil {
		return err
	}
	if err := closePersist(disk.Persist); err != nil {
		return err
	}
	return nil
}

func finalizeMigrationDir(tempDir string, destDataDir string) error {
	if err := os.Rename(tempDir, destDataDir); err != nil {
		_ = os.RemoveAll(tempDir)
		return errors.WithStack(err)
	}
	if err := syncDir(filepath.Dir(destDataDir)); err != nil {
		_ = os.RemoveAll(destDataDir)
		return err
	}
	return nil
}
