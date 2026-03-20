package raftstore

import (
	"bytes"
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/hashicorp/go-msgpack/v2/codec"
	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
)

const (
	legacyLogsBucket      = "logs"
	legacyStableBucket    = "conf"
	legacyBatchSize       = 1024
	legacyBoltFileMode    = 0o600
	legacyMigrationSuffix = ".migrating"
)

type MigrationStats struct {
	Logs       uint64
	StableKeys uint64
}

func MigrateLegacyBoltDB(logsPath, stablePath, destDir string) (*MigrationStats, error) {
	tempDir, err := prepareMigrationPaths(logsPath, stablePath, destDir)
	if err != nil {
		return nil, err
	}

	logsDB, stableDB, closeSources, err := openLegacySourceDBs(logsPath, stablePath)
	if err != nil {
		return nil, err
	}
	defer closeSources()

	stats, err := migrateLegacyBoltToTempDir(logsDB, stableDB, tempDir)
	if err != nil {
		return nil, err
	}
	if err := finalizeMigratedStore(tempDir, destDir); err != nil {
		return nil, err
	}
	return stats, nil
}

func prepareMigrationPaths(logsPath, stablePath, destDir string) (string, error) {
	if logsPath == "" {
		return "", errors.New("logs path is required")
	}
	if stablePath == "" {
		return "", errors.New("stable path is required")
	}
	if destDir == "" {
		return "", errors.New("destination dir is required")
	}

	destDir = filepath.Clean(destDir)

	if err := requireExistingFile(logsPath); err != nil {
		return "", err
	}
	if err := requireExistingFile(stablePath); err != nil {
		return "", err
	}
	if err := requireDestinationAbsent(destDir); err != nil {
		return "", err
	}

	tempDir := destDir + legacyMigrationSuffix
	if err := requireDestinationAbsent(tempDir); err != nil {
		return "", err
	}
	return tempDir, nil
}

func openLegacySourceDBs(logsPath, stablePath string) (logsDB *bbolt.DB, stableDB *bbolt.DB, closeFn func(), err error) {
	logsDB, err = openLegacyBoltReadOnly(logsPath)
	if err != nil {
		return nil, nil, nil, err
	}

	stableDB, err = openLegacyBoltReadOnly(stablePath)
	if err != nil {
		_ = logsDB.Close()
		return nil, nil, nil, err
	}

	closeFn = func() {
		_ = stableDB.Close()
		_ = logsDB.Close()
	}
	return logsDB, stableDB, closeFn, nil
}

func migrateLegacyBoltToTempDir(logsDB, stableDB *bbolt.DB, tempDir string) (*MigrationStats, error) {
	store, err := NewPebbleStore(tempDir)
	if err != nil {
		return nil, err
	}

	cleanupTemp := func() {
		_ = store.Close()
		_ = os.RemoveAll(tempDir)
	}

	stats, err := migrateLegacyBoltData(logsDB, stableDB, store)
	if err != nil {
		cleanupTemp()
		return nil, err
	}
	if err := store.Close(); err != nil {
		_ = os.RemoveAll(tempDir)
		return nil, err
	}
	return stats, nil
}

func finalizeMigratedStore(tempDir, destDir string) error {
	if err := os.MkdirAll(filepath.Dir(destDir), pebbleDirPerm); err != nil {
		_ = os.RemoveAll(tempDir)
		return errors.WithStack(err)
	}
	if err := os.Rename(tempDir, destDir); err != nil {
		_ = os.RemoveAll(tempDir)
		return errors.WithStack(err)
	}
	return nil
}

func migrateLegacyBoltData(logsDB, stableDB *bbolt.DB, dest *PebbleStore) (*MigrationStats, error) {
	stats := &MigrationStats{}

	if err := copyLegacyStable(stableDB, dest, stats); err != nil {
		return nil, err
	}
	if err := copyLegacyLogs(logsDB, dest, stats); err != nil {
		return nil, err
	}

	return stats, nil
}

func copyLegacyStable(stableDB *bbolt.DB, dest *PebbleStore, stats *MigrationStats) error {
	return errors.WithStack(stableDB.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(legacyStableBucket))
		if bucket == nil {
			return errors.Newf("legacy stable bucket %q not found", legacyStableBucket)
		}
		return bucket.ForEach(func(k, v []byte) error {
			if err := dest.Set(k, append([]byte(nil), v...)); err != nil {
				return err
			}
			stats.StableKeys++
			return nil
		})
	}))
}

func copyLegacyLogs(logsDB *bbolt.DB, dest *PebbleStore, stats *MigrationStats) error {
	batch := make([]*raft.Log, 0, legacyBatchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := dest.StoreLogs(batch); err != nil {
			return err
		}
		stats.Logs += uint64(len(batch))
		batch = batch[:0]
		return nil
	}

	err := logsDB.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(legacyLogsBucket))
		if bucket == nil {
			return errors.Newf("legacy logs bucket %q not found", legacyLogsBucket)
		}
		return bucket.ForEach(func(_, v []byte) error {
			var entry raft.Log
			if err := decodeLegacyLog(v, &entry); err != nil {
				return err
			}
			batch = append(batch, &entry)
			if len(batch) < legacyBatchSize {
				return nil
			}
			return flush()
		})
	})
	if err != nil {
		return errors.WithStack(err)
	}

	return flush()
}

func openLegacyBoltReadOnly(path string) (*bbolt.DB, error) {
	db, err := bbolt.Open(path, legacyBoltFileMode, &bbolt.Options{ReadOnly: true})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return db, nil
}

func requireExistingFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		return errors.WithStack(err)
	}
	if info.IsDir() {
		return errors.WithStack(errors.Newf("%s is a directory, expected file", path))
	}
	return nil
}

func requireDestinationAbsent(path string) error {
	if _, err := os.Stat(path); err == nil {
		return errors.WithStack(errors.Newf("destination already exists: %s", path))
	} else if !os.IsNotExist(err) {
		return errors.WithStack(err)
	}
	return nil
}

func decodeLegacyLog(payload []byte, out *raft.Log) error {
	handle := codec.MsgpackHandle{}
	decoder := codec.NewDecoder(bytes.NewReader(payload), &handle)
	return errors.WithStack(decoder.Decode(out))
}
