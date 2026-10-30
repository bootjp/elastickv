package etcd

import (
	"log/slog"
	"path/filepath"

	"github.com/cockroachdb/errors"
	etcdstorage "go.etcd.io/etcd/server/v3/storage"
	"go.etcd.io/raft/v3/raftpb"
)

// snapshotDiskPhase captures persistence dependencies without FSM or owner
// callbacks or MemoryStorage access. Callers retain snapshotMu and resource
// ownership across the durable-index bump and synchronous persistence.
type snapshotDiskPhase struct {
	persist     etcdstorage.Storage
	dataDir     string
	fsmSnapDir  string
	maxWALFiles int
}

func (d snapshotDiskPhase) persistCreated(snapshot raftpb.Snapshot) error {
	if err := d.save(&snapshot); err != nil {
		return err
	}
	if err := d.release(&snapshot); err != nil {
		return err
	}
	d.purge()
	return nil
}

func (d snapshotDiskPhase) save(snapshot *raftpb.Snapshot) error {
	return errors.WithStack(d.persist.SaveSnap(snapshot))
}

func (d snapshotDiskPhase) release(snapshot *raftpb.Snapshot) error {
	return errors.WithStack(d.persist.Release(snapshot))
}

// Cleanup remains best-effort and runs only after successful persistence.
func (d snapshotDiskPhase) purge() {
	snapDir := filepath.Join(d.dataDir, snapDirName)
	if purgeErr := purgeOldSnapshotFiles(snapDir, d.fsmSnapDir); purgeErr != nil {
		slog.Warn("failed to purge old snap files", "error", purgeErr)
	}
	walDir := filepath.Join(d.dataDir, walDirName)
	if purgeErr := purgeOldWALFiles(walDir, d.maxWALFiles); purgeErr != nil {
		slog.Warn("failed to purge old wal files", "error", purgeErr)
	}
}
