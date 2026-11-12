package server

import (
	"context"
	"flag"
	"math"
	"strings"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/fsmount"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// filesystemFlags configures the optional FUSE mount.
type filesystemFlags struct {
	mount                 string
	clientID              string
	strict                bool
	strictRecover         bool
	capacity              uint64
	maxFiles              uint64
	rootMode              uint
	rootUID               uint
	rootGID               uint
	placementScanInterval time.Duration
	leaseReapInterval     time.Duration
}

func (f *filesystemFlags) register(fs *flag.FlagSet) {
	fs.StringVar(&f.mount, "filesystemMount", "", "FUSE mount point for the Elastickv filesystem; empty to disable")
	fs.StringVar(&f.clientID, "filesystemClientID", "", "Stable open-handle lease client ID for the FUSE mount; empty uses raftId")
	fs.BoolVar(&f.strict, "filesystemStrict", false, "Use the operation journal for supported FUSE mutations")
	fs.BoolVar(&f.strictRecover, "filesystemStrictRecover", false, "Fence and recover a previous strict FUSE session for the same client ID; use only after the previous mount has stopped")
	fs.Uint64Var(&f.capacity, "filesystemCapacity", 0, "Filesystem byte capacity reported by statfs; zero reports unlimited")
	fs.Uint64Var(&f.maxFiles, "filesystemMaxFiles", 0, "Filesystem inode capacity reported by statfs; zero reports unlimited")
	fs.UintVar(&f.rootMode, "filesystemRootMode", defaultFilesystemRootMode, "Root directory permission mode for first initialization")
	fs.UintVar(&f.rootUID, "filesystemRootUID", 0, "Root directory owner UID for first initialization")
	fs.UintVar(&f.rootGID, "filesystemRootGID", 0, "Root directory owner GID for first initialization")
	fs.DurationVar(&f.placementScanInterval, "filesystemPlacementScanInterval", defaultFilesystemPlacementScanInterval, "Interval for filesystem placement and recovery-state metrics; non-positive disables periodic scans")
	fs.DurationVar(&f.leaseReapInterval, "filesystemLeaseReapInterval", defaultFilesystemLeaseReapInterval, "Interval for reclaiming expired filesystem open-handle leases; non-positive disables periodic reaping")
}

// filesystemStartConfig resolves the mount configuration; raftID is the
// fallback lease client ID.
func filesystemStartConfig(f filesystemFlags, raftID string) (fsmount.Config, error) {
	config := fsmount.Config{MountPoint: strings.TrimSpace(f.mount)}
	if config.MountPoint == "" {
		return config, nil
	}
	if f.rootMode > 0o7777 || f.rootUID > math.MaxUint32 || f.rootGID > math.MaxUint32 {
		return fsmount.Config{}, errors.New("filesystem root mode, uid, or gid is out of range")
	}
	config.ClientID = strings.TrimSpace(f.clientID)
	config.Strict = f.strict
	config.StrictRecover = f.strictRecover
	if config.StrictRecover && !config.Strict {
		return fsmount.Config{}, errors.New("filesystemStrictRecover requires filesystemStrict")
	}
	if config.ClientID == "" {
		config.ClientID = strings.TrimSpace(raftID)
	}
	if config.ClientID == "" {
		return fsmount.Config{}, errors.New("filesystem client ID is required")
	}
	config.RootMode = uint32(f.rootMode)
	config.RootUID = uint32(f.rootUID)
	config.RootGID = uint32(f.rootGID)
	return config, nil
}

func startFilesystemIfEnabled(
	ctx context.Context,
	eg *errgroup.Group,
	cleanup *internalutil.CleanupStack,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	observer monitoring.FileSystemObserver,
	f filesystemFlags,
	raftID string,
) error {
	config, err := filesystemStartConfig(f, raftID)
	if err != nil {
		return err
	}
	config.Capacity = f.capacity
	config.MaxFiles = f.maxFiles
	config.PlacementScanInterval = f.placementScanInterval
	config.LeaseReapInterval = f.leaseReapInterval
	return errors.WithStack(fsmount.Start(ctx, eg, cleanup, shardStore, coordinate, observer, config))
}

// startFilesystemAfterServing mounts the optional FUSE filesystem once the
// public services are up. On failure the public services are already serving
// on eg, so they are drained before the deferred CleanupStack closes the
// stores they use; a shutdown signal during the FUSE pre-mount work surfaces
// here as context.Canceled and must not skip that drain either.
func startFilesystemAfterServing(in serversInput) error {
	if err := startFilesystemIfEnabled(
		in.ctx,
		in.eg,
		in.cleanup,
		in.shardStore,
		in.coordinate,
		in.metricsRegistry.FileSystemObserver(),
		in.filesystem,
		in.raftID,
	); err != nil {
		return waitErrgroupAfterStartupFailure(in.cancel, in.eg, err)
	}
	return nil
}
