package main

import (
	"context"
	"math"
	"strings"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/fsmount"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

func filesystemStartConfigFromFlags() (fsmount.Config, error) {
	config := fsmount.Config{MountPoint: strings.TrimSpace(*filesystemMount)}
	if config.MountPoint == "" {
		return config, nil
	}
	if *filesystemRootMode > 0o7777 || *filesystemRootUID > math.MaxUint32 || *filesystemRootGID > math.MaxUint32 {
		return fsmount.Config{}, errors.New("filesystem root mode, uid, or gid is out of range")
	}
	config.ClientID = strings.TrimSpace(*filesystemClientID)
	config.Strict = *filesystemStrict
	config.StrictRecover = *filesystemStrictRecover
	if config.StrictRecover && !config.Strict {
		return fsmount.Config{}, errors.New("filesystemStrictRecover requires filesystemStrict")
	}
	if config.ClientID == "" {
		config.ClientID = strings.TrimSpace(*raftId)
	}
	if config.ClientID == "" {
		return fsmount.Config{}, errors.New("filesystem client ID is required")
	}
	config.RootMode = uint32(*filesystemRootMode)
	config.RootUID = uint32(*filesystemRootUID)
	config.RootGID = uint32(*filesystemRootGID)
	return config, nil
}

func startFilesystemIfEnabled(
	ctx context.Context,
	eg *errgroup.Group,
	cleanup *internalutil.CleanupStack,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	observer monitoring.FileSystemObserver,
) error {
	config, err := filesystemStartConfigFromFlags()
	if err != nil {
		return err
	}
	config.Capacity = *filesystemCapacity
	config.MaxFiles = *filesystemMaxFiles
	config.PlacementScanInterval = *filesystemPlacementScanInterval
	config.LeaseReapInterval = *filesystemLeaseReapInterval
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
	); err != nil {
		return waitErrgroupAfterStartupFailure(in.cancel, in.eg, err)
	}
	return nil
}
