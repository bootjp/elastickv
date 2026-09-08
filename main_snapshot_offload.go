package main

import (
	"context"
	"flag"
	"log/slog"
	"strings"

	"github.com/bootjp/elastickv/internal/raftengine"
	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// Physical snapshot object offload (design doc §4 / §7). Opt-in: the
// whole subsystem stays dormant unless --snapshotOffloadBucket (S3) or
// --snapshotOffloadLocalDir (filesystem) is set.
//
// The design requires that only a group's current leader publishes, so
// each group contributes both a cheap pre-check and a pre-commit
// leadership re-verification; the scheduler bounds the latter itself.
var (
	snapshotOffloadBucket = flag.String("snapshotOffloadBucket", "",
		"S3 bucket for physical snapshot offload; empty to disable")
	snapshotOffloadLocalDir = flag.String("snapshotOffloadLocalDir", "",
		"filesystem root for physical snapshot offload; an alternative to --snapshotOffloadBucket, mainly for testing")
	snapshotOffloadPrefix = flag.String("snapshotOffloadPrefix", "",
		"key prefix below which snapshot artifacts are written")
	snapshotOffloadRegion = flag.String("snapshotOffloadRegion", "",
		"AWS region for the snapshot offload bucket")
	snapshotOffloadEndpoint = flag.String("snapshotOffloadEndpoint", "",
		"custom S3 endpoint for snapshot offload; empty uses the AWS default")
	snapshotOffloadProfile = flag.String("snapshotOffloadProfile", "",
		"shared-credentials profile for snapshot offload")
	snapshotOffloadForcePathStyle = flag.Bool("snapshotOffloadForcePathStyle", false,
		"use path-style addressing for the snapshot offload endpoint")
	snapshotOffloadSSE = flag.String("snapshotOffloadServerSideEncryption", "",
		"server-side encryption mode for snapshot objects (AES256 or aws:kms)")
	snapshotOffloadSSEKMSKeyID = flag.String("snapshotOffloadSSEKMSKeyId", "",
		"KMS key ARN when --snapshotOffloadServerSideEncryption is aws:kms")
	snapshotOffloadInterval = flag.Duration("snapshotOffloadInterval", snapshotoffload.DefaultSchedulerInterval,
		"how often to scan local groups for a publishable snapshot")
	snapshotOffloadJitter = flag.Duration("snapshotOffloadJitter", 0,
		"random spread applied to the offload schedule; zero uses a quarter of the interval")
	snapshotOffloadConcurrency = flag.Int("snapshotOffloadConcurrency", snapshotoffload.DefaultSchedulerConcurrency,
		"maximum concurrent snapshot uploads for this process")
	snapshotOffloadSpoolDir = flag.String("snapshotOffloadSpoolDir", "",
		"directory for snapshot spool files; empty uses the data dir's filesystem")
	snapshotOffloadSourceCluster = flag.String("snapshotOffloadSourceCluster", "",
		"source cluster identity recorded in every manifest; required when offload is enabled")
)

// snapshotOffloadEnabled reports whether the operator configured a
// destination. Checked before any other offload flag is validated so a
// node that never opts in cannot fail startup on offload config.
func snapshotOffloadEnabled() bool {
	return strings.TrimSpace(*snapshotOffloadBucket) != "" ||
		strings.TrimSpace(*snapshotOffloadLocalDir) != ""
}

// buildSnapshotOffloadStore constructs the configured object store.
//
// Bucket and local dir are mutually exclusive: accepting both would
// leave which destination actually receives the artifacts ambiguous,
// and a backup written to the wrong place is discovered only when a
// restore is attempted.
func buildSnapshotOffloadStore(ctx context.Context) (snapshotoffload.ObjectStore, error) {
	bucket := strings.TrimSpace(*snapshotOffloadBucket)
	localDir := strings.TrimSpace(*snapshotOffloadLocalDir)
	if bucket != "" && localDir != "" {
		return nil, errors.Wrap(snapshotoffload.ErrInvalidOptions,
			"--snapshotOffloadBucket and --snapshotOffloadLocalDir are mutually exclusive")
	}
	if localDir != "" {
		store, err := snapshotoffload.NewLocalStore(localDir)
		if err != nil {
			return nil, errors.Wrap(err, "snapshot offload: local store")
		}
		return store, nil
	}
	store, err := snapshotoffload.NewS3Store(ctx, snapshotoffload.S3StoreConfig{
		Bucket:               bucket,
		Region:               strings.TrimSpace(*snapshotOffloadRegion),
		Endpoint:             strings.TrimSpace(*snapshotOffloadEndpoint),
		Profile:              strings.TrimSpace(*snapshotOffloadProfile),
		ForcePathStyle:       *snapshotOffloadForcePathStyle,
		ServerSideEncryption: strings.TrimSpace(*snapshotOffloadSSE),
		SSEKMSKeyID:          strings.TrimSpace(*snapshotOffloadSSEKMSKeyID),
	})
	if err != nil {
		return nil, errors.Wrap(err, "snapshot offload: s3 store")
	}
	return store, nil
}

// snapshotOffloadGroups builds one OffloadGroup per local Raft group.
//
// Both leadership callbacks read the engine through snapshotEngine():
// the scheduler outlives startup and races Close(), so a direct field
// read would be a data race. A runtime whose engine has been cleared
// reports "not leader", which fails closed.
func snapshotOffloadGroups(
	runtimes []*raftGroupRuntime, raftDir, raftID string, multi bool,
) []snapshotoffload.OffloadGroup {
	groups := make([]snapshotoffload.OffloadGroup, 0, len(runtimes))
	for _, rt := range runtimes {
		if rt == nil {
			continue
		}
		groups = append(groups, snapshotoffload.OffloadGroup{
			GroupID:      rt.spec.id,
			DataDir:      groupDataDir(raftDir, raftID, rt.spec.id, multi),
			IsLeader:     snapshotOffloadIsLeader(rt),
			VerifyLeader: snapshotOffloadVerifyLeader(rt),
		})
	}
	return groups
}

func snapshotOffloadIsLeader(rt *raftGroupRuntime) func() bool {
	return func() bool {
		engine := rt.snapshotEngine()
		return engine != nil && engine.State() == raftengine.StateLeader
	}
}

// snapshotOffloadVerifyLeader is the §4 pre-commit re-verification: a
// multi-gigabyte spool takes long enough to lose an election, so
// leadership must hold at the instant the manifest commits, not merely
// when the snapshot was opened.
func snapshotOffloadVerifyLeader(rt *raftGroupRuntime) func(context.Context) error {
	return func(ctx context.Context) error {
		engine := rt.snapshotEngine()
		if engine == nil {
			return errors.Wrap(snapshotoffload.ErrInvalidOptions,
				"snapshot offload: raft engine closed")
		}
		verifier, ok := engine.(interface {
			VerifyLeader(context.Context) error
		})
		if !ok {
			return errors.Wrap(snapshotoffload.ErrInvalidOptions,
				"snapshot offload: raft engine cannot verify leadership")
		}
		return errors.Wrap(verifier.VerifyLeader(ctx), "snapshot offload: verify leadership")
	}
}

// startSnapshotOffload wires and starts the scheduler when offload is
// configured. It returns an error rather than logging and continuing:
// an operator who configured a backup destination and got no backups
// is worse off than one whose node refused to start.
func startSnapshotOffload(
	ctx context.Context,
	eg *errgroup.Group,
	runtimes []*raftGroupRuntime,
	raftDir, raftID string,
	multi bool,
	observer snapshotoffload.SchedulerObserver,
	logger *slog.Logger,
) error {
	if !snapshotOffloadEnabled() {
		return nil
	}
	store, err := buildSnapshotOffloadStore(ctx)
	if err != nil {
		return err
	}

	opts := []snapshotoffload.SchedulerOption{
		snapshotoffload.WithSchedulerInterval(*snapshotOffloadInterval),
		snapshotoffload.WithSchedulerConcurrency(*snapshotOffloadConcurrency),
		snapshotoffload.WithSchedulerObserver(observer),
		snapshotoffload.WithSchedulerLogger(logger),
	}
	if *snapshotOffloadJitter > 0 {
		opts = append(opts, snapshotoffload.WithSchedulerJitter(*snapshotOffloadJitter))
	}
	if dir := strings.TrimSpace(*snapshotOffloadSpoolDir); dir != "" {
		opts = append(opts, snapshotoffload.WithSchedulerSpoolDir(dir))
	}

	scheduler, err := snapshotoffload.NewScheduler(
		store,
		snapshotOffloadGroups(runtimes, raftDir, raftID, multi),
		strings.TrimSpace(*snapshotOffloadPrefix),
		strings.TrimSpace(*snapshotOffloadSourceCluster),
		buildVersion(),
		opts...,
	)
	if err != nil {
		return errors.Wrap(err, "snapshot offload: scheduler")
	}

	logger.Info("snapshot offload enabled",
		slog.Int("groups", len(runtimes)),
		slog.Duration("interval", *snapshotOffloadInterval),
		slog.Int("concurrency", *snapshotOffloadConcurrency))

	eg.Go(func() error {
		// Run returns only on context cancellation; a failing group is
		// retried on the next tick rather than tearing the process
		// down, because an object-store outage must not stop serving.
		if err := scheduler.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return errors.Wrap(err, "snapshot offload scheduler")
		}
		return nil
	})
	return nil
}
