package main

import (
	"context"
	"flag"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/bootjp/elastickv/internal/snapshotoffload"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

const defaultSnapshotRetentionTimeout = 10 * time.Minute

var (
	snapshotOffloadGCEnabled               = flag.Bool("snapshotOffloadGCEnabled", false, "enable snapshot retention on this worker after completing the publisher claims rollout")
	snapshotOffloadGCClaimsComplete        = flag.Bool("snapshotOffloadGCClaimsRolloutComplete", false, "operator assertion: every publisher sharing the destination/prefix uses claims and all legacy publications have drained")
	snapshotOffloadGCInterval              = flag.Duration("snapshotOffloadGCInterval", time.Hour, "interval between serial snapshot retention passes")
	snapshotOffloadGCTimeout               = flag.Duration("snapshotOffloadGCTimeout", defaultSnapshotRetentionTimeout, "deadline for one snapshot retention pass")
	snapshotOffloadGCMinGenerations        = flag.Int("snapshotOffloadGCMinGenerations", snapshotoffload.DefaultMinGenerations, "minimum snapshot generations retained per Raft group")
	snapshotOffloadGCMaxAge                = flag.Duration("snapshotOffloadGCMaxAge", snapshotoffload.DefaultMaxAge, "age window retained in addition to minimum generations")
	snapshotOffloadGCPayloadGrace          = flag.Duration("snapshotOffloadGCPayloadGrace", snapshotoffload.DefaultPayloadGrace, "minimum age of an unreferenced snapshot payload before marking")
	snapshotOffloadGCMinMarkAge            = flag.Duration("snapshotOffloadGCMinMarkAge", snapshotoffload.DefaultMinMarkAge, "minimum delay between marking and reclaiming a snapshot payload")
	snapshotOffloadAllowVersionedLifecycle = flag.Bool("snapshotOffloadAllowVersionedWithLifecycle", false, "allow retention in a versioned S3 bucket after confirming noncurrent-version expiration")
)

type snapshotOffloadStore interface {
	snapshotoffload.PublishStore
	snapshotoffload.RetentionStore
}

type snapshotOffloadRetention struct {
	gc       *snapshotoffload.GC
	interval time.Duration
	timeout  time.Duration
	logger   *slog.Logger
}

func buildSnapshotOffloadRetention(ctx context.Context, store snapshotOffloadStore, logger *slog.Logger) (*snapshotOffloadRetention, error) {
	if !*snapshotOffloadGCEnabled {
		return nil, nil
	}
	if err := validateSnapshotRetentionConfig(); err != nil {
		return nil, err
	}
	confirmed := *snapshotOffloadGCClaimsComplete
	verify := func(ctx context.Context) error {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		if !confirmed {
			return errors.Wrap(snapshotoffload.ErrInvalidOptions, "snapshot retention requires --snapshotOffloadGCClaimsRolloutComplete after all legacy publishers are stopped and drained")
		}
		return nil
	}
	if err := verify(ctx); err != nil {
		return nil, err
	}
	gc, err := snapshotoffload.NewGC(snapshotoffload.GCOptions{
		Store: store, Prefix: strings.TrimSpace(*snapshotOffloadPrefix),
		Policy: snapshotoffload.RetentionPolicy{
			MinGenerations: *snapshotOffloadGCMinGenerations, MaxAge: *snapshotOffloadGCMaxAge,
			PayloadGrace: *snapshotOffloadGCPayloadGrace, MinMarkAge: *snapshotOffloadGCMinMarkAge,
		},
		VerifyPublishersUseClaims: verify, Logger: logger,
	})
	if err != nil {
		return nil, errors.Wrap(err, "snapshot retention: create GC")
	}
	bounded, cancel := context.WithTimeout(ctx, *snapshotOffloadGCTimeout)
	defer cancel()
	if err := store.ValidateRetention(bounded); err != nil {
		return nil, errors.Wrap(err, "snapshot retention: validate bucket policy")
	}
	return &snapshotOffloadRetention{gc: gc, interval: *snapshotOffloadGCInterval, timeout: *snapshotOffloadGCTimeout, logger: logger}, nil
}

func validateSnapshotRetentionConfig() error {
	if *snapshotOffloadGCInterval <= 0 || *snapshotOffloadGCTimeout <= 0 ||
		*snapshotOffloadGCMinGenerations < 1 || *snapshotOffloadGCMaxAge <= 0 ||
		*snapshotOffloadGCPayloadGrace <= 0 || *snapshotOffloadGCMinMarkAge <= 0 {
		return errors.Wrap(snapshotoffload.ErrInvalidOptions, "snapshot retention intervals, deadlines and policy must be positive")
	}
	return nil
}

func (r *snapshotOffloadRetention) run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		bounded, cancel := context.WithTimeout(ctx, r.timeout)
		result, err := r.gc.RunOnce(bounded)
		cancel()
		if ctx.Err() != nil {
			return errors.WithStack(ctx.Err())
		}
		if err != nil {
			r.logger.WarnContext(ctx, "snapshot retention pass failed", "err", err)
		} else {
			r.logger.InfoContext(ctx, "snapshot retention pass completed",
				"manifests_deleted", len(result.ManifestsDeleted), "payloads_deleted", len(result.PayloadsDeleted),
				"payloads_awaiting_sweep", result.PayloadsAwaitingSweep, "skip_reason", result.SkipReason)
		}
		timer := time.NewTimer(r.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

func closeSnapshotOffloadStore(store snapshotOffloadStore) error {
	if closer, ok := store.(io.Closer); ok {
		return errors.Wrap(closer.Close(), "close snapshot offload store")
	}
	return nil
}

func runSnapshotOffloadWorkers(ctx context.Context, store snapshotOffloadStore, scheduler *snapshotoffload.Scheduler, retention *snapshotOffloadRetention) (retErr error) {
	defer func() { retErr = errors.CombineErrors(retErr, closeSnapshotOffloadStore(store)) }()
	workers, childCtx := errgroup.WithContext(ctx)
	workers.Go(func() error { return scheduler.Run(childCtx) })
	if retention != nil {
		workers.Go(func() error { return retention.run(childCtx) })
	}
	err := workers.Wait()
	if ctx.Err() != nil {
		return nil
	}
	return errors.Wrap(err, "snapshot offload workers")
}
