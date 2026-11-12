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

// snapshotRetentionFlags configures snapshot retention (GC) on top of
// the offload destination.
type snapshotRetentionFlags struct {
	enabled                 bool
	claimsComplete          bool
	interval                time.Duration
	timeout                 time.Duration
	minGenerations          int
	maxAge                  time.Duration
	payloadGrace            time.Duration
	minMarkAge              time.Duration
	allowVersionedLifecycle bool
}

func (f *snapshotRetentionFlags) register(fs *flag.FlagSet) {
	fs.BoolVar(&f.enabled, "snapshotOffloadGCEnabled", false, "enable snapshot retention on this worker after completing the publisher claims rollout")
	fs.BoolVar(&f.claimsComplete, "snapshotOffloadGCClaimsRolloutComplete", false, "operator assertion: every publisher sharing the destination/prefix uses claims and all legacy publications have drained")
	fs.DurationVar(&f.interval, "snapshotOffloadGCInterval", time.Hour, "interval between serial snapshot retention passes")
	fs.DurationVar(&f.timeout, "snapshotOffloadGCTimeout", defaultSnapshotRetentionTimeout, "deadline for one snapshot retention pass")
	fs.IntVar(&f.minGenerations, "snapshotOffloadGCMinGenerations", snapshotoffload.DefaultMinGenerations, "minimum snapshot generations retained per Raft group")
	fs.DurationVar(&f.maxAge, "snapshotOffloadGCMaxAge", snapshotoffload.DefaultMaxAge, "age window retained in addition to minimum generations")
	fs.DurationVar(&f.payloadGrace, "snapshotOffloadGCPayloadGrace", snapshotoffload.DefaultPayloadGrace, "minimum age of an unreferenced snapshot payload before marking")
	fs.DurationVar(&f.minMarkAge, "snapshotOffloadGCMinMarkAge", snapshotoffload.DefaultMinMarkAge, "minimum delay between marking and reclaiming a snapshot payload")
	fs.BoolVar(&f.allowVersionedLifecycle, "snapshotOffloadAllowVersionedWithLifecycle", false, "allow retention in a versioned S3 bucket after confirming noncurrent-version expiration")
}

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

func buildSnapshotOffloadRetention(ctx context.Context, offload snapshotOffloadFlags, store snapshotOffloadStore, logger *slog.Logger) (*snapshotOffloadRetention, error) {
	f := offload.retention
	if !f.enabled {
		return nil, nil
	}
	if err := validateSnapshotRetentionConfig(f); err != nil {
		return nil, err
	}
	confirmed := f.claimsComplete
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
		Store: store, Prefix: strings.TrimSpace(offload.prefix),
		Policy: snapshotoffload.RetentionPolicy{
			MinGenerations: f.minGenerations, MaxAge: f.maxAge,
			PayloadGrace: f.payloadGrace, MinMarkAge: f.minMarkAge,
		},
		VerifyPublishersUseClaims: verify, Logger: logger,
	})
	if err != nil {
		return nil, errors.Wrap(err, "snapshot retention: create GC")
	}
	bounded, cancel := context.WithTimeout(ctx, f.timeout)
	defer cancel()
	if err := store.ValidateRetention(bounded); err != nil {
		return nil, errors.Wrap(err, "snapshot retention: validate bucket policy")
	}
	return &snapshotOffloadRetention{gc: gc, interval: f.interval, timeout: f.timeout, logger: logger}, nil
}

func validateSnapshotRetentionConfig(f snapshotRetentionFlags) error {
	if f.interval <= 0 || f.timeout <= 0 ||
		f.minGenerations < 1 || f.maxAge <= 0 ||
		f.payloadGrace <= 0 || f.minMarkAge <= 0 {
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
