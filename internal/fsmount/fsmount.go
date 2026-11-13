// Package fsmount mounts the Elastickv filesystem service over FUSE for
// the server binary and runs its background collectors and lease reaper.
// It owns the mount lifecycle: startup retries while Raft elects a
// leader, strict-mode journal recovery, and a bounded unmount (with lazy
// detach) on shutdown.
package fsmount

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	internalutil "github.com/bootjp/elastickv/internal"
	"github.com/bootjp/elastickv/internal/filesystem"
	"github.com/bootjp/elastickv/internal/filesystem/fuseadapter"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
	"github.com/cockroachdb/errors"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sync/errgroup"
)

// Startup waits up to startupRetryBudget for Raft leadership and its HLC
// lease, retrying every startupRetryInterval.
const (
	startupRetryBudget   = 90 * time.Second
	startupRetryInterval = 250 * time.Millisecond
)

// Config is the resolved mount configuration. An empty MountPoint
// disables the filesystem.
type Config struct {
	MountPoint    string
	ClientID      string
	Strict        bool
	StrictRecover bool
	RootMode      uint32
	RootUID       uint32
	RootGID       uint32
	// Capacity and MaxFiles are the byte and inode capacity reported by
	// statfs; zero leaves the service default.
	Capacity uint64
	MaxFiles uint64
	// PlacementScanInterval paces the placement and journal collectors;
	// LeaseReapInterval paces the open-handle lease reaper.
	PlacementScanInterval time.Duration
	LeaseReapInterval     time.Duration
	// MountOptions overrides go-fuse's mount options. Production leaves it
	// nil (fusermount3 path); tests set DirectMount where no helper exists.
	MountOptions *fuse.MountOptions
}

// Start mounts the filesystem described by config and registers its
// lifecycle on eg and cleanup. It is a no-op when config.MountPoint is
// empty.
func Start(
	ctx context.Context,
	eg *errgroup.Group,
	cleanup *internalutil.CleanupStack,
	shardStore *kv.ShardStore,
	coordinate kv.Coordinator,
	observer monitoring.FileSystemObserver,
	config Config,
) error {
	if config.MountPoint == "" {
		return nil
	}
	if eg == nil || cleanup == nil {
		return errors.New("filesystem lifecycle is required")
	}
	service, err := filesystem.NewService(
		shardStore,
		coordinate,
		filesystem.WithCapacity(config.Capacity),
		filesystem.WithMaxFiles(config.MaxFiles),
		filesystem.WithOperationalObserver(observer),
		filesystem.WithReadObserver(coordinate),
	)
	if err != nil {
		return errors.Wrap(err, "create filesystem service")
	}
	startupCtx, cancel := context.WithTimeout(ctx, startupRetryBudget)
	defer cancel()
	if err := retryFilesystemStartup(startupCtx, startupRetryInterval, func(ctx context.Context) error {
		return service.InitializeRoot(ctx, config.RootMode, config.RootUID, config.RootGID)
	}); err != nil {
		return errors.Wrap(err, "initialize filesystem root")
	}
	server, serveDone, err := MountService(ctx, service, config)
	if err != nil {
		return err
	}
	InstallLifecycle(ctx, eg, cleanup, server, serveDone, config.MountPoint)
	startFilesystemPlacementCollector(ctx, eg, service, config.PlacementScanInterval)
	if config.Strict {
		startFilesystemJournalCollector(ctx, eg, service, observer, config.PlacementScanInterval)
	}
	startFilesystemLeaseReaper(ctx, eg, service, config.LeaseReapInterval)
	slog.Info("filesystem FUSE mounted", "mount_point", config.MountPoint, "client_id", config.ClientID)
	return nil
}

// retryFilesystemStartup waits for Raft leadership and its HLC lease during
// cluster election. Other failures remain terminal, and the caller supplies
// the startup deadline.
func retryFilesystemStartup(ctx context.Context, interval time.Duration, attempt func(context.Context) error) error {
	for {
		err := attempt(ctx)
		if err == nil || (!errors.Is(err, kv.ErrLeaderNotFound) && !errors.Is(err, kv.ErrCeilingExpired)) {
			return err
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return errors.Join(err, ctx.Err())
		}
	}
}

// MountService mounts service at the configured mount point. ctx
// bounds only the pre-mount work (stale-mount detach, intent recovery) so a
// shutdown signal during startup is not held up by it; the mount itself is
// ended by the lifecycle installed afterwards.
func MountService(
	ctx context.Context,
	service *filesystem.Service,
	config Config,
) (*fuseadapter.Server, <-chan struct{}, error) {
	adapterOptions, err := strictFilesystemOptions(ctx, service, config)
	if err != nil {
		return nil, nil, err
	}
	frontend := fuseadapter.New(service, []byte(config.ClientID), adapterOptions...)
	server, err := fuseadapter.MountContext(ctx, config.MountPoint, frontend, config.MountOptions)
	if err != nil {
		frontend.Close()
		return nil, nil, errors.Wrap(err, "mount filesystem frontend")
	}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		<-serveDone
		return nil, nil, errors.Wrap(err, "wait for filesystem FUSE mount")
	}
	return server, serveDone, nil
}

func strictFilesystemOptions(ctx context.Context, service *filesystem.Service, config Config) ([]fuseadapter.Option, error) {
	if !config.Strict {
		return nil, nil
	}
	journal, err := filesystem.NewOperationJournal(service)
	if err != nil {
		return nil, errors.Wrap(err, "create strict filesystem journal")
	}
	filesystemID, err := journal.EnsureFilesystem(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "identify strict filesystem")
	}
	if config.StrictRecover {
		recoveryCtx, cancel := context.WithTimeout(ctx, startupRetryBudget)
		defer cancel()
		if err := retryFilesystemStartup(recoveryCtx, startupRetryInterval, func(ctx context.Context) error {
			return journal.RecoverOwnerSession(ctx, filesystemID, config.ClientID)
		}); err != nil {
			return nil, errors.Wrap(err, "recover previous strict filesystem session")
		}
	}
	session, err := journal.RegisterSession(ctx, filesystemID, config.ClientID)
	if err != nil {
		return nil, errors.Wrap(err, "register strict filesystem session")
	}
	return []fuseadapter.Option{fuseadapter.WithStrictMode(session)}, nil
}

// MountServer is the part of *fuseadapter.Server the shutdown
// lifecycle drives; tests substitute a fake to exercise the failure paths.
type MountServer interface {
	Unmount() error
	UnmountContext(context.Context) error
	UnmountLazy(context.Context) error
}

// filesystemServeStopTimeout bounds how long a signal shutdown waits for the
// FUSE serve loop after the unmount. After a plain unmount failed (EBUSY: a
// client still holds files open) the mount is detached lazily and the loop
// only ends once those handles close, which may never happen; the shutdown
// proceeds after this wait so a SIGTERM cannot hang the process until
// SIGKILL. A variable so tests can shorten it.
var filesystemServeStopTimeout = 10 * time.Second

// OverrideServeStopTimeout replaces the serve-loop stop bound and returns
// a function restoring the previous value. It exists so kernel-mount tests
// in other packages can run the shutdown path quickly; it must not be
// called while a mount lifecycle is running.
func OverrideServeStopTimeout(d time.Duration) (restore func()) {
	old := filesystemServeStopTimeout
	filesystemServeStopTimeout = d
	return func() { filesystemServeStopTimeout = old }
}

// InstallLifecycle ties a mounted server to ctx: when ctx ends it unmounts
// (falling back to a lazy detach when the mount is busy), waits a bounded
// time for the serve loop, and reports a serve loop that stops on its own
// as an errgroup failure. cleanup runs a bounded unmount if the lifecycle
// never started one.
func InstallLifecycle(
	ctx context.Context,
	eg *errgroup.Group,
	cleanup *internalutil.CleanupStack,
	server MountServer,
	serveDone <-chan struct{},
	mountPoint string,
) {
	unmountCtx, cancelUnmount := context.WithCancel(context.Background())
	unmounter := newFilesystemUnmounter(func() error { return server.UnmountContext(unmountCtx) })
	unmounter.cancel = cancelUnmount
	cleanup.Add(func() {
		// Normally the lifecycle goroutine has already run the shutdown by
		// the time CleanupStack runs (eg.Wait precedes it); then there is
		// nothing left to do, and re-waiting on an unmount that is stuck in
		// a backend request would hang the WAL sync and store close that
		// follow. Only an unmount nobody started yet is run here, bounded.
		if unmounter.handled.Load() {
			return
		}
		select {
		case <-unmounter.begin():
			if err := unmounter.result(); err != nil {
				slog.Warn("filesystem FUSE unmount failed", "mount_point", mountPoint, "err", err)
			}
		case <-time.After(filesystemServeStopTimeout):
			cancelUnmount()
			slog.Error("filesystem FUSE unmount did not return in time during cleanup, continuing",
				"mount_point", mountPoint,
				"timeout", filesystemServeStopTimeout,
			)
		}
	})
	eg.Go(func() error {
		select {
		case <-ctx.Done():
			stopFilesystemServer(ctx, unmounter, server, serveDone, mountPoint)
			return nil
		case <-serveDone:
			select {
			case <-ctx.Done():
				// Shutdown already began; the serve loop exiting after the
				// unmount is the expected path, not a failure.
				return nil
			default:
				return errors.New("filesystem FUSE server stopped unexpectedly")
			}
		}
	})
}

// stopFilesystemServer unmounts during a shutdown within one bounded budget
// (filesystemServeStopTimeout). The plain unmount runs in its own goroutine
// because a plain unmount can block or fail with EBUSY while a client holds
// files open. Its helper is context-bound and must be killed and reaped
// before the lazy detach can start. The mount is then detached lazily, which
// also cancels the requests still in flight so a handler blocked in the
// backend returns before the stores close, and the wait for the serve loop
// is bounded, so the rest of the shutdown (WAL sync, store close) still runs
// and the process exits. Exiting closes the /dev/fuse descriptor, which
// makes the kernel drop whatever the lazy detach left behind.
func stopFilesystemServer(
	ctx context.Context,
	unmounter *filesystemUnmounter,
	server MountServer,
	serveDone <-chan struct{},
	mountPoint string,
) {
	defer unmounter.handled.Store(true)
	// ctx is already cancelled; the whole shutdown shares one budget.
	stopCtx, cancel := context.WithTimeout(context.Background(), filesystemServeStopTimeout)
	defer cancel()
	// The plain unmount may use most of the budget, but a slice is kept so
	// the lazy detach still runs inside it when the unmount hangs.
	unmountWait := time.NewTimer(filesystemServeStopTimeout - filesystemServeStopTimeout/filesystemLazyDetachShare)
	defer unmountWait.Stop()
	select {
	case <-unmounter.begin():
		if err := unmounter.result(); err != nil {
			slog.WarnContext(ctx, "filesystem FUSE unmount failed, detaching lazily", "mount_point", mountPoint, "err", err)
			lazyDetachFilesystem(ctx, stopCtx, server, mountPoint)
		}
	case <-unmountWait.C:
		slog.ErrorContext(ctx, "filesystem FUSE unmount did not return in time (a request may be stuck in the backend), detaching lazily and continuing shutdown",
			"mount_point", mountPoint,
			"timeout", filesystemServeStopTimeout,
		)
		// Fall through: the reserved remainder of the budget still waits for
		// the serve loop, so a request that finishes in time is not cut off
		// by the store close that follows.
		// Killing a context-aware unmount must also reap its helper before
		// another path-based unmount can target a replacement filesystem.
		if !unmounter.cancelAndJoin(stopCtx) {
			slog.ErrorContext(ctx, "filesystem unmount helper did not exit; refusing concurrent lazy detach", "mount_point", mountPoint)
			return
		}
		lazyDetachFilesystem(ctx, stopCtx, server, mountPoint)
	}
	select {
	case <-serveDone:
	case <-stopCtx.Done():
		slog.ErrorContext(ctx, "filesystem FUSE serve loop did not stop in time, continuing shutdown",
			"mount_point", mountPoint,
			"timeout", filesystemServeStopTimeout,
		)
	}
}

// filesystemUnmounter runs the context-bound unmount at most once, in the background,
// and lets any number of waiters observe its completion without a later
// caller ever blocking on an earlier call that is stuck (go-fuse's Unmount
// waits for an event loop that may be held by a hung backend request;
// sync.OnceValue would make the second caller wait for the first).
type filesystemUnmounter struct {
	run     func() error
	cancel  context.CancelFunc
	started atomic.Bool
	done    chan struct{}
	err     error
	// handled is set once the lifecycle's shutdown has run to completion
	// (unmounted, or gave up after its budget), so the CleanupStack entry
	// knows there is nothing left to wait for.
	handled atomic.Bool
}

func (u *filesystemUnmounter) cancelAndJoin(ctx context.Context) bool {
	if u.cancel == nil {
		return true
	}
	u.cancel()
	select {
	case <-u.done:
		return true
	case <-ctx.Done():
		return false
	}
}

func newFilesystemUnmounter(run func() error) *filesystemUnmounter {
	return &filesystemUnmounter{run: run, done: make(chan struct{})}
}

// begin starts the unmount the first time it is called and returns a channel
// that is closed when it finishes; later calls return the same channel.
func (u *filesystemUnmounter) begin() <-chan struct{} {
	if u.started.CompareAndSwap(false, true) {
		go func() {
			defer close(u.done)
			u.err = u.run()
		}()
	}
	return u.done
}

// result is the unmount's error; valid once the channel from begin is closed.
func (u *filesystemUnmounter) result() error { return u.err }

// filesystemLazyDetachShare is the fraction (1/n) of filesystemServeStopTimeout
// held back from the plain-unmount wait so the lazy detach fallback still
// runs inside the one advertised budget when the unmount hangs.
const filesystemLazyDetachShare = 5

// lazyDetachFilesystem runs the lazy detach fallback within the shutdown's
// remaining budget (stopCtx); logCtx only correlates the log line.
func lazyDetachFilesystem(logCtx, stopCtx context.Context, server MountServer, mountPoint string) {
	if err := server.UnmountLazy(stopCtx); err != nil {
		slog.WarnContext(logCtx, "filesystem FUSE lazy detach failed", "mount_point", mountPoint, "err", err)
	}
}

func startFilesystemPlacementCollector(
	ctx context.Context,
	eg *errgroup.Group,
	service *filesystem.Service,
	interval time.Duration,
) {
	if eg == nil || service == nil || interval <= 0 {
		return
	}
	eg.Go(func() error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if _, err := service.ListFilePlacementStats(ctx); err != nil && ctx.Err() == nil {
				slog.WarnContext(ctx, "filesystem placement scan failed", "err", err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})
}

func startFilesystemJournalCollector(
	ctx context.Context,
	eg *errgroup.Group,
	service *filesystem.Service,
	observer monitoring.FileSystemObserver,
	interval time.Duration,
) {
	if eg == nil || service == nil || observer == nil || interval <= 0 {
		return
	}
	eg.Go(func() error {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			stats, err := service.ListOperationJournalStats(ctx)
			if err != nil {
				if ctx.Err() == nil {
					slog.WarnContext(ctx, "filesystem journal usage scan failed", "err", err)
				}
			} else {
				observer.ObserveOperationJournalStats(stats)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})
}

type filesystemLeaseReaper interface {
	ReapExpiredOpenHandleLeases(context.Context, int) (filesystem.LeaseReapStats, error)
}

func startFilesystemLeaseReaper(
	ctx context.Context,
	eg *errgroup.Group,
	reaper filesystemLeaseReaper,
	interval time.Duration,
) {
	if eg == nil || reaper == nil || interval <= 0 {
		return
	}
	eg.Go(func() error {
		return runFilesystemLeaseReaper(ctx, reaper, interval)
	})
}

func runFilesystemLeaseReaper(
	ctx context.Context,
	reaper filesystemLeaseReaper,
	interval time.Duration,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		stats, err := reaper.ReapExpiredOpenHandleLeases(ctx, 0)
		if err != nil && ctx.Err() == nil {
			slog.WarnContext(ctx, "filesystem lease reaper failed", "err", err)
		}
		if stats.ExpiredRefs > 0 || stats.OrphanedInodesGCed > 0 {
			slog.InfoContext(ctx, "filesystem lease reaper reclaimed state",
				"expired_refs", stats.ExpiredRefs,
				"orphaned_inodes_gced", stats.OrphanedInodesGCed,
			)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
