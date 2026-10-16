package adapter

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
)

// ErrAdminS3Stopping is returned by the admin object operations
// (AdminPutObject, AdminGetObject, AdminDeleteObject) once Stop has begun:
// the blob peer cluster and the cleanup gate are about to close, so the
// operation is refused rather than started against them. The admin handler
// maps it to the not-leader response so the client retries against another
// node.
var ErrAdminS3Stopping = errors.New("s3 admin: server is stopping")

// enterAdminBlobUse registers an admin object operation that may replicate
// or fetch chunk blobs through the blob peer cluster, or schedule a
// background cleanup. Stop waits, bounded, for every registered operation
// before it drains the cleanups and closes the cluster: the admin
// listener's object bridge (and the gRPC admin forwarder) call into this
// server outside its HTTP listener, so draining that listener does not
// cover them. Each successful call must be paired with exitAdminBlobUse.
func (s *S3Server) enterAdminBlobUse() error {
	if s == nil || s.adminBlobUsers.enter() {
		return nil
	}
	return ErrAdminS3Stopping
}

func (s *S3Server) exitAdminBlobUse() {
	if s != nil {
		s.adminBlobUsers.exit()
	}
}

// startCleanup runs one background cleanup (stale manifest blobs, upload
// parts, part blobs or upload data) on its own goroutine, under the
// cleanup semaphore and with s3ManifestCleanupTimeout. The cleanup is
// counted so Stop can join it before the blob cluster and the stores close,
// and its context derives from the server's cleanup context so Stop can
// cancel it once the bound has passed. A cleanup is skipped when the
// semaphore is saturated, as before, and once Stop has begun.
func (s *S3Server) startCleanup(run func(ctx context.Context)) {
	select {
	case s.cleanupSem <- struct{}{}:
	default:
		// Semaphore saturated; skip to avoid unbounded goroutine accumulation.
		return
	}
	if !s.cleanupWorkers.enter() {
		<-s.cleanupSem
		slog.Warn("s3 server: background cleanup skipped, the server is stopping")
		return
	}
	parent := s.cleanupCtx
	if parent == nil {
		parent = context.Background()
	}
	go func() {
		defer func() { <-s.cleanupSem }()
		defer s.cleanupWorkers.exit()
		ctx, cancel := context.WithTimeout(parent, s3ManifestCleanupTimeout)
		defer cancel()
		run(ctx)
	}()
}

// s3CleanupJoinTimeout bounds how long Stop waits, after cancelling the
// background cleanups that outlived the shutdown bound, for them to unwind
// out of their dispatch or scan. A variable so tests can shorten it.
var s3CleanupJoinTimeout = 2 * time.Second

// drainCleanups waits up to timeout for the background cleanups still
// running, then cancels the context of any that remain and waits, under
// s3CleanupJoinTimeout, for them to return, so none is still dispatching
// or scanning when the blob cluster and the stores close. New cleanups are
// refused from the moment the wait begins.
func (s *S3Server) drainCleanups(timeout time.Duration) {
	finished := s.cleanupWorkers.drain(timeout)
	if s.cancelCleanups != nil {
		s.cancelCleanups()
	}
	if finished {
		return
	}
	slog.Warn("s3 server: background cleanups still running after the shutdown bound, cancelling them",
		slog.Int64("in_flight", s.cleanupWorkers.remaining()),
		slog.Duration("timeout", timeout),
	)
	if !s.cleanupWorkers.join(s3CleanupJoinTimeout) {
		slog.Warn("s3 server: cancelled background cleanups did not return, closing the blob cluster",
			slog.Int64("in_flight", s.cleanupWorkers.remaining()),
			slog.Duration("timeout", s3CleanupJoinTimeout),
		)
	}
}
