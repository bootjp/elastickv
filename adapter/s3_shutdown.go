package adapter

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
)

// ErrAdminS3Stopping is returned by the admin object operations that use
// the blob peer cluster (AdminPutObject, AdminGetObject) once Stop has
// begun: the cluster is about to close, so the operation is refused rather
// than started against it. The admin handler maps it to the not-leader
// response so the client retries against another node.
var ErrAdminS3Stopping = errors.New("s3 admin: server is stopping")

// enterAdminBlobUse registers an admin object operation that may replicate
// or fetch chunk blobs through the blob peer cluster. Stop waits, bounded,
// for every registered operation before it closes the cluster: the admin
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

// drainCleanups waits up to timeout for the background cleanups still
// running, then cancels the context of any that remain so they stop
// dispatching against stores that are about to close. New cleanups are
// refused from the moment the wait begins.
func (s *S3Server) drainCleanups(timeout time.Duration) {
	if !s.cleanupWorkers.drain(timeout) {
		slog.Warn("s3 server: background cleanups still running after the shutdown bound, cancelling them",
			slog.Int64("in_flight", s.cleanupWorkers.remaining()),
			slog.Duration("timeout", timeout),
		)
	}
	if s.cancelCleanups != nil {
		s.cancelCleanups()
	}
}
