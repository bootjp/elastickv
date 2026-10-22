package adapter

import (
	"context"
)

// An admin mutation claims its cleanup slot before committing, so a timed
// out admin drain cannot close cleanup admission underneath that mutation.
type s3CleanupReservation struct {
	s           *S3Server
	reservation *backgroundReservation
}

func (s *S3Server) reserveAdminCleanup() (*s3CleanupReservation, error) {
	reservation, ok := s.reservedCleanups.reserve()
	if !ok {
		return nil, ErrAdminS3Stopping
	}
	return &s3CleanupReservation{s: s, reservation: reservation}, nil
}

func (r *s3CleanupReservation) release() {
	r.reservation.release()
}

func (r *s3CleanupReservation) start(run func(context.Context)) {
	r.reservation.start(s3ManifestCleanupTimeout, func(ctx context.Context) {
		// Unlike opportunistic cleanups this slot predates shutdown and is
		// required by an admitted commit. Wait boundedly for the semaphore.
		select {
		case r.s.cleanupSem <- struct{}{}:
			defer func() { <-r.s.cleanupSem }()
		case <-ctx.Done():
			return
		}
		run(ctx)
	})
}
