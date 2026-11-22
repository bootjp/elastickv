package adapter

import (
	"context"
	"log/slog"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

const (
	s3BucketCleanupRecoveryInterval = 5 * time.Minute
	s3BucketRecoveryBackoffFactor   = 2
)

func (s *S3Server) runBucketCleanupRecovery(ctx context.Context, generation uint64) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	retry := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		err := s.recoverPendingBucketDeletes(ctx)
		if ctx.Err() != nil {
			return
		}
		s.setBlobGCReadyIfCurrent(ctx, generation, err == nil)
		if err != nil {
			slog.WarnContext(ctx, "s3 bucket cleanup recovery failed", "err", err)
		}
		// A cluster can open its stores before electing its first leader.
		// Retry the fenced scan without waiting a full maintenance period.
		if s.blobOffloadEnabled && errors.Is(err, kv.ErrTSOCommitFloorUnavailable) {
			timer.Reset(retry)
			retry = min(s3BucketRecoveryBackoffFactor*retry, s3BucketCleanupRecoveryInterval)
		} else {
			retry = time.Second
			timer.Reset(s3BucketCleanupRecoveryInterval)
		}
	}
}

func (s *S3Server) recoverPendingBucketDeletes(ctx context.Context) error {
	start := []byte(s3keys.BucketCleanupPrefix)
	end := prefixScanEnd(start)
	for {
		readTimestamp, err := s.beginTxnReadTimestamp(ctx, "s3 bucket cleanup recovery")
		if err != nil {
			return err
		}
		readPin := s.pinReadTS(readTimestamp.Timestamp())
		rows, err := requestScanAt(ctx, s.store, start, end, s3MetaBatchOps, readTimestamp.Timestamp())
		readPin.Release()
		if err != nil {
			return errors.WithStack(err)
		}
		if len(rows) == 0 {
			return nil
		}
		for _, row := range rows {
			bucket, generation, ok := s3keys.ParseBucketCleanupKey(row.Key)
			if !ok || len(row.Value) != 0 {
				return errors.New("s3: malformed bucket cleanup marker")
			}
			if err := s.runBucketDeleteSafetyNet(ctx, bucket, generation); err != nil {
				return errors.Wrapf(err, "s3: recover bucket cleanup %q generation %d", bucket, generation)
			}
		}
		start = nextScanCursor(rows[len(rows)-1].Key)
	}
}
