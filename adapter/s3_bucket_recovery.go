package adapter

import (
	"context"
	"log/slog"
	"time"

	"github.com/bootjp/elastickv/internal/s3keys"
	"github.com/cockroachdb/errors"
)

const s3BucketCleanupRecoveryInterval = 5 * time.Minute

func (s *S3Server) runBucketCleanupRecovery(ctx context.Context, generation uint64) {
	ticker := time.NewTicker(s3BucketCleanupRecoveryInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		if err := s.recoverPendingBucketDeletes(ctx); err != nil {
			s.setBlobGCReadyIfCurrent(ctx, generation, false)
			if ctx.Err() == nil {
				slog.WarnContext(ctx, "s3 bucket cleanup recovery failed", "err", err)
			}
		} else if ctx.Err() == nil {
			s.setBlobGCReadyIfCurrent(ctx, generation, true)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *S3Server) recoverPendingBucketDeletes(ctx context.Context) error {
	start := []byte(s3keys.BucketCleanupPrefix)
	end := prefixScanEnd(start)
	for {
		readTimestamp, err := s.beginTxnReadTimestamp(ctx, ^uint64(0), "s3 bucket cleanup recovery")
		if err != nil {
			return err
		}
		readPin := s.pinReadTS(readTimestamp.Timestamp())
		rows, err := s.store.ScanAt(ctx, start, end, s3MetaBatchOps, readTimestamp.Timestamp())
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
