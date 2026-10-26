package filesystem

import (
	"context"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

// OperationJournalStats is a snapshot of retained strict-mode work across
// every mount session. Pending and retained bytes must drain before rollback.
type OperationJournalStats struct {
	Sessions       uint64
	ClosedSessions uint64
	Pending        uint64
	RetainedCount  uint64
	RetainedBytes  uint64
	MaxBytes       uint64
}

func (s *Service) ListOperationJournalStats(ctx context.Context) (OperationJournalStats, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return OperationJournalStats{}, errors.Wrap(err, "snapshot operation journal usage")
	}
	var stats OperationJournalStats
	err = s.scanVisiblePrefix(ctx, fskeys.OperationSessionAllPrefix(), ts, func(pair *store.KVPair) error {
		session, decodeErr := decodeOperationSession(pair.Value)
		if decodeErr != nil {
			return decodeErr
		}
		stats.Sessions++
		if session.Closed {
			stats.ClosedSessions++
		}
		stats.Pending += uint64(session.Pending)
		stats.RetainedCount += uint64(session.RetainedCount)
		stats.RetainedBytes += session.RetainedBytes
		stats.MaxBytes += session.MaxBytes
		return nil
	})
	return stats, errors.Wrap(err, "scan operation journal usage")
}
