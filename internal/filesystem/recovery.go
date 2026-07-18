package filesystem

import (
	"bytes"
	"context"
	"crypto/rand"
	"time"

	"github.com/bootjp/elastickv/internal/fskeys"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	defaultIntentRecoveryLimit = 256
	intentCleanupTimeout       = 5 * time.Second
)

type RecoveryStats struct {
	MoveJobsResumed uint64
	MoveJobsCleared uint64
	IntentsCleared  uint64
}

func (s *Service) createNodeWithIntent(
	ctx context.Context,
	parent uint64,
	name []byte,
	typ FileType,
	opts CreateOptions,
) (result CreateResult, err error) {
	intent, err := s.prepareNamespaceIntent(ctx, IntentKindCreate, parent, name)
	if err != nil {
		return CreateResult{}, err
	}
	intentKey := fskeys.IntentKey(intent.ID)
	defer func() {
		if err != nil {
			s.clearIntentBestEffort(ctx, intentKey)
		}
	}()
	return s.createNode(ctx, parent, name, typ, opts, intentKey)
}

func (s *Service) unlinkWithIntent(ctx context.Context, parent uint64, name []byte, directory bool) (err error) {
	intent, err := s.prepareNamespaceIntent(ctx, IntentKindDelete, parent, name)
	if err != nil {
		return err
	}
	intentKey := fskeys.IntentKey(intent.ID)
	defer func() {
		if err != nil {
			s.clearIntentBestEffort(ctx, intentKey)
		}
	}()
	return s.unlink(ctx, parent, name, directory, intentKey)
}

func (s *Service) prepareNamespaceIntent(
	ctx context.Context,
	kind IntentKind,
	parent uint64,
	name []byte,
) (IntentState, error) {
	id, err := randomIntentID()
	if err != nil {
		return IntentState{}, err
	}
	now := s.now().UnixNano()
	intent := IntentState{
		ID:        append(append([]byte(nil), []byte(kind)...), id...),
		Kind:      kind,
		Phase:     "prepared",
		Parent:    parent,
		Name:      append([]byte(nil), name...),
		CreatedAt: now,
		UpdatedAt: now,
	}
	ts, err := s.readTS(ctx)
	if err != nil {
		return IntentState{}, err
	}
	key := fskeys.IntentKey(intent.ID)
	elem, err := putElem(key, intent)
	if err != nil {
		return IntentState{}, err
	}
	if err := s.dispatchTxn(ctx, ts, []*kv.Elem[kv.OP]{elem}, [][]byte{key}); err != nil {
		return IntentState{}, err
	}
	return intent, nil
}

// RecoverIntents resumes durable move jobs and aborts prepared namespace
// intents. Create/delete publish their intent before the atomic namespace txn
// and delete it inside that txn, so a surviving marker proves the namespace
// mutation did not commit and is safe to clear.
func (s *Service) RecoverIntents(ctx context.Context, limit int) (RecoveryStats, error) {
	if limit <= 0 {
		limit = defaultIntentRecoveryLimit
	}
	var stats RecoveryStats
	moveStats, err := s.recoverMoveJobs(ctx, limit)
	stats.MoveJobsResumed += moveStats.MoveJobsResumed
	stats.MoveJobsCleared += moveStats.MoveJobsCleared
	if err != nil {
		return stats, err
	}
	intentStats, err := s.recoverIntentRecords(ctx, limit)
	stats.MoveJobsResumed += intentStats.MoveJobsResumed
	stats.MoveJobsCleared += intentStats.MoveJobsCleared
	stats.IntentsCleared += intentStats.IntentsCleared
	return stats, err
}

func (s *Service) recoverMoveJobs(ctx context.Context, limit int) (RecoveryStats, error) {
	jobs, err := s.scanRecoveryPrefix(ctx, fskeys.MoveJobAllPrefix(), limit)
	if err != nil {
		return RecoveryStats{}, err
	}
	var stats RecoveryStats
	for _, pair := range jobs {
		job, decodeErr := decodeJSON[MoveJob](pair.Value)
		if decodeErr != nil {
			return stats, decodeErr
		}
		if job.Phase != MovePhaseCompleted {
			if _, resumeErr := s.ResumeMoveFile(ctx, job.ID); resumeErr != nil {
				return stats, resumeErr
			}
			stats.MoveJobsResumed++
		}
		if clearErr := s.clearMoveJob(ctx, job.ID); clearErr != nil {
			return stats, clearErr
		}
		stats.MoveJobsCleared++
	}
	return stats, nil
}

func (s *Service) recoverIntentRecords(ctx context.Context, limit int) (RecoveryStats, error) {
	var stats RecoveryStats
	intents, err := s.scanRecoveryPrefix(ctx, fskeys.IntentAllPrefix(), limit)
	if err != nil {
		return stats, err
	}
	for _, pair := range intents {
		intent, decodeErr := decodeJSON[IntentState](pair.Value)
		if decodeErr != nil {
			return stats, decodeErr
		}
		resumed, jobCleared, resumeErr := s.recoverMoveIntent(ctx, intent)
		if resumeErr != nil {
			return stats, resumeErr
		}
		if jobCleared {
			stats.MoveJobsCleared++
		}
		if resumed {
			stats.MoveJobsResumed++
			continue
		}
		if err := s.clearIntent(ctx, pair.Key); err != nil {
			return stats, err
		}
		stats.IntentsCleared++
	}
	return stats, nil
}

func (s *Service) recoverMoveIntent(ctx context.Context, intent IntentState) (bool, bool, error) {
	if intent.Kind != IntentKindMove {
		return false, false, nil
	}
	job, err := s.moveJob(ctx, intent.JobID)
	if errors.Is(err, ErrMoveJobNotFound) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	resumed := job.Phase != MovePhaseCompleted
	if resumed {
		if _, err = s.ResumeMoveFile(ctx, job.ID); err != nil {
			return false, false, err
		}
	}
	if err = s.clearMoveJob(ctx, job.ID); err != nil {
		return resumed, false, err
	}
	return resumed, true, nil
}

func (s *Service) scanRecoveryPrefix(ctx context.Context, prefix []byte, limit int) ([]*store.KVPair, error) {
	ts, err := s.readTS(ctx)
	if err != nil {
		return nil, err
	}
	end := prefixEnd(prefix)
	cursor := prefix
	out := make([]*store.KVPair, 0)
	for {
		page, err := s.store.ScanAt(ctx, cursor, end, limit, ts)
		if err != nil {
			return nil, errors.Wrap(err, "filesystem scan recovery records")
		}
		for _, pair := range page {
			if pair != nil && bytes.HasPrefix(pair.Key, prefix) {
				out = append(out, pair)
				if len(out) == limit {
					return out, nil
				}
			}
		}
		if len(page) < limit {
			return out, nil
		}
		cursor = nextScanCursorForMove(page[len(page)-1].Key)
	}
}

func (s *Service) clearIntent(ctx context.Context, key []byte) error {
	return s.clearRecoveryRecord(ctx, key)
}

func (s *Service) clearMoveJob(ctx context.Context, jobID []byte) error {
	return s.clearRecoveryRecord(ctx, fskeys.MoveJobKey(jobID))
}

func (s *Service) clearRecoveryRecord(ctx context.Context, key []byte) error {
	ts, err := s.readTS(ctx)
	if err != nil {
		return err
	}
	return s.dispatchTxn(ctx, ts,
		[]*kv.Elem[kv.OP]{{Op: kv.Del, Key: append([]byte(nil), key...)}},
		[][]byte{append([]byte(nil), key...)},
	)
}

func (s *Service) clearIntentBestEffort(ctx context.Context, key []byte) {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), intentCleanupTimeout)
	defer cancel()
	_ = s.clearIntent(cleanupCtx, key)
}

func randomIntentID() ([]byte, error) {
	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, errors.Wrap(err, "filesystem allocate intent id")
	}
	return id[:], nil
}
