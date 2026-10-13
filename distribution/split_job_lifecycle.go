package distribution

import (
	"bytes"
	"context"

	"github.com/cockroachdb/errors"
)

var (
	ErrCatalogSplitJobNotFound      = errors.New("catalog split job is not found")
	ErrCatalogSplitJobCannotRetry   = errors.New("catalog split job cannot be retried")
	ErrCatalogSplitJobCannotAbandon = errors.New("catalog split job cannot be abandoned")
)

// RetrySplitJobState returns the durable state transition for RetrySplitJob.
// The retry target must come from the recorded RetryPhase witness; callers must
// not infer a phase from cursors, timestamps, or diagnostics after restart.
func RetrySplitJobState(job SplitJob, nowMs int64) (SplitJob, error) {
	out := CloneSplitJob(job)
	if out.Phase != SplitJobPhaseFailed || !splitJobRetryPhaseAllowed(out.RetryPhase) {
		return SplitJob{}, errors.WithStack(ErrCatalogSplitJobCannotRetry)
	}
	out.Phase = out.RetryPhase
	out.RetryPhase = SplitJobPhaseNone
	out.AbandonFromPhase = SplitJobPhaseNone
	out.LastError = ""
	out.UpdatedAtMs = nowMs
	return out, nil
}

// BeginSplitJobAbandon records the durable ABANDONING witness before any
// pre-CUTOVER cleanup side effect is removed.
func BeginSplitJobAbandon(job SplitJob, nowMs int64) (SplitJob, error) {
	out := CloneSplitJob(job)
	from, ok := splitJobAbandonFromPhase(out)
	if !ok {
		return SplitJob{}, errors.WithStack(ErrCatalogSplitJobCannotAbandon)
	}
	if out.Phase == SplitJobPhaseAbandoning {
		return out, nil
	}
	out.Phase = SplitJobPhaseAbandoning
	out.RetryPhase = SplitJobPhaseNone
	out.AbandonFromPhase = from
	out.UpdatedAtMs = nowMs
	return out, nil
}

// RetrySplitJob CASes a FAILED live job back to its recorded retry phase.
func (s *CatalogStore) RetrySplitJob(ctx context.Context, jobID uint64, nowMs int64) (SplitJob, error) {
	expected, _, err := s.LiveSplitJobForUpdate(ctx, jobID)
	if err != nil {
		return SplitJob{}, err
	}
	next, err := RetrySplitJobState(expected, nowMs)
	if err != nil {
		return SplitJob{}, err
	}
	return next, s.SaveSplitJob(ctx, expected, next)
}

// BeginSplitJobAbandon CASes a live pre-CUTOVER job into ABANDONING.
func (s *CatalogStore) BeginSplitJobAbandon(ctx context.Context, jobID uint64, nowMs int64) (SplitJob, error) {
	expected, _, err := s.LiveSplitJobForUpdate(ctx, jobID)
	if err != nil {
		return SplitJob{}, err
	}
	next, err := BeginSplitJobAbandon(expected, nowMs)
	if err != nil {
		return SplitJob{}, err
	}
	if SplitJobsEquivalent(expected, next) {
		return next, nil
	}
	return next, s.SaveSplitJob(ctx, expected, next)
}

// LiveSplitJobForUpdate reads the latest live split job and the snapshot
// timestamp used for the read. RPC callers use readTS as a CAS StartTS when
// proposing the update through Raft.
func (s *CatalogStore) LiveSplitJobForUpdate(ctx context.Context, jobID uint64) (SplitJob, uint64, error) {
	if err := ensureCatalogStore(s); err != nil {
		return SplitJob{}, 0, err
	}
	if jobID == 0 {
		return SplitJob{}, 0, errors.WithStack(ErrCatalogSplitJobIDRequired)
	}
	ctx = contextOrBackground(ctx)
	readTS := s.store.LastCommitTS()
	job, found, err := s.liveSplitJobAt(ctx, jobID, readTS)
	if err != nil {
		return SplitJob{}, 0, err
	}
	if !found {
		return SplitJob{}, 0, errors.WithStack(ErrCatalogSplitJobNotFound)
	}
	return job, readTS, nil
}

func splitJobRetryPhaseAllowed(phase SplitJobPhase) bool {
	switch phase {
	case SplitJobPhaseBackfill, SplitJobPhaseFence, SplitJobPhaseDeltaCopy, SplitJobPhaseCutover, SplitJobPhaseCleanup:
		return true
	case SplitJobPhaseNone, SplitJobPhasePlanned, SplitJobPhaseDone, SplitJobPhaseFailed, SplitJobPhaseAbandoning, SplitJobPhaseAbandoned:
		return false
	}
	return false
}

func splitJobAbandonFromPhase(job SplitJob) (SplitJobPhase, bool) {
	switch job.Phase {
	case SplitJobPhasePlanned, SplitJobPhaseBackfill, SplitJobPhaseFence, SplitJobPhaseDeltaCopy:
		return job.Phase, true
	case SplitJobPhaseFailed:
		if splitJobAbandonRetryPhaseAllowed(job.RetryPhase) {
			return job.RetryPhase, true
		}
	case SplitJobPhaseAbandoning:
		if splitJobAbandonRetryPhaseAllowed(job.AbandonFromPhase) {
			return job.AbandonFromPhase, true
		}
	case SplitJobPhaseNone, SplitJobPhaseCutover, SplitJobPhaseCleanup, SplitJobPhaseDone, SplitJobPhaseAbandoned:
	}
	return SplitJobPhaseNone, false
}

func splitJobAbandonRetryPhaseAllowed(phase SplitJobPhase) bool {
	switch phase {
	case SplitJobPhasePlanned, SplitJobPhaseBackfill, SplitJobPhaseFence, SplitJobPhaseDeltaCopy:
		return true
	case SplitJobPhaseNone, SplitJobPhaseCutover, SplitJobPhaseCleanup, SplitJobPhaseDone, SplitJobPhaseFailed, SplitJobPhaseAbandoning, SplitJobPhaseAbandoned:
		return false
	}
	return false
}

// SplitJobsEquivalent reports whether two split jobs encode to the same
// durable catalog value.
func SplitJobsEquivalent(left, right SplitJob) bool {
	leftRaw, leftErr := EncodeSplitJob(left)
	rightRaw, rightErr := EncodeSplitJob(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	return bytes.Equal(leftRaw, rightRaw)
}
