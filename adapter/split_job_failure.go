package adapter

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxSplitJobPhaseFailures = 5

type splitJobAttemptKey struct {
	jobID uint64
	phase distribution.SplitJobPhase
}

type splitJobFailureState struct {
	phase distribution.SplitJobPhase
	count int
}

func (s *DistributionServer) runSplitJobAttempt(ctx context.Context, job distribution.SplitJob) error {
	err := s.runSplitJobPhase(ctx, job)
	if errors.Is(err, errMigrationOversizedSourceVersion) && splitTransferCanAutoAbandon(job.Phase) {
		return errors.WithStack(errors.CombineErrors(err, s.abandonOversizedSplitTransfer(ctx, job, err)))
	}
	key := splitJobAttemptKey{jobID: job.JobID, phase: job.Phase}
	if splitJobPhaseErrorRetryable(err) {
		s.resetSplitJobFailures(key)
		return err
	}
	if !s.splitJobFailureLimitReached(key) {
		return err
	}
	persistErr := s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		// A concurrent runner may have made progress or changed phase. Do not
		// terminalize it based on a stale failed attempt.
		if !distribution.SplitJobsEquivalent(current, job) {
			return current, nil
		}
		return distribution.FailSplitJobState(current, err.Error(), time.Now().UnixMilli())
	})
	if persistErr == nil {
		s.resetSplitJobFailures(key)
	}
	return errors.WithStack(errors.CombineErrors(err, persistErr))
}

func splitJobPhaseErrorRetryable(err error) bool {
	if err == nil || splitJobRunnerContextDone(err) || splitJobCoordinatorLeadershipError(err) || errors.Is(err, errSplitMigrationVoterBarrierIncomplete) || errors.Is(err, store.ErrWriteConflict) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Canceled, codes.Aborted, codes.ResourceExhausted:
		return true
	case codes.OK, codes.Unknown, codes.InvalidArgument, codes.NotFound,
		codes.AlreadyExists, codes.PermissionDenied, codes.FailedPrecondition,
		codes.OutOfRange, codes.Unimplemented, codes.Internal, codes.DataLoss,
		codes.Unauthenticated:
		return false
	}
	return false
}

func (s *DistributionServer) resetSplitJobFailures(key splitJobAttemptKey) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.splitJobFailures, key.jobID)
}

func (s *DistributionServer) splitJobFailureLimitReached(key splitJobAttemptKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.splitJobFailures == nil {
		s.splitJobFailures = make(map[uint64]splitJobFailureState)
	}
	state := s.splitJobFailures[key.jobID]
	if state.phase != key.phase {
		state = splitJobFailureState{phase: key.phase}
	}
	if state.count < maxSplitJobPhaseFailures {
		state.count++
	}
	s.splitJobFailures[key.jobID] = state
	return state.count >= maxSplitJobPhaseFailures
}
