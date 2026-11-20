package adapter

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/bootjp/elastickv/distribution"
)

const splitPreflightContinuationDelay = 100 * time.Millisecond

// A short wake is only a scheduling hint. Every attempt still enters through
// the ordinary leader, capability and ownership checks. Do not accelerate a
// completed preflight into its fence, or turn stalled work into a busy loop.
func (s *DistributionServer) splitJobPreflightContinuation(ctx context.Context, previous distribution.SplitJob) (bool, error) {
	if !splitPreflightIncomplete(previous) {
		return false, nil
	}
	current, found, err := s.catalog.SplitJob(ctx, previous.JobID)
	if err != nil {
		return false, splitJobCatalogStatusError(err)
	}
	if !found || current.Phase != previous.Phase || !splitPreflightIncomplete(current) {
		return false, nil
	}
	return preflightCheckpointChanged(previous, current), nil
}

func preflightCheckpointChanged(previous, current distribution.SplitJob) bool {
	if current.TransferPreflightVersion != splitTransferPreflightVersion || current.TransferPreflightTS == 0 {
		return false
	}
	if previous.TransferPreflightVersion == 0 {
		return true
	}
	if previous.TransferPreflightVersion != current.TransferPreflightVersion || previous.TransferPreflightTS != current.TransferPreflightTS {
		return false
	}
	return !slices.EqualFunc(previous.TransferPreflightProgress, current.TransferPreflightProgress, splitPreflightProgressEqual)
}

func splitPreflightIncomplete(job distribution.SplitJob) bool {
	return !job.TransferPreflightDone && (job.Phase == distribution.SplitJobPhaseFence || job.Phase == distribution.SplitJobPhaseDeltaCopy)
}

func splitPreflightProgressEqual(a, b distribution.SplitJobBracketProgress) bool {
	return a.BracketID == b.BracketID && a.Family == b.Family && a.ExportPhase == b.ExportPhase && a.Done == b.Done && a.AcceptedRows == b.AcceptedRows && a.ScannedBytes == b.ScannedBytes && a.LastAckedBatchSeq == b.LastAckedBatchSeq && bytes.Equal(a.Cursor, b.Cursor)
}

func waitSplitJobRunner(ctx context.Context, tick <-chan time.Time, continueSoon bool) bool {
	var wake <-chan time.Time
	if continueSoon {
		timer := time.NewTimer(splitPreflightContinuationDelay)
		defer timer.Stop()
		wake = timer.C
	}
	select {
	case <-ctx.Done():
		return false
	case <-tick:
		return true
	case <-wake:
		return true
	}
}
