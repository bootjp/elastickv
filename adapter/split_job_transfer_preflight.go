package adapter

import (
	"bytes"
	"context"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const splitTransferPreflightVersion = uint32(1)

// Each tick validates one bounded export page without importing it. Its cursor
// is independent of copy progress, so a legacy copy cursor is never proof.
func (s *DistributionServer) preflightSplitJobTransfer(ctx context.Context, job distribution.SplitJob) (bool, error) {
	_, route, err := s.splitJobSourceRoute(ctx, job)
	if err != nil {
		return false, err
	}
	brackets, err := distribution.PlanExportBrackets(job.SplitKey, route.End)
	if err != nil {
		return false, errors.WithStack(err)
	}
	source, err := s.splitJobSourceClient(ctx, job, route.GroupID)
	if err != nil {
		return false, err
	}
	witness, err := s.ensureSplitJobTransferPhase(ctx, job, source, nil)
	if err != nil {
		return false, err
	}
	if job.TransferPreflightVersion == 0 {
		return false, s.initializeSplitTransferPreflight(ctx, job, source, brackets, witness)
	}
	if err := validateSplitTransferPreflight(job, brackets); err != nil {
		return false, err
	}
	if job.TransferPreflightDone {
		return true, nil
	}
	for index, progress := range job.TransferPreflightProgress {
		if !progress.Done {
			return false, s.preflightSplitTransferPage(ctx, job, source, route.End, brackets[index], index, witness)
		}
	}
	next := distribution.CloneSplitJob(job)
	next.TransferPreflightDone = true
	return false, s.persistSplitTransferPreflight(ctx, job, next, witness)
}

func (s *DistributionServer) initializeSplitTransferPreflight(ctx context.Context, job distribution.SplitJob, source SplitMigrationClient, brackets []distribution.MigrationBracket, witness *splitTransferAdmissionWitness) error {
	issued, err := issueSplitMigrationTimestamp(ctx, source)
	if err != nil {
		return err
	}
	if issued.GetTimestamp() == 0 || issued.GetTimestamp() <= issued.GetLastCommitTs() {
		return errors.New("invalid migration preflight timestamp")
	}
	next := distribution.CloneSplitJob(job)
	next.TransferPreflightVersion = splitTransferPreflightVersion
	next.TransferPreflightTS = issued.GetTimestamp()
	next.TransferPreflightProgress = distribution.SplitJobBracketProgressForPlan(brackets, distribution.SplitJobExportPhaseBackfill)
	next.TransferPreflightDone = false
	return s.persistSplitTransferPreflight(ctx, job, next, witness)
}

func validateSplitTransferPreflight(job distribution.SplitJob, brackets []distribution.MigrationBracket) error {
	if job.TransferPreflightVersion != splitTransferPreflightVersion || job.TransferPreflightTS == 0 || len(job.TransferPreflightProgress) != len(brackets) {
		return errors.New("migration preflight lacks a complete versioned plan")
	}
	for index, progress := range job.TransferPreflightProgress {
		bracket := brackets[index]
		if progress.BracketID != bracket.BracketID || progress.Family != bracket.Family ||
			progress.ExportPhase != distribution.SplitJobExportPhaseBackfill || (job.TransferPreflightDone && !progress.Done) {
			return errors.New("migration preflight plan or completion witness does not match")
		}
	}
	return nil
}

func (s *DistributionServer) preflightSplitTransferPage(ctx context.Context, job distribution.SplitJob, source SplitMigrationClient, routeEnd []byte, bracket distribution.MigrationBracket, index int, witness *splitTransferAdmissionWitness) error {
	progress := job.TransferPreflightProgress[index]
	attemptCtx, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	stream, err := source.ExportRangeVersions(attemptCtx, &pb.ExportRangeVersionsRequest{
		RangeStart: bracket.Start, RangeEnd: bracket.End, MaxCommitTs: job.TransferPreflightTS,
		MinCommitTs: 0, Cursor: progress.Cursor, ChunkBytes: defaultSplitMigrationChunkBytes,
		RouteStart: job.SplitKey, RouteEnd: routeEnd, MaxScannedBytes: defaultSplitMigrationMaxScannedBytes,
		KeyFamily: bracket.Family, ExcludeKnownInternal: bracket.ExcludeKnownInternal, ExcludePrefixes: bracket.ExcludePrefixes,
	})
	if err != nil {
		return errors.WithStack(migrationSourceExportError(err))
	}
	resp, err := stream.Recv()
	if err != nil {
		return errors.WithStack(migrationSourceExportError(err))
	}
	if !resp.GetDone() && (len(resp.GetNextCursor()) == 0 || bytes.Equal(progress.Cursor, resp.GetNextCursor())) {
		return errors.New("migration preflight cursor did not progress")
	}
	if err := checkMigrationExportPageSize(resp); err != nil {
		// A local validation error is not an explicit failure from the source.
		return err
	}
	next := distribution.CloneSplitJob(job)
	next.TransferPreflightProgress[index].Cursor = distribution.CloneBytes(resp.GetNextCursor())
	next.TransferPreflightProgress[index].Done = resp.GetDone()
	if resp.GetDone() {
		// Completed families need only their completion witness, not a retained
		// full-key cursor for every family in the catalog job record.
		next.TransferPreflightProgress[index].Cursor = nil
	}
	next.TransferPreflightProgress[index].AcceptedRows += uint64(len(resp.GetVersions()))
	return s.persistSplitTransferPreflight(ctx, job, next, witness)
}

func (s *DistributionServer) persistSplitTransferPreflight(ctx context.Context, expected, next distribution.SplitJob, witness *splitTransferAdmissionWitness) error {
	if err := s.revalidateSplitTransferAdmission(ctx, witness); err != nil {
		return err
	}
	return s.updateSplitJobViaCoordinator(ctx, expected.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if !distribution.SplitJobsEquivalent(current, expected) {
			return current, nil
		}
		next.UpdatedAtMs = time.Now().UnixMilli()
		return next, nil
	})
}

func (s *DistributionServer) abandonOversizedSplitTransfer(ctx context.Context, job distribution.SplitJob, cause error) error {
	if !errors.Is(cause, errMigrationOversizedSourceVersion) {
		return cause
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if !splitTransferCanAutoAbandon(current.Phase) || current.Phase != job.Phase || current.SnapshotTS != job.SnapshotTS || current.FenceTS != job.FenceTS {
			return current, nil
		}
		next, err := distribution.BeginSplitJobAbandon(current, time.Now().UnixMilli())
		if err != nil {
			return current, errors.WithStack(err)
		}
		next.LastError = errors.Wrapf(cause, "migration transfer failed before cutover in phase %d", current.Phase).Error()
		if current.LastError != "" {
			const maxPreviousDiagnosticBytes = 512
			previous := current.LastError
			if len(previous) > maxPreviousDiagnosticBytes {
				previous = previous[:maxPreviousDiagnosticBytes]
			}
			next.LastError += "; previous: " + previous
		}
		return next, nil
	})
}

func splitTransferCanAutoAbandon(phase distribution.SplitJobPhase) bool {
	return phase == distribution.SplitJobPhasePlanned || phase == distribution.SplitJobPhaseBackfill ||
		phase == distribution.SplitJobPhaseFence || phase == distribution.SplitJobPhaseDeltaCopy
}
