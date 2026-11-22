package adapter

import (
	"bytes"
	"context"
	"slices"
	"time"

	"github.com/bootjp/elastickv/distribution"
	pb "github.com/bootjp/elastickv/proto"
	"github.com/cockroachdb/errors"
)

const (
	splitTransferPreflightVersion = uint32(1)
	preflightPagesPerTick         = 8
	preflightPageBytes            = 512 << 10
	preflightBytesPerTick         = 4 * defaultSplitMigrationChunkBytes
	preflightTimePerTick          = 200 * time.Millisecond
)

// A phase caller can check the witness or take one step without entering the
// full runner. Only an already durable completion witness authorizes a fence.
func (s *DistributionServer) preflightSplitJobTransfer(ctx context.Context, job distribution.SplitJob) (bool, error) {
	return s.preflightSplitTransferPages(ctx, job, 1)
}

func (s *DistributionServer) preflightSplitTransferPages(ctx context.Context, job distribution.SplitJob, maxPages int, admitted ...bool) (bool, error) {
	deadline := time.Now().Add(preflightTimePerTick)
	snapshot, route, err := s.splitJobSourceRoute(ctx, job)
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
	witness, err := s.ensureSplitJobTransferPhase(ctx, job, source, nil, splitTransferAdmissionOptions{readExistingControl: true})
	if err != nil {
		return false, err
	}
	pass := splitPreflightPass{source: source, route: route, brackets: brackets, witness: witness, catalogVersion: snapshot.Version, namespaceAdmission: len(admitted) > 0 && admitted[0]}
	if job.TransferPreflightVersion == 0 {
		return false, s.initializeSplitTransferPreflight(ctx, job, pass)
	}
	if err := validateSplitTransferPreflight(job, brackets); err != nil {
		return false, err
	}
	if job.TransferPreflightDone {
		return true, nil
	}
	return false, s.runSplitPreflightPages(ctx, job, pass, deadline, maxPages)
}

type splitPreflightPass struct {
	source             SplitMigrationClient
	route              distribution.RouteDescriptor
	brackets           []distribution.MigrationBracket
	witness            *splitTransferAdmissionWitness
	catalogVersion     uint64
	namespaceAdmission bool
}

func (s *DistributionServer) runSplitPreflightPages(ctx context.Context, job distribution.SplitJob, pass splitPreflightPass, deadline time.Time, maxPages int) error {
	bytesLeft := preflightBytesPerTick
	next := distribution.CloneSplitJob(job)
	for page := 1; ; page++ {
		size, err := pass.advance(ctx, &next, bytesLeft)
		if err != nil {
			return bindPreflightExportFailure(err, job, pass)
		}
		bytesLeft -= size
		if next.TransferPreflightDone || page >= maxPages || bytesLeft <= 0 || !time.Now().Before(deadline) {
			break
		}
		// These pages are read-only. Validate against the original durable
		// checkpoint until the whole bounded batch can be committed atomically.
		ready, err := s.canContinueSplitPreflight(ctx, job, pass)
		if err != nil || !ready {
			return err
		}
		if !time.Now().Before(deadline) {
			break
		}
	}
	return s.persistSplitTransferPreflight(ctx, job, &next, pass)
}

// Re-read the exact checkpoint we wrote. Another runner or administrator can
// change the job between two pages even when its phase is unchanged.
func (s *DistributionServer) canContinueSplitPreflight(ctx context.Context, expected distribution.SplitJob, pass splitPreflightPass) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, errors.WithStack(err)
	}
	leader, err := s.verifySplitJobRunnerLeader(ctx)
	if err != nil || !leader {
		return false, err
	}
	current, err := s.splitPreflightCheckpointCurrent(ctx, expected, pass)
	if err != nil || !current {
		return false, err
	}
	ready, err := s.splitJobCapabilityReady(ctx, expected)
	if err != nil || !ready {
		return false, err
	}
	if err := s.verifyPreflightNamespace(ctx, expected, pass); err != nil {
		return false, err
	}
	err = s.revalidateSplitTransferAdmission(ctx, pass.witness)
	return err == nil, err
}

func (s *DistributionServer) splitPreflightCheckpointCurrent(ctx context.Context, expected distribution.SplitJob, pass splitPreflightPass) (bool, error) {
	snapshot, err := s.loadCatalogSnapshot(ctx)
	if err != nil || !pass.matchesCatalog(snapshot) {
		return false, err
	}
	current, found, err := s.catalog.SplitJobAt(ctx, expected.JobID, snapshot.ReadTS)
	if err != nil {
		return false, splitJobCatalogStatusError(err)
	}
	return found && distribution.SplitJobsEquivalent(current, expected), nil
}

func (s *DistributionServer) initializeSplitTransferPreflight(ctx context.Context, job distribution.SplitJob, pass splitPreflightPass) error {
	issued, err := issueSplitMigrationTimestamp(ctx, pass.source)
	if err != nil {
		return err
	}
	if issued.GetTimestamp() == 0 || issued.GetTimestamp() <= issued.GetLastCommitTs() {
		return errors.New("invalid migration preflight timestamp")
	}
	next := distribution.CloneSplitJob(job)
	next.TransferPreflightVersion = splitTransferPreflightVersion
	next.TransferPreflightTS = issued.GetTimestamp()
	next.TransferPreflightProgress = distribution.SplitJobBracketProgressForPlan(pass.brackets, distribution.SplitJobExportPhaseBackfill)
	next.TransferPreflightDone = false
	return s.persistSplitTransferPreflight(ctx, job, &next, pass)
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

func (pass splitPreflightPass) advance(ctx context.Context, job *distribution.SplitJob, bytesLeft int) (int, error) {
	index := slices.IndexFunc(job.TransferPreflightProgress, func(p distribution.SplitJobBracketProgress) bool { return !p.Done })
	if index < 0 {
		job.TransferPreflightDone = true
		return 0, nil
	}
	bracket := pass.brackets[index]
	progress := job.TransferPreflightProgress[index]
	attemptCtx, cancel := context.WithTimeout(ctx, defaultSplitMigrationAttemptTimeout)
	defer cancel()
	chunkBytes := min(preflightPageBytes, max(1, bytesLeft))
	stream, err := pass.source.ExportRangeVersions(attemptCtx, &pb.ExportRangeVersionsRequest{
		RangeStart: bracket.Start, RangeEnd: bracket.End, MaxCommitTs: job.TransferPreflightTS,
		MinCommitTs: 0, Cursor: progress.Cursor, ChunkBytes: uint32(chunkBytes),
		RouteStart: job.SplitKey, RouteEnd: pass.route.End, MaxScannedBytes: preflightPageBytes,
		MaxPages: 1, KeyFamily: bracket.Family, ExcludeKnownInternal: bracket.ExcludeKnownInternal, ExcludePrefixes: bracket.ExcludePrefixes,
	})
	if err != nil {
		return 0, errors.WithStack(migrationSourceExportError(err))
	}
	resp, err := stream.Recv()
	if err != nil {
		return 0, errors.WithStack(migrationSourceExportError(err))
	}
	if !resp.GetDone() && (len(resp.GetNextCursor()) == 0 || bytes.Equal(progress.Cursor, resp.GetNextCursor())) {
		return 0, errors.New("migration preflight cursor did not progress")
	}
	if err := checkMigrationExportPageSize(resp); err != nil {
		// A local validation error is not an explicit failure from the source.
		return 0, err
	}
	progress.Cursor = distribution.CloneBytes(resp.GetNextCursor())
	progress.Done = resp.GetDone()
	if resp.GetDone() {
		// Completed families need only their completion witness, not a retained
		// full-key cursor for every family in the catalog job record.
		progress.Cursor = nil
	}
	progress.AcceptedRows += uint64(len(resp.GetVersions()))
	job.TransferPreflightProgress[index] = progress
	return migrationExportPagePayloadBytes(resp), nil
}

func (s *DistributionServer) persistSplitTransferPreflight(ctx context.Context, expected distribution.SplitJob, next *distribution.SplitJob, pass splitPreflightPass) error {
	if err := recordOnlineSplitCatchUpGoal(ctx, next, pass.source, pass.route.End); err != nil {
		return err
	}
	ready, err := s.canContinueSplitPreflight(ctx, expected, pass)
	if err != nil || !ready {
		return err
	}
	return s.writeSplitPreflightJob(ctx, expected, *next, pass)
}

func (s *DistributionServer) abandonOversizedSplitTransfer(ctx context.Context, job distribution.SplitJob, cause error) error {
	if !errors.Is(cause, errMigrationOversizedSourceVersion) {
		return cause
	}
	if err := ctx.Err(); err != nil {
		return errors.WithStack(err)
	}
	var failure *splitPreflightExportFailure
	if errors.As(cause, &failure) {
		return s.abandonSplitPreflightFailure(ctx, failure, cause)
	}
	return s.updateSplitJobViaCoordinator(ctx, job.JobID, func(current distribution.SplitJob) (distribution.SplitJob, error) {
		if splitPreflightIncomplete(job) && !distribution.SplitJobsEquivalent(current, job) {
			return current, nil
		}
		if !splitTransferCanAutoAbandon(current.Phase) || current.Phase != job.Phase || current.SnapshotTS != job.SnapshotTS || current.FenceTS != job.FenceTS {
			return current, nil
		}
		return oversizedSplitAbandonState(current, cause)
	})
}

func splitTransferCanAutoAbandon(phase distribution.SplitJobPhase) bool {
	return phase == distribution.SplitJobPhasePlanned || phase == distribution.SplitJobPhaseBackfill ||
		phase == distribution.SplitJobPhaseFence || phase == distribution.SplitJobPhaseDeltaCopy
}

func (s *DistributionServer) verifyPreflightNamespace(ctx context.Context, expected distribution.SplitJob, pass splitPreflightPass) error {
	if !pass.namespaceAdmission || !s.MigrationPartitionRuntimeConfigured(expected.SourceGroupID) {
		return nil
	}
	return s.ensureSplitJobNamespaceAdmission(ctx, expected)
}
