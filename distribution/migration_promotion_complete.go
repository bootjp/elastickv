package distribution

import (
	"bytes"
	"context"
	"math"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

var (
	ErrMigrationPromotionNotReady     = errors.New("migration target promotion is not ready")
	ErrMigrationPromotionTargetAbsent = errors.New("migration target promotion route is missing")
)

// TargetPromotionCompletion is the result of applying the default-group
// promotion-complete transition to a SplitJob and its catalog routes.
type TargetPromotionCompletion struct {
	Job             SplitJob
	Routes          []RouteDescriptor
	Changed         bool
	ClearedRouteIDs []uint64
}

// CompleteTargetPromotionState clears the target route's staged visibility
// fields after target-local promotion has completed. It deliberately preserves
// MinWriteTSExclusive; the timestamp floor remains a durable route invariant
// after the staged/live merge is no longer needed.
func CompleteTargetPromotionState(job SplitJob, routes []RouteDescriptor, nowMs int64) (TargetPromotionCompletion, error) {
	normalized, err := normalizePromotionCompletionInput(job, routes)
	if err != nil {
		return TargetPromotionCompletion{}, err
	}

	out := TargetPromotionCompletion{
		Job:    CloneSplitJob(job),
		Routes: normalized,
	}
	if out.Job.TargetPromotionDone {
		if targetClearedDescriptorPresent(out.Job, out.Routes) {
			return out, nil
		}
		return TargetPromotionCompletion{}, errors.WithStack(ErrMigrationPromotionTargetAbsent)
	}

	cleared, err := clearTargetPromotionRoutes(job, out.Routes)
	if err != nil {
		return TargetPromotionCompletion{}, err
	}
	if len(cleared) == 0 {
		return TargetPromotionCompletion{}, errors.WithStack(ErrMigrationPromotionTargetAbsent)
	}

	out.Changed = true
	out.ClearedRouteIDs = cleared
	out.Job.TargetPromotionDone = true
	out.Job.UpdatedAtMs = nowMs
	return out, nil
}

func normalizePromotionCompletionInput(job SplitJob, routes []RouteDescriptor) ([]RouteDescriptor, error) {
	if err := validateSplitJob(job); err != nil {
		return nil, err
	}
	if job.Phase != SplitJobPhaseCleanup {
		return nil, errors.WithStack(ErrMigrationPromotionNotReady)
	}
	return normalizeRoutes(routes)
}

func clearTargetPromotionRoutes(job SplitJob, routes []RouteDescriptor) ([]uint64, error) {
	cleared := make([]uint64, 0, 1)
	for i := range routes {
		route := &routes[i]
		if route.MigrationJobID != job.JobID {
			continue
		}
		if !route.StagedVisibilityActive ||
			route.GroupID != job.TargetGroupID ||
			route.ParentRouteID != job.SourceRouteID ||
			!bytes.Equal(route.Start, job.SplitKey) {
			return nil, errors.WithStack(ErrMigrationInvalidRoute)
		}
		route.StagedVisibilityActive = false
		route.MigrationJobID = 0
		cleared = append(cleared, route.RouteID)
	}
	return cleared, nil
}

// CompleteSplitJobTargetPromotion applies the promotion-complete catalog CAS:
// route descriptor staged fields are cleared, catalog version is bumped, and
// the SplitJob witness is updated in the same MVCC batch.
func (s *CatalogStore) CompleteSplitJobTargetPromotion(
	ctx context.Context,
	expectedVersion uint64,
	expected SplitJob,
	nowMs int64,
) (CatalogSnapshot, SplitJob, error) {
	if err := ensureCatalogStore(s); err != nil {
		return CatalogSnapshot{}, SplitJob{}, err
	}
	ctx = contextOrBackground(ctx)

	readTS, currentVersion, routes, err := s.loadPromotionCompleteInputs(ctx, expectedVersion, expected)
	if err != nil {
		return CatalogSnapshot{}, SplitJob{}, err
	}
	completion, err := CompleteTargetPromotionState(expected, routes, nowMs)
	if err != nil {
		return CatalogSnapshot{}, SplitJob{}, err
	}
	if !completion.Changed {
		return CatalogSnapshot{
			Version: currentVersion,
			Routes:  cloneRouteDescriptors(completion.Routes),
			ReadTS:  readTS,
		}, completion.Job, nil
	}

	plan, mutations, commitTS, err := s.buildPromotionCompleteMutations(ctx, readTS, expectedVersion, expected.JobID, &completion)
	if err != nil {
		return CatalogSnapshot{}, SplitJob{}, err
	}
	if err := s.applyPromotionCompleteMutations(ctx, plan, mutations, expected.JobID, commitTS); err != nil {
		return CatalogSnapshot{}, SplitJob{}, err
	}

	return CatalogSnapshot{
		Version: plan.nextVersion,
		Routes:  cloneRouteDescriptors(completion.Routes),
	}, completion.Job, nil
}

func (s *CatalogStore) loadPromotionCompleteInputs(ctx context.Context, expectedVersion uint64, expected SplitJob) (uint64, uint64, []RouteDescriptor, error) {
	expectedRaw, err := EncodeSplitJob(expected)
	if err != nil {
		return 0, 0, nil, err
	}
	readTS := s.store.LastCommitTS()
	currentVersion, err := s.versionAt(ctx, readTS)
	if err != nil {
		return 0, 0, nil, err
	}
	if currentVersion != expectedVersion {
		return 0, 0, nil, errors.WithStack(ErrCatalogVersionMismatch)
	}
	if err := s.expectLiveSplitJobAt(ctx, expected.JobID, expectedRaw, readTS); err != nil {
		return 0, 0, nil, err
	}
	routes, err := s.routesAt(ctx, readTS)
	if err != nil {
		return 0, 0, nil, err
	}
	return readTS, currentVersion, routes, nil
}

func (s *CatalogStore) buildPromotionCompleteMutations(
	ctx context.Context,
	readTS uint64,
	expectedVersion uint64,
	jobID uint64,
	completion *TargetPromotionCompletion,
) (savePlan, []*store.KVPairMutation, uint64, error) {
	if expectedVersion == math.MaxUint64 {
		return savePlan{}, nil, 0, errors.WithStack(ErrCatalogVersionOverflow)
	}
	minCommitTS := readTS + 1
	if minCommitTS == 0 {
		return savePlan{}, nil, 0, errors.WithStack(ErrCatalogVersionOverflow)
	}

	plan := savePlan{
		readTS:      readTS,
		minCommitTS: minCommitTS,
		nextVersion: expectedVersion + 1,
		routes:      completion.Routes,
	}
	mutations, err := s.buildSaveMutations(ctx, plan)
	if err != nil {
		return savePlan{}, nil, 0, err
	}
	commitTS, err := s.commitTSForApply(plan.minCommitTS)
	if err != nil {
		return savePlan{}, nil, 0, err
	}
	completion.Job.PromotionCompletedTS = commitTS
	encodedJob, err := EncodeSplitJob(completion.Job)
	if err != nil {
		return savePlan{}, nil, 0, err
	}
	jobMutations, err := s.buildSplitJobPutMutations(ctx, readTS, CatalogSplitJobKey(jobID), encodedJob, jobID)
	if err != nil {
		return savePlan{}, nil, 0, err
	}
	return plan, append(mutations, jobMutations...), commitTS, nil
}

func (s *CatalogStore) applyPromotionCompleteMutations(ctx context.Context, plan savePlan, mutations []*store.KVPairMutation, jobID uint64, commitTS uint64) error {
	readKeys := [][]byte{
		CatalogVersionKey(),
		CatalogSplitJobKey(jobID),
	}
	if err := s.store.ApplyMutations(ctx, mutations, readKeys, plan.readTS, commitTS); err != nil {
		if errors.Is(err, store.ErrWriteConflict) {
			return errors.WithStack(ErrCatalogSplitJobConflict)
		}
		return errors.WithStack(err)
	}
	return nil
}

func targetClearedDescriptorPresent(job SplitJob, routes []RouteDescriptor) bool {
	for _, route := range routes {
		if route.GroupID != job.TargetGroupID {
			continue
		}
		if route.StagedVisibilityActive || route.MigrationJobID != 0 {
			continue
		}
		if route.ParentRouteID != job.SourceRouteID {
			continue
		}
		if bytes.Equal(route.Start, job.SplitKey) {
			return true
		}
	}
	return false
}
