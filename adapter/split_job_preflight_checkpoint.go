package adapter

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/distribution"
	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

func (pass splitPreflightPass) matchesCatalog(snapshot distribution.CatalogSnapshot) bool {
	if snapshot.Version != pass.catalogVersion {
		return false
	}
	route, found := findRouteByID(snapshot.Routes, pass.route.RouteID)
	return found && pass.route.Equal(route)
}

// The catalog geometry and the live checkpoint are observed at one read TS.
// OCC closes the gap after validation, including catalog-only route changes.
func (s *DistributionServer) writeSplitPreflightJob(ctx context.Context, expected distribution.SplitJob, next distribution.SplitJob, pass splitPreflightPass) error {
	current, readTS, err := s.catalog.LiveSplitJobForUpdate(ctx, expected.JobID)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	snapshot, err := s.loadCatalogSnapshotAt(ctx, readTS)
	if err != nil {
		return err
	}
	if !pass.matchesCatalog(snapshot) || !distribution.SplitJobsEquivalent(current, expected) {
		return nil
	}
	next.UpdatedAtMs = time.Now().UnixMilli()
	encoded, err := distribution.EncodeSplitJob(next)
	if err != nil {
		return splitJobCatalogStatusError(err)
	}
	key := distribution.CatalogSplitJobKey(expected.JobID)
	_, err = s.coordinator.Dispatch(ctx, &kv.OperationGroup[kv.OP]{Elems: []*kv.Elem[kv.OP]{{Op: kv.Put, Key: key, Value: encoded}}, IsTxn: true, StartTS: readTS, ReadKeys: [][]byte{key, distribution.CatalogVersionKey(), distribution.CatalogRouteKey(pass.route.RouteID)}})
	if err != nil {
		return splitJobCoordinatorStatusError(err)
	}
	return nil
}

// Only authenticated oversized preflight verdicts carry the original scan
// authority into compensation. Tentative pages never authorize abandonment.
type splitPreflightExportFailure struct {
	cause    error
	expected distribution.SplitJob
	pass     splitPreflightPass
}

func (e *splitPreflightExportFailure) Error() string { return e.cause.Error() }
func (e *splitPreflightExportFailure) Unwrap() error { return e.cause }

func bindPreflightExportFailure(err error, expected distribution.SplitJob, pass splitPreflightPass) error {
	if !errors.Is(err, errMigrationOversizedSourceVersion) {
		return err
	}
	return &splitPreflightExportFailure{cause: err, expected: expected, pass: pass}
}

func (s *DistributionServer) abandonSplitPreflightFailure(ctx context.Context, failure *splitPreflightExportFailure, cause error) error {
	ready, err := s.canContinueSplitPreflight(ctx, failure.expected, failure.pass)
	if err != nil || !ready {
		return err
	}
	next, err := oversizedSplitAbandonState(failure.expected, cause)
	if err != nil {
		return err
	}
	return s.writeSplitPreflightJob(ctx, failure.expected, next, failure.pass)
}

func oversizedSplitAbandonState(current distribution.SplitJob, cause error) (distribution.SplitJob, error) {
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
}
