package store

import "github.com/cockroachdb/errors"

// MigrationCaptureWriter records the outcomes of sequential point requests in
// one explicit entry. Like the entry context, it cannot be retained or shared
// with another goroutine. A callback error aborts the entire entry.
type MigrationCaptureWriter struct {
	collector *migrationCaptureCollector
	active    bool
	failed    error
}

func (w *MigrationCaptureWriter) Request(apply func() (MigrationCaptureOutcome, error)) error {
	if w.failed != nil {
		return w.failed
	}
	if !w.active || apply == nil || w.collector.recording || len(w.collector.entry.Outcomes) >= MaxMigrationCaptureRequests {
		return w.fail(errors.WithStack(ErrMigrationCaptureInput))
	}
	w.collector.request = uint64(len(w.collector.entry.Outcomes))
	w.collector.stagedRequest = false
	w.collector.recording = true
	defer func() { w.collector.recording = false }()
	outcome, err := apply()
	if err != nil {
		return w.fail(errors.WithStack(err))
	}
	if !validMigrationCaptureOutcomes([]MigrationCaptureOutcome{outcome}) {
		return w.fail(errors.WithStack(ErrMigrationCaptureInput))
	}
	if w.collector.stagedRequest && outcome != MigrationCaptureApplied {
		return w.fail(errors.WithStack(ErrMigrationCaptureCorrupt))
	}
	w.collector.entry.Outcomes = append(w.collector.entry.Outcomes, outcome)
	return nil
}

func (w *MigrationCaptureWriter) fail(err error) error {
	if w.failed == nil {
		w.failed = err
	}
	return w.failed
}
