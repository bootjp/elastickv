package server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/bootjp/elastickv/distribution"
)

// A separate observer bounds the durable transitions without changing the
// usual 100 ms attribution of foreground traffic to migration phases.
func watchPerformancePreflight(t *testing.T, catalog *distribution.CatalogStore, jobID uint64) func() {
	t.Helper()
	if os.Getenv("ELASTICKV_PERF_PREFLIGHT_TIMING") != "1" {
		return func() {}
	}
	ctx, cancel := context.WithCancel(t.Context())
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		var window performancePreflightWindow
		stopping := false
		for {
			start := time.Now()
			job, found, err := catalog.SplitJob(ctx, jobID)
			if err == nil && found && window.observe(t, job, start, time.Now()) {
				return
			}
			if stopping {
				return
			}
			select {
			case <-ticker.C:
			case <-stop:
				stopping = true
			case <-ctx.Done():
				return
			}
		}
	}()
	return func() { close(stop); <-done; cancel() }
}

type performancePreflightWindow struct{ lastBefore, firstInside, lastInside time.Time }

func (w *performancePreflightWindow) observe(t *testing.T, job distribution.SplitJob, start, end time.Time) bool {
	t.Helper()
	switch {
	case job.TransferPreflightDone:
		if w.lastBefore.IsZero() || w.firstInside.IsZero() {
			t.Errorf("missing observations bounding preflight for job %d", job.JobID)
			return true
		}
		t.Logf("PERF preflight-duration lower_ms=%.3f upper_ms=%.3f", float64(w.lastInside.Sub(w.firstInside).Microseconds())/1000, float64(end.Sub(w.lastBefore).Microseconds())/1000)
		var versions uint64
		completed := 0
		for _, progress := range job.TransferPreflightProgress {
			versions += progress.AcceptedRows
			if progress.Done {
				completed++
			}
		}
		t.Logf("PERF preflight-work families=%d completed=%d versions=%d", len(job.TransferPreflightProgress), completed, versions)
		return true
	case job.Phase == distribution.SplitJobPhaseFence:
		if w.firstInside.IsZero() {
			w.firstInside = end
		}
		w.lastInside = start
	default:
		w.lastBefore = start
	}
	return false
}
