package monitoring

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestColdStartObserver_ExecutedAbsoluteGap pins codex P2 #934
// round 3. The skip-gate threshold shifted to the WAL committed
// tail, so an executed restore can now fire when have >= snapIndex
// (FSM ahead of snapshot but behind the committed tail after a
// crash). The previous `snapIndex - have` subtraction underflowed
// into ~2^64 in that case. Verify the gauge stores the absolute
// distance.
func TestColdStartObserver_ExecutedAbsoluteGap(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		snapIndex uint64
		have      uint64
		wantGap   float64
	}{
		{"FSM behind snapshot (legacy executed case)", 200, 100, 100},
		{"FSM ahead of snapshot but behind WAL tail (new case)", 100, 150, 50},
		{"FSM equal to snapshot", 100, 100, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			reg := prometheus.NewRegistry()
			metrics := newColdStartMetrics(reg)
			obs := newColdStartObserver(metrics)

			obs.RestoreExecuted(c.snapIndex, c.have)

			got := testutil.ToFloat64(metrics.appliedIndexGap.WithLabelValues("executed"))
			if got != c.wantGap {
				t.Errorf("appliedIndexGap[executed] = %v, want %v", got, c.wantGap)
			}
			// Counter must increment once.
			if cnt := testutil.ToFloat64(metrics.restoreTotal.WithLabelValues("executed", "")); cnt != 1 {
				t.Errorf("restoreTotal[executed,\"\"] = %v, want 1", cnt)
			}
		})
	}
}

// TestColdStartObserver_SkippedGap pins the skipped-path gauge:
// gap = have - snapIndex (always >= 0 on the skip path; the gate
// guarantees have >= threshold >= snapIndex).
func TestColdStartObserver_SkippedGap(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	metrics := newColdStartMetrics(reg)
	obs := newColdStartObserver(metrics)

	obs.RestoreSkipped(100, 250)

	got := testutil.ToFloat64(metrics.appliedIndexGap.WithLabelValues("skipped"))
	if got != 150 {
		t.Errorf("appliedIndexGap[skipped] = %v, want 150", got)
	}
}

// TestColdStartObserver_NilSafe verifies that a nil observer is a
// no-op (the engine constructs nil observers when monitoring is
// disabled).
func TestColdStartObserver_NilSafe(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("nil observer must not panic, got: %v", r)
		}
	}()
	var obs *ColdStartObserver
	obs.RestoreSkipped(1, 2)
	obs.RestoreExecuted(1, 2)
	obs.RestoreFallback(1, "read_err")
}

// TestColdStartObserver_FallbackReasonLabel pins the strictly-
// additive fallback path: the counter increments under the
// fallback outcome with the supplied reason label; no gauge value
// is reported.
func TestColdStartObserver_FallbackReasonLabel(t *testing.T) {
	t.Parallel()
	for _, reason := range []string{"not_reader", "missing_meta", "read_err"} {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			reg := prometheus.NewRegistry()
			metrics := newColdStartMetrics(reg)
			obs := newColdStartObserver(metrics)

			obs.RestoreFallback(100, reason)
			if cnt := testutil.ToFloat64(metrics.restoreTotal.WithLabelValues("fallback", reason)); cnt != 1 {
				t.Errorf("restoreTotal[fallback,%q] = %v, want 1", reason, cnt)
			}
		})
	}
}
