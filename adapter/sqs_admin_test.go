package adapter

import (
	"testing"
	"time"
)

// TestAdminQueueSummary_CreatedAtUsesMillisNotHLC pins the regression
// from the fifth-round Claude review on PR #670: the admin
// AdminDescribeQueue path was producing CreatedAt from
// hlcToTime(meta.CreatedAtHLC), but sqsQueueMeta documents HLC as
// "unsuitable for wall-clock display" and the SigV4 path
// (sqs_catalog.go:942) reads CreatedAtMillis. Two failure modes the
// test pins:
//
//  1. CreatedAtMillis == 0 must yield a zero time.Time so the JSON
//     encoder's omitempty drops the field and the SPA renders "—"
//     rather than the HLC-derived 1970-01-01T00:00:00Z.
//  2. CreatedAtMillis > 0 must round-trip through time.UnixMilli in
//     UTC.
func TestAdminQueueSummary_CreatedAtUsesMillisNotHLC(t *testing.T) {
	t.Parallel()

	t.Run("zero millis yields zero time even with HLC populated", func(t *testing.T) {
		t.Parallel()
		meta := sqsQueueMeta{
			Name:         "orders",
			Generation:   1,
			CreatedAtHLC: 42 << s3HLCPhysicalShift, // would render as ~1970 epoch via hlcToTime
			// CreatedAtMillis intentionally zero
		}
		summary := adminQueueSummary("orders", &meta, sqsApproxCounters{})
		if !summary.CreatedAt.IsZero() {
			t.Fatalf("CreatedAt should be zero when CreatedAtMillis==0; got %v", summary.CreatedAt)
		}
	})

	t.Run("positive millis round-trips via time.UnixMilli UTC", func(t *testing.T) {
		t.Parallel()
		const wantMillis int64 = 1_724_419_200_000 // 2024-08-23T12:00:00Z
		meta := sqsQueueMeta{
			Name:            "orders",
			Generation:      2,
			CreatedAtMillis: wantMillis,
			CreatedAtHLC:    1, // must be ignored
		}
		summary := adminQueueSummary("orders", &meta, sqsApproxCounters{})
		want := time.UnixMilli(wantMillis).UTC()
		if !summary.CreatedAt.Equal(want) {
			t.Fatalf("CreatedAt=%v want=%v", summary.CreatedAt, want)
		}
		if summary.CreatedAt.Location() != time.UTC {
			t.Fatalf("CreatedAt location=%v want UTC", summary.CreatedAt.Location())
		}
	})
}
