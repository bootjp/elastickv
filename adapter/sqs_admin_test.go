package adapter

import (
	"strconv"
	"testing"
	"time"
)

const testQueueArn = "arn:aws:sqs:us-east-1:000000000000:orders"

// TestAdminQueueSummary_CreatedAtUsesMillisNotHLC pins the
// invariant that the admin AdminDescribeQueue path derives
// CreatedAt from sqsQueueMeta.CreatedAtMillis (the canonical
// wall-clock field), not from hlcToTime(CreatedAtHLC) — the meta
// struct documents HLC as "unsuitable for wall-clock display" and
// the SigV4 path (sqs_catalog.go:942) reads CreatedAtMillis. Two
// failure modes the test pins:
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
		summary := adminQueueSummary("orders", &meta, sqsApproxCounters{}, testQueueArn)
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
		summary := adminQueueSummary("orders", &meta, sqsApproxCounters{}, testQueueArn)
		want := time.UnixMilli(wantMillis).UTC()
		if !summary.CreatedAt.Equal(want) {
			t.Fatalf("CreatedAt=%v want=%v", summary.CreatedAt, want)
		}
		if summary.CreatedAt.Location() != time.UTC {
			t.Fatalf("CreatedAt location=%v want UTC", summary.CreatedAt.Location())
		}
	})
}

// TestMetaAttributesForAdmin_IncludesQueueArnAndLastModified pins
// the parity contract between metaAttributesForAdmin and
// queueMetaToAttributes("All"): QueueArn (the AWS-shaped identifier
// the SPA shows for change-tracking) and LastModifiedTimestamp
// (updated on SetQueueAttributes — the only handle operators have
// on "when did somebody last touch this queue's config") must both
// be present.
func TestMetaAttributesForAdmin_IncludesQueueArnAndLastModified(t *testing.T) {
	t.Parallel()

	t.Run("QueueArn always present", func(t *testing.T) {
		t.Parallel()
		meta := sqsQueueMeta{Name: "orders", Generation: 1}
		attrs := metaAttributesForAdmin(&meta, testQueueArn)
		got, ok := attrs["QueueArn"]
		if !ok {
			t.Fatalf("QueueArn missing from attributes: %v", attrs)
		}
		if got != testQueueArn {
			t.Fatalf("QueueArn=%q want=%q", got, testQueueArn)
		}
	})

	t.Run("LastModifiedTimestamp emitted in unix seconds when populated", func(t *testing.T) {
		t.Parallel()
		const wantMillis int64 = 1_724_419_200_000 // 2024-08-23T12:00:00Z
		meta := sqsQueueMeta{
			Name:                 "orders",
			Generation:           1,
			LastModifiedAtMillis: wantMillis,
		}
		attrs := metaAttributesForAdmin(&meta, testQueueArn)
		got, ok := attrs["LastModifiedTimestamp"]
		if !ok {
			t.Fatalf("LastModifiedTimestamp missing from attributes: %v", attrs)
		}
		want := strconv.FormatInt(wantMillis/sqsMillisPerSecond, 10)
		if got != want {
			t.Fatalf("LastModifiedTimestamp=%q want=%q (unix seconds)", got, want)
		}
	})

	t.Run("LastModifiedTimestamp omitted when zero", func(t *testing.T) {
		t.Parallel()
		meta := sqsQueueMeta{Name: "orders", Generation: 1}
		attrs := metaAttributesForAdmin(&meta, testQueueArn)
		if _, ok := attrs["LastModifiedTimestamp"]; ok {
			t.Fatalf("LastModifiedTimestamp should be omitted when zero: got %q", attrs["LastModifiedTimestamp"])
		}
	})

	t.Run("CreatedTimestamp deliberately not in map (typed field instead)", func(t *testing.T) {
		t.Parallel()
		meta := sqsQueueMeta{
			Name:            "orders",
			Generation:      1,
			CreatedAtMillis: 1_724_419_200_000,
		}
		attrs := metaAttributesForAdmin(&meta, testQueueArn)
		if _, ok := attrs["CreatedTimestamp"]; ok {
			t.Fatalf("CreatedTimestamp must NOT be in attrs (it lives on AdminQueueSummary.CreatedAt): got %q", attrs["CreatedTimestamp"])
		}
	})
}
