package adapter

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
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

// TestAdminPurgeQueue_HappyPath confirms AdminPurgeQueue against the
// happy path: the queue exists, the principal is full-access, and
// the call commits a generation bump that the returned AdminPurgeResult
// faithfully records. Mirrors TestDynamoDB_AdminDeleteTable_HappyPath
// on the dynamo side so the admin patterns stay parallel.
func TestAdminPurgeQueue_HappyPath(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_ = createSQSQueueForTest(t, node, "purgeable")

	res, err := node.sqsServer.AdminPurgeQueue(context.Background(), fullAdminPrincipal, "purgeable")
	if err != nil {
		t.Fatalf("AdminPurgeQueue: %v", err)
	}
	if res.GenerationBefore == 0 {
		t.Fatalf("GenerationBefore=%d want >0", res.GenerationBefore)
	}
	if res.GenerationAfter != res.GenerationBefore+1 {
		t.Fatalf("GenerationAfter=%d want %d (before+1)", res.GenerationAfter, res.GenerationBefore+1)
	}
}

// TestAdminPurgeQueue_RateLimitedReturnsTypedError exercises the
// 60-second cooldown. A second purge inside the window must return
// a typed *PurgeInProgressError that satisfies
// errors.Is(ErrAdminSQSPurgeInProgress) and carries a non-zero
// RetryAfter duration in (0, 60s]. This pins the typed-error
// propagation path that AdminPurgeQueue uses to avoid a second meta
// read (which would race a concurrent purge).
func TestAdminPurgeQueue_RateLimitedReturnsTypedError(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_ = createSQSQueueForTest(t, node, "rate-limited")
	ctx := context.Background()

	if _, err := node.sqsServer.AdminPurgeQueue(ctx, fullAdminPrincipal, "rate-limited"); err != nil {
		t.Fatalf("first AdminPurgeQueue: %v", err)
	}
	_, err := node.sqsServer.AdminPurgeQueue(ctx, fullAdminPrincipal, "rate-limited")
	if err == nil {
		t.Fatalf("second AdminPurgeQueue: want PurgeInProgressError, got nil")
	}
	if !errors.Is(err, ErrAdminSQSPurgeInProgress) {
		t.Fatalf("errors.Is(ErrAdminSQSPurgeInProgress): got false; err=%v", err)
	}
	var typed *PurgeInProgressError
	if !errors.As(err, &typed) {
		t.Fatalf("errors.As(*PurgeInProgressError): got false; err=%v", err)
	}
	if typed.RetryAfter <= 0 {
		t.Fatalf("RetryAfter=%v want >0", typed.RetryAfter)
	}
	if typed.RetryAfter > 60*time.Second {
		t.Fatalf("RetryAfter=%v want <=60s", typed.RetryAfter)
	}
}

// TestAdminPurgeQueue_ReadOnlyForbidden pins the live-role check:
// a read-only principal must not be able to purge, mirroring
// AdminDeleteQueue's gate exactly.
func TestAdminPurgeQueue_ReadOnlyForbidden(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	_ = createSQSQueueForTest(t, node, "read-only-test")

	_, err := node.sqsServer.AdminPurgeQueue(context.Background(), readOnlyAdminPrincipal, "read-only-test")
	if !errors.Is(err, ErrAdminForbidden) {
		t.Fatalf("read-only principal: want ErrAdminForbidden, got %v", err)
	}
}

// TestAdminPurgeQueue_MissingQueue confirms a missing queue surfaces
// the structured ErrAdminSQSNotFound sentinel rather than a generic
// 500 — admin handlers map this to HTTP 404 without sniffing the
// AWS error code.
func TestAdminPurgeQueue_MissingQueue(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	_, err := node.sqsServer.AdminPurgeQueue(context.Background(), fullAdminPrincipal, "no-such-queue")
	if !errors.Is(err, ErrAdminSQSNotFound) {
		t.Fatalf("missing queue: want ErrAdminSQSNotFound, got %v", err)
	}
}

// TestAdminPurgeQueue_EmptyName pins the up-front guard so empty /
// whitespace names never reach the coordinator. Mirrors
// AdminDeleteQueue's identical guard.
func TestAdminPurgeQueue_EmptyName(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)

	for _, name := range []string{"", "   "} {
		_, err := node.sqsServer.AdminPurgeQueue(context.Background(), fullAdminPrincipal, name)
		if !errors.Is(err, ErrAdminSQSValidation) {
			t.Fatalf("name=%q: want ErrAdminSQSValidation, got %v", name, err)
		}
	}
}

// TestPurgeQueueSigV4_PreservesWireShapeAfterTypedErrorChange is the
// caller-audit pin from the design doc §6.1 ("TestPurgeQueueSigV4_Still
// Compiles"). It exercises the SigV4 PurgeQueue handler through the
// modified purgeQueueWithRetry signature and asserts the wire shape
// (status 400 + __type ==
// AWS.SimpleQueueService.PurgeQueueInProgress) is unchanged. If a
// future refactor drops the *purgeRateLimitedError branch from
// writeSQSErrorFromErr, this test fails — without it the SigV4
// response silently regresses to a generic 500.
func TestPurgeQueueSigV4_PreservesWireShapeAfterTypedErrorChange(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	url := createSQSQueueForTest(t, node, "sigv4-rate-limit")

	status, _ := callSQS(t, node, sqsPurgeQueueTarget, map[string]any{"QueueUrl": url})
	if status != http.StatusOK {
		t.Fatalf("first purge: %d want 200", status)
	}
	status, out := callSQS(t, node, sqsPurgeQueueTarget, map[string]any{"QueueUrl": url})
	if status != http.StatusBadRequest {
		t.Fatalf("second purge: status=%d want 400 (%v)", status, out)
	}
	got, _ := out["__type"].(string)
	if got != sqsErrPurgeInProgress {
		t.Fatalf("__type=%q want %q", got, sqsErrPurgeInProgress)
	}
}

// TestAdminQueueSummary_IsDLQ_NoSources confirms a queue that nobody
// points at reports IsDLQ=false and an empty DLQSources slice.
func TestAdminQueueSummary_IsDLQ_NoSources(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	_ = createSQSQueueForTest(t, node, "lonely")

	summary, exists, err := node.sqsServer.AdminDescribeQueue(context.Background(), "lonely")
	if err != nil {
		t.Fatalf("AdminDescribeQueue: %v", err)
	}
	if !exists {
		t.Fatalf("queue not found")
	}
	if summary.IsDLQ {
		t.Fatalf("IsDLQ=true want false")
	}
	if len(summary.DLQSources) != 0 {
		t.Fatalf("DLQSources=%v want empty", summary.DLQSources)
	}
}

// TestAdminQueueSummary_IsDLQ_OneSource confirms one inbound
// RedrivePolicy is detected and surfaced. Uses the public CreateQueue
// path so the meta record carries the RedrivePolicy attribute
// exactly as a real client would have written it.
func TestAdminQueueSummary_IsDLQ_OneSource(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	_ = createSQSQueueForTest(t, node, "dlq-target")

	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:dlq-target","maxReceiveCount":5}`
	status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
		"QueueName":  "source-a",
		"Attributes": map[string]string{"RedrivePolicy": policy},
	})
	if status != http.StatusOK {
		t.Fatalf("create source: %d %v", status, out)
	}

	summary, exists, err := node.sqsServer.AdminDescribeQueue(context.Background(), "dlq-target")
	if err != nil {
		t.Fatalf("AdminDescribeQueue: %v", err)
	}
	if !exists {
		t.Fatalf("queue not found")
	}
	if !summary.IsDLQ {
		t.Fatalf("IsDLQ=false want true")
	}
	if len(summary.DLQSources) != 1 || summary.DLQSources[0] != "source-a" {
		t.Fatalf("DLQSources=%v want [source-a]", summary.DLQSources)
	}
}

// TestAdminQueueSummary_IsDLQ_TwoSourcesSorted pins the sort
// invariant: callers can rely on DLQSources being lexicographic so
// the SPA renders chips in a stable order and audit log diffs
// are noise-free.
func TestAdminQueueSummary_IsDLQ_TwoSourcesSorted(t *testing.T) {
	t.Parallel()
	nodes, _, _ := createNode(t, 1)
	defer shutdown(nodes)
	node := sqsLeaderNode(t, nodes)
	_ = createSQSQueueForTest(t, node, "shared-dlq")

	policy := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:shared-dlq","maxReceiveCount":3}`
	// Create in reverse-alphabetical order on purpose so the
	// sort.Strings call is exercised (not just a coincidental
	// insertion-order match).
	for _, name := range []string{"zeta", "alpha"} {
		status, out := callSQS(t, node, sqsCreateQueueTarget, map[string]any{
			"QueueName":  name,
			"Attributes": map[string]string{"RedrivePolicy": policy},
		})
		if status != http.StatusOK {
			t.Fatalf("create %s: %d %v", name, status, out)
		}
	}

	summary, exists, err := node.sqsServer.AdminDescribeQueue(context.Background(), "shared-dlq")
	if err != nil {
		t.Fatalf("AdminDescribeQueue: %v", err)
	}
	if !exists {
		t.Fatalf("queue not found")
	}
	if !summary.IsDLQ {
		t.Fatalf("IsDLQ=false want true")
	}
	want := []string{"alpha", "zeta"}
	if len(summary.DLQSources) != len(want) {
		t.Fatalf("DLQSources=%v want %v", summary.DLQSources, want)
	}
	for i, name := range want {
		if summary.DLQSources[i] != name {
			t.Fatalf("DLQSources[%d]=%q want %q (full=%v)", i, summary.DLQSources[i], name, summary.DLQSources)
		}
	}
}
