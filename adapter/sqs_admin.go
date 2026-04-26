package adapter

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
)

// AdminQueueSummary is the per-queue projection the admin dashboard
// surfaces. It deliberately covers only the fields the SPA renders so
// the package's wire-format types stay internal.
//
// Counters mirror the AWS Approximate* attribute set produced by
// scanApproxCounters; they are best-effort by AWS contract and stop
// counting once the catalog's per-call cap is reached (the SPA polls
// continuously, so an unbounded scan would pin the leader).
type AdminQueueSummary struct {
	Name       string
	IsFIFO     bool
	Generation uint64
	CreatedAt  time.Time
	Attributes map[string]string
	Counters   AdminQueueCounters
}

// AdminQueueCounters matches sqsApproxCounters (int64) so the admin
// bridge does not have to convert between widths. Visible /
// NotVisible / Delayed are the AWS Approximate* triple.
type AdminQueueCounters struct {
	Visible    int64
	NotVisible int64
	Delayed    int64
}

// AdminListQueues returns every queue name this server knows about,
// in the lexicographic order the queue catalog index produces. Read
// path; runs on follower or leader and uses the same scanQueueNames
// helper the SigV4 ListQueues handler does.
func (s *SQSServer) AdminListQueues(ctx context.Context) ([]string, error) {
	return s.scanQueueNames(ctx) //nolint:wrapcheck // pure pass-through; the adapter owns the error context.
}

// AdminDescribeQueue returns a snapshot of name's metadata plus the
// approximate counters. The triple (result, present, error) lets
// admin callers distinguish a missing queue from a storage error
// without sniffing sentinels.
//
// Like AdminDescribeTable on the Dynamo side, this entrypoint runs
// on either the leader or a follower (read-only); the counter scan
// uses a fresh nextTxnReadTS so the result is consistent with what
// SigV4 GetQueueAttributes would have returned at the same instant.
func (s *SQSServer) AdminDescribeQueue(ctx context.Context, name string) (*AdminQueueSummary, bool, error) {
	if strings.TrimSpace(name) == "" {
		return nil, false, ErrAdminSQSValidation
	}
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, name, readTS)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if !exists {
		return nil, false, nil
	}
	counters, err := s.scanApproxCounters(ctx, name, meta.Generation, readTS)
	if err != nil {
		return nil, false, err
	}
	return adminQueueSummary(name, meta, counters, s.queueArn(name)), true, nil
}

// adminQueueSummary projects a queue meta + counters into the
// SPA-facing AdminQueueSummary. CreatedAt comes from the canonical
// wall-clock CreatedAtMillis (not CreatedAtHLC, which the meta's own
// comment calls "unsuitable for wall-clock display"); a zero millis
// value yields a zero time.Time so the JSON omitempty drops the field
// and the SPA renders "—" instead of an HLC-derived 1970 epoch.
// queueArn is threaded in by the caller (AdminDescribeQueue) because
// the server's region lives on *SQSServer and the helper itself is
// kept method-free for unit-testability without a coordinator.
// Pulled into a helper so the conversion is unit-testable without
// standing up a full coordinator.
func adminQueueSummary(name string, meta *sqsQueueMeta, counters sqsApproxCounters, queueArn string) *AdminQueueSummary {
	var createdAt time.Time
	if meta.CreatedAtMillis > 0 {
		createdAt = time.UnixMilli(meta.CreatedAtMillis).UTC()
	}
	return &AdminQueueSummary{
		Name:       name,
		IsFIFO:     meta.IsFIFO,
		Generation: meta.Generation,
		CreatedAt:  createdAt,
		Attributes: metaAttributesForAdmin(meta, queueArn),
		Counters:   AdminQueueCounters(counters),
	}
}

// AdminDeleteQueue is the SigV4-bypass counterpart to deleteQueue.
// Returns the same sentinel errors as AdminCreateTable on the Dynamo
// side: ErrAdminForbidden on a read-only principal, ErrAdminNotLeader
// on a follower, ErrAdminSQSNotFound when the queue is absent.
func (s *SQSServer) AdminDeleteQueue(ctx context.Context, principal AdminPrincipal, name string) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !isVerifiedSQSLeader(s.coordinator) {
		return ErrAdminNotLeader
	}
	if strings.TrimSpace(name) == "" {
		return ErrAdminSQSValidation
	}
	if err := s.deleteQueueWithRetry(ctx, name); err != nil {
		// deleteQueueWithRetry returns sqsAPIError with
		// sqsErrQueueDoesNotExist when the queue is missing; map
		// to the structured ErrAdminSQSNotFound so the admin
		// handler can render 404 without sniffing the AWS code.
		if isSQSAdminQueueDoesNotExist(err) {
			return ErrAdminSQSNotFound
		}
		return errors.Wrap(err, "admin delete queue")
	}
	return nil
}

// metaAttributesForAdmin renders the non-counter queue config
// attributes. Mirrors queueMetaToAttributes("All") (sqs_catalog.go)
// except for two deliberate omissions:
//
//   - The Approximate* counters — the admin summary surfaces them as
//     the typed AdminQueueCounters struct alongside this map, so the
//     SPA can render them without round-tripping strings.
//   - CreatedTimestamp — surfaced as the typed AdminQueueSummary.CreatedAt
//     field for the same reason.
//
// LastModifiedTimestamp stays in the map (SetQueueAttributes updates
// LastModifiedAtMillis and operators need it for change-tracking;
// there is no dedicated typed field for it). QueueArn is included so
// the SPA can show the AWS-shaped identifier without recomputing it
// client-side. Claude P1 on PR #670 caught both gaps — the prior
// docstring claimed parity with queueMetaToAttributes("All") but
// QueueArn and LastModifiedTimestamp were absent.
func metaAttributesForAdmin(meta *sqsQueueMeta, queueArn string) map[string]string {
	out := map[string]string{
		"QueueArn":                      queueArn,
		"VisibilityTimeout":             strconv.FormatInt(meta.VisibilityTimeoutSeconds, 10),
		"MessageRetentionPeriod":        strconv.FormatInt(meta.MessageRetentionSeconds, 10),
		"DelaySeconds":                  strconv.FormatInt(meta.DelaySeconds, 10),
		"ReceiveMessageWaitTimeSeconds": strconv.FormatInt(meta.ReceiveMessageWaitSeconds, 10),
		"MaximumMessageSize":            strconv.FormatInt(meta.MaximumMessageSize, 10),
		"FifoQueue":                     strconv.FormatBool(meta.IsFIFO),
		"ContentBasedDeduplication":     strconv.FormatBool(meta.ContentBasedDedup),
	}
	if mod := meta.LastModifiedAtMillis; mod > 0 {
		out["LastModifiedTimestamp"] = strconv.FormatInt(mod/sqsMillisPerSecond, 10)
	}
	if meta.RedrivePolicy != "" {
		out["RedrivePolicy"] = meta.RedrivePolicy
	}
	return out
}

// ErrAdminSQSValidation is returned when an admin entrypoint receives
// a request with a missing or syntactically-bad queue name. Maps to
// 400 in the admin HTTP handler.
var ErrAdminSQSValidation = errors.New("sqs admin: invalid queue name")

// ErrAdminSQSNotFound is returned by write entrypoints when the
// target queue does not exist. Maps to 404. The describe path uses
// the (nil, false, nil) tuple instead of this sentinel for the
// not-found signal, mirroring AdminDescribeTable.
var ErrAdminSQSNotFound = errors.New("sqs admin: queue not found")

// isSQSAdminQueueDoesNotExist matches the deleteQueueWithRetry path's
// "queue does not exist" sqsAPIError so AdminDeleteQueue can normalise
// it to ErrAdminSQSNotFound. Falls through to false on any unrelated
// error, which AdminDeleteQueue then wraps and propagates.
func isSQSAdminQueueDoesNotExist(err error) bool {
	var apiErr *sqsAPIError
	if !errors.As(err, &apiErr) || apiErr == nil {
		return false
	}
	return apiErr.errorType == sqsErrQueueDoesNotExist
}
