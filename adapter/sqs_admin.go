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
	summary := &AdminQueueSummary{
		Name:       name,
		IsFIFO:     meta.IsFIFO,
		Generation: meta.Generation,
		CreatedAt:  hlcToTime(meta.CreatedAtHLC),
		Attributes: metaAttributesForAdmin(meta),
		Counters:   AdminQueueCounters(counters),
	}
	return summary, true, nil
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

// metaAttributesForAdmin renders the queue meta into the same shape
// queueMetaToAttributes("All") would, minus the counters (the admin
// summary surfaces them as a typed struct alongside, not as strings).
// Kept as a small dedicated helper so the SigV4 path's selection
// machinery stays untouched.
func metaAttributesForAdmin(meta *sqsQueueMeta) map[string]string {
	out := map[string]string{
		"VisibilityTimeout":             strconv.FormatInt(meta.VisibilityTimeoutSeconds, 10),
		"MessageRetentionPeriod":        strconv.FormatInt(meta.MessageRetentionSeconds, 10),
		"DelaySeconds":                  strconv.FormatInt(meta.DelaySeconds, 10),
		"ReceiveMessageWaitTimeSeconds": strconv.FormatInt(meta.ReceiveMessageWaitSeconds, 10),
		"MaximumMessageSize":            strconv.FormatInt(meta.MaximumMessageSize, 10),
		"FifoQueue":                     strconv.FormatBool(meta.IsFIFO),
		"ContentBasedDeduplication":     strconv.FormatBool(meta.ContentBasedDedup),
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
