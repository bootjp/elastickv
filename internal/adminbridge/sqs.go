package adminbridge

import (
	"context"
	"time"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/internal/admin"
	"github.com/cockroachdb/errors"
)

// NewSQSQueuesSource adapts *adapter.SQSServer to admin.QueuesSource.
// Same architectural reasoning as NewDynamoTablesSource and
// NewBucketsSource: the bridge lives in this package (rather than
// internal/admin) so the admin package stays free of the heavy
// adapter-package dependency tree.
//
// Production always passes a non-nil *SQSServer now that
// startSQSServer constructs a listenless server when sqsAddress is
// empty (admin endpoints don't need the public SigV4 listener).
// The nil guard below exists only for tests that build an admin
// server without a SQS adapter wired in; an unreachable-in-prod
// nil here would otherwise crash the admin handler chain instead
// of cleanly omitting /admin/api/v1/sqs/* from the wire.
func NewSQSQueuesSource(sqsServer *adapter.SQSServer) admin.QueuesSource {
	if sqsServer == nil {
		return nil
	}
	return &sqsQueuesBridge{server: sqsServer}
}

// sqsQueuesBridge re-shapes adapter.AdminQueueSummary into
// admin.QueueSummary. The two structs are deliberately isomorphic so
// this translation does no allocation more than necessary; if a
// future field is added on one side, the build breaks here, which
// is the drift signal we want.
type sqsQueuesBridge struct {
	server *adapter.SQSServer
}

// AdminQueueObserver satisfies admin.AdminQueueObserverSource so the admin
// handler's pre-dispatch rejections are counted on the same metrics the
// adapter uses. Returns nil when the server has no observer, which
// WithAdminQueueObserver treats as "do not count".
func (b *sqsQueuesBridge) AdminQueueObserver() admin.AdminQueueObserver {
	if b == nil || b.server == nil {
		return nil
	}
	observer := b.server.AdminObserver()
	if observer == nil {
		return nil
	}
	return observer
}

func (b *sqsQueuesBridge) AdminListQueues(ctx context.Context) ([]string, error) {
	return b.server.AdminListQueues(ctx) //nolint:wrapcheck // pure pass-through; adapter owns the error context.
}

func (b *sqsQueuesBridge) AdminDescribeQueue(ctx context.Context, name string) (*admin.QueueSummary, bool, error) {
	// Phase 4 wires IsDLQ / DLQSources: the SPA's Messages tab and
	// Purge button need this flag to switch their framing, so the
	// bridge opts into the catalog reverse-scan that Phase 2 gated
	// behind AdminDescribeQueueOptions{IncludeDLQSources}.
	summary, exists, err := b.server.AdminDescribeQueue(ctx, name, adapter.AdminDescribeQueueOptions{IncludeDLQSources: true})
	if err != nil {
		return nil, false, translateAdminQueuesError(err)
	}
	if !exists {
		return nil, false, nil
	}
	return convertAdminQueueSummary(summary), true, nil
}

func (b *sqsQueuesBridge) AdminCreateQueue(ctx context.Context, principal admin.AuthPrincipal, name string, attrs map[string]string) error {
	if err := b.server.AdminCreateQueue(ctx, convertAdminPrincipal(principal), name, attrs); err != nil {
		if errors.Is(err, adapter.ErrAdminSQSValidation) {
			return errors.Wrap(admin.ErrQueuesValidation, err.Error())
		}
		return translateAdminQueuesError(err)
	}
	return nil
}

func (b *sqsQueuesBridge) AdminDeleteQueue(ctx context.Context, principal admin.AuthPrincipal, name string) error {
	if err := b.server.AdminDeleteQueue(ctx, convertAdminPrincipal(principal), name); err != nil {
		return translateAdminQueuesError(err)
	}
	return nil
}

func (b *sqsQueuesBridge) AdminSetQueueAttributes(ctx context.Context, principal admin.AuthPrincipal, name string, attrs map[string]string) error {
	if err := b.server.AdminSetQueueAttributes(ctx, convertAdminPrincipal(principal), name, attrs); err != nil {
		return translateAdminQueuesError(err)
	}
	return nil
}

func (b *sqsQueuesBridge) AdminPeekQueue(ctx context.Context, principal admin.AuthPrincipal, name string, opts admin.PeekMessageOptions) (admin.PeekResult, error) {
	rows, nextCursor, err := b.server.AdminPeekQueue(ctx, convertAdminPrincipal(principal), name, adapter.AdminPeekMessageOptions{
		Limit:        opts.Limit,
		Cursor:       opts.Cursor,
		BodyMaxBytes: opts.BodyMaxBytes,
	})
	if err != nil {
		return admin.PeekResult{}, translateAdminQueuesError(err)
	}
	return admin.PeekResult{
		Messages:   convertPeekedMessages(rows),
		NextCursor: nextCursor,
	}, nil
}

func (b *sqsQueuesBridge) AdminPurgeQueue(ctx context.Context, principal admin.AuthPrincipal, name string) (admin.PurgeResult, error) {
	res, err := b.server.AdminPurgeQueue(ctx, convertAdminPrincipal(principal), name)
	if err != nil {
		return admin.PurgeResult{}, translateAdminQueuesError(err)
	}
	return admin.PurgeResult{
		GenerationBefore: res.GenerationBefore,
		GenerationAfter:  res.GenerationAfter,
	}, nil
}

// convertPeekedMessages re-shapes the adapter's []AdminPeekedMessage
// into the admin package's []PeekedMessage. The two structs are
// deliberately isomorphic so this translation is a per-field copy;
// if either side grows a field, the build breaks here, which is the
// drift signal we want.
func convertPeekedMessages(in []adapter.AdminPeekedMessage) []admin.PeekedMessage {
	if len(in) == 0 {
		return nil
	}
	out := make([]admin.PeekedMessage, len(in))
	for i, m := range in {
		out[i] = admin.PeekedMessage{
			MessageID:        m.MessageID,
			Body:             m.Body,
			BodyTruncated:    m.BodyTruncated,
			BodyOriginalSize: m.BodyOriginalSize,
			SentTimestamp:    m.SentTimestamp,
			ReceiveCount:     m.ReceiveCount,
			GroupID:          m.GroupID,
			DeduplicationID:  m.DeduplicationID,
			Attributes:       convertPeekedAttributes(m.Attributes),
		}
	}
	return out
}

// convertPeekedAttributes re-shapes the typed attribute map; both
// structs share the same field layout so a direct type conversion
// would suffice if the JSON tags matched, but they differ (adapter
// vs admin packages) so an explicit copy keeps the wire shape stable
// per package.
func convertPeekedAttributes(in map[string]adapter.AdminPeekedAttribute) map[string]admin.PeekedAttribute {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]admin.PeekedAttribute, len(in))
	for k, v := range in {
		out[k] = admin.PeekedAttribute{
			DataType:    v.DataType,
			StringValue: v.StringValue,
			BinaryValue: v.BinaryValue,
		}
	}
	return out
}

// convertAdminQueueSummary mirrors adapter.AdminQueueSummary into
// admin.QueueSummary. Counter fields are int64 on both sides; if
// either side grows a field, this function should be extended in the
// same commit so a compile error catches the drift.
//
// CreatedAt collapses a zero time.Time on the adapter side (queues
// stored before CreatedAtMillis was populated, or never populated)
// to a nil pointer on the admin side so omitempty actually drops the
// field at the wire layer. Without this collapse the SPA would see
// "0001-01-01T00:00:00Z" and render an ancient date.
func convertAdminQueueSummary(in *adapter.AdminQueueSummary) *admin.QueueSummary {
	if in == nil {
		return nil
	}
	var createdAt *time.Time
	if !in.CreatedAt.IsZero() {
		t := in.CreatedAt
		createdAt = &t
	}
	return &admin.QueueSummary{
		Name:       in.Name,
		IsFIFO:     in.IsFIFO,
		Generation: in.Generation,
		CreatedAt:  createdAt,
		Attributes: in.Attributes,
		Counters: admin.QueueCounters{
			Visible:    in.Counters.Visible,
			NotVisible: in.Counters.NotVisible,
			Delayed:    in.Counters.Delayed,
		},
		IsDLQ:      in.IsDLQ,
		DLQSources: in.DLQSources,
	}
}

// translateAdminQueuesError maps the adapter's SQS error vocabulary
// onto the admin-package sentinels the SQS handler matches against.
// Anything not recognised is forwarded as-is and answered with 500
// + a sanitised body, matching the dynamo / s3 bridges' behaviour.
func translateAdminQueuesError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, adapter.ErrAdminForbidden):
		return admin.ErrQueuesForbidden
	case errors.Is(err, adapter.ErrAdminNotLeader):
		return admin.ErrQueuesNotLeader
	case errors.Is(err, adapter.ErrAdminSQSNotFound):
		return admin.ErrQueuesNotFound
	case errors.Is(err, adapter.ErrAdminSQSValidation):
		return admin.ErrQueuesValidation
	case errors.Is(err, adapter.ErrAdminSQSPurgeInProgress):
		// Pull the typed RetryAfter duration from the adapter's
		// *PurgeInProgressError and rewrap as admin's typed error.
		// Both errors.Is(err, ErrQueuesPurgeInProgress) and
		// errors.As(err, &*admin.PurgeInProgressError) match on the
		// admin side, so the handler can branch on the sentinel
		// while still extracting RetryAfter for the Retry-After
		// header.
		var adapterErr *adapter.PurgeInProgressError
		if errors.As(err, &adapterErr) {
			return &admin.PurgeInProgressError{RetryAfter: adapterErr.RetryAfter}
		}
		return &admin.PurgeInProgressError{}
	case isLeaderChurnError(err):
		// Leadership can be lost between AdminDeleteQueue's upfront
		// isVerifiedSQSLeader check and the coordinator dispatch
		// inside deleteQueueWithRetry. The kv coordinator surfaces
		// that as ErrLeaderNotFound / ErrNotLeader (or a wrapped
		// "not leader" / "leader not found" suffix), and the
		// retry loop's isRetryableTransactWriteError does not catch
		// them. Without this arm the error falls to default and the
		// admin handler renders a generic 500. Mirrors the same arm
		// in translateAdminTablesError on the Dynamo side.
		return admin.ErrQueuesNotLeader
	default:
		return err //nolint:wrapcheck // forwarded so the handler logs but does not surface it.
	}
}
