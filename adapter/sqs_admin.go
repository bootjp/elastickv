package adapter

import (
	"bytes"
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/store"
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
	// IsDLQ is true when at least one other queue's RedrivePolicy
	// resolves its deadLetterTargetArn to this queue. The SPA uses
	// the flag to switch the Messages-tab framing and the Purge
	// button label between "Purge messages" and "Purge DLQ".
	IsDLQ bool
	// DLQSources lists the names of queues whose RedrivePolicy
	// points at this queue, in lexicographic order. Empty when
	// IsDLQ is false. Used by the SPA to render confirmation copy
	// like "This queue is the DLQ for orders, payments". The
	// computation is a paginated reverse scan over the queue-meta
	// prefix at the same read timestamp as the meta load; values
	// reflect the same MVCC snapshot the rest of the summary does.
	DLQSources []string
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

// AdminDescribeQueueOptions opts the Describe call into the more
// expensive lookups. Each option defaults to off so the cheap path
// (the only one Phase 2's bridge actually consumes) stays O(1).
type AdminDescribeQueueOptions struct {
	// IncludeDLQSources turns on the paginated reverse-scan over
	// the queue-meta catalog that populates IsDLQ and DLQSources.
	// Cost is ceil(N/sqsQueueScanPageLimit) round-trips plus a
	// JSON decode per record (same envelope as scanQueueNamesAt),
	// so it is opt-in to keep AdminDescribeQueue calls from
	// callers that don't surface the DLQ relationship (e.g.
	// today's admin SPA, which drops the fields in the bridge)
	// O(1). Phase 4 (the SPA wiring) flips this on; reviewers
	// flagged the unconditional version as dead work otherwise
	// (Codex r1 P2, Gemini r1).
	IncludeDLQSources bool
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
//
// Optional AdminDescribeQueueOptions toggle additional lookups
// (currently DLQ source enumeration). Existing callers that pass no
// options retain the cheap path; Phase 4 wiring opts in when the SPA
// needs the DLQ relationship.
func (s *SQSServer) AdminDescribeQueue(ctx context.Context, name string, opts ...AdminDescribeQueueOptions) (*AdminQueueSummary, bool, error) {
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
	summary := adminQueueSummary(name, meta, counters, s.queueArn(name))
	opt := mergeDescribeQueueOptions(opts)
	if opt.IncludeDLQSources {
		sources, err := s.findQueuesPointingAt(ctx, name, readTS)
		if err != nil {
			return nil, false, errors.WithStack(err)
		}
		if len(sources) > 0 {
			summary.IsDLQ = true
			summary.DLQSources = sources
		}
	}
	return summary, true, nil
}

// mergeDescribeQueueOptions folds the variadic opts into a single
// effective option set. Variadic was chosen over a struct parameter
// so the change is source-compatible with existing callers; a future
// opt that conflicts with another can fail explicitly here. Each
// enable-flag uses "any-true wins" semantics: once any opt sets a
// field to true, no later opt can unset it. This matches the
// intuitive read of opts as a sequence of "things I want enabled"
// rather than an override chain — if any opt asks for the DLQ scan,
// it runs. When a future field needs override semantics (e.g. a
// limit that the caller wants to lower), the merge logic for that
// field should be spelled out explicitly here instead.
func mergeDescribeQueueOptions(opts []AdminDescribeQueueOptions) AdminDescribeQueueOptions {
	var out AdminDescribeQueueOptions
	for _, o := range opts {
		if o.IncludeDLQSources {
			out.IncludeDLQSources = true
		}
	}
	return out
}

// findQueuesPointingAt walks the queue-meta catalog at readTS and
// returns the names of queues whose parsed RedrivePolicy resolves
// deadLetterTargetArn to dlqName. The result is sorted
// lexicographically so callers (and audit logs) see a stable order.
//
// The scan reuses scanQueueNamesAt's loop shape — paginated
// ScanAt(prefix, end, sqsQueueScanPageLimit) calls advanced via
// nextScanCursorAfter — so a deployment with > 1024 queues still
// surfaces every source link. A single-page formulation would
// silently cap at 1024 and mislabel the SPA Purge button on any
// queue whose sources spilled past the first page.
//
// Catalog records that fail to decode are skipped (a malformed meta
// record is a programmer / corruption bug, not a DLQ-detection bug —
// dropping it keeps Describe usable rather than failing the whole
// call). Records without a RedrivePolicy are skipped after the JSON
// decode — every record pays the decode cost, but only RedrivePolicy-
// bearing records pay the parseRedrivePolicy cost on top. A future
// reverse-index would let us skip the decode too; out of scope here.
func (s *SQSServer) findQueuesPointingAt(ctx context.Context, dlqName string, readTS uint64) ([]string, error) {
	prefix := []byte(SqsQueueMetaPrefix)
	end := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)
	var sources []string
	for {
		page, err := s.store.ScanAt(ctx, start, end, sqsQueueScanPageLimit, readTS)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(page) == 0 {
			break
		}
		var stop bool
		sources, stop = collectDLQSources(page, prefix, dlqName, sources)
		if stop {
			// Saw a key outside the queue-meta prefix —
			// the catalog has ended, no further pages can
			// contain queues. Avoid the extra ScanAt that
			// would otherwise be needed to detect the
			// empty-page sentinel.
			break
		}
		if len(page) < sqsQueueScanPageLimit {
			break
		}
		start = nextScanCursorAfter(page[len(page)-1].Key)
		if end != nil && bytes.Compare(start, end) > 0 {
			break
		}
	}
	sort.Strings(sources)
	return sources, nil
}

// collectDLQSources walks one ScanAt page and appends every queue
// whose RedrivePolicy resolves to dlqName. Pulled out of
// findQueuesPointingAt so the outer pagination loop stays under the
// cyclop budget. The bool return signals "stop scanning further
// pages" — set when a key outside the queue-meta prefix is observed
// (the catalog ended; subsequent pages cannot contain queues).
func collectDLQSources(page []*store.KVPair, prefix []byte, dlqName string, sources []string) ([]string, bool) {
	for _, kvp := range page {
		if !bytes.HasPrefix(kvp.Key, prefix) {
			return sources, true
		}
		srcName, ok := queueNameFromMetaKey(kvp.Key)
		if !ok || srcName == dlqName {
			// Skip self: a queue's own RedrivePolicy
			// pointing at itself is rejected at catalog
			// write time (registerRedriveTarget); this is
			// defense-in-depth for that invariant.
			continue
		}
		meta, decErr := decodeSQSQueueMeta(kvp.Value)
		if decErr != nil || meta.RedrivePolicy == "" {
			continue
		}
		policy, parseErr := parseRedrivePolicy(meta.RedrivePolicy)
		if parseErr != nil || policy.DLQName != dlqName {
			continue
		}
		sources = append(sources, srcName)
	}
	return sources, false
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

// AdminPurgeResult is the success return of AdminPurgeQueue. The
// generation values are captured from the committed OCC round, not a
// separate pre/post meta read — a second loadQueueMetaAt call would
// race a concurrent purge resetting LastPurgedAtMillis in the
// 60-second window and could log generation values that never
// appeared as a single consistent state.
type AdminPurgeResult struct {
	GenerationBefore uint64
	GenerationAfter  uint64
}

// ErrAdminSQSPurgeInProgress is the sentinel admin handlers match
// against for the 60-second PurgeQueue rate limit. errors.Is(err,
// ErrAdminSQSPurgeInProgress) returns true for any *PurgeInProgressError
// via the latter's Is method, so handlers can branch on the sentinel
// while extracting the typed RetryAfter duration from the wrapped
// value.
var ErrAdminSQSPurgeInProgress = errors.New("sqs admin: purge in progress")

// PurgeInProgressError is the typed admin error returned by
// AdminPurgeQueue when the meta-stored 60-second rate limit is
// active. RetryAfter carries the wall-clock duration the caller
// should wait, derived from the same LastPurgedAtMillis value the
// rate-limit check itself read inside the OCC transaction.
type PurgeInProgressError struct {
	RetryAfter time.Duration
}

func (e *PurgeInProgressError) Error() string {
	return "sqs admin: purge already in progress; retry after " + e.RetryAfter.String()
}

// Is implements errors.Is so handlers can sniff
// ErrAdminSQSPurgeInProgress while still extracting RetryAfter via
// errors.As. Standard errors-wrapper pattern shared with other typed
// admin errors in the package.
func (e *PurgeInProgressError) Is(target error) bool {
	return target == ErrAdminSQSPurgeInProgress
}

// AdminPurgeQueue is the SigV4-bypass counterpart to purgeQueue.
// Bumps the queue's generation so every message under the old
// generation becomes unreachable, leaving the meta record (name,
// ARN, RedrivePolicy, tags, attributes) in place. The reaper
// eventually deletes the orphaned message keys via the existing
// tombstone path.
//
// Returns the captured generation pair so the admin handler's audit
// line can record the value that actually landed; a separate
// loadQueueMetaAt call would race a concurrent purge in the
// 60-second window. The 60-second rate limit lives on the meta
// record itself so the SigV4 and admin paths interlock uniformly.
//
// Sentinel errors:
//   - ErrAdminForbidden          — read-only principal
//   - ErrAdminNotLeader          — follower
//   - ErrAdminSQSNotFound        — queue absent
//   - *PurgeInProgressError      — last purge < 60 s ago; errors.Is matches
//     ErrAdminSQSPurgeInProgress, RetryAfter carries the duration.
//   - ErrAdminSQSValidation      — empty / whitespace name
func (s *SQSServer) AdminPurgeQueue(ctx context.Context, principal AdminPrincipal, name string) (AdminPurgeResult, error) {
	if !principal.Role.canWrite() {
		return AdminPurgeResult{}, ErrAdminForbidden
	}
	if !isVerifiedSQSLeader(ctx, s.coordinator) {
		return AdminPurgeResult{}, ErrAdminNotLeader
	}
	if strings.TrimSpace(name) == "" {
		return AdminPurgeResult{}, ErrAdminSQSValidation
	}
	oldGen, newGen, err := s.purgeQueueWithRetry(ctx, name)
	if err != nil {
		var rateLimit *purgeRateLimitedError
		if errors.As(err, &rateLimit) {
			return AdminPurgeResult{}, &PurgeInProgressError{RetryAfter: rateLimit.remaining}
		}
		if isSQSAdminQueueDoesNotExist(err) {
			return AdminPurgeResult{}, ErrAdminSQSNotFound
		}
		return AdminPurgeResult{}, errors.Wrap(err, "admin purge queue")
	}
	return AdminPurgeResult{GenerationBefore: oldGen, GenerationAfter: newGen}, nil
}

// AdminDeleteQueue is the SigV4-bypass counterpart to deleteQueue.
// Returns the same sentinel errors as AdminCreateTable on the Dynamo
// side: ErrAdminForbidden on a read-only principal, ErrAdminNotLeader
// on a follower, ErrAdminSQSNotFound when the queue is absent.
func (s *SQSServer) AdminDeleteQueue(ctx context.Context, principal AdminPrincipal, name string) error {
	if !principal.Role.canWrite() {
		return ErrAdminForbidden
	}
	if !isVerifiedSQSLeader(ctx, s.coordinator) {
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
// client-side.
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
