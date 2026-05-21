package adapter

import (
	"bytes"
	"context"
	"encoding/base64"
	"strings"
	"time"

	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

// AdminPeekedAttribute mirrors the typed shape SQS uses for
// MessageAttribute values — DataType (e.g. "String", "Number",
// "Binary", "String.MyCustom") plus the value in the appropriate
// representation. The earlier draft used map[string]string here,
// which would have flattened the typed attribute set stored in
// sqsMessageRecord.MessageAttributes and silently dropped binary
// payloads + the DataType discriminator (Codex r11 on the design
// doc). Operators triaging a DLQ need both — a message routed there
// because of an attribute-encoding mismatch is invisible if peek
// only surfaces stringified values.
type AdminPeekedAttribute struct {
	DataType    string `json:"data_type"`
	StringValue string `json:"string_value,omitempty"`
	// BinaryValue carries the raw bytes; the JSON wire form
	// base64-encodes (standard Go encoding/json behaviour for
	// []byte) so binary payloads survive the SPA round-trip.
	BinaryValue []byte `json:"binary_value,omitempty"`
}

// AdminPeekedMessage is one row in the peek result. JSON tags pin
// the snake_case wire shape the design doc §3.5 specifies; without
// them the encoder would emit Go-style PascalCase field names and
// the SPA's client adapter would silently misparse every row.
// CodeRabbit r4 caught the regression.
type AdminPeekedMessage struct {
	MessageID        string                          `json:"message_id"`
	Body             string                          `json:"body"`                       // truncated per opts.BodyMaxBytes
	BodyTruncated    bool                            `json:"body_truncated"`             // true when Body was cut
	BodyOriginalSize int64                           `json:"body_original_size"`         // bytes in the original body, for display
	SentTimestamp    time.Time                       `json:"sent_timestamp"`             // SQS SentTimestamp
	ReceiveCount     int32                           `json:"receive_count"`              // ApproximateReceiveCount
	GroupID          string                          `json:"group_id,omitempty"`         // FIFO MessageGroupId, empty for standard
	DeduplicationID  string                          `json:"deduplication_id,omitempty"` // FIFO MessageDeduplicationId, empty for standard
	Attributes       map[string]AdminPeekedAttribute `json:"attributes,omitempty"`       // typed SQS message attributes
}

// AdminPeekMessageOptions controls a peek call. Zero values map to
// the documented defaults: Limit=20, Cursor=empty, BodyMaxBytes=4096.
type AdminPeekMessageOptions struct {
	// Limit caps the number of messages returned. Clamped to
	// [1, adminPeekMaxLimit]; 0 means "use default
	// (adminPeekDefaultLimit)".
	Limit int
	// Cursor is an opaque continuation token from a prior call;
	// empty means "start from the front of the visibility index".
	Cursor string
	// BodyMaxBytes truncates message bodies at this length to
	// bound response size. Clamped to
	// [adminPeekMinBodyBytes, sqsMaximumAllowedMaximumMessageSize]
	// (= 256 KiB, matching AWS SQS's hard cap on stored message
	// size). 0 means "use default (adminPeekDefaultBodyBytes)".
	// The full body is always retained on the server; only the
	// wire representation is truncated.
	BodyMaxBytes int
}

const (
	// adminPeekDefaultLimit is the row count an empty
	// AdminPeekMessageOptions.Limit maps to. Matches the SPA's
	// default Messages-tab page size so the cheapest call (default
	// opts) is the one the SPA issues most often.
	adminPeekDefaultLimit = 20
	// adminPeekMaxLimit caps Limit. The hard ceiling exists so an
	// operator script cannot accidentally issue million-row peeks
	// against the leader.
	adminPeekMaxLimit = 100
	// adminPeekDefaultBodyBytes is the truncation length applied
	// when AdminPeekMessageOptions.BodyMaxBytes is zero. 4 KiB
	// keeps a default 20-row response well under typical JSON
	// budgets while still covering the vast majority of message
	// payloads in real DLQ triage workflows.
	adminPeekDefaultBodyBytes = 4096
	// adminPeekMinBodyBytes is the smallest legal BodyMaxBytes
	// (after the zero-means-default mapping). Anything smaller
	// would be a probable client bug: 256 bytes still fits a JSON
	// preview the SPA can render, so we round up.
	adminPeekMinBodyBytes = 256
	// adminPeekCursorMaxBytes hard-caps the encoded cursor size.
	// Anything larger is either client-supplied junk or a sign
	// that a future cursor field grew unbounded; either way the
	// admin handler returns ErrAdminSQSValidation.
	adminPeekCursorMaxBytes = 512
	// peekCursorSchemaV1 pins the cursor wire format. Bumping
	// requires a corresponding decoder branch.
	peekCursorSchemaV1 = 1
)

// peekCursor is the wire shape of the continuation token. JSON-encoded
// then base64url-wrapped so the SPA can pass it back unchanged. The
// version field exists so a future field rename can be handled
// explicitly instead of silently mis-decoding old SPA tabs after a
// rolling deploy.
type peekCursor struct {
	V              int    `json:"v"`            // schema version, currently peekCursorSchemaV1
	Generation     uint64 `json:"gen"`          // queue generation at scan start
	StartPartition uint32 `json:"sp,omitempty"` // partition where this peek walk began (partitioned only)
	Partition      uint32 `json:"p,omitempty"`  // current partition (advances during the walk)
	LastKey        []byte `json:"k,omitempty"`  // last scanned visibility-index key
}

// errPeekCursorTooLarge is the sentinel returned when encodePeekCursor
// would emit a base64 token larger than adminPeekCursorMaxBytes. A
// fresh cursor with a vis-index key tops out around ~150-200 bytes
// encoded, so hitting this is a regression flag rather than an
// expected wire-level failure: surfacing it as an internal error
// keeps the response shape predictable and pinpoints whoever added
// the bloated field.
var errPeekCursorTooLarge = errors.New("admin peek: encoded cursor exceeds maximum size")

// encodePeekCursor JSON-encodes the cursor then base64url-wraps it.
// Returns the empty string when the walk has fully completed
// (caller-supplied sentinel: an empty cursor means "no more pages").
func encodePeekCursor(c *peekCursor) (string, error) {
	if c == nil {
		return "", nil
	}
	raw, err := json.Marshal(c)
	if err != nil {
		return "", errors.WithStack(err)
	}
	out := base64.RawURLEncoding.EncodeToString(raw)
	if len(out) > adminPeekCursorMaxBytes {
		return "", errors.WithStack(errPeekCursorTooLarge)
	}
	return out, nil
}

// decodePeekCursor unwraps the base64url + JSON envelope. Returns
// ErrAdminSQSValidation on any wire-shape error (oversize, malformed
// base64, malformed JSON, unsupported schema version). The empty
// string returns (nil, nil) — callers treat that as "start from the
// front".
func decodePeekCursor(s string) (*peekCursor, error) {
	if s == "" {
		return nil, nil
	}
	if len(s) > adminPeekCursorMaxBytes {
		return nil, errors.Wrap(ErrAdminSQSValidation, "admin peek: cursor too large")
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, errors.Wrap(ErrAdminSQSValidation, "admin peek: cursor is not valid base64url")
	}
	var c peekCursor
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, errors.Wrap(ErrAdminSQSValidation, "admin peek: cursor is not valid JSON")
	}
	if c.V != peekCursorSchemaV1 {
		return nil, errors.Wrapf(ErrAdminSQSValidation, "admin peek: cursor schema version %d unsupported", c.V)
	}
	return &c, nil
}

// clampPeekLimit folds a user-supplied Limit into the legal
// [1, adminPeekMaxLimit] range, mapping 0 to adminPeekDefaultLimit.
func clampPeekLimit(limit int) int {
	if limit <= 0 {
		return adminPeekDefaultLimit
	}
	if limit > adminPeekMaxLimit {
		return adminPeekMaxLimit
	}
	return limit
}

// clampPeekBodyBytes folds a user-supplied BodyMaxBytes into
// [adminPeekMinBodyBytes, sqsMaximumAllowedMaximumMessageSize],
// mapping 0 to adminPeekDefaultBodyBytes.
func clampPeekBodyBytes(b int) int {
	if b <= 0 {
		return adminPeekDefaultBodyBytes
	}
	if b < adminPeekMinBodyBytes {
		return adminPeekMinBodyBytes
	}
	if b > sqsMaximumAllowedMaximumMessageSize {
		return sqsMaximumAllowedMaximumMessageSize
	}
	return b
}

// AdminPeekQueue returns a non-destructive sample of currently-visible
// messages in name. Receive counts are NOT incremented and visibility
// timers are NOT started — peek is a pure read over the leader's
// visibility index. Returns the rows plus a continuation cursor that
// the caller passes back as opts.Cursor to fetch the next page (empty
// when the walk is complete).
//
// Sentinel errors:
//   - ErrAdminForbidden     — peek requires read role; nil principal is denied
//   - ErrAdminNotLeader     — peek runs on the leader (the visibility
//     index is leader-only-written; a follower read would race the
//     leader's apply)
//   - ErrAdminSQSNotFound   — queue absent
//   - ErrAdminSQSValidation — empty / malformed name, malformed /
//     oversized / stale-generation cursor
func (s *SQSServer) AdminPeekQueue(
	ctx context.Context,
	principal AdminPrincipal,
	name string,
	opts AdminPeekMessageOptions,
) ([]AdminPeekedMessage, string, error) {
	if !principal.Role.canRead() {
		return nil, "", ErrAdminForbidden
	}
	if !isVerifiedSQSLeader(ctx, s.coordinator) {
		return nil, "", ErrAdminNotLeader
	}
	if strings.TrimSpace(name) == "" {
		return nil, "", ErrAdminSQSValidation
	}
	limit := clampPeekLimit(opts.Limit)
	bodyMaxBytes := clampPeekBodyBytes(opts.BodyMaxBytes)
	cursor, err := decodePeekCursor(opts.Cursor)
	if err != nil {
		return nil, "", err
	}
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, name, readTS)
	if err != nil {
		return nil, "", errors.WithStack(err)
	}
	if !exists {
		return nil, "", ErrAdminSQSNotFound
	}
	cursor, err = preparePeekCursor(cursor, meta, name, s.peekStartPartition(name, cursor, meta))
	if err != nil {
		return nil, "", err
	}
	rows, nextCursor, err := s.walkPeek(ctx, name, meta, readTS, cursor, limit, bodyMaxBytes)
	if err != nil {
		return nil, "", err
	}
	encoded, err := encodePeekCursor(nextCursor)
	if err != nil {
		return nil, "", err
	}
	return rows, encoded, nil
}

// peekStartPartition returns the starting partition for a fresh peek
// walk, or 0 when a starting partition is not needed (continuation
// pages already carry one in the cursor; non-partitioned queues only
// have partition 0).
//
// The fanout counter is consulted ONLY on the first page of a peek
// walk. nextReceiveFanoutStart increments the per-queue counter on
// every call regardless of whether the returned value is consumed;
// advancing it on every continuation page would perturb the same
// counter ReceiveMessage reads for partition fairness, reintroducing
// fixed-stride aliasing (Codex r2 P1: e.g. PartitionCount=4 with 3
// peek pages between receives lands every receive on the same
// partition, starving the other three).
func (s *SQSServer) peekStartPartition(queueName string, cursor *peekCursor, meta *sqsQueueMeta) uint32 {
	if cursor != nil {
		return 0
	}
	effective := effectivePartitionCount(meta)
	if effective <= 1 {
		return 0
	}
	return s.nextReceiveFanoutStart(queueName, effective)
}

// preparePeekCursor builds the effective cursor for this call. On the
// first page (cursor==nil) it stamps Generation + a rotated
// StartPartition (partitioned queues only — non-partitioned and
// perQueue-throughput FIFO queues collapse to partition 0). On a
// follow-up page it validates the stored Generation matches the
// queue's current generation AND that StartPartition / Partition are
// within [0, effectivePartitionCount(meta)); a mismatch on either
// returns ErrAdminSQSValidation.
//
// effectivePartitionCount (not meta.PartitionCount) is the
// authoritative iteration bound. perQueue FIFO mode collapses every
// MessageGroupId to partition 0 (see partitionFor /
// effectivePartitionCount), so partitions 1..N-1 are guaranteed empty
// for those queues; walking them would be pointless read
// amplification (Codex r3 P2). Keying validation off the effective
// count keeps peek aligned with the data-plane's actual partition
// usage.
//
// Bounds-check rationale: walkPeek terminates the partitioned
// rotation when `(Partition + 1) % effectivePartitionCount ==
// StartPartition`. If a client supplies StartPartition outside
// [0, effectivePartitionCount), that termination condition never
// fires and the call loops ScanAt-by-ScanAt over guaranteed-empty
// partitions forever — a request-amplification DoS against the
// admin endpoint (Codex r1 P1 on PR #794). Rejecting bad cursor
// partition indices up-front closes the vector. Generation mismatch
// separately forces a front-of-stream refresh after a purge.
//
// startPartition is computed by the caller (the SQSServer method's
// peekStartPartition wrapper around nextReceiveFanoutStart) so this
// helper stays method-free for unit-testability. Non-partitioned and
// perQueue-collapsed queues pass 0.
//
// queueName is consumed to validate cursor.LastKey on continuation
// pages: a forged LastKey outside the queue's visibility-index
// prefix would otherwise let an attacker start a ScanAt at any byte
// offset in the leader's keyspace (Codex r4 P2). Continuation
// cursors are admin-supplied and signed only by base64-encoding, so
// every field must be validated against the live queue meta before
// being used as a storage cursor.
func preparePeekCursor(cursor *peekCursor, meta *sqsQueueMeta, queueName string, startPartition uint32) (*peekCursor, error) {
	effective := effectivePartitionCount(meta)
	if cursor == nil {
		out := &peekCursor{
			V:          peekCursorSchemaV1,
			Generation: meta.Generation,
		}
		if effective > 1 {
			out.StartPartition = startPartition
			out.Partition = startPartition
		}
		return out, nil
	}
	if cursor.Generation != meta.Generation {
		return nil, errors.Wrap(ErrAdminSQSValidation,
			"admin peek: cursor is from a prior generation; restart from the front")
	}
	if cursor.StartPartition >= effective || cursor.Partition >= effective {
		return nil, errors.Wrapf(ErrAdminSQSValidation,
			"admin peek: cursor partition index out of range (StartPartition=%d, Partition=%d, max=%d)",
			cursor.StartPartition, cursor.Partition, effective)
	}
	if len(cursor.LastKey) > 0 {
		expectedPrefix := sqsMsgVisPrefixForQueueDispatch(meta, queueName, cursor.Partition, meta.Generation)
		if !bytes.HasPrefix(cursor.LastKey, expectedPrefix) {
			return nil, errors.Wrap(ErrAdminSQSValidation,
				"admin peek: cursor LastKey is outside the queue's visibility-index prefix")
		}
	}
	return cursor, nil
}

// walkPeek pages the visibility index, accumulating up to limit rows
// across partitions for partitioned queues (rotated sequential scan)
// or across the single keyspace for non-partitioned queues. Returns
// the rows and the cursor to pass back on the next call (nil when the
// walk has fully completed).
func (s *SQSServer) walkPeek(
	ctx context.Context,
	queueName string,
	meta *sqsQueueMeta,
	readTS uint64,
	cursor *peekCursor,
	limit int,
	bodyMaxBytes int,
) ([]AdminPeekedMessage, *peekCursor, error) {
	now := time.Now().UnixMilli()
	rows := make([]AdminPeekedMessage, 0, limit)
	for {
		next, exhausted, err := s.walkPeekPartition(ctx, queueName, meta, readTS, now, cursor.Partition, cursor.LastKey, limit-len(rows), bodyMaxBytes, &rows)
		if err != nil {
			return nil, nil, err
		}
		if len(rows) >= limit {
			cursor.LastKey = next
			return rows, cursor, nil
		}
		if !exhausted {
			cursor.LastKey = next
			return rows, cursor, nil
		}
		// Partition exhausted before Limit reached. effectivePartitionCount
		// (not meta.PartitionCount) caps the rotation so perQueue-collapsed
		// FIFO queues stop after partition 0 instead of walking N-1
		// guaranteed-empty partitions (Codex r3 P2 on PR #794).
		effective := effectivePartitionCount(meta)
		if effective <= 1 {
			return rows, nil, nil
		}
		nextPart := (cursor.Partition + 1) % effective
		if nextPart == cursor.StartPartition {
			return rows, nil, nil
		}
		cursor.Partition = nextPart
		cursor.LastKey = nil
	}
}

// walkPeekPartition scans the visibility index for one partition
// (legacy queues invoke this with partition=0 and the legacy
// keyspace). Returns the next cursor key (for resume), whether the
// partition was scanned to completion (`exhausted=true` means no
// further pages), and any storage error. Appends to *rows.
func (s *SQSServer) walkPeekPartition(
	ctx context.Context,
	queueName string,
	meta *sqsQueueMeta,
	readTS uint64,
	nowMillis int64,
	partition uint32,
	lastKey []byte,
	want int,
	bodyMaxBytes int,
	rows *[]AdminPeekedMessage,
) ([]byte, bool, error) {
	if want <= 0 {
		return lastKey, false, nil
	}
	start, end := sqsMsgVisScanBoundsDispatch(meta, queueName, partition, meta.Generation, nowMillis)
	if len(lastKey) > 0 {
		start = nextScanCursorAfter(lastKey)
		if end != nil && bytes.Compare(start, end) > 0 {
			return lastKey, true, nil
		}
	}
	page, err := s.store.ScanAt(ctx, start, end, want, readTS)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	if len(page) == 0 {
		return lastKey, true, nil
	}
	for _, kvp := range page {
		messageID := string(kvp.Value)
		rec, ok, err := s.loadPeekMessageRecord(ctx, meta, queueName, partition, messageID, readTS)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			// The vis-index entry points at a data record that
			// has since been deleted (e.g. between our index
			// scan and the per-record GetAt). Skip rather than
			// fail: a single tombstoned record is not a peek
			// failure — the SPA gets a shorter page, which is
			// a benign outcome.
			continue
		}
		*rows = append(*rows, projectPeekedMessage(rec, bodyMaxBytes))
	}
	lastKey = bytes.Clone(page[len(page)-1].Key)
	exhausted := len(page) < want
	return lastKey, exhausted, nil
}

// loadPeekMessageRecord fetches the message record by ID at the
// caller's MVCC snapshot. Returns (nil, false, nil) when the record
// is absent — the caller treats this as a benign skip (the vis-index
// entry may point at a data record that was deleted between the
// index scan and the per-record GetAt; surfacing the absence as an
// error would fail the whole peek call over a single tombstoned row).
//
// The data-record key is partition-aware via sqsMsgDataKeyDispatch.
// Partitioned FIFO queues store the data record under the same
// partition the vis-index entry was found under; using the legacy
// sqsMsgDataKey here would silently miss every row on partitioned
// queues.
func (s *SQSServer) loadPeekMessageRecord(ctx context.Context, meta *sqsQueueMeta, queueName string, partition uint32, messageID string, readTS uint64) (*sqsMessageRecord, bool, error) {
	dataKey := sqsMsgDataKeyDispatch(meta, queueName, partition, meta.Generation, messageID)
	b, err := s.store.GetAt(ctx, dataKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	if len(b) == 0 {
		return nil, false, nil
	}
	rec, err := decodeSQSMessageRecord(b)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return rec, true, nil
}

// projectPeekedMessage maps the stored record into the wire-side
// AdminPeekedMessage, truncating the body per bodyMaxBytes and
// converting the typed attribute map.
func projectPeekedMessage(rec *sqsMessageRecord, bodyMaxBytes int) AdminPeekedMessage {
	body, truncated, originalSize := truncatePeekBody(rec.Body, bodyMaxBytes)
	return AdminPeekedMessage{
		MessageID:        rec.MessageID,
		Body:             body,
		BodyTruncated:    truncated,
		BodyOriginalSize: originalSize,
		SentTimestamp:    time.UnixMilli(rec.SendTimestampMillis).UTC(),
		ReceiveCount:     clampReceiveCountToInt32(rec.ReceiveCount),
		GroupID:          rec.MessageGroupId,
		DeduplicationID:  rec.MessageDeduplicationId,
		Attributes:       projectPeekedAttributes(rec.MessageAttributes),
	}
}

// clampReceiveCountToInt32 converts the stored int64 ReceiveCount to
// the wire-side int32, saturating at math.MaxInt32 rather than
// overflowing. AWS publishes ApproximateReceiveCount as a uint32 on
// the SQS API, so int32 is sufficient for every realistic queue;
// saturation is the safe fallback if a future bug or corrupted
// record somehow pushes the counter past 2 billion.
func clampReceiveCountToInt32(n int64) int32 {
	const maxInt32 = int32(1<<31 - 1)
	if n < 0 {
		return 0
	}
	if n > int64(maxInt32) {
		return maxInt32
	}
	return int32(n)
}

// truncatePeekBody returns the wire-side body (truncated to
// bodyMaxBytes), the truncated flag, and the original size in bytes.
func truncatePeekBody(body []byte, bodyMaxBytes int) (string, bool, int64) {
	originalSize := int64(len(body))
	if int(originalSize) <= bodyMaxBytes {
		return string(body), false, originalSize
	}
	return string(body[:bodyMaxBytes]), true, originalSize
}

// projectPeekedAttributes maps the stored sqsMessageAttributeValue map
// into the wire-side AdminPeekedAttribute map. Returns nil when the
// stored map is nil so the JSON encoder's omitempty drops the field
// rather than emitting "attributes":{}. The two structs share the
// same field shape, so the conversion is a direct type cast.
func projectPeekedAttributes(attrs map[string]sqsMessageAttributeValue) map[string]AdminPeekedAttribute {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]AdminPeekedAttribute, len(attrs))
	for name, v := range attrs {
		out[name] = AdminPeekedAttribute(v)
	}
	return out
}
