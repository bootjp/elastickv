package backup

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/errors"
)

// Snapshot key prefixes the SQS encoder dispatches on. Kept in sync with
// adapter/sqs_keys.go and adapter/sqs_messages.go (see SqsQueueMetaPrefix /
// SqsMsgDataPrefix); a renamed prefix in the live code is caught here at
// dispatch time by the corresponding tests that synthesise records with
// these literal byte strings.
const (
	SQSQueueMetaPrefix      = "!sqs|queue|meta|"
	SQSQueueGenPrefix       = "!sqs|queue|gen|"
	SQSQueueSeqPrefix       = "!sqs|queue|seq|"
	SQSQueueTombstonePrefix = "!sqs|queue|tombstone|"
	SQSMsgDataPrefix        = "!sqs|msg|data|"
	SQSMsgVisPrefix         = "!sqs|msg|vis|"
	SQSMsgByAgePrefix       = "!sqs|msg|byage|"
	SQSMsgDedupPrefix       = "!sqs|msg|dedup|"
	SQSMsgGroupPrefix       = "!sqs|msg|group|"

	// HT-FIFO partitioned-keyspace discriminator. Kept in sync with
	// adapter/sqs_keys.go sqsPartitionedDiscriminator. The literal
	// "p|" segment is inserted between the family and the queue
	// segment in every partitioned key:
	//
	//	legacy:      !sqs|msg|<family>|<encQueue><gen><rest>
	//	partitioned: !sqs|msg|<family>|p|<encQueue>|<partition u32><gen u64><rest>
	//
	// validateQueueName rejects raw '|' in queue names, so a legacy
	// queue name can never start with the literal byte 'p' followed
	// by '|'; the discriminator unambiguously selects the parser
	// variant. Codex P1 round 9.
	sqsPartitionedDiscriminator = "p|"
	// partitionBytes is the fixed BE-uint32 partition field width.
	sqsPartitionBytes = 4
)

// Stored value magic prefixes (mirrors adapter/sqs_catalog.go and
// adapter/sqs_messages.go). Values that don't carry the right magic are
// rejected — they are either from a future schema version or genuinely
// corrupt, both of which warrant aborting rather than silently emitting
// garbage.
var (
	storedSQSMetaMagic = []byte{0x00, 'S', 'Q', 0x01}
	storedSQSMsgMagic  = []byte{0x00, 'S', 'M', 0x01}
)

// genBytes is the fixed width of the BE uint64 generation field in
// !sqs|msg|data|<queue><gen><msgID> keys.
const genBytes = 8

// ErrSQSInvalidQueueMeta is returned for !sqs|queue|meta values that miss
// the magic prefix or fail JSON decoding.
var ErrSQSInvalidQueueMeta = errors.New("backup: invalid !sqs|queue|meta value")

// ErrSQSInvalidMessage is returned for !sqs|msg|data values that miss the
// magic prefix or fail JSON decoding.
var ErrSQSInvalidMessage = errors.New("backup: invalid !sqs|msg|data value")

// ErrSQSMalformedKey is returned when an SQS key cannot be parsed for the
// queue-name segment (e.g., the heuristic boundary detection found no
// transition byte).
var ErrSQSMalformedKey = errors.New("backup: malformed SQS key")

// SQSEncoder encodes the SQS prefix family into the per-queue layout
// described in docs/design/2026_04_29_proposed_snapshot_logical_decoder.md
// (Phase 0): one `_queue.json` per queue and one ordered `messages.jsonl`.
//
// Lifecycle: per-snapshot pass calls Handle* for each record, then exactly
// one Finalize. Side-records (vis/byage/dedup/group/tombstone) are
// excluded by default; opt in with WithIncludeSideRecords. Visibility
// state on emitted messages is zeroed by default; opt in to preserve with
// WithPreserveVisibility.
//
// The encoder buffers messages per queue in memory and sorts them at
// Finalize-time by (SendTimestampMillis, SequenceNumber, MessageID). This
// is acceptable for typical operational queues; queues with hundreds of
// millions of messages will need a future stream-and-merge variant.
type SQSEncoder struct {
	outRoot            string
	includeSideRecords bool
	preserveVisibility bool

	// queues is keyed by the base64url-encoded queue name (the on-disk
	// segment in the !sqs|queue|meta|<seg> key). Pending messages are
	// keyed the same way so meta records arriving later (lex 'q' > 'm')
	// can resolve them.
	queues map[string]*sqsQueueState

	warn func(event string, fields ...any)
}

type sqsQueueState struct {
	encoded  string // base64url segment from the meta key
	name     string // decoded queue name; populated on meta arrival
	meta     *sqsQueueMetaPublic
	messages []sqsMessageRecord
	// internalBuf accumulates side records in their on-disk shape if
	// includeSideRecords is on. Each line is the encoded prefix +
	// hex(rest-of-key) + value (b64) — implementation-grade detail
	// landing in a follow-up PR; for now this PR keeps it as a bag.
	internalBuf []sqsInternalRecord
}

type sqsInternalRecord struct {
	Prefix   string `json:"prefix"`
	KeyHex   string `json:"key_hex"`
	ValueB64 string `json:"value_b64"`
}

// sqsQueueMetaPublic is the dump-format projection of the live
// adapter/sqs_catalog.go sqsQueueMeta. Field names match the AWS-style
// vocabulary an external restore tool would use.
type sqsQueueMetaPublic struct {
	FormatVersion             uint32 `json:"format_version"`
	Name                      string `json:"name"`
	FIFO                      bool   `json:"fifo,omitempty"`
	ContentBasedDeduplication bool   `json:"content_based_deduplication,omitempty"`
	VisibilityTimeoutSeconds  int64  `json:"visibility_timeout_seconds"`
	MessageRetentionSeconds   int64  `json:"message_retention_seconds"`
	DelaySeconds              int64  `json:"delay_seconds"`
	ReceiveMessageWaitSeconds int64  `json:"receive_message_wait_seconds,omitempty"`
	MaximumMessageSize        int64  `json:"maximum_message_size,omitempty"`
	RedrivePolicy             string `json:"redrive_policy,omitempty"`
}

// sqsMessageBody is the dump-format projection's body field. It marshals
// as a JSON string when the bytes are valid UTF-8 (the AWS SQS contract
// — body is XML-text), so restorers can pipe each `body` straight into
// SendMessage. For non-UTF-8 bytes the encoder falls back to a
// `{"base64":"..."}` envelope so binary payloads still round-trip
// without lossy replacement-character rewrites. Codex P1 round 9.
type sqsMessageBody []byte

// MarshalJSON implements json.Marshaler.
func (b sqsMessageBody) MarshalJSON() ([]byte, error) {
	if utf8.Valid(b) {
		// Emit as a plain JSON string. json.Marshal handles
		// escaping (`"`, `\`, control chars) — the bytes that
		// reach this path are valid UTF-8, so no information is
		// lost.
		out, err := json.Marshal(string(b))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return out, nil
	}
	envelope := struct {
		Base64 string `json:"base64"`
	}{Base64: base64.RawURLEncoding.EncodeToString(b)}
	out, err := json.Marshal(envelope)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return out, nil
}

// sqsMessageRecord is the dump-format projection. Mirrors the live
// adapter/sqs_messages.go:80 record one-to-one — JSON tag names match so
// a restorer can call SendMessage with each line as the input. Visibility
// state is included in the schema so --preserve-visibility consumers can
// round-trip; the encoder zeroes the visibility-state fields by default.
type sqsMessageRecord struct {
	MessageID              string                     `json:"message_id"`
	Body                   sqsMessageBody             `json:"body"`
	MD5OfBody              string                     `json:"md5_of_body,omitempty"`
	MD5OfMessageAttributes string                     `json:"md5_of_message_attributes,omitempty"`
	MessageAttributes      map[string]json.RawMessage `json:"message_attributes,omitempty"`
	SenderID               string                     `json:"sender_id,omitempty"`
	SendTimestampMillis    int64                      `json:"send_timestamp_millis"`
	AvailableAtMillis      int64                      `json:"available_at_millis"`
	VisibleAtMillis        int64                      `json:"visible_at_millis"`
	ReceiveCount           int64                      `json:"receive_count"`
	FirstReceiveMillis     int64                      `json:"first_receive_millis,omitempty"`
	CurrentReceiptToken    []byte                     `json:"current_receipt_token,omitempty"`
	QueueGeneration        uint64                     `json:"queue_generation"`
	MessageGroupID         string                     `json:"message_group_id,omitempty"`
	MessageDedupID         string                     `json:"message_deduplication_id,omitempty"`
	SequenceNumber         uint64                     `json:"sequence_number,omitempty"`
	DeadLetterSourceArn    string                     `json:"dead_letter_source_arn,omitempty"`
}

// NewSQSEncoder constructs an encoder rooted at <outRoot>/sqs/.
func NewSQSEncoder(outRoot string) *SQSEncoder {
	return &SQSEncoder{
		outRoot: outRoot,
		queues:  make(map[string]*sqsQueueState),
	}
}

// WithIncludeSideRecords routes vis/byage/dedup/group/tombstone records
// into _internals/. Default is to exclude them — they are derivable from
// the queue config + message records and replaying them on restore can
// resurrect aborted state.
func (s *SQSEncoder) WithIncludeSideRecords(on bool) *SQSEncoder {
	s.includeSideRecords = on
	return s
}

// WithPreserveVisibility passes the visibility-state fields
// (visible_at_millis, current_receipt_token, receive_count,
// first_receive_millis) through to the dump. Default is to zero them so
// the restored queue starts with every message visible.
func (s *SQSEncoder) WithPreserveVisibility(on bool) *SQSEncoder {
	s.preserveVisibility = on
	return s
}

// WithWarnSink wires a structured warning hook (same shape as
// RedisDB.WithWarnSink). Used for orphan messages and unresolvable side
// records.
func (s *SQSEncoder) WithWarnSink(fn func(event string, fields ...any)) *SQSEncoder {
	s.warn = fn
	return s
}

// HandleQueueMeta processes one !sqs|queue|meta|<encoded> record. Strips
// the magic prefix, decodes the JSON, projects to the dump-format
// sqsQueueMetaPublic, and parks it on the per-queue state.
func (s *SQSEncoder) HandleQueueMeta(key, value []byte) error {
	encoded, err := stripPrefixSegment(key, []byte(SQSQueueMetaPrefix))
	if err != nil {
		return err
	}
	name, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil {
		return errors.Wrap(ErrSQSMalformedKey, err.Error())
	}
	meta, err := decodeSQSQueueMetaValue(value)
	if err != nil {
		return err
	}
	st := s.queueState(encoded)
	st.name = string(name)
	st.meta = meta
	// The live record carries Name internally; surface it explicitly so
	// the dump's _queue.json is self-describing.
	if meta.Name == "" {
		meta.Name = st.name
	}
	return nil
}

// HandleMessageData processes one !sqs|msg|data|<encQueue><gen><encMsgID>
// record. The encoded queue segment is parsed out of the key and used as
// the per-queue routing key; the message is buffered until Finalize so it
// can be sorted and emitted in send-order.
func (s *SQSEncoder) HandleMessageData(key, value []byte) error {
	encQueue, err := parseSQSMessageDataKey(key)
	if err != nil {
		return err
	}
	rec, err := decodeSQSMessageValue(value)
	if err != nil {
		return err
	}
	if !s.preserveVisibility {
		rec.VisibleAtMillis = 0
		rec.CurrentReceiptToken = nil
		rec.ReceiveCount = 0
		rec.FirstReceiveMillis = 0
	}
	st := s.queueState(encQueue)
	st.messages = append(st.messages, rec)
	return nil
}

// HandleSideRecord buffers (vis|byage|dedup|group|tombstone) records when
// includeSideRecords is on; otherwise drops them silently (this is the
// documented Phase 0 default).
func (s *SQSEncoder) HandleSideRecord(prefix string, key, value []byte) error {
	if !s.includeSideRecords {
		return nil
	}
	encQueue, err := parseSQSGenericKey(key, prefix)
	if err != nil {
		// Tombstones include a fixed-width gen but no msg ID; the
		// generic parser tolerates the empty trailer.
		return err
	}
	st := s.queueState(encQueue)
	st.internalBuf = append(st.internalBuf, sqsInternalRecord{
		Prefix:   prefix,
		KeyHex:   fmt.Sprintf("%x", key),
		ValueB64: base64.RawURLEncoding.EncodeToString(value),
	})
	return nil
}

// Finalize flushes every queue's _queue.json and messages.jsonl. Queues
// with buffered messages but no meta record (orphans) emit a warning
// and have their messages dropped — restoring orphan messages without
// a queue config would silently create a queue with default settings,
// which is rarely what the operator wants. However, if
// --include-sqs-side-records is on and this orphan queue has buffered
// side records (vis/byage/dedup/group/tombstone), those are still
// flushed under the encoded-prefix directory: the most common reason
// for a missing meta is a DeleteQueue that left tombstones, and
// dropping exactly those records is the opposite of what the operator
// asked for. Codex P2 round 8.
func (s *SQSEncoder) Finalize() error {
	var firstErr error
	for _, st := range s.queues {
		if st.meta == nil {
			if err := s.flushOrphanQueueSideRecords(st); err != nil && firstErr == nil {
				firstErr = err
			}
			continue
		}
		if err := s.flushQueue(st); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// flushOrphanQueueSideRecords emits buffered side records for a queue
// whose !sqs|queue|meta row never arrived. Without this branch,
// --include-sqs-side-records silently drops the post-DeleteQueue
// tombstones and dedup-window history operators most often opt in
// for. The orphan dir is named by the encoded prefix because no
// decoded queue name is available; restore tools can join it with
// the messages-dropped warning to reconstruct context.
func (s *SQSEncoder) flushOrphanQueueSideRecords(st *sqsQueueState) error {
	s.emitWarn("sqs_orphan_messages",
		"encoded_queue", st.encoded,
		"buffered_messages", len(st.messages),
		"buffered_side_records", len(st.internalBuf),
		"hint", "no !sqs|queue|meta record matched this encoded prefix; messages dropped from the dump")
	if !s.includeSideRecords || len(st.internalBuf) == 0 {
		return nil
	}
	// Use the encoded prefix as the directory name — it's the only
	// stable identifier available when meta is missing. Suffix it
	// with `.orphan` so a restore tool cannot mistake it for a real
	// queue dir produced from a successful meta flush.
	dir := filepath.Join(s.outRoot, "sqs", st.encoded+".orphan")
	return s.flushInternals(dir, st.internalBuf)
}

func (s *SQSEncoder) flushQueue(st *sqsQueueState) error {
	dir := filepath.Join(s.outRoot, "sqs", EncodeSegment([]byte(st.name)))
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	if err := writeFileAtomic(filepath.Join(dir, "_queue.json"), mustMarshalIndent(st.meta)); err != nil {
		return err
	}
	if len(st.messages) > 0 {
		sortMessagesForEmit(st.messages)
		jl, err := openJSONL(filepath.Join(dir, "messages.jsonl"))
		if err != nil {
			return err
		}
		for i := range st.messages {
			if err := jl.enc.Encode(st.messages[i]); err != nil {
				_ = closeJSONL(jl)
				return errors.WithStack(err)
			}
		}
		if err := closeJSONL(jl); err != nil {
			return err
		}
	}
	// Side records ("--include-sqs-side-records") flush regardless of
	// whether the queue has any current messages. A purged or
	// metadata-only queue can legitimately have side records (e.g.,
	// dedup window history, vis/byage entries from in-flight reaper
	// state) and dropping them when messages == 0 silently weakens
	// the --include-sqs-side-records contract — flagged as Codex P2.
	if len(st.internalBuf) > 0 {
		if err := s.flushInternals(dir, st.internalBuf); err != nil {
			return err
		}
	}
	return nil
}

func (s *SQSEncoder) flushInternals(queueDir string, recs []sqsInternalRecord) error {
	dir := filepath.Join(queueDir, "_internals")
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:mnd // 0755 == standard dir mode
		return errors.WithStack(err)
	}
	jl, err := openJSONL(filepath.Join(dir, "side_records.jsonl"))
	if err != nil {
		return err
	}
	for i := range recs {
		if err := jl.enc.Encode(recs[i]); err != nil {
			_ = closeJSONL(jl)
			return errors.WithStack(err)
		}
	}
	return closeJSONL(jl)
}

func (s *SQSEncoder) emitWarn(event string, fields ...any) {
	if s.warn == nil {
		return
	}
	s.warn(event, fields...)
}

func (s *SQSEncoder) queueState(encoded string) *sqsQueueState {
	if st, ok := s.queues[encoded]; ok {
		return st
	}
	st := &sqsQueueState{encoded: encoded}
	s.queues[encoded] = st
	return st
}

// stripPrefixSegment returns the trailing string after a literal prefix.
// It does NOT decode the segment — the caller decides whether base64url
// or raw bytes are appropriate for the prefix family.
func stripPrefixSegment(key, prefix []byte) (string, error) {
	if !bytes.HasPrefix(key, prefix) {
		return "", errors.Wrapf(ErrSQSMalformedKey, "key does not start with %q", prefix)
	}
	return string(key[len(prefix):]), nil
}

// parseSQSMessageDataKey peels !sqs|msg|data|<encQueue><gen 8B><encMsgID>
// (or its partitioned variant !sqs|msg|data|p|<encQueue>|<part 4B><gen 8B><encMsgID>)
// and returns encQueue. The gen, partition, and msgID are not surfaced
// because the dump format pulls those fields out of the value record.
//
// Boundary detection (legacy): encQueue is base64url-no-padding, alphabet
// [A-Za-z0-9-_]. The gen is 8 raw bytes. For any production gen value
// (< 2^56), the first byte is 0x00, which is not in the base64url
// alphabet, so the first non-alphabet byte is the gen-start. We document
// this assumption rather than build a more elaborate prober — gens
// approaching 2^56 would have already wrapped many other invariants.
//
// Boundary detection (partitioned): the queue segment is terminated by
// a literal '|' before the fixed-width partition u32. Codex P1 round 9.
func parseSQSMessageDataKey(key []byte) (string, error) {
	rest, err := stripPrefixSegment(key, []byte(SQSMsgDataPrefix))
	if err != nil {
		return "", err
	}
	if isPartitionedRest(rest) {
		return parseSQSPartitionedQueueAndTrailer(rest, true /*hasMsgID*/, key)
	}
	idx := scanBase64URLBoundary(rest)
	// idx == 0 -> no queue segment; idx+genBytes >= len(rest) -> no
	// room for any msg-id segment after the gen. Both are malformed.
	// AWS SQS message IDs are non-empty by construction, so an empty
	// msg-id segment can never be a legitimate snapshot record.
	if idx == 0 || idx+genBytes >= len(rest) {
		return "", errors.Wrapf(ErrSQSMalformedKey,
			"queue segment or message-id segment not found in %q", key)
	}
	encQueue := rest[:idx]
	if _, err := base64.RawURLEncoding.DecodeString(encQueue); err != nil {
		return "", errors.Wrap(ErrSQSMalformedKey, err.Error())
	}
	// Validate the msg-id segment decodes too; if it doesn't, the
	// boundary detection got it wrong and we surface an error rather
	// than emit a record under a wrong queue.
	encMsgID := rest[idx+genBytes:]
	if _, err := base64.RawURLEncoding.DecodeString(encMsgID); err != nil {
		return "", errors.Wrap(ErrSQSMalformedKey, err.Error())
	}
	return encQueue, nil
}

// parseSQSGenericKey is a coarse parser for the side-record prefixes
// (vis/byage/dedup/group/tombstone). Callers in this PR only need to
// know the encoded queue segment for routing; full structural parsing
// of side-record keys is deferred until Phase 0a's reaper-aware mode
// lands. Both the legacy and partitioned (`p|<queue>|...`) shapes are
// recognised — Codex P2 round 9.
func parseSQSGenericKey(key []byte, prefix string) (string, error) {
	rest, err := stripPrefixSegment(key, []byte(prefix))
	if err != nil {
		return "", err
	}
	if isPartitionedRest(rest) {
		return parseSQSPartitionedQueueAndTrailer(rest, false /*hasMsgID*/, key)
	}
	idx := scanBase64URLBoundary(rest)
	// All side-record key shapes (vis / byage / dedup / group /
	// tombstone) terminate the encoded queue segment with at least
	// one binary trailer (the gen u64), so idx must be strictly less
	// than len(rest). idx == len(rest) means the trailer is missing —
	// either a truncated key or the wrong prefix.
	if idx == 0 || idx == len(rest) {
		return "", errors.Wrapf(ErrSQSMalformedKey,
			"queue segment not found after prefix %q", prefix)
	}
	return rest[:idx], nil
}

// isPartitionedRest reports whether `rest` (the suffix after a
// !sqs|msg|<family>| prefix has been stripped) starts with the
// HT-FIFO partitioned discriminator "p|".
func isPartitionedRest(rest string) bool {
	return strings.HasPrefix(rest, sqsPartitionedDiscriminator)
}

// parseSQSPartitionedQueueAndTrailer parses the partitioned suffix
// `p|<encQueue>|<partition 4B><gen 8B>[<encMsgID>]`. Returns the
// encoded queue segment when the structural invariants hold:
//
//   - the discriminator is followed by a non-empty queue segment
//   - the queue segment is terminated by a literal '|'
//   - the trailer carries at least partition u32 + gen u64 bytes
//   - if hasMsgID == true, an additional non-empty base64url
//     msg-id segment follows the trailer.
//
// Anything else surfaces ErrSQSMalformedKey rather than emitting
// records under a wrong queue.
func parseSQSPartitionedQueueAndTrailer(rest string, hasMsgID bool, originalKey []byte) (string, error) {
	body := rest[len(sqsPartitionedDiscriminator):]
	terminator := strings.IndexByte(body, '|')
	if terminator <= 0 {
		return "", errors.Wrapf(ErrSQSMalformedKey,
			"partitioned key missing queue terminator in %q", originalKey)
	}
	encQueue := body[:terminator]
	if _, err := base64.RawURLEncoding.DecodeString(encQueue); err != nil {
		return "", errors.Wrap(ErrSQSMalformedKey, err.Error())
	}
	trailer := body[terminator+1:]
	const fixedTrailerBytes = sqsPartitionBytes + genBytes
	if hasMsgID {
		// Need partition+gen plus at least 1 byte of msg-id.
		if len(trailer) <= fixedTrailerBytes {
			return "", errors.Wrapf(ErrSQSMalformedKey,
				"partitioned msg-data key missing message-id in %q", originalKey)
		}
		encMsgID := trailer[fixedTrailerBytes:]
		if _, err := base64.RawURLEncoding.DecodeString(encMsgID); err != nil {
			return "", errors.Wrap(ErrSQSMalformedKey, err.Error())
		}
		return encQueue, nil
	}
	// Side records: trailer must carry at least partition+gen.
	if len(trailer) < fixedTrailerBytes {
		return "", errors.Wrapf(ErrSQSMalformedKey,
			"partitioned side-record key trailer truncated in %q", originalKey)
	}
	return encQueue, nil
}

// scanBase64URLBoundary returns the index of the first byte in s that is
// NOT in the base64url alphabet [A-Za-z0-9-_]. Returns len(s) if every
// byte is alphabet.
func scanBase64URLBoundary(s string) int {
	for i := 0; i < len(s); i++ {
		if !isBase64URLByte(s[i]) {
			return i
		}
	}
	return len(s)
}

func isBase64URLByte(c byte) bool {
	switch {
	case c >= 'A' && c <= 'Z':
		return true
	case c >= 'a' && c <= 'z':
		return true
	case c >= '0' && c <= '9':
		return true
	case c == '-', c == '_':
		return true
	}
	return false
}

// decodeSQSQueueMetaValue strips the SQ magic prefix, JSON-decodes the
// live sqsQueueMeta, and projects to the dump-format
// sqsQueueMetaPublic. Unknown fields in the live record are tolerated
// (forward-compat with new live-side fields the dump format hasn't
// learned yet).
func decodeSQSQueueMetaValue(value []byte) (*sqsQueueMetaPublic, error) {
	if !bytes.HasPrefix(value, storedSQSMetaMagic) {
		return nil, errors.Wrap(ErrSQSInvalidQueueMeta, "missing magic prefix")
	}
	body := value[len(storedSQSMetaMagic):]
	var live struct {
		Name                      string `json:"name"`
		IsFIFO                    bool   `json:"is_fifo"`
		ContentBasedDedup         bool   `json:"content_based_dedup"`
		VisibilityTimeoutSeconds  int64  `json:"visibility_timeout_seconds"`
		MessageRetentionSeconds   int64  `json:"message_retention_seconds"`
		DelaySeconds              int64  `json:"delay_seconds"`
		ReceiveMessageWaitSeconds int64  `json:"receive_message_wait_seconds"`
		MaximumMessageSize        int64  `json:"maximum_message_size"`
		RedrivePolicy             string `json:"redrive_policy"`
	}
	if err := json.Unmarshal(body, &live); err != nil {
		return nil, errors.Wrap(ErrSQSInvalidQueueMeta, err.Error())
	}
	return &sqsQueueMetaPublic{
		FormatVersion:             1,
		Name:                      live.Name,
		FIFO:                      live.IsFIFO,
		ContentBasedDeduplication: live.ContentBasedDedup,
		VisibilityTimeoutSeconds:  live.VisibilityTimeoutSeconds,
		MessageRetentionSeconds:   live.MessageRetentionSeconds,
		DelaySeconds:              live.DelaySeconds,
		ReceiveMessageWaitSeconds: live.ReceiveMessageWaitSeconds,
		MaximumMessageSize:        live.MaximumMessageSize,
		RedrivePolicy:             live.RedrivePolicy,
	}, nil
}

// decodeSQSMessageValue strips the SM magic prefix and JSON-decodes the
// live sqsMessageRecord into the dump-format projection. Unlike the
// queue-meta path, every documented field is preserved (the dump format
// is the public projection, so there is nothing to filter).
func decodeSQSMessageValue(value []byte) (sqsMessageRecord, error) {
	if !bytes.HasPrefix(value, storedSQSMsgMagic) {
		return sqsMessageRecord{}, errors.Wrap(ErrSQSInvalidMessage, "missing magic prefix")
	}
	body := value[len(storedSQSMsgMagic):]
	// The live record uses different JSON tag names for the fields we
	// expose under AWS-style names (message_group_id, sequence_number,
	// etc.). Unmarshal into a shape that mirrors the live tags, then
	// translate.
	var live struct {
		MessageID              string                     `json:"message_id"`
		Body                   []byte                     `json:"body"`
		MD5OfBody              string                     `json:"md5_of_body"`
		MD5OfMessageAttributes string                     `json:"md5_of_message_attributes"`
		MessageAttributes      map[string]json.RawMessage `json:"message_attributes"`
		SenderID               string                     `json:"sender_id"`
		SendTimestampMillis    int64                      `json:"send_timestamp_millis"`
		AvailableAtMillis      int64                      `json:"available_at_millis"`
		VisibleAtMillis        int64                      `json:"visible_at_millis"`
		ReceiveCount           int64                      `json:"receive_count"`
		FirstReceiveMillis     int64                      `json:"first_receive_millis"`
		CurrentReceiptToken    []byte                     `json:"current_receipt_token"`
		QueueGeneration        uint64                     `json:"queue_generation"`
		MessageGroupID         string                     `json:"message_group_id"`
		MessageDedupID         string                     `json:"message_deduplication_id"`
		SequenceNumber         uint64                     `json:"sequence_number"`
		DeadLetterSourceArn    string                     `json:"dead_letter_source_arn"`
	}
	if err := json.Unmarshal(body, &live); err != nil {
		return sqsMessageRecord{}, errors.Wrap(ErrSQSInvalidMessage, err.Error())
	}
	return sqsMessageRecord{
		MessageID:              live.MessageID,
		Body:                   live.Body,
		MD5OfBody:              live.MD5OfBody,
		MD5OfMessageAttributes: live.MD5OfMessageAttributes,
		MessageAttributes:      live.MessageAttributes,
		SenderID:               live.SenderID,
		SendTimestampMillis:    live.SendTimestampMillis,
		AvailableAtMillis:      live.AvailableAtMillis,
		VisibleAtMillis:        live.VisibleAtMillis,
		ReceiveCount:           live.ReceiveCount,
		FirstReceiveMillis:     live.FirstReceiveMillis,
		CurrentReceiptToken:    live.CurrentReceiptToken,
		QueueGeneration:        live.QueueGeneration,
		MessageGroupID:         live.MessageGroupID,
		MessageDedupID:         live.MessageDedupID,
		SequenceNumber:         live.SequenceNumber,
		DeadLetterSourceArn:    live.DeadLetterSourceArn,
	}, nil
}

func sortMessagesForEmit(msgs []sqsMessageRecord) {
	sort.SliceStable(msgs, func(i, j int) bool {
		a, b := msgs[i], msgs[j]
		switch {
		case a.SendTimestampMillis != b.SendTimestampMillis:
			return a.SendTimestampMillis < b.SendTimestampMillis
		case a.SequenceNumber != b.SequenceNumber:
			return a.SequenceNumber < b.SequenceNumber
		default:
			return a.MessageID < b.MessageID
		}
	})
}

func mustMarshalIndent(v any) []byte {
	out, err := json.MarshalIndent(v, "", "  ") //nolint:mnd // 2-space indent matches MANIFEST
	if err != nil {
		// MarshalIndent only fails on unsupported types; sqsQueueMetaPublic
		// is a plain struct of primitives. A panic here is a programmer
		// error rather than a runtime condition we should plan to handle.
		panic(err)
	}
	return out
}

// keyComponents is a debugging helper exposed for tests; not used by
// production code paths.
type keyComponents struct {
	Prefix  string
	Encoded string
	GenRaw  []byte
	MsgID   string
}

// peekMsgDataKey is a test/debug helper that returns the structural
// components of a !sqs|msg|data key. Production code uses
// parseSQSMessageDataKey directly because it never needs gen or msgID.
func peekMsgDataKey(key []byte) (keyComponents, error) {
	rest, err := stripPrefixSegment(key, []byte(SQSMsgDataPrefix))
	if err != nil {
		return keyComponents{}, err
	}
	idx := scanBase64URLBoundary(rest)
	if idx == 0 || idx+genBytes > len(rest) {
		return keyComponents{}, errors.Wrap(ErrSQSMalformedKey, "boundary not found")
	}
	return keyComponents{
		Prefix:  SQSMsgDataPrefix,
		Encoded: rest[:idx],
		GenRaw:  []byte(rest[idx : idx+genBytes]),
		MsgID:   rest[idx+genBytes:],
	}, nil
}

// EncodeMsgDataKey constructs a !sqs|msg|data key for tests. Mirrors the
// live sqsMsgDataKey constructor in adapter/sqs_messages.go.
func EncodeMsgDataKey(queueName string, gen uint64, messageID string) []byte {
	out := make([]byte, 0, len(SQSMsgDataPrefix)+64) //nolint:mnd // 64 == sqsKeyCapLarge
	out = append(out, SQSMsgDataPrefix...)
	out = append(out, base64.RawURLEncoding.EncodeToString([]byte(queueName))...)
	var b [genBytes]byte
	binary.BigEndian.PutUint64(b[:], gen)
	out = append(out, b[:]...)
	out = append(out, base64.RawURLEncoding.EncodeToString([]byte(messageID))...)
	return out
}

// EncodeQueueMetaKey constructs a !sqs|queue|meta key for tests.
func EncodeQueueMetaKey(queueName string) []byte {
	return []byte(SQSQueueMetaPrefix + base64.RawURLEncoding.EncodeToString([]byte(queueName)))
}
