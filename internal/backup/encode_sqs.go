package backup

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/errors"
)

// encode_sqs.go is the Phase 0b SQS reverse encoder — the inverse of the
// SQS decoder in sqs.go (type SQSEncoder, which turns internal !sqs|*
// records into an sqs/<queue>/ dump tree). Design:
// docs/design/2026_05_25_proposed_snapshot_logical_encoder.md §"SQS".
//
// This slice (M5-1) covers the QUEUE config and the MESSAGE data records,
// plus the generation and (FIFO) sequence counters:
//   - _queue.json        -> !sqs|queue|meta|  (storedSQSMetaMagic + JSON) + !sqs|queue|gen|
//   - messages.jsonl     -> !sqs|msg|data|    (storedSQSMsgMagic + JSON), one per line
//   - (FIFO) seq counter -> !sqs|queue|seq|   = max(sequence_number)+1
//
// The derived side records (!sqs|msg|{vis,byage,dedup,group}) — re-derivable
// from the message set + config — land in a follow-up slice (M5-2, the
// design's side-record decision gate). Visibility state restores zeroed
// (every message visible), matching the decoder's default.
//
// Generation: a uniform sqsRestoreGeneration is stamped into the queue
// meta's Generation field, the !sqs|queue|gen| counter, and every message
// key + each message's queue_generation, matching the Option-B decision
// used by the DynamoDB/S3 encoders. The original generation/incarnation,
// created-at timestamps, tags, and throttle config are not represented in
// the dump and restore to their zero values.
//
// Partitioned (HT-FIFO) queues (partition_count > 1) use a different key
// family (SqsPartitionedMsg* in adapter/sqs_keys.go) that this slice does
// not reproduce, so they fail closed rather than emit classic keys a
// partitioned queue would never read.

// sqsRestoreGeneration is the uniform generation stamped across the queue
// meta, gen counter, and every message key (Option B; see file header).
const sqsRestoreGeneration uint64 = 1

var (
	// ErrSQSEncodeInvalidQueue is returned when a _queue.json cannot be
	// parsed or carries an empty queue name.
	ErrSQSEncodeInvalidQueue = errors.New("backup: sqs encode invalid _queue.json")
	// ErrSQSEncodeInvalidMessage is returned when a messages.jsonl line
	// cannot be parsed.
	ErrSQSEncodeInvalidMessage = errors.New("backup: sqs encode invalid messages.jsonl")
	// ErrSQSEncodeNotRegular is returned when a dump file is not a regular
	// file (symlink / FIFO / device / directory).
	ErrSQSEncodeNotRegular = errors.New("backup: sqs dump file is not a regular file")
	// ErrSQSEncodeUnsupportedPartitioned is returned for HT-FIFO queues
	// (partition_count > 1), whose partitioned key family this slice does
	// not yet reproduce.
	ErrSQSEncodeUnsupportedPartitioned = errors.New("backup: sqs partitioned (HT-FIFO) queue not yet supported by encoder")
)

// sqsStoredQueueMeta mirrors the live adapter's sqsQueueMeta JSON shape
// (adapter/sqs_catalog.go). The encoder fills the fields recoverable from
// _queue.json plus the uniform restore generation; the rest default.
type sqsStoredQueueMeta struct {
	Name                      string `json:"name"`
	Generation                uint64 `json:"generation"`
	IsFIFO                    bool   `json:"is_fifo,omitempty"`
	ContentBasedDedup         bool   `json:"content_based_dedup,omitempty"`
	VisibilityTimeoutSeconds  int64  `json:"visibility_timeout_seconds"`
	MessageRetentionSeconds   int64  `json:"message_retention_seconds"`
	DelaySeconds              int64  `json:"delay_seconds"`
	ReceiveMessageWaitSeconds int64  `json:"receive_message_wait_seconds"`
	MaximumMessageSize        int64  `json:"maximum_message_size"`
	RedrivePolicy             string `json:"redrive_policy,omitempty"`
	PartitionCount            uint32 `json:"partition_count,omitempty"`
	FifoThroughputLimit       string `json:"fifo_throughput_limit,omitempty"`
	DeduplicationScope        string `json:"deduplication_scope,omitempty"`
}

// sqsStoredMessage mirrors the live adapter's message value JSON
// (adapter/sqs_messages.go). Body is a base64-std []byte (Go default) and
// MessageAttributes pass through as raw JSON, so the emitted value is
// byte-compatible with what the live adapter writes.
type sqsStoredMessage struct {
	MessageID              string                     `json:"message_id"`
	Body                   []byte                     `json:"body"`
	MD5OfBody              string                     `json:"md5_of_body"`
	MD5OfMessageAttributes string                     `json:"md5_of_message_attributes,omitempty"`
	MessageAttributes      map[string]json.RawMessage `json:"message_attributes,omitempty"`
	SenderID               string                     `json:"sender_id,omitempty"`
	SendTimestampMillis    int64                      `json:"send_timestamp_millis"`
	AvailableAtMillis      int64                      `json:"available_at_millis"`
	VisibleAtMillis        int64                      `json:"visible_at_millis"`
	ReceiveCount           int64                      `json:"receive_count"`
	FirstReceiveMillis     int64                      `json:"first_receive_millis,omitempty"`
	CurrentReceiptToken    []byte                     `json:"current_receipt_token"`
	QueueGeneration        uint64                     `json:"queue_generation"`
	MessageGroupID         string                     `json:"message_group_id,omitempty"`
	MessageDeduplicationID string                     `json:"message_deduplication_id,omitempty"`
	SequenceNumber         uint64                     `json:"sequence_number,omitempty"`
	DeadLetterSourceArn    string                     `json:"dead_letter_source_arn,omitempty"`
}

// SQSRecordEncoder reconstructs the internal SQS keyspace from the decoded
// sqs/ directory tree. (Named distinctly from the decoder's SQSEncoder in
// sqs.go.)
type SQSRecordEncoder struct {
	inRoot string
}

// NewSQSRecordEncoder constructs an encoder rooted at <inRoot>/sqs/.
func NewSQSRecordEncoder(inRoot string) *SQSRecordEncoder {
	return &SQSRecordEncoder{inRoot: inRoot}
}

func (e *SQSRecordEncoder) sqsDir() string {
	return filepath.Join(e.inRoot, "sqs")
}

// Encode walks sqs/<queue>/ and stages each queue's meta + counters +
// message records. A missing sqs/ directory is not an error.
func (e *SQSRecordEncoder) Encode(b *snapshotBuilder) error {
	dir := e.sqsDir()
	if err := lstatDumpDir(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() { _ = root.Close() }()
	entries, err := readRootDirEntries(root)
	if err != nil {
		return err
	}
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		if err := e.encodeQueue(b, root, ent.Name()); err != nil {
			return err
		}
	}
	return nil
}

// encodeQueue reads one <queue>/_queue.json + messages.jsonl and stages the
// queue meta, generation/sequence counters, and message data records.
func (e *SQSRecordEncoder) encodeQueue(b *snapshotBuilder, root *os.Root, queueDir string) error {
	meta, err := e.readQueueMeta(root, queueDir)
	if err != nil {
		return err
	}
	if meta.Name == "" {
		return errors.Wrapf(ErrSQSEncodeInvalidQueue, "%s/_queue.json: empty queue name", queueDir)
	}
	if meta.PartitionCount > 1 {
		return errors.Wrapf(ErrSQSEncodeUnsupportedPartitioned,
			"%s: partition_count %d", queueDir, meta.PartitionCount)
	}
	if err := e.addQueueMeta(b, meta); err != nil {
		return err
	}
	return e.encodeQueueMessages(b, root, queueDir, meta)
}

// addQueueMeta stages the !sqs|queue|meta| record and the !sqs|queue|gen|
// counter.
func (e *SQSRecordEncoder) addQueueMeta(b *snapshotBuilder, pub sqsQueueMetaPublic) error {
	stored := sqsStoredQueueMeta{
		Name:                      pub.Name,
		Generation:                sqsRestoreGeneration,
		IsFIFO:                    pub.FIFO,
		ContentBasedDedup:         pub.ContentBasedDeduplication,
		VisibilityTimeoutSeconds:  pub.VisibilityTimeoutSeconds,
		MessageRetentionSeconds:   pub.MessageRetentionSeconds,
		DelaySeconds:              pub.DelaySeconds,
		ReceiveMessageWaitSeconds: pub.ReceiveMessageWaitSeconds,
		MaximumMessageSize:        pub.MaximumMessageSize,
		RedrivePolicy:             pub.RedrivePolicy,
		PartitionCount:            pub.PartitionCount,
		FifoThroughputLimit:       pub.FifoThroughputLimit,
		DeduplicationScope:        pub.DeduplicationScope,
	}
	metaVal, err := marshalStoredSQS(storedSQSMetaMagic, stored)
	if err != nil {
		return err
	}
	if err := b.Add(sqsQueueMetaKeyBytes(pub.Name), metaVal, 0); err != nil {
		return err
	}
	genVal := []byte(strconv.FormatUint(sqsRestoreGeneration, 10))
	return b.Add(sqsQueueGenKeyBytes(pub.Name), genVal, 0)
}

// encodeQueueMessages reads messages.jsonl and stages a !sqs|msg|data|
// record per message, then (for FIFO queues) the !sqs|queue|seq| counter.
func (e *SQSRecordEncoder) encodeQueueMessages(b *snapshotBuilder, root *os.Root, queueDir string, meta sqsQueueMetaPublic) error {
	records, err := e.readMessages(root, queueDir)
	if err != nil {
		return err
	}
	var maxSeq uint64
	for i := range records {
		seq, err := e.addMessage(b, meta.Name, records[i])
		if err != nil {
			return err
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	if meta.FIFO {
		// The sequence counter is the next value the live FIFO send path
		// will allocate; restore it to max(seen)+1 so it never reissues a
		// sequence number an existing message already holds.
		seqVal := []byte(strconv.FormatUint(maxSeq+1, 10))
		if err := b.Add(sqsQueueSeqKeyBytes(meta.Name), seqVal, 0); err != nil {
			return err
		}
	}
	return nil
}

// addMessage stages one !sqs|msg|data| record and returns the message's
// sequence number (for the seq-counter computation). Visibility state is
// zeroed (every message restores visible), matching the decoder default.
func (e *SQSRecordEncoder) addMessage(b *snapshotBuilder, queueName string, rec sqsMessageRecord) (uint64, error) {
	if rec.MessageID == "" {
		return 0, errors.Wrap(ErrSQSEncodeInvalidMessage, "message missing message_id")
	}
	stored := sqsStoredMessage{
		MessageID:              rec.MessageID,
		Body:                   []byte(rec.Body),
		MD5OfBody:              rec.MD5OfBody,
		MD5OfMessageAttributes: rec.MD5OfMessageAttributes,
		MessageAttributes:      rec.MessageAttributes,
		SenderID:               rec.SenderID,
		SendTimestampMillis:    rec.SendTimestampMillis,
		AvailableAtMillis:      rec.AvailableAtMillis,
		VisibleAtMillis:        0,
		ReceiveCount:           0,
		FirstReceiveMillis:     0,
		CurrentReceiptToken:    nil,
		QueueGeneration:        sqsRestoreGeneration,
		MessageGroupID:         rec.MessageGroupID,
		MessageDeduplicationID: rec.MessageDedupID,
		SequenceNumber:         rec.SequenceNumber,
		DeadLetterSourceArn:    rec.DeadLetterSourceArn,
	}
	val, err := marshalStoredSQS(storedSQSMsgMagic, stored)
	if err != nil {
		return 0, err
	}
	key := sqsMsgDataKeyBytes(queueName, sqsRestoreGeneration, rec.MessageID)
	if err := b.Add(key, val, 0); err != nil {
		return 0, err
	}
	return rec.SequenceNumber, nil
}

// openSQSRootFile opens rel within root with the Lstat + IsRegular +
// refuseHardLink guard (refusing a symlink / FIFO / device / directory
// before the read), the same pattern the DynamoDB schema reader uses. A
// missing file surfaces as a wrapped os.ErrNotExist so callers can treat
// an absent optional file as empty.
func (e *SQSRecordEncoder) openSQSRootFile(root *os.Root, rel string) (*os.File, error) {
	// Pre-open Lstat refuses a symlink / FIFO / device / directory BEFORE
	// Open, so a reader-less FIFO already in place cannot block the open.
	linfo, err := root.Lstat(rel)
	if err != nil {
		return nil, errors.WithStack(err) // wraps os.ErrNotExist when absent
	}
	if !linfo.Mode().IsRegular() {
		return nil, errors.Wrapf(ErrSQSEncodeNotRegular, "%s (mode=%s)", rel, linfo.Mode())
	}
	if err := refuseHardLink(linfo, rel); err != nil {
		return nil, err
	}
	f, err := root.Open(rel)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Post-open fstat is authoritative: it closes the Lstat->Open TOCTOU
	// gap by re-checking the actual opened descriptor, so a regular file
	// swapped to a hard link / non-regular after the Lstat is caught here
	// (gemini security-high on PR #846).
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, errors.WithStack(err)
	}
	if !fi.Mode().IsRegular() {
		_ = f.Close()
		return nil, errors.Wrapf(ErrSQSEncodeNotRegular, "%s (mode=%s)", rel, fi.Mode())
	}
	if err := refuseHardLink(fi, rel); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// readQueueMeta opens <queue>/_queue.json within root (symlink / FIFO /
// hard-link safe) and decodes the public queue projection.
func (e *SQSRecordEncoder) readQueueMeta(root *os.Root, queueDir string) (sqsQueueMetaPublic, error) {
	rel := filepath.Join(queueDir, "_queue.json")
	f, err := e.openSQSRootFile(root, rel)
	if err != nil {
		return sqsQueueMetaPublic{}, err
	}
	defer func() { _ = f.Close() }()
	var pub sqsQueueMetaPublic
	if err := decodeOneJSON(f, &pub); err != nil {
		return sqsQueueMetaPublic{}, errors.Wrapf(ErrSQSEncodeInvalidQueue, "%s: %v", rel, err)
	}
	if pub.FormatVersion != 1 {
		return sqsQueueMetaPublic{}, errors.Wrapf(ErrSQSEncodeInvalidQueue,
			"%s: unsupported format_version %d", rel, pub.FormatVersion)
	}
	return pub, nil
}

// readMessages reads <queue>/messages.jsonl (one sqsMessageRecord per
// line). A missing file means an empty queue.
func (e *SQSRecordEncoder) readMessages(root *os.Root, queueDir string) ([]sqsMessageRecord, error) {
	rel := filepath.Join(queueDir, "messages.jsonl")
	f, err := e.openSQSRootFile(root, rel)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	dec := json.NewDecoder(f)
	var out []sqsMessageRecord
	for dec.More() {
		var rec sqsMessageRecord
		if err := dec.Decode(&rec); err != nil {
			return nil, errors.Wrapf(ErrSQSEncodeInvalidMessage, "%s: %v", rel, err)
		}
		out = append(out, rec)
	}
	return out, nil
}

// marshalStoredSQS builds magic + deterministic JSON for a stored SQS
// value.
func marshalStoredSQS(magic []byte, v any) ([]byte, error) {
	body, err := json.Marshal(v)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// const-capacity, zero-length start + append (magic then body): keeps
	// the len(magic)+len(body) arithmetic out of make() — which CodeQL
	// flags as a potential allocation-size overflow — while makezero stays
	// happy. Same pattern as marshalStoredDDBSchema/Item.
	out := make([]byte, 0, len(magic))
	out = append(out, magic...)
	return append(out, body...), nil
}

// encodeSQSSegment mirrors adapter/sqs_keys.go: base64 raw-URL (never
// emits the '|' separator).
func encodeSQSSegment(v string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(v))
}

func sqsQueueMetaKeyBytes(queueName string) []byte {
	return []byte(SQSQueueMetaPrefix + encodeSQSSegment(queueName))
}

func sqsQueueGenKeyBytes(queueName string) []byte {
	return []byte(SQSQueueGenPrefix + encodeSQSSegment(queueName))
}

func sqsQueueSeqKeyBytes(queueName string) []byte {
	return []byte(SQSQueueSeqPrefix + encodeSQSSegment(queueName))
}

// sqsMsgDataKeyBytes reproduces adapter/sqs_messages.go sqsMsgDataKey:
// prefix + base64url(queue) + BE-u64(gen) + base64url(messageID).
func sqsMsgDataKeyBytes(queueName string, generation uint64, messageID string) []byte {
	out := []byte(SQSMsgDataPrefix)
	out = append(out, encodeSQSSegment(queueName)...)
	out = binary.BigEndian.AppendUint64(out, generation)
	return append(out, encodeSQSSegment(messageID)...)
}
