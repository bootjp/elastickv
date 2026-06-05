package backup

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
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
//   - (FIFO) seq counter -> !sqs|queue|seq|   = max(sequence_number)
//     (last issued — matches adapter/sqs_fifo.go's loadFifoSequence + prevSeq+1)
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
	// ErrSQSEncodeMissingPartition fires when a partitioned queue
	// (meta.PartitionCount > 1) has a message whose Partition field is
	// nil. Pre-M5-3 dumps lack the field entirely; replaying one against
	// a partitioned queue would silently route every message to
	// partition 0 via the `partition != nil` dispatch in addMessage,
	// while the live readers scan the partitioned keyspace selected
	// from raw PartitionCount > 1 — i.e. classic-shape keys against a
	// partitioned reader = invisible on first read. Pinned by
	// TestSQSEncodeRejectsMissingPartitionOnPartitionedQueue.
	ErrSQSEncodeMissingPartition = errors.New("backup: sqs partitioned queue message missing partition field")
	// ErrSQSEncodeOutOfRangePartition fires when a partitioned-queue
	// message carries *Partition >= meta.PartitionCount. The dump is
	// malformed (operator hand-edit or a future M5-3 decoder bug).
	ErrSQSEncodeOutOfRangePartition = errors.New("backup: sqs message partition out of range for queue partition_count")
	// ErrSQSEncodePartitionRoutingMismatch fires when a partitioned
	// FIFO queue with FifoThroughputLimit == "perQueue" has a message
	// with *Partition != 0. The live partitionFor
	// (adapter/sqs_partitioning.go:71-72) forces every group to
	// partition 0 in perQueue mode regardless of PartitionCount, and
	// ReceiveMessage only scans the partition-0 lane. Accepting any
	// other partition value would restore messages onto |p|N|... lanes
	// the live receive fan-out never visits — silent data loss on
	// first read. Codex P2 v914 v4 caught this gap.
	ErrSQSEncodePartitionRoutingMismatch = errors.New("backup: sqs perQueue HT-FIFO queue must keep all messages on partition 0")
	// ErrSQSEncodePartitionHashMismatch fires for a perMessageGroupId
	// HT-FIFO message whose Partition disagrees with
	// partitionFor(MessageGroupID). The live send/receive/group-lock
	// path uses partitionFor's FNV-1a-mod-PartitionCount mapping
	// (adapter/sqs_partitioning.go:64); restoring under the wrong
	// lane would split a FIFO group across two partition-scoped
	// group-lock keyspaces and break FIFO order on first read. Codex
	// P2 #929.
	ErrSQSEncodePartitionHashMismatch = errors.New("backup: sqs perMessageGroupId HT-FIFO message partition disagrees with partitionFor(message_group_id)")
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
	// Fail-closed validation up-front so a malformed dump never stages
	// partial records. Uses raw meta.PartitionCount > 1 as the
	// partitioned-queue predicate (NOT effectivePartitionCount; codex
	// P2 v914 v7 - using the effective count would allow a perQueue
	// dump with Partition == nil to slip past the missing-partition
	// gate, then addMessage's `partition != nil` dispatch would emit
	// classic-shape keys against a partitioned-keyspace queue, making
	// every restored message invisible).
	if err := validatePartitioning(&meta, records); err != nil {
		return err
	}
	// Per-partition deterministic emission for partitioned queues. The
	// snapshotBuilder sorts by key bytes on WriteTo so this does not
	// directly affect the .fsm byte output, but it makes the in-loop
	// state (maxSeq, future per-partition counters) deterministic and
	// matches the design doc's stated contract.
	if meta.PartitionCount > 1 {
		sortMessagesForPartitionedEmit(records)
	}
	maxSeq, err := e.stageMessageRecords(b, &meta, records)
	if err != nil {
		return err
	}
	if meta.FIFO && maxSeq > 0 {
		// The live FIFO send path (adapter/sqs_fifo.go: loadFifoSequence
		// + prevSeq+1) reads this key as the LAST issued sequence number
		// and advances by one before writing back. To preserve that
		// contract on restore, store max(seen) — not max+1, which would
		// cause the next send to skip a sequence number (codex P1 #846).
		// Empty FIFO queues (maxSeq == 0) emit no counter at all,
		// mirroring the live "no send yet" state where the missing key
		// reads as 0 and the first send issues sequence 1.
		seqVal := []byte(strconv.FormatUint(maxSeq, 10))
		if err := b.Add(sqsQueueSeqKeyBytes(meta.Name), seqVal, 0); err != nil {
			return err
		}
	}
	return nil
}

// stageMessageRecords iterates the (pre-validated, pre-sorted) records
// and stages each one's data + side records on b, returning the
// maximum SequenceNumber observed (used to write the FIFO seq counter).
// Split out of encodeQueueMessages so the parent stays under cyclop.
//
// isPartitioned is derived once from raw meta.PartitionCount > 1 — NOT
// from rec.Partition != nil. Codex P1 / gemini critical (PR #929):
// a classic queue dump (PartitionCount<=1) may carry an explicit
// rec.Partition = &0 that validatePartitioningOne lets through (gate 4
// only rejects *Partition != 0). Routing on `partition != nil` would
// emit partitioned keys for that classic queue, and the live reader
// only scans the classic keyspace - the messages become invisible on
// first read. Keying the dispatch on isPartitioned (a property of the
// QUEUE, not the message) closes that gap.
func (e *SQSRecordEncoder) stageMessageRecords(b *snapshotBuilder, meta *sqsQueueMetaPublic, records []sqsMessageRecord) (uint64, error) {
	var maxSeq uint64
	isPartitioned := meta.PartitionCount > 1
	// Pre-pass: pick the LATEST send per dedup-key tuple so only one
	// dedup row is emitted per (group, dedupID) [classic] or
	// (partition, group, dedupID) [partitioned]. When the live FIFO
	// dedup window expires, a duplicate (group, dedupID) re-send
	// overwrites the prior row; the dump may carry BOTH messages but
	// the live keyspace only ever held one dedup row at a time. Naive
	// per-message emission would collide on snapshotBuilder.Add and
	// abort the restore. Codex P2 #929.
	dedupWinner := pickDedupWinners(meta, records, isPartitioned)
	for i := range records {
		var partition uint32
		if isPartitioned && records[i].Partition != nil {
			partition = *records[i].Partition
		}
		seq, err := e.addMessage(b, meta.Name, isPartitioned, partition, records[i])
		if err != nil {
			return 0, err
		}
		if err := e.addSideRecords(b, meta.Name, isPartitioned, partition, dedupWinner[i], meta, &records[i]); err != nil {
			return 0, err
		}
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	return maxSeq, nil
}

// pickDedupWinners returns a boolean slice indexed by records' position;
// true means "this message is the LATEST send for its dedup-key tuple
// and therefore owns the dedup row". Caller passes the boolean into
// addSideRecords so non-winners skip dedup emission.
//
// The winner-key tuple MUST match the live dedup-key shape exactly,
// otherwise two messages that would collide on snapshotBuilder.Add
// can both be marked as winners:
//   - Partitioned: live key is sqsPartitionedMsgDedupKey(queue,
//     partition, gen, groupID, dedupID), so the tuple is
//     (partition, group, dedupID).
//   - Classic: live key is sqsMsgDedupKey(queue, gen, dedupID) — NO
//     group / partition in the key — so the tuple collapses to
//     ("", "", dedupID). Codex P2 #929: keeping MessageGroupID in the
//     classic winner key would let two retained-but-expired duplicates
//     in DIFFERENT groups but with the same dedupID both win, then
//     both emit the same sqsMsgDedupKeyBytes(queue, dedupID) and
//     collide.
//
// For non-FIFO queues or messages with empty resolvedDedupID the
// returned slice entry is false — dedup emission was already skipped
// by addSideRecords for those cases.
func pickDedupWinners(meta *sqsQueueMetaPublic, records []sqsMessageRecord, isPartitioned bool) []bool {
	winners := make([]bool, len(records))
	if !meta.FIFO {
		return winners
	}
	type dedupKey struct {
		partition uint32
		group     string
		dedup     string
	}
	latest := make(map[dedupKey]int, len(records))
	for i := range records {
		d := resolveDedupID(&records[i], meta)
		if d == "" {
			continue
		}
		k := dedupKey{dedup: d}
		if isPartitioned {
			// Partitioned dedup key includes partition + group +
			// dedupID; group is load-bearing because the partitioned
			// constructor splits group / dedup segments with a '|'.
			k.group = records[i].MessageGroupID
			if records[i].Partition != nil {
				k.partition = *records[i].Partition
			}
		}
		// Classic case leaves k.partition=0 and k.group="" so two
		// retained-but-expired duplicates in different groups with
		// the same dedupID collapse to the same winner key —
		// matching the classic sqsMsgDedupKey shape.
		prev, seen := latest[k]
		if !seen || records[i].SendTimestampMillis > records[prev].SendTimestampMillis {
			latest[k] = i
		}
	}
	for _, idx := range latest {
		winners[idx] = true
	}
	return winners
}

// validatePartitioning runs the four fail-closed gates from the M5-3
// design doc §"Validation invariants" before any message is staged.
// All four use raw meta.PartitionCount > 1 as the partitioned-queue
// predicate, never effectivePartitionCount (codex P2 v914 v7).
func validatePartitioning(meta *sqsQueueMetaPublic, records []sqsMessageRecord) error {
	for i := range records {
		if err := validatePartitioningOne(meta, &records[i]); err != nil {
			return err
		}
	}
	return nil
}

// validatePartitioningOne checks one message against the four gates.
// Split out of validatePartitioning so the outer loop stays under the
// cyclop limit.
func validatePartitioningOne(meta *sqsQueueMetaPublic, rec *sqsMessageRecord) error {
	if meta.PartitionCount > 1 {
		return validatePartitioningPartitioned(meta, rec)
	}
	// Classic queue (PartitionCount <= 1): the only invalid state is
	// a non-zero Partition field — the dump is internally
	// inconsistent (classic-queue keyspace has no partition concept).
	if rec.Partition != nil && *rec.Partition != 0 {
		return errors.Wrapf(ErrSQSEncodeInvalidMessage,
			"queue %q (partition_count=%d): classic queue message %q has partition=%d",
			meta.Name, meta.PartitionCount, rec.MessageID, *rec.Partition)
	}
	return nil
}

// validatePartitioningPartitioned runs the three partitioned-queue
// gates (missing partition, out-of-range, perQueue routing mismatch).
// Caller has already verified meta.PartitionCount > 1.
func validatePartitioningPartitioned(meta *sqsQueueMetaPublic, rec *sqsMessageRecord) error {
	// Gate 1: missing partition. Pre-M5-3 dump replayed against a
	// partitioned queue, or M5-3 decoder bug. The operator must
	// re-decode with an M5-3 decoder.
	if rec.Partition == nil {
		return errors.Wrapf(ErrSQSEncodeMissingPartition,
			"queue %q (partition_count=%d): message %q missing partition field",
			meta.Name, meta.PartitionCount, rec.MessageID)
	}
	// Gate 2: out-of-range partition. Dump is malformed.
	if *rec.Partition >= meta.PartitionCount {
		return errors.Wrapf(ErrSQSEncodeOutOfRangePartition,
			"queue %q (partition_count=%d): message %q partition=%d out of range",
			meta.Name, meta.PartitionCount, rec.MessageID, *rec.Partition)
	}
	// Gate 3: perQueue HT-FIFO with nonzero partition. The live
	// partitionFor collapses every group to partition 0 in perQueue
	// mode; accepting any other partition would write to a lane the
	// live receive never scans.
	if meta.FifoThroughputLimit == sqsFifoThroughputPerQueue {
		if *rec.Partition != 0 {
			return errors.Wrapf(ErrSQSEncodePartitionRoutingMismatch,
				"queue %q (partition_count=%d, fifo_throughput_limit=%q): message %q partition=%d, want 0",
				meta.Name, meta.PartitionCount, meta.FifoThroughputLimit, rec.MessageID, *rec.Partition)
		}
		return nil
	}
	// Gate 5: perMessageGroupId HT-FIFO partition-hash consistency.
	// For perMessageGroupId queues, the live partitionFor maps each
	// group_id to one partition via FNV-1a hash; the receivers /
	// group-lock reader use that partition value to find messages.
	// A dump line with an in-range but inconsistent partition would
	// be restored under the wrong lane and split a FIFO group across
	// two partition-scoped group-lock keyspaces, allowing concurrent
	// out-of-order delivery. Mirror the hash here. Codex P2 #929.
	want := partitionForGroup(meta, rec.MessageGroupID)
	if *rec.Partition != want {
		return errors.Wrapf(ErrSQSEncodePartitionHashMismatch,
			"queue %q (partition_count=%d, group_id=%q): message %q partition=%d, partitionFor=%d",
			meta.Name, meta.PartitionCount, rec.MessageGroupID, rec.MessageID, *rec.Partition, want)
	}
	return nil
}

// partitionForGroup mirrors adapter/sqs_partitioning.go:partitionFor
// for use by the encoder's validation gate 5. Same FNV-1a 32-bit
// inlined hash, same masking on (PartitionCount - 1). MUST be a copy
// (M3b-3 circular-dep ban: internal/backup cannot import adapter).
// Returns 0 when partitionFor would: classic queues, perQueue mode,
// or empty MessageGroupID.
func partitionForGroup(meta *sqsQueueMetaPublic, messageGroupID string) uint32 {
	if meta == nil || meta.PartitionCount <= 1 {
		return 0
	}
	if meta.FifoThroughputLimit == sqsFifoThroughputPerQueue {
		return 0
	}
	if messageGroupID == "" {
		return 0
	}
	const (
		fnv32Offset uint32 = 2166136261
		fnv32Prime  uint32 = 16777619
	)
	hash := fnv32Offset
	for i := 0; i < len(messageGroupID); i++ {
		hash ^= uint32(messageGroupID[i])
		hash *= fnv32Prime
	}
	// PartitionCount is power-of-two (live validator-enforced).
	return hash & (meta.PartitionCount - 1)
}

// sortMessagesForPartitionedEmit sorts messages by
// (partition, send_timestamp_millis, sequence_number, message_id).
// Partition is the leading key so per-partition state (maxSeq, future
// counters) is computed deterministically; the remaining three fields
// match sortMessagesForEmit (sqs.go:842) for the classic path.
// validatePartitioning ensures Partition is non-nil whenever the
// caller invokes this fn (PartitionCount > 1), so the *rec.Partition
// dereference is safe.
func sortMessagesForPartitionedEmit(msgs []sqsMessageRecord) {
	partitionOf := func(r *sqsMessageRecord) uint32 {
		if r.Partition == nil {
			return 0
		}
		return *r.Partition
	}
	sort.SliceStable(msgs, func(i, j int) bool {
		a, b := &msgs[i], &msgs[j]
		pa, pb := partitionOf(a), partitionOf(b)
		switch {
		case pa != pb:
			return pa < pb
		case a.SendTimestampMillis != b.SendTimestampMillis:
			return a.SendTimestampMillis < b.SendTimestampMillis
		case a.SequenceNumber != b.SequenceNumber:
			return a.SequenceNumber < b.SequenceNumber
		default:
			return a.MessageID < b.MessageID
		}
	})
}

// addMessage stages one !sqs|msg|data| record and returns the message's
// sequence number (for the seq-counter computation). Visibility state is
// zeroed (every message restores visible), matching the decoder default.
//
// isPartitioned is derived from raw meta.PartitionCount > 1 by the
// caller, NOT from rec.Partition != nil. The partition uint32 carries
// the value (0 if the queue is classic). See stageMessageRecords doc
// for why the key-shape decision MUST be made on the queue-level
// predicate (codex P1 / gemini critical PR #929).
func (e *SQSRecordEncoder) addMessage(b *snapshotBuilder, queueName string, isPartitioned bool, partition uint32, rec sqsMessageRecord) (uint64, error) {
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
	var key []byte
	if isPartitioned {
		key = sqsPartitionedMsgDataKeyBytes(queueName, partition, sqsRestoreGeneration, rec.MessageID)
	} else {
		key = sqsMsgDataKeyBytes(queueName, sqsRestoreGeneration, rec.MessageID)
	}
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
	// PartitionCount must be a power of two when > 1. The live
	// validator (adapter/sqs_partitioning.go:isPowerOfTwo) enforces
	// this so partitionFor's mask AND (h & (n-1)) is equivalent to
	// (h % n). A malformed dump with e.g. partition_count=3 would
	// hash inconsistently and route messages to wrong partitions.
	// Coderabbit Major #929.
	if pub.PartitionCount > 1 && pub.PartitionCount&(pub.PartitionCount-1) != 0 {
		return sqsQueueMetaPublic{}, errors.Wrapf(ErrSQSEncodeInvalidQueue,
			"%s: partition_count %d must be a power of two", rel, pub.PartitionCount)
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

// sqsPartitionedQueueTerminator mirrors adapter/sqs_keys.go:82 — the
// literal '|' that terminates the encoded queue segment (and, in
// dedup keys, the encoded group segment) inside a partitioned key.
// encodeSQSSegment uses base64.RawURLEncoding which never emits '|',
// so the terminator is unambiguous.
const sqsPartitionedQueueTerminator byte = '|'

// sqsFifoThroughputPerQueue mirrors adapter/sqs_partitioning.go:37
// (htfifoThroughputPerQueue). When this is the FifoThroughputLimit
// value, partitionFor collapses every group to partition 0
// regardless of meta.PartitionCount; the encoder uses it for one
// fail-closed gate (ErrSQSEncodePartitionRoutingMismatch in slice D)
// and as an effectivePartitionCount input. CANNOT be imported from
// adapter — htfifoThroughputPerQueue is unexported (M3b-3 circular
// dependency pattern).
const sqsFifoThroughputPerQueue = "perQueue"

// Partitioned key prefixes mirror adapter/sqs_keys.go:91..95. They
// are the legacy prefixes with the partitioned discriminator "p|"
// appended.
const (
	SQSPartitionedMsgDataPrefix  = SQSMsgDataPrefix + sqsPartitionedDiscriminator
	SQSPartitionedMsgVisPrefix   = SQSMsgVisPrefix + sqsPartitionedDiscriminator
	SQSPartitionedMsgByAgePrefix = SQSMsgByAgePrefix + sqsPartitionedDiscriminator
	SQSPartitionedMsgDedupPrefix = SQSMsgDedupPrefix + sqsPartitionedDiscriminator
)

// sqsPartitionedMsgDataKeyBytes reproduces
// adapter/sqs_keys.go:sqsPartitionedMsgDataKey for a partitioned
// queue: prefix + base64url(queue) + '|' + BE-u32(partition) +
// BE-u64(gen) + base64url(messageID). Mirrored locally because
// internal/backup/ cannot import adapter (M3b-3 pattern).
func sqsPartitionedMsgDataKeyBytes(queueName string, partition uint32, generation uint64, messageID string) []byte {
	out := []byte(SQSPartitionedMsgDataPrefix)
	out = append(out, encodeSQSSegment(queueName)...)
	out = append(out, sqsPartitionedQueueTerminator)
	out = binary.BigEndian.AppendUint32(out, partition)
	out = binary.BigEndian.AppendUint64(out, generation)
	return append(out, encodeSQSSegment(messageID)...)
}

// effectivePartitionCount mirrors adapter/sqs_keys_dispatch.go:121
// (which operates on the unexported *adapter.sqsQueueMeta). Returns
// 1 for nil meta or PartitionCount <= 1, 1 when FifoThroughputLimit
// == "perQueue" (the live router collapses every group to partition
// 0 in that mode), otherwise meta.PartitionCount.
//
// NOTE: this helper is NOT used by the encoder's validation gates —
// the M5-3 design pins those gates on raw meta.PartitionCount > 1
// (codex P2 v914 v7). Kept here for diagnostics, ReceiveMessage scan
// fan-out cross-checks, and future symmetry with the adapter; using
// it in a gate predicate would silently let a perQueue dump with
// Partition == nil emit classic keys against a partitioned-keyspace
// queue.
func effectivePartitionCount(meta *sqsQueueMetaPublic) uint32 {
	if meta == nil || meta.PartitionCount <= 1 {
		return 1
	}
	if meta.FifoThroughputLimit == sqsFifoThroughputPerQueue {
		return 1
	}
	return meta.PartitionCount
}
