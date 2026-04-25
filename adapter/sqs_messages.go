package adapter

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // AWS SQS ETag specifies MD5; not used as a cryptographic primitive.
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

// Message-keyspace prefixes. The data record holds the message body and
// state; the visibility index is a separate, visible_at-sorted key family
// so ReceiveMessage can find the next visible message with a single bounded
// prefix scan.
const (
	SqsMsgDataPrefix = "!sqs|msg|data|"
	SqsMsgVisPrefix  = "!sqs|msg|vis|"
)

const (
	sqsMessageIDBytes             = 16
	sqsReceiptTokenBytes          = 16
	sqsReceiveDefaultMaxMessages  = 1
	sqsReceiveHardMaxMessages     = 10
	sqsReceiveScanOverfetchFactor = 2
	sqsChangeVisibilityMaxSeconds = sqsMaxVisibilityTimeoutSeconds
	sqsVisScanPageLimit           = 1024
	// sqsLongPollInterval is how often the poll loop re-scans the
	// visibility index when no messages were deliverable on the first
	// scan. 200 ms is small enough that a ~20 s WaitTimeSeconds still
	// has <1% tail-latency overhead, large enough that an empty queue
	// does not spin.
	sqsLongPollInterval = 200 * time.Millisecond
	// Version byte prefixed to encoded receipt handles. Bumped when the
	// on-wire handle format changes so old handles fail to decode loudly.
	sqsReceiptHandleVersion = byte(0x01)
	// Byte sizes used when pre-sizing key buffers. The exact value is not
	// critical; it only avoids one append growth for typical queue/ID
	// lengths.
	sqsKeyCapSmall = 32
	sqsKeyCapLarge = 64
	// Conversion factors for SQS second-granularity inputs.
	sqsMillisPerSecond = 1000
)

// AWS error codes specific to message operations.
const (
	sqsErrReceiptHandleInvalid = "ReceiptHandleIsInvalid"
	sqsErrInvalidReceiptHandle = "InvalidReceiptHandle"
	sqsErrMessageTooLong       = "InvalidParameterValue"
	sqsErrMessageNotInflight   = "MessageNotInflight"
)

// sqsMessageAttributeValue is the AWS-shaped MessageAttribute payload.
// We accept the same JSON shape AWS SDKs send: DataType (required),
// plus exactly one of StringValue or BinaryValue depending on the
// declared type. BinaryValue is base64 on the wire and []byte in
// memory; Go's json package handles the base64 conversion.
type sqsMessageAttributeValue struct {
	DataType    string `json:"DataType"`
	StringValue string `json:"StringValue,omitempty"`
	BinaryValue []byte `json:"BinaryValue,omitempty"`
}

// sqsMessageRecord mirrors !sqs|msg|data|... on disk. Visibility state
// (VisibleAtMillis, CurrentReceiptToken, ReceiveCount) lives here rather
// than in a side-record so a single OCC transaction can rotate it.
type sqsMessageRecord struct {
	MessageID              string                              `json:"message_id"`
	Body                   []byte                              `json:"body"`
	MD5OfBody              string                              `json:"md5_of_body"`
	MD5OfMessageAttributes string                              `json:"md5_of_message_attributes,omitempty"`
	MessageAttributes      map[string]sqsMessageAttributeValue `json:"message_attributes,omitempty"`
	SenderID               string                              `json:"sender_id,omitempty"`
	SendTimestampMillis    int64                               `json:"send_timestamp_millis"`
	AvailableAtMillis      int64                               `json:"available_at_millis"`
	VisibleAtMillis        int64                               `json:"visible_at_millis"`
	ReceiveCount           int64                               `json:"receive_count"`
	FirstReceiveMillis     int64                               `json:"first_receive_millis,omitempty"`
	CurrentReceiptToken    []byte                              `json:"current_receipt_token"`
	QueueGeneration        uint64                              `json:"queue_generation"`
	MessageGroupId         string                              `json:"message_group_id,omitempty"`
	MessageDeduplicationId string                              `json:"message_deduplication_id,omitempty"`
	// SequenceNumber is the per-queue strict-order counter assigned at
	// FIFO send time. AWS surfaces it on the SendMessage response and on
	// every ReceiveMessage; ordering across messages with different
	// MessageGroupId is undefined, but within a group the consumer sees
	// monotonically increasing sequence numbers.
	SequenceNumber uint64 `json:"sequence_number,omitempty"`
	// DeadLetterSourceArn is set on records that arrived in this queue
	// via DLQ redrive. AWS surfaces it on the DLQ-side ReceiveMessage so
	// consumers can correlate moved messages back to their origin.
	DeadLetterSourceArn string `json:"dead_letter_source_arn,omitempty"`
}

var storedSQSMsgPrefix = []byte{0x00, 'S', 'M', 0x01}

func encodeSQSMessageRecord(m *sqsMessageRecord) ([]byte, error) {
	body, err := json.Marshal(m)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	out := make([]byte, 0, len(storedSQSMsgPrefix)+len(body))
	out = append(out, storedSQSMsgPrefix...)
	out = append(out, body...)
	return out, nil
}

func decodeSQSMessageRecord(b []byte) (*sqsMessageRecord, error) {
	if !bytes.HasPrefix(b, storedSQSMsgPrefix) {
		return nil, errors.New("unrecognized sqs message format")
	}
	var m sqsMessageRecord
	if err := json.Unmarshal(b[len(storedSQSMsgPrefix):], &m); err != nil {
		return nil, errors.WithStack(err)
	}
	return &m, nil
}

// ------------------------ key helpers ------------------------

func sqsMsgDataKey(queueName string, gen uint64, messageID string) []byte {
	buf := make([]byte, 0, len(SqsMsgDataPrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsMsgDataPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	buf = append(buf, encodeSQSSegment(messageID)...)
	return buf
}

func sqsMsgVisKey(queueName string, gen uint64, visibleAtMillis int64, messageID string) []byte {
	buf := make([]byte, 0, len(SqsMsgVisPrefix)+sqsKeyCapLarge)
	buf = append(buf, SqsMsgVisPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	buf = appendU64(buf, uint64MaxZero(visibleAtMillis))
	buf = append(buf, encodeSQSSegment(messageID)...)
	return buf
}

func sqsMsgVisPrefixForQueue(queueName string, gen uint64) []byte {
	buf := make([]byte, 0, len(SqsMsgVisPrefix)+sqsKeyCapSmall)
	buf = append(buf, SqsMsgVisPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	return buf
}

// uint64MaxZero clamps negative int64 (which never happens for wall-clock
// timestamps but would silently overflow under uint64() cast) to zero.
func uint64MaxZero(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}

func sqsMsgVisScanBounds(queueName string, gen uint64, maxVisibleAtMillis int64) (start, end []byte) {
	prefix := sqsMsgVisPrefixForQueue(queueName, gen)
	start = append(bytes.Clone(prefix), zeroU64()...)
	upper := uint64MaxZero(maxVisibleAtMillis)
	if upper < ^uint64(0) {
		upper++
	}
	end = append(bytes.Clone(prefix), encodedU64(upper)...)
	return start, end
}

func appendU64(dst []byte, v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return append(dst, buf[:]...)
}

func encodedU64(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func zeroU64() []byte {
	var buf [8]byte
	return buf[:]
}

// ------------------------ message id + receipt handle ------------------------

func newMessageIDHex() (string, error) {
	var buf [sqsMessageIDBytes]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", errors.WithStack(err)
	}
	return hex.EncodeToString(buf[:]), nil
}

func newReceiptToken() ([]byte, error) {
	buf := make([]byte, sqsReceiptTokenBytes)
	if _, err := rand.Read(buf); err != nil {
		return nil, errors.WithStack(err)
	}
	return buf, nil
}

// encodeReceiptHandle packs (queue_gen, message_id, receipt_token) into a
// single opaque blob. Format:
//
//	[ 0 ] byte version = 0x01
//	[ 1..9 ] uint64 queue_gen (BE)
//	[ 9..25 ] 16 bytes message_id (raw bytes from hex decode)
//	[ 25..41 ] 16 bytes receipt_token
//
// The result is base64-urlsafe (no padding) so it passes through JSON and
// HTTP query parameters untouched.
func encodeReceiptHandle(queueGen uint64, messageIDHex string, receiptToken []byte) (string, error) {
	if len(receiptToken) != sqsReceiptTokenBytes {
		return "", errors.New("receipt token has wrong length")
	}
	idBytes, err := hex.DecodeString(messageIDHex)
	if err != nil || len(idBytes) != sqsMessageIDBytes {
		return "", errors.New("message id has wrong format")
	}
	buf := make([]byte, 0, 1+8+sqsMessageIDBytes+sqsReceiptTokenBytes)
	buf = append(buf, sqsReceiptHandleVersion)
	buf = appendU64(buf, queueGen)
	buf = append(buf, idBytes...)
	buf = append(buf, receiptToken...)
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

type decodedReceiptHandle struct {
	QueueGeneration uint64
	MessageIDHex    string
	ReceiptToken    []byte
}

func decodeReceiptHandle(raw string) (*decodedReceiptHandle, error) {
	b, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	want := 1 + 8 + sqsMessageIDBytes + sqsReceiptTokenBytes
	if len(b) != want || b[0] != sqsReceiptHandleVersion {
		return nil, errors.New("receipt handle length or version mismatch")
	}
	out := &decodedReceiptHandle{
		QueueGeneration: binary.BigEndian.Uint64(b[1:9]),
		MessageIDHex:    hex.EncodeToString(b[9 : 9+sqsMessageIDBytes]),
		ReceiptToken:    bytes.Clone(b[9+sqsMessageIDBytes:]),
	}
	return out, nil
}

// ------------------------ input decoding ------------------------

type sqsSendMessageInput struct {
	QueueUrl               string                              `json:"QueueUrl"`
	MessageBody            string                              `json:"MessageBody"`
	DelaySeconds           *int64                              `json:"DelaySeconds,omitempty"`
	MessageAttributes      map[string]sqsMessageAttributeValue `json:"MessageAttributes,omitempty"`
	MessageGroupId         string                              `json:"MessageGroupId,omitempty"`
	MessageDeduplicationId string                              `json:"MessageDeduplicationId,omitempty"`
}

type sqsReceiveMessageInput struct {
	QueueUrl              string   `json:"QueueUrl"`
	MaxNumberOfMessages   *int     `json:"MaxNumberOfMessages,omitempty"`
	VisibilityTimeout     *int64   `json:"VisibilityTimeout,omitempty"`
	WaitTimeSeconds       *int64   `json:"WaitTimeSeconds,omitempty"`
	MessageAttributeNames []string `json:"MessageAttributeNames,omitempty"`
}

type sqsDeleteMessageInput struct {
	QueueUrl      string `json:"QueueUrl"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

type sqsChangeVisibilityInput struct {
	QueueUrl          string `json:"QueueUrl"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout *int64 `json:"VisibilityTimeout"`
}

// ------------------------ handlers ------------------------

func (s *SQSServer) sendMessage(w http.ResponseWriter, r *http.Request) {
	var in sqsSendMessageInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	queueName, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	meta, readTS, apiErr := s.loadQueueMetaForSend(r.Context(), queueName, []byte(in.MessageBody))
	if apiErr != nil {
		writeSQSErrorFromErr(w, apiErr)
		return
	}
	if apiErr := validateMessageAttributes(in.MessageAttributes); apiErr != nil {
		writeSQSErrorFromErr(w, apiErr)
		return
	}
	if apiErr := validateSendFIFOParams(meta, in); apiErr != nil {
		writeSQSErrorFromErr(w, apiErr)
		return
	}
	delay, apiErr := resolveSendDelay(meta, in.DelaySeconds)
	if apiErr != nil {
		writeSQSErrorFromErr(w, apiErr)
		return
	}
	if meta.IsFIFO {
		s.sendMessageFifoLoop(w, r, queueName, meta, in, delay, readTS)
		return
	}

	rec, recordBytes, apiErr := buildSendRecord(meta, in, delay)
	if apiErr != nil {
		writeSQSErrorFromErr(w, apiErr)
		return
	}

	dataKey := sqsMsgDataKey(queueName, meta.Generation, rec.MessageID)
	visKey := sqsMsgVisKey(queueName, meta.Generation, rec.AvailableAtMillis, rec.MessageID)
	byAgeKey := sqsMsgByAgeKey(queueName, meta.Generation, rec.SendTimestampMillis, rec.MessageID)
	metaKey := sqsQueueMetaKey(queueName)
	genKey := sqsQueueGenKey(queueName)
	// StartTS + ReadKeys fence against a concurrent DeleteQueue /
	// PurgeQueue / SetQueueAttributes that commits between our meta
	// read and this dispatch. Without the fence, a DeleteQueue that
	// bumps the generation would land first, and this send would then
	// commit under the old generation — silently storing a message
	// that is no longer reachable via routing (acknowledged loss).
	// ErrWriteConflict surfaces via writeSQSErrorFromErr so clients
	// retry against the current queue state.
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{metaKey, genKey},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: dataKey, Value: recordBytes},
			{Op: kv.Put, Key: visKey, Value: []byte(rec.MessageID)},
			{Op: kv.Put, Key: byAgeKey, Value: []byte(rec.MessageID)},
		},
	}
	if _, err := s.coordinator.Dispatch(r.Context(), req); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}

	writeSQSJSON(w, map[string]string{
		"MessageId":              rec.MessageID,
		"MD5OfMessageBody":       rec.MD5OfBody,
		"MD5OfMessageAttributes": md5OfAttributesHex(in.MessageAttributes),
	})
}

// sendMessageFifoLoop runs the dedup-aware OCC send for FIFO queues
// under the standard retry budget. Stamping the dedup record + new
// sequence number happens inside one transaction so a concurrent send
// either observes the dedup hit or loses the OCC race.
func (s *SQSServer) sendMessageFifoLoop(w http.ResponseWriter, r *http.Request, queueName string, meta *sqsQueueMeta, in sqsSendMessageInput, delay int64, initialReadTS uint64) {
	dedupID := resolveFifoDedupID(meta, in)
	if dedupID == "" {
		writeSQSError(w, http.StatusBadRequest, sqsErrMissingParameter,
			"FIFO send requires MessageDeduplicationId or ContentBasedDeduplication=true")
		return
	}
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	readTS := initialReadTS
	for range transactRetryMaxAttempts {
		resp, retry, err := s.sendFifoMessage(r.Context(), queueName, meta, in, dedupID, delay, readTS)
		if err != nil {
			writeSQSErrorFromErr(w, err)
			return
		}
		if !retry {
			writeSQSJSON(w, resp)
			return
		}
		if err := waitRetryWithDeadline(r.Context(), deadline, backoff); err != nil {
			writeSQSErrorFromErr(w, errors.WithStack(err))
			return
		}
		backoff = nextTransactRetryBackoff(backoff)
		readTS = s.nextTxnReadTS(r.Context())
	}
	writeSQSError(w, http.StatusInternalServerError, sqsErrInternalFailure, "FIFO send retry attempts exhausted")
}

// loadQueueMetaForSend reads the queue metadata and body-size-gates the
// send. Returns the snapshot read timestamp alongside the metadata so
// the caller can pin its OCC dispatch to it; without that fence a
// concurrent DeleteQueue / PurgeQueue could slip in between our read
// and the write, storing a message under a dead generation.
func (s *SQSServer) loadQueueMetaForSend(ctx context.Context, queueName string, body []byte) (*sqsQueueMeta, uint64, error) {
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return nil, readTS, errors.WithStack(err)
	}
	if !exists {
		return nil, readTS, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	// AWS rejects SendMessage when MessageBody is empty (InvalidParameterValue
	// "Message body cannot be empty"). Silently enqueuing a zero-length
	// message would let producer bugs (missing serialization, a forgotten
	// body parameter) land in the queue where they later look like real
	// messages to consumers.
	if len(body) == 0 {
		return nil, readTS, newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "MessageBody is required")
	}
	if int64(len(body)) > meta.MaximumMessageSize {
		return nil, readTS, newSQSAPIError(http.StatusBadRequest, sqsErrMessageTooLong, "message body exceeds MaximumMessageSize")
	}
	return meta, readTS, nil
}

// validateSendFIFOParams enforces the AWS-compatible rules around
// MessageGroupId and MessageDeduplicationId:
//
//   - FIFO queues REQUIRE MessageGroupId on every send.
//   - FIFO queues without ContentBasedDeduplication REQUIRE
//     MessageDeduplicationId as well.
//   - Standard queues REJECT both fields — accepting them silently
//     would let misbehaving clients think they are getting FIFO
//     semantics.
//   - FIFO queues REJECT per-message DelaySeconds (already handled in
//     resolveSendDelay below, but we also short-circuit it here so the
//     error ordering matches AWS).
//
// Note: Milestone 1 does not yet enforce the per-group ordering /
// dedup invariants — the queue type gate is still useful so clients
// get the right AWS error shape, and the persisted MessageGroupId /
// MessageDeduplicationId fields are there for Milestone 2's group-
// lock implementation.
func validateSendFIFOParams(meta *sqsQueueMeta, in sqsSendMessageInput) error {
	if meta.IsFIFO {
		if in.MessageGroupId == "" {
			return newSQSAPIError(http.StatusBadRequest, sqsErrMissingParameter, "FIFO queue requires MessageGroupId")
		}
		if !meta.ContentBasedDedup && in.MessageDeduplicationId == "" {
			return newSQSAPIError(http.StatusBadRequest, sqsErrMissingParameter, "FIFO queue without ContentBasedDeduplication requires MessageDeduplicationId")
		}
		if in.DelaySeconds != nil {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "FIFO queue does not accept per-message DelaySeconds")
		}
		return nil
	}
	// Standard queue: both FIFO-only fields must be empty.
	if in.MessageGroupId != "" {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "MessageGroupId is only valid on FIFO queues")
	}
	if in.MessageDeduplicationId != "" {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "MessageDeduplicationId is only valid on FIFO queues")
	}
	return nil
}

func resolveSendDelay(meta *sqsQueueMeta, requested *int64) (int64, error) {
	delay := meta.DelaySeconds
	if requested == nil {
		return delay, nil
	}
	if *requested < 0 || *requested > sqsMaxDelaySeconds {
		return 0, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "DelaySeconds out of range")
	}
	return *requested, nil
}

func buildSendRecord(meta *sqsQueueMeta, in sqsSendMessageInput, delay int64) (*sqsMessageRecord, []byte, error) {
	messageID, err := newMessageIDHex()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	token, err := newReceiptToken()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	now := time.Now().UnixMilli()
	availableAt := now + delay*sqsMillisPerSecond
	body := []byte(in.MessageBody)
	rec := &sqsMessageRecord{
		MessageID:              messageID,
		Body:                   body,
		MD5OfBody:              sqsMD5Hex(body),
		MD5OfMessageAttributes: md5OfAttributesHex(in.MessageAttributes),
		MessageAttributes:      in.MessageAttributes,
		SendTimestampMillis:    now,
		AvailableAtMillis:      availableAt,
		VisibleAtMillis:        availableAt,
		CurrentReceiptToken:    token,
		QueueGeneration:        meta.Generation,
		MessageGroupId:         in.MessageGroupId,
		MessageDeduplicationId: in.MessageDeduplicationId,
	}
	recordBytes, err := encodeSQSMessageRecord(rec)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return rec, recordBytes, nil
}

//nolint:cyclop // AWS ReceiveMessage branches on per-message eligibility; splitting further just moves the branching around.
func (s *SQSServer) receiveMessage(w http.ResponseWriter, r *http.Request) {
	var in sqsReceiveMessageInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	queueName, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	ctx := r.Context()

	// Use LeaseRead to fence this scan against a leader that silently lost
	// quorum mid-request. When the lease is warm this is a local
	// wall-clock compare; when it is cold it falls back to a full
	// LinearizableRead.
	if _, err := kv.LeaseReadThrough(s.coordinator, ctx); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}

	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if !exists {
		writeSQSError(w, http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
		return
	}
	max, maxErr := resolveReceiveMaxMessages(in.MaxNumberOfMessages)
	if maxErr != nil {
		writeSQSErrorFromErr(w, maxErr)
		return
	}
	visibilityTimeout := meta.VisibilityTimeoutSeconds
	if in.VisibilityTimeout != nil {
		if *in.VisibilityTimeout < 0 || *in.VisibilityTimeout > sqsChangeVisibilityMaxSeconds {
			writeSQSError(w, http.StatusBadRequest, sqsErrInvalidAttributeValue, "VisibilityTimeout out of range")
			return
		}
		visibilityTimeout = *in.VisibilityTimeout
	}
	waitSeconds, waitErr := resolveReceiveWaitSeconds(in.WaitTimeSeconds, meta.ReceiveMessageWaitSeconds)
	if waitErr != nil {
		writeSQSErrorFromErr(w, waitErr)
		return
	}

	opts := sqsReceiveOptions{
		Max:                   max,
		VisibilityTimeout:     visibilityTimeout,
		WaitSeconds:           waitSeconds,
		MessageAttributeNames: in.MessageAttributeNames,
	}
	delivered, err := s.longPollReceive(ctx, queueName, opts)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{"Messages": delivered})
}

// sqsReceiveOptions bundles the per-request settings that ride down
// the receive call chain. Threading individual params through
// longPollReceive → scanAndDeliverOnce → rotateMessagesForDelivery →
// tryDeliverCandidate → commitReceiveRotation gets unwieldy fast,
// especially as new optional fields like MessageAttributeNames land.
type sqsReceiveOptions struct {
	Max                   int
	VisibilityTimeout     int64
	WaitSeconds           int64
	MessageAttributeNames []string
}

// resolveReceiveWaitSeconds picks the effective long-poll duration: the
// per-request WaitTimeSeconds if provided, else the queue default. AWS
// permits 0..20 and rejects anything outside with
// InvalidParameterValue; silently clamping a bad client value would
// mask bugs and change behavior (negative becomes immediate polling,
// oversized becomes long polls). The queue default is trusted — it was
// validated at SetQueueAttributes time.
func resolveReceiveWaitSeconds(requested *int64, queueDefault int64) (int64, error) {
	if requested == nil {
		return queueDefault, nil
	}
	v := *requested
	if v < 0 || v > sqsMaxReceiveMessageWaitSeconds {
		return 0, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "WaitTimeSeconds must be between 0 and 20")
	}
	return v, nil
}

// longPollReceive performs one scan+rotate attempt; if it returned 0
// messages and the caller asked to wait, it polls the visibility index
// on a fixed interval until a message arrives, WaitTimeSeconds elapses,
// or the request context is canceled. Milestone 1 uses polling rather
// than the commit-stream notifier described in §7.3 of the design; the
// poll interval is short enough (200 ms) to mask the difference for
// typical client-side WaitTimeSeconds values.
//
// Scan errors are propagated to the caller so a backend / routing
// failure surfaces as an actionable 5xx instead of a silent empty 200
// that would stall consumers. Each pass re-resolves queue metadata so
// a DeleteQueue / PurgeQueue that commits during a long wait is
// observed on the very next scan — otherwise we'd keep scanning
// orphan keys under the old generation.
func (s *SQSServer) longPollReceive(ctx context.Context, queueName string, opts sqsReceiveOptions) ([]map[string]any, error) {
	delivered, err := s.scanAndDeliverOnce(ctx, queueName, opts)
	if err != nil {
		return nil, err
	}
	if len(delivered) > 0 || opts.WaitSeconds <= 0 {
		return delivered, nil
	}
	deadline := time.Now().Add(time.Duration(opts.WaitSeconds) * time.Second)
	ticker := time.NewTicker(sqsLongPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return delivered, nil
		case <-ticker.C:
		}
		if time.Now().After(deadline) {
			return delivered, nil
		}
		delivered, err = s.scanAndDeliverOnce(ctx, queueName, opts)
		if err != nil {
			return nil, err
		}
		if len(delivered) > 0 {
			return delivered, nil
		}
	}
}

// scanAndDeliverOnce is the single-pass scan+rotate the long-poll loop
// re-runs. Each pass takes its own snapshot so the OCC StartTS tracks
// the most recent visible_at for the candidates it picked, AND each
// pass re-reads queue metadata so a concurrent DeleteQueue /
// PurgeQueue that bumps the generation is observed immediately. If
// the queue has been deleted the method returns QueueDoesNotExist;
// scan errors and other non-retryable failures propagate.
func (s *SQSServer) scanAndDeliverOnce(ctx context.Context, queueName string, opts sqsReceiveOptions) ([]map[string]any, error) {
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !exists {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	candidates, err := s.scanVisibleMessageCandidates(ctx, queueName, meta.Generation, opts.Max*sqsReceiveScanOverfetchFactor, readTS)
	if err != nil {
		return nil, err
	}
	return s.rotateMessagesForDelivery(ctx, queueName, meta, candidates, readTS, opts)
}

// resolveReceiveMaxMessages validates MaxNumberOfMessages against the
// AWS-documented range [1, 10]. An omitted value defaults to 1.
// Anything explicitly outside the range is an InvalidParameterValue
// — silently clamping would let a caller bug (e.g. passing 0 or a
// negative value) change to active polling behavior without surfacing
// the error, matching the same policy we apply to WaitTimeSeconds.
func resolveReceiveMaxMessages(requested *int) (int, error) {
	if requested == nil {
		return sqsReceiveDefaultMaxMessages, nil
	}
	v := *requested
	if v < 1 || v > sqsReceiveHardMaxMessages {
		return 0, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue, "MaxNumberOfMessages must be between 1 and 10")
	}
	return v, nil
}

// scanVisibleMessageCandidates returns vis-index entries with
// visible_at <= now, up to limit. Each entry carries the key (needed
// for the delete-old-vis step) and the message_id pointed at by its
// value.
type sqsMsgCandidate struct {
	visKey    []byte
	messageID string
}

func (s *SQSServer) scanVisibleMessageCandidates(ctx context.Context, queueName string, gen uint64, limit int, readTS uint64) ([]sqsMsgCandidate, error) {
	if limit <= 0 {
		return nil, nil
	}
	now := time.Now().UnixMilli()
	start, end := sqsMsgVisScanBounds(queueName, gen, now)
	page := limit
	if page > sqsVisScanPageLimit {
		page = sqsVisScanPageLimit
	}
	kvs, err := s.store.ScanAt(ctx, start, end, page, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	out := make([]sqsMsgCandidate, 0, len(kvs))
	for _, kvp := range kvs {
		out = append(out, sqsMsgCandidate{visKey: bytes.Clone(kvp.Key), messageID: string(kvp.Value)})
	}
	return out, nil
}

// rotateMessagesForDelivery runs an OCC transaction per candidate to
// rotate its visibility entry + receipt token. Expected race
// conditions (the message was deleted between scan and load, or
// another worker already rotated the same candidate — ErrWriteConflict)
// skip the candidate rather than aborting the whole batch; AWS lets
// ReceiveMessage return fewer messages than requested. But any
// non-retryable dispatch error (coordinator outage, shard routing
// failure, storage failure) propagates, because silently returning
// an empty 200 in those cases would stall consumers and hide the
// incident.
// loadCandidateRecord fetches and decodes the message record for a
// receive candidate. Returns (rec, dataKey, skip, err):
//   - skip=true, err=nil : ErrKeyNotFound race; caller skips this one.
//   - skip=false, err!=nil : non-retryable; propagate.
//   - skip=false, err=nil : record loaded.
func (s *SQSServer) loadCandidateRecord(ctx context.Context, queueName string, gen uint64, cand sqsMsgCandidate, readTS uint64) (*sqsMessageRecord, []byte, bool, error) {
	dataKey := sqsMsgDataKey(queueName, gen, cand.messageID)
	raw, err := s.store.GetAt(ctx, dataKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, dataKey, true, nil
		}
		return nil, dataKey, false, errors.WithStack(err)
	}
	rec, err := decodeSQSMessageRecord(raw)
	if err != nil {
		return nil, dataKey, false, err
	}
	return rec, dataKey, false, nil
}

// expireMessage removes a retention-expired record, its current
// visibility index entry, and the byage index entry in a single OCC
// transaction. On ErrWriteConflict (another worker raced us to delete
// or rotate this same message) we treat it as success: the message is
// no longer our responsibility either way. Any other error propagates
// so a coordinator / storage failure does not silently fall through
// to "delivered empty", matching the receive-error policy.
func (s *SQSServer) expireMessage(ctx context.Context, queueName string, gen uint64, visKey, dataKey []byte, rec *sqsMessageRecord, readTS uint64) error {
	byAgeKey := sqsMsgByAgeKey(queueName, gen, rec.SendTimestampMillis, rec.MessageID)
	readKeys := [][]byte{visKey, dataKey, sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: visKey},
		{Op: kv.Del, Key: dataKey},
		{Op: kv.Del, Key: byAgeKey},
	}
	// FIFO retention expiry must release the group lock so a successor
	// in the same group can become deliverable. This mirrors the delete
	// and redrive paths.
	if rec.MessageGroupId != "" {
		lockKey := sqsMsgGroupKey(queueName, gen, rec.MessageGroupId)
		lock, err := s.loadFifoGroupLock(ctx, queueName, gen, rec.MessageGroupId, readTS)
		if err != nil {
			return err
		}
		if lock != nil && lock.MessageID == rec.MessageID {
			readKeys = append(readKeys, lockKey)
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: lockKey})
		}
	}
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: readKeys,
		Elems:    elems,
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		if isRetryableTransactWriteError(err) {
			return nil
		}
		return errors.WithStack(err)
	}
	return nil
}

func (s *SQSServer) rotateMessagesForDelivery(
	ctx context.Context,
	queueName string,
	meta *sqsQueueMeta,
	candidates []sqsMsgCandidate,
	readTS uint64,
	opts sqsReceiveOptions,
) ([]map[string]any, error) {
	// Parse RedrivePolicy once per receive call rather than per
	// candidate. The struct only changes via SetQueueAttributes /
	// CreateQueue, so cross-candidate caching is safe.
	var redrive *parsedRedrivePolicy
	if meta.RedrivePolicy != "" {
		if p, err := parseRedrivePolicy(meta.RedrivePolicy); err == nil {
			redrive = p
		}
	}
	delivered := make([]map[string]any, 0, opts.Max)
	for _, cand := range candidates {
		if len(delivered) >= opts.Max {
			break
		}
		msg, skip, err := s.tryDeliverCandidate(ctx, queueName, meta.Generation, cand, meta.MessageRetentionSeconds, readTS, opts, redrive)
		if err != nil {
			return delivered, err
		}
		if skip {
			continue
		}
		delivered = append(delivered, msg)
	}
	return delivered, nil
}

// tryDeliverCandidate attempts one scan→load→rotate for a single
// candidate. The return triple is:
//
//   - (msg, false, nil)  → delivered, caller appends.
//   - (nil, true,  nil)  → expected race; skip this candidate only.
//     Covers ErrKeyNotFound (someone deleted the record between the
//     vis-index scan and our GetAt) and ErrWriteConflict on dispatch
//     (another receive rotated the same record).
//   - (nil, false, err)  → non-retryable failure; propagate up the
//     stack so ReceiveMessage returns an actionable 5xx instead of
//     a false-empty 200.
func (s *SQSServer) tryDeliverCandidate(
	ctx context.Context,
	queueName string,
	gen uint64,
	cand sqsMsgCandidate,
	retentionSeconds int64,
	readTS uint64,
	opts sqsReceiveOptions,
	redrive *parsedRedrivePolicy,
) (map[string]any, bool, error) {
	rec, dataKey, skip, err := s.loadCandidateRecord(ctx, queueName, gen, cand, readTS)
	if skip || err != nil {
		return nil, skip, err
	}
	if expired, err := s.handleRetentionExpiry(ctx, queueName, gen, cand, dataKey, rec, retentionSeconds, readTS); expired || err != nil {
		return nil, expired, err
	}
	if shouldRedrive(rec, redrive) {
		// The candidate has hit maxReceiveCount; move it to the DLQ
		// inside its own OCC transaction and skip past it. The
		// receive response intentionally omits redriven messages —
		// AWS does the same and consumers polling the source queue
		// must not observe a poison message past the limit.
		moved, err := s.redriveCandidateToDLQ(ctx, queueName, gen, cand, dataKey, rec, redrive, s.queueArn(queueName), readTS)
		if err != nil {
			return nil, false, err
		}
		return nil, moved, nil
	}
	// FIFO group lock filter: skip candidates whose group is held by
	// another in-flight message. Standard queues short-circuit because
	// MessageGroupId is empty.
	lockState := fifoLockAcquire
	var lockKey []byte
	if rec.MessageGroupId != "" {
		state, key, err := s.classifyFifoGroupLock(ctx, queueName, gen, rec, readTS)
		if err != nil {
			return nil, false, err
		}
		if state == fifoLockSkip {
			return nil, true, nil
		}
		lockState = state
		lockKey = key
	}
	return s.commitReceiveRotation(ctx, queueName, gen, cand, dataKey, rec, readTS, opts, lockKey, lockState)
}

// handleRetentionExpiry deletes the candidate inline when its
// send age exceeds MessageRetentionPeriod, so the vis-index scan
// does not keep re-finding it. Returns (expired, err): expired=true
// means the candidate has been (or is being) reaped and the caller
// must skip.
func (s *SQSServer) handleRetentionExpiry(ctx context.Context, queueName string, gen uint64, cand sqsMsgCandidate, dataKey []byte, rec *sqsMessageRecord, retentionSeconds int64, readTS uint64) (bool, error) {
	if retentionSeconds <= 0 {
		return false, nil
	}
	now := time.Now().UnixMilli()
	if now-rec.SendTimestampMillis <= retentionSeconds*sqsMillisPerSecond {
		return false, nil
	}
	if err := s.expireMessage(ctx, queueName, gen, cand.visKey, dataKey, rec, readTS); err != nil {
		return false, err
	}
	return true, nil
}

// commitReceiveRotation runs the final OCC dispatch that rotates
// receipt token + visibility index for a non-expired candidate. When
// the candidate carries a MessageGroupId the transaction also
// installs (or refreshes) the per-group lock so a later message in
// the same group cannot overtake it on the next receive.
func (s *SQSServer) commitReceiveRotation(ctx context.Context, queueName string, gen uint64, cand sqsMsgCandidate, dataKey []byte, rec *sqsMessageRecord, readTS uint64, opts sqsReceiveOptions, lockKey []byte, lockState fifoCandidateLockState) (map[string]any, bool, error) {
	newToken, err := newReceiptToken()
	if err != nil {
		return nil, false, err
	}
	now := time.Now().UnixMilli()
	newVisibleAt := now + opts.VisibilityTimeout*sqsMillisPerSecond
	rec.VisibleAtMillis = newVisibleAt
	rec.CurrentReceiptToken = newToken
	rec.ReceiveCount++
	if rec.FirstReceiveMillis == 0 {
		rec.FirstReceiveMillis = now
	}
	recordBytes, err := encodeSQSMessageRecord(rec)
	if err != nil {
		return nil, false, err
	}
	newVisKey := sqsMsgVisKey(queueName, gen, newVisibleAt, cand.messageID)
	req, err := buildReceiveRotationOps(queueName, cand, dataKey, recordBytes, newVisKey, lockKey, lockState, newVisibleAt, readTS)
	if err != nil {
		return nil, false, err
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		if isRetryableTransactWriteError(err) {
			return nil, true, nil
		}
		return nil, false, errors.WithStack(err)
	}

	handle, err := encodeReceiptHandle(gen, cand.messageID, newToken)
	if err != nil {
		return nil, false, err
	}
	sysAttrs := buildReceiveSysAttributes(rec)
	resp := map[string]any{
		"MessageId":     cand.messageID,
		"ReceiptHandle": handle,
		"Body":          string(rec.Body),
		"MD5OfBody":     rec.MD5OfBody,
		"Attributes":    sysAttrs,
	}
	if filtered := selectMessageAttributes(rec.MessageAttributes, opts.MessageAttributeNames); len(filtered) > 0 {
		resp["MessageAttributes"] = filtered
		resp["MD5OfMessageAttributes"] = md5OfAttributesHex(filtered)
	}
	return resp, false, nil
}

// buildReceiveRotationOps assembles the OCC OperationGroup for a
// successful receive rotation. The FIFO group lock branch is split out
// here so commitReceiveRotation stays under the cyclomatic budget.
func buildReceiveRotationOps(
	queueName string,
	cand sqsMsgCandidate,
	dataKey []byte,
	recordBytes []byte,
	newVisKey []byte,
	lockKey []byte,
	lockState fifoCandidateLockState,
	newVisibleAt int64,
	readTS uint64,
) (*kv.OperationGroup[kv.OP], error) {
	readKeys := [][]byte{cand.visKey, dataKey, sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: cand.visKey},
		{Op: kv.Put, Key: newVisKey, Value: []byte(cand.messageID)},
		{Op: kv.Put, Key: dataKey, Value: recordBytes},
	}
	if lockKey != nil {
		_ = lockState // both fifoLockOwn and fifoLockAcquire write the same shape; documented for clarity
		readKeys = append(readKeys, lockKey)
		mut, err := fifoLockMutationsForReceive(lockKey, cand.messageID, newVisibleAt)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		elems = append(elems, mut...)
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: readKeys,
		Elems:    elems,
	}, nil
}

// buildReceiveSysAttributes flattens the message record's system
// attributes into the AWS-shaped string map. Splitting it out keeps
// commitReceiveRotation under the cyclomatic budget.
func buildReceiveSysAttributes(rec *sqsMessageRecord) map[string]string {
	sysAttrs := map[string]string{
		"ApproximateReceiveCount":          strconv.FormatInt(rec.ReceiveCount, 10),
		"SentTimestamp":                    strconv.FormatInt(rec.SendTimestampMillis, 10),
		"ApproximateFirstReceiveTimestamp": strconv.FormatInt(rec.FirstReceiveMillis, 10),
	}
	if rec.MessageGroupId != "" {
		sysAttrs["MessageGroupId"] = rec.MessageGroupId
	}
	if rec.MessageDeduplicationId != "" {
		sysAttrs["MessageDeduplicationId"] = rec.MessageDeduplicationId
	}
	if rec.SequenceNumber > 0 {
		sysAttrs["SequenceNumber"] = strconv.FormatUint(rec.SequenceNumber, 10)
	}
	if rec.DeadLetterSourceArn != "" {
		sysAttrs["DeadLetterQueueSourceArn"] = rec.DeadLetterSourceArn
	}
	return sysAttrs
}

// selectMessageAttributes filters the stored MessageAttributes by the
// names the caller asked for. AWS supports:
//
//   - omission / empty list  → return no attributes.
//   - "All" or ".*"          → return everything.
//   - explicit list          → return only those exact names.
//
// Returning a non-empty filter means the response also carries an
// MD5OfMessageAttributes computed over the *filtered* set so SDKs that
// re-verify the digest do not see a hash over attributes they did not
// receive.
func selectMessageAttributes(attrs map[string]sqsMessageAttributeValue, names []string) map[string]sqsMessageAttributeValue {
	if len(attrs) == 0 || len(names) == 0 {
		return nil
	}
	all := false
	want := make(map[string]bool, len(names))
	for _, n := range names {
		if n == "All" || n == ".*" {
			all = true
			break
		}
		want[n] = true
	}
	if all {
		return attrs
	}
	out := make(map[string]sqsMessageAttributeValue, len(want))
	for name, v := range attrs {
		if want[name] {
			out[name] = v
		}
	}
	return out
}

func (s *SQSServer) deleteMessage(w http.ResponseWriter, r *http.Request) {
	var in sqsDeleteMessageInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	queueName, handle, err := s.parseQueueAndReceipt(in.QueueUrl, in.ReceiptHandle)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := s.deleteMessageWithRetry(r.Context(), queueName, handle); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

// deleteMessageWithRetry runs the load-check-commit flow under one OCC
// budget. AWS SQS semantics: a stale receipt handle (message already
// gone, or token rotated by another consumer) is a 200 no-op, NOT an
// error. The only error cases are structural (malformed handle, caught
// before this function) and infrastructure (retry budget exhausted).
// ErrWriteConflict on the delete Dispatch means a concurrent rotation
// / delete landed between our read and our commit; we retry so the
// next pass either sees the rotated token (no-op success) or the
// missing record (no-op success).
func (s *SQSServer) deleteMessageWithRetry(ctx context.Context, queueName string, handle *decodedReceiptHandle) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		rec, dataKey, readTS, outcome, err := s.loadMessageForDelete(ctx, queueName, handle)
		if err != nil {
			return err
		}
		if outcome == sqsDeleteNoOp {
			return nil
		}
		req, err := s.buildDeleteOps(ctx, queueName, handle, rec, dataKey, readTS)
		if err != nil {
			return err
		}
		if _, err := s.coordinator.Dispatch(ctx, req); err == nil {
			return nil
		} else if !isRetryableTransactWriteError(err) {
			return errors.WithStack(err)
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "delete message retry attempts exhausted")
}

// buildDeleteOps assembles the OCC OperationGroup for a DeleteMessage
// commit. The FIFO group-lock release branch lives here so the
// retry-loop wrapper stays readable and within the cyclomatic budget.
func (s *SQSServer) buildDeleteOps(ctx context.Context, queueName string, handle *decodedReceiptHandle, rec *sqsMessageRecord, dataKey []byte, readTS uint64) (*kv.OperationGroup[kv.OP], error) {
	visKey := sqsMsgVisKey(queueName, handle.QueueGeneration, rec.VisibleAtMillis, rec.MessageID)
	byAgeKey := sqsMsgByAgeKey(queueName, handle.QueueGeneration, rec.SendTimestampMillis, rec.MessageID)
	readKeys := [][]byte{dataKey, visKey, sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: dataKey},
		{Op: kv.Del, Key: visKey},
		{Op: kv.Del, Key: byAgeKey},
	}
	if rec.MessageGroupId != "" {
		lockKey := sqsMsgGroupKey(queueName, handle.QueueGeneration, rec.MessageGroupId)
		lock, err := s.loadFifoGroupLock(ctx, queueName, handle.QueueGeneration, rec.MessageGroupId, readTS)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.MessageID == rec.MessageID {
			readKeys = append(readKeys, lockKey)
			elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: lockKey})
		}
	}
	return &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: readKeys,
		Elems:    elems,
	}, nil
}

// sqsDeleteOutcome is a ternary tag returned by loadMessageForDelete so
// the caller can cleanly distinguish the AWS-idempotent no-op case from
// the proceed-to-commit case without conflating them with errors.
type sqsDeleteOutcome int

const (
	sqsDeleteProceed sqsDeleteOutcome = iota
	sqsDeleteNoOp
)

// loadMessageForDelete reads the message record and classifies the
// outcome for AWS-compatible DeleteMessage semantics: structural errors
// propagate; missing records and token mismatches on an otherwise-valid
// queue return sqsDeleteNoOp; matching tokens return sqsDeleteProceed
// with the loaded record. The readTS it took the snapshot at is
// returned so the caller can pass it as StartTS on the OCC dispatch,
// pinning the read-write conflict detection window.
//
// The caller-supplied QueueUrl is cross-checked against the handle's
// embedded queue_generation: if the queue does not exist or its current
// generation does not match the handle's generation, the handle refers
// to a different (or recreated) queue and we reject it as a structural
// error — silently succeeding would let misrouted deletes ack messages
// that cannot possibly be deleted on this queue.
func (s *SQSServer) loadMessageForDelete(ctx context.Context, queueName string, handle *decodedReceiptHandle) (*sqsMessageRecord, []byte, uint64, sqsDeleteOutcome, error) {
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return nil, nil, readTS, sqsDeleteProceed, errors.WithStack(err)
	}
	if !exists {
		return nil, nil, readTS, sqsDeleteProceed, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	if meta.Generation != handle.QueueGeneration {
		return nil, nil, readTS, sqsDeleteProceed, newSQSAPIError(http.StatusBadRequest, sqsErrReceiptHandleInvalid, "receipt handle does not belong to this queue")
	}
	dataKey := sqsMsgDataKey(queueName, handle.QueueGeneration, handle.MessageIDHex)
	raw, err := s.store.GetAt(ctx, dataKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil, readTS, sqsDeleteNoOp, nil
		}
		return nil, nil, readTS, sqsDeleteProceed, errors.WithStack(err)
	}
	rec, err := decodeSQSMessageRecord(raw)
	if err != nil {
		return nil, nil, readTS, sqsDeleteProceed, errors.WithStack(err)
	}
	if !bytes.Equal(rec.CurrentReceiptToken, handle.ReceiptToken) {
		return nil, nil, readTS, sqsDeleteNoOp, nil
	}
	return rec, dataKey, readTS, sqsDeleteProceed, nil
}

func (s *SQSServer) changeMessageVisibility(w http.ResponseWriter, r *http.Request) {
	var in sqsChangeVisibilityInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	// AWS requires VisibilityTimeout on ChangeMessageVisibility —
	// omitting it returns MissingParameter, not an implicit 0 (which
	// would unconditionally make the message visible).
	if in.VisibilityTimeout == nil {
		writeSQSError(w, http.StatusBadRequest, sqsErrMissingParameter, "VisibilityTimeout is required")
		return
	}
	timeout := *in.VisibilityTimeout
	if timeout < 0 || timeout > sqsChangeVisibilityMaxSeconds {
		writeSQSError(w, http.StatusBadRequest, sqsErrInvalidAttributeValue, "VisibilityTimeout out of range")
		return
	}
	queueName, handle, err := s.parseQueueAndReceipt(in.QueueUrl, in.ReceiptHandle)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := s.changeVisibilityWithRetry(r.Context(), queueName, handle, timeout); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

// changeVisibilityWithRetry runs the validate-and-swap flow under an OCC
// retry budget. ReadKeys cover the data record and the current vis
// entry; a concurrent receive or delete will bump their commitTS past
// our startTS and we re-validate.
func (s *SQSServer) changeVisibilityWithRetry(ctx context.Context, queueName string, handle *decodedReceiptHandle, newTimeout int64) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		rec, dataKey, readTS, apiErr := s.loadAndVerifyMessage(ctx, queueName, handle)
		if apiErr != nil {
			return apiErr
		}
		now := time.Now().UnixMilli()
		if rec.VisibleAtMillis <= now {
			return newSQSAPIError(http.StatusBadRequest, sqsErrMessageNotInflight, "message is not currently in flight")
		}
		oldVisKey := sqsMsgVisKey(queueName, handle.QueueGeneration, rec.VisibleAtMillis, rec.MessageID)
		rec.VisibleAtMillis = now + newTimeout*sqsMillisPerSecond
		recordBytes, err := encodeSQSMessageRecord(rec)
		if err != nil {
			return errors.WithStack(err)
		}
		newVisKey := sqsMsgVisKey(queueName, handle.QueueGeneration, rec.VisibleAtMillis, rec.MessageID)
		// StartTS pins OCC to the snapshot; without it the coordinator
		// would auto-assign a newer StartTS and a concurrent receive /
		// delete that commits between our load and dispatch could slip
		// past the ReadKeys validation. Meta + generation keys are
		// included so a DeleteQueue race (which only mutates those
		// two keys) also forces this visibility change to abort.
		req := &kv.OperationGroup[kv.OP]{
			IsTxn:    true,
			StartTS:  readTS,
			ReadKeys: [][]byte{dataKey, oldVisKey, sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)},
			Elems: []*kv.Elem[kv.OP]{
				{Op: kv.Del, Key: oldVisKey},
				{Op: kv.Put, Key: newVisKey, Value: []byte(rec.MessageID)},
				{Op: kv.Put, Key: dataKey, Value: recordBytes},
			},
		}
		if _, err := s.coordinator.Dispatch(ctx, req); err == nil {
			return nil
		} else if !isRetryableTransactWriteError(err) {
			return errors.WithStack(err)
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "change visibility retry attempts exhausted")
}

// parseQueueAndReceipt extracts the queue name and decodes the receipt
// handle from a DeleteMessage / ChangeMessageVisibility input.
func (s *SQSServer) parseQueueAndReceipt(queueUrl, receiptHandle string) (string, *decodedReceiptHandle, error) {
	queueName, err := queueNameFromURL(queueUrl)
	if err != nil {
		return "", nil, err
	}
	handle, err := decodeReceiptHandle(receiptHandle)
	if err != nil {
		return "", nil, newSQSAPIError(http.StatusBadRequest, sqsErrReceiptHandleInvalid, "receipt handle is not parseable")
	}
	return queueName, handle, nil
}

// loadAndVerifyMessage reads the data record for the given handle and
// verifies that the receipt token matches the current one on record.
// Returns the record, its key, the snapshot timestamp the read ran at,
// or a typed SQS error. Callers use the snapshot as StartTS on the
// OCC dispatch so concurrent commits cannot slip past ReadKeys.
//
// The caller-supplied QueueUrl is cross-checked against the handle's
// embedded queue_generation, mirroring loadMessageForDelete: an
// existing DeleteQueue leaves orphan message keys until retention
// cleans them up, so a handle from a deleted / recreated queue must
// be rejected with ReceiptHandleIsInvalid instead of silently
// mutating the orphan record.
func (s *SQSServer) loadAndVerifyMessage(ctx context.Context, queueName string, handle *decodedReceiptHandle) (*sqsMessageRecord, []byte, uint64, error) {
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return nil, nil, readTS, errors.WithStack(err)
	}
	if !exists {
		return nil, nil, readTS, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	if meta.Generation != handle.QueueGeneration {
		return nil, nil, readTS, newSQSAPIError(http.StatusBadRequest, sqsErrReceiptHandleInvalid, "receipt handle does not belong to this queue")
	}
	dataKey := sqsMsgDataKey(queueName, handle.QueueGeneration, handle.MessageIDHex)
	raw, err := s.store.GetAt(ctx, dataKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil, readTS, newSQSAPIError(http.StatusBadRequest, sqsErrReceiptHandleInvalid, "message not found")
		}
		return nil, nil, readTS, errors.WithStack(err)
	}
	rec, err := decodeSQSMessageRecord(raw)
	if err != nil {
		return nil, nil, readTS, errors.WithStack(err)
	}
	if !bytes.Equal(rec.CurrentReceiptToken, handle.ReceiptToken) {
		return nil, nil, readTS, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidReceiptHandle, "receipt handle token does not match")
	}
	return rec, dataKey, readTS, nil
}

// ------------------------ small helpers ------------------------

func sqsMD5Hex(body []byte) string {
	sum := md5.Sum(body) //nolint:gosec // AWS-specified ETag hashing, not a crypto primitive.
	return hex.EncodeToString(sum[:])
}

// md5OfAttributesHex computes the AWS-canonical MD5 over a
// MessageAttributes map.
//
// Wire format (binary, hashed in this exact order):
//
//	for each name in sorted(names):
//	  uint32be(len(name))         + name
//	  uint32be(len(dataType))     + dataType
//	  byte(0x01) for String/Number, byte(0x02) for Binary
//	  for String/Number: uint32be(len(stringValue)) + stringValue
//	  for Binary       : uint32be(len(binaryValue)) + binaryValue
//
// AWS SDKs (and `aws sqs` since CLI v2) verify this hash on
// SendMessage / SendMessageBatch responses; a non-matching value makes
// every send call fail with MessageAttributeMD5Mismatch, so the
// algorithm is part of the wire contract, not an implementation
// detail.
// AWS canonical-MD5 wire format constants.
const (
	// sqsAttributeBaseTypeBinary is the canonical name AWS expects for
	// the Binary base type; suffix-extended forms ("Binary.gzipped")
	// share the same type byte.
	sqsAttributeBaseTypeBinary = "Binary"
	// sqsAttributeTransportByteString applies to String and Number;
	// sqsAttributeTransportByteBinary applies to Binary.
	sqsAttributeTransportByteString = byte(0x01)
	sqsAttributeTransportByteBinary = byte(0x02)
)

func md5OfAttributesHex(attrs map[string]sqsMessageAttributeValue) string {
	if len(attrs) == 0 {
		return ""
	}
	names := make([]string, 0, len(attrs))
	for k := range attrs {
		names = append(names, k)
	}
	sort.Strings(names)
	var buf bytes.Buffer
	for _, name := range names {
		v := attrs[name]
		writeMD5Length(&buf, name)
		buf.WriteString(name)
		writeMD5Length(&buf, v.DataType)
		buf.WriteString(v.DataType)
		if attributeTypeIsBinary(v.DataType) {
			buf.WriteByte(sqsAttributeTransportByteBinary)
			writeMD5LengthBytes(&buf, v.BinaryValue)
			buf.Write(v.BinaryValue)
		} else {
			buf.WriteByte(sqsAttributeTransportByteString)
			writeMD5Length(&buf, v.StringValue)
			buf.WriteString(v.StringValue)
		}
	}
	return sqsMD5Hex(buf.Bytes())
}

func writeMD5Length(b *bytes.Buffer, s string) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], safeUint32Len(len(s)))
	b.Write(lenBuf[:])
}

func writeMD5LengthBytes(b *bytes.Buffer, p []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], safeUint32Len(len(p)))
	b.Write(lenBuf[:])
}

// safeUint32Len narrows an int length into a uint32 with an explicit
// gate. AWS's canonical MD5 spec uses a 4-byte length prefix, so any
// payload over 4 GiB is malformed by definition; wrapping the cast
// silently would corrupt the hash, so we clamp to the max value.
func safeUint32Len(n int) uint32 {
	if n < 0 {
		return 0
	}
	const max = int(^uint32(0))
	if n > max {
		return ^uint32(0)
	}
	return uint32(n)
}

// attributeTypeIsBinary returns true when the AWS DataType (which may
// carry a custom suffix after a `.`) declares a Binary payload.
func attributeTypeIsBinary(dataType string) bool {
	base := dataType
	if i := strings.Index(dataType, "."); i >= 0 {
		base = dataType[:i]
	}
	return base == sqsAttributeBaseTypeBinary
}

// validateMessageAttributes enforces the AWS rules a client expects to
// see:
//
//   - DataType must start with String, Number, or Binary; a custom
//     suffix `<Base>.<custom>` is allowed.
//   - String / Number attributes carry a non-empty StringValue; Binary
//     attributes carry a non-empty BinaryValue.
//   - At most 10 message attributes per send call.
//
// Returning the AWS error shape early lets the SDK MD5 verification
// path stay clean: by the time we hash the map we know every entry is
// well-formed.
func validateMessageAttributes(attrs map[string]sqsMessageAttributeValue) error {
	const maxAttrs = 10
	if len(attrs) > maxAttrs {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"MessageAttributes is limited to 10 entries per call")
	}
	for name, v := range attrs {
		if err := validateOneMessageAttribute(name, v); err != nil {
			return err
		}
	}
	return nil
}

func validateOneMessageAttribute(name string, v sqsMessageAttributeValue) error {
	if name == "" {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"MessageAttribute name must be non-empty")
	}
	if v.DataType == "" {
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"MessageAttribute "+name+" missing DataType")
	}
	base := v.DataType
	if i := strings.Index(v.DataType, "."); i >= 0 {
		base = v.DataType[:i]
	}
	switch base {
	case "String", "Number":
		if v.StringValue == "" {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"MessageAttribute "+name+" requires StringValue")
		}
	case sqsAttributeBaseTypeBinary:
		if len(v.BinaryValue) == 0 {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"MessageAttribute "+name+" requires BinaryValue")
		}
	default:
		return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"MessageAttribute "+name+" has unsupported DataType "+v.DataType)
	}
	return nil
}
