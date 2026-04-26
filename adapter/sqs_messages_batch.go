package adapter

import (
	"context"
	"net/http"
	"regexp"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// sqsBatchEntryIdPattern is AWS's allowed character set for the
// per-entry Id of any batch operation: 1-80 chars, alphanumeric
// plus `-` and `_`. Anything else returns InvalidBatchEntryId.
var sqsBatchEntryIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,80}$`)

// AWS-documented per-batch limits.
const (
	sqsBatchMaxEntries = 10
	// sqsBatchMaxTotalPayloadBytes mirrors AWS's 256 KiB total cap on
	// SendMessageBatch (the cap is on the sum of message bodies, not
	// the encoded request). Enforcing it adapter-side keeps a noisy
	// producer from blowing past MaximumMessageSize by spreading a big
	// payload across many entries.
	sqsBatchMaxTotalPayloadBytes = 262144
)

// AWS error codes specific to batch operations.
const (
	sqsErrEmptyBatchRequest            = "AWS.SimpleQueueService.EmptyBatchRequest"
	sqsErrBatchEntryIdsNotDistinct     = "AWS.SimpleQueueService.BatchEntryIdsNotDistinct"
	sqsErrTooManyEntriesInBatchRequest = "AWS.SimpleQueueService.TooManyEntriesInBatchRequest"
	sqsErrInvalidBatchEntryId          = "AWS.SimpleQueueService.InvalidBatchEntryId"
	sqsErrBatchRequestTooLong          = "AWS.SimpleQueueService.BatchRequestTooLong"
)

// ------------------------ SendMessageBatch ------------------------

type sqsSendMessageBatchInput struct {
	QueueUrl string                          `json:"QueueUrl"`
	Entries  []sqsSendMessageBatchEntryInput `json:"Entries"`
}

type sqsSendMessageBatchEntryInput struct {
	Id                     string                              `json:"Id"`
	MessageBody            string                              `json:"MessageBody"`
	DelaySeconds           *int64                              `json:"DelaySeconds,omitempty"`
	MessageAttributes      map[string]sqsMessageAttributeValue `json:"MessageAttributes,omitempty"`
	MessageGroupId         string                              `json:"MessageGroupId,omitempty"`
	MessageDeduplicationId string                              `json:"MessageDeduplicationId,omitempty"`
}

type sqsBatchResultErrorEntry struct {
	Id          string `json:"Id"`
	Code        string `json:"Code"`
	Message     string `json:"Message"`
	SenderFault bool   `json:"SenderFault"`
}

type sqsSendMessageBatchResultEntry struct {
	Id                     string `json:"Id"`
	MessageId              string `json:"MessageId"`
	MD5OfMessageBody       string `json:"MD5OfMessageBody"`
	MD5OfMessageAttributes string `json:"MD5OfMessageAttributes,omitempty"`
	// SequenceNumber is non-empty only on FIFO queues, matching AWS's
	// shape. Standard-queue sends omit the field.
	SequenceNumber string `json:"SequenceNumber,omitempty"`
}

func (s *SQSServer) sendMessageBatch(w http.ResponseWriter, r *http.Request) {
	var in sqsSendMessageBatchInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	queueName, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := validateBatchEntryShape(len(in.Entries), batchEntryIDs(in.Entries)); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	// Total-payload-size gate is request-level, not per-entry: silently
	// accepting an oversized batch would let one producer push tens of
	// MiB through a single call and DoS the leader's Raft pipeline.
	// MessageAttributes contribute to the size — without them in the
	// total a client could ship tiny bodies plus a few-MiB BinaryValue
	// per entry and bypass the cap.
	if total := totalBatchPayloadBytes(in.Entries); total > sqsBatchMaxTotalPayloadBytes {
		writeSQSError(w, http.StatusBadRequest, sqsErrBatchRequestTooLong,
			"total batch payload exceeds 262144 bytes")
		return
	}
	if !s.chargeQueue(w, r, queueName, bucketActionSend, throttleChargeCount(len(in.Entries))) {
		return
	}

	successful, failed, err := s.sendMessageBatchWithRetry(r.Context(), queueName, in.Entries)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	resp := map[string]any{
		"Successful": successful,
		"Failed":     failed,
	}
	writeSQSJSON(w, resp)
}

// sendMessageBatchWithRetry pre-validates every entry, splits them into
// "will-attempt" and "rejected before storage", and runs one OCC
// transaction over the will-attempt set. On ErrWriteConflict the whole
// transaction (and the validation pass that fed it, since the OCC
// snapshot is shared) is retried — that way a concurrent DeleteQueue
// or PurgeQueue is observed before we re-commit.
func (s *SQSServer) sendMessageBatchWithRetry(
	ctx context.Context,
	queueName string,
	entries []sqsSendMessageBatchEntryInput,
) ([]sqsSendMessageBatchResultEntry, []sqsBatchResultErrorEntry, error) {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		successful, failed, retry, err := s.trySendMessageBatchOnce(ctx, queueName, entries)
		if err != nil {
			return nil, nil, err
		}
		if !retry {
			return successful, failed, nil
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return nil, nil, errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return nil, nil, newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "send message batch retry attempts exhausted")
}

// trySendMessageBatchOnce runs one snapshot read + per-entry validate +
// dispatch pass. retry=true means OCC saw a write conflict and the
// caller should re-run; retry=false means we have a final response.
//
// FIFO queues take a slow per-entry path because the dedup record and
// per-queue sequence counter both have to be inspected and mutated
// inside the same OCC transaction as the data write — bundling all
// entries into a single batch transaction would either skip the
// dedup check (allowing duplicate-id sends to land twice in the
// queue) or assign the same sequence number to every entry, both of
// which violate AWS's FIFO contract.
func (s *SQSServer) trySendMessageBatchOnce(
	ctx context.Context,
	queueName string,
	entries []sqsSendMessageBatchEntryInput,
) ([]sqsSendMessageBatchResultEntry, []sqsBatchResultErrorEntry, bool, error) {
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return nil, nil, false, errors.WithStack(err)
	}
	if !exists {
		return nil, nil, false, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	if meta.IsFIFO {
		return s.sendBatchFifoEntries(ctx, queueName, meta, entries)
	}
	return s.sendBatchStandardOnce(ctx, queueName, meta, entries, readTS)
}

// sendBatchStandardOnce is the original single-OCC fast path for
// Standard queues: every entry that survives validation is bundled
// into one Dispatch.
func (s *SQSServer) sendBatchStandardOnce(
	ctx context.Context,
	queueName string,
	meta *sqsQueueMeta,
	entries []sqsSendMessageBatchEntryInput,
	readTS uint64,
) ([]sqsSendMessageBatchResultEntry, []sqsBatchResultErrorEntry, bool, error) {
	successful := make([]sqsSendMessageBatchResultEntry, 0, len(entries))
	failed := make([]sqsBatchResultErrorEntry, 0)
	// Each entry produces three OCC ops: data, vis, byage. Pre-sizing
	// the slice avoids a couple of grow operations in the batch hot
	// path; oversizing is fine, undersizing is what we are gating
	// against.
	const opsPerEntry = 3
	elems := make([]*kv.Elem[kv.OP], 0, opsPerEntry*len(entries))
	for _, entry := range entries {
		rec, recordBytes, apiErr := buildBatchSendRecord(meta, entry)
		if apiErr != nil {
			failed = append(failed, batchErrorEntryFromAPIErr(entry.Id, apiErr))
			continue
		}
		dataKey := sqsMsgDataKey(queueName, meta.Generation, rec.MessageID)
		visKey := sqsMsgVisKey(queueName, meta.Generation, rec.AvailableAtMillis, rec.MessageID)
		byAgeKey := sqsMsgByAgeKey(queueName, meta.Generation, rec.SendTimestampMillis, rec.MessageID)
		elems = append(elems,
			&kv.Elem[kv.OP]{Op: kv.Put, Key: dataKey, Value: recordBytes},
			&kv.Elem[kv.OP]{Op: kv.Put, Key: visKey, Value: []byte(rec.MessageID)},
			&kv.Elem[kv.OP]{Op: kv.Put, Key: byAgeKey, Value: []byte(rec.MessageID)},
		)
		successful = append(successful, sqsSendMessageBatchResultEntry{
			Id:                     entry.Id,
			MessageId:              rec.MessageID,
			MD5OfMessageBody:       rec.MD5OfBody,
			MD5OfMessageAttributes: md5OfAttributesHex(entry.MessageAttributes),
		})
	}
	if len(elems) == 0 {
		// Every entry was rejected before storage; nothing to commit.
		return successful, failed, false, nil
	}
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)},
		Elems:    elems,
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		if isRetryableTransactWriteError(err) {
			return nil, nil, true, nil
		}
		return nil, nil, false, errors.WithStack(err)
	}
	return successful, failed, false, nil
}

// sendBatchFifoEntries dispatches FIFO batch entries one at a time
// through the same per-message OCC path used by single-message FIFO
// sends. Per-entry isolation lets us:
//
//   - read and bump the per-queue sequence counter once per entry,
//     handing each successful send a strictly-increasing
//     SequenceNumber;
//   - check + write the dedup record per entry, so a batch that
//     repeats the same MessageDeduplicationId behaves the same as
//     two single sends with the same id (idempotent);
//   - report per-entry failures (validation, FIFO param errors,
//     OCC conflicts that exceed the inner retry budget) without
//     poisoning successful entries.
//
// We never need the outer batch retry loop here because each entry
// already carries its own retry budget through sendMessageFifoLoop's
// counterpart, sendFifoMessage's reply contract.
func (s *SQSServer) sendBatchFifoEntries(
	ctx context.Context,
	queueName string,
	meta *sqsQueueMeta,
	entries []sqsSendMessageBatchEntryInput,
) ([]sqsSendMessageBatchResultEntry, []sqsBatchResultErrorEntry, bool, error) {
	successful := make([]sqsSendMessageBatchResultEntry, 0, len(entries))
	failed := make([]sqsBatchResultErrorEntry, 0)
	for _, entry := range entries {
		ok, success, errEntry := s.sendOneFifoBatchEntry(ctx, queueName, meta, entry)
		if !ok {
			failed = append(failed, errEntry)
			continue
		}
		successful = append(successful, success)
	}
	// Per-entry retries already happened inside sendOneFifoBatchEntry;
	// we never ask the outer batch loop to retry the whole pass.
	return successful, failed, false, nil
}

// sendOneFifoBatchEntry validates a single FIFO batch entry and runs
// the dedup-aware OCC send under its own retry budget. Returns
// ok=true with the success payload on a successful send (including
// dedup hits, which AWS reports as success); ok=false with a populated
// error entry otherwise.
func (s *SQSServer) sendOneFifoBatchEntry(
	ctx context.Context,
	queueName string,
	_ *sqsQueueMeta,
	entry sqsSendMessageBatchEntryInput,
) (bool, sqsSendMessageBatchResultEntry, sqsBatchResultErrorEntry) {
	// Only meta-independent shape checks live here. Anything that
	// reads queue metadata (MaximumMessageSize, FIFO flag, content-
	// based dedup) is re-evaluated inside runFifoSendWithRetry per
	// attempt, so a SetQueueAttributes commit racing this batch
	// cannot fail an entry that the per-attempt path would accept.
	if apiErr := validateMessageAttributes(entry.MessageAttributes); apiErr != nil {
		return false, sqsSendMessageBatchResultEntry{}, batchErrorEntryFromAPIErr(entry.Id, apiErr)
	}
	if len(entry.MessageBody) == 0 {
		return false, sqsSendMessageBatchResultEntry{}, batchErrorEntryFromAPIErr(entry.Id,
			newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "MessageBody is required"))
	}
	asSingle := sqsSendMessageInput{
		MessageBody:            entry.MessageBody,
		DelaySeconds:           entry.DelaySeconds,
		MessageAttributes:      entry.MessageAttributes,
		MessageGroupId:         entry.MessageGroupId,
		MessageDeduplicationId: entry.MessageDeduplicationId,
	}

	resp, err := s.runFifoSendWithRetry(ctx, queueName, asSingle)
	if err != nil {
		return false, sqsSendMessageBatchResultEntry{}, batchErrorEntryFromAPIErr(entry.Id, err)
	}
	return true, sqsSendMessageBatchResultEntry{
		Id:                     entry.Id,
		MessageId:              resp["MessageId"],
		MD5OfMessageBody:       resp["MD5OfMessageBody"],
		MD5OfMessageAttributes: resp["MD5OfMessageAttributes"],
		SequenceNumber:         resp["SequenceNumber"],
	}, sqsBatchResultErrorEntry{}
}

// runFifoSendWithRetry is the entry-loop counterpart of
// sendMessageFifoLoop. It exists separately so the batch path can
// surface per-entry errors as Failed[] entries rather than as a
// whole-call failure.
//
// Each attempt — including the first — re-loads queue metadata at the
// same readTS used for the OCC dispatch *and* re-derives the FIFO
// dedup id and effective delay from that fresh meta. Without that,
// attempt N would pair a fresh readTS with stale FIFO rules — if
// SetQueueAttributes flipped ContentBasedDeduplication or rotated
// DelaySeconds between the original meta read and the chosen retry
// snapshot, the send could commit with the previous generation's
// rules. Re-deriving per attempt guarantees the (meta, readTS,
// dedupID, delay) tuple is coherent.
func (s *SQSServer) runFifoSendWithRetry(
	ctx context.Context,
	queueName string,
	in sqsSendMessageInput,
) (map[string]string, error) {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		readTS := s.nextTxnReadTS(ctx)
		meta, dedupID, delay, err := s.resolveFreshFifoSnapshot(ctx, queueName, in, readTS)
		if err != nil {
			return nil, err
		}
		resp, retry, err := s.sendFifoMessage(ctx, queueName, meta, in, dedupID, delay, readTS)
		if err != nil {
			return nil, err
		}
		if !retry {
			return resp, nil
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return nil, errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return nil, newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "FIFO send retry attempts exhausted")
}

// resolveFreshFifoSnapshot loads queue meta at readTS and re-derives
// every meta-dependent value (size cap, FIFO params, dedup id,
// effective delay). Pulled out of runFifoSendWithRetry so the retry
// loop stays under the cyclomatic budget.
func (s *SQSServer) resolveFreshFifoSnapshot(ctx context.Context, queueName string, in sqsSendMessageInput, readTS uint64) (*sqsQueueMeta, string, int64, error) {
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return nil, "", 0, err
	}
	if !exists {
		return nil, "", 0, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	if int64(len(in.MessageBody)) > meta.MaximumMessageSize {
		return nil, "", 0, newSQSAPIError(http.StatusBadRequest, sqsErrMessageTooLong, "message body exceeds MaximumMessageSize")
	}
	if err := validateSendFIFOParams(meta, in); err != nil {
		return nil, "", 0, err
	}
	dedupID := resolveFifoDedupID(meta, in)
	if dedupID == "" {
		return nil, "", 0, newSQSAPIError(http.StatusBadRequest, sqsErrMissingParameter,
			"FIFO send requires MessageDeduplicationId or ContentBasedDeduplication=true")
	}
	delay, err := resolveSendDelay(meta, in.DelaySeconds)
	if err != nil {
		return nil, "", 0, err
	}
	return meta, dedupID, delay, nil
}

// buildBatchSendRecord runs every per-entry validation a single
// SendMessage would, but returns the *sqsAPIError so the batch path
// can drop the entry into Failed[] instead of failing the whole
// request.
func buildBatchSendRecord(meta *sqsQueueMeta, entry sqsSendMessageBatchEntryInput) (*sqsMessageRecord, []byte, error) {
	if len(entry.MessageBody) == 0 {
		return nil, nil, newSQSAPIError(http.StatusBadRequest, sqsErrValidation, "MessageBody is required")
	}
	if int64(len(entry.MessageBody)) > meta.MaximumMessageSize {
		return nil, nil, newSQSAPIError(http.StatusBadRequest, sqsErrMessageTooLong, "message body exceeds MaximumMessageSize")
	}
	if err := validateMessageAttributes(entry.MessageAttributes); err != nil {
		return nil, nil, err
	}
	asSingle := sqsSendMessageInput{
		MessageBody:            entry.MessageBody,
		DelaySeconds:           entry.DelaySeconds,
		MessageAttributes:      entry.MessageAttributes,
		MessageGroupId:         entry.MessageGroupId,
		MessageDeduplicationId: entry.MessageDeduplicationId,
	}
	if err := validateSendFIFOParams(meta, asSingle); err != nil {
		return nil, nil, err
	}
	delay, err := resolveSendDelay(meta, entry.DelaySeconds)
	if err != nil {
		return nil, nil, err
	}
	return buildSendRecord(meta, asSingle, delay)
}

// ------------------------ DeleteMessageBatch ------------------------

type sqsDeleteMessageBatchInput struct {
	QueueUrl string                            `json:"QueueUrl"`
	Entries  []sqsDeleteMessageBatchEntryInput `json:"Entries"`
}

type sqsDeleteMessageBatchEntryInput struct {
	Id            string `json:"Id"`
	ReceiptHandle string `json:"ReceiptHandle"`
}

type sqsBatchResultEntry struct {
	Id string `json:"Id"`
}

func (s *SQSServer) deleteMessageBatch(w http.ResponseWriter, r *http.Request) {
	var in sqsDeleteMessageBatchInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	queueName, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	ids := make([]string, 0, len(in.Entries))
	for _, e := range in.Entries {
		ids = append(ids, e.Id)
	}
	if err := validateBatchEntryShape(len(in.Entries), ids); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := s.requireQueueExists(r.Context(), queueName); err != nil {
		// AWS classes a missing queue as a request-level error
		// (HTTP 400 QueueDoesNotExist) on batch APIs, not a per-
		// entry failure inside an HTTP-200 envelope. Returning per-
		// entry would let SDK retry logic mistake a hard queue-level
		// failure for a partial-success batch and keep retrying.
		writeSQSErrorFromErr(w, err)
		return
	}
	if !s.chargeQueue(w, r, queueName, bucketActionReceive, throttleChargeCount(len(in.Entries))) {
		return
	}

	successful := make([]sqsBatchResultEntry, 0, len(in.Entries))
	failed := make([]sqsBatchResultErrorEntry, 0)
	for _, entry := range in.Entries {
		// Each entry decodes its own handle and runs through the same
		// retry-bound stale-is-success delete that single DeleteMessage
		// uses. Per-entry isolation matches AWS, where a malformed
		// handle in slot 3 must not poison slot 4.
		handle, decodeErr := decodeReceiptHandle(entry.ReceiptHandle)
		if decodeErr != nil {
			failed = append(failed, sqsBatchResultErrorEntry{
				Id:          entry.Id,
				Code:        sqsErrReceiptHandleInvalid,
				Message:     "receipt handle is not parseable",
				SenderFault: true,
			})
			continue
		}
		if err := s.deleteMessageWithRetry(r.Context(), queueName, handle); err != nil {
			failed = append(failed, batchErrorEntryFromErr(entry.Id, err))
			continue
		}
		successful = append(successful, sqsBatchResultEntry{Id: entry.Id})
	}
	writeSQSJSON(w, map[string]any{
		"Successful": successful,
		"Failed":     failed,
	})
}

// ------------------------ ChangeMessageVisibilityBatch ------------------------

type sqsChangeVisBatchInput struct {
	QueueUrl string                        `json:"QueueUrl"`
	Entries  []sqsChangeVisBatchEntryInput `json:"Entries"`
}

type sqsChangeVisBatchEntryInput struct {
	Id                string `json:"Id"`
	ReceiptHandle     string `json:"ReceiptHandle"`
	VisibilityTimeout *int64 `json:"VisibilityTimeout"`
}

func (s *SQSServer) changeMessageVisibilityBatch(w http.ResponseWriter, r *http.Request) {
	var in sqsChangeVisBatchInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	queueName, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	ids := make([]string, 0, len(in.Entries))
	for _, e := range in.Entries {
		ids = append(ids, e.Id)
	}
	if err := validateBatchEntryShape(len(in.Entries), ids); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := s.requireQueueExists(r.Context(), queueName); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if !s.chargeQueue(w, r, queueName, bucketActionReceive, throttleChargeCount(len(in.Entries))) {
		return
	}

	successful := make([]sqsBatchResultEntry, 0, len(in.Entries))
	failed := make([]sqsBatchResultErrorEntry, 0)
	for _, entry := range in.Entries {
		ok, errEntry := s.applyChangeVisibilityBatchEntry(r.Context(), queueName, entry)
		if !ok {
			failed = append(failed, errEntry)
			continue
		}
		successful = append(successful, sqsBatchResultEntry{Id: entry.Id})
	}
	writeSQSJSON(w, map[string]any{
		"Successful": successful,
		"Failed":     failed,
	})
}

// applyChangeVisibilityBatchEntry runs the per-entry validate-and-commit
// flow for a single ChangeMessageVisibilityBatch entry. Returns false
// with a populated error entry when validation or the OCC commit fails;
// returns true when the change was applied.
func (s *SQSServer) applyChangeVisibilityBatchEntry(ctx context.Context, queueName string, entry sqsChangeVisBatchEntryInput) (bool, sqsBatchResultErrorEntry) {
	if entry.VisibilityTimeout == nil {
		return false, sqsBatchResultErrorEntry{
			Id:          entry.Id,
			Code:        sqsErrMissingParameter,
			Message:     "VisibilityTimeout is required",
			SenderFault: true,
		}
	}
	timeout := *entry.VisibilityTimeout
	if timeout < 0 || timeout > sqsChangeVisibilityMaxSeconds {
		return false, sqsBatchResultErrorEntry{
			Id:          entry.Id,
			Code:        sqsErrInvalidAttributeValue,
			Message:     "VisibilityTimeout out of range",
			SenderFault: true,
		}
	}
	handle, decodeErr := decodeReceiptHandle(entry.ReceiptHandle)
	if decodeErr != nil {
		return false, sqsBatchResultErrorEntry{
			Id:          entry.Id,
			Code:        sqsErrReceiptHandleInvalid,
			Message:     "receipt handle is not parseable",
			SenderFault: true,
		}
	}
	if err := s.changeVisibilityWithRetry(ctx, queueName, handle, timeout); err != nil {
		return false, batchErrorEntryFromErr(entry.Id, err)
	}
	return true, sqsBatchResultErrorEntry{}
}

// ------------------------ batch helpers ------------------------

// validateBatchEntryShape enforces the request-level invariants that AWS
// applies before any per-entry processing: at least one entry, no more
// than 10, and unique non-empty Ids. These are different error codes
// from per-entry InvalidParameterValue, so callers can distinguish a
// malformed request from a partial-failure response.
func validateBatchEntryShape(count int, ids []string) error {
	if count == 0 {
		return newSQSAPIError(http.StatusBadRequest, sqsErrEmptyBatchRequest, "Entries is required and non-empty")
	}
	if count > sqsBatchMaxEntries {
		return newSQSAPIError(http.StatusBadRequest, sqsErrTooManyEntriesInBatchRequest,
			"a batch request supports up to 10 entries")
	}
	seen := make(map[string]bool, count)
	for _, id := range ids {
		if id == "" {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidBatchEntryId,
				"every batch entry requires a non-empty Id")
		}
		// AWS limits batch entry Ids to 1-80 alphanumeric + `-` / `_`.
		// Without this check, malformed Ids (e.g. arbitrary user
		// strings, whitespace, multi-byte unicode) would pass through
		// to per-entry processing instead of returning the documented
		// InvalidBatchEntryId error.
		if !sqsBatchEntryIDPattern.MatchString(id) {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidBatchEntryId,
				"batch entry Id must be 1-80 chars of alphanumeric, hyphen, or underscore")
		}
		if seen[id] {
			return newSQSAPIError(http.StatusBadRequest, sqsErrBatchEntryIdsNotDistinct,
				"batch entry Ids must be distinct")
		}
		seen[id] = true
	}
	return nil
}

func batchEntryIDs(entries []sqsSendMessageBatchEntryInput) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Id)
	}
	return out
}

// requireQueueExists returns a request-level QueueDoesNotExist error
// when the queue's meta record is gone. Batch DeleteMessage /
// ChangeMessageVisibility use this as an upfront gate so callers see
// the documented top-level error, not per-entry failures inside a
// 200-envelope that retry logic can misclassify.
func (s *SQSServer) requireQueueExists(ctx context.Context, queueName string) error {
	_, exists, err := s.loadQueueMetaAt(ctx, queueName, s.nextTxnReadTS(ctx))
	if err != nil {
		return errors.WithStack(err)
	}
	if !exists {
		return newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	return nil
}

// totalBatchPayloadBytes sums the message-body length and every
// MessageAttribute (name + DataType + value) length across a batch.
// Both fields count toward AWS's 256 KiB request cap; counting only
// MessageBody would let a client stuff several MiB into a single
// BinaryValue while passing the size gate.
func totalBatchPayloadBytes(entries []sqsSendMessageBatchEntryInput) int {
	total := 0
	for _, e := range entries {
		total += len(e.MessageBody)
		for name, v := range e.MessageAttributes {
			total += len(name) + len(v.DataType) + len(v.StringValue) + len(v.BinaryValue)
		}
	}
	return total
}

func batchErrorEntryFromAPIErr(id string, err error) sqsBatchResultErrorEntry {
	var apiErr *sqsAPIError
	if errors.As(err, &apiErr) {
		return sqsBatchResultErrorEntry{
			Id:          id,
			Code:        apiErr.errorType,
			Message:     apiErr.message,
			SenderFault: apiErr.status >= 400 && apiErr.status < 500,
		}
	}
	return sqsBatchResultErrorEntry{
		Id:          id,
		Code:        sqsErrInternalFailure,
		Message:     "internal error",
		SenderFault: false,
	}
}

// batchErrorEntryFromErr is the per-entry counterpart for paths that
// already use an error result type — DeleteMessage / ChangeMessageVisibility
// can return either an *sqsAPIError or a wrapped store error, and we want
// the same body shape either way.
func batchErrorEntryFromErr(id string, err error) sqsBatchResultErrorEntry {
	return batchErrorEntryFromAPIErr(id, err)
}
