package adapter

import (
	"context"
	"net/http"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

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
	total := 0
	for _, e := range in.Entries {
		total += len(e.MessageBody)
	}
	if total > sqsBatchMaxTotalPayloadBytes {
		writeSQSError(w, http.StatusBadRequest, sqsErrBatchRequestTooLong,
			"total batch payload exceeds 262144 bytes")
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
