package adapter

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

// parsedRedrivePolicy is the in-memory shape of the RedrivePolicy JSON
// blob clients send. AWS allows maxReceiveCount as either a JSON
// number or a string, so the parser handles both.
type parsedRedrivePolicy struct {
	DeadLetterTargetArn string
	DLQName             string
	MaxReceiveCount     int64
}

// rawRedrivePolicy mirrors the AWS JSON shape. maxReceiveCount uses
// json.Number so we can accept both numeric and string forms without
// disagreeing with the SDKs.
type rawRedrivePolicy struct {
	DeadLetterTargetArn string      `json:"deadLetterTargetArn"`
	MaxReceiveCount     json.Number `json:"maxReceiveCount"`
}

// AWS SQS allows maxReceiveCount in [1, 1000].
const (
	sqsRedriveMaxReceiveCountMax = 1000
	sqsRedriveMaxReceiveCountMin = 1
)

// parseRedrivePolicy validates a RedrivePolicy JSON blob and extracts
// the DLQ queue name from the deadLetterTargetArn. ARNs are expected
// to be of the form arn:aws:sqs:<region>:<account>:<name>; we
// tolerate cluster-local synthesized ARNs by treating the segment
// after the last colon as the queue name.
func parseRedrivePolicy(s string) (*parsedRedrivePolicy, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy must be non-empty JSON")
	}
	var raw rawRedrivePolicy
	if err := json.Unmarshal([]byte(s), &raw); err != nil {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy is not valid JSON")
	}
	if raw.DeadLetterTargetArn == "" {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy.deadLetterTargetArn is required")
	}
	maxReceive, err := raw.MaxReceiveCount.Int64()
	if err != nil {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy.maxReceiveCount must be an integer")
	}
	if maxReceive < sqsRedriveMaxReceiveCountMin || maxReceive > sqsRedriveMaxReceiveCountMax {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy.maxReceiveCount must be between 1 and 1000")
	}
	dlqName := dlqNameFromArn(raw.DeadLetterTargetArn)
	if dlqName == "" {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy.deadLetterTargetArn is malformed")
	}
	return &parsedRedrivePolicy{
		DeadLetterTargetArn: raw.DeadLetterTargetArn,
		DLQName:             dlqName,
		MaxReceiveCount:     maxReceive,
	}, nil
}

// dlqNameFromArn returns the queue name segment of an SQS ARN. AWS
// ARNs always have name as the final colon-delimited segment, so a
// last-colon split is correct for both production and test ARNs.
func dlqNameFromArn(arn string) string {
	idx := strings.LastIndex(arn, ":")
	if idx < 0 || idx == len(arn)-1 {
		return ""
	}
	return arn[idx+1:]
}

// shouldRedrive reports whether a candidate's *next* receive would
// trip the redrive policy. Bumping ReceiveCount by 1 first matches
// AWS's "maxReceiveCount is the number of times a message can be
// received before being moved to the DLQ" definition.
func shouldRedrive(rec *sqsMessageRecord, policy *parsedRedrivePolicy) bool {
	if policy == nil {
		return false
	}
	return rec.ReceiveCount+1 > policy.MaxReceiveCount
}

// redriveCandidateToDLQ atomically moves a candidate from the source
// queue to the DLQ inside one OCC transaction. The source's data and
// vis-index entries are deleted; a fresh DLQ message record (with
// reset ReceiveCount and a new receipt token) is written along with
// its visibility entry.
//
// The DeadLetterQueueSourceArn attribute is added so consumers reading
// the DLQ can correlate moved messages back to the originating queue.
//
// On ErrWriteConflict the caller treats this as a skip (another
// receiver may have moved or rotated the same record). Other errors
// propagate so an operational failure does not silently leave the
// poison message in the source queue.
func (s *SQSServer) redriveCandidateToDLQ(
	ctx context.Context,
	srcQueueName string,
	srcMeta *sqsQueueMeta,
	cand sqsMsgCandidate,
	srcDataKey []byte,
	srcRec *sqsMessageRecord,
	policy *parsedRedrivePolicy,
	srcArn string,
	readTS uint64,
) (bool, error) {
	dlqMeta, err := s.validateRedriveTargets(ctx, srcQueueName, srcRec, policy, readTS)
	if err != nil {
		return false, err
	}
	// FIFO DLQs require the redrive write to participate in the
	// per-queue SequenceNumber sequence, otherwise the DLQ record
	// is committed with SequenceNumber=0 (AWS surfaces this
	// verbatim, and 0 violates AWS's invariant that sequences
	// start at 1) and the next normal FIFO send to the DLQ assigns
	// a number lower than the redriven message — non-monotonic to
	// consumers. Load the seq snapshot at readTS, increment, and
	// pass it into both buildDLQRecord (encoded onto the record)
	// and buildRedriveOps (Put + ReadKeys fence).
	var dlqSeq uint64
	if dlqMeta.IsFIFO {
		prevSeq, err := s.loadFifoSequence(ctx, policy.DLQName, readTS)
		if err != nil {
			return false, err
		}
		dlqSeq = prevSeq + 1
	}
	dlqRec, dlqRecordBytes, err := buildDLQRecord(srcRec, dlqMeta, srcArn, dlqSeq)
	if err != nil {
		return false, err
	}
	req, err := s.buildRedriveOps(ctx, srcQueueName, srcMeta, cand, srcDataKey, srcRec, policy, dlqMeta, dlqRec, dlqRecordBytes, dlqSeq, readTS)
	if err != nil {
		return false, err
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		if isRetryableTransactWriteError(err) {
			return true, nil
		}
		return false, errors.WithStack(err)
	}
	return true, nil
}

// validateRedriveTargets enforces every static precondition on the
// (source, DLQ, policy) triple before the OCC dispatch is built.
// Returns the loaded DLQ meta on success so the caller does not have
// to re-load it.
//
// Failure modes (all surfaced as 4xx sqsAPIError):
//   - self-referential RedrivePolicy (defense-in-depth against records
//     that predate the attribute-time validator),
//   - DLQ vanished between policy-set and receive,
//   - source queue vanished mid-redrive (DeleteQueue race),
//   - source/DLQ queue-type mismatch (FIFO ↔ Standard) — AWS forbids
//     this and runtime is the only place it can be enforced because
//     the catalog accepts a RedrivePolicy that names a queue created
//     or recreated later as a different type,
//   - FIFO DLQ paired with a source record lacking MessageGroupId
//     (defense in depth against malformed records that slip past the
//     type-equality check).
func (s *SQSServer) validateRedriveTargets(
	ctx context.Context,
	srcQueueName string,
	srcRec *sqsMessageRecord,
	policy *parsedRedrivePolicy,
	readTS uint64,
) (*sqsQueueMeta, error) {
	if policy.DLQName == srcQueueName {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy.deadLetterTargetArn must not point at the source queue")
	}
	dlqMeta, dlqExists, err := s.loadQueueMetaAt(ctx, policy.DLQName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !dlqExists {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy targets non-existent DLQ "+policy.DLQName)
	}
	srcMeta, srcExists, err := s.loadQueueMetaAt(ctx, srcQueueName, readTS)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !srcExists {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist,
			"source queue disappeared during redrive")
	}
	if srcMeta.IsFIFO != dlqMeta.IsFIFO {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy queue-type mismatch: source and DLQ must both be FIFO or both Standard")
	}
	if dlqMeta.IsFIFO && srcRec.MessageGroupId == "" {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"FIFO DLQ requires source records to carry MessageGroupId")
	}
	return dlqMeta, nil
}

// buildDLQRecord assembles the DLQ-side message record. Reset
// ReceiveCount and FirstReceiveMillis so the DLQ consumer sees a
// fresh delivery, not the source's bounce history.
//
// dlqSeq is the SequenceNumber to assign on the DLQ record, computed
// by the caller as `loadFifoSequence(dlq) + 1` for FIFO DLQs and
// passed as 0 for Standard DLQs (the field is unused in that case).
// The seq must be the same value the caller will Put into the DLQ's
// sqsQueueSeqKey inside the same OCC transaction (see buildRedriveOps);
// otherwise the redriven message and the on-disk counter disagree
// and a later FIFO send to the DLQ produces a non-monotonic
// SequenceNumber.
func buildDLQRecord(srcRec *sqsMessageRecord, dlqMeta *sqsQueueMeta, srcArn string, dlqSeq uint64) (*sqsMessageRecord, []byte, error) {
	dlqMsgID, err := newMessageIDHex()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	dlqToken, err := newReceiptToken()
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	now := time.Now().UnixMilli()
	rec := &sqsMessageRecord{
		MessageID:              dlqMsgID,
		Body:                   srcRec.Body,
		MD5OfBody:              srcRec.MD5OfBody,
		MD5OfMessageAttributes: srcRec.MD5OfMessageAttributes,
		MessageAttributes:      srcRec.MessageAttributes,
		SenderID:               srcRec.SenderID,
		SendTimestampMillis:    now,
		AvailableAtMillis:      now,
		VisibleAtMillis:        now,
		ReceiveCount:           0,
		FirstReceiveMillis:     0,
		CurrentReceiptToken:    dlqToken,
		QueueGeneration:        dlqMeta.Generation,
		MessageGroupId:         srcRec.MessageGroupId,
		MessageDeduplicationId: srcRec.MessageDeduplicationId,
		DeadLetterSourceArn:    srcArn,
		SequenceNumber:         dlqSeq,
	}
	body, err := encodeSQSMessageRecord(rec)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	return rec, body, nil
}

// buildRedriveOps assembles the cross-queue OCC OperationGroup that
// atomically removes the source's keyspace and writes the DLQ
// version. The FIFO group-lock release branch lives here so the
// caller stays under the cyclomatic budget.
//
// dlqSeq is non-zero only when the DLQ is FIFO (per the caller's
// pre-load via loadFifoSequence). When non-zero, the txn additionally
// reads sqsQueueSeqKey(policy.DLQName) — guarding against a
// concurrent FIFO send / redrive racing for the same sequence — and
// writes the new value back. dlqRec.SequenceNumber is already set to
// dlqSeq inside buildDLQRecord; this function is responsible only for
// the OCC plumbing.
func (s *SQSServer) buildRedriveOps(
	ctx context.Context,
	srcQueueName string,
	srcMeta *sqsQueueMeta,
	cand sqsMsgCandidate,
	srcDataKey []byte,
	srcRec *sqsMessageRecord,
	policy *parsedRedrivePolicy,
	dlqMeta *sqsQueueMeta,
	dlqRec *sqsMessageRecord,
	dlqRecordBytes []byte,
	dlqSeq uint64,
	readTS uint64,
) (*kv.OperationGroup[kv.OP], error) {
	srcGen := srcMeta.Generation
	now := dlqRec.SendTimestampMillis
	// DLQ partition for FIFO sources: redrive carries the source's
	// MessageGroupId forward, so the DLQ partition is the result of
	// hashing that group through the DLQ's partitionFor. Standard
	// DLQs (or any DLQ with PartitionCount <= 1) collapse this to 0.
	dlqPartition := partitionFor(dlqMeta, dlqRec.MessageGroupId)
	dlqDataKey := sqsMsgDataKeyDispatch(dlqMeta, policy.DLQName, dlqPartition, dlqMeta.Generation, dlqRec.MessageID)
	dlqVisKey := sqsMsgVisKeyDispatch(dlqMeta, policy.DLQName, dlqPartition, dlqMeta.Generation, now, dlqRec.MessageID)
	dlqByAgeKey := sqsMsgByAgeKeyDispatch(dlqMeta, policy.DLQName, dlqPartition, dlqMeta.Generation, now, dlqRec.MessageID)
	srcByAgeKey := sqsMsgByAgeKeyDispatch(srcMeta, srcQueueName, cand.partition, srcGen, srcRec.SendTimestampMillis, srcRec.MessageID)
	readKeys := [][]byte{
		cand.visKey, srcDataKey,
		sqsQueueMetaKey(srcQueueName), sqsQueueGenKey(srcQueueName),
		sqsQueueMetaKey(policy.DLQName), sqsQueueGenKey(policy.DLQName),
	}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: cand.visKey},
		{Op: kv.Del, Key: srcDataKey},
		{Op: kv.Del, Key: srcByAgeKey},
		{Op: kv.Put, Key: dlqDataKey, Value: dlqRecordBytes},
		{Op: kv.Put, Key: dlqVisKey, Value: []byte(dlqRec.MessageID)},
		{Op: kv.Put, Key: dlqByAgeKey, Value: []byte(dlqRec.MessageID)},
	}
	if dlqMeta.IsFIFO {
		seqKey := sqsQueueSeqKey(policy.DLQName)
		readKeys = append(readKeys, seqKey)
		elems = append(elems, &kv.Elem[kv.OP]{
			Op:    kv.Put,
			Key:   seqKey,
			Value: []byte(strconv.FormatUint(dlqSeq, 10)),
		})
	}
	if srcRec.MessageGroupId != "" {
		lockKey := sqsMsgGroupKeyDispatch(srcMeta, srcQueueName, cand.partition, srcGen, srcRec.MessageGroupId)
		lock, err := s.loadFifoGroupLock(ctx, srcQueueName, srcMeta, cand.partition, srcGen, srcRec.MessageGroupId, readTS)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.MessageID == srcRec.MessageID {
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
