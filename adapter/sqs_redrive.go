package adapter

import (
	"context"
	"net/http"
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
	srcGen uint64,
	cand sqsMsgCandidate,
	srcDataKey []byte,
	srcRec *sqsMessageRecord,
	policy *parsedRedrivePolicy,
	srcArn string,
	readTS uint64,
) (bool, error) {
	// Self-referential RedrivePolicy is rejected at attribute-apply
	// time, but a defense-in-depth check here keeps the receive path
	// safe even against records committed before the validator
	// existed (or under a future relaxation of the validator).
	// Without this, redrive would delete and rewrite the same record
	// in place, looping the poison message forever.
	if policy.DLQName == srcQueueName {
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy.deadLetterTargetArn must not point at the source queue")
	}
	dlqMeta, exists, err := s.loadQueueMetaAt(ctx, policy.DLQName, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !exists {
		// DLQ vanished between policy-set and receive. Refusing the
		// move keeps the source message in flight; the operator can
		// recreate the DLQ or detach the policy.
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"RedrivePolicy targets non-existent DLQ "+policy.DLQName)
	}
	// FIFO DLQs require source records to carry MessageGroupId so the
	// DLQ-side ReceiveMessage enforces group-lock semantics. Without
	// this gate, a redrive from a Standard source into a FIFO DLQ
	// would write records with empty MessageGroupId — tryDeliverCandidate
	// only takes the FIFO path when MessageGroupId is non-empty, so
	// those messages would bypass FIFO ordering inside what clients
	// believe is a strictly-ordered queue. We refuse the move and
	// surface the misconfiguration to operators.
	if dlqMeta.IsFIFO && srcRec.MessageGroupId == "" {
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
			"FIFO DLQ requires source records to carry MessageGroupId")
	}
	dlqRec, dlqRecordBytes, err := buildDLQRecord(srcRec, dlqMeta, srcArn)
	if err != nil {
		return false, err
	}
	req, err := s.buildRedriveOps(ctx, srcQueueName, srcGen, cand, srcDataKey, srcRec, policy, dlqMeta, dlqRec, dlqRecordBytes, readTS)
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

// buildDLQRecord assembles the DLQ-side message record. Reset
// ReceiveCount and FirstReceiveMillis so the DLQ consumer sees a
// fresh delivery, not the source's bounce history.
func buildDLQRecord(srcRec *sqsMessageRecord, dlqMeta *sqsQueueMeta, srcArn string) (*sqsMessageRecord, []byte, error) {
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
func (s *SQSServer) buildRedriveOps(
	ctx context.Context,
	srcQueueName string,
	srcGen uint64,
	cand sqsMsgCandidate,
	srcDataKey []byte,
	srcRec *sqsMessageRecord,
	policy *parsedRedrivePolicy,
	dlqMeta *sqsQueueMeta,
	dlqRec *sqsMessageRecord,
	dlqRecordBytes []byte,
	readTS uint64,
) (*kv.OperationGroup[kv.OP], error) {
	now := dlqRec.SendTimestampMillis
	dlqDataKey := sqsMsgDataKey(policy.DLQName, dlqMeta.Generation, dlqRec.MessageID)
	dlqVisKey := sqsMsgVisKey(policy.DLQName, dlqMeta.Generation, now, dlqRec.MessageID)
	dlqByAgeKey := sqsMsgByAgeKey(policy.DLQName, dlqMeta.Generation, now, dlqRec.MessageID)
	srcByAgeKey := sqsMsgByAgeKey(srcQueueName, srcGen, srcRec.SendTimestampMillis, srcRec.MessageID)
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
	if srcRec.MessageGroupId != "" {
		lockKey := sqsMsgGroupKey(srcQueueName, srcGen, srcRec.MessageGroupId)
		lock, err := s.loadFifoGroupLock(ctx, srcQueueName, srcGen, srcRec.MessageGroupId, readTS)
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
