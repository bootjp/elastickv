package adapter

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
	json "github.com/goccy/go-json"
)

// FIFO dedup window — AWS guarantees exactly-once within 5 minutes of
// the first SendMessage that carried a given (dedup-id, queue-gen).
const sqsFifoDedupWindowMillis = 5 * 60 * 1000

// sqsFifoDedupRecord is the value persisted at !sqs|msg|dedup|... so a
// retry within the dedup window can short-circuit to the original
// message-id without writing a second copy of the body. Holding the
// original send timestamp lets the receive path drop expired records
// lazily once we add a reaper.
type sqsFifoDedupRecord struct {
	MessageID        string `json:"message_id"`
	SendTimestampMs  int64  `json:"send_timestamp_ms"`
	ExpiresAtMillis  int64  `json:"expires_at_millis"`
	OriginalSequence uint64 `json:"original_sequence,omitempty"`
}

func encodeFifoDedupRecord(r *sqsFifoDedupRecord) ([]byte, error) {
	b, err := json.Marshal(r)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b, nil
}

func decodeFifoDedupRecord(b []byte) (*sqsFifoDedupRecord, error) {
	var r sqsFifoDedupRecord
	if err := json.Unmarshal(b, &r); err != nil {
		return nil, errors.WithStack(err)
	}
	return &r, nil
}

// sqsFifoGroupLock is the value persisted at !sqs|msg|group|... while a
// group has an in-flight head message. Holding the message id (rather
// than just a boolean) lets the receive path tell "I already own this
// lock" (a redelivery) apart from "another message owns it" (skip the
// whole group).
type sqsFifoGroupLock struct {
	MessageID    string `json:"message_id"`
	AcquiredAtMs int64  `json:"acquired_at_ms"`
	VisibleAtMs  int64  `json:"visible_at_ms"`
}

func encodeFifoGroupLock(l *sqsFifoGroupLock) ([]byte, error) {
	b, err := json.Marshal(l)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return b, nil
}

func decodeFifoGroupLock(b []byte) (*sqsFifoGroupLock, error) {
	var l sqsFifoGroupLock
	if err := json.Unmarshal(b, &l); err != nil {
		return nil, errors.WithStack(err)
	}
	return &l, nil
}

// resolveFifoDedupID returns the dedup-id AWS would compute for this
// send. ContentBasedDeduplication=true uses SHA-256 over the body
// (matching AWS exactly); otherwise the caller-supplied
// MessageDeduplicationId is used.
func resolveFifoDedupID(meta *sqsQueueMeta, in sqsSendMessageInput) string {
	if in.MessageDeduplicationId != "" {
		return in.MessageDeduplicationId
	}
	if meta.ContentBasedDedup {
		sum := sha256.Sum256([]byte(in.MessageBody))
		return hex.EncodeToString(sum[:])
	}
	return ""
}

// loadFifoDedupRecord returns the dedup record at the given snapshot,
// or (nil, nil) when there is no live record for this dedup-id.
// Expired records are surfaced as nil so a stale entry does not block
// a fresh send within the same FIFO queue.
//
// groupID is the MessageGroupId; it participates in the partitioned
// dedup key so that two groups colliding onto the same partition keep
// disjoint dedup namespaces (the AWS messageGroup-scope contract).
func (s *SQSServer) loadFifoDedupRecord(ctx context.Context, queueName string, meta *sqsQueueMeta, partition uint32, gen uint64, groupID, dedupID string, readTS uint64) (*sqsFifoDedupRecord, []byte, error) {
	key := sqsMsgDedupKeyDispatch(meta, queueName, partition, gen, groupID, dedupID)
	raw, err := s.store.GetAt(ctx, key, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, key, nil
		}
		return nil, key, errors.WithStack(err)
	}
	rec, err := decodeFifoDedupRecord(raw)
	if err != nil {
		return nil, key, errors.WithStack(err)
	}
	if rec.ExpiresAtMillis > 0 && time.Now().UnixMilli() > rec.ExpiresAtMillis {
		return nil, key, nil
	}
	return rec, key, nil
}

// loadFifoSequence fetches the current per-queue sequence counter at
// the given snapshot. Missing keys (fresh queue) read as zero so the
// first FIFO send observes 1.
func (s *SQSServer) loadFifoSequence(ctx context.Context, queueName string, readTS uint64) (uint64, error) {
	raw, err := s.store.GetAt(ctx, sqsQueueSeqKey(queueName), readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, errors.WithStack(err)
	}
	v, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	return v, nil
}

// loadFifoGroupLock fetches the in-flight lock for a group, if any.
// Returns nil when no lock is held. Callers that also need the key
// can recompute it via sqsMsgGroupKeyDispatch — the helper used to
// return it alongside the lock, but every caller already had the
// key in scope from a different code path.
func (s *SQSServer) loadFifoGroupLock(ctx context.Context, queueName string, meta *sqsQueueMeta, partition uint32, gen uint64, groupID string, readTS uint64) (*sqsFifoGroupLock, error) {
	key := sqsMsgGroupKeyDispatch(meta, queueName, partition, gen, groupID)
	raw, err := s.store.GetAt(ctx, key, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, nil
		}
		return nil, errors.WithStack(err)
	}
	lock, err := decodeFifoGroupLock(raw)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return lock, nil
}

// sendFifoMessage is the FIFO-aware analog of the OCC dispatch in
// sendMessage. It runs inside the existing retry loop and:
//
//  1. Looks up the dedup record for this (queue, gen, dedup-id). On
//     hit it returns the original message-id without writing.
//  2. Loads the per-queue sequence counter and bumps it.
//  3. Writes the data + vis-index entries plus the dedup record and
//     the new sequence counter, all under one OCC transaction so a
//     concurrent FIFO send cannot reuse our sequence number.
//
// Returns the response payload (MessageId, MD5OfMessageBody, etc.)
// the caller should hand to the JSON encoder, and a retry hint: when
// the OCC dispatch fails with a retryable error the bool is true and
// the caller re-runs the whole pass against a fresh snapshot.
func (s *SQSServer) sendFifoMessage(
	ctx context.Context,
	queueName string,
	meta *sqsQueueMeta,
	in sqsSendMessageInput,
	dedupID string,
	delay int64,
	readTS uint64,
) (map[string]string, bool, error) {
	// HT-FIFO: hash the MessageGroupId once at the entry point so
	// every key built in this transaction (data, vis, byage, dedup,
	// group-lock, sequence) lands in the same partition. partitionFor
	// returns 0 on legacy / non-partitioned queues and on the
	// perQueue throughput short-circuit, so the dispatch helpers
	// round-trip to legacy output for those cases.
	partition := partitionFor(meta, in.MessageGroupId)
	dedup, dedupKey, err := s.loadFifoDedupRecord(ctx, queueName, meta, partition, meta.Generation, in.MessageGroupId, dedupID, readTS)
	if err != nil {
		return nil, false, err
	}
	if dedup != nil {
		// AWS semantics: dedup hit returns success with the original
		// message-id, no error. Idempotent-by-design retries from a
		// crashed-and-restarted producer keep working.
		return map[string]string{
			"MessageId":              dedup.MessageID,
			"MD5OfMessageBody":       sqsMD5Hex([]byte(in.MessageBody)),
			"MD5OfMessageAttributes": md5OfAttributesHex(in.MessageAttributes),
			"SequenceNumber":         strconv.FormatUint(dedup.OriginalSequence, 10),
		}, false, nil
	}

	prevSeq, err := s.loadFifoSequence(ctx, queueName, readTS)
	if err != nil {
		return nil, false, err
	}
	nextSeq := prevSeq + 1

	rec, _, err := buildSendRecord(meta, in, delay)
	if err != nil {
		return nil, false, err
	}
	rec.SequenceNumber = nextSeq
	recordBytes, err := encodeSQSMessageRecord(rec)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}

	now := time.Now().UnixMilli()
	dedupRec := &sqsFifoDedupRecord{
		MessageID:        rec.MessageID,
		SendTimestampMs:  now,
		ExpiresAtMillis:  now + sqsFifoDedupWindowMillis,
		OriginalSequence: nextSeq,
	}
	dedupBytes, err := encodeFifoDedupRecord(dedupRec)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}

	dataKey := sqsMsgDataKeyDispatch(meta, queueName, partition, meta.Generation, rec.MessageID)
	visKey := sqsMsgVisKeyDispatch(meta, queueName, partition, meta.Generation, rec.AvailableAtMillis, rec.MessageID)
	byAgeKey := sqsMsgByAgeKeyDispatch(meta, queueName, partition, meta.Generation, rec.SendTimestampMillis, rec.MessageID)
	seqKey := sqsQueueSeqKey(queueName)
	metaKey := sqsQueueMetaKey(queueName)
	genKey := sqsQueueGenKey(queueName)
	// ReadKeys: meta + gen guard against DeleteQueue / PurgeQueue,
	// dedupKey guards a concurrent send under the same dedup-id, seqKey
	// guards a concurrent FIFO send taking the same sequence number.
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{metaKey, genKey, dedupKey, seqKey},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: dataKey, Value: recordBytes},
			{Op: kv.Put, Key: visKey, Value: []byte(rec.MessageID)},
			{Op: kv.Put, Key: byAgeKey, Value: []byte(rec.MessageID)},
			{Op: kv.Put, Key: dedupKey, Value: dedupBytes},
			{Op: kv.Put, Key: seqKey, Value: []byte(strconv.FormatUint(nextSeq, 10))},
		},
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		if isRetryableTransactWriteError(err) {
			return nil, true, nil
		}
		return nil, false, errors.WithStack(err)
	}
	return map[string]string{
		"MessageId":              rec.MessageID,
		"MD5OfMessageBody":       rec.MD5OfBody,
		"MD5OfMessageAttributes": md5OfAttributesHex(in.MessageAttributes),
		"SequenceNumber":         strconv.FormatUint(nextSeq, 10),
	}, false, nil
}

// fifoCandidateLockState classifies what a candidate's group lock
// allows the receive path to do. Centralising the three-way decision
// keeps tryDeliverCandidate readable.
type fifoCandidateLockState int

const (
	// fifoLockSkip means the group is held by a different message; the
	// candidate must be skipped without consuming the receive budget.
	fifoLockSkip fifoCandidateLockState = iota
	// fifoLockOwn means the group is already held by this very
	// message — a redelivery after visibility expiry. Continue.
	fifoLockOwn
	// fifoLockAcquire means no lock exists and the receive path must
	// install one as part of the OCC transaction.
	fifoLockAcquire
)

// classifyFifoGroupLock decides whether a FIFO candidate is eligible
// for delivery. Standard queues bypass the function entirely.
func (s *SQSServer) classifyFifoGroupLock(ctx context.Context, queueName string, meta *sqsQueueMeta, partition uint32, gen uint64, rec *sqsMessageRecord, readTS uint64) (fifoCandidateLockState, []byte, error) {
	lockKey := sqsMsgGroupKeyDispatch(meta, queueName, partition, gen, rec.MessageGroupId)
	lock, err := s.loadFifoGroupLock(ctx, queueName, meta, partition, gen, rec.MessageGroupId, readTS)
	if err != nil {
		return fifoLockSkip, lockKey, err
	}
	if lock == nil {
		return fifoLockAcquire, lockKey, nil
	}
	if lock.MessageID == rec.MessageID {
		return fifoLockOwn, lockKey, nil
	}
	return fifoLockSkip, lockKey, nil
}

// fifoLockMutationsForReceive returns the OCC ops the receive path
// must add when it commits a FIFO delivery. The lock's VisibleAtMs is
// always rewritten so a concurrent re-acquire sees the freshest
// in-flight deadline.
func fifoLockMutationsForReceive(lockKey []byte, msgID string, newVisibleAt int64) ([]*kv.Elem[kv.OP], error) {
	lock := &sqsFifoGroupLock{
		MessageID:    msgID,
		AcquiredAtMs: time.Now().UnixMilli(),
		VisibleAtMs:  newVisibleAt,
	}
	body, err := encodeFifoGroupLock(lock)
	if err != nil {
		return nil, err
	}
	return []*kv.Elem[kv.OP]{
		{Op: kv.Put, Key: lockKey, Value: body},
	}, nil
}
