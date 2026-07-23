package adapter

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

type sqsPurgeQueueInput struct {
	QueueUrl string `json:"QueueUrl"`
}

// purgeRateLimitedError is the typed internal error returned by
// tryPurgeQueueOnce when the meta-stored 60-second rate limit rejects
// a call. The remaining field captures the wall-clock duration the
// caller should wait, computed from the same LastPurgedAtMillis value
// the rate-limit check itself read inside the OCC transaction. The
// admin path extracts it via errors.As to populate
// *PurgeInProgressError.RetryAfter; the SigV4 path renders the same
// wire shape the previous *sqsAPIError emitted via
// writeSQSErrorFromErr's dedicated branch.
type purgeRateLimitedError struct {
	remaining time.Duration
}

func (e *purgeRateLimitedError) Error() string {
	return "only one PurgeQueue operation on each queue is allowed every 60 seconds"
}

// purgeQueue bumps the queue generation so every message under the old
// generation becomes unreachable via routing, leaving the meta record in
// place so the queue still "exists" to clients. AWS rate-limits PurgeQueue
// to one call per 60 seconds per queue; the limiter survives leader
// failover because it is stored on the meta record itself.
func (s *SQSServer) purgeQueue(w http.ResponseWriter, r *http.Request) {
	var in sqsPurgeQueueInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	name, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if err := s.purgeQueueCore(r.Context(), name); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

func (s *SQSServer) purgeQueueCore(ctx context.Context, name string) error {
	_, _, err := s.purgeQueueWithRetry(ctx, name)
	return err
}

// purgeQueueWithRetry runs tryPurgeQueueOnce under the standard OCC
// retry loop. On success it returns the generation captured from the
// committed transaction so callers can audit-log the exact value that
// landed (a separate loadQueueMetaAt call would race a concurrent
// purge in the 60-second window). On any non-retryable error it
// returns zero generations alongside the error.
func (s *SQSServer) purgeQueueWithRetry(ctx context.Context, queueName string) (uint64, uint64, error) {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		done, oldGen, newGen, err := s.tryPurgeQueueOnce(ctx, queueName)
		if err == nil && done {
			return oldGen, newGen, nil
		}
		if err != nil && !isRetryableTransactWriteError(err) {
			return 0, 0, err
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return 0, 0, errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return 0, 0, newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "purge queue retry attempts exhausted")
}

// tryPurgeQueueOnce performs one read-validate-commit pass. The first
// return reports whether the caller should stop retrying (true means
// the purge is committed); a non-retryable error short-circuits the
// loop. On success the second and third returns carry the
// pre-bump and post-bump generations so the caller can audit-log the
// committed value without a second meta read.
func (s *SQSServer) tryPurgeQueueOnce(ctx context.Context, queueName string) (bool, uint64, uint64, error) {
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, "sqs purge queue: begin read timestamp")
	if err != nil {
		return false, 0, 0, errors.WithStack(err)
	}
	readTS := readTimestamp.Timestamp()
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return false, 0, 0, errors.WithStack(err)
	}
	if !exists {
		return false, 0, 0, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	now := time.Now().UnixMilli()
	if meta.LastPurgedAtMillis > 0 && now-meta.LastPurgedAtMillis < sqsPurgeRateLimitMillis {
		// Compute the remaining wait inside the OCC read so the
		// duration cannot race a concurrent purge resetting
		// LastPurgedAtMillis in the 60-second window. Both
		// sqsPurgeRateLimitMillis and the (now - LastPurgedAtMillis)
		// difference are int64 millisecond counts; multiplying by
		// time.Millisecond is the unit-correct conversion to
		// time.Duration.
		remaining := time.Duration(sqsPurgeRateLimitMillis-(now-meta.LastPurgedAtMillis)) * time.Millisecond
		return false, 0, 0, &purgeRateLimitedError{remaining: remaining}
	}
	lastGen, err := s.loadQueueGenerationAt(ctx, queueName, readTS)
	if err != nil {
		return false, 0, 0, errors.WithStack(err)
	}
	meta.Generation = lastGen + 1
	meta.LastPurgedAtMillis = now
	meta.LastModifiedAtMillis = now
	metaBytes, err := encodeSQSQueueMeta(meta)
	if err != nil {
		return false, 0, 0, errors.WithStack(err)
	}
	metaKey := sqsQueueMetaKey(queueName)
	genKey := sqsQueueGenKey(queueName)
	// Tombstone the pre-bump generation so the reaper can find its
	// orphan keyspace even if DeleteQueue lands before the next reaper
	// tick. Without this, a Purge → Delete sequence within the
	// reaper interval permanently leaks data/vis/byage/dedup/group
	// rows for the pre-purge generation: scanQueueNames sees no meta,
	// and reapTombstonedQueues only sees the post-delete tombstone
	// (which is keyed on the post-purge gen). reapDeadByAge filters
	// by exact generation, so the older cohort is never visited.
	tombstoneKey := sqsQueueTombstoneKey(queueName, lastGen)
	// Encode the pre-purge generation's PartitionCount in the
	// tombstone value so the reaper can enumerate partitioned
	// dedup / group / byage prefixes for that cohort (PR 6a).
	// PartitionCount is immutable across SetQueueAttributes /
	// PurgeQueue (§3.2 immutability rule), so the post-purge meta
	// and the pre-purge tombstone agree on the partition count.
	// Legacy / non-partitioned queues still write []byte{1} via
	// the encoder's PartitionCount<=1 branch.
	tombstoneValue := encodeQueueTombstoneValue(meta.PartitionCount)
	// StartTS + ReadKeys fence against a concurrent CreateQueue /
	// DeleteQueue / SetQueueAttributes / PurgeQueue landing between
	// our load and dispatch. ErrWriteConflict surfaces via the
	// retry loop so a later pass observes the new state.
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{metaKey, genKey},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: metaKey, Value: metaBytes},
			{Op: kv.Put, Key: genKey, Value: []byte(strconv.FormatUint(meta.Generation, 10))},
			{Op: kv.Put, Key: tombstoneKey, Value: tombstoneValue},
		},
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		return false, 0, 0, errors.WithStack(err)
	}
	return true, lastGen, meta.Generation, nil
}
