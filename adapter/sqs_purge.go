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
	if err := s.purgeQueueWithRetry(r.Context(), name); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

func (s *SQSServer) purgeQueueWithRetry(ctx context.Context, queueName string) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		done, err := s.tryPurgeQueueOnce(ctx, queueName)
		if err == nil && done {
			return nil
		}
		if err != nil && !isRetryableTransactWriteError(err) {
			return err
		}
		if err := waitRetryWithDeadline(ctx, deadline, backoff); err != nil {
			return errors.WithStack(err)
		}
		backoff = nextTransactRetryBackoff(backoff)
	}
	return newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "purge queue retry attempts exhausted")
}

// tryPurgeQueueOnce performs one read-validate-commit pass. The first
// return reports whether the caller should stop retrying (true means
// the purge is committed); a non-retryable error short-circuits the
// loop.
func (s *SQSServer) tryPurgeQueueOnce(ctx context.Context, queueName string) (bool, error) {
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !exists {
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	now := time.Now().UnixMilli()
	if meta.LastPurgedAtMillis > 0 && now-meta.LastPurgedAtMillis < sqsPurgeRateLimitMillis {
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrPurgeInProgress,
			"only one PurgeQueue operation on each queue is allowed every 60 seconds")
	}
	lastGen, err := s.loadQueueGenerationAt(ctx, queueName, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	meta.Generation = lastGen + 1
	meta.LastPurgedAtMillis = now
	meta.LastModifiedAtMillis = now
	metaBytes, err := encodeSQSQueueMeta(meta)
	if err != nil {
		return false, errors.WithStack(err)
	}
	metaKey := sqsQueueMetaKey(queueName)
	genKey := sqsQueueGenKey(queueName)
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
		},
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}
