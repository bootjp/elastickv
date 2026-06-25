package adapter

import (
	"context"
	"net/http"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// AWS allows up to 50 tags per resource. Enforced adapter-side so a
// runaway client cannot bloat the meta record.
const sqsMaxTagsPerQueue = 50

type sqsTagQueueInput struct {
	QueueUrl string            `json:"QueueUrl"`
	Tags     map[string]string `json:"Tags"`
}

type sqsUntagQueueInput struct {
	QueueUrl string   `json:"QueueUrl"`
	TagKeys  []string `json:"TagKeys"`
}

type sqsListQueueTagsInput struct {
	QueueUrl string `json:"QueueUrl"`
}

func (s *SQSServer) tagQueue(w http.ResponseWriter, r *http.Request) {
	var in sqsTagQueueInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	name, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if len(in.Tags) == 0 {
		writeSQSError(w, http.StatusBadRequest, sqsErrMissingParameter, "Tags is required")
		return
	}
	if err := s.mutateQueueTagsWithRetry(r.Context(), name, func(meta *sqsQueueMeta) error {
		if meta.Tags == nil {
			meta.Tags = make(map[string]string, len(in.Tags))
		}
		for k, v := range in.Tags {
			meta.Tags[k] = v
		}
		// Cap the per-queue tag count after merging so a follow-up
		// TagQueue cannot push a queue past the AWS limit one tag at
		// a time.
		if len(meta.Tags) > sqsMaxTagsPerQueue {
			return newSQSAPIError(http.StatusBadRequest, sqsErrInvalidAttributeValue,
				"queue tag count exceeds 50")
		}
		return nil
	}); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

func (s *SQSServer) untagQueue(w http.ResponseWriter, r *http.Request) {
	var in sqsUntagQueueInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	name, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if len(in.TagKeys) == 0 {
		writeSQSError(w, http.StatusBadRequest, sqsErrMissingParameter, "TagKeys is required")
		return
	}
	if err := s.mutateQueueTagsWithRetry(r.Context(), name, func(meta *sqsQueueMeta) error {
		for _, k := range in.TagKeys {
			delete(meta.Tags, k)
		}
		return nil
	}); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

func (s *SQSServer) listQueueTags(w http.ResponseWriter, r *http.Request) {
	var in sqsListQueueTagsInput
	if err := decodeSQSJSONInput(r, &in); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	name, err := queueNameFromURL(in.QueueUrl)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	meta, exists, err := s.loadQueueMetaAt(r.Context(), name, s.nextTxnReadTS(r.Context()))
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	if !exists {
		writeSQSError(w, http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
		return
	}
	tags := meta.Tags
	if tags == nil {
		// AWS always returns a Tags field; a nil map would marshal to
		// null and trip strict JSON consumers.
		tags = map[string]string{}
	}
	writeSQSJSON(w, map[string]any{"Tags": tags})
}

// mutateQueueTagsWithRetry runs an OCC-bounded read-modify-write of the
// queue meta record. The mutator may inspect or modify meta.Tags; any
// validation error it returns short-circuits the retry loop.
func (s *SQSServer) mutateQueueTagsWithRetry(
	ctx context.Context,
	queueName string,
	mutate func(*sqsQueueMeta) error,
) error {
	backoff := transactRetryInitialBackoff
	deadline := time.Now().Add(transactRetryMaxDuration)
	for range transactRetryMaxAttempts {
		done, err := s.tryMutateQueueTagsOnce(ctx, queueName, mutate)
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
	return newSQSAPIError(http.StatusInternalServerError, sqsErrInternalFailure, "tag mutation retry attempts exhausted")
}

func (s *SQSServer) tryMutateQueueTagsOnce(
	ctx context.Context,
	queueName string,
	mutate func(*sqsQueueMeta) error,
) (bool, error) {
	readTS := s.nextTxnReadTS(ctx)
	meta, exists, err := s.loadQueueMetaAt(ctx, queueName, readTS)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if !exists {
		return false, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	if err := mutate(meta); err != nil {
		return false, err
	}
	if len(meta.Tags) == 0 {
		// Drop the empty map so the encoded record stays compact and
		// equality checks (attributesEqual) treat absent vs empty the
		// same.
		meta.Tags = nil
	}
	meta.LastModifiedAtMillis = time.Now().UnixMilli()
	metaBytes, err := encodeSQSQueueMeta(meta)
	if err != nil {
		return false, errors.WithStack(err)
	}
	metaKey := sqsQueueMetaKey(queueName)
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{metaKey},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Put, Key: metaKey, Value: metaBytes},
		},
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		return false, errors.WithStack(err)
	}
	return true, nil
}
