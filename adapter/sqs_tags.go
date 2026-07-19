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
	if err := s.tagQueueCore(r.Context(), name, in.Tags); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

func (s *SQSServer) tagQueueCore(ctx context.Context, name string, tags map[string]string) error {
	if len(tags) == 0 {
		return newSQSAPIError(http.StatusBadRequest, sqsErrMissingParameter, "Tags is required")
	}
	return s.mutateQueueTagsWithRetry(ctx, name, func(meta *sqsQueueMeta) error {
		if meta.Tags == nil {
			meta.Tags = make(map[string]string, len(tags))
		}
		for k, v := range tags {
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
	})
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
	if err := s.untagQueueCore(r.Context(), name, in.TagKeys); err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{})
}

func (s *SQSServer) untagQueueCore(ctx context.Context, name string, tagKeys []string) error {
	if len(tagKeys) == 0 {
		return newSQSAPIError(http.StatusBadRequest, sqsErrMissingParameter, "TagKeys is required")
	}
	return s.mutateQueueTagsWithRetry(ctx, name, func(meta *sqsQueueMeta) error {
		for _, k := range tagKeys {
			delete(meta.Tags, k)
		}
		return nil
	})
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
	tags, err := s.listQueueTagsCore(r.Context(), name)
	if err != nil {
		writeSQSErrorFromErr(w, err)
		return
	}
	writeSQSJSON(w, map[string]any{"Tags": tags})
}

func (s *SQSServer) listQueueTagsCore(ctx context.Context, name string) (map[string]string, error) {
	meta, exists, err := s.loadQueueMetaAt(ctx, name, s.nextTxnReadTS(ctx))
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, newSQSAPIError(http.StatusBadRequest, sqsErrQueueDoesNotExist, "queue does not exist")
	}
	tags := meta.Tags
	if tags == nil {
		// AWS always returns a Tags field; a nil map would marshal to
		// null and trip strict JSON consumers.
		tags = map[string]string{}
	}
	return tags, nil
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
	readTimestamp, err := s.beginTxnReadTimestamp(ctx, "sqs mutate queue tags: begin read timestamp")
	if err != nil {
		return false, errors.WithStack(err)
	}
	readTS := readTimestamp.Timestamp()
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
