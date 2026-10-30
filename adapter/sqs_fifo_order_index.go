package adapter

import (
	"bytes"
	"context"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/cockroachdb/errors"
)

// A zero byte cannot occur in an encoded MessageGroupId. Members share the
// existing group family/routing/generation cleanup but never alias a lock or
// another group's key. Sequence is fixed-width big endian, independent of time.
func sqsFifoOrderPrefix(meta *sqsQueueMeta, queue string, partition uint32, gen uint64, group string) []byte {
	return append(sqsMsgGroupKeyDispatch(meta, queue, partition, gen, group), 0)
}

func sqsFifoOrderKey(meta *sqsQueueMeta, queue string, partition uint32, gen uint64, rec *sqsMessageRecord) []byte {
	key := appendU64(sqsFifoOrderPrefix(meta, queue, partition, gen, rec.MessageGroupId), rec.SequenceNumber)
	return append(key, encodeSQSSegment(rec.MessageID)...)
}

func appendFifoOrderPut(req *kv.OperationGroup[kv.OP], meta *sqsQueueMeta, queue string, partition uint32, rec *sqsMessageRecord) {
	if meta.FifoOrderIndexed {
		req.Elems = append(req.Elems, &kv.Elem[kv.OP]{Op: kv.Put,
			Key: sqsFifoOrderKey(meta, queue, partition, meta.Generation, rec), Value: []byte(rec.MessageID)})
	}
}

// Delete even during backfill: a received record can be deleted after its
// membership entry was built but before the queue's indexed flag is published.
func appendFifoOrderDelete(elems []*kv.Elem[kv.OP], meta *sqsQueueMeta, queue string, partition uint32, gen uint64, rec *sqsMessageRecord) []*kv.Elem[kv.OP] {
	if meta != nil && !meta.FifoOrderIndexed && meta.DelayReindex == nil {
		return elems
	}
	return appendFifoOrderDeleteAlways(elems, meta, queue, partition, gen, rec)
}

// Reaping can use a layout-only synthetic meta after the original metadata was
// removed. It must remove membership without relying on the activation flag.
func appendFifoOrderDeleteAlways(elems []*kv.Elem[kv.OP], meta *sqsQueueMeta, queue string, partition uint32, gen uint64, rec *sqsMessageRecord) []*kv.Elem[kv.OP] {
	if rec.MessageGroupId != "" {
		elems = append(elems, &kv.Elem[kv.OP]{Op: kv.Del, Key: sqsFifoOrderKey(meta, queue, partition, gen, rec)})
	}
	return elems
}

func (s *SQSServer) resolveIndexedFifoCandidate(ctx context.Context, queueName string, meta *sqsQueueMeta, cand sqsMsgCandidate, rec *sqsMessageRecord, readTS uint64, lockKey []byte) (*sqsFifoHead, fifoCandidateLockState, []byte, error) {
	prefix := sqsFifoOrderPrefix(meta, queueName, cand.partition, meta.Generation, rec.MessageGroupId)
	page, groupID, err := s.scanVisibleIndexAt(ctx, prefix, prefixScanEnd(prefix), 1, readTS)
	if err != nil {
		return nil, fifoLockSkip, lockKey, err
	}
	if len(page) == 0 {
		return nil, fifoLockSkip, lockKey, errors.New("FIFO sequence index is missing its group head")
	}
	headCandidate := sqsMsgCandidate{messageID: string(page[0].Value), partition: cand.partition, groupID: groupID}
	headRecord, dataKey, gone, err := s.loadCandidateRecord(ctx, queueName, meta, meta.Generation, headCandidate, readTS)
	if err != nil {
		return nil, fifoLockSkip, lockKey, err
	}
	if gone || headRecord.MessageGroupId != rec.MessageGroupId || !bytes.Equal(page[0].Key, sqsFifoOrderKey(meta, queueName, cand.partition, meta.Generation, headRecord)) {
		return nil, fifoLockSkip, lockKey, errors.New("FIFO sequence index has an invalid group head")
	}
	if headRecord.VisibleAtMillis > time.Now().UnixMilli() {
		return nil, fifoLockSkip, lockKey, nil
	}
	headCandidate.visKey = sqsMsgVisKeyDispatch(meta, queueName, cand.partition, meta.Generation, headRecord.VisibleAtMillis, headRecord.MessageID)
	return &sqsFifoHead{candidate: headCandidate, record: headRecord, dataKey: dataKey}, fifoLockAcquire, lockKey, nil
}
