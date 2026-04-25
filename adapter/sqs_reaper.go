package adapter

import (
	"bytes"
	"context"
	"log/slog"
	"time"

	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/store"
	"github.com/cockroachdb/errors"
)

const (
	// sqsReaperInterval is how often the leader's retention sweeper
	// wakes up to look for expired records. AWS does not promise a
	// specific reaping cadence; the documented retention guarantee is
	// that messages older than MessageRetentionPeriod are eventually
	// dropped. 30 s is fast enough that a queue with the minimum
	// 60 s retention sees expiries within the same minute, and slow
	// enough that an idle cluster pays close to zero CPU.
	sqsReaperInterval = 30 * time.Second
	// sqsReaperPageLimit caps the per-pass scan of byage entries so
	// one tick cannot pin the leader on a backlog. The reaper resumes
	// from the next tick, so eventual reaping holds even when the
	// backlog exceeds the per-tick budget.
	sqsReaperPageLimit = 256
	// sqsReaperPerQueueBudget caps the work per queue per tick to
	// avoid starvation across queues — a single queue with millions
	// of expired entries should not lock out the others.
	sqsReaperPerQueueBudget = 1024
)

// startReaper kicks off the retention sweeper on the leader. It is
// safe to call multiple times; the first call wins and subsequent
// calls are no-ops. Stop() cancels the context so the goroutine
// returns promptly.
func (s *SQSServer) startReaper(ctx context.Context) {
	if s == nil || s.coordinator == nil || s.store == nil {
		return
	}
	go s.runReaper(ctx)
}

func (s *SQSServer) runReaper(ctx context.Context) {
	t := time.NewTicker(sqsReaperInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		// Only the leader should reap; followers would emit
		// duplicate Dispatches that the leader would still have to
		// adjudicate, costing a round-trip per record. The check is
		// a cheap local read.
		if s.coordinator == nil || !s.coordinator.IsLeader() {
			continue
		}
		if err := s.reapAllQueues(ctx); err != nil {
			slog.Warn("sqs reaper pass failed", "err", err)
		}
	}
}

func (s *SQSServer) reapAllQueues(ctx context.Context) error {
	names, err := s.scanQueueNames(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return errors.WithStack(err)
		}
		readTS := s.nextTxnReadTS(ctx)
		meta, exists, err := s.loadQueueMetaAt(ctx, name, readTS)
		if err != nil || !exists || meta.MessageRetentionSeconds <= 0 {
			continue
		}
		if err := s.reapQueue(ctx, name, meta, readTS); err != nil {
			slog.Warn("sqs reaper queue pass failed", "queue", name, "err", err)
		}
	}
	return nil
}

// reapQueue scans the byage index for records whose
// (now - sendTimestamp) exceeds retention and removes them under OCC
// transactions, one record at a time. AWS allows the reaper to lag
// retention slightly, so per-record dispatch keeps each transaction
// small and bounded; a single mega-batch transaction would balloon
// memory and increase abort probability.
func (s *SQSServer) reapQueue(ctx context.Context, queueName string, meta *sqsQueueMeta, readTS uint64) error {
	now := time.Now().UnixMilli()
	cutoff := now - meta.MessageRetentionSeconds*sqsMillisPerSecond
	if cutoff <= 0 {
		return nil
	}
	prefix := sqsMsgByAgePrefixForQueue(queueName, meta.Generation)
	start := bytes.Clone(prefix)
	upper := append(bytes.Clone(prefix), encodedU64(uint64MaxZero(cutoff)+1)...)

	processed := 0
	for processed < sqsReaperPerQueueBudget {
		page, err := s.store.ScanAt(ctx, start, upper, sqsReaperPageLimit, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(page) == 0 {
			return nil
		}
		done, newProcessed, err := s.reapPage(ctx, queueName, meta.Generation, page, readTS, processed)
		if err != nil {
			return err
		}
		processed = newProcessed
		if done {
			return nil
		}
		start = nextScanCursorAfter(page[len(page)-1].Key)
		if bytes.Compare(start, upper) >= 0 {
			return nil
		}
	}
	return nil
}

// reapPage walks one ScanAt page, dispatching a per-record reap
// transaction. Returns done=true when the per-queue budget is hit or
// the page was short (last page in the scan).
func (s *SQSServer) reapPage(ctx context.Context, queueName string, gen uint64, page []*store.KVPair, readTS uint64, processed int) (bool, int, error) {
	for _, kvp := range page {
		if err := ctx.Err(); err != nil {
			return true, processed, errors.WithStack(err)
		}
		if err := s.reapOneRecord(ctx, queueName, gen, kvp.Key, string(kvp.Value), readTS); err != nil {
			return true, processed, err
		}
		processed++
		if processed >= sqsReaperPerQueueBudget {
			return true, processed, nil
		}
	}
	if len(page) < sqsReaperPageLimit {
		return true, processed, nil
	}
	return false, processed, nil
}

// reapOneRecord deletes one (data, vis, byage, optional group-lock)
// quartet under a single OCC dispatch. ErrWriteConflict is treated as
// success — the message has just been touched (received, deleted,
// redriven) by another path and is no longer ours to reap.
func (s *SQSServer) reapOneRecord(ctx context.Context, queueName string, gen uint64, byAgeKey []byte, messageID string, readTS uint64) error {
	dataKey := sqsMsgDataKey(queueName, gen, messageID)
	parsed, found, err := s.loadDataForReaper(ctx, dataKey, readTS)
	if err != nil {
		return err
	}
	if !found {
		// Stale byage index without a backing record. Drop the
		// index entry alone — without this branch the reaper would
		// loop on the same orphan key forever.
		s.dispatchOrphanByAgeDrop(ctx, byAgeKey, readTS)
		return nil
	}
	req, err := s.buildReapOps(ctx, queueName, gen, byAgeKey, dataKey, parsed, readTS)
	if err != nil {
		return err
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		if isRetryableTransactWriteError(err) {
			return nil
		}
		return errors.WithStack(err)
	}
	return nil
}

// loadDataForReaper fetches and decodes the data record for a byage
// entry. found=false signals "byage points at a missing record — drop
// the byage entry" to the caller. Read errors other than ErrKeyNotFound
// surface to the caller so a transient storage problem is logged and
// retried on the next tick instead of silently scrubbing the index.
func (s *SQSServer) loadDataForReaper(ctx context.Context, dataKey []byte, readTS uint64) (*sqsMessageRecord, bool, error) {
	raw, err := s.store.GetAt(ctx, dataKey, readTS)
	if err != nil {
		if errors.Is(err, store.ErrKeyNotFound) {
			return nil, false, nil
		}
		return nil, false, errors.WithStack(err)
	}
	parsed, err := decodeSQSMessageRecord(raw)
	if err != nil {
		return nil, false, errors.WithStack(err)
	}
	return parsed, true, nil
}

func (s *SQSServer) dispatchOrphanByAgeDrop(ctx context.Context, byAgeKey []byte, readTS uint64) {
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{byAgeKey},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Del, Key: byAgeKey},
		},
	}
	_, _ = s.coordinator.Dispatch(ctx, req)
}

func (s *SQSServer) buildReapOps(ctx context.Context, queueName string, gen uint64, byAgeKey, dataKey []byte, parsed *sqsMessageRecord, readTS uint64) (*kv.OperationGroup[kv.OP], error) {
	visKey := sqsMsgVisKey(queueName, gen, parsed.VisibleAtMillis, parsed.MessageID)
	readKeys := [][]byte{byAgeKey, dataKey, visKey, sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: byAgeKey},
		{Op: kv.Del, Key: dataKey},
		{Op: kv.Del, Key: visKey},
	}
	if parsed.MessageGroupId != "" {
		lockKey := sqsMsgGroupKey(queueName, gen, parsed.MessageGroupId)
		lock, err := s.loadFifoGroupLock(ctx, queueName, gen, parsed.MessageGroupId, readTS)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.MessageID == parsed.MessageID {
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
