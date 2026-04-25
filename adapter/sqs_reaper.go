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
		if err != nil || !exists {
			// Even when meta is gone (DeleteQueue), prior-generation
			// orphans need reaping; reapDeletedQueueOrphans handles
			// that case. Here we only skip the queue if loading
			// itself failed (transient).
			continue
		}
		if err := s.reapQueue(ctx, name, meta, readTS); err != nil {
			slog.Warn("sqs reaper queue pass failed", "queue", name, "err", err)
		}
		if err := s.reapExpiredDedup(ctx, name, readTS); err != nil {
			slog.Warn("sqs dedup reaper pass failed", "queue", name, "err", err)
		}
	}
	return nil
}

// reapQueue scans the byage index across every queue generation and
// removes records that are either (a) past the current generation's
// retention deadline, or (b) leftovers from a prior generation that
// PurgeQueue / DeleteQueue advanced past. Without case (b), each
// purge would permanently leak data/vis/byage/group-lock state for
// every message it left behind — those keys are unreachable via
// normal routing once the generation bumps, so the reaper is the
// only path that can free them.
//
// One OCC dispatch per record keeps each transaction small and
// bounded; a mega-batch transaction would balloon memory and abort
// more often.
func (s *SQSServer) reapQueue(ctx context.Context, queueName string, meta *sqsQueueMeta, readTS uint64) error {
	now := time.Now().UnixMilli()
	cutoff := now - meta.MessageRetentionSeconds*sqsMillisPerSecond
	if meta.MessageRetentionSeconds <= 0 {
		// Retention was set to a non-positive value: only orphan
		// reaping (case b) makes sense. Keep cutoff at MaxInt64-ish
		// for the live generation so we never delete live records.
		cutoff = 0
	}
	prefix := sqsMsgByAgePrefixAllGenerations(queueName)
	upper := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)

	processed := 0
	for processed < sqsReaperPerQueueBudget {
		page, err := s.store.ScanAt(ctx, start, upper, sqsReaperPageLimit, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(page) == 0 {
			return nil
		}
		done, newProcessed, err := s.reapPage(ctx, queueName, meta.Generation, cutoff, page, readTS, processed)
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
// transaction. currentGen is the queue's *live* generation; entries
// under any earlier generation are unconditionally reaped, while
// entries on the live generation are gated by `cutoff`. Returns
// done=true when the per-queue budget is hit or the page was short
// (last page in the scan).
func (s *SQSServer) reapPage(ctx context.Context, queueName string, currentGen uint64, cutoff int64, page []*store.KVPair, readTS uint64, processed int) (bool, int, error) {
	for _, kvp := range page {
		if err := ctx.Err(); err != nil {
			return true, processed, errors.WithStack(err)
		}
		parsed, ok := parseSqsMsgByAgeKey(kvp.Key, queueName)
		if !ok {
			continue
		}
		// Live generation is gated by retention; older generations
		// are unconditional orphans. Skipping a live record that is
		// still inside the retention window keeps the reaper honest
		// — the receive path expects to see it again until retention
		// elapses.
		if parsed.Generation == currentGen && parsed.SendTimestampMs > cutoff {
			continue
		}
		if parsed.Generation > currentGen {
			// Defensive: a key from a generation strictly newer than
			// what the meta says would mean the byage index races
			// the gen counter. Skip it; the next reaper pass will
			// see meta caught up.
			continue
		}
		if err := s.reapOneRecord(ctx, queueName, parsed.Generation, kvp.Key, parsed.MessageID, readTS); err != nil {
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

// reapExpiredDedup walks every FIFO dedup record under the given
// queue (across generations) and deletes the ones whose
// ExpiresAtMillis has passed. Without this sweep, queues with mostly
// unique MessageDeduplicationIds would accumulate permanent
// dedup-row leaks because the send path treats expired records as
// misses but never removes them.
func (s *SQSServer) reapExpiredDedup(ctx context.Context, queueName string, readTS uint64) error {
	prefix := []byte(SqsMsgDedupPrefix)
	prefix = append(prefix, []byte(encodeSQSSegment(queueName))...)
	upper := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)
	now := time.Now().UnixMilli()

	processed := 0
	for processed < sqsReaperPerQueueBudget {
		page, err := s.store.ScanAt(ctx, start, upper, sqsReaperPageLimit, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(page) == 0 {
			return nil
		}
		done, newProcessed, err := s.reapDedupPage(ctx, page, now, readTS, processed)
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

// reapDedupPage walks one ScanAt page of dedup records and removes
// any whose ExpiresAtMillis is in the past. Returns done=true when
// the per-queue budget runs out or the page was short.
func (s *SQSServer) reapDedupPage(ctx context.Context, page []*store.KVPair, now int64, readTS uint64, processed int) (bool, int, error) {
	for _, kvp := range page {
		if err := ctx.Err(); err != nil {
			return true, processed, errors.WithStack(err)
		}
		rec, err := decodeFifoDedupRecord(kvp.Value)
		if err != nil {
			continue
		}
		if rec.ExpiresAtMillis <= 0 || rec.ExpiresAtMillis > now {
			continue
		}
		if err := s.dispatchDedupDelete(ctx, kvp.Key, readTS); err != nil {
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

func (s *SQSServer) dispatchDedupDelete(ctx context.Context, key []byte, readTS uint64) error {
	req := &kv.OperationGroup[kv.OP]{
		IsTxn:    true,
		StartTS:  readTS,
		ReadKeys: [][]byte{key},
		Elems: []*kv.Elem[kv.OP]{
			{Op: kv.Del, Key: key},
		},
	}
	if _, err := s.coordinator.Dispatch(ctx, req); err != nil {
		if isRetryableTransactWriteError(err) {
			return nil
		}
		return errors.WithStack(err)
	}
	return nil
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
