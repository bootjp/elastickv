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
			// orphans need reaping; reapTombstonedQueues (called
			// after this loop) handles that case. Here we only skip
			// the queue if loading itself failed (transient).
			continue
		}
		if err := s.reapQueue(ctx, name, meta, readTS); err != nil {
			slog.Warn("sqs reaper queue pass failed", "queue", name, "err", err)
		}
		if err := s.reapExpiredDedup(ctx, name, readTS); err != nil {
			slog.Warn("sqs dedup reaper pass failed", "queue", name, "err", err)
		}
	}
	// Tombstones fire on DeleteQueue and outlive the meta row, so a
	// purely meta-driven enumeration would never reach orphan keys
	// for deleted queues. Walk them after the live-queue pass.
	if err := s.reapTombstonedQueues(ctx); err != nil {
		slog.Warn("sqs reaper tombstone pass failed", "err", err)
	}
	return nil
}

// reapTombstonedQueues enumerates every (queue, gen) tombstone left
// by DeleteQueue and reaps the message keyspace for that
// (queue, gen). Once a tombstone has nothing left to clean — no
// byage, dedup, or group rows — the tombstone itself is deleted so
// the next pass does not re-walk an empty queue forever.
func (s *SQSServer) reapTombstonedQueues(ctx context.Context) error {
	prefix := []byte(SqsQueueTombstonePrefix)
	upper := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)
	for {
		readTS := s.nextTxnReadTS(ctx)
		page, err := s.store.ScanAt(ctx, start, upper, sqsReaperPageLimit, readTS)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(page) == 0 {
			return nil
		}
		for _, kvp := range page {
			if err := ctx.Err(); err != nil {
				return errors.WithStack(err)
			}
			queueName, gen, ok := parseSqsQueueTombstoneKey(kvp.Key)
			if !ok {
				continue
			}
			// PartitionCount is encoded in the tombstone value
			// (PR 6a). decodeQueueTombstoneValue maps legacy /
			// non-canonical values to 1 so pre-PR-6a tombstones
			// retain their byte-identical legacy reaper path.
			partitionCount := decodeQueueTombstoneValue(kvp.Value)
			s.reapTombstonedGeneration(ctx, queueName, gen, partitionCount, kvp.Key, readTS)
		}
		if len(page) < sqsReaperPageLimit {
			return nil
		}
		start = nextScanCursorAfter(page[len(page)-1].Key)
		if bytes.Compare(start, upper) >= 0 {
			return nil
		}
	}
}

// reapTombstonedGeneration cleans a single (queue, gen) cohort under
// its own per-queue budget. Once every prefix the cohort can occupy
// is empty, the tombstone itself is deleted; otherwise it stays so
// the next tick can finish what was left.
//
// partitionCount drives partition-iterative cleanup: 1 (legacy /
// non-partitioned queue, or pre-PR-6a tombstone whose value
// decoded to the default) takes the byte-identical legacy path —
// one byage / dedup / group sweep — and leaves the partitioned
// keyspace untouched. Greater than 1 ALSO sweeps the partitioned
// byage / dedup / group prefix family for each partition in
// [0, partitionCount), which is the §6 "partitions × budget"
// reaper contract from the split-queue-FIFO design.
func (s *SQSServer) reapTombstonedGeneration(ctx context.Context, queueName string, gen uint64, partitionCount uint32, tombstoneKey []byte, readTS uint64) {
	// Legacy keyspace is always swept — covers all pre-HT-FIFO
	// queues plus any partitioned queue that briefly carried legacy
	// records (defensive: data is nominally never written to the
	// legacy keyspace for partitioned queues, but the sweep is
	// idempotent and cheap).
	dataDone, err := s.reapDeadByAge(ctx, queueName, gen, readTS)
	if err != nil {
		slog.Warn("sqs tombstone byage reap failed", "queue", queueName, "gen", gen, "err", err)
		return
	}
	dedupDone, err := s.deleteAllPrefix(ctx, sqsMsgDedupKeyPrefix(queueName, gen), readTS)
	if err != nil {
		slog.Warn("sqs tombstone dedup reap failed", "queue", queueName, "gen", gen, "err", err)
		return
	}
	groupDone, err := s.deleteAllPrefix(ctx, sqsMsgGroupKeyPrefix(queueName, gen), readTS)
	if err != nil {
		slog.Warn("sqs tombstone group reap failed", "queue", queueName, "gen", gen, "err", err)
		return
	}
	allDone := dataDone && dedupDone && groupDone
	// Partitioned sweep: one (byage, dedup, group) triple per
	// partition. Each triple shares the per-queue budget with the
	// legacy sweep, so a wide-fanout queue may need multiple reaper
	// ticks to fully drain — same contract as the live-queue reap.
	if partitionCount > 1 {
		partDone, err := s.reapPartitionedGeneration(ctx, queueName, gen, partitionCount, readTS)
		if err != nil {
			slog.Warn("sqs tombstone partitioned reap failed",
				"queue", queueName, "gen", gen, "partitionCount", partitionCount, "err", err)
			return
		}
		allDone = allDone && partDone
	}
	if allDone {
		_ = s.dispatchDedupDelete(ctx, tombstoneKey, readTS)
	}
}

// reapPartitionedGeneration sweeps the partitioned byage, dedup,
// and group prefix family for every partition of one tombstoned
// (queue, gen) cohort. Returns done=true only when EVERY partition
// AND every prefix family is fully drained — short-circuiting on
// the first unfinished partition would leave the tombstone in
// place but skip later partitions on this tick, starving them
// under churn.
func (s *SQSServer) reapPartitionedGeneration(ctx context.Context, queueName string, gen uint64, partitionCount uint32, readTS uint64) (bool, error) {
	allDone := true
	for partition := uint32(0); partition < partitionCount; partition++ {
		if err := ctx.Err(); err != nil {
			return false, errors.WithStack(err)
		}
		byageDone, err := s.reapDeadByAgePartition(ctx, queueName, gen, partition, readTS)
		if err != nil {
			return false, err
		}
		dedupDone, err := s.deleteAllPrefix(ctx,
			sqsPartitionedMsgDedupKeyPrefix(queueName, partition, gen), readTS)
		if err != nil {
			return false, err
		}
		groupDone, err := s.deleteAllPrefix(ctx,
			sqsPartitionedMsgGroupKeyPrefix(queueName, partition, gen), readTS)
		if err != nil {
			return false, err
		}
		if !byageDone || !dedupDone || !groupDone {
			allDone = false
		}
	}
	return allDone, nil
}

// reapDeadByAge walks the byage prefix for one (queue, gen) cohort
// and reaps each record found, regardless of retention age — every
// row under a tombstoned generation is by definition orphaned.
// Returns done=true when the cohort is fully drained.
func (s *SQSServer) reapDeadByAge(ctx context.Context, queueName string, gen uint64, readTS uint64) (bool, error) {
	prefix := append(sqsMsgByAgePrefixAllGenerations(queueName), encodedU64(gen)...)
	upper := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)
	processed := 0
	for processed < sqsReaperPerQueueBudget {
		page, err := s.store.ScanAt(ctx, start, upper, sqsReaperPageLimit, readTS)
		if err != nil {
			return false, errors.WithStack(err)
		}
		if len(page) == 0 {
			return true, nil
		}
		done, newProcessed, err := s.reapDeadByAgePage(ctx, queueName, gen, page, readTS, processed)
		if err != nil {
			return false, err
		}
		processed = newProcessed
		if done {
			return processed < sqsReaperPerQueueBudget, nil
		}
		start = nextScanCursorAfter(page[len(page)-1].Key)
	}
	return false, nil
}

// reapDeadByAgePage processes one ScanAt page during a tombstone reap
// pass. Returns done=true when either the page was the last one or
// the per-queue budget ran out.
func (s *SQSServer) reapDeadByAgePage(ctx context.Context, queueName string, gen uint64, page []*store.KVPair, readTS uint64, processed int) (bool, int, error) {
	for _, kvp := range page {
		if err := ctx.Err(); err != nil {
			return true, processed, errors.WithStack(err)
		}
		parsed, ok := parseSqsMsgByAgeKey(kvp.Key, queueName)
		if !ok || parsed.Generation != gen {
			continue
		}
		// Legacy byage path: nil meta + partition 0 keeps the
		// dispatch helpers on the legacy constructors (byte-
		// identical to the pre-PR-5b reaper). The partitioned
		// twin (reapDeadByAgePartitionPage) takes the meta-aware
		// branch via reapOneRecordPartitioned.
		if err := s.reapOneRecord(ctx, queueName, nil, 0, gen, kvp.Key, parsed.MessageID, readTS); err != nil {
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

// reapDeadByAgePartition is the partitioned-keyspace twin of
// reapDeadByAge. Each iteration scans one partition's byage prefix
// for one (queue, gen) cohort, parses the partitioned byage key,
// and dispatches the (data, vis, byage, optional group-lock)
// quartet delete for the message. Threads partition through
// reapOneRecord so the dispatch helpers route to the partitioned
// data / vis keys, not the legacy ones.
func (s *SQSServer) reapDeadByAgePartition(ctx context.Context, queueName string, gen uint64, partition uint32, readTS uint64) (bool, error) {
	prefix := sqsPartitionedMsgByAgePrefixForPartition(queueName, partition, gen)
	upper := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)
	processed := 0
	for processed < sqsReaperPerQueueBudget {
		page, err := s.store.ScanAt(ctx, start, upper, sqsReaperPageLimit, readTS)
		if err != nil {
			return false, errors.WithStack(err)
		}
		if len(page) == 0 {
			return true, nil
		}
		done, newProcessed, err := s.reapDeadByAgePartitionPage(ctx, queueName, gen, partition, page, readTS, processed)
		if err != nil {
			return false, err
		}
		processed = newProcessed
		if done {
			return processed < sqsReaperPerQueueBudget, nil
		}
		start = nextScanCursorAfter(page[len(page)-1].Key)
	}
	return false, nil
}

// reapDeadByAgePartitionPage is the partitioned twin of
// reapDeadByAgePage. Parses each entry as a partitioned byage key
// (verifying the partition matches — defensive against page
// boundaries that span partitions, which the prefix scan should
// already prevent) and feeds the partition-aware reapOneRecord.
func (s *SQSServer) reapDeadByAgePartitionPage(ctx context.Context, queueName string, gen uint64, partition uint32, page []*store.KVPair, readTS uint64, processed int) (bool, int, error) {
	for _, kvp := range page {
		if err := ctx.Err(); err != nil {
			return true, processed, errors.WithStack(err)
		}
		parsed, ok := parseSqsPartitionedMsgByAgeKey(kvp.Key, queueName)
		if !ok || parsed.Partition != partition || parsed.Generation != gen {
			continue
		}
		if err := s.reapOneRecordPartitioned(ctx, queueName, partition, gen, kvp.Key, parsed.MessageID, readTS); err != nil {
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

// deleteAllPrefix scans the given prefix and Dispatch-deletes every
// key it finds, one at a time. Returns done=true when the prefix is
// empty (or empty enough that this tick exhausted its work).
func (s *SQSServer) deleteAllPrefix(ctx context.Context, prefix []byte, readTS uint64) (bool, error) {
	upper := prefixScanEnd(prefix)
	start := bytes.Clone(prefix)
	processed := 0
	for processed < sqsReaperPerQueueBudget {
		page, err := s.store.ScanAt(ctx, start, upper, sqsReaperPageLimit, readTS)
		if err != nil {
			return false, errors.WithStack(err)
		}
		if len(page) == 0 {
			return true, nil
		}
		for _, kvp := range page {
			if err := ctx.Err(); err != nil {
				return false, errors.WithStack(err)
			}
			if err := s.dispatchDedupDelete(ctx, kvp.Key, readTS); err != nil {
				return false, err
			}
			processed++
			if processed >= sqsReaperPerQueueBudget {
				return false, nil
			}
		}
		if len(page) < sqsReaperPageLimit {
			return true, nil
		}
		start = nextScanCursorAfter(page[len(page)-1].Key)
	}
	return false, nil
}

// sqsMsgDedupKeyPrefix / sqsMsgGroupKeyPrefix return the (queue, gen)
// prefix for the dedup and group keyspaces. Pulled out as helpers
// so the tombstone reaper does not need to know the encoding.
func sqsMsgDedupKeyPrefix(queueName string, gen uint64) []byte {
	buf := make([]byte, 0, len(SqsMsgDedupPrefix)+sqsKeyCapSmall)
	buf = append(buf, SqsMsgDedupPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	return buf
}

func sqsMsgGroupKeyPrefix(queueName string, gen uint64) []byte {
	buf := make([]byte, 0, len(SqsMsgGroupPrefix)+sqsKeyCapSmall)
	buf = append(buf, SqsMsgGroupPrefix...)
	buf = append(buf, encodeSQSSegment(queueName)...)
	buf = appendU64(buf, gen)
	return buf
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
		// Live-queue retention reap currently iterates only the
		// legacy byage keyspace; partitioned-byage live-queue
		// retention is a follow-up to PR 6a (the tombstoned-cohort
		// path is what this PR addresses).
		if err := s.reapOneRecord(ctx, queueName, nil, 0, parsed.Generation, kvp.Key, parsed.MessageID, readTS); err != nil {
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
//
// Legacy reaper callers pass nil meta + partition 0 so the dispatch
// helpers route to the legacy constructors (byte-identical to the
// pre-PR-5b layout). Partitioned reaper callers (PR 6a) pass a
// synthetic *sqsQueueMeta carrying the tombstone-encoded
// PartitionCount so the dispatch helpers route to the partitioned
// constructors.
func (s *SQSServer) reapOneRecord(ctx context.Context, queueName string, meta *sqsQueueMeta, partition uint32, gen uint64, byAgeKey []byte, messageID string, readTS uint64) error {
	dataKey := sqsMsgDataKeyDispatch(meta, queueName, partition, gen, messageID)
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
	req, err := s.buildReapOps(ctx, queueName, meta, partition, gen, byAgeKey, dataKey, parsed, readTS)
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

// reapOneRecordPartitioned is a thin convenience wrapper around
// reapOneRecord for the partitioned-byage enumeration: synthesises
// a meta carrying any value of PartitionCount > 1 so the dispatch
// helpers route to the partitioned key family. The exact value is
// not consulted by the reaper's per-key dispatch path — the
// helpers only branch on the legacy-vs-partitioned bit
// (PartitionCount > 1) — so we use the minimum legal partitioned
// value as a sentinel rather than the queue's real count, which
// would imply the synthetic meta carries information it actually
// does not (Claude review on PR #735).
func (s *SQSServer) reapOneRecordPartitioned(ctx context.Context, queueName string, partition uint32, gen uint64, byAgeKey []byte, messageID string, readTS uint64) error {
	const partitionedDispatchSentinel uint32 = 2
	syntheticMeta := &sqsQueueMeta{PartitionCount: partitionedDispatchSentinel}
	return s.reapOneRecord(ctx, queueName, syntheticMeta, partition, gen, byAgeKey, messageID, readTS)
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

func (s *SQSServer) buildReapOps(ctx context.Context, queueName string, meta *sqsQueueMeta, partition uint32, gen uint64, byAgeKey, dataKey []byte, parsed *sqsMessageRecord, readTS uint64) (*kv.OperationGroup[kv.OP], error) {
	// meta + partition route the dispatch helpers to the right key
	// family: nil meta + partition 0 is the legacy reaper path
	// (byte-identical to pre-PR-5b layout); a synthetic meta with
	// PartitionCount>1 + a real partition is the partitioned reaper
	// path landed in PR 6a.
	visKey := sqsMsgVisKeyDispatch(meta, queueName, partition, gen, parsed.VisibleAtMillis, parsed.MessageID)
	readKeys := [][]byte{byAgeKey, dataKey, visKey, sqsQueueMetaKey(queueName), sqsQueueGenKey(queueName)}
	elems := []*kv.Elem[kv.OP]{
		{Op: kv.Del, Key: byAgeKey},
		{Op: kv.Del, Key: dataKey},
		{Op: kv.Del, Key: visKey},
	}
	if parsed.MessageGroupId != "" {
		lockKey := sqsMsgGroupKeyDispatch(meta, queueName, partition, gen, parsed.MessageGroupId)
		lock, err := s.loadFifoGroupLock(ctx, queueName, meta, partition, gen, parsed.MessageGroupId, readTS)
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
