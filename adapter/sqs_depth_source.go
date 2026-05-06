package adapter

import (
	"context"
	"log/slog"
)

// SQSQueueDepth is one queue's depth-attribute snapshot, the unit
// the SQSServer hands to monitoring.SQSObserver on each tick. The
// fields mirror sqsApproxCounters byte-for-byte and the public
// AdminQueueCounters JSON shape — operators see consistent numbers
// in dashboards and the admin SPA.
type SQSQueueDepth struct {
	Queue      string
	Visible    int64
	NotVisible int64
	Delayed    int64
}

// SnapshotQueueDepths satisfies monitoring.SQSDepthSource. The
// observer Start loop calls this on every tick; the SQSServer
// returns one entry per known queue when this node is the verified
// Raft leader, or an empty slice on followers. Leader-only
// emission keeps the dashboard's queue-depth gauges consistent
// with what AdminListQueues / AdminDescribeQueue would return at
// the same instant — followers that scanned the catalog at the
// same time would race the leader's writes and emit conflicting
// values for the same series.
//
// Per-queue scan errors are logged and the offending queue is
// dropped from this tick's snapshot. The observer detects the
// disappearance and ForgetQueue's the gauges so the dashboard
// surfaces "scrape failed" as a missing series rather than as a
// pinned stale backlog.
func (s *SQSServer) SnapshotQueueDepths(ctx context.Context) []SQSQueueDepth {
	if s == nil || s.coordinator == nil || s.store == nil || !s.coordinator.IsLeader() {
		return nil
	}
	names, err := s.scanQueueNames(ctx)
	if err != nil {
		slog.Warn("sqs depth snapshot: scanQueueNames failed", "err", err)
		return nil
	}
	// Take a single read timestamp for the whole tick so all queues
	// in this snapshot share the same MVCC view. With per-queue
	// nextTxnReadTS the first queue's read could see a state the
	// last queue's read can't (catalog mutation between calls), and
	// every call burns an HLC tick on the leader. One ts per tick
	// is both more consistent and lighter on the leader's HLC.
	readTS := s.nextTxnReadTS(ctx)
	out := make([]SQSQueueDepth, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return out
		}
		if snap, ok := s.snapshotOneQueueDepth(ctx, name, readTS); ok {
			out = append(out, snap)
		}
	}
	return out
}

// snapshotOneQueueDepth runs the per-queue catalog read pair
// (loadQueueMetaAt + scanApproxCounters) at the caller-supplied
// readTS and returns the resulting snapshot. Pulled out of the
// loop body so SnapshotQueueDepths stays under the cyclop budget;
// ok=false means "skip this queue from this tick" (queue gone,
// transient catalog read failure). Per-queue scan errors are
// logged and the offending queue is dropped from this tick's
// snapshot rather than aborting the entire pass.
func (s *SQSServer) snapshotOneQueueDepth(ctx context.Context, name string, readTS uint64) (SQSQueueDepth, bool) {
	meta, exists, err := s.loadQueueMetaAt(ctx, name, readTS)
	if err != nil || !exists {
		return SQSQueueDepth{}, false
	}
	counters, err := s.scanApproxCounters(ctx, name, meta.Generation, readTS)
	if err != nil {
		slog.Warn("sqs depth snapshot: counters failed", "queue", name, "err", err)
		return SQSQueueDepth{}, false
	}
	return SQSQueueDepth{
		Queue:      name,
		Visible:    counters.Visible,
		NotVisible: counters.NotVisible,
		Delayed:    counters.Delayed,
	}, true
}
