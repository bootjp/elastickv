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
// observer Start loop calls this on every tick.
//
// Returns:
//
//   - (snaps, true)  — leader, scrape OK. Observer writes snaps to
//     the gauges and diffs against the previous tick (forgetting
//     any queue that disappeared from this snapshot).
//   - (nil, true)    — this node is a follower (leader-only emission
//     keeps gauges consistent with AdminListQueues / AdminDescribeQueue
//     at the same instant — follower scans would race the leader's
//     writes). Empty-but-OK so the observer ForgetQueue's any
//     gauges this node was emitting before stepping down.
//   - (nil, false)   — leader, but scrape failed (transient
//     catalog-read error or ctx cancel mid-scan). Tells the
//     observer to skip this tick: leave existing gauges in place
//     rather than wiping every depth series — a single failed
//     scrape would otherwise dashboard-render as a false "all
//     queues drained" event until the next successful tick.
//
// Per-queue scan errors (loadQueueMetaAt / scanApproxCounters)
// remain handled in-line by snapshotOneQueueDepth: the offending
// queue is dropped from this tick's snapshot but ok stays true,
// so the observer ForgetQueue's just that one queue's gauges.
// Only a top-level scanQueueNames failure (which would silently
// turn into "no queues anywhere") flips ok to false.
func (s *SQSServer) SnapshotQueueDepths(ctx context.Context) ([]SQSQueueDepth, bool) {
	if s == nil || s.coordinator == nil || s.store == nil || !s.coordinator.IsLeader() {
		return nil, true
	}
	// Take ONE read timestamp up front and pass it through both the
	// membership scan and the per-queue counter scans. With separate
	// timestamps the membership read and the per-queue reads land on
	// different MVCC views, so a queue created or deleted between
	// them would be silently missed (or reported with stale
	// counters) for one tick — and the observer's "current vs
	// previous" diff would then ForgetQueue it spuriously, dashboard-
	// rendering as a phantom drop. One ts per tick is also lighter
	// on the leader's HLC than two.
	readTS := s.nextTxnReadTS(ctx)
	names, err := s.scanQueueNamesAt(ctx, readTS)
	if err != nil {
		slog.Warn("sqs depth snapshot: scanQueueNamesAt failed", "err", err)
		return nil, false
	}
	out := make([]SQSQueueDepth, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			// ctx cancel mid-iteration: partial snapshot is
			// useless because the observer would diff against it
			// and ForgetQueue everything we hadn't reached yet.
			// Signal skip-tick instead.
			return nil, false
		}
		if snap, ok := s.snapshotOneQueueDepth(ctx, name, readTS); ok {
			out = append(out, snap)
		}
	}
	return out, true
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
