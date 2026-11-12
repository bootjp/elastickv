package main

import (
	"context"
	"strconv"

	"github.com/bootjp/elastickv/adapter"
	"github.com/bootjp/elastickv/kv"
	"github.com/bootjp/elastickv/monitoring"
)

func raftMonitorRuntimes(runtimes []*raftGroupRuntime) []monitoring.RaftRuntime {
	out := make([]monitoring.RaftRuntime, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.Engine == nil {
			continue
		}
		out = append(out, monitoring.RaftRuntime{
			GroupID:      runtime.Spec.ID,
			StatusReader: runtime.Engine,
			ConfigReader: runtime.Engine,
		})
	}
	return out
}

// fsmApplySyncModeLabeler narrows an MVCCStore to those implementations
// that can report the resolved ELASTICKV_FSM_SYNC_MODE label. The
// pebble-backed store satisfies this today; alternate backends (none
// yet) would either implement it or be skipped.
type fsmApplySyncModeLabeler interface {
	FSMApplySyncModeLabel() string
}

// fsmApplySyncModeLabelFromRuntimes returns the FSM apply sync-mode
// label resolved by the first shard store that exposes it. All shards
// on a node read the same ELASTICKV_FSM_SYNC_MODE env var at
// construction time so the label is uniform across the runtimes;
// returning the first one suffices. Returns "" when no runtime
// exposes the accessor, in which case the caller skips emitting the
// gauge to avoid publishing a misleading default.
func fsmApplySyncModeLabelFromRuntimes(runtimes []*raftGroupRuntime) string {
	for _, runtime := range runtimes {
		if runtime == nil || runtime.Store == nil {
			continue
		}
		src, ok := runtime.Store.(fsmApplySyncModeLabeler)
		if !ok {
			continue
		}
		return src.FSMApplySyncModeLabel()
	}
	return ""
}

func recordFSMApplySyncMode(reg *monitoring.Registry, runtimes []*raftGroupRuntime) {
	if label := fsmApplySyncModeLabelFromRuntimes(runtimes); label != "" {
		reg.SetFSMApplySyncMode(label)
	}
}

// pebbleMonitorSources extracts the MVCC stores that expose
// *pebble.DB.Metrics() so monitoring can poll LSM internals (L0
// sublevels, compaction debt, memtable, block cache) for the
// elastickv_pebble_* metrics family. Stores that do not satisfy the
// interface (non-Pebble backends, if any are added later) are skipped
// silently.
func pebbleMonitorSources(runtimes []*raftGroupRuntime) []monitoring.PebbleSource {
	out := make([]monitoring.PebbleSource, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.Store == nil {
			continue
		}
		src, ok := runtime.Store.(monitoring.PebbleMetricsSource)
		if !ok {
			continue
		}
		out = append(out, monitoring.PebbleSource{
			GroupID:    runtime.Spec.ID,
			GroupIDStr: strconv.FormatUint(runtime.Spec.ID, 10),
			Source:     src,
		})
	}
	return out
}

// dispatchMonitorSources extracts the raft engines that expose etcd
// dispatch counters so monitoring can poll them for the hot-path
// dashboard. Engines that do not satisfy the interface are skipped
// silently; their groups simply won't contribute to
// elastickv_raft_dispatch_* metrics.
func dispatchMonitorSources(runtimes []*raftGroupRuntime) []monitoring.DispatchSource {
	out := make([]monitoring.DispatchSource, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.Engine == nil {
			continue
		}
		src, ok := runtime.Engine.(monitoring.DispatchCounterSource)
		if !ok {
			continue
		}
		out = append(out, monitoring.DispatchSource{
			GroupID: runtime.Spec.ID,
			Source:  src,
		})
	}
	return out
}

// startMonitoringCollectors wires up the per-tick Prometheus
// collectors (raft dispatch, Pebble LSM, store-layer OCC conflicts)
// on top of the running raft runtimes. Kept separate from run() so
// the latter stays under the cyclop complexity budget and so new
// collectors can be added without widening run() further.
func startMonitoringCollectors(
	ctx context.Context,
	reg *monitoring.Registry,
	runtimes []*raftGroupRuntime,
	clock *kv.HLC,
	encryptionState monitoring.EncryptionStateSource,
) {
	reg.RaftObserver().Start(ctx, raftMonitorRuntimes(runtimes), raftMetricsObserveInterval)
	if collector := reg.DispatchCollector(); collector != nil {
		collector.Start(ctx, dispatchMonitorSources(runtimes), raftMetricsObserveInterval)
	}
	if collector := reg.PebbleCollector(); collector != nil {
		collector.Start(ctx, pebbleMonitorSources(runtimes), raftMetricsObserveInterval)
	}
	if collector := reg.WriteConflictCollector(); collector != nil {
		collector.Start(ctx, writeConflictMonitorSources(runtimes), raftMetricsObserveInterval)
	}
	if obs := reg.HLCObserver(); obs != nil && clock != nil {
		obs.Start(ctx, clock, raftMetricsObserveInterval)
	}
	if obs := reg.EncryptionStateObserver(); obs != nil {
		obs.Start(ctx, encryptionState, raftMetricsObserveInterval)
	}
}

// startSQSDepthObserver wires the SQS adapter (when enabled on this
// node) into the monitoring registry's SQSObserver so the
// elastickv_sqs_queue_messages gauges start updating. Mirrors the
// Raft / Redis pattern: the source is plugged in once after startup,
// then the observer owns the ticker. nil sqsServer (e.g.
// --sqsAddress empty on this node) is a no-op.
//
// The thin adapter exists because monitoring.SQSQueueDepth and
// adapter.SQSQueueDepth are intentionally distinct types — having
// the adapter import monitoring would invert the dependency
// direction (every adapter would then know about Prometheus).
// Conversion is a fixed 4-field copy and a shape mismatch surfaces
// at compile time here, not at runtime on the metrics path.
func startSQSDepthObserver(ctx context.Context, reg *monitoring.Registry, sqsServer *adapter.SQSServer) {
	if reg == nil || sqsServer == nil {
		return
	}
	if observer := reg.SQSObserver(); observer != nil {
		observer.Start(ctx, sqsDepthSourceAdapter{inner: sqsServer}, 0)
	}
}

// sqsDepthSourceAdapter bridges *adapter.SQSServer (which returns
// []adapter.SQSQueueDepth) to monitoring.SQSDepthSource (which
// expects []monitoring.SQSQueueDepth). Same shape both sides; the
// loop is a fixed-size copy.
type sqsDepthSourceAdapter struct {
	inner interface {
		SnapshotQueueDepths(context.Context) ([]adapter.SQSQueueDepth, bool)
	}
}

func (a sqsDepthSourceAdapter) SnapshotQueueDepths(ctx context.Context) ([]monitoring.SQSQueueDepth, bool) {
	if a.inner == nil {
		// Empty-but-OK: nothing to emit. Mirrors the
		// follower / nil-receiver case of the underlying source.
		return nil, true
	}
	snaps, ok := a.inner.SnapshotQueueDepths(ctx)
	if !ok {
		// Propagate skip-tick verbatim so the observer leaves
		// existing gauges alone on a transient scan failure.
		return nil, false
	}
	if snaps == nil {
		return nil, true
	}
	out := make([]monitoring.SQSQueueDepth, len(snaps))
	for i, s := range snaps {
		out[i] = monitoring.SQSQueueDepth{
			Queue:      s.Queue,
			Visible:    s.Visible,
			NotVisible: s.NotVisible,
			Delayed:    s.Delayed,
		}
	}
	return out, true
}

// writeConflictMonitorSources extracts the MVCC stores that expose
// per-(kind, key_prefix) OCC conflict counters so monitoring can poll
// them for the elastickv_store_write_conflict_total metric. Every
// store.MVCCStore implements WriteConflictCountsByPrefix(); stores
// that do not track conflicts return an empty map and simply do not
// contribute series.
func writeConflictMonitorSources(runtimes []*raftGroupRuntime) []monitoring.WriteConflictSource {
	out := make([]monitoring.WriteConflictSource, 0, len(runtimes))
	for _, runtime := range runtimes {
		if runtime == nil || runtime.Store == nil {
			continue
		}
		src, ok := runtime.Store.(monitoring.WriteConflictCounterSource)
		if !ok {
			continue
		}
		out = append(out, monitoring.WriteConflictSource{
			GroupID:    runtime.Spec.ID,
			GroupIDStr: strconv.FormatUint(runtime.Spec.ID, 10),
			Source:     src,
		})
	}
	return out
}
