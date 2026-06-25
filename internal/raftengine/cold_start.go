package raftengine

// ColdStartObserver receives cold-start snapshot-restore lifecycle
// events from restoreSnapshotState. Implementations live in the
// monitoring package and wire to Prometheus counters/gauges; the
// engine receives a value through OpenConfig and treats nil as
// "no metrics emitted" (preserves the byte-for-byte cold-start
// behaviour for tests and callers that do not wire monitoring).
//
// Three outcomes match the design's strictly-additive policy
// (docs/design/2026_06_02_idempotent_snapshot_restore.md §9):
//
//   - RestoreSkipped: the gate fired. `gap = haveAppliedIndex -
//     snapshot.Metadata.Index` (how far ahead the live store was).
//     This is the user-visible perf win.
//
//   - RestoreExecuted: the gate did NOT fire because the live store
//     was genuinely stale (haveAppliedIndex < snapshot.Metadata.Index).
//     `gap = snapshot.Metadata.Index - haveAppliedIndex` (the work
//     the full restore re-did).
//
//   - RestoreFallback: the strictly-additive fallback path — the
//     FSM did not expose AppliedIndexReader, LastAppliedIndex
//     reported the meta key missing, or it returned an error. The
//     full restore runs but the skip was never even attempted.
//     `reason` carries a stable short label so Prometheus can
//     surface why the optimisation could not engage:
//
//     not_reader   — FSM does not implement AppliedIndexReader
//     missing_meta — meta key absent (pre-upgrade fsm.db)
//     read_err     — LastAppliedIndex returned an error
//
// Implementations MUST NOT block; the engine calls these on the
// cold-start critical path. Treat all label/string arguments as
// untrusted enum values from the engine's enumeration above.
type ColdStartObserver interface {
	RestoreSkipped(snapIndex, haveAppliedIndex uint64)
	RestoreExecuted(snapIndex, haveAppliedIndex uint64)
	RestoreFallback(snapIndex uint64, reason string)
}
