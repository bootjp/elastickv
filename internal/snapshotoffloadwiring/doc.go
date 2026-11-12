// Package snapshotoffloadwiring starts physical snapshot offload for a
// server process: it owns the offload and retention flags, validates them,
// builds the S3 or filesystem object store with a TLS-enforcing HTTP client,
// cleans stale spool files, and runs the scheduler and retention workers.
// The binary reduces its Raft groups to snapshotoffload.OffloadGroup values
// before calling Start, so this package never sees the node runtime.
package snapshotoffloadwiring
