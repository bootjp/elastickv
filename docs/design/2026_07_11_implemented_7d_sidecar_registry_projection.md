# Implemented 7d: sidecar registry projection

Status: Implemented
Author: bootjp
Date: 2026-07-11

## Scope

This closes the remaining Stage 7 §5.5 recovery surface for
`WriterRegistryForCaller`.

The prior Stage 7 slices already registered writers at process start,
storage-layer admission, runtime cutover/rotation, and membership
change time. The open gap was the compaction-fallback recovery RPC:
a node whose sidecar fell behind a compacted Raft log needs the
leader's per-DEK `last_seen_local_epoch` record for that specific
caller so it can choose a strictly monotonic replacement
`local_epoch`.

## Implementation

`EncryptionAdminServer` now accepts the production
`encryption.WriterRegistryStore` through
`WithEncryptionAdminWriterRegistry`.

`GetSidecarState` projects registry rows for the local full node ID.
`ResyncSidecar` projects rows for `ResyncSidecarRequest.caller_full_node_id`.
Both responses keep returning a non-nil empty map when the registry is
not wired, preserving the encryption-disabled and test posture.

For each DEK present in the sidecar, the server reads
`RegistryKey(dek_id, NodeID16(full_node_id))` and returns the decoded
`LastSeenLocalEpoch`. Missing rows are omitted instead of zero-filled,
because zero could be mistaken for a real registry observation. A
decoded row whose stored `FullNodeID` does not match the requested
caller is rejected as an internal node-ID collision.

Production wiring stores the writer registry on each
`raftGroupRuntime` as the group is built, then passes it into the
per-shard `EncryptionAdmin` registration in `startRaftServers`.

## Validation

- `go test ./adapter -run 'TestEncryptionAdmin_(GetSidecarState|ResyncSidecar)' -count=1 -timeout=240s`
- `go test ./adapter -run TestEncryptionAdmin -count=1 -timeout=300s`
- `go test ./store ./internal/encryption . -count=1 -timeout=180s`
- `golangci-lint run ./adapter ./store ./internal/encryption . --timeout=5m`

The broader `go test ./adapter ./store ./internal/encryption . -count=1
-timeout=300s` run timed out in the full adapter package. The targeted
encryption admin tests and the directly affected non-adapter packages
passed.
