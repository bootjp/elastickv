# Physical Snapshot Object Offload

Status: Proposed — M0 implemented; M1 object-store-neutral substrate partial
Author: bootjp
Date: 2026-07-19
Updated: 2026-07-23

## 1. Scope

This design owns storage-tier milestone M4 from the scaling roadmap: periodic,
per-Raft-group physical snapshots stored in an external S3-compatible object
store, bounded retention, and restore into a fresh elastickv data directory.

The design depends on the Pebble SST ingest snapshot transfer contract from
PR `#1130`. It does not copy, parse, or reimplement that format. The object layer
treats the complete FSM payload as opaque bytes. On restore, `kvFSM.Restore`
dispatches the inner store payload to the existing receiver, so `EKVSSTI1`
uses the same manifest validation and `pebble.DB.Ingest` path as routine Raft
snapshot transfer. Legacy payloads remain valid through the same receiver.

PR #1130 is still in review. The first implementation slice therefore lands
only the independent substrate:

- open the newest WAL-valid, persisted Raft/FSM snapshot as a single-use
  stream paired with its index, term, ConfState, byte count, and CRC32C;
- pin the opened `.fsm` descriptor while it is streamed so local snapshot
  retention cannot switch the artifact under an upload;
- prepare a fresh Raft data directory from a complete opaque FSM payload
  without adding a second KV snapshot header;
- preserve the existing logical/external restore API and its header behavior.

No object client or runtime scheduling is enabled by this first slice.

The M1 object-store-neutral substrate now adds:

- a `snapshotoffload.ObjectStore` interface with a local filesystem
  implementation for deterministic tests and offline drills;
- the v1 JSON manifest schema and content-addressed payload key layout;
- payload-first publish from `OpenPersistedSnapshotExport`, including exact
  byte count and SHA-256 verification before manifest commit;
- manifest-driven restore that downloads the opaque payload, checks exact
  length and SHA-256, then calls `PreparePhysicalSnapshotRestore` with
  operator-supplied target membership.

The S3-compatible client, operator CLI, runtime scheduler, and retention/GC
remain pending.

## 2. Safety boundary

The exporter consumes only snapshots already committed by the etcd engine:
the WAL-valid `.snap` metadata names an index and term, its `EKVT` token names
the matching `.fsm` file and CRC32C, and the `.fsm` payload is streamed from one
open descriptor. It must not call `StateMachine.Snapshot()` independently from
a timer. Doing so would capture FSM bytes without an atomic Raft
index/term/ConfState witness.

For token-backed snapshots, export performs three integrity checks:

1. token index equals `Snapshot.Metadata.Index`;
2. the footer read from the pinned descriptor equals the token CRC32C before
   bytes are exposed;
3. CRC32C recomputed while streaming equals the token after the final byte.

The object publisher additionally computes SHA-256 and exact byte count during
that same stream. A short read, local mutation, remote write error, or checksum
mismatch publishes no manifest.

## 3. Object layout and commit protocol

The external bucket is a disaster-recovery dependency and must not be the S3
adapter of the cluster being backed up. Storing the cluster's own physical
snapshot inside its FSM creates circular recovery and reintroduces payload
growth into Raft.

```text
<prefix>/v1/payloads/sha256/<first-two>/<sha256>.fsm
<prefix>/v1/groups/<group-id>/snapshots/<index>-<term>.json
```

Payload objects are immutable and content-addressed. The per-group JSON
manifest is the commit marker and contains:

- schema version and creation time;
- source cluster identity and Raft group ID;
- snapshot index, term, and ConfState;
- payload object key, exact length, SHA-256, and source CRC32C;
- binary version and snapshot feature capabilities used by the writer.

Publication order is payload first, manifest last. A retry may overwrite an
identical payload key, but it must reject a different length or checksum. A
manifest is visible only after the payload upload and remote integrity check
succeed. Temporary multipart uploads are aborted on cancellation and swept by
bucket lifecycle policy.

## 4. Scheduling

Each process examines its local Raft groups, but only the current group leader
may publish. The scheduler uploads a persisted snapshot only when its index is
greater than the last successfully published index for that group. Leadership
is checked before opening the snapshot and again before publishing the
manifest; loss of leadership may leave an unreferenced content-addressed
payload but never a committed manifest.

The schedule is opt-in. Initial defaults are one scan every 15 minutes and one
upload at a time per process. Jitter spreads multi-group work. Snapshot
creation cadence remains owned by the Raft engine; object offload never forces
an extra state-machine snapshot in this milestone.

## 5. Retention and reference lifecycle

Retention is per group and combines a minimum generation count with a time
window. GC runs in two phases:

1. list and validate manifests, retain every manifest inside the policy plus
   the newest successful manifest, then delete expired manifest objects;
2. after a grace period, rebuild the live payload SHA set from all remaining
   manifests and delete only payload objects with no live reference.

Malformed manifests fail closed: they are reported and excluded from both
automatic manifest deletion and payload reclamation. Listing failure,
pagination failure, or an incomplete group scan performs no deletes. This
keeps retryable publication or object-store inconsistency from reclaiming a
payload still referenced by a committed manifest.

## 6. Restore

Restore is offline and targets an absent data directory:

1. fetch and validate the selected manifest;
2. download the payload to a same-filesystem temporary regular file while
   enforcing exact length and SHA-256;
3. fsync and atomically rename the verified download;
4. call `PreparePhysicalSnapshotRestore` with manifest index/term and
   operator-supplied target membership;
5. start elastickv normally. `kvFSM.Restore` consumes the complete FSM header,
   and the store receiver performs SST manifest verification and ingest when
   the opaque inner payload is `EKVSSTI1`.

The target membership is explicit operator input rather than copied blindly
from the source ConfState. This supports disaster recovery onto replacement
addresses while keeping source membership in the manifest for audit.

## 7. Configuration and security

The runtime slice will require an external endpoint, bucket, prefix, region,
credentials provider, schedule, retention count/window, upload concurrency,
and server-side encryption mode. Static secrets must use file or environment
providers and must not appear in process arguments or manifests.

Storage-envelope encryption protects values but not all physical keys and
metadata. The external bucket therefore requires private ACLs, TLS, and
server-side encryption (SSE-S3 or SSE-KMS). Anonymous reads and writes are a
deployment failure. Object credentials need only scoped list/get/put/delete
permissions below the configured prefix.

## 8. Milestones

| Milestone | Scope | Status |
|---|---|---|
| M0 | Persisted snapshot export handle, complete-payload restore preparation, focused design | Implemented in the first substrate PR |
| M1 | Object client interface, S3-compatible implementation, immutable payload/manifest publication, download verification, operator CLI | Partial: object-store interface, local implementation, manifest schema, payload-first publish, and verified restore are implemented; S3 client and CLI pending |
| M2 | Leader-only per-group scheduler, metrics, jitter, concurrency bounds, cancellation and restart idempotency | Pending |
| M3 | Retention/GC, restore drills, corruption tests, multi-node acceptance, operational documentation | Pending |

The filename and header remain `proposed` until M1-M3 complete the central
object-offload subsystem. At that point the completion PR must use `git mv` to
rename this file to `2026_07_19_implemented_physical_snapshot_object_offload.md`,
change the header status, update every reference, and verify with `rg` that the
old path is absent before requesting review.

## 9. Acceptance criteria

- Opaque `EKVSSTI1` bytes round-trip without object-layer parsing or header
  rewriting and restore through the #1130 ingest receiver.
- Snapshot metadata and FSM payload always refer to the same Raft index.
- Corrupt local payload, footer/token mismatch, truncated upload, remote
  checksum mismatch, or malformed manifest cannot publish a committed
  snapshot.
- A publish interrupted before manifest creation is invisible to restore and
  later reclaimed only after the GC grace period.
- Restore never mutates an existing destination and leaves no published target
  directory after checksum or preparation failure.
- Retention never deletes the newest valid manifest for a group and never
  deletes a payload referenced by any retained manifest.
- Restart and leadership change do not duplicate committed indexes or run
  unbounded concurrent uploads.
- A multi-node drill restores each group from external objects into fresh data
  directories and passes adapter reads after normal Raft startup.

## 10. Intentional non-goals

- Parsing or duplicating the SST ingest stream owned by #1130.
- Uploading user S3 object blobs; that is owned by the S3 Raft blob-offload
  design.
- Logical, cross-adapter, vendor-independent backup; that is owned by the
  logical backup design.
- A cluster-wide atomic timestamp across groups. This milestone produces
  independently restorable per-group physical snapshots.
- Continuous WAL archiving, incremental backup, or cross-region authority and
  fencing.
