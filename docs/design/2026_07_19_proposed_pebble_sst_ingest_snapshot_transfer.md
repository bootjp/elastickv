# Pebble SST Ingest Snapshot Transfer

Status: Proposed
Author: bootjp
Date: 2026-07-19

## 1. Scope

This design implements storage-tier milestone M1 from the scaling roadmap. A
Pebble-backed Raft state machine may emit a checkpoint-derived set of external
SST files. The existing Raft snapshot transport carries those files in a
self-describing stream, and the receiver verifies and ingests them into an
empty temporary Pebble database before replacing the live database.

The central scope is:

- capture a point-in-time checkpoint that matches the Raft snapshot request;
- export non-overlapping, ingestible SST files;
- carry an integrity-protected manifest and file bodies through the existing
  disk-spooled gRPC snapshot transport;
- restore by `pebble.DB.Ingest`, not by per-entry Pebble batches;
- leave the current database untouched on parse, checksum, ingest, metadata,
  rename, or reopen failure;
- clean temporary checkpoint, export, receive, ingest, and rollback paths;
- preserve the legacy `EKVPBBL1` stream as the default and as a sender-side
  fallback.

This change is independent of physical object offload. It does not reference
S3, remote Pebble objects, shared storage locators, or the backup subsystem.

## 2. Pebble constraint

Pebble's local ingest API accepts external SSTs whose internal sequence number
is zero, then assigns the ingest sequence number atomically. SST files already
owned by a live Pebble database contain non-zero sequence numbers and therefore
cannot be copied directly into `DB.Ingest`.

The sender consequently performs one merge iteration over a stable checkpoint
and writes equivalent external SSTs with `sstable.Writer`. MVCC versions remain
distinct because elastickv's physical Pebble user key already includes the
inverted commit timestamp. Values, including storage-encryption envelopes, are
copied byte-for-byte. This removes the receiver's per-key batch rewrite and
write amplification while respecting Pebble's ingest contract.

## 3. Consistency boundary

`pebbleStore.Snapshot` is called synchronously from the Raft run loop before
the next committed entry is applied. When SST snapshots are enabled it acquires
locks in the existing order:

1. `maintenanceMu`;
2. `dbMu.Lock`;
3. `applyMu`.

While those locks are held, the store flushes the memtable and calls
`DB.Checkpoint(..., pebble.WithFlushedWAL())`. The exclusive database lock is
required because direct `PutAt`, `DeleteAt`, and `ExpireAt` writes do not use
`applyMu`. Compaction, restore, close, direct writes, Raft apply batches,
applied-index checkpoints, and prefix-delete batches cannot cross this
boundary. The checkpoint is therefore the same logical store image captured
by the fallback `pebble.Snapshot` handle.

Checkpoint construction stays in `Snapshot`; expensive SST export remains in
`Snapshot.WriteTo`. This is necessary because deferring `DB.Checkpoint` to the
background writer would allow later Raft applies into an older snapshot index.

## 4. Stream format

The store payload uses the following format. The existing KV FSM header and
the Raft engine's `.fsm` CRC32C footer remain outside it and are unchanged.

```text
magic                 8 bytes  "EKVSSTI1"
manifest_length       8 bytes  signed big-endian, 1..64 MiB
manifest_sha256      32 bytes
manifest JSON         manifest_length bytes
file[0] bytes         manifest.files[0].size bytes
...
file[n] bytes         manifest.files[n].size bytes
EOF                   required; trailing bytes are rejected
```

The manifest records:

- format version;
- `last_commit_ts`, committed and pending retention watermarks;
- optional durable Raft applied index;
- entry count and uncompressed byte count;
- total SST bytes;
- ordered file name, size, SHA-256, smallest key, and largest key.

File names are canonical (`000000.sst`, `000001.sst`, ...), so a manifest
cannot escape the receive directory. Key bounds must be strictly
non-overlapping. The manifest SHA-256 is checked before any file is created;
each file SHA-256 and exact byte count are checked before ingest. The outer
Raft CRC32C remains a transport and disk corruption check, while the inner
hashes prevent the store from swapping state before the outer restore pass
finishes computing its footer checksum.

The stream remains compatible with arbitrary gRPC chunk boundaries. No file is
materialized in memory; the sender copies files to the existing `.fsm` spool
and the receiver copies each bounded body directly to a staging file.

## 5. Export and transport

The checkpoint is opened read-only. A sorted iterator writes approximately
64 MiB external SST files using the checkpoint's table format and Pebble's
default comparer. The boundary is approximate because an individual entry is
never split. The exporter computes the physical maximum MVCC commit timestamp
while iterating and raises the manifest watermark if an older metadata key is
stale.

All files are completed and hashed before the first new-format byte is written.
If checkpoint creation, checkpoint open, iteration, SST close, manifest encode,
or pre-stream preparation fails, `WriteTo` emits the legacy `EKVPBBL1` stream
from the point-in-time fallback snapshot. Once streaming begins, an I/O error
fails the snapshot write; it never appends a second format to a partial stream.

The existing `SendSnapshot` gRPC RPC remains the authoritative Raft transport.
It already provides disk spooling, bounded chunks, flow control, CRC32C, retry,
and orphan cleanup. Parallel multi-RPC transfer of files is a future extension:
it needs a resumable transfer ID and random-access receiver assembly to retain
the current one-token/one-snapshot publication rule. It is not required for
the safe ingest format and does not couple this work to remote object storage.

## 6. Restore and rollback

The receiver accepts `EKVSSTI1` regardless of whether local emission is
enabled. It performs these steps while `Restore` holds the existing exclusive
maintenance, DB, and metadata locks:

1. parse and validate the manifest;
2. receive each file into a sibling staging directory and verify SHA-256;
3. reject truncation and trailing bytes;
4. open a fresh sibling Pebble database;
5. ingest all files in one `DB.Ingest` call;
6. synchronously write and verify snapshot metadata;
7. close the temporary database;
8. rename the current database to a same-filesystem rollback directory;
9. rename and reopen the replacement;
10. verify metadata again, then delete the rollback directory.

If rename, reopen, or metadata verification fails after step 8, the replacement
is closed and removed, the old directory is renamed back, and the old database
is reopened. The same rollback helper also strengthens the existing native and
streaming-MVCC restore callers.

At startup, a missing live directory and exactly one rollback directory means
the process stopped between the two renames, so the rollback directory is
restored before Pebble is opened. Multiple rollback directories are ambiguous
and fail closed without deleting any candidate. A rollback directory beside a
live database is removed only after the live database opens and its metadata
scans succeed. Startup also removes stale directories matching only the
store-specific checkpoint, receive, and ingest prefixes. Normal
`Snapshot.Close` removes its checkpoint, and every export/restore error path
removes its temporary files.

### 6.1 Caller audit

The return and success semantics of `swapInTempDB` are unchanged, but its
failure semantics now restore the previous database. Its three production
callers were audited: native Pebble restore, streaming-MVCC restore, and the new
SST-ingest restore. Each creates a closed temporary database in a sibling
directory and invokes the swap while `Restore` holds `maintenanceMu`, the
exclusive `dbMu`, and `mtx`.

Snapshot callers were also audited. The Raft FSM and leader-routed wrapper keep
the `Snapshot`/`WriteTo`/`Close` contract, while the etcd migration helper still
accepts either payload because it restores through the same receiver-capable
binary. With emission disabled, all callers continue to receive `EKVPBBL1`.

## 7. Rollout and fallback

Emission is disabled by default. Operators first deploy a receiver-capable
binary to every member, then set:

```text
ELASTICKV_PEBBLE_SST_INGEST_SNAPSHOT=true
```

on all members in a rolling restart. This cluster-wide gate is required because
an older receiver does not recognize `EKVSSTI1`. Setting the variable to an
invalid value is fail-safe and leaves legacy emission enabled. Disabling it and
restarting restores legacy emission without changing persisted data.

New receivers continue to restore `EKVPBBL1` and streaming MVCC snapshots. A
new sender also falls back to `EKVPBBL1` before output if SST preparation fails.

## 8. Acceptance evidence

| Requirement | Test or code evidence |
|---|---|
| Checkpoint excludes later writes | `TestPebbleStoreSSTIngestSnapshotRoundTrip` |
| Multiple sorted SST files ingest correctly | `TestPebbleStoreSSTIngestSnapshotRoundTrip` |
| Arbitrary transport chunk boundaries | one-byte reader in `TestPebbleStoreSSTIngestSnapshotRoundTrip` |
| gRPC spool, CRC/token, and fresh-FSM restore | `TestGRPCSnapshotTransportRoundTripsSSTIngestPayload` |
| Manifest, file, truncation/trailing integrity | `TestPebbleStoreSSTIngestSnapshotCorruptionPreservesDestination` |
| Invalid signed manifest length is rejected | `TestSSTIngestManifestRejectsNegativeLength` |
| Corruption leaves destination unchanged | `TestPebbleStoreSSTIngestSnapshotCorruptionPreservesDestination` |
| Preparation failure uses legacy stream | `TestPebbleStoreSSTIngestSnapshotFallsBackBeforeStreaming` |
| Rename/open verification rollback | `TestSwapInTempDBRollsBackOnMetadataFailure` |
| Interrupted rename recovers old DB; ambiguous backups fail closed | `TestPebbleStoreCleansStaleSSTSnapshotArtifacts` |
| Env is opt-in and invalid values fail closed | `TestSSTIngestSnapshotConfigurationAndManifestValidation` |
| KV FSM header preserves the store payload | `TestKVFSMSnapshotRoundTripsSSTIngestPayload` |
| Existing native/MVCC restore behavior | full `go test ./store` suite |

## 9. Intentional non-goals

- physical S3/object offload and shared Pebble object references;
- resumable or parallel multi-RPC snapshot files;
- changing the Raft snapshot token, membership protocol, or snapshot cadence;
- eliminating sender-side iteration, which Pebble's zero-sequence ingest
  contract prevents for live database SSTs;
- proving the roadmap's 5 TiB/one-hour production SLO in unit tests. That
  requires deployment-scale evidence after this safe transport format lands.
